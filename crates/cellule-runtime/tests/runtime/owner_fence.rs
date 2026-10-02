//! Admission fence exposure, stale operation rejection and exact outcome replay.
use crate::support::fixtures::mutation_identity;
use cellule_ltx::{CellReplica, CellStorageLayout, Limits};
use cellule_runtime::cell::actor::CellHandle;
use cellule_runtime::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use cellule_runtime::control::authority::CellAuthority;
use cellule_runtime::control::{Owner, OwnerFence};
use cellule_runtime::identity::{
    ApplicationId, CellTarget, Digest, IncarnationId, NamespaceId, SessionId, TenantId,
};
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};
use cellule_runtime::registry::{
    BuildDescriptor, CellModule, Command, CommandContext, CommandResult, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, OperationDescriptor,
};
use cellule_runtime::{
    CellClient, CellRuntime, InvocationError, Registry, RegistryBuilder, SqlWorkerPool,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::{Arc, OnceLock};

const MODULE: &str = "owner-fence-test";
const NAMESPACE: NamespaceId = NamespaceId::from_bytes([12; 16]);
const SCHEMA: &str = "CREATE TABLE fence_executions(fence BLOB NOT NULL)";
const COMMANDS: &[OperationDescriptor] = &[OperationDescriptor {
    id: 1,
    codec_version: 1,
    schema_min: 1,
    schema_max: 1,
    input_limit: 64,
    output_limit: 64,
}];
struct FenceModule;
impl CellModule for FenceModule {
    const NAME: &'static str = MODULE;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: MODULE,
            source_digest: Digest::from_bytes([13; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: SCHEMA,
                digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
            }])),
            commands: COMMANDS,
            queries: &[],
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: Box::leak(Box::new([NamespaceDescriptor {
                id: NAMESPACE,
                name: MODULE,
                role: CatalogRole::Application,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }])),
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<FencedCommand>()
    }
}
fn encoded(fence: OwnerFence) -> Vec<u8> {
    let mut bytes = fence.incarnation.as_bytes().to_vec();
    bytes.extend_from_slice(&fence.epoch.to_be_bytes());
    bytes
}
struct FencedCommand;
impl Command for FencedCommand {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        expected: Vec<u8>,
    ) -> cellule_runtime::Result<CommandResult<Vec<u8>>> {
        let fence = encoded(context.owner_fence());
        if !expected.is_empty() && expected != fence {
            return Ok(CommandResult::Rejected(fence));
        }
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO fence_executions VALUES(?1)".into(),
                parameters: vec![SqlValue::Blob(fence.clone())],
            }],
        })?;
        Ok(CommandResult::Success(fence))
    }
}
struct Fixture {
    root: tempfile::TempDir,
    target: CellTarget,
    layout: CellStorageLayout,
    replica: CellReplica,
    registry: Arc<Registry>,
    runtime: CellRuntime,
    handle: CellHandle,
}
impl Fixture {
    async fn new() -> Self {
        let session = SessionId::from_bytes([18; 16]);
        let runtime =
            CellRuntime::new(SqlWorkerPool::new(1, 4).unwrap(), 64 << 20, session).unwrap();
        Self::with_runtime(session, Store::new(Arc::new(InMemory::new())), runtime).await
    }

    async fn with_runtime(session: SessionId, store: Store, runtime: CellRuntime) -> Self {
        let mut builder = RegistryBuilder::new(BuildDescriptor {
            source_revision: "owner-fence-test".into(),
            cargo_lock_digest: Digest::from_bytes([14; 32]),
        });
        builder.register(FenceModule).unwrap();
        let registry = Arc::new(builder.finish().unwrap());
        let target = CellTarget::new(
            TenantId::from_bytes([15; 16]),
            ApplicationId::from_bytes([16; 16]),
            NAMESPACE,
            b"operation",
        )
        .unwrap();
        let layout = CellStorageLayout::new(store, Path::from("owner-fence"), [16; 16]);
        let incarnation = IncarnationId::from_bytes([17; 16]);
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let root = tempfile::TempDir::new().unwrap();
        let proof = CellCatalog::new(layout.clone(), target.tenant())
            .provision(
                CatalogEntry::new(
                    &target,
                    CatalogRole::Application,
                    registry.module_code(MODULE).unwrap(),
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(layout.clone());
        let control = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session,
                    endpoint: "https://owner-a.invalid".into(),
                },
            )
            .await
            .unwrap();
        let handle = runtime
            .bootstrap(
                proof,
                replica.clone(),
                authority,
                control,
                root.path().join("a.sqlite"),
                |tx| {
                    tx.execute_batch(SCHEMA)?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        Self {
            root,
            target,
            layout,
            replica,
            registry,
            runtime,
            handle,
        }
    }
    fn client(&self) -> CellClient {
        CellClient::local(self.registry.clone(), self.handle.clone())
    }
}
async fn rows(handle: &CellHandle) -> Vec<Vec<u8>> {
    let bytes = handle
        .query(0, 1024, |connection| {
            let mut statement =
                connection.prepare("SELECT fence FROM fence_executions ORDER BY rowid")?;
            let rows: Vec<Vec<u8>> = statement
                .query_map([], |row| row.get(0))?
                .collect::<std::result::Result<_, _>>()?;
            Ok(rows.concat())
        })
        .await
        .unwrap();
    bytes
        .as_chunks::<24>()
        .0
        .iter()
        .map(|row| row.to_vec())
        .collect()
}

#[tokio::test]
async fn typed_handlers_observe_the_admitted_fence_and_replay_does_not_reexecute() {
    let fixture = Fixture::new().await;
    let client = fixture.client();
    let fence = fixture.handle.owner_fence();
    let observed = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fence, observed.value().owner_fence());
    let identity = mutation_identity(21);
    let first = client
        .command::<FencedCommand>(&fixture.target, identity, Vec::new())
        .await
        .unwrap();
    assert_eq!(first.output, encoded(fence));
    let replay = client
        .command::<FencedCommand>(&fixture.target, identity, Vec::new())
        .await
        .unwrap();
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(rows(&fixture.handle).await, vec![encoded(fence)]);
    // An unrelated root publication keeps the epoch of the same admission.
    let second = client
        .command::<FencedCommand>(&fixture.target, mutation_identity(22), encoded(fence))
        .await
        .unwrap();
    assert_eq!(second.output, encoded(fence));
    let mut wrong_incarnation = fence;
    wrong_incarnation.incarnation = IncarnationId::from_bytes([99; 16]);
    let mismatch = client
        .command::<FencedCommand>(
            &fixture.target,
            mutation_identity(27),
            encoded(wrong_incarnation),
        )
        .await;
    assert!(
        matches!(mismatch,Err(InvocationError::Rejected(ref outcome)) if outcome.output==encoded(fence))
    );
    assert_eq!(
        rows(&fixture.handle).await,
        vec![encoded(fence), encoded(fence)]
    );
    let resident = fixture
        .runtime
        .resident_handle(&fixture.target, CatalogRole::Application)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resident.owner_fence(), fence);
    fixture.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn successor_owner_rejects_the_old_token_but_replays_its_recorded_outcome() {
    let fixture = Fixture::new().await;
    let old = fixture.handle.owner_fence();
    let identity = mutation_identity(23);
    let committed = fixture
        .client()
        .command::<FencedCommand>(&fixture.target, identity, Vec::new())
        .await
        .unwrap();
    fixture.handle.drain().await.unwrap();
    fixture.runtime.shutdown().await.unwrap();
    let session = SessionId::from_bytes([19; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4).unwrap(), 64 << 20, session).unwrap();
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let proof = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let successor = runtime
        .acquire_idle_restored(
            proof,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("b.sqlite"),
            Owner {
                session,
                endpoint: "https://owner-b.invalid".into(),
            },
        )
        .await
        .unwrap();
    let new = successor.owner_fence();
    assert_eq!(new.incarnation, old.incarnation);
    assert!(new.epoch > old.epoch);
    let client = CellClient::local(fixture.registry.clone(), successor.clone());
    let replay = client
        .command::<FencedCommand>(&fixture.target, identity, Vec::new())
        .await
        .unwrap();
    assert_eq!(replay.output, committed.output); // Original fence A, not B.
    assert_eq!(replay.receipt, committed.receipt);
    assert_eq!(rows(&successor).await, vec![encoded(old)]);
    let stale = client
        .command::<FencedCommand>(&fixture.target, mutation_identity(24), encoded(old))
        .await;
    assert!(
        matches!(stale,Err(InvocationError::Rejected(ref result)) if result.output==encoded(new))
    );
    assert_eq!(rows(&successor).await, vec![encoded(old)]);
    let result = client
        .command::<FencedCommand>(&fixture.target, mutation_identity(25), encoded(new))
        .await
        .unwrap();
    assert_eq!(result.output, encoded(new));
    assert_eq!(rows(&successor).await, vec![encoded(old), encoded(new)]);
    assert_eq!(fixture.handle.owner_fence(), old);
    assert!(
        fixture
            .client()
            .command::<FencedCommand>(&fixture.target, mutation_identity(26), Vec::new())
            .await
            .is_err()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_recovered_typed_outcome_keeps_its_fence_after_owner_loss() {
    use super::lifecycle::{PausingStore, TestNodeAuthority, fence_log_session};
    use cellule_runtime::identity::NodeId;
    use cellule_runtime::ltx::{DiskBudget, Host as ReplicaHost};
    use cellule_runtime::node::durability::NodeDurability;
    use cellule_runtime::node::lease::NodeLeaseGuard;
    use cellule_runtime::node::log::DurabilityGate;
    use cellule_runtime::node::log_recovery::{
        NodeLogRecovery, RecoveryCoordinator, recoverable_cells,
    };
    use cellule_runtime::node::log_shipper::NodeLogShipper;
    use cellule_runtime::node::log_transport::{LocalFollowerTransport, NodeLogTransport};
    use cellule_runtime::recovery::manifest::RecoveryManifestStore;
    use std::time::Duration;

    let session = SessionId::from_bytes([18; 16]);
    let follower_session = SessionId::from_bytes([30; 16]);
    let follower = NodeId::from_bytes(*follower_session.as_bytes());
    let successor_session = SessionId::from_bytes([31; 16]);
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 4).unwrap(),
        64 << 20,
        session,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let follower_directory = tempfile::TempDir::new().unwrap();
    let follower_store = cellule_runtime::FollowerStore::open(
        follower_directory.path().to_owned(),
        Limits::default(),
        DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(LocalFollowerTransport::new(
        follower,
        follower_store.clone(),
    ));
    let gate = DurabilityGate::new(
        session,
        NodeId::from_bytes(*session.as_bytes()),
        1,
        [follower],
    )
    .unwrap();
    let shipper = NodeLogShipper::new(gate.clone(), transport.clone(), Limits::default()).unwrap();
    runtime
        .install_node_durability(
            ApplicationId::from_bytes([16; 16]),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(TestNodeAuthority::default()),
                transport.clone(),
                lease.clone(),
            )),
        )
        .unwrap();
    let store = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixture = Fixture::with_runtime(session, Store::new(store.clone()), runtime).await;
    let old = fixture.handle.owner_fence();
    let authority = CellAuthority::new(fixture.layout.clone());
    let catalog = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant());
    let identity = mutation_identity(28);
    store.arm_next_update();
    let committed = tokio::time::timeout(
        Duration::from_secs(5),
        fixture
            .client()
            .command::<FencedCommand>(&fixture.target, identity, Vec::new()),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), store.wait_until_blocked())
        .await
        .unwrap();
    let pending = authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.value().root.as_ref().unwrap().commit_sequence, 0);
    assert_eq!(committed.receipt.commit_sequence, 1);
    assert_eq!(committed.output, encoded(old));
    assert!(follower_store.retained_bytes() > 0);

    // Fence the owner with its object CAS still paused. Recovery must obtain
    // the command and its original output from the actual fsynced follower log.
    lease.fence();
    let fenced = fence_log_session(
        &fixture.layout,
        session,
        successor_session,
        follower_session,
        0,
    )
    .await;
    let recovery = NodeLogRecovery::from_fenced(transport, &fenced, Limits::default()).unwrap();
    let manifests = RecoveryManifestStore::new(fixture.layout.clone(), Limits::default());
    let coordinator = RecoveryCoordinator::new(recovery, manifests.clone());
    let inventory = recoverable_cells(&catalog, &authority, session, 10)
        .await
        .unwrap();
    let directory = cellule_runtime::node::NodeDirectory::new(
        fixture.layout.clone(),
        Digest::from_bytes([90; 32]),
        Digest::from_bytes([91; 32]),
        Digest::from_bytes([92; 32]),
    );
    let completed = coordinator
        .recover_and_seal(&directory, fenced, inventory, 10_002)
        .await
        .unwrap();
    // The recovery attachment supersedes the paused CAS before it resumes.
    store.release();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), fixture.runtime.shutdown())
            .await
            .unwrap()
            .is_err()
    );
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 4).unwrap(),
        64 << 20,
        successor_session,
    )
    .unwrap();
    let successor = runtime
        .takeover_restored(
            catalog
                .lookup(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap(),
            fixture.replica.clone(),
            authority,
            completed.controls.into_iter().next().unwrap(),
            completed.takeover,
            manifests,
            fixture.root.path().join("follower-successor.sqlite"),
            Owner {
                session: successor_session,
                endpoint: "https://follower-successor.invalid".into(),
            },
        )
        .await
        .unwrap();
    let new = successor.owner_fence();
    assert_eq!(new.incarnation, old.incarnation);
    assert!(new.epoch > old.epoch);
    let client = CellClient::local(fixture.registry.clone(), successor.clone());
    let replay = client
        .command::<FencedCommand>(&fixture.target, identity, Vec::new())
        .await
        .unwrap();
    assert_eq!(replay.output, committed.output);
    assert_eq!(replay.receipt, committed.receipt);
    assert_eq!(rows(&successor).await, vec![encoded(old)]);
    let stale = client
        .command::<FencedCommand>(&fixture.target, mutation_identity(29), encoded(old))
        .await;
    assert!(
        matches!(stale, Err(InvocationError::Rejected(ref outcome)) if outcome.output == encoded(new))
    );
    assert_eq!(rows(&successor).await, vec![encoded(old)]);
    let current = client
        .command::<FencedCommand>(&fixture.target, mutation_identity(32), encoded(new))
        .await
        .unwrap();
    assert_eq!(current.output, encoded(new));
    assert_eq!(rows(&successor).await, vec![encoded(old), encoded(new)]);
    runtime.shutdown().await.unwrap();
}
