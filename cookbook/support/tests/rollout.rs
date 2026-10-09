//! Public node activation, immutable catalog, and native definition retention.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogRole, CellModule, CellTarget, Digest, Error,
    MigrationDescriptor, ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder,
    TenantId,
    cell::catalog::CellCatalog,
    control::{ControlState, authority::CellAuthority},
    partition_for_shard,
    primitives::{
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{
            WorkflowAction, WorkflowContext, WorkflowDecision, WorkflowDefinition, WorkflowModule,
            WorkflowSignal, WorkflowStatus, register_workflow,
        },
    },
    registry::{OperationDescriptor, RetainedCodeDescriptor},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const APPLICATION: ApplicationId = ApplicationId::from_bytes([0xa1; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xa2; 16]);
const NAMESPACE: NamespaceId = NamespaceId::from_bytes([0xa3; 16]);
const SCHEMA: &str = "CREATE TABLE release_fixture (id INTEGER PRIMARY KEY)";
const MODULE: &str = "support.rollout-fixture";

struct Definition(u8);
static LEGACY: Definition = Definition(1);
static CURRENT: Definition = Definition(2);
static LEGACY_DEFINITIONS: &[&dyn WorkflowDefinition] = &[&LEGACY];
static RETAINED_DEFINITIONS: &[&dyn WorkflowDefinition] = &[&LEGACY, &CURRENT];

impl WorkflowDefinition for Definition {
    fn digest(&self) -> Digest {
        Digest::from_bytes([self.0; 32])
    }
    fn transition(
        &self,
        state: &[u8],
        event: &[u8],
        context: WorkflowContext,
    ) -> cellule_runtime::Result<WorkflowDecision> {
        if state.is_empty() && event == b"start" {
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: vec![self.0, 0],
                result: None,
                actions: Vec::new(),
            });
        }
        if state == [self.0, 0] && event == b"approve" {
            let mut next = vec![self.0, 1];
            next.extend_from_slice(&context.action_id(0));
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Running,
                state: next,
                result: None,
                actions: vec![WorkflowAction::Timer {
                    due_at_ms: context.now_ms() + 250,
                }],
            });
        }
        if state.len() == 18
            && state[..2] == [self.0, 1]
            && event.strip_prefix(b"timer\0") == Some(&state[2..])
        {
            return Ok(WorkflowDecision {
                status: WorkflowStatus::Completed,
                state: vec![self.0, 2],
                result: Some(if self.0 == 1 {
                    b"legacy-verification".to_vec()
                } else {
                    b"new-verification".to_vec()
                }),
                actions: Vec::new(),
            });
        }
        Err(Error::Command("invalid fixture transition"))
    }
}

struct Flows<const VERSION: u8>;
impl<const VERSION: u8> WorkflowModule for Flows<VERSION> {
    const MODULE: &'static str = MODULE;
    const NAMESPACE: NamespaceId = NAMESPACE;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition =
        if VERSION == 1 { &LEGACY } else { &CURRENT };
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = if VERSION == 1 {
        LEGACY_DEFINITIONS
    } else {
        RETAINED_DEFINITIONS
    };
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl<const VERSION: u8> MaintenanceModule for Flows<VERSION> {
    const MODULE: &'static str = MODULE;
    const TICK_COMMAND_ID: u32 = 6;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] =
        <Self as WorkflowModule>::DEFINITIONS;
}
const fn op(id: u32, input_limit: u32, output_limit: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit,
        output_limit,
    }
}
impl<const VERSION: u8> CellModule for Flows<VERSION> {
    const NAME: &'static str = MODULE;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 128, 64),
            op(2, 256, 64),
            op(3, 256, 64),
            op(4, 256, 64),
            op(6, 8, 8),
        ];
        const QUERIES: &[OperationDescriptor] = &[op(5, 64, 1024)];
        const OLD_DIGESTS: &[Digest] = &[Digest::from_bytes([1; 32])];
        const NEW_DIGESTS: &[Digest] = &[Digest::from_bytes([1; 32]), Digest::from_bytes([2; 32])];
        static VALUES: [OnceLock<ModuleDescriptor>; 3] = [const { OnceLock::new() }; 3];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static RETAINED: OnceLock<[RetainedCodeDescriptor; 1]> = OnceLock::new();
        VALUES[usize::from(VERSION - 1)].get_or_init(|| ModuleDescriptor {
            name: MODULE,
            source_digest: Digest::from_bytes([VERSION + 10; 32]),
            retained_codes: if VERSION == 2 {
                RETAINED.get_or_init(|| {
                    [RetainedCodeDescriptor {
                        code: compile::<1>().registry().module_code(MODULE).unwrap(),
                        schema_min: 1,
                        schema_max: 1,
                    }]
                })
            } else {
                &[]
            },
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
                }]
            }),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: if VERSION == 1 {
                OLD_DIGESTS
            } else {
                NEW_DIGESTS
            },
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: NAMESPACE,
                name: "rollout-fixture",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_maintenance::<Self>(registry)
    }
}
struct Application<const VERSION: u8>;
impl<const VERSION: u8> CellApplication for Application<VERSION> {
    const NAME: &'static str = "cookbook-rollout-fixture";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Flows::<VERSION>)?;
        builder.cell_type(
            CellType::new(MODULE, "flows", NAMESPACE, CatalogRole::Workflow, 1)?
                .with_limits(16 << 20, 4 << 20)?,
        )
    }
}
fn compile<const VERSION: u8>() -> Arc<CompiledApplication> {
    Arc::new(
        Application::<VERSION>::compile(BuildDescriptor {
            source_revision: format!("rollout-fixture-{VERSION}"),
            cargo_lock_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("../../Cargo.lock")).as_bytes(),
            ),
        })
        .unwrap(),
    )
}
fn target() -> CellTarget {
    CellTarget::new(TENANT, APPLICATION, NAMESPACE, &partition_for_shard(0)).unwrap()
}
fn layout(store: Store) -> cellule_ltx::CellStorageLayout {
    cellule_ltx::CellStorageLayout::new(
        store,
        Path::from("rollout-fixture"),
        *APPLICATION.as_bytes(),
    )
}
async fn start<const VERSION: u8>(store: Store, root: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile::<VERSION>(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("rollout-fixture"),
            application_id: APPLICATION,
        },
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_preserves_old_run_definition_and_installs_new_runs() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start::<1>(store.clone(), root.path()).await;
    node.open_cell(&target(), &Flows::<1>).await.unwrap();
    let client = node.application_handle::<Application<1>>(TENANT).unwrap();
    let workflows = client.workflow::<Flows<1>>().unwrap();
    let started = workflows
        .start(
            new_identity().unwrap(),
            b"old-release".to_vec(),
            b"start".to_vec(),
        )
        .await
        .unwrap();
    let old_run = workflows
        .state(b"old-release".to_vec(), Some(started.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    node.shutdown().await.unwrap();

    let catalog = CellCatalog::new(layout(store.clone()), TENANT);
    let original_catalog = catalog
        .lookup(target().cell_id())
        .await
        .unwrap()
        .unwrap()
        .entry()
        .clone();
    let authority = CellAuthority::new(layout(store.clone()));
    let previous = authority.load(target().cell_id()).await.unwrap().unwrap();
    let node = start::<2>(store.clone(), root.path()).await;
    let handle = node
        .open_cell_after_rollout(&target(), &Flows::<2>, &compile::<1>())
        .await
        .unwrap();
    assert_eq!(
        handle.code(),
        compile::<2>().registry().module_code(MODULE).unwrap()
    );
    let published = authority.load(target().cell_id()).await.unwrap().unwrap();
    assert_eq!(
        published.value().root.as_ref().unwrap().commit_sequence,
        previous.value().root.as_ref().unwrap().commit_sequence + 1
    );
    assert_eq!(
        catalog
            .lookup(target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .entry(),
        &original_catalog
    );
    // An acknowledged upgrade is repeatable and ordinary reopen does not replay it.
    assert_eq!(
        node.open_cell_after_rollout(&target(), &Flows::<2>, &compile::<1>())
            .await
            .unwrap()
            .code(),
        handle.code()
    );
    node.open_cell(&target(), &Flows::<2>).await.unwrap();
    assert_eq!(
        authority
            .load(target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        published.value()
    );
    let client = node.application_handle::<Application<2>>(TENANT).unwrap();
    let workflows = client.workflow::<Flows<2>>().unwrap();
    assert_eq!(
        workflows
            .state(b"old-release".to_vec(), Some(started.receipt))
            .await
            .unwrap()
            .output
            .unwrap(),
        old_run
    );
    workflows
        .start(
            new_identity().unwrap(),
            b"new-release".to_vec(),
            b"start".to_vec(),
        )
        .await
        .unwrap();
    let new_run = workflows
        .state(b"new-release".to_vec(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(new_run.definition_digest, CURRENT.digest());
    assert_eq!(old_run.definition_digest, LEGACY.digest());
    for run in [&old_run, &new_run] {
        workflows
            .signal(
                new_identity().unwrap(),
                WorkflowSignal {
                    workflow_id: run.workflow_id.clone(),
                    run_id: run.run_id,
                    signal_id: *uuid::Uuid::now_v7().as_bytes(),
                    event: b"approve".to_vec(),
                },
            )
            .await
            .unwrap();
    }
    // Native maintenance must retain both definitions as well as signal dispatch.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let old = workflows
                .state(old_run.workflow_id.clone(), None)
                .await
                .unwrap()
                .output
                .unwrap();
            let new = workflows
                .state(new_run.workflow_id.clone(), None)
                .await
                .unwrap()
                .output
                .unwrap();
            if old.status == WorkflowStatus::Completed && new.status == WorkflowStatus::Completed {
                assert_eq!(
                    old.result.as_deref(),
                    Some(b"legacy-verification".as_slice())
                );
                assert_eq!(new.result.as_deref(), Some(b"new-verification".as_slice()));
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    node.shutdown().await.unwrap();
    // Cold restoration uses the original catalog proof with migrated authority.
    tokio::fs::remove_dir_all(root.path()).await.unwrap();
    let node = start::<2>(store, root.path()).await;
    let restored = node.open_cell(&target(), &Flows::<2>).await.unwrap();
    assert_eq!(restored.code(), handle.code());
    let client = node.application_handle::<Application<2>>(TENANT).unwrap();
    let old = client
        .workflow::<Flows<2>>()
        .unwrap()
        .state(old_run.workflow_id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(
        old.result.as_deref(),
        Some(b"legacy-verification".as_slice())
    );
    assert_eq!(old.definition_digest, LEGACY.digest());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn normal_open_and_unrelated_predecessor_refuse_before_ownership_changes() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start::<1>(store.clone(), root.path()).await;
    node.open_cell(&target(), &Flows::<1>).await.unwrap();
    node.shutdown().await.unwrap();
    let authority = CellAuthority::new(layout(store.clone()));
    let before = authority.load(target().cell_id()).await.unwrap().unwrap();
    assert_eq!(before.value().state, ControlState::Idle);
    let node = start::<2>(store, root.path()).await;
    assert!(node.open_cell(&target(), &Flows::<2>).await.is_err());
    assert!(
        node.open_cell_after_rollout(&target(), &Flows::<2>, &compile::<3>())
            .await
            .is_err()
    );
    // Module compatibility alone does not pin application partitioning or limits.
    let mut builder = ApplicationBuilder::new(
        Application::<1>::NAME,
        BuildDescriptor {
            source_revision: "different-topology".into(),
            cargo_lock_digest: Digest::from_bytes([0x99; 32]),
        },
    )
    .unwrap();
    builder.register(Flows::<1>).unwrap();
    builder
        .cell_type(
            CellType::new(MODULE, "flows", NAMESPACE, CatalogRole::Workflow, 1)
                .unwrap()
                .with_limits(32 << 20, 4 << 20)
                .unwrap(),
        )
        .unwrap();
    let incompatible = builder.finish().unwrap();
    assert!(
        node.open_cell_after_rollout(&target(), &Flows::<2>, &incompatible)
            .await
            .is_err()
    );
    assert_eq!(
        authority
            .load(target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        before.value()
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_predecessor_must_drain_before_upgrade() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let old = start::<1>(store.clone(), root.path()).await;
    old.open_cell(&target(), &Flows::<1>).await.unwrap();
    let authority = CellAuthority::new(layout(store.clone()));
    let before = authority.load(target().cell_id()).await.unwrap().unwrap();
    let new = start::<2>(store, root.path()).await;
    assert!(
        new.open_cell_after_rollout(&target(), &Flows::<2>, &compile::<1>())
            .await
            .is_err()
    );
    assert_eq!(
        authority
            .load(target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        before.value()
    );
    old.shutdown().await.unwrap();
    new.open_cell_after_rollout(&target(), &Flows::<2>, &compile::<1>())
        .await
        .unwrap();
    new.shutdown().await.unwrap();
}

#[tokio::test]
async fn predecessor_binary_refuses_migrated_authority_without_rewriting_catalog() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let old = start::<1>(store.clone(), root.path()).await;
    old.open_cell(&target(), &Flows::<1>).await.unwrap();
    old.shutdown().await.unwrap();
    let new = start::<2>(store.clone(), root.path()).await;
    new.open_cell_after_rollout(&target(), &Flows::<2>, &compile::<1>())
        .await
        .unwrap();
    new.shutdown().await.unwrap();
    let authority = CellAuthority::new(layout(store.clone()));
    let before = authority.load(target().cell_id()).await.unwrap().unwrap();
    let catalog = CellCatalog::new(layout(store.clone()), TENANT);
    let original = catalog
        .lookup(target().cell_id())
        .await
        .unwrap()
        .unwrap()
        .entry()
        .clone();
    assert_eq!(
        original.initial_code(),
        compile::<1>().registry().module_code(MODULE).unwrap()
    );
    assert_eq!(
        before.value().code,
        compile::<2>().registry().module_code(MODULE).unwrap()
    );
    let old = start::<1>(store, root.path()).await;
    assert!(old.open_cell(&target(), &Flows::<1>).await.is_err());
    assert_eq!(
        authority
            .load(target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        before.value()
    );
    assert_eq!(
        catalog
            .lookup(target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .entry(),
        &original
    );
    old.shutdown().await.unwrap();
}
