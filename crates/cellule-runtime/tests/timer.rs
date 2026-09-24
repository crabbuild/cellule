use std::{sync::Arc, time::UNIX_EPOCH};

mod support;

use cellule_ltx::CellStorageLayout;
use cellule_ltx::{CellReplica, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellModule, CellRuntime, CellTarget, Command, CommandContext, CommandResult,
    Digest, IncarnationId, MaintenanceModule, MaintenanceTickOutcome, MaintenanceTickRequest,
    MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor, NamespaceId,
    OperationDescriptor, Owner, RegistryBuilder, RequestId, SessionId, SqlWorkerPool, TenantId,
    TimerInvocation, TimerModule, TimerMutation, TimerMutationOutcome, TimerNamespace,
    TimerQueryResult, TimerTarget, register_timer,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const TIMER_MODULE: &str = "timer-test";
const TIMER_NAMESPACE: NamespaceId = NamespaceId::from_bytes([41; 16]);
const TIMER_MIGRATION: &str = include_str!("../src/migrations/timer.sql");
const TARGET_MODULE: &str = "timer-target-test";
const TARGET_NAMESPACE: NamespaceId = NamespaceId::from_bytes([42; 16]);
const TARGET_MIGRATION: &str = "CREATE TABLE timer_target(value BLOB) STRICT;";
const TARGET_INPUT_LIMIT: u32 = 300 * 1024;
const TIMER_TARGETS: &[TimerTarget] = &[TimerTarget::new(
    TARGET_MODULE,
    TARGET_NAMESPACE,
    9,
    1,
    TARGET_INPUT_LIMIT,
)];

const TIMER_COMMANDS: &[OperationDescriptor] = &[operation(1, 300 * 1024, 16), operation(2, 8, 5)];
const TIMER_QUERIES: &[OperationDescriptor] = &[operation(1, 64, 600 * 1024)];
const TARGET_COMMANDS: &[OperationDescriptor] = &[operation(9, TARGET_INPUT_LIMIT, 1)];

struct TestTimer;

impl MaintenanceModule for TestTimer {
    const MODULE: &'static str = TIMER_MODULE;
    const TICK_COMMAND_ID: u32 = 2;
    const TIMER_TARGETS: &'static [TimerTarget] = TIMER_TARGETS;
}

impl TimerModule for TestTimer {
    const NAMESPACE: NamespaceId = TIMER_NAMESPACE;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 1;
}

impl CellModule for TestTimer {
    const NAME: &'static str = TIMER_MODULE;

    fn descriptor(&self) -> &'static ModuleDescriptor {
        descriptor(
            TIMER_MODULE,
            TIMER_NAMESPACE,
            CatalogRole::Timer,
            TIMER_MIGRATION,
            TIMER_COMMANDS,
            TIMER_QUERIES,
            &[TARGET_NAMESPACE],
            41,
        )
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_timer::<Self>(registry)
    }
}

struct TimerTargetModule;

struct ReceiveDeadline;

impl Command for ReceiveDeadline {
    const MODULE: &'static str = TARGET_MODULE;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 1;
    type Input = TimerInvocation;
    type Output = ();

    fn execute(
        _: &mut CommandContext<'_, '_>,
        _: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        Ok(CommandResult::Success(()))
    }
}

impl CellModule for TimerTargetModule {
    const NAME: &'static str = TARGET_MODULE;

    fn descriptor(&self) -> &'static ModuleDescriptor {
        descriptor(
            TARGET_MODULE,
            TARGET_NAMESPACE,
            CatalogRole::Application,
            TARGET_MIGRATION,
            TARGET_COMMANDS,
            &[],
            &[],
            42,
        )
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<ReceiveDeadline>()
    }
}

#[test]
fn timer_bindings_match_release_descriptors() {
    let mut registry = RegistryBuilder::new(BuildDescriptor {
        source_revision: "timer-test".into(),
        cargo_lock_digest: Digest::from_bytes([43; 32]),
    });
    registry.register(TimerTargetModule).unwrap();
    registry.register(TestTimer).unwrap();
    registry.finish().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_deadline_survives_owner_loss_and_fires_once() {
    let mut registry = RegistryBuilder::new(BuildDescriptor {
        source_revision: "timer-test".into(),
        cargo_lock_digest: Digest::from_bytes([43; 32]),
    });
    registry.register(TimerTargetModule).unwrap();
    registry.register(TestTimer).unwrap();
    let registry = Arc::new(registry.finish().unwrap());

    let tenant = TenantId::from_bytes([44; 16]);
    let application = ApplicationId::from_bytes([45; 16]);
    let store = Store::new(Arc::new(InMemory::new()));
    let layout =
        CellStorageLayout::new(store, Path::from("timer-runtime"), *application.as_bytes());
    let directory = tempfile::tempdir().unwrap();
    let target =
        CellTarget::new(tenant, application, TIMER_NAMESPACE, &0_u32.to_be_bytes()).unwrap();
    let incarnation = IncarnationId::from_bytes([46; 16]);
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Timer,
                registry.module_code(TIMER_MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout.clone());
    let session = SessionId::from_bytes([47; 16]);
    let control = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://timer.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 10).unwrap(),
        16 * 1024 * 1024,
        session,
    )
    .unwrap();
    let handle = runtime
        .bootstrap(
            proof,
            CellReplica::new(
                layout.clone(),
                *target.cell_id().as_bytes(),
                *incarnation.as_bytes(),
                Limits::default(),
            )
            .unwrap(),
            authority.clone(),
            control,
            directory.path().join("timer.sqlite"),
            cellule_runtime::install_timer_schema,
        )
        .await
        .unwrap();
    let timers = TimerNamespace::<TestTimer>::new(
        CellClient::local(registry.clone(), handle.clone()),
        tenant,
        application,
    )
    .unwrap();

    let timer_id = [48; 16];
    let start_ms = now_ms();
    let set = timers
        .mutate(
            identity(49, start_ms),
            TimerMutation::Set {
                timer_id,
                target_index: 0,
                target_partition: b"destination".to_vec(),
                payload: b"expire".to_vec(),
                due_at_ms: start_ms + 60_000,
            },
        )
        .await
        .unwrap();
    assert_eq!(set.output, TimerMutationOutcome::Applied { generation: 1 });

    drop(timers);
    drop(handle);
    drop(runtime);

    let stale = authority.load(target.cell_id()).await.unwrap().unwrap();
    let successor = SessionId::from_bytes([50; 16]);
    let takeover = support::fence_session(&layout, session, successor).await;
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 10).unwrap(),
        16 * 1024 * 1024,
        successor,
    )
    .unwrap();
    let restored = runtime
        .takeover_restored(
            catalog.lookup(target.cell_id()).await.unwrap().unwrap(),
            CellReplica::new(
                layout.clone(),
                *target.cell_id().as_bytes(),
                *incarnation.as_bytes(),
                Limits::default(),
            )
            .unwrap(),
            authority.clone(),
            stale,
            takeover.direct_takeover().unwrap(),
            cellule_runtime::RecoveryManifestStore::new(layout.clone(), Limits::default()),
            directory.path().join("timer-takeover.sqlite"),
            Owner {
                session: successor,
                endpoint: "https://timer-successor.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let client = CellClient::local(registry.clone(), restored.clone());
    let timers = TimerNamespace::<TestTimer>::new(client.clone(), tenant, application).unwrap();

    let TimerQueryResult::Get(Some(entry)) = timers
        .get(timer_id, Some(set.receipt))
        .await
        .unwrap()
        .output
    else {
        panic!("timer deadline did not survive owner loss");
    };
    assert_eq!(entry.generation, 1);
    assert_eq!(entry.payload, b"expire");

    let due_ms = now_ms();
    let due = timers
        .mutate(
            identity(51, due_ms),
            TimerMutation::Set {
                timer_id,
                target_index: 0,
                target_partition: b"destination".to_vec(),
                payload: b"expire".to_vec(),
                due_at_ms: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(due.output, TimerMutationOutcome::Applied { generation: 2 });

    let tick = registry
        .run_maintenance_once(
            client.clone(),
            target.clone(),
            identity(52, due_ms),
            MaintenanceTickRequest {
                expected_commit_sequence: due.receipt.commit_sequence,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        tick.output,
        MaintenanceTickOutcome::Applied { processed: 1 }
    );
    assert!(matches!(
        timers
            .get(timer_id, Some(tick.receipt))
            .await
            .unwrap()
            .output,
        TimerQueryResult::Get(None)
    ));
    let second = registry
        .run_maintenance_once(
            client,
            target,
            identity(53, due_ms),
            MaintenanceTickRequest {
                expected_commit_sequence: tick.receipt.commit_sequence,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        second.output,
        MaintenanceTickOutcome::Applied { processed: 0 }
    );

    drop(timers);
    restored.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn identity(byte: u8, now_ms: i64) -> MutationIdentity {
    MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
}

fn descriptor(
    name: &'static str,
    namespace: NamespaceId,
    role: CatalogRole,
    migration: &'static str,
    commands: &'static [OperationDescriptor],
    queries: &'static [OperationDescriptor],
    effect_targets: &'static [NamespaceId],
    digest: u8,
) -> &'static ModuleDescriptor {
    Box::leak(Box::new(ModuleDescriptor {
        name,
        source_digest: Digest::from_bytes([digest; 32]),
        retained_codes: &[],
        schema_min: 1,
        schema_max: 1,
        migrations: Box::leak(Box::new([MigrationDescriptor {
            version: 1,
            sql: migration,
            digest: Digest::from_bytes(*blake3::hash(migration.as_bytes()).as_bytes()),
        }])),
        commands,
        queries,
        workflow_definitions: &[],
        activity_types: &[],
        namespaces: Box::leak(Box::new([NamespaceDescriptor {
            id: namespace,
            name,
            role,
            shards: 1,
            effect_targets,
            dead_letter: None,
        }])),
    }))
}

const fn operation(id: u32, input_limit: u32, output_limit: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit,
        output_limit,
    }
}
