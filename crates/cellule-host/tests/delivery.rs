use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_host::{CellDelivery, CellDeliveryConfig, CellNodeBuilder};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, BlockingActivityPool, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority,
    CellCatalog, CellClient, CellHandle, CellModule, CellRuntime, CellStorageLayout, CellTarget,
    Command, CommandContext, CommandResult, Digest, EffectModule, EffectPeerClient, Error,
    IncarnationId, MaintenanceModule, MigrationDescriptor, ModuleDescriptor, MutationIdentity,
    NamespaceDescriptor, NamespaceId, NodeLeaseGuard, OperationDescriptor, Owner, PeerAuthorizer,
    PeerCellResolver, PeerDispatcher, PeerPrincipal, PeerRoundTrip, PeerSigner, PeerVerifier,
    Registry, RegistryBuilder, RequestId, Result, SessionId, SqlBatch, SqlModule, SqlStatement,
    SqlValue, SqlWorkerPool, TIMER_SCHEMA_SQL, TenantId, TimerInvocation, TimerModule,
    TimerMutation, TimerMutationOutcome, TimerQueryResult, TimerTarget, VerifiedPeerRequest,
    install_timer_schema, partition_for_shard, register_effect_delivery, register_sql,
    register_timer,
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};
use tokio_util::sync::CancellationToken;

const RESERVATIONS: NamespaceId = NamespaceId::from_bytes([81; 16]);
const TIMEOUTS: NamespaceId = NamespaceId::from_bytes([82; 16]);
const RESERVATION_ID: i64 = 42;
const SECOND_RESERVATION_ID: i64 = 43;
const RESERVATION_SCHEMA: &str =
    "CREATE TABLE reservations (id INTEGER PRIMARY KEY, state INTEGER NOT NULL)";
const RESERVATION_COMMANDS: [OperationDescriptor; 2] = [operation(1), operation(3)];
const RESERVATION_QUERIES: [OperationDescriptor; 1] = [operation(2)];
const TIMEOUT_COMMANDS: [OperationDescriptor; 4] =
    [operation(1), operation(3), operation(4), operation(5)];
const TIMEOUT_QUERIES: [OperationDescriptor; 3] = [operation(2), operation(6), operation(7)];

const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: 1 << 20,
        output_limit: 1 << 20,
    }
}

struct Reservations;

impl SqlModule for Reservations {
    const MODULE: &'static str = Self::NAME;
    const BATCH_COMMAND_ID: u32 = 1;
    const BATCH_QUERY_ID: u32 = 2;
}

struct ReleaseReservation;

impl Command for ReleaseReservation {
    const MODULE: &'static str = Reservations::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = TimerInvocation;
    type Output = ();

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        let id = i64::from_be_bytes(
            input
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| Error::Command("timeout payload is not a reservation id"))?,
        );
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "UPDATE reservations SET state = 1 WHERE id = ?1".into(),
                parameters: vec![SqlValue::Integer(id)],
            }],
        })?;
        Ok(CommandResult::Success(()))
    }
}

impl CellModule for Reservations {
    const NAME: &'static str = "reservations";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes([84; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: RESERVATION_SCHEMA,
                    digest: Digest::from_bytes(
                        *blake3::hash(RESERVATION_SCHEMA.as_bytes()).as_bytes(),
                    ),
                }]
            }),
            commands: &RESERVATION_COMMANDS,
            queries: &RESERVATION_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: RESERVATIONS,
                name: Self::NAME,
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        register_sql::<Self>(registry)?;
        registry.bind_command::<ReleaseReservation>()
    }
}

struct Timeouts;

impl MaintenanceModule for Timeouts {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
    const TIMER_TARGETS: &'static [TimerTarget] = &[TimerTarget::new(
        Reservations::NAME,
        RESERVATIONS,
        3,
        1,
        1 << 20,
    )];
}

impl TimerModule for Timeouts {
    const NAMESPACE: NamespaceId = TIMEOUTS;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}

impl EffectModule for Timeouts {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}

impl CellModule for Timeouts {
    const NAME: &'static str = "timeouts";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes([85; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: TIMER_SCHEMA_SQL,
                    digest: Digest::from_bytes(
                        *blake3::hash(TIMER_SCHEMA_SQL.as_bytes()).as_bytes(),
                    ),
                }]
            }),
            commands: &TIMEOUT_COMMANDS,
            queries: &TIMEOUT_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: TIMEOUTS,
                name: Self::NAME,
                role: CatalogRole::Timer,
                shards: 2,
                effect_targets: &[RESERVATIONS],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        register_timer::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}

struct TimeoutApp;

impl CellApplication for TimeoutApp {
    const NAME: &'static str = "host-delivery-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> Result<()> {
        builder.register(Reservations)?;
        builder.register(Timeouts)?;
        builder.cell_type(CellType::new(
            Reservations::NAME,
            "reservations",
            RESERVATIONS,
            CatalogRole::Sql,
            1,
        )?)?;
        builder.cell_type(CellType::new(
            Timeouts::NAME,
            "timeouts",
            TIMEOUTS,
            CatalogRole::Timer,
            2,
        )?)?;
        Ok(())
    }
}

type Schema = for<'a> fn(&cellule_ltx::rusqlite::Transaction<'a>) -> Result<()>;

struct CellSpec {
    target: CellTarget,
    role: CatalogRole,
    module: &'static str,
    incarnation: IncarnationId,
    path: PathBuf,
    install: Schema,
}

async fn bootstrap_cell(
    runtime: &CellRuntime,
    registry: &Registry,
    layout: &CellStorageLayout,
    session: SessionId,
    spec: CellSpec,
) -> Result<CellHandle> {
    let code = registry
        .module_code(spec.module)
        .ok_or(Error::Registry("host delivery module is missing"))?;
    let proof = CellCatalog::new(layout.clone(), spec.target.tenant())
        .provision(CatalogEntry::new(&spec.target, spec.role, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let observed = authority
        .create_initial(
            &proof,
            spec.incarnation,
            Owner {
                session,
                endpoint: "https://host-delivery.local".into(),
            },
        )
        .await?;
    runtime
        .bootstrap(
            proof,
            CellReplica::new(
                layout.clone(),
                *spec.target.cell_id().as_bytes(),
                *spec.incarnation.as_bytes(),
                Limits::default(),
            )?,
            authority,
            observed,
            spec.path,
            spec.install,
        )
        .await
}

struct ReservationResolver {
    target: CellTarget,
    handle: CellHandle,
}

impl PeerCellResolver for ReservationResolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellHandle>> + Send + 'static>> {
        let allowed = target == self.target;
        let handle = self.handle.clone();
        Box::pin(async move {
            if allowed {
                Ok(handle)
            } else {
                Err(Error::CellNotActive)
            }
        })
    }
}

struct ReservationAuthorizer;

impl PeerAuthorizer for ReservationAuthorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> Result<()> {
        if request.permits("timeout.deliver") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization(
                "timeout delivery is not authorized",
            ))
        }
    }
}

struct Loopback {
    verifier: Arc<PeerVerifier>,
    dispatcher: Arc<PeerDispatcher>,
}

impl PeerRoundTrip for Loopback {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'static>> {
        let verifier = Arc::clone(&self.verifier);
        let dispatcher = Arc::clone(&self.dispatcher);
        Box::pin(async move {
            tokio::time::timeout(Duration::from_millis(u64::from(remaining_ms)), async {
                let verified = verifier.verify(&request, now_ms()?)?;
                if verified.target() != &target {
                    return Err(Error::Peer("timeout target changed in transit"));
                }
                dispatcher.dispatch_bytes(&verified, now_ms()?).await
            })
            .await
            .map_err(|source| Error::PeerTransportUnknown {
                context: "timeout peer deadline",
                source: Box::new(source),
            })?
        })
    }
}

fn timer_id_for_shard(shard: u32) -> [u8; 16] {
    for candidate in 0_u8..=u8::MAX {
        let id = [candidate; 16];
        if cellule_runtime::shard_for_scope(TIMEOUTS, &id, 2).unwrap() == shard {
            return id;
        }
    }
    panic!("no timer ID maps to shard {shard}");
}

fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Command("system time is before the Unix epoch"))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::Command("system time exceeds the supported range"))
}

fn identity(request_id: u8) -> MutationIdentity {
    let now_ms = now_ms().unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes([request_id; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_delivery_fires_a_deadline_and_releases_the_destination() {
    let application = Arc::new(
        TimeoutApp::compile(BuildDescriptor {
            source_revision: "host-delivery-test".into(),
            cargo_lock_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
            ),
        })
        .unwrap(),
    );
    let registry = application.registry();
    let tenant = TenantId::from_bytes([86; 16]);
    let application_id = ApplicationId::from_bytes([87; 16]);
    let reservation_target = CellTarget::new(
        tenant,
        application_id,
        RESERVATIONS,
        &partition_for_shard(0),
    )
    .unwrap();
    let timeout_target =
        CellTarget::new(tenant, application_id, TIMEOUTS, &partition_for_shard(0)).unwrap();
    let second_timeout_target =
        CellTarget::new(tenant, application_id, TIMEOUTS, &partition_for_shard(1)).unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("host-delivery"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new().unwrap();
    let session = SessionId::from_bytes([88; 16]);
    let node = CellNodeBuilder::new(Arc::clone(&application))
        .with_runtime(SqlWorkerPool::new(2, 8).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(session)
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_node_lease(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    let runtime = node.runtime();
    let reservation_handle = bootstrap_cell(
        &runtime,
        &registry,
        &layout,
        session,
        CellSpec {
            target: reservation_target.clone(),
            role: CatalogRole::Sql,
            module: Reservations::NAME,
            incarnation: IncarnationId::from_bytes([89; 16]),
            path: files.path().join("reservations.sqlite"),
            install: |transaction| {
                transaction.execute_batch(RESERVATION_SCHEMA)?;
                Ok(())
            },
        },
    )
    .await
    .unwrap();
    let timeout_handle = bootstrap_cell(
        &runtime,
        &registry,
        &layout,
        session,
        CellSpec {
            target: timeout_target.clone(),
            role: CatalogRole::Timer,
            module: Timeouts::NAME,
            incarnation: IncarnationId::from_bytes([90; 16]),
            path: files.path().join("timeouts.sqlite"),
            install: install_timer_schema,
        },
    )
    .await
    .unwrap();
    let second_timeout_handle = bootstrap_cell(
        &runtime,
        &registry,
        &layout,
        session,
        CellSpec {
            target: second_timeout_target.clone(),
            role: CatalogRole::Timer,
            module: Timeouts::NAME,
            incarnation: IncarnationId::from_bytes([94; 16]),
            path: files.path().join("timeouts-second.sqlite"),
            install: install_timer_schema,
        },
    )
    .await
    .unwrap();
    let client = CellClient::local_many(
        Arc::clone(&registry),
        vec![
            reservation_handle.clone(),
            timeout_handle,
            second_timeout_handle,
        ],
    )
    .unwrap();

    let peer_session = SessionId::from_bytes([91; 16]);
    let signer = Arc::new(PeerSigner::new(
        peer_session,
        registry.release_digest(),
        SigningKey::from_bytes(&[92; 32]),
    ));
    let verifier = Arc::new(PeerVerifier::new(
        peer_session,
        registry.release_digest(),
        signer.verifying_key(),
    ));
    let dispatcher = Arc::new(PeerDispatcher::new(
        Arc::clone(&registry),
        Arc::new(ReservationResolver {
            target: reservation_target.clone(),
            handle: reservation_handle,
        }),
        Arc::new(ReservationAuthorizer),
    ));
    let peer = EffectPeerClient::new(
        signer,
        PeerPrincipal {
            issuer: "host-delivery-test".into(),
            subject: "timeout-runner".into(),
            actions: vec!["timeout.deliver".into()],
        },
        Arc::new(Loopback {
            verifier,
            dispatcher,
        }),
    );
    // One shard per pass: the loop must rotate through the catalog rather than
    // starve the Cells whose shards fall outside the first window.
    let config = CellDeliveryConfig::new(tenant, application_id)
        .with_poll_interval(Duration::from_millis(50))
        .with_max_concurrent_cells(1)
        .with_max_shards_per_pass(1)
        .with_lease_ms(5_000);
    let delivery = CellDelivery::new(
        config,
        CellCatalog::new(layout.clone(), tenant),
        CellAuthority::new(layout.clone()),
        client.clone(),
        node.runtime(),
        Arc::clone(&registry),
        peer,
        Arc::new(BlockingActivityPool::for_system().unwrap()),
    )
    .unwrap();
    node.install_delivery(delivery).unwrap();
    node.start().unwrap();

    let typed = ApplicationHandle::<TimeoutApp>::new(
        client,
        Arc::clone(&application),
        tenant,
        application_id,
    );
    // One deadline per Timer shard, so the bounded pass must deliver two Cells.
    let first_timer_id = timer_id_for_shard(0);
    let second_timer_id = timer_id_for_shard(1);
    assert_ne!(first_timer_id, second_timer_id);
    let reservations = typed.sql::<Reservations>(reservation_target).unwrap();
    reservations
        .batch(
            identity(93),
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO reservations (id, state) VALUES (?1, 0), (?2, 0)".into(),
                    parameters: vec![
                        SqlValue::Integer(RESERVATION_ID),
                        SqlValue::Integer(SECOND_RESERVATION_ID),
                    ],
                }],
            },
        )
        .await
        .unwrap();
    let timeouts = typed.timer::<Timeouts>().unwrap();
    let scheduled = timeouts
        .mutate(
            identity(94),
            TimerMutation::Set {
                timer_id: first_timer_id,
                target_index: 0,
                target_partition: partition_for_shard(0).to_vec(),
                payload: RESERVATION_ID.to_be_bytes().to_vec(),
                due_at_ms: now_ms().unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        scheduled.output,
        TimerMutationOutcome::Applied { generation: 1 }
    );
    let second_timeouts = typed.timer::<Timeouts>().unwrap();
    let second_scheduled = second_timeouts
        .mutate(
            identity(96),
            TimerMutation::Set {
                timer_id: second_timer_id,
                target_index: 0,
                target_partition: partition_for_shard(0).to_vec(),
                payload: SECOND_RESERVATION_ID.to_be_bytes().to_vec(),
                due_at_ms: now_ms().unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        second_scheduled.output,
        TimerMutationOutcome::Applied { generation: 1 }
    );

    // Nothing in this test drives the tick, the consumer, or the effect
    // supervisor: the host delivery loop must release both reservations alone,
    // even though the pass delivers one due Cell at a time.
    let delivered = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let observed = reservations
                .query(
                    None,
                    SqlBatch {
                        statements: vec![SqlStatement {
                            sql: "SELECT count(*) FROM reservations WHERE state = 1".into(),
                            parameters: Vec::new(),
                        }],
                    },
                )
                .await
                .unwrap();
            if matches!(
                observed
                    .output
                    .first()
                    .and_then(|set| set.rows.first())
                    .and_then(|row| row.first()),
                Some(SqlValue::Integer(2))
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(
        delivered.is_ok(),
        "host delivery did not release both reservations"
    );
    assert!(matches!(
        timeouts.get(first_timer_id, None).await.unwrap().output,
        TimerQueryResult::Get(None)
    ));
    assert!(matches!(
        second_timeouts
            .get(second_timer_id, None)
            .await
            .unwrap()
            .output,
        TimerQueryResult::Get(None)
    ));
    let stats = node.delivery_stats().expect("host delivery is installed");
    assert!(
        stats.passes() >= 1 && stats.due_cells() >= 2,
        "delivery loop did not observe both deadlines: {stats:?}"
    );
    assert!(
        stats.delivered() >= 2,
        "delivery loop did not record both deliveries: {stats:?}"
    );
    assert_eq!(
        stats.failed(),
        0,
        "delivery loop reported a failure: {stats:?}"
    );
    assert_eq!(
        stats.in_flight(),
        0,
        "delivery loop still holds in-flight work: {stats:?}"
    );
    let probed = cellule_store::probe_storage(
        &Store::new(Arc::new(InMemory::new())),
        &Path::from("host-delivery-probe"),
        now_ms().unwrap(),
    )
    .await
    .unwrap();
    node.require_storage_capabilities(&probed).unwrap();
    let broken = cellule_store::StorageProbeReport {
        reject_stale_etag: false,
        ..probed.clone()
    };
    assert!(node.require_storage_capabilities(&broken).is_err());
    node.shutdown().await.unwrap();
}
