//! Two local Cells that schedule, deliver, and read a reservation timeout.

use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellHandle, CellModule, CellRuntime, CellStorageLayout, CellTarget, Command,
    CommandContext, CommandResult, Digest, EffectModule, EffectPeerClient, EffectRunOutcome, Error,
    IncarnationId, MaintenanceModule, MaintenanceTickOutcome, MaintenanceTickRequest,
    MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor, NamespaceId,
    OperationDescriptor, Owner, PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal,
    PeerRoundTrip, PeerSigner, PeerVerifier, Registry, RegistryBuilder, RequestId, Result,
    SessionId, SqlBatch, SqlModule, SqlStatement, SqlValue, SqlWorkerPool, TIMER_SCHEMA_SQL,
    TenantId, TimerInvocation, TimerModule, TimerMutation, TimerMutationOutcome, TimerQueryResult,
    TimerTarget, VerifiedPeerRequest, install_timer_schema, partition_for_shard,
    register_effect_delivery, register_sql, register_timer,
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

const RESERVATIONS: NamespaceId = NamespaceId::from_bytes([61; 16]);
const TIMEOUTS: NamespaceId = NamespaceId::from_bytes([62; 16]);
const RESERVATION_ID: i64 = 42;
const TIMER_ID: [u8; 16] = [63; 16];
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
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("timeouts.rs")).as_bytes(),
            ),
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
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("timeouts.rs")).as_bytes(),
            ),
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
                shards: 1,
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
    const NAME: &'static str = "timeouts-example";

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
            1,
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
        .ok_or(Error::Registry("timeout module is missing"))?;
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
                endpoint: "https://timeouts.local".into(),
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

struct TimeoutResolver {
    target: CellTarget,
    handle: CellHandle,
}

impl PeerCellResolver for TimeoutResolver {
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

struct TimeoutAuthorizer;

impl PeerAuthorizer for TimeoutAuthorizer {
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

fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Command("system time is before the Unix epoch"))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::Command("system time exceeds the supported range"))
}

fn identity(request_id: u8) -> Result<MutationIdentity> {
    let now_ms = now_ms()?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes([request_id; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    })
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(TimeoutApp::compile(BuildDescriptor {
        source_revision: "local-timeouts-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let registry = application.registry();
    let tenant = TenantId::from_bytes([64; 16]);
    let application_id = ApplicationId::from_bytes([65; 16]);
    let reservation_target = CellTarget::new(
        tenant,
        application_id,
        RESERVATIONS,
        &partition_for_shard(0),
    )?;
    let timeout_target =
        CellTarget::new(tenant, application_id, TIMEOUTS, &partition_for_shard(0))?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("timeouts-example"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new()?;
    let session = SessionId::from_bytes([66; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 8)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let result: std::result::Result<String, Box<dyn std::error::Error>> = async {
        let reservation_handle = bootstrap_cell(
            &runtime,
            &registry,
            &layout,
            session,
            CellSpec {
                target: reservation_target.clone(),
                role: CatalogRole::Sql,
                module: Reservations::NAME,
                incarnation: IncarnationId::from_bytes([67; 16]),
                path: files.path().join("reservations.sqlite"),
                install: |transaction| {
                    transaction.execute_batch(RESERVATION_SCHEMA)?;
                    Ok(())
                },
            },
        )
        .await?;
        let timeout_handle = bootstrap_cell(
            &runtime,
            &registry,
            &layout,
            session,
            CellSpec {
                target: timeout_target.clone(),
                role: CatalogRole::Timer,
                module: Timeouts::NAME,
                incarnation: IncarnationId::from_bytes([68; 16]),
                path: files.path().join("timeouts.sqlite"),
                install: install_timer_schema,
            },
        )
        .await?;
        let client = CellClient::local_many(
            Arc::clone(&registry),
            vec![reservation_handle.clone(), timeout_handle],
        )?;
        let typed = ApplicationHandle::<TimeoutApp>::new(
            client.clone(),
            application,
            tenant,
            application_id,
        );
        let reservations = typed.sql::<Reservations>(reservation_target.clone())?;
        reservations
            .batch(
                identity(69)?,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO reservations (id, state) VALUES (?1, 0)".into(),
                        parameters: vec![SqlValue::Integer(RESERVATION_ID)],
                    }],
                },
            )
            .await?;

        let timeouts = typed.timer::<Timeouts>()?;
        let scheduled = timeouts
            .mutate(
                identity(70)?,
                TimerMutation::Set {
                    timer_id: TIMER_ID,
                    target_index: 0,
                    target_partition: partition_for_shard(0).to_vec(),
                    payload: RESERVATION_ID.to_be_bytes().to_vec(),
                    due_at_ms: now_ms()?,
                },
            )
            .await?;
        if !matches!(scheduled.output, TimerMutationOutcome::Applied { .. }) {
            return Err(Error::Control("reservation timeout was not scheduled").into());
        }

        let tick = registry
            .run_maintenance_once(
                client.clone(),
                timeout_target.clone(),
                identity(71)?,
                MaintenanceTickRequest {
                    expected_commit_sequence: scheduled.receipt.commit_sequence,
                },
            )
            .await?;
        if !matches!(
            tick.output,
            MaintenanceTickOutcome::Applied { processed: 1 }
        ) || !matches!(
            timeouts.get(TIMER_ID, Some(tick.receipt)).await?.output,
            TimerQueryResult::Get(None)
        ) {
            return Err(Error::Control("reservation timeout did not fire once").into());
        }

        // The example keeps the private peer protocol signed and verified over an in-process hop.
        let peer_session = SessionId::from_bytes([72; 16]);
        let signer = Arc::new(PeerSigner::new(
            peer_session,
            registry.release_digest(),
            SigningKey::from_bytes(&[73; 32]),
        ));
        let verifier = Arc::new(PeerVerifier::new(
            peer_session,
            registry.release_digest(),
            signer.verifying_key(),
        ));
        let dispatcher = Arc::new(PeerDispatcher::new(
            Arc::clone(&registry),
            Arc::new(TimeoutResolver {
                target: reservation_target.clone(),
                handle: reservation_handle,
            }),
            Arc::new(TimeoutAuthorizer),
        ));
        let peer = EffectPeerClient::new(
            signer,
            PeerPrincipal {
                issuer: "timeouts-example".into(),
                subject: "timeout-runner".into(),
                actions: vec!["timeout.deliver".into()],
            },
            Arc::new(Loopback {
                verifier,
                dispatcher,
            }),
        );
        if !matches!(
            registry
                .run_effect_once(client, timeout_target, peer, 5_000)
                .await?,
            EffectRunOutcome::Delivered {
                destination: cellule_runtime::StoredOutcome::Success { .. },
                ..
            }
        ) {
            return Err(Error::Control("reservation timeout was not delivered").into());
        }
        let observed = reservations
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT state FROM reservations WHERE id = ?1".into(),
                        parameters: vec![SqlValue::Integer(RESERVATION_ID)],
                    }],
                },
            )
            .await?;
        let Some(SqlValue::Integer(1)) = observed
            .output
            .first()
            .and_then(|set| set.rows.first())
            .and_then(|row| row.first())
        else {
            return Err(Error::Control("released reservation is missing").into());
        };
        Ok(format!("reservation {RESERVATION_ID} released by timeout"))
    }
    .await;
    let shutdown = runtime.shutdown().await;
    let released = result?;
    shutdown?;
    println!("timeout delivered: {released}");
    Ok(())
}
