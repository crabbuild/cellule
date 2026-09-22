//! Two local Cells that schedule, deliver, and read an invoice notification.

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
    ApplicationId, BuildDescriptor, CRON_SCHEMA_SQL, CatalogEntry, CatalogRole, CellAuthority,
    CellCatalog, CellClient, CellHandle, CellModule, CellRuntime, CellStorageLayout, CellTarget,
    Command, CommandContext, CommandResult, CronInvocation, CronModule, CronMutation,
    CronMutationOutcome, CronQueryResult, CronTarget, Digest, EffectModule, EffectPeerClient,
    EffectRunOutcome, Error, IncarnationId, MaintenanceModule, MaintenanceTickOutcome,
    MaintenanceTickRequest, MigrationDescriptor, ModuleDescriptor, MutationIdentity,
    NamespaceDescriptor, NamespaceId, OperationDescriptor, Owner, PeerAuthorizer, PeerCellResolver,
    PeerDispatcher, PeerPrincipal, PeerRoundTrip, PeerSigner, PeerVerifier, Registry,
    RegistryBuilder, RequestId, Result, SessionId, SqlBatch, SqlModule, SqlStatement, SqlValue,
    SqlWorkerPool, TenantId, VerifiedPeerRequest, install_cron_schema, partition_for_shard,
    register_cron, register_effect_delivery, register_sql,
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path};

const INVOICES: NamespaceId = NamespaceId::from_bytes([41; 16]);
const SCHEDULES: NamespaceId = NamespaceId::from_bytes([42; 16]);
const SCHEDULE_ID: [u8; 16] = [43; 16];
const INVOICE_SCHEMA: &str = "CREATE TABLE invoice_receipts (schedule_id BLOB NOT NULL, occurrence INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(schedule_id, occurrence))";
const INVOICE_COMMANDS: [OperationDescriptor; 2] = [operation(1), operation(3)];
const INVOICE_QUERIES: [OperationDescriptor; 1] = [operation(2)];
const SCHEDULE_COMMANDS: [OperationDescriptor; 4] =
    [operation(1), operation(3), operation(4), operation(5)];
const SCHEDULE_QUERIES: [OperationDescriptor; 3] = [operation(2), operation(6), operation(7)];

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

struct InvoiceReceipts;

impl SqlModule for InvoiceReceipts {
    const MODULE: &'static str = Self::NAME;
    const BATCH_COMMAND_ID: u32 = 1;
    const BATCH_QUERY_ID: u32 = 2;
}

struct ReceiveInvoice;

impl Command for ReceiveInvoice {
    const MODULE: &'static str = InvoiceReceipts::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = CronInvocation;
    type Output = ();

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO invoice_receipts (schedule_id, occurrence, payload) VALUES (?1, ?2, ?3)".into(),
                parameters: vec![
                    SqlValue::Blob(input.schedule_id.to_vec()),
                    SqlValue::Integer(i64::try_from(input.occurrence).map_err(|_| {
                        Error::Command("invoice occurrence exceeds SQL integer range")
                    })?),
                    SqlValue::Blob(input.payload),
                ],
            }],
        })?;
        Ok(CommandResult::Success(()))
    }
}

impl CellModule for InvoiceReceipts {
    const NAME: &'static str = "invoice-receipts";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("invoices.rs")).as_bytes(),
            ),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: INVOICE_SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(INVOICE_SCHEMA.as_bytes()).as_bytes()),
                }]
            }),
            commands: &INVOICE_COMMANDS,
            queries: &INVOICE_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: INVOICES,
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
        registry.bind_command::<ReceiveInvoice>()
    }
}

struct InvoiceSchedules;

impl MaintenanceModule for InvoiceSchedules {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
    const CRON_TARGETS: &'static [CronTarget] = &[CronTarget::new(
        InvoiceReceipts::NAME,
        INVOICES,
        3,
        1,
        1 << 20,
    )];
}

impl CronModule for InvoiceSchedules {
    const NAMESPACE: NamespaceId = SCHEDULES;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}

impl EffectModule for InvoiceSchedules {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}

impl CellModule for InvoiceSchedules {
    const NAME: &'static str = "invoice-schedules";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("invoices.rs")).as_bytes(),
            ),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: CRON_SCHEMA_SQL,
                    digest: Digest::from_bytes(
                        *blake3::hash(CRON_SCHEMA_SQL.as_bytes()).as_bytes(),
                    ),
                }]
            }),
            commands: &SCHEDULE_COMMANDS,
            queries: &SCHEDULE_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: SCHEDULES,
                name: Self::NAME,
                role: CatalogRole::Cron,
                shards: 1,
                effect_targets: &[INVOICES],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        register_cron::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}

struct InvoiceApp;

impl CellApplication for InvoiceApp {
    const NAME: &'static str = "invoices-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> Result<()> {
        builder.register(InvoiceReceipts)?;
        builder.register(InvoiceSchedules)?;
        builder.cell_type(CellType::new(
            InvoiceReceipts::NAME,
            "invoice-receipts",
            INVOICES,
            CatalogRole::Sql,
            1,
        )?)?;
        builder.cell_type(CellType::new(
            InvoiceSchedules::NAME,
            "invoice-schedules",
            SCHEDULES,
            CatalogRole::Cron,
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
        .ok_or(Error::Registry("invoice module is missing"))?;
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
                endpoint: "https://invoices.local".into(),
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

struct InvoiceResolver {
    target: CellTarget,
    handle: CellHandle,
}

impl PeerCellResolver for InvoiceResolver {
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

struct InvoiceAuthorizer;

impl PeerAuthorizer for InvoiceAuthorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> Result<()> {
        if request.permits("invoice.deliver") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization(
                "invoice delivery is not authorized",
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
                    return Err(Error::Peer("invoice target changed in transit"));
                }
                dispatcher.dispatch_bytes(&verified, now_ms()?).await
            })
            .await
            .map_err(|source| Error::PeerTransportUnknown {
                context: "invoice peer deadline",
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
    let application = Arc::new(InvoiceApp::compile(BuildDescriptor {
        source_revision: "local-invoices-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let registry = application.registry();
    let tenant = TenantId::from_bytes([44; 16]);
    let application_id = ApplicationId::from_bytes([45; 16]);
    let invoice_target =
        CellTarget::new(tenant, application_id, INVOICES, &partition_for_shard(0))?;
    let cron_target = CellTarget::new(tenant, application_id, SCHEDULES, &partition_for_shard(0))?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("invoices-example"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new()?;
    let session = SessionId::from_bytes([46; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 8)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let result: std::result::Result<String, Box<dyn std::error::Error>> = async {
        let invoice_handle = bootstrap_cell(
            &runtime,
            &registry,
            &layout,
            session,
            CellSpec {
                target: invoice_target.clone(),
                role: CatalogRole::Sql,
                module: InvoiceReceipts::NAME,
                incarnation: IncarnationId::from_bytes([47; 16]),
                path: files.path().join("invoice-receipts.sqlite"),
                install: |transaction| {
                    transaction.execute_batch(INVOICE_SCHEMA)?;
                    Ok(())
                },
            },
        )
        .await?;
        let cron_handle = bootstrap_cell(
            &runtime,
            &registry,
            &layout,
            session,
            CellSpec {
                target: cron_target.clone(),
                role: CatalogRole::Cron,
                module: InvoiceSchedules::NAME,
                incarnation: IncarnationId::from_bytes([48; 16]),
                path: files.path().join("invoice-schedules.sqlite"),
                install: install_cron_schema,
            },
        )
        .await?;
        let client = CellClient::local_many(
            Arc::clone(&registry),
            vec![invoice_handle.clone(), cron_handle],
        )?;
        let typed = ApplicationHandle::<InvoiceApp>::new(
            client.clone(),
            application,
            tenant,
            application_id,
        );
        let cron = typed.cron::<InvoiceSchedules>()?;
        let payload = b"invoice 42 ready".to_vec();
        let scheduled = cron
            .mutate(
                identity(49)?,
                CronMutation::Upsert {
                    schedule_id: SCHEDULE_ID,
                    target_index: 0,
                    target_partition: partition_for_shard(0).to_vec(),
                    payload: payload.clone(),
                    interval_ms: 60_000,
                    next_due_ms: now_ms()? + 5,
                },
            )
            .await?;
        if !matches!(scheduled.output, CronMutationOutcome::Applied { .. }) {
            return Err(Error::Control("invoice schedule was not created").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let tick = registry
            .run_maintenance_once(
                client.clone(),
                cron_target.clone(),
                identity(50)?,
                MaintenanceTickRequest {
                    expected_commit_sequence: scheduled.receipt.commit_sequence,
                },
            )
            .await?;
        if !matches!(tick.output, MaintenanceTickOutcome::Applied { processed: 1 })
            || !matches!(
                cron.get(SCHEDULE_ID, Some(tick.receipt)).await?.output,
                CronQueryResult::Get(Some(schedule)) if schedule.occurrence == 1
            )
        {
            return Err(Error::Control("invoice schedule did not fire").into());
        }

        // The example keeps the private peer protocol signed and verified over an in-process hop.
        let peer_session = SessionId::from_bytes([51; 16]);
        let signer = Arc::new(PeerSigner::new(
            peer_session,
            registry.release_digest(),
            SigningKey::from_bytes(&[52; 32]),
        ));
        let verifier = Arc::new(PeerVerifier::new(
            peer_session,
            registry.release_digest(),
            signer.verifying_key(),
        ));
        let dispatcher = Arc::new(PeerDispatcher::new(
            Arc::clone(&registry),
            Arc::new(InvoiceResolver {
                target: invoice_target.clone(),
                handle: invoice_handle,
            }),
            Arc::new(InvoiceAuthorizer),
        ));
        let peer = EffectPeerClient::new(
            signer,
            PeerPrincipal {
                issuer: "invoices-example".into(),
                subject: "invoice-runner".into(),
                actions: vec!["invoice.deliver".into()],
            },
            Arc::new(Loopback {
                verifier,
                dispatcher,
            }),
        );
        if !matches!(
            registry
                .run_effect_once(client, cron_target, peer, 5_000)
                .await?,
            EffectRunOutcome::Delivered {
                destination: cellule_runtime::StoredOutcome::Success { .. },
                ..
            }
        ) {
            return Err(Error::Control("invoice effect was not delivered").into());
        }
        let sql = typed.sql::<InvoiceReceipts>(invoice_target)?;
        let observed = sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT payload FROM invoice_receipts WHERE schedule_id = ?1 AND occurrence = 1".into(),
                        parameters: vec![SqlValue::Blob(SCHEDULE_ID.to_vec())],
                    }],
                },
            )
            .await?;
        let Some(SqlValue::Blob(delivered)) = observed
            .output
            .first()
            .and_then(|set| set.rows.first())
            .and_then(|row| row.first())
        else {
            return Err(Error::Control("delivered invoice is missing").into());
        };
        if delivered != &payload {
            return Err(Error::Control("delivered invoice differs").into());
        }
        Ok(String::from_utf8(delivered.clone())?)
    }
    .await;
    let shutdown = runtime.shutdown().await;
    let invoice = result?;
    shutdown?;
    println!("invoice delivered: {invoice}");
    Ok(())
}
