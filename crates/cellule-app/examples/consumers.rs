//! One local Queue Cell consumed by a native Rust consumer.

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellModule, CellRuntime, CellStorageLayout, CellTarget, Digest, Error,
    IncarnationId, MaintenanceModule, MigrationDescriptor, ModuleDescriptor, MutationIdentity,
    NamespaceDescriptor, NamespaceId, OperationDescriptor, Owner, QUEUE_SCHEMA_SQL, QueueBatch,
    QueueConsumer, QueueConsumerFuture, QueueConsumerOutcome, QueueModule, QueueSendOutcome,
    QueueSendRequest, QueueSettlement, RegistryBuilder, RequestId, SessionId, SqlWorkerPool,
    TenantId, install_queue_schema, partition_for_shard, register_queue, register_queue_consumer,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const JOBS: NamespaceId = NamespaceId::from_bytes([31; 16]);
const RETRY_DELAY_MS: u32 = 100;
const BATCH_TIMEOUT_MS: u32 = 1_000;
const COMMANDS: [OperationDescriptor; 5] = [
    operation(1),
    operation(2),
    operation(3),
    operation(4),
    operation(5),
];
const QUERIES: [OperationDescriptor; 2] = [operation(1), operation(2)];

struct Jobs;

impl MaintenanceModule for Jobs {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 5;
}

impl QueueModule for Jobs {
    const NAMESPACE: NamespaceId = JOBS;
    const SEND_COMMAND_ID: u32 = 1;
    const CLAIM_COMMAND_ID: u32 = 2;
    const LEASE_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const VALIDATE_QUERY_ID: u32 = 1;
    const INFO_QUERY_ID: u32 = 2;
}

impl QueueConsumer for Jobs {
    const MAX_BATCH_SIZE: u32 = 2;
    const MAX_BATCH_TIMEOUT_MS: u32 = BATCH_TIMEOUT_MS;
    const RETRY_DELAY_MS: u32 = RETRY_DELAY_MS;

    fn consume(batch: QueueBatch) -> QueueConsumerFuture {
        Box::pin(async move {
            Ok(batch
                .messages
                .iter()
                .map(|message| {
                    // A job that fails on its first attempt is retried with the
                    // consumer's delay; the second attempt settles it.
                    if message.payload == b"flaky" && message.attempt == 1 {
                        QueueSettlement::Retry {
                            delay_ms: RETRY_DELAY_MS,
                        }
                    } else {
                        QueueSettlement::Ack
                    }
                })
                .collect())
        })
    }
}

impl CellModule for Jobs {
    const NAME: &'static str = "jobs";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("consumers.rs")).as_bytes(),
            ),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: QUEUE_SCHEMA_SQL,
                    digest: Digest::from_bytes(
                        *blake3::hash(QUEUE_SCHEMA_SQL.as_bytes()).as_bytes(),
                    ),
                }]
            }),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: JOBS,
                name: Self::NAME,
                role: CatalogRole::Queue,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_queue::<Self>(registry)?;
        register_queue_consumer::<Self>(registry)
    }
}

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

struct JobsApp;

impl CellApplication for JobsApp {
    const NAME: &'static str = "consumers-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Jobs)?;
        builder.cell_type(CellType::new(
            Jobs::NAME,
            "jobs",
            JOBS,
            CatalogRole::Queue,
            1,
        )?)?;
        Ok(())
    }
}

fn identity(request_id: u8, now_ms: i64) -> MutationIdentity {
    MutationIdentity {
        request_id: RequestId::from_bytes([request_id; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(JobsApp::compile(BuildDescriptor {
        source_revision: "local-consumers-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([32; 16]);
    let application_id = ApplicationId::from_bytes([33; 16]);
    let target = CellTarget::new(tenant, application_id, JOBS, &partition_for_shard(0))?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("consumers-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Jobs::NAME)
        .ok_or(Error::Registry("jobs module is missing"))?;
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Queue, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([34; 16]);
    let session = SessionId::from_bytes([35; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://consumers.local".into(),
            },
        )
        .await?;
    let files = tempfile::TempDir::new()?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let result: Result<String, Box<dyn std::error::Error>> = async {
        let handle = runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    layout,
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    Limits::default(),
                )?,
                authority,
                observed,
                files.path().join("jobs.sqlite"),
                install_queue_schema,
            )
            .await?;
        let client = CellClient::local(registry, handle);
        let typed = ApplicationHandle::<JobsApp>::new(client, application, tenant, application_id);
        let queue = typed.queue::<Jobs>()?;
        let consumer = typed.queue_consumer::<Jobs>(30_000)?;
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;

        // Two ready messages close a full batch without waiting for the timeout.
        for (producer, payload) in [
            ([36_u8; 16], b"steady".to_vec()),
            ([37_u8; 16], b"flaky".to_vec()),
        ] {
            let sent = queue
                .send(
                    identity(producer[0], now_ms),
                    QueueSendRequest {
                        producer_id: producer,
                        payload,
                        available_at_ms: now_ms,
                    },
                )
                .await?;
            if !matches!(sent.output, QueueSendOutcome::Sent { .. }) {
                return Err(Error::Control("job was not sent").into());
            }
        }
        let first = consumer.run_once(0).await?;
        if !matches!(
            first,
            QueueConsumerOutcome::Completed {
                acked: 1,
                retried: 1,
                ..
            }
        ) {
            return Err(Error::Control("consumer did not settle the full batch").into());
        }
        // A retried job also waits for the batch timeout before it is claimable.
        tokio::time::sleep(Duration::from_millis(
            u64::from(RETRY_DELAY_MS) + u64::from(BATCH_TIMEOUT_MS) + 100,
        ))
        .await;
        let retried = consumer.run_once(0).await?;
        if !matches!(
            retried,
            QueueConsumerOutcome::Completed {
                acked: 1,
                retried: 0,
                ..
            }
        ) {
            return Err(Error::Control("retried job was not acknowledged").into());
        }

        // A single ready message waits for the batch timeout before it runs.
        let sent = queue
            .send(
                identity(38, now_ms),
                QueueSendRequest {
                    producer_id: [38; 16],
                    payload: b"steady".to_vec(),
                    available_at_ms: now_ms,
                },
            )
            .await?;
        if !matches!(sent.output, QueueSendOutcome::Sent { .. }) {
            return Err(Error::Control("second job was not sent").into());
        }
        if !matches!(
            consumer.run_once(0).await?,
            QueueConsumerOutcome::Deferred { .. }
        ) {
            return Err(Error::Control("partial batch did not wait for its timeout").into());
        }
        tokio::time::sleep(Duration::from_millis(u64::from(BATCH_TIMEOUT_MS) + 100)).await;
        let aged = consumer.run_once(0).await?;
        if !matches!(
            aged,
            QueueConsumerOutcome::Completed {
                acked: 1,
                retried: 0,
                ..
            }
        ) {
            return Err(Error::Control("aged batch did not settle").into());
        }

        let info = queue.info(0, None).await?.output;
        if info.acked != 3 || info.ready != 0 || info.leased != 0 {
            return Err(Error::Control("consumer counts do not match").into());
        }
        Ok(format!("consumer settled {} jobs", info.acked))
    }
    .await;
    let shutdown = runtime.shutdown().await;
    let settled = result?;
    shutdown?;
    println!("{settled}");
    Ok(())
}
