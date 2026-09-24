use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_ltx::CellStorageLayout;
use cellule_ltx::{CellReplica, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellModule, CellRuntime, CellTarget, Digest, Error, IncarnationId,
    MaintenanceModule, MigrationDescriptor, ModuleDescriptor, MutationIdentity,
    NamespaceDescriptor, NamespaceId, OperationDescriptor, Owner, QueueBatch, QueueClaimRequest,
    QueueConsumer, QueueConsumerFuture, QueueConsumerOutcome, QueueModule, QueueNamespace,
    QueueSendOutcome, QueueSendRequest, QueueSettlement, RegistryBuilder, RequestId, Result,
    SessionId, SqlWorkerPool, TenantId, install_queue_schema, register_queue,
    register_queue_consumer,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const QUEUE_MODULE: &str = "queue-consumer-test";
const QUEUE_NAMESPACE: NamespaceId = NamespaceId::from_bytes([21; 16]);
const QUEUE_MIGRATION: &str = include_str!("../src/migrations/queue.sql");
const QUEUE_COMMANDS: &[OperationDescriptor] = &[
    operation(1, 270 * 1024, 32),
    operation(2, 16, 530 * 1024),
    operation(3, 64, 16),
    operation(4, 8, 5),
    operation(5, 8, 16),
];
const QUEUE_QUERIES: &[OperationDescriptor] = &[operation(1, 530 * 1024, 1), operation(2, 1, 64)];
const BATCH_TIMEOUT_MS: u32 = 50;

struct ConsumerQueue;

impl QueueModule for ConsumerQueue {
    const NAMESPACE: NamespaceId = QUEUE_NAMESPACE;
    const SEND_COMMAND_ID: u32 = 1;
    const CLAIM_COMMAND_ID: u32 = 2;
    const LEASE_COMMAND_ID: u32 = 3;
    const VALIDATE_QUERY_ID: u32 = 1;
    const CONTROL_COMMAND_ID: u32 = 5;
    const INFO_QUERY_ID: u32 = 2;
}

impl MaintenanceModule for ConsumerQueue {
    const MODULE: &'static str = QUEUE_MODULE;
    const TICK_COMMAND_ID: u32 = 4;
}

impl QueueConsumer for ConsumerQueue {
    const MAX_BATCH_SIZE: u32 = 2;
    const MAX_BATCH_TIMEOUT_MS: u32 = BATCH_TIMEOUT_MS;
    const RETRY_DELAY_MS: u32 = 10;

    fn consume(batch: QueueBatch) -> QueueConsumerFuture {
        Box::pin(async move {
            if batch
                .messages
                .iter()
                .any(|message| message.payload.starts_with(b"fail"))
            {
                return Err(Error::Command("poison message"));
            }
            Ok(vec![QueueSettlement::Ack; batch.messages.len()])
        })
    }
}

impl CellModule for ConsumerQueue {
    const NAME: &'static str = QUEUE_MODULE;

    fn descriptor(&self) -> &'static ModuleDescriptor {
        Box::leak(Box::new(ModuleDescriptor {
            name: QUEUE_MODULE,
            source_digest: Digest::from_bytes([4; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: QUEUE_MIGRATION,
                digest: Digest::from_bytes(*blake3::hash(QUEUE_MIGRATION.as_bytes()).as_bytes()),
            }])),
            commands: QUEUE_COMMANDS,
            queries: QUEUE_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: Box::leak(Box::new([NamespaceDescriptor {
                id: QUEUE_NAMESPACE,
                name: QUEUE_MODULE,
                role: CatalogRole::Queue,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }])),
        }))
    }

    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        register_queue::<Self>(registry)?;
        register_queue_consumer::<Self>(registry)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_consumer_forms_batches_and_settles_every_message() {
    let mut builder = RegistryBuilder::new(BuildDescriptor {
        source_revision: "queue-consumer-test".into(),
        cargo_lock_digest: Digest::from_bytes([22; 32]),
    });
    builder.register(ConsumerQueue).unwrap();
    let registry = Arc::new(builder.finish().unwrap());
    assert!(registry.has_queue_consumer(QUEUE_NAMESPACE));

    let tenant = TenantId::from_bytes([23; 16]);
    let application = ApplicationId::from_bytes([24; 16]);
    let target =
        CellTarget::new(tenant, application, QUEUE_NAMESPACE, &0_u32.to_be_bytes()).unwrap();
    let incarnation = IncarnationId::from_bytes([25; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("queue-consumer"),
        *application.as_bytes(),
    );
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Queue,
                registry.module_code(QUEUE_MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout.clone());
    let session = SessionId::from_bytes([26; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://queue-consumer.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let directory = tempfile::TempDir::new().unwrap();
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
            authority,
            observed,
            directory.path().join("queue-consumer.sqlite"),
            install_queue_schema,
        )
        .await
        .unwrap();
    let client = CellClient::local(registry.clone(), handle.clone());
    let queue = QueueNamespace::<ConsumerQueue>::new(client.clone(), tenant, application).unwrap();
    let now_ms = now_ms();

    send(&queue, 30, b"ok-1", now_ms).await;
    let deferred = registry
        .run_queue_consumer_once(client.clone(), &target, 5_000)
        .await
        .unwrap();
    assert!(
        matches!(deferred, QueueConsumerOutcome::Deferred { .. }),
        "young partial batch was not deferred: {deferred:?}"
    );
    tokio::time::sleep(Duration::from_millis(u64::from(BATCH_TIMEOUT_MS) + 20)).await;
    let completed = registry
        .run_queue_consumer_once(client.clone(), &target, 5_000)
        .await
        .unwrap();
    assert!(
        matches!(completed, QueueConsumerOutcome::Completed { acked: 1, .. }),
        "aged partial batch was not acked: {completed:?}"
    );
    let info = queue.info(0, None).await.unwrap().output;
    assert_eq!((info.ready, info.leased, info.acked), (0, 0, 1));

    send(&queue, 31, b"full-1", now_ms).await;
    send(&queue, 32, b"full-2", now_ms).await;
    let full = registry
        .run_queue_consumer_once(client.clone(), &target, 5_000)
        .await
        .unwrap();
    assert!(
        matches!(full, QueueConsumerOutcome::Completed { acked: 2, .. }),
        "full batch was not acked: {full:?}"
    );

    send(&queue, 33, b"fail-1", now_ms).await;
    tokio::time::sleep(Duration::from_millis(u64::from(BATCH_TIMEOUT_MS) + 20)).await;
    let failed = registry
        .run_queue_consumer_once(client.clone(), &target, 5_000)
        .await
        .unwrap();
    assert!(
        matches!(
            failed,
            QueueConsumerOutcome::HandlerFailed { retried: 1, .. }
        ),
        "poison message was not retried: {failed:?}"
    );
    let info = queue.info(0, None).await.unwrap().output;
    assert_eq!((info.ready, info.leased, info.acked), (1, 0, 3));
    assert!(
        queue
            .claim(
                identity(34, now_ms),
                0,
                QueueClaimRequest {
                    limit: 2,
                    lease_ms: 5_000,
                    max_batch_timeout_ms: 0,
                },
            )
            .await
            .unwrap()
            .output
            .is_empty()
    );

    drop(queue);
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

async fn send(queue: &QueueNamespace<ConsumerQueue>, producer: u8, payload: &[u8], now_ms: i64) {
    let sent = queue
        .send(
            identity(producer, now_ms),
            QueueSendRequest {
                producer_id: [producer; 16],
                payload: payload.to_vec(),
                available_at_ms: now_ms,
            },
        )
        .await
        .unwrap();
    assert!(matches!(sent.output, QueueSendOutcome::Sent { .. }));
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
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
