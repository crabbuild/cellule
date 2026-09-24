//! One local Queue Cell that sends, claims, and acknowledges a notification.

use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellModule, CellRuntime, CellStorageLayout, CellTarget, Digest, Error,
    IncarnationId, MaintenanceModule, MigrationDescriptor, ModuleDescriptor, MutationIdentity,
    NamespaceDescriptor, NamespaceId, OperationDescriptor, Owner, QUEUE_SCHEMA_SQL,
    QueueClaimRequest, QueueLeaseOutcome, QueueModule, QueueSendOutcome, QueueSendRequest,
    QueueState, RegistryBuilder, RequestId, SessionId, SqlWorkerPool, TenantId,
    install_queue_schema, partition_for_shard, register_queue,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const NOTIFICATIONS: NamespaceId = NamespaceId::from_bytes([21; 16]);
const COMMANDS: [OperationDescriptor; 5] = [
    operation(1),
    operation(2),
    operation(3),
    operation(4),
    operation(5),
];
const QUERIES: [OperationDescriptor; 2] = [operation(1), operation(2)];

struct Notifications;

impl MaintenanceModule for Notifications {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 5;
}

impl QueueModule for Notifications {
    const NAMESPACE: NamespaceId = NOTIFICATIONS;
    const SEND_COMMAND_ID: u32 = 1;
    const CLAIM_COMMAND_ID: u32 = 2;
    const LEASE_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const VALIDATE_QUERY_ID: u32 = 1;
    const INFO_QUERY_ID: u32 = 2;
}

impl CellModule for Notifications {
    const NAME: &'static str = "notifications";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("notifications.rs")).as_bytes(),
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
                id: NOTIFICATIONS,
                name: Self::NAME,
                role: CatalogRole::Queue,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_queue::<Self>(registry)
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

struct NotificationsApp;

impl CellApplication for NotificationsApp {
    const NAME: &'static str = "notifications-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Notifications)?;
        builder.cell_type(CellType::new(
            Notifications::NAME,
            "notifications",
            NOTIFICATIONS,
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
    let application = Arc::new(NotificationsApp::compile(BuildDescriptor {
        source_revision: "local-notifications-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([22; 16]);
    let application_id = ApplicationId::from_bytes([23; 16]);
    let target = CellTarget::new(
        tenant,
        application_id,
        NOTIFICATIONS,
        &partition_for_shard(0),
    )?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("notifications-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Notifications::NAME)
        .ok_or(Error::Registry("notifications module is missing"))?;
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Queue, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([24; 16]);
    let session = SessionId::from_bytes([25; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://notifications.local".into(),
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
                files.path().join("notifications.sqlite"),
                install_queue_schema,
            )
            .await?;
        let client = CellClient::local(registry, handle);
        let typed =
            ApplicationHandle::<NotificationsApp>::new(client, application, tenant, application_id);
        let queue = typed.queue::<Notifications>()?;
        let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let payload = b"order 42 ready".to_vec();
        let sent = queue
            .send(
                identity(26, now_ms),
                QueueSendRequest {
                    producer_id: [27; 16],
                    payload: payload.clone(),
                    available_at_ms: now_ms,
                },
            )
            .await?;
        if !matches!(sent.output, QueueSendOutcome::Sent { .. }) {
            return Err(Error::Control("notification was not sent").into());
        }
        let claimed = queue
            .claim(
                identity(28, now_ms),
                0,
                QueueClaimRequest {
                    limit: 1,
                    lease_ms: 30_000,
                    max_batch_timeout_ms: 0,
                },
            )
            .await?;
        let [message] = claimed.output.as_slice() else {
            return Err(Error::Control("notification was not claimed").into());
        };
        if message.payload != payload
            || !queue
                .validate_claim(0, claimed.output.clone(), Some(claimed.receipt))
                .await?
                .output
        {
            return Err(Error::Control("notification lease is invalid").into());
        }
        let acknowledged = queue
            .ack(identity(29, now_ms), 0, message.message_id, message.token)
            .await?;
        if !matches!(
            acknowledged.output,
            QueueLeaseOutcome::Applied {
                state: QueueState::Acked,
                ..
            }
        ) || queue
            .info(0, Some(acknowledged.receipt))
            .await?
            .output
            .acked
            != 1
        {
            return Err(Error::Control("notification was not acknowledged").into());
        }
        Ok(String::from_utf8(message.payload.clone())?)
    }
    .await;
    let shutdown = runtime.shutdown().await;
    let notification = result?;
    shutdown?;
    println!("notification acknowledged: {notification}");
    Ok(())
}
