use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{
            WorkflowActivityModule, WorkflowDefinition, WorkflowModule, register_activity,
            register_workflow, register_workflow_activities,
        },
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};
/// One SQL publisher Cell and permanent subscriber/event history.
pub const FEED: NamespaceId = NamespaceId::from_bytes([0x21; 16]);
/// Two native delivery Workflow shards, selected by immutable business delivery key.
pub const DELIVERIES: NamespaceId = NamespaceId::from_bytes([0x22; 16]);
/// One SQL receiver Cell, independent of publisher and Workflow receipt domains.
pub const RECEIVER: NamespaceId = NamespaceId::from_bytes([0x23; 16]);
/// SQL source and native subscriber Effect delivery module.
#[derive(Clone)]
pub struct Feed;
/// Native delivery Workflow and HTTP Activity module.
#[derive(Clone)]
pub struct Deliveries;
/// Durable reference receiver idempotency and synthetic fault policy.
#[derive(Clone)]
pub struct Receiver;
impl MaintenanceModule for Feed {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Feed {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl MaintenanceModule for Receiver {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl MaintenanceModule for Deliveries {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}
impl WorkflowModule for Deliveries {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = DELIVERIES;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl WorkflowActivityModule for Deliveries {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::http_activity::HTTP_TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
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
fn migration(schema: &'static str) -> [MigrationDescriptor; 1] {
    [MigrationDescriptor {
        version: 1,
        sql: schema,
        digest: Digest::from_bytes(*blake3::hash(schema.as_bytes()).as_bytes()),
    }]
}
impl CellModule for Feed {
    const NAME: &'static str = "webhook.publisher";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 4096, 32768),
            operation(3, 8, 8),
            operation(4, 8, 1 << 20),
            operation(5, 1 << 20, 1 << 20),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            operation(2, 32, 32768),
            operation(6, 1 << 20, 8),
            operation(7, 64, 1 << 20),
            operation(8, 8, 8192),
        ];
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("feed.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: FEED,
                name: "webhook-publisher",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[DELIVERIES],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeFeed>()?;
        registry.bind_query::<crate::queries::ReadEvent>()?;
        registry.bind_query::<crate::queries::ListSubscriptions>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Receiver {
    const NAME: &'static str = "webhook.receiver";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 128, 8),
            operation(2, 4096, 4096),
            operation(3, 8, 8),
        ];
        const QUERIES: &[OperationDescriptor] = &[operation(4, 64, 4096)];
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("receiver.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: RECEIVER,
                name: "webhook-receiver",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::SetReceiverPolicy>()?;
        registry.bind_command::<crate::ReceiveDelivery>()?;
        registry.bind_query::<crate::queries::ReadReceiver>()?;
        register_maintenance::<Self>(registry)
    }
}
impl CellModule for Deliveries {
    const NAME: &'static str = "webhook.deliveries";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DIGESTS: OnceLock<[Digest; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 8192, 64),
            operation(2, 4096, 64),
            operation(3, 4096, 64),
            operation(4, 8192, 64),
            operation(6, 16, 1 << 20),
            operation(7, 1 << 20, 1 << 20),
            operation(8, 1 << 20, 64),
            operation(9, 8, 8),
            operation(11, 4096, 64),
        ];
        const QUERIES: &[OperationDescriptor] =
            &[operation(5, 64, 65536), operation(10, 1 << 20, 8)];
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("delivery.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGESTS.get_or_init(|| [crate::definition::digest()]),
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: DELIVERIES,
                name: "webhook-deliveries",
                role: CatalogRole::Workflow,
                shards: 2,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::http_activity::SendHttp>(registry)?;
        registry.bind_command::<crate::StartDelivery>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("application.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("http_activity.rs"),
        include_bytes!("queries.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("service.rs"),
        include_bytes!("feed.sql"),
        include_bytes!("receiver.sql"),
        include_bytes!("delivery.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Reusable source, delivery, and local receiver application; embeddings own admission and HTTP.
pub struct WebhookApplication;
impl CellApplication for WebhookApplication {
    const NAME: &'static str = "cellule-cookbook-webhook-delivery";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Feed)?;
        builder.register(Deliveries)?;
        builder.register(Receiver)?;
        builder.cell_type(
            CellType::new(Feed::NAME, "publisher", FEED, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Deliveries::NAME,
                "deliveries",
                DELIVERIES,
                CatalogRole::Workflow,
                2,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Receiver::NAME, "receiver", RECEIVER, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles pinned domain source, descriptors, native Activities, and Workflow definition.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(WebhookApplication::compile(BuildDescriptor {
        source_revision: format!("webhook-delivery:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
