use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        blob::register_blob,
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
/// Ticket-local SQL conversations and coordination intents.
pub const TICKETS: NamespaceId = NamespaceId::from_bytes([0x85; 16]);
/// Independently committed generation-fenced native deadlines.
pub const DEADLINES: NamespaceId = NamespaceId::from_bytes([0x86; 16]);
/// Independently committed notification Workflow and Activity history.
pub const NOTIFICATIONS: NamespaceId = NamespaceId::from_bytes([0x87; 16]);
/// Tenant-private immutable native Blob manifests.
pub const ATTACHMENTS: NamespaceId = NamespaceId::from_bytes([0x88; 16]);
/// SQL ticket aggregate module.
#[derive(Clone)]
pub struct Tickets;
/// Native generation-specific timers and callback intents.
#[derive(Clone)]
pub struct Deadlines;
/// Bounded notification delivery workflows and HTTP Activities.
pub struct Notifications;
/// Native immutable Blob publication.
pub struct Attachments;
static DEADLINE_DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
static NOTIFICATION_DEFINITIONS: &[&dyn WorkflowDefinition] =
    &[&crate::notification::NOTIFICATION_DEFINITION];
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
fn migration(sql: &'static str) -> [MigrationDescriptor; 1] {
    [MigrationDescriptor {
        version: 1,
        sql,
        digest: Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
    }]
}
impl MaintenanceModule for Tickets {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Tickets {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl MaintenanceModule for Deadlines {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 6;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEADLINE_DEFINITIONS;
}
impl EffectModule for Deadlines {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 7;
    const LEASE_COMMAND_ID: u32 = 8;
    const VALIDATE_QUERY_ID: u32 = 9;
    const STATUS_QUERY_ID: u32 = 10;
}
impl WorkflowModule for Deadlines {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = DEADLINES;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEADLINE_DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl MaintenanceModule for Notifications {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] =
        NOTIFICATION_DEFINITIONS;
}
impl WorkflowModule for Notifications {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = NOTIFICATIONS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition =
        &crate::notification::NOTIFICATION_DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = NOTIFICATION_DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl WorkflowActivityModule for Notifications {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::http_activity::HTTP_TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}
impl MaintenanceModule for Attachments {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl cellule_runtime::primitives::blob::BlobModule for Attachments {
    const NAMESPACE: NamespaceId = ATTACHMENTS;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}

impl CellModule for Tickets {
    const NAME: &'static str = "support-desk.tickets";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 16384, 131080),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
            op(8, 4096, 64),
            op(9, 8192, 131080),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(2, 8, 131080),
            op(6, 1 << 20, 8),
            op(7, 64, 1 << 20),
            op(10, 256, 262152),
        ];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();

        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("tickets.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: TICKETS,
                name: "support-tickets",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[DEADLINES, NOTIFICATIONS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeTicket>()?;
        registry.bind_command::<crate::EscalateTicket>()?;
        registry.bind_command::<crate::commands::LinkAttachment>()?;
        registry.bind_query::<crate::GetTicket>()?;
        registry.bind_query::<crate::ListMessages>()?;
        register_effect_delivery::<Self>(registry)?;
        register_maintenance::<Self>(registry)
    }
}

impl CellModule for Deadlines {
    const NAME: &'static str = "support-desk.deadlines";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 4096, 64),
            op(2, 4096, 64),
            op(3, 4096, 64),
            op(4, 4096, 64),
            op(6, 8, 8),
            op(7, 8, 1 << 20),
            op(8, 1 << 20, 1 << 20),
            op(11, 2048, 64),
        ];
        const QUERIES: &[OperationDescriptor] =
            &[op(5, 64, 8192), op(9, 1 << 20, 8), op(10, 64, 1 << 20)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DIGEST: OnceLock<[Digest; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("bindings.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGEST.get_or_init(|| [crate::definition::digest()]),
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: DEADLINES,
                name: "support-deadlines",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[TICKETS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        registry.bind_command::<crate::ScheduleDeadline>()?;
        register_effect_delivery::<Self>(registry)?;
        register_maintenance::<Self>(registry)
    }
}

impl CellModule for Notifications {
    const NAME: &'static str = "support-desk.notifications";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 16384, 64),
            op(2, 4096, 64),
            op(3, 4096, 64),
            op(4, 16384, 64),
            op(6, 16, 1 << 20),
            op(7, 1 << 20, 1 << 20),
            op(8, 1 << 20, 64),
            op(9, 8, 8),
            op(11, 8192, 64),
        ];
        const QUERIES: &[OperationDescriptor] = &[op(5, 64, 65536), op(10, 1 << 20, 8)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DIGEST: OnceLock<[Digest; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("bindings.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGEST
                .get_or_init(|| [crate::notification::notification_digest()]),
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: NOTIFICATIONS,
                name: "support-notifications",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        registry.bind_command::<crate::ScheduleNotification>()?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::http_activity::SendHttp>(registry)?;
        register_maintenance::<Self>(registry)
    }
}

impl CellModule for Attachments {
    const NAME: &'static str = "support-desk.attachments";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 4096, 128), op(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 2048, (64 << 10) + 8192)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();

        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION
                .get_or_init(|| migration("-- native immutable support-desk Blob schema v1")),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: ATTACHMENTS,
                name: "support-attachments",
                role: CatalogRole::Blob,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_blob::<Self>(registry)
    }
}

fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("notification.rs"),
        include_bytes!("http_activity.rs"),
        include_bytes!("client.rs"),
        include_bytes!("service.rs"),
        include_bytes!("attachments.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("tickets.sql"),
        include_bytes!("bindings.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Reusable support product with explicit local and asynchronous transaction domains.
pub struct SupportDesk;
impl CellApplication for SupportDesk {
    const NAME: &'static str = "cellule-cookbook-support-desk";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Tickets)?;
        builder.register(Deadlines)?;
        builder.register(Notifications)?;
        builder.register(Attachments)?;
        builder.cell_type(
            CellType::new(Tickets::NAME, "tickets", TICKETS, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Deadlines::NAME,
                "deadlines",
                DEADLINES,
                CatalogRole::Workflow,
                1,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Notifications::NAME,
                "notifications",
                NOTIFICATIONS,
                CatalogRole::Workflow,
                1,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Attachments::NAME,
                "attachments",
                ATTACHMENTS,
                CatalogRole::Blob,
                1,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles stable namespace, schema, typed codec, dependency, and retained-definition inventory.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(SupportDesk::compile(BuildDescriptor {
        source_revision: format!("support-desk:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
