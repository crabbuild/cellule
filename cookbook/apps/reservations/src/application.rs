use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{WorkflowDefinition, WorkflowModule, register_workflow},
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};
/// Event SQL entity namespace; all seats for an event share one transaction domain.
pub const INVENTORY: NamespaceId = NamespaceId::from_bytes([0xf1; 16]);
/// Two Workflow shards; identity hashes both event key and permanent hold ID.
pub const DEADLINES: NamespaceId = NamespaceId::from_bytes([0xf2; 16]);
/// SQL inventory, generation history, and deadline-start intent module.
#[derive(Clone)]
pub struct InventoryCells;
/// Native durable deadline Workflow and expiration delivery module.
#[derive(Clone)]
pub struct Deadlines;
impl MaintenanceModule for InventoryCells {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for InventoryCells {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl MaintenanceModule for Deadlines {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 6;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
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
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
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
impl CellModule for InventoryCells {
    const NAME: &'static str = "reservations.inventory";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = include_str!("schema.sql");
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 1024, 2048),
            operation(3, 8, 8),
            operation(4, 8, 1 << 20),
            operation(5, 1 << 20, 1 << 20),
            operation(8, 1024, 16),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            operation(2, 64, 64 << 10),
            operation(6, 1 << 20, 8),
            operation(7, 64, 1 << 20),
            operation(9, 8, 256),
            operation(10, 32, 2048),
            operation(11, 8, 64 << 10),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
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
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: INVENTORY,
                name: "event-seats",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[DEADLINES],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeReservation>()?;
        registry.bind_command::<crate::ExpireHold>()?;
        registry.bind_query::<crate::ReadInventory>()?;
        registry.bind_query::<crate::ReadHold>()?;
        registry.bind_query::<crate::ListHolds>()?;
        registry.bind_query::<crate::queries::ActiveHolds>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Deadlines {
    const NAME: &'static str = "reservations.deadlines";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = include_str!("deadline.sql");
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 4096, 64),
            operation(2, 4096, 64),
            operation(3, 4096, 64),
            operation(4, 4096, 64),
            operation(6, 8, 8),
            operation(7, 8, 1 << 20),
            operation(8, 1 << 20, 1 << 20),
            operation(11, 1024, 64),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            operation(5, 64, 4096),
            operation(9, 1 << 20, 8),
            operation(10, 64, 1 << 20),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DEFINITIONS: OnceLock<[Digest; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
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
            workflow_definitions: DEFINITIONS.get_or_init(|| [crate::definition::digest()]),
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: DEADLINES,
                name: "hold-deadlines",
                role: CatalogRole::Workflow,
                shards: 2,
                effect_targets: &[INVENTORY],
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
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("queries.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("service.rs"),
        include_bytes!("schema.sql"),
        include_bytes!("deadline.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Scarce event inventory coordinated with independent durable deadline Workflows.
pub struct Reservations;
impl CellApplication for Reservations {
    const NAME: &'static str = "cellule-cookbook-reservations";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(InventoryCells)?;
        builder.register(Deadlines)?;
        builder.cell_type(
            CellType::new(
                InventoryCells::NAME,
                "events",
                INVENTORY,
                CatalogRole::Sql,
                1,
            )?
            .with_entity_partitions()?
            .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Deadlines::NAME,
                "deadlines",
                DEADLINES,
                CatalogRole::Workflow,
                2,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles pinned source, namespace, migration, codec and Workflow definition contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(Reservations::compile(BuildDescriptor {
        source_revision: format!("reservations:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
