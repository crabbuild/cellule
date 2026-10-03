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
/// Separate orders transaction domain.
pub const ORDERS: NamespaceId = NamespaceId::from_bytes([0x71; 16]);
/// Typed orders module and its pinned storage contracts.
#[derive(Clone)]
pub struct Orders;
/// Separate inventory transaction domain.
pub const INVENTORY: NamespaceId = NamespaceId::from_bytes([0x72; 16]);
/// Typed inventory module and its pinned storage contracts.
#[derive(Clone)]
pub struct Inventory;
/// Separate sagas transaction domain.
pub const SAGAS: NamespaceId = NamespaceId::from_bytes([0x73; 16]);
/// Typed sagas module and its pinned storage contracts.
#[derive(Clone)]
pub struct Sagas;
/// Separate payments transaction domain.
pub const PAYMENTS: NamespaceId = NamespaceId::from_bytes([0x74; 16]);
/// Typed payments module and its pinned storage contracts.
#[derive(Clone)]
pub struct Payments;
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
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl WorkflowModule for Sagas {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = SAGAS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl WorkflowActivityModule for Sagas {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::activity::TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}
impl MaintenanceModule for Sagas {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}
impl MaintenanceModule for Orders {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl MaintenanceModule for Inventory {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl MaintenanceModule for Payments {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Orders {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl EffectModule for Inventory {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl EffectModule for Sagas {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 13;
    const LEASE_COMMAND_ID: u32 = 14;
    const VALIDATE_QUERY_ID: u32 = 15;
    const STATUS_QUERY_ID: u32 = 16;
}
impl CellModule for Orders {
    const NAME: &'static str = "checkout.orders";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 2048, 256),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
            op(8, 4096, 128),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(2, 64, 4096),
            op(6, 1 << 20, 8),
            op(7, 64, 1 << 20),
            op(9, 256, 262144),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("orders.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: ORDERS,
                name: "checkout-orders",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[SAGAS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeOrder>()?;
        registry.bind_command::<crate::OrderStep>()?;
        registry.bind_query::<crate::orders::GetOrder>()?;
        registry.bind_query::<crate::orders::ListOrders>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Inventory {
    const NAME: &'static str = "checkout.inventory";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 256, 128),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
            op(8, 4096, 128),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(2, 128, 1024),
            op(6, 1 << 20, 8),
            op(7, 64, 1 << 20),
            op(9, 64, 4096),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("inventory.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: INVENTORY,
                name: "checkout-inventory",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[SAGAS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::SeedStock>()?;
        registry.bind_command::<crate::StockStep>()?;
        registry.bind_query::<crate::inventory::GetStock>()?;
        registry.bind_query::<crate::inventory::GetReservation>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Sagas {
    const NAME: &'static str = "checkout.sagas";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 4096, 64),
            op(2, 8192, 64),
            op(3, 1024, 64),
            op(4, 4096, 64),
            op(6, 16, 1 << 20),
            op(7, 1 << 20, 1 << 20),
            op(8, 1 << 20, 64),
            op(9, 8, 8),
            op(11, 2048, 128),
            op(12, 4096, 128),
            op(13, 8, 1 << 20),
            op(14, 1 << 20, 1 << 20),
            op(17, 512, 128),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(5, 64, 16384),
            op(10, 1 << 20, 8),
            op(15, 1 << 20, 8),
            op(16, 64, 1 << 20),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static DIGESTS: OnceLock<[Digest; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("sagas.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGESTS.get_or_init(|| [crate::definition::digest()]),
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: SAGAS,
                name: "checkout-sagas",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[ORDERS, INVENTORY],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::activity::HttpPayment>(registry)?;
        register_effect_delivery::<Self>(registry)?;
        registry.bind_command::<crate::StartSaga>()?;
        registry.bind_command::<crate::ReplySaga>()?;
        registry.bind_command::<crate::ReconcileSaga>()?;
        register_maintenance::<Self>(registry)
    }
}
impl CellModule for Payments {
    const NAME: &'static str = "checkout.payments";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 4096, 128), op(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 64, 4096)];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("payments.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: PAYMENTS,
                name: "checkout-payments",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ApplyPayment>()?;
        registry.bind_query::<crate::payments::GetPayment>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("application.rs"),
        include_bytes!("orders.rs"),
        include_bytes!("inventory.rs"),
        include_bytes!("payments.rs"),
        include_bytes!("sagas.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("activity.rs"),
        include_bytes!("service.rs"),
        include_bytes!("orders.sql"),
        include_bytes!("inventory.sql"),
        include_bytes!("payments.sql"),
        include_bytes!("sagas.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Checkout and an independent payment simulator, assembled from four transaction domains.
pub struct CheckoutApplication;
impl CellApplication for CheckoutApplication {
    const NAME: &'static str = "cellule-cookbook-checkout";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Orders)?;
        builder.register(Inventory)?;
        builder.register(Sagas)?;
        builder.register(Payments)?;
        for (module, name, namespace, role) in [
            (Orders::NAME, "orders", ORDERS, CatalogRole::Sql),
            (Inventory::NAME, "inventory", INVENTORY, CatalogRole::Sql),
            (Sagas::NAME, "sagas", SAGAS, CatalogRole::Workflow),
            (Payments::NAME, "payments", PAYMENTS, CatalogRole::Sql),
        ] {
            builder.cell_type(
                CellType::new(module, name, namespace, role, 1)?.with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles immutable source, wire, dependency, and schema contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(CheckoutApplication::compile(BuildDescriptor {
        source_revision: format!("checkout:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
