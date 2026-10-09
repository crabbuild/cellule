use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        cron::{CronModule, CronTarget, register_cron},
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

/// Two fixed Cron shards selected by canonical schedule identity.
pub const SCHEDULES: NamespaceId = NamespaceId::from_bytes([0x81; 16]);
/// One SQL reminder inbox, a separate transaction and receipt domain.
pub const INBOX: NamespaceId = NamespaceId::from_bytes([0x82; 16]);
/// Native Cron module and source effect supervisor bindings.
pub struct Schedules;
/// Application SQL inbox and durable occurrence deduplication.
pub struct Inbox;
impl CronModule for Schedules {
    const NAMESPACE: NamespaceId = SCHEDULES;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}
impl MaintenanceModule for Schedules {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
    const CRON_TARGETS: &'static [CronTarget] = &[CronTarget::new(
        "interval-scheduler.inbox",
        INBOX,
        1,
        1,
        2048,
    )];
}
impl EffectModule for Schedules {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl MaintenanceModule for Inbox {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
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
impl CellModule for Schedules {
    const NAME: &'static str = "interval-scheduler.schedules";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = "-- native fixed-interval Cron schema v1";
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 4096, 16),
            operation(3, 8, 8),
            operation(4, 8, 1 << 20),
            operation(5, 1 << 20, 1 << 20),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            operation(2, 64, 1 << 20),
            operation(6, 1 << 20, 8),
            operation(7, 64, 1 << 20),
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
                id: SCHEDULES,
                name: "reminder-schedules",
                role: CatalogRole::Cron,
                shards: 2,
                effect_targets: &[INBOX],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_cron::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Inbox {
    const NAME: &'static str = "interval-scheduler.inbox";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = include_str!("schema.sql");
        const COMMANDS: &[OperationDescriptor] = &[operation(1, 2048, 16), operation(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[operation(2, 64, 128 << 10)];
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
                id: INBOX,
                name: "reminder-inbox",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::RecordReminder>()?;
        registry.bind_query::<crate::ListDeliveries>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("inbox.rs"),
        include_bytes!("service.rs"),
        include_bytes!("schema.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// A fixed-interval reminder application; calendar and notification policies stay in the embedding.
pub struct IntervalScheduler;
impl CellApplication for IntervalScheduler {
    const NAME: &'static str = "cellule-cookbook-interval-scheduler";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Schedules)?;
        builder.register(Inbox)?;
        builder.cell_type(
            CellType::new(
                Schedules::NAME,
                "schedules",
                SCHEDULES,
                CatalogRole::Cron,
                2,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Inbox::NAME, "inbox", INBOX, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles pinned source, dependency, schema, operation, and topology contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(IntervalScheduler::compile(BuildDescriptor {
        source_revision: format!("interval-scheduler:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
