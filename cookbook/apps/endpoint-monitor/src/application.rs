use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        cron::{CronModule, CronTarget, register_cron},
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
/// Native scheduled intent namespace.
pub const SCHEDULES: NamespaceId = NamespaceId::from_bytes([0x61; 16]);
/// Native probe Workflow and first durable observation namespace.
pub const PROBES: NamespaceId = NamespaceId::from_bytes([0x62; 16]);
/// SQL check history, incident state, and notification intent namespace.
pub const CHECKS: NamespaceId = NamespaceId::from_bytes([0x63; 16]);
/// Independent SQL notification inbox namespace.
pub const ALERTS: NamespaceId = NamespaceId::from_bytes([0x64; 16]);
/// Fixed-interval native Cron module with bounded immutable configuration history.
#[derive(Clone)]
pub struct Schedules;
/// Native probe execution and signed result delivery module.
#[derive(Clone)]
pub struct Probes;
/// Atomic SQL check and incident projection module.
#[derive(Clone)]
pub struct Checks;
/// Permanent incident-edge notification inbox module.
#[derive(Clone)]
pub struct Alerts;
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
impl CronModule for Schedules {
    const NAMESPACE: NamespaceId = SCHEDULES;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}
impl MaintenanceModule for Schedules {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
    const CRON_TARGETS: &'static [CronTarget] =
        &[CronTarget::new("monitor.probes", PROBES, 11, 1, 2048)];
}
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl MaintenanceModule for Probes {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}
impl WorkflowModule for Probes {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = PROBES;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}
impl WorkflowActivityModule for Probes {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::activity::TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}
impl MaintenanceModule for Checks {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl MaintenanceModule for Alerts {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Schedules {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl EffectModule for Probes {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 12;
    const LEASE_COMMAND_ID: u32 = 13;
    const VALIDATE_QUERY_ID: u32 = 14;
    const STATUS_QUERY_ID: u32 = 15;
}
impl EffectModule for Checks {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl CellModule for Schedules {
    const NAME: &'static str = "monitor.schedules";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 4096, 16),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
            op(8, 4096, 128),
        ];
        const QUERIES: &[OperationDescriptor] =
            &[op(2, 64, 1 << 20), op(6, 1 << 20, 8), op(7, 64, 1 << 20)];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("schedules.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: SCHEDULES,
                name: "monitor-schedules",
                role: CatalogRole::Cron,
                shards: 1,
                effect_targets: &[PROBES],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_cron::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)?;
        registry.bind_command::<crate::ChangeSchedule>()
    }
}
impl CellModule for Probes {
    const NAME: &'static str = "monitor.probes";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 4096, 64),
            op(2, 1024, 64),
            op(3, 1024, 64),
            op(4, 4096, 64),
            op(6, 16, 1 << 20),
            op(7, 1 << 20, 1 << 20),
            op(8, 1 << 20, 64),
            op(9, 8, 8),
            op(11, 2048, 128),
            op(12, 8, 1 << 20),
            op(13, 1 << 20, 1 << 20),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(5, 64, 16384),
            op(10, 1 << 20, 8),
            op(14, 1 << 20, 8),
            op(15, 64, 1 << 20),
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
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("probes.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGESTS.get_or_init(|| [crate::definition::digest()]),
            activity_types: <Self as WorkflowActivityModule>::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: PROBES,
                name: "monitor-probes",
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[CHECKS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::activity::HttpProbe>(registry)?;
        register_effect_delivery::<Self>(registry)?;
        registry.bind_command::<crate::StartProbe>()?;
        register_maintenance::<Self>(registry)
    }
}
impl CellModule for Checks {
    const NAME: &'static str = "monitor.checks";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 4096, 4096),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            op(2, 512, 262144),
            op(6, 1 << 20, 8),
            op(7, 64, 1 << 20),
            op(8, 2048, 8192),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("checks.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: CHECKS,
                name: "monitor-checks",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[ALERTS],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::RecordCheck>()?;
        registry.bind_query::<crate::checks::Inspect>()?;
        registry.bind_query::<crate::checks::ReadCheck>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Alerts {
    const NAME: &'static str = "monitor.alerts";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 2048, 128), op(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 512, 262144)];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("alerts.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: ALERTS,
                name: "monitor-alerts",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::RecordAlert>()?;
        registry.bind_query::<crate::alerts::ListAlerts>()?;
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
        include_bytes!("schedules.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("activity.rs"),
        include_bytes!("checks.rs"),
        include_bytes!("alerts.rs"),
        include_bytes!("service.rs"),
        include_bytes!("schedules.sql"),
        include_bytes!("probes.sql"),
        include_bytes!("checks.sql"),
        include_bytes!("alerts.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Four separate domains: scheduled intent, completed observation, incident projection, notifications.
pub struct MonitorApplication;
impl CellApplication for MonitorApplication {
    const NAME: &'static str = "cellule-cookbook-endpoint-monitor";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Schedules)?;
        builder.register(Probes)?;
        builder.register(Checks)?;
        builder.register(Alerts)?;
        for (module, name, namespace, role) in [
            (Schedules::NAME, "schedules", SCHEDULES, CatalogRole::Cron),
            (Probes::NAME, "probes", PROBES, CatalogRole::Workflow),
            (Checks::NAME, "checks", CHECKS, CatalogRole::Sql),
            (Alerts::NAME, "alerts", ALERTS, CatalogRole::Sql),
        ] {
            builder.cell_type(
                CellType::new(module, name, namespace, role, 1)?.with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles pinned application source, dependency, wire, schema, and topology contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(MonitorApplication::compile(BuildDescriptor {
        source_revision: format!("endpoint-monitor:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
