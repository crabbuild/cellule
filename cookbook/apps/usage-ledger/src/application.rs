use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BlobModule, BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder,
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

/// Entity-partitioned permanent source usage Cells.
pub const ACCOUNTS: NamespaceId = NamespaceId::from_bytes([0xa1; 16]);
/// Entity-partitioned billing period and eventual projection Cells.
pub const PERIODS: NamespaceId = NamespaceId::from_bytes([0xa2; 16]);
/// Immutable CSV statements.
pub const FILES: NamespaceId = NamespaceId::from_bytes([0xa3; 16]);
/// Native close Workflow and Activity ledger.
pub const RUNS: NamespaceId = NamespaceId::from_bytes([0xa4; 16]);

/// Typed account source and Effect sender module.
#[derive(Clone)]
pub struct Accounts;
/// Typed period inbox, reconciliation, and sealing module.
#[derive(Clone)]
pub struct Periods;
/// Typed immutable statement Blob module.
#[derive(Clone)]
pub struct Files;
/// Typed native close workflow module.
#[derive(Clone)]
pub struct Runs;

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

fn migration(sql: &'static str) -> [MigrationDescriptor; 1] {
    [MigrationDescriptor {
        version: 1,
        sql,
        digest: Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
    }]
}

impl EffectModule for Accounts {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}

macro_rules! maintenance {
    ($ty:ty, $id:literal) => {
        impl MaintenanceModule for $ty {
            const MODULE: &'static str = Self::NAME;
            const TICK_COMMAND_ID: u32 = $id;
        }
    };
}
maintenance!(Periods, 7);
maintenance!(Files, 3);

static CLOSE_DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::workflow::DEFINITION];

impl MaintenanceModule for Runs {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = CLOSE_DEFINITIONS;
}

impl WorkflowModule for Runs {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = RUNS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::workflow::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = CLOSE_DEFINITIONS;
    const START_COMMAND_ID: u32 = 1;
    const SIGNAL_COMMAND_ID: u32 = 2;
    const CANCEL_COMMAND_ID: u32 = 3;
    const CONTROL_COMMAND_ID: u32 = 4;
    const GET_QUERY_ID: u32 = 5;
}

impl WorkflowActivityModule for Runs {
    const ACTIVITY_TYPES: &'static [&'static str] = &[crate::activity::TYPE];
    const ACTIVITY_CLAIM_COMMAND_ID: u32 = 6;
    const ACTIVITY_COMPLETE_COMMAND_ID: u32 = 7;
    const ACTIVITY_EXTEND_COMMAND_ID: u32 = 8;
    const ACTIVITY_VALIDATE_QUERY_ID: u32 = 10;
}

macro_rules! module {
    ($ty:ty, $name:literal, $role:ident, $schema:expr, $commands:expr, $queries:expr, $targets:expr, $bind:expr) => {
        impl CellModule for $ty {
            const NAME: &'static str = $name;
            fn descriptor(&self) -> &'static ModuleDescriptor {
                const COMMANDS: &[OperationDescriptor] = $commands;
                const QUERIES: &[OperationDescriptor] = $queries;
                static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
                static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
                VALUE.get_or_init(|| ModuleDescriptor {
                    name: Self::NAME,
                    source_digest: source_digest(),
                    retained_codes: &[],
                    schema_min: 1,
                    schema_max: 1,
                    migrations: MIGRATIONS.get_or_init(|| migration($schema)),
                    commands: COMMANDS,
                    queries: QUERIES,
                    workflow_definitions: &[],
                    activity_types: &[],
                    namespaces: &[NamespaceDescriptor {
                        id: <$ty>::NAMESPACE_ID,
                        name: Self::NAME,
                        role: CatalogRole::$role,
                        shards: 1,
                        effect_targets: $targets,
                        dead_letter: None,
                    }],
                })
            }
            fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
                $bind(registry)
            }
        }
    };
}

impl Accounts {
    const NAMESPACE_ID: NamespaceId = ACCOUNTS;
}
impl Periods {
    const NAMESPACE_ID: NamespaceId = PERIODS;
}
module!(
    Accounts,
    "usage-ledger.accounts",
    Sql,
    include_str!("account.sql"),
    &[
        operation(1, 4096, 4096),
        operation(2, 4096, 4096),
        operation(3, 4096, 256),
        operation(4, 1 << 20, 1 << 20),
        operation(5, 1 << 20, 1 << 20),
        operation(8, 8, 8),
        operation(9, 4096, 64 << 10),
    ],
    &[
        operation(6, 1 << 20, 8),
        operation(7, 256, 4096),
        operation(10, 1024, 1024),
    ],
    &[PERIODS],
    |registry: &mut RegistryBuilder| {
        registry.bind_command::<crate::account::BindAccount>()?;
        registry.bind_command::<crate::account::ActivateAccount>()?;
        registry.bind_command::<crate::account::RecordUsage>()?;
        registry.bind_command::<crate::account::CloseUsage>()?;
        registry.bind_query::<crate::account::GetAccount>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
);

module!(
    Periods,
    "usage-ledger.periods",
    Sql,
    include_str!("period.sql"),
    &[
        operation(1, 4096, 256),
        operation(2, 2048, 256),
        operation(3, 4096, 256),
        operation(4, 1024, 256),
        operation(5, 64 << 10, 256),
        operation(6, 1024, 1 << 20),
        operation(7, 8, 8),
    ],
    &[operation(10, 256, 1 << 20)],
    &[],
    |registry: &mut RegistryBuilder| {
        registry.bind_command::<crate::period::CreatePeriod>()?;
        registry.bind_command::<crate::period::ConfirmReady>()?;
        registry.bind_command::<crate::period::ProjectUsage>()?;
        registry.bind_command::<crate::period::BeginClose>()?;
        registry.bind_command::<crate::period::ReconcileAccountCommand>()?;
        registry.bind_command::<crate::period::SealPeriod>()?;
        registry.bind_query::<crate::period::GetPeriod>()?;
        register_maintenance::<Self>(registry)
    }
);

impl BlobModule for Files {
    const NAMESPACE: NamespaceId = FILES;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}

impl CellModule for Runs {
    const NAME: &'static str = "usage-ledger.runs";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 1 << 20, 1 << 20),
            operation(2, 1 << 20, 1 << 20),
            operation(3, 1 << 20, 1 << 20),
            operation(4, 1 << 20, 1 << 20),
            operation(6, 1 << 20, 1 << 20),
            operation(7, 1 << 20, 1 << 20),
            operation(8, 1 << 20, 1 << 20),
            operation(9, 8, 8),
            operation(11, 4096, 256),
        ];
        const QUERIES: &[OperationDescriptor] =
            &[operation(5, 64, 1 << 20), operation(10, 1 << 20, 8)];
        static DIGESTS: OnceLock<[Digest; 1]> = OnceLock::new();
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(
                "CREATE TABLE close_bindings(period_id BLOB PRIMARY KEY CHECK(length(period_id)=16),request BLOB NOT NULL) STRICT;",
            )),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: DIGESTS.get_or_init(|| [crate::workflow::digest()]),
            activity_types: Self::ACTIVITY_TYPES,
            namespaces: &[NamespaceDescriptor {
                id: RUNS,
                name: Self::NAME,
                role: CatalogRole::Workflow,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::activity::Process>(registry)?;
        registry.bind_command::<crate::commands::StartClose>()?;
        register_maintenance::<Self>(registry)
    }
}

impl CellModule for Files {
    const NAME: &'static str = "usage-ledger.files";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[operation(1, 1024, 128), operation(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[operation(2, 1024, (256 << 10) + 4096)];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration("-- immutable usage-ledger Blob")),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: FILES,
                name: Self::NAME,
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

impl MaintenanceModule for Accounts {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 8;
}

fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("application.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("account.rs"),
        include_bytes!("period.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("client.rs"),
        include_bytes!("workflow.rs"),
        include_bytes!("activity.rs"),
        include_bytes!("adapter.rs"),
        include_bytes!("engine.rs"),
        include_bytes!("service.rs"),
        include_bytes!("account.sql"),
        include_bytes!("period.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}

/// Complete usage ledger with source SQL, period projection, Blob, and close Workflow.
pub struct UsageLedger;

impl CellApplication for UsageLedger {
    const NAME: &'static str = "cellule-cookbook-usage-ledger";

    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Accounts)?;
        builder.register(Periods)?;
        builder.register(Files)?;
        builder.register(Runs)?;
        builder.cell_type(
            CellType::new(Accounts::NAME, "accounts", ACCOUNTS, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Periods::NAME, "periods", PERIODS, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Files::NAME, "files", FILES, CatalogRole::Blob, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Runs::NAME, "runs", RUNS, CatalogRole::Workflow, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        Ok(())
    }
}

/// Compiles pinned modules, SQL schemas, codecs, and Workflow definition.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(UsageLedger::compile(BuildDescriptor {
        source_revision: format!("usage-ledger:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
