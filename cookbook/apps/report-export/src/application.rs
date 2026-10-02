use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BlobModule, BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        blob::register_blob,
        maintenance::{MaintenanceModule, register_maintenance},
        workflow::{
            WorkflowActivityModule, WorkflowDefinition, WorkflowModule, register_activity,
            register_workflow, register_workflow_activities,
        },
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};
/// Draft and sealed SQL dataset namespace.
pub const DATA: NamespaceId = NamespaceId::from_bytes([0x41; 16]);
/// Immutable page and report Blob namespace.
pub const FILES: NamespaceId = NamespaceId::from_bytes([0x42; 16]);
/// Native report Workflow namespace.
pub const RUNS: NamespaceId = NamespaceId::from_bytes([0x44; 16]);
/// Native relational dataset and immutable versions.
#[derive(Clone)]
pub struct Dataset;
/// Native CSV page and final report manifests.
#[derive(Clone)]
pub struct Files;
/// Native Workflow and local computation Activities.
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
const BLOB_COMMANDS: &[OperationDescriptor] = &[operation(1, 1024, 128), operation(3, 8, 8)];
const BLOB_QUERIES: &[OperationDescriptor] = &[operation(2, 1024, (256 << 10) + 4096)];
const RUN_COMMANDS: &[OperationDescriptor] = &[
    operation(1, 8192, 64),
    operation(2, 1024, 64),
    operation(3, 1024, 64),
    operation(4, 8192, 64),
    operation(6, 16, 1 << 20),
    operation(7, 1 << 20, 1 << 20),
    operation(8, 1 << 20, 64),
    operation(9, 8, 8),
    operation(11, 8192, 64),
];
const RUN_QUERIES: &[OperationDescriptor] = &[operation(5, 64, 65536), operation(10, 1 << 20, 8)];
fn migration(sql: &'static str) -> [MigrationDescriptor; 1] {
    [MigrationDescriptor {
        version: 1,
        sql,
        digest: Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
    }]
}
impl MaintenanceModule for Dataset {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl CellModule for Dataset {
    const NAME: &'static str = "report.dataset";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] =
            &[operation(1, 128 << 10, 256), operation(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[
            operation(2, 8, 128),
            operation(4, 128, 256),
            operation(5, 256, 32768),
        ];
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration(include_str!("dataset.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: DATA,
                name: "report-dataset",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeDataset>()?;
        registry.bind_query::<crate::dataset::ReadDataset>()?;
        registry.bind_query::<crate::dataset::ReadSnapshot>()?;
        registry.bind_query::<crate::dataset::ReadPage>()?;
        register_maintenance::<Self>(registry)
    }
}
impl MaintenanceModule for Files {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl BlobModule for Files {
    const NAMESPACE: NamespaceId = FILES;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}
impl CellModule for Files {
    const NAME: &'static str = "report.files";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| migration("-- report native Blob schema v1")),
            commands: BLOB_COMMANDS,
            queries: BLOB_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: FILES,
                name: "report-files",
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
static DEFINITIONS: &[&dyn WorkflowDefinition] = &[&crate::definition::DEFINITION];
impl MaintenanceModule for Runs {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 9;
    const WORKFLOW_DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
}
impl WorkflowModule for Runs {
    const MODULE: &'static str = Self::NAME;
    const NAMESPACE: NamespaceId = RUNS;
    const CURRENT_DEFINITION: &'static dyn WorkflowDefinition = &crate::definition::DEFINITION;
    const DEFINITIONS: &'static [&'static dyn WorkflowDefinition] = DEFINITIONS;
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
impl CellModule for Runs {
    const NAME: &'static str = "report.runs";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DIGESTS: OnceLock<[Digest; 1]> = OnceLock::new();
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        VALUE.get_or_init(||ModuleDescriptor{name:Self::NAME,source_digest:source_digest(),retained_codes:&[],schema_min:1,schema_max:1,migrations:MIGRATIONS.get_or_init(||migration("CREATE TABLE export_bindings(run_id BLOB PRIMARY KEY CHECK(length(run_id)=16),request BLOB NOT NULL) STRICT;")),commands:RUN_COMMANDS,queries:RUN_QUERIES,workflow_definitions:DIGESTS.get_or_init(||[crate::definition::digest()]),activity_types:<Self as WorkflowActivityModule>::ACTIVITY_TYPES,namespaces:&[NamespaceDescriptor{id:RUNS,name:"report-runs",role:CatalogRole::Workflow,shards:1,effect_targets:&[],dead_letter:None}]})
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_workflow::<Self>(registry)?;
        register_workflow_activities::<Self>(registry)?;
        register_activity::<Self, crate::activity::Process>(registry)?;
        registry.bind_command::<crate::Start>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("encoding.rs"),
        include_bytes!("dataset.rs"),
        include_bytes!("dataset.sql"),
        include_bytes!("wire.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("engine.rs"),
        include_bytes!("artifacts.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("definition.rs"),
        include_bytes!("activity.rs"),
        include_bytes!("application.rs"),
        include_bytes!("service.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// SQL dataset, immutable CSV files, and Workflow modules with separate transactions.
pub struct ExportApplication;
impl CellApplication for ExportApplication {
    const NAME: &'static str = "cellule-cookbook-report-export";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Dataset)?;
        builder.register(Files)?;
        builder.register(Runs)?;
        for (module, name, namespace, role) in [
            (Dataset::NAME, "dataset", DATA, CatalogRole::Sql),
            (Files::NAME, "files", FILES, CatalogRole::Blob),
            (Runs::NAME, "runs", RUNS, CatalogRole::Workflow),
        ] {
            builder.cell_type(
                CellType::new(module, name, namespace, role, 1)?.with_limits(64 << 20, 16 << 20)?,
            )?;
        }
        Ok(())
    }
}
/// Compiles pinned domain source, codec, namespace, migration, and Workflow contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(ExportApplication::compile(BuildDescriptor {
        source_revision: format!("report-export:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
