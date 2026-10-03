use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        kv::{KvModule, register_kv},
        maintenance::{MaintenanceModule, register_maintenance},
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

pub(crate) const PROJECTS: NamespaceId = NamespaceId::from_bytes([0xa5; 16]);
pub(crate) const PREFERENCES: NamespaceId = NamespaceId::from_bytes([0xa6; 16]);
const PROJECT_COMMANDS: [OperationDescriptor; 2] = [operation(1, 1024, 1024), operation(3, 8, 8)];
const PROJECT_QUERIES: [OperationDescriptor; 1] = [operation(2, 1, 1024)];
const PREFERENCE_COMMANDS: [OperationDescriptor; 2] =
    [operation(1, 2048, 1024), operation(4, 8, 8)];
const PREFERENCE_QUERIES: [OperationDescriptor; 2] =
    [operation(2, 128, 1024), operation(3, 128, 4096)];
const PROJECT_SCHEMA: &str = include_str!("schema.sql");
const PREFERENCE_SCHEMA: &str = "-- schema v1 uses the native KV installer";
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
/// One tenant-scoped SQL entity Cell per configured project key.
pub struct Projects;
impl MaintenanceModule for Projects {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl CellModule for Projects {
    const NAME: &'static str = "tenant-workspace.projects";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: PROJECT_SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(PROJECT_SCHEMA.as_bytes()).as_bytes()),
                }]
            }),
            commands: &PROJECT_COMMANDS,
            queries: &PROJECT_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: PROJECTS,
                name: "projects",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeProject>()?;
        registry.bind_query::<crate::GetProject>()?;
        register_maintenance::<Self>(registry)
    }
}
/// One fixed native KV shard per tenant for its two workspace preferences.
pub struct Preferences;
impl KvModule for Preferences {
    const MODULE: &'static str = Self::NAME;
    const ATOMIC_COMMAND_ID: u32 = 1;
    const GET_QUERY_ID: u32 = 2;
    const LIST_QUERY_ID: u32 = 3;
}
impl MaintenanceModule for Preferences {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 4;
}
impl CellModule for Preferences {
    const NAME: &'static str = "tenant-workspace.preferences";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: PREFERENCE_SCHEMA,
                    digest: Digest::from_bytes(
                        *blake3::hash(PREFERENCE_SCHEMA.as_bytes()).as_bytes(),
                    ),
                }]
            }),
            commands: &PREFERENCE_COMMANDS,
            queries: &PREFERENCE_QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: PREFERENCES,
                name: "preferences",
                role: CatalogRole::Kv,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_kv::<Self>(registry)?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("auth.rs"),
        include_bytes!("model.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("client.rs"),
        include_bytes!("http.rs"),
        PROJECT_SCHEMA.as_bytes(),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Projects and preferences remain separate transaction domains within each tenant.
pub struct WorkspaceApplication;
impl CellApplication for WorkspaceApplication {
    const NAME: &'static str = "cellule-cookbook-tenant-workspace";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Projects)?;
        builder.register(Preferences)?;
        builder.cell_type(
            CellType::new(Projects::NAME, "projects", PROJECTS, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Preferences::NAME,
                "preferences",
                PREFERENCES,
                CatalogRole::Kv,
                1,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles canonical routing, migrations, operation IDs, and source/lockfile contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(WorkspaceApplication::compile(BuildDescriptor {
        source_revision: format!("tenant-workspace-source:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
