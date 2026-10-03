use std::sync::{Arc, OnceLock};

use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::primitives::{
    kv::{KvModule, register_kv},
    maintenance::{MaintenanceModule, register_maintenance},
};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder, registry::OperationDescriptor,
};

pub(crate) const NAMESPACE: NamespaceId = NamespaceId::from_bytes([
    0x22, 0x02, 0x43, 0x19, 0x72, 0x35, 0x41, 0xe3, 0x81, 0x3e, 0x61, 0x38, 0xa6, 0xf2, 0x05, 0x02,
]);
const SCHEMA: &str = "-- preferences schema v1 uses the native KV installer";
const COMMANDS: [OperationDescriptor; 2] = [operation(1, 16 << 10, 8 << 10), operation(4, 8, 8)];
const QUERIES: [OperationDescriptor; 2] = [operation(2, 256, 1024), operation(3, 512, 64 << 10)];

/// Native KV module and its supervised maintenance command.
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
    const NAME: &'static str = "settings.preferences";
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
                    sql: SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
                }]
            }),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: NAMESPACE,
                name: "preferences",
                role: CatalogRole::Kv,
                shards: 4,
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

fn source_digest() -> Digest {
    let mut hasher = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
    ] {
        hasher.update(&(source.len() as u64).to_be_bytes());
        hasher.update(source);
    }
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

/// Four fixed KV shards, with organization scopes isolated within each shard.
pub struct Settings;
impl CellApplication for Settings {
    const NAME: &'static str = "cellule-cookbook-settings";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Preferences)?;
        builder.cell_type(
            CellType::new(
                Preferences::NAME,
                "preferences",
                NAMESPACE,
                CatalogRole::Kv,
                4,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )
    }
}

/// Compiles source, dependency, schema, operation, and fixed-shard contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(Settings::compile(BuildDescriptor {
        source_revision: format!("settings-source:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
