use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

/// Independent SQL entity Cells selected by canonical device keys.
pub const DEVICES: NamespaceId = NamespaceId::from_bytes([0xb1; 16]);
/// Fixed SQL directory, a separate transaction and receipt domain.
pub const DIRECTORY: NamespaceId = NamespaceId::from_bytes([0xb2; 16]);
/// Authoritative device module and native Effect supervisor bindings.
pub struct Devices;
/// Monotonic directory projection and bounded reads.
pub struct Directory;
impl MaintenanceModule for Devices {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Devices {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl MaintenanceModule for Directory {
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
impl CellModule for Devices {
    const NAME: &'static str = "entity-registry.devices";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = include_str!("device.sql");
        const COMMANDS: &[OperationDescriptor] = &[
            operation(1, 1024, 1024),
            operation(3, 8, 8),
            operation(4, 8, 1 << 20),
            operation(5, 1 << 20, 1 << 20),
        ];
        const QUERIES: &[OperationDescriptor] = &[
            operation(2, 8, 1024),
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
                id: DEVICES,
                name: "device-entities",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[DIRECTORY],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeDevice>()?;
        registry.bind_query::<crate::GetDevice>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Directory {
    const NAME: &'static str = "entity-registry.directory";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const SCHEMA: &str = include_str!("directory.sql");
        const COMMANDS: &[OperationDescriptor] = &[operation(1, 1024, 16), operation(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] =
            &[operation(2, 128, 64 << 10), operation(8, 80, 1024)];
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
                id: DIRECTORY,
                name: "device-directory",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ProjectDevice>()?;
        registry.bind_query::<crate::ListDirectory>()?;
        registry.bind_query::<crate::LookupDevice>()?;
        register_maintenance::<Self>(registry)
    }
}
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("domain.rs"),
        include_bytes!("service.rs"),
        include_bytes!("device.sql"),
        include_bytes!("directory.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Device entity application with an eventually consistent directory.
pub struct EntityRegistry;
impl CellApplication for EntityRegistry {
    const NAME: &'static str = "cellule-cookbook-entity-registry";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Devices)?;
        builder.register(Directory)?;
        builder.cell_type(
            CellType::new(Devices::NAME, "devices", DEVICES, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Directory::NAME, "directory", DIRECTORY, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles pinned source, dependency, schema, operation, and topology contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(EntityRegistry::compile(BuildDescriptor {
        source_revision: format!("entity-registry:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
