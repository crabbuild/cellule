use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BlobModule, BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{blob::register_blob, maintenance::MaintenanceModule},
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

pub(crate) const NAMESPACE: NamespaceId = NamespaceId::from_bytes([
    0x33, 0x03, 0x43, 0x19, 0x72, 0x35, 0x41, 0xe3, 0x81, 0x3e, 0x61, 0x38, 0xa6, 0xf2, 0x05, 0x03,
]);
const SCHEMA: &str = "-- file-vault v1 uses the native Blob schema installer";
const COMMANDS: [OperationDescriptor; 2] = [operation(1, 1024, 128), operation(3, 8, 8)];
const QUERIES: [OperationDescriptor; 1] = [operation(2, 1024, (256 << 10) + 4096)];

/// Native immutable part and file manifest operations.
pub struct Files;
impl MaintenanceModule for Files {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl BlobModule for Files {
    const NAMESPACE: NamespaceId = NAMESPACE;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}
impl CellModule for Files {
    const NAME: &'static str = "file-vault.files";
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
                name: "files",
                role: CatalogRole::Blob,
                shards: 4,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_blob::<Self>(registry)
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
        include_bytes!("model.rs"),
        include_bytes!("application.rs"),
        include_bytes!("plan.rs"),
    ] {
        hasher.update(&(source.len() as u64).to_be_bytes());
        hasher.update(source);
    }
    Digest::from_bytes(*hasher.finalize().as_bytes())
}
/// Four fixed Blob shards with an application-private artifact store.
pub struct FileVault;
impl CellApplication for FileVault {
    const NAME: &'static str = "cellule-cookbook-file-vault";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Files)?;
        builder.cell_type(
            CellType::new(Files::NAME, "files", NAMESPACE, CatalogRole::Blob, 4)?
                .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles fixed namespace, schema, operation, source, and dependency contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(FileVault::compile(BuildDescriptor {
        source_revision: format!("file-vault-source:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
