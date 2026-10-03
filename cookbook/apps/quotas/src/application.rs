use std::sync::{Arc, OnceLock};

use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::primitives::maintenance::{MaintenanceModule, register_maintenance};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder, registry::OperationDescriptor,
};

use crate::{ChangeQuota, ListReservations, ReadAccount};

/// Stable SQL customer namespace; each customer key selects an entity Cell.
pub const NAMESPACE: NamespaceId = NamespaceId::from_bytes([0xd1; 16]);
const SCHEMA: &str = include_str!("schema.sql");
const COMMANDS: [OperationDescriptor; 2] = [operation(1, 1024, 1024), operation(3, 8, 8)];
const QUERIES: [OperationDescriptor; 2] = [operation(2, 64, 32 << 10), operation(4, 8, 1024)];

/// Compiled quota domain and checked schema migration.
pub struct Credits;

impl MaintenanceModule for Credits {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}

impl CellModule for Credits {
    const NAME: &'static str = "quotas.credits";

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
                name: "credits",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<ChangeQuota>()?;
        registry.bind_query::<ListReservations>()?;
        registry.bind_query::<ReadAccount>()?;
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
    // Include every executable domain source. A handler or codec change must
    // not accidentally keep the old published module identity.
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("commands.rs"),
        include_bytes!("queries.rs"),
        include_bytes!("sql.rs"),
        SCHEMA.as_bytes(),
    ] {
        hasher.update(&(source.len() as u64).to_be_bytes());
        hasher.update(source);
    }
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

/// One SQL entity Cell per customer.
pub struct Quotas;

impl CellApplication for Quotas {
    const NAME: &'static str = "cellule-cookbook-quotas";

    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Credits)?;
        builder.cell_type(
            CellType::new(Credits::NAME, "customers", NAMESPACE, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )
    }
}

/// Compiles the domain with source and lockfile digests bound to this binary.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(Quotas::compile(BuildDescriptor {
        source_revision: format!("quotas-source:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
