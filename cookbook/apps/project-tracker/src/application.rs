use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BlobModule, BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder,
    primitives::{
        blob::register_blob,
        effects::{EffectModule, register_effect_delivery},
        maintenance::{MaintenanceModule, register_maintenance},
    },
    registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

/// Independent SQL project aggregates selected by canonical entity keys.
pub const PROJECTS: NamespaceId = NamespaceId::from_bytes([0x71; 16]);
/// One independently committed SQL dashboard per authorized tenant.
pub const DASHBOARD: NamespaceId = NamespaceId::from_bytes([0x72; 16]);
/// Private immutable native Blob manifests, separate from project reference transactions.
pub const ATTACHMENTS: NamespaceId = NamespaceId::from_bytes([0x73; 16]);
/// Project domain commands, bounded reads, and native dashboard outbox.
pub struct Projects;
/// Revisioned dashboard receiver and bounded keyset reads.
pub struct Dashboard;
/// Immutable attachment manifests and staged content.
pub struct Attachments;
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
fn migration(schema: &'static str) -> [MigrationDescriptor; 1] {
    [MigrationDescriptor {
        version: 1,
        sql: schema,
        digest: Digest::from_bytes(*blake3::hash(schema.as_bytes()).as_bytes()),
    }]
}
impl MaintenanceModule for Projects {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl MaintenanceModule for Dashboard {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl MaintenanceModule for Attachments {
    const MODULE: &'static str = Self::NAME;
    const TICK_COMMAND_ID: u32 = 3;
}
impl EffectModule for Projects {
    const MODULE: &'static str = Self::NAME;
    const CLAIM_COMMAND_ID: u32 = 4;
    const LEASE_COMMAND_ID: u32 = 5;
    const VALIDATE_QUERY_ID: u32 = 6;
    const STATUS_QUERY_ID: u32 = 7;
}
impl BlobModule for Attachments {
    const NAMESPACE: NamespaceId = ATTACHMENTS;
    const MUTATE_COMMAND_ID: u32 = 1;
    const QUERY_ID: u32 = 2;
}
impl CellModule for Projects {
    const NAME: &'static str = "project-tracker.projects";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[
            op(1, 2048, 1024),
            op(3, 8, 8),
            op(4, 8, 1 << 20),
            op(5, 1 << 20, 1 << 20),
            op(8, 2048, 1024),
        ];
        const QUERIES: &[OperationDescriptor] =
            &[op(2, 64, 128 << 10), op(6, 1 << 20, 8), op(7, 64, 1 << 20)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("projects.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: PROJECTS,
                name: "project-aggregates",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[DASHBOARD],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ChangeProject>()?;
        registry.bind_command::<crate::LinkAttachment>()?;
        registry.bind_query::<crate::GetProject>()?;
        register_maintenance::<Self>(registry)?;
        register_effect_delivery::<Self>(registry)
    }
}
impl CellModule for Dashboard {
    const NAME: &'static str = "project-tracker.dashboard";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 1024, 16), op(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 128, 16 << 10), op(8, 64, 1024)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| migration(include_str!("dashboard.sql"))),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: DASHBOARD,
                name: "tenant-dashboard",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ProjectDashboard>()?;
        registry.bind_query::<crate::LookupProject>()?;
        registry.bind_query::<crate::ListDashboard>()?;
        register_maintenance::<Self>(registry)
    }
}
impl CellModule for Attachments {
    const NAME: &'static str = "project-tracker.attachments";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 2048, 128), op(3, 8, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 1024, (64 << 10) + 8192)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION
                .get_or_init(|| migration("-- private native tracker Blob schema v1")),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: ATTACHMENTS,
                name: "immutable-attachments",
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
fn source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("application.rs"),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("projects.rs"),
        include_bytes!("dashboard.rs"),
        include_bytes!("client.rs"),
        include_bytes!("attachments.rs"),
        include_bytes!("service.rs"),
        include_bytes!("projects.sql"),
        include_bytes!("dashboard.sql"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Project SQL aggregates, a tenant dashboard, and private Blob publication are separate transaction domains.
pub struct ProjectTracker;
impl CellApplication for ProjectTracker {
    const NAME: &'static str = "cellule-cookbook-project-tracker";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Projects)?;
        builder.register(Dashboard)?;
        builder.register(Attachments)?;
        builder.cell_type(
            CellType::new(Projects::NAME, "projects", PROJECTS, CatalogRole::Sql, 1)?
                .with_entity_partitions()?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(Dashboard::NAME, "dashboard", DASHBOARD, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        builder.cell_type(
            CellType::new(
                Attachments::NAME,
                "attachments",
                ATTACHMENTS,
                CatalogRole::Blob,
                1,
            )?
            .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles stable source, dependency, codec, schema, and topology contracts.
pub fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(ProjectTracker::compile(BuildDescriptor {
        source_revision: format!("project-tracker:{:?}", source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
