use cellule_app::{ApplicationBuilder, CellApplication, CellType, CompiledApplication};
use cellule_runtime::{
    BuildDescriptor, CatalogRole, CellModule, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, RegistryBuilder, registry::OperationDescriptor,
};
use std::sync::{Arc, OnceLock};

/// Independently owned local deployment target; pipeline state is a different application domain.
pub const TARGETS: NamespaceId = NamespaceId::from_bytes([0x91; 16]);
/// Typed simulator domain, with no dependence on a native Workflow execution attempt.
#[derive(Clone)]
pub struct Targets;
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
impl CellModule for Targets {
    const NAME: &'static str = "release.target";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        const COMMANDS: &[OperationDescriptor] = &[op(1, 4608, 8)];
        const QUERIES: &[OperationDescriptor] = &[op(2, 64, 1024), op(3, 64, 512), op(4, 64, 8192)];
        static VALUE: OnceLock<ModuleDescriptor> = OnceLock::new();
        static MIGRATION: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        VALUE.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: target_source_digest(),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATION.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
                    sql: include_str!("target.sql"),
                    digest: Digest::from_bytes(
                        *blake3::hash(include_bytes!("target.sql")).as_bytes(),
                    ),
                }]
            }),
            commands: COMMANDS,
            queries: QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: TARGETS,
                name: "release-targets",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<crate::ApplyTarget>()?;
        registry.bind_query::<crate::GetTargetRecord>()?;
        registry.bind_query::<crate::GetTargetState>()?;
        registry.bind_query::<crate::GetTargetArtifact>()
    }
}
fn target_source_digest() -> Digest {
    let mut hash = blake3::Hasher::new();
    for source in [
        include_bytes!("lib.rs").as_slice(),
        include_bytes!("model.rs"),
        include_bytes!("wire.rs"),
        include_bytes!("target.rs"),
        include_bytes!("sql.rs"),
        include_bytes!("target.sql"),
        include_bytes!("application.rs"),
    ] {
        hash.update(&(source.len() as u64).to_be_bytes());
        hash.update(source);
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}
/// Separate deployment simulator application, served and drained independently of release workers.
pub struct TargetApplication;
impl CellApplication for TargetApplication {
    const NAME: &'static str = "cellule-cookbook-release-target";
    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Targets)?;
        builder.cell_type(
            CellType::new(Targets::NAME, "targets", TARGETS, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )
    }
}
/// Compiles the independently durable simulator's source, schema, and dependency contracts.
pub fn compile_target() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    Ok(Arc::new(TargetApplication::compile(BuildDescriptor {
        source_revision: format!("release-target:{:?}", target_source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?))
}
