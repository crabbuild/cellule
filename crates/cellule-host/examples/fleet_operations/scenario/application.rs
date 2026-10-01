use cellule_app::{ApplicationBuilder, CellType, CompiledApplication};
use cellule_runtime::{
    cell::catalog::CatalogRole,
    identity::{Digest, NamespaceId},
    registry::{
        BuildDescriptor, CellModule, MigrationDescriptor, ModuleDescriptor, NamespaceDescriptor,
        RegistryBuilder,
    },
};
use std::sync::Arc;

pub(super) const NAMESPACE: NamespaceId = NamespaceId::from_bytes([2; 16]);
struct Module;
impl CellModule for Module {
    const NAME: &'static str = "fleet-example";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: ModuleDescriptor = ModuleDescriptor {
            name: "fleet-example",
            source_digest: Digest::from_bytes([1; 32]),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: &[MigrationDescriptor {
                version: 1,
                sql: "-- host migration v1",
                digest: Digest::from_bytes([
                    0xd7, 0x41, 0xcb, 0x18, 0xae, 0xd4, 0x80, 0xb0, 0xe1, 0x55, 0x8e, 0x34, 0x5a,
                    0x6b, 0xef, 0xf5, 0xe1, 0x60, 0x80, 0x59, 0x06, 0xba, 0xfe, 0x75, 0xff, 0x9f,
                    0xa0, 0x7d, 0x10, 0xe7, 0x77, 0xbf,
                ]),
            }],
            commands: &[],
            queries: &[],
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: NAMESPACE,
                name: "fleet-example",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        };
        &DESCRIPTOR
    }
    fn register(self, _: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        Ok(())
    }
}
pub(super) fn compile() -> cellule_runtime::Result<Arc<CompiledApplication>> {
    let mut builder = ApplicationBuilder::new(
        "fleet-example",
        BuildDescriptor {
            source_revision: "fleet-example".into(),
            cargo_lock_digest: Digest::from_bytes([7; 32]),
        },
    )?;
    builder.register(Module)?;
    builder.cell_type(CellType::new(
        "fleet-example",
        "fleet-example",
        NAMESPACE,
        CatalogRole::Sql,
        1,
    )?)?;
    Ok(Arc::new(builder.finish()?))
}
