use cellule_app::{ApplicationBuilder, CellType, CompiledApplication};
use cellule_runtime::{
    cell::catalog::CatalogRole,
    identity::{Digest, NamespaceId},
    registry::{
        BuildDescriptor, CellModule, MigrationDescriptor, ModuleDescriptor, NamespaceDescriptor,
        OperationDescriptor, Query, QueryContext, RegistryBuilder,
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
            queries: &[OperationDescriptor {
                id: 1,
                codec_version: 1,
                schema_min: 1,
                schema_max: 1,
                input_limit: 8,
                output_limit: 8,
            }],
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
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_query::<ReadValue>()
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

/// Receipt-bound counter read through the same typed reader path as applications.
pub(super) struct ReadValue;
impl Query for ReadValue {
    const MODULE: &'static str = "fleet-example";
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = u64;
    type Output = i64;
    fn execute(context: &mut QueryContext<'_>, _: u64) -> cellule_runtime::Result<i64> {
        use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};
        let sets = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT value FROM counter".into(),
                parameters: vec![],
            }],
        })?;
        match sets
            .first()
            .and_then(|set| set.rows.first())
            .and_then(|row| row.first())
        {
            Some(SqlValue::Integer(value)) => Ok(*value),
            _ => Err(cellule_runtime::Error::Command(
                "fleet example counter is missing",
            )),
        }
    }
}
