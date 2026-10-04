use std::sync::OnceLock;

use cellule_app::{ApplicationBuilder, CellApplication, CellBinding};
use cellule_runtime::{
    CatalogRole, CellModule, Command, Digest, MigrationDescriptor, ModuleDescriptor,
    NamespaceDescriptor, NamespaceId, Query, RegistryBuilder,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
    registry::{CommandContext, CommandResult, OperationDescriptor, QueryContext},
};

pub const ORDERS: NamespaceId = NamespaceId::from_bytes([23; 16]);
const SCHEMA: &str = "CREATE TABLE totals (id INTEGER PRIMARY KEY, cents INTEGER NOT NULL)";
const COMMANDS: [OperationDescriptor; 1] = [operation(SetTotal::ID)];
const QUERIES: [OperationDescriptor; 1] = [operation(ReadTotal::ID)];

pub struct Orders;
pub struct OrdersApp;
pub struct SetTotal;
pub struct ReadTotal;

impl CellModule for Orders {
    const NAME: &'static str = "example.orders";

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
                id: ORDERS,
                name: "orders",
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        registry.bind_command::<SetTotal>()?;
        registry.bind_query::<ReadTotal>()
    }
}

impl CellApplication for OrdersApp {
    const NAME: &'static str = "builder-service-example";

    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        // The helper reads role, shards and schema from Orders' descriptor.
        // Stable namespace, Cell name, partition mode and limits stay explicit.
        builder.module(
            Orders,
            [CellBinding::sharded(ORDERS, "orders").with_limits(64 << 20, 16 << 20)],
        )
    }
}

impl Command for SetTotal {
    const MODULE: &'static str = Orders::NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = i64;
    type Output = i64;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        cents: i64,
    ) -> cellule_runtime::Result<CommandResult<i64>> {
        if cents < 0 {
            return Ok(CommandResult::Rejected(cents));
        }
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO totals VALUES (1, ?1) ON CONFLICT(id) DO UPDATE SET cents=excluded.cents".into(),
                parameters: vec![SqlValue::Integer(cents)],
            }],
        })?;
        Ok(CommandResult::Success(cents))
    }
}

impl Query for ReadTotal {
    const MODULE: &'static str = Orders::NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = i64;

    fn execute(context: &mut QueryContext<'_>, (): ()) -> cellule_runtime::Result<i64> {
        let results = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT cents FROM totals WHERE id=1".into(),
                parameters: vec![],
            }],
        })?;
        match results.first().map(|result| result.rows.as_slice()) {
            Some([]) => Ok(0),
            Some([row]) => match row.as_slice() {
                [SqlValue::Integer(cents)] => Ok(*cents),
                _ => Err(cellule_runtime::Error::Control("invalid total row")),
            },
            _ => Err(cellule_runtime::Error::Control("invalid total result")),
        }
    }
}

const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
        schema_min: 1,
        schema_max: 1,
        input_limit: 1024,
        output_limit: 1024,
    }
}

pub fn source_digest() -> Digest {
    Digest::from_bytes(*blake3::hash(include_bytes!("application.rs")).as_bytes())
}
