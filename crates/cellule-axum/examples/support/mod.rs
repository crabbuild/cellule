use std::sync::{Arc, OnceLock};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::cell::catalog::{CatalogEntry, CellCatalog};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::identity::IncarnationId;
use cellule_runtime::ltx::CellStorageLayout;
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue, register_sql};
use cellule_runtime::registry::OperationDescriptor;
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogRole, CellClient, CellModule, CellRuntime, CellTarget,
    Digest, Error, MigrationDescriptor, ModuleDescriptor, NamespaceDescriptor, NamespaceId,
    RegistryBuilder, SessionId, SqlModule, SqlWorkerPool, TenantId, partition_for_shard,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

pub const ORDERS: NamespaceId = NamespaceId::from_bytes([1; 16]);
const SCHEMA: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, total_cents INTEGER NOT NULL)";
const COMMANDS: [OperationDescriptor; 2] = [operation(1), operation(3)];
const QUERIES: [OperationDescriptor; 2] = [operation(2), operation(4)];
pub type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;

pub struct Orders;

impl SqlModule for Orders {
    const MODULE: &'static str = Self::NAME;
    const BATCH_COMMAND_ID: u32 = 1;
    const BATCH_QUERY_ID: u32 = 2;
}

impl CellModule for Orders {
    const NAME: &'static str = "orders";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: Digest::from_bytes(*blake3::hash(include_bytes!("mod.rs")).as_bytes()),
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
                name: Self::NAME,
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_sql::<Self>(registry)?;
        registry.bind_command::<SetTotal>()?;
        registry.bind_query::<ReadTotal>()
    }
}

/// A domain command with a public JSON-compatible input and output.
pub struct SetTotal;
impl cellule_runtime::Command for SetTotal {
    const MODULE: &'static str = "orders";
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = i64;
    type Output = i64;
    fn execute(
        context: &mut cellule_runtime::registry::CommandContext<'_, '_>,
        total: i64,
    ) -> cellule_runtime::Result<cellule_runtime::registry::CommandResult<i64>> {
        if total < 0 {
            return Ok(cellule_runtime::registry::CommandResult::Rejected(total));
        }
        context.sql(&SqlBatch { statements: vec![SqlStatement {
            sql: "INSERT INTO orders (id, total_cents) VALUES (1, ?1) ON CONFLICT(id) DO UPDATE SET total_cents=excluded.total_cents".into(),
            parameters: vec![SqlValue::Integer(total)],
        }] })?;
        Ok(cellule_runtime::registry::CommandResult::Success(total))
    }
}

/// An owner-ordered query, or a query under an explicitly configured read policy.
pub struct ReadTotal;
impl cellule_runtime::Query for ReadTotal {
    const MODULE: &'static str = "orders";
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = ();
    type Output = i64;
    fn execute(
        context: &mut cellule_runtime::registry::QueryContext<'_>,
        (): (),
    ) -> cellule_runtime::Result<i64> {
        let results = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT total_cents FROM orders WHERE id=1".into(),
                parameters: vec![],
            }],
        })?;
        match results.first().map(|result| result.rows.as_slice()) {
            Some([]) => Ok(0),
            Some([row]) => match row.as_slice() {
                [SqlValue::Integer(total)] => Ok(*total),
                _ => Err(Error::Control("invalid total row")),
            },
            _ => Err(Error::Control("invalid total result")),
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

pub struct OrdersApp;

impl CellApplication for OrdersApp {
    const NAME: &'static str = "axum-orders-example";

    fn register(builder: &mut cellule_app::ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(Orders)?;
        builder.cell_type(CellType::new(
            Orders::NAME,
            "orders",
            ORDERS,
            CatalogRole::Sql,
            1,
        )?)?;
        Ok(())
    }
}

pub struct ExampleNode {
    pub app: ApplicationHandle<OrdersApp>,
    pub runtime: CellRuntime,
    pub _files: tempfile::TempDir,
}

pub async fn start() -> ExampleResult<ExampleNode> {
    let application = Arc::new(OrdersApp::compile(BuildDescriptor {
        source_revision: "local-axum-orders-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([2; 16]);
    let application_id = ApplicationId::from_bytes([3; 16]);
    let target = CellTarget::new(tenant, application_id, ORDERS, &partition_for_shard(0))?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("axum-orders-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Orders::NAME)
        .ok_or(Error::Registry("orders module is missing"))?;
    // Publish the catalog entry and fenced owner before bootstrapping the Cell.
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([4; 16]);
    let session = SessionId::from_bytes([5; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://orders.local".into(),
            },
        )
        .await?;
    let files = tempfile::TempDir::new()?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let result: ExampleResult<ApplicationHandle<OrdersApp>> = async {
        let handle = runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    layout,
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    Limits::default(),
                )?,
                authority,
                observed,
                files.path().join("orders.sqlite"),
                |transaction| {
                    transaction.execute_batch(SCHEMA)?;
                    Ok(())
                },
            )
            .await?;
        let client = CellClient::local(registry, handle);
        let typed =
            ApplicationHandle::<OrdersApp>::new(client, application, tenant, application_id)?;
        Ok(typed)
    }
    .await;
    match result {
        Ok(app) => Ok(ExampleNode {
            app,
            runtime,
            _files: files,
        }),
        Err(error) => {
            let _ = runtime.shutdown().await;
            Err(error)
        }
    }
}
