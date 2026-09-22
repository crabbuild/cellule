//! One local Cell that commits an order and reads its published total.

use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogEntry, CatalogRole, CellAuthority, CellCatalog,
    CellClient, CellModule, CellRuntime, CellStorageLayout, CellTarget, Digest, Error,
    IncarnationId, MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor,
    NamespaceId, OperationDescriptor, Owner, RegistryBuilder, RequestId, SessionId, SqlBatch,
    SqlModule, SqlStatement, SqlValue, SqlWorkerPool, TenantId, partition_for_shard, register_sql,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const ORDERS: NamespaceId = NamespaceId::from_bytes([1; 16]);
const SCHEMA: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, total_cents INTEGER NOT NULL)";
const COMMANDS: [OperationDescriptor; 1] = [operation(1)];
const QUERIES: [OperationDescriptor; 1] = [operation(2)];

struct Orders;

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
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("orders.rs")).as_bytes(),
            ),
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
        register_sql::<Self>(registry)
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

struct OrdersApp;

impl CellApplication for OrdersApp {
    const NAME: &'static str = "orders-example";

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(OrdersApp::compile(BuildDescriptor {
        source_revision: "local-orders-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([2; 16]);
    let application_id = ApplicationId::from_bytes([3; 16]);
    let target = CellTarget::new(tenant, application_id, ORDERS, &partition_for_shard(0))?;
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(
        store,
        Path::from("orders-example"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(Orders::NAME)
        .ok_or(Error::Registry("orders module is missing"))?;
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
    let typed = ApplicationHandle::<OrdersApp>::new(client, application, tenant, application_id);
    let sql = typed.sql::<Orders>(target)?;
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let committed = sql
        .batch(
            MutationIdentity {
                request_id: RequestId::from_bytes([6; 16]),
                issued_at_ms: now_ms,
                expires_at_ms: now_ms + 60_000,
            },
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO orders (id, total_cents) VALUES (?1, ?2)".into(),
                    parameters: vec![SqlValue::Integer(42), SqlValue::Integer(1_999)],
                }],
            },
        )
        .await?;
    let observed = sql
        .query(
            Some(committed.receipt),
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT total_cents FROM orders WHERE id = ?1".into(),
                    parameters: vec![SqlValue::Integer(42)],
                }],
            },
        )
        .await?;
    if observed.output[0].rows != vec![vec![SqlValue::Integer(1_999)]] {
        return Err(Error::Control("published order total differs").into());
    }
    println!("order 42 total: 1999 cents");
    runtime.shutdown().await?;
    Ok(())
}
