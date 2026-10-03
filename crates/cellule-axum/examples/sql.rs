//! A local Axum orders service backed by one durable SQL Cell.
//!
//! Run: `cargo run -p cellule-axum --example sql --locked`
//!
//!   POST /orders -> scoped Cellule extractor -> SQL command -> publication
//!   command receipt -> verified SELECT -> CellJson order + receipt
//!   GET /orders/{id} -> owner-ordered SELECT -> CellJson optional order
//!   Ctrl-C -> drain HTTP handlers -> drain runtime and SQLite workers
//!
//! Uses in-memory objects by default; CELLULE_TEST_ENDPOINT selects real S3.
//! SQLite files are always temporary, including after a cold restart.
//! The application owns routes, authorization, request identity, and shutdown.

use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::Path as HttpPath,
    http::StatusCode,
    routing::{get, post},
};
use cellule_app::{ApplicationHandle, CellApplication, CellType};
use cellule_axum::{CellJson, Cellule, HttpError};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::cell::catalog::{CatalogEntry, CellCatalog};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::identity::{IncarnationId, RequestId};
use cellule_runtime::ltx::CellStorageLayout;
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue, register_sql};
use cellule_runtime::registry::OperationDescriptor;
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogRole, CellClient, CellModule, CellRuntime, CellTarget,
    Digest, Error, MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor,
    NamespaceId, RegistryBuilder, SessionId, SqlModule, SqlWorkerPool, TenantId,
    partition_for_shard,
};
use cellule_store::{ObjectStoreCredentials, Store, build_explicit_store, probe_storage};
use object_store::{memory::InMemory, path::Path};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const ORDERS: NamespaceId = NamespaceId::from_bytes([1; 16]);
const SCHEMA: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, total_cents INTEGER NOT NULL)";
const COMMANDS: [OperationDescriptor; 1] = [operation(1)];
const QUERIES: [OperationDescriptor; 1] = [operation(2)];
type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;

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
            source_digest: Digest::from_bytes(*blake3::hash(include_bytes!("sql.rs")).as_bytes()),
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

#[derive(Deserialize)]
struct CreateOrder {
    request_id: Uuid,
    issued_at_ms: i64,
    expires_at_ms: i64,
    id: i64,
    total_cents: i64,
}

#[derive(Serialize)]
struct Order {
    id: i64,
    total_cents: i64,
}

async fn create_order(
    app: Cellule<OrdersApp>,
    Json(input): Json<CreateOrder>,
) -> Result<(StatusCode, CellJson<Order>), HttpError> {
    let target = app.target_for_scope(ORDERS, b"orders")?;
    let sql = app.sql::<Orders>(target)?;
    // Retries must retain all three identity fields and the exact input. The
    // service accepts them explicitly instead of inventing an ID per attempt.
    let committed = sql
        .batch(
            MutationIdentity {
                request_id: RequestId::from_bytes(*input.request_id.as_bytes()),
                issued_at_ms: input.issued_at_ms,
                expires_at_ms: input.expires_at_ms,
            },
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO orders (id, total_cents) VALUES (?1, ?2)".into(),
                    parameters: vec![
                        SqlValue::Integer(input.id),
                        SqlValue::Integer(input.total_cents),
                    ],
                }],
            },
        )
        .await?;
    // A success reply follows publication and a read proving this receipt.
    let observed = read_order(&app, input.id, Some(committed.receipt)).await?;
    let order = observed
        .output
        .ok_or(Error::Control("committed order is missing"))?;
    Ok((
        StatusCode::CREATED,
        CellJson {
            output: order,
            receipt: committed.receipt,
        },
    ))
}

async fn get_order(
    app: Cellule<OrdersApp>,
    HttpPath(id): HttpPath<i64>,
) -> Result<CellJson<Option<Order>>, HttpError> {
    read_order(&app, id, None).await
}

async fn read_order(
    app: &ApplicationHandle<OrdersApp>,
    id: i64,
    minimum: Option<cellule_runtime::Receipt>,
) -> Result<CellJson<Option<Order>>, HttpError> {
    let target = app.target_for_scope(ORDERS, b"orders")?;
    let observed = app
        .sql::<Orders>(target)?
        .query(
            minimum,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT total_cents FROM orders WHERE id = ?1".into(),
                    parameters: vec![SqlValue::Integer(id)],
                }],
            },
        )
        .await?;
    let [result] = observed.output.as_slice() else {
        return Err(Error::Control("expected one query result").into());
    };
    let output = match result.rows.as_slice() {
        [] => None,
        [row] => {
            let [SqlValue::Integer(total_cents)] = row.as_slice() else {
                return Err(Error::Control("expected an integer order total").into());
            };
            Some(Order {
                id,
                total_cents: *total_cents,
            })
        }
        _ => return Err(Error::Control("expected at most one order").into()),
    };
    Ok(CellJson {
        output,
        receipt: observed.receipt,
    })
}

async fn example_storage() -> ExampleResult<(Store, Path)> {
    let endpoint = match std::env::var("CELLULE_TEST_ENDPOINT") {
        Ok(endpoint) => endpoint,
        Err(std::env::VarError::NotPresent) => {
            if std::env::var_os("CELLULE_TEST_BUCKET").is_some()
                || std::env::var_os("CELLULE_TEST_PREFIX").is_some()
            {
                return Err("S3 bucket/prefix requires CELLULE_TEST_ENDPOINT".into());
            }
            return Ok((
                Store::new(Arc::new(InMemory::new())),
                Path::from("axum-orders-example"),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    let prefix = Path::parse(std::env::var("CELLULE_TEST_PREFIX")?)?;
    if prefix.as_ref().is_empty() {
        return Err("CELLULE_TEST_PREFIX must be nonempty and isolated to this application".into());
    }
    let store = build_explicit_store(
        &std::env::var("CELLULE_TEST_BUCKET")?,
        ObjectStoreCredentials::Aws {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
            region: std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into()),
        },
        Some(&endpoint),
        true,
    )?;
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let report = probe_storage(&store, &prefix, now_ms).await?;
    if !report.passed() {
        return Err(format!(
            "storage capability probe failed: {:?}",
            report.failed_checks()
        )
        .into());
    }
    println!("Storage probe: passed all six checks; prefix: {prefix}");
    Ok((store, prefix))
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let bind: SocketAddr = std::env::var("CELLULE_AXUM_BIND")
        .unwrap_or_else(|_| "127.0.0.1:3000".into())
        .parse()?;
    if !bind.ip().is_loopback() {
        return Err("this example requires a loopback HTTP listener".into());
    }
    let application = Arc::new(OrdersApp::compile(BuildDescriptor {
        source_revision: "local-axum-orders-example".into(),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../../../Cargo.lock")).as_bytes(),
        ),
    })?);
    let tenant = TenantId::from_bytes([2; 16]);
    let application_id = ApplicationId::from_bytes([3; 16]);
    let target = CellTarget::new(tenant, application_id, ORDERS, &partition_for_shard(0))?;
    let (store, prefix) = example_storage().await?;
    let layout = CellStorageLayout::new(store, prefix, *application_id.as_bytes());
    let registry = application.registry();
    let code = registry
        .module_code(Orders::NAME)
        .ok_or(Error::Registry("orders module is missing"))?;
    // Publish the catalog entry and fenced owner before bootstrapping the Cell.
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let session = SessionId::from_bytes(*Uuid::now_v7().as_bytes());
    let owner = Owner {
        session,
        endpoint: "https://orders.local".into(),
    };
    let existing = authority.load(target.cell_id()).await?;
    let restoring = existing.is_some();
    let observed = match existing {
        Some(observed) => observed,
        None => {
            authority
                .create_initial(&proof, IncarnationId::from_bytes([4; 16]), owner.clone())
                .await?
        }
    };
    let incarnation = observed.value().incarnation;
    let files = tempfile::TempDir::new()?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let result: ExampleResult<()> = async {
        let replica = CellReplica::new(
            layout,
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )?;
        let destination = files.path().join("orders.sqlite");
        let handle = if restoring {
            // A drained owner released authority to Idle. This public API claims
            // it with a fresh session and verifies/restores the pinned S3 root.
            runtime
                .acquire_idle_restored(
                    proof,
                    replica,
                    authority.clone(),
                    observed,
                    destination,
                    owner,
                )
                .await?
        } else {
            runtime
                .bootstrap(
                    proof,
                    replica,
                    authority.clone(),
                    observed,
                    destination,
                    |transaction| {
                        transaction.execute_batch(SCHEMA)?;
                        Ok(())
                    },
                )
                .await?
        };
        let client = CellClient::local(registry, handle);
        let typed =
            ApplicationHandle::<OrdersApp>::new(client, application, tenant, application_id)?;
        let router = Router::new()
            .route("/orders", post(create_order))
            .route("/orders/{id}", get(get_order))
            .with_state(typed);
        // This local fixture binds only loopback. A product installs its own
        // authentication and authorization before exposing these handlers.
        let listener = tokio::net::TcpListener::bind(bind).await?;
        println!(
            "Cell startup: {}; SQLite directory: {}",
            if restoring {
                "restored"
            } else {
                "bootstrapped"
            },
            files.path().display()
        );
        println!(
            "Orders service: http://{} (Ctrl-C to drain)",
            listener.local_addr()?
        );
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let served = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = signal_tx.send(tokio::signal::ctrl_c().await);
            })
            .await;
        served?;
        signal_rx.await??;
        Ok(())
    }
    .await;
    // HTTP finishes accepted handlers before the runtime drains its workers.
    // Setup and serving failures also pass through this runtime cleanup.
    let shutdown = runtime.shutdown().await;
    result?;
    shutdown?;
    let drained = authority
        .load(target.cell_id())
        .await?
        .ok_or(Error::Fenced)?;
    if drained.value().state != cellule_runtime::control::ControlState::Idle
        || drained.value().owner.is_some()
    {
        return Err(Error::Control("shutdown did not release Cell authority").into());
    }
    let root = drained
        .value()
        .root
        .as_ref()
        .ok_or(Error::Control("drained Cell has no root"))?;
    println!(
        "Cell drained: idle; epoch: {}; commit_sequence: {}",
        drained.value().epoch,
        root.commit_sequence
    );
    Ok(())
}
