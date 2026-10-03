//! A local Axum orders service backed by one durable SQL Cell.
//!
//! Run: `cargo run -p cellule-axum --example sql --locked`
//!
//!   POST /orders -> scoped Cellule extractor -> SQL command -> publication
//!   command receipt -> verified SELECT -> CellJson order + receipt
//!   GET /orders/{id} -> owner-ordered SELECT -> CellJson optional order
//!   Ctrl-C -> drain HTTP handlers -> drain runtime and SQLite workers
//!
//! In-memory objects and temporary SQLite files make this a local tutorial.
//! The application owns routes, authorization, request identity, and shutdown.

mod support;

use axum::{
    Json, Router,
    extract::Path as HttpPath,
    http::StatusCode,
    routing::{get, post},
};
use cellule_app::ApplicationHandle;
use cellule_axum::{CellJson, Cellule, HttpError, MinimumReceipt};
use cellule_runtime::{
    Error, MutationIdentity,
    identity::RequestId,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
};
use serde::{Deserialize, Serialize};
use support::{ExampleResult, ORDERS, Orders, OrdersApp};
use uuid::Uuid;

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
    MinimumReceipt(minimum): MinimumReceipt,
) -> Result<CellJson<Option<Order>>, HttpError> {
    read_order(&app, id, minimum).await
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

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let node = support::start().await?;
    let result: ExampleResult<()> = async {
        let router = Router::new()
            .route("/orders", post(create_order))
            .route("/orders/{id}", get(get_order))
            .with_state(node.app.clone());
        // This local fixture binds only loopback. A product installs its own
        // authentication and authorization before exposing these handlers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
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
    let shutdown = node.runtime.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}
