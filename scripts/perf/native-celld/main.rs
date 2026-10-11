//! Native Rust HTTP workload; never constructs a V8 isolate or calls Node.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// This embedding owns host scheduling outside celld's execution boundary.
#![allow(clippy::disallowed_methods)]
mod ownership;
mod sql;

use anyhow::{anyhow, ensure, Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use celld::{ltx_repl::LtxRepl, replication::ActivationOptions, storage};
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::{Arc, Mutex};

fn log(message: std::fmt::Arguments<'_>) -> Result<()> {
    writeln!(std::io::stdout().lock(), "{message}")?;
    Ok(())
}

struct Cell {
    id: String,
    shard: usize,
    epoch: u64,
    storage: Arc<Mutex<storage::Cells>>,
    gate: tokio::sync::Mutex<()>,
}
struct App {
    cells: Vec<Option<Arc<Cell>>>,
    repl: Arc<LtxRepl>,
    authority: Arc<ownership::Authority>,
    total: usize,
    index: usize,
}

impl App {
    fn cell(&self, id: i64) -> Result<Arc<Cell>> {
        let shard = usize::try_from(id)? % self.total;
        self.cells[shard].clone().context("wrong owner node")
    }
    async fn call(&self, id: i64, command: Option<Value>) -> Result<Value> {
        let cell = self.cell(id)?;
        let _gate = cell.gate.lock().await;
        self.authority.check()?;
        let writing = command.is_some();
        let executing = cell.clone();
        let (reply, position) = tokio::task::spawn_blocking(move || {
            let local = executing
                .storage
                .lock()
                .map_err(|_| anyhow!("storage lock poisoned"))?;
            let _turn = local.install();
            let reply = match command {
                Some(body) => sql::write(&executing.id, &body)?,
                None => sql::read(&executing.id, id)?,
            };
            Ok::<_, anyhow::Error>((reply, storage::write_position(&executing.id)?))
        })
        .await??;
        if writing {
            let position = position.context("no committed position")?;
            let (_, proof) = self
                .repl
                .await_durable(&cell.id, cell.epoch, position)
                .await?;
            ensure!(
                matches!(proof, celld_logic::ProofSource::Bucket),
                "ACK lacks bucket proof"
            );
        }
        // Never return a late success after authority expires during an await.
        self.authority.check()?;
        Ok(reply)
    }

    async fn drain(&self) -> Result<()> {
        for cell in self.cells.iter().flatten() {
            let local = cell.clone();
            let reply = tokio::task::spawn_blocking(move || {
                let storage = local
                    .storage
                    .lock()
                    .map_err(|_| anyhow!("storage lock poisoned"))?;
                let _turn = storage.install();
                let reply = sql::read(&local.id, 0)?;
                celld::storage::close(&local.id);
                Ok::<_, anyhow::Error>(reply)
            })
            .await??;
            self.authority.check()?;
            self.repl.release(&cell.id, cell.epoch).await?;
            ensure!(
                matches!(
                    self.authority
                        .store
                        .release_owner(&cell.id, cell.epoch)
                        .await?,
                    celld_logic::CasOutcome::Applied
                ),
                "owner release rejected"
            );
            log(format_args!(
                "Cell drained: {}",
                json!({"cell":cell.id,"shard":cell.shard,"epoch":cell.epoch,"commit_sequence":reply["receipt"]["commit_sequence"]})
            ))?;
        }
        Ok(())
    }
}

type HttpResult = std::result::Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)>;
fn failure(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    let status = if error.to_string() == "request identity conflict" {
        StatusCode::CONFLICT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (
        status,
        Json(json!({"error":format!("{error:#}"),"outcome":"unconfirmed"})),
    )
}
async fn write(State(app): State<Arc<App>>, Json(body): Json<Value>) -> HttpResult {
    let id = body["id"]
        .as_i64()
        .ok_or_else(|| failure(anyhow!("missing id")))?;
    Ok((
        StatusCode::CREATED,
        Json(app.call(id, Some(body)).await.map_err(failure)?),
    ))
}
async fn read(State(app): State<Arc<App>>, Path(id): Path<i64>) -> HttpResult {
    Ok((
        StatusCode::OK,
        Json(app.call(id, None).await.map_err(failure)?),
    ))
}
async fn topology(State(app): State<Arc<App>>) -> Json<Value> {
    Json(json!({"total_cells":app.total,"node_count":3,"node_index":app.index}))
}

async fn run() -> Result<()> {
    let total: usize = std::env::var("CELLULE_AXUM_CELLS")?.parse()?;
    let index: usize = std::env::var("CELLULE_AXUM_NODE_INDEX")?.parse()?;
    let address = std::env::var("CELLULE_AXUM_BIND")?;
    ensure!(total >= 3 && index < 3, "invalid placement");
    let (authority, lease_task, stop_lease) = ownership::Authority::start(&address).await?;
    let repl = Arc::new(LtxRepl::native_http_start(std::path::Path::new(
        "/scratch/cells",
    ))?);
    let result = async {
        let started = std::time::Instant::now();
        let accepted: Vec<_> = stream::iter((index..total).step_by(3).map(|shard| {
            let repl = repl.clone();
            let authority = authority.clone();
            async move {
                let id = format!("{:x}", Sha256::digest(format!("native-http-cell-{shard}")));
                let (epoch, fresh, took_over) = authority.acquire(&id).await?;
                let activation = repl
                    .activate(ActivationOptions {
                        cell: &id,
                        epoch,
                        fresh,
                        took_over,
                        resume_local: false,
                        prior: None,
                    })
                    .await?;
                ensure!(
                    activation.restored != fresh,
                    "unexpected fresh/restore outcome"
                );
                let cell = Arc::new(Cell {
                    id,
                    shard,
                    epoch,
                    storage: Arc::new(Mutex::new(storage::Cells::default())),
                    gate: tokio::sync::Mutex::new(()),
                });
                let opening = cell.clone();
                tokio::task::spawn_blocking(move || {
                    let local = opening
                        .storage
                        .lock()
                        .map_err(|_| anyhow!("storage lock poisoned"))?;
                    let _turn = local.install();
                    celld::native_http_open(
                        &opening.id,
                        activation.path.to_str().context("non-UTF8 SQLite path")?,
                        epoch,
                        activation.vfs.as_deref(),
                    )?;
                    sql::migrate(&opening.id)?;
                    Ok::<_, anyhow::Error>(())
                })
                .await??;
                Ok::<_, anyhow::Error>((cell, !fresh, took_over))
            }
        }))
        .buffer_unordered(8)
        .collect()
        .await;
        // Join every accepted activation before propagating a source error.
        let activated = accepted.into_iter().collect::<Result<Vec<_>>>()?;
        log(format_args!(
            "Cells startup: {}; restored: {}",
            activated.len(),
            activated
                .iter()
                .filter(|(_, restored, _)| *restored)
                .count()
        ))?;
        log(format_args!(
            "Cells taken over: {}",
            activated.iter().filter(|(_, _, taken)| *taken).count()
        ))?;
        log(format_args!(
            "Cells activation milliseconds: {}",
            started.elapsed().as_millis()
        ))?;
        let mut cells = vec![None; total];
        for (cell, _, _) in activated {
            let shard = cell.shard;
            cells[shard] = Some(cell);
        }
        let app = Arc::new(App {
            cells,
            repl: repl.clone(),
            authority,
            total,
            index,
        });
        let router = Router::new()
            .route("/orders", post(write))
            .route("/orders/:id", get(read))
            .route("/debug/topology", get(topology))
            .route(
                "/debug/metrics",
                get(|| async { Json(json!({"implementation":"celld-native-storage-ltx"})) }),
            )
            .with_state(app.clone());
        let serving = async {
            let listener = tokio::net::TcpListener::bind(&address).await?;
            log(format_args!("Orders service: http://{address}"))?;
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let drained = app.drain().await;
        serving?;
        drained?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Even a failed startup/serve path joins owned replication and lease work.
    let closed = repl.native_http_shutdown().await;
    let _ = stop_lease.send(());
    let lease = lease_task.await;
    result?;
    closed?;
    lease??;
    Ok(())
}
fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    celld::asyncrt::set_host_handle(runtime.handle().clone());
    runtime.block_on(run())
}
