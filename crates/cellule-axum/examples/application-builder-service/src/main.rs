mod application;
mod auth;
mod journal;
mod recovery;
mod service;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    response::Html,
    routing::{get, post},
};
use cellule_app::CellApplication;
use cellule_axum::{CellApi, EndpointSpec, utoipa};
use cellule_cookbook_support::{NodeConfig, shutdown_signal};
use cellule_runtime::{ApplicationId, BuildDescriptor, Digest};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use utoipa::OpenApi as _;

use application::{ORDERS, OrdersApp, ReadTotal, SetTotal};
use auth::{ALPHA, BETA, ReadOrders, WriteOrders};
use journal::Journal;
use service::ServiceNode;

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

pub struct ServiceState {
    node: ServiceNode,
    journal: Journal,
}

#[derive(utoipa::OpenApi)]
#[openapi(paths(auth::ready, recovery::resolve))]
struct ManualRoutes;

async fn serve(state: Arc<ServiceState>) -> AppResult<()> {
    let (routes, mut document) =
        CellApi::<OrdersApp, Arc<ServiceState>>::new(state.node.application())?
            .command::<SetTotal, WriteOrders>(
                ORDERS,
                EndpointSpec::new("/orders/total", "setTotal")
                    .description("Set the authorized tenant's total with a caller-created mutation identity. Retain the exact original body for retries and recovery. Negative totals are durable rejections."),
            )?
            .query::<ReadTotal, ReadOrders>(
                ORDERS,
                EndpointSpec::new("/orders/total/read", "readTotal")
                    .description("Read the authorized tenant's total using JSON null as the body. Supply x-cellule-receipt to require observation of a previous write."),
            )?
            .into_router()
            .split_for_parts();
    document.merge(ManualRoutes::openapi());
    document.info.title = "Orders service".into();
    document.info.version = env!("CARGO_PKG_VERSION").into();
    document.info.description = Some(
        "Local example: use Authorize with token `alpha-writer` for read/write or `beta-reader` for read only (without the Bearer prefix). Queries take JSON `null`. Mutations require a UUID request_id and current issued_at_ms/expires_at_ms Unix timestamps in milliseconds; use a 60-second window and keep the same identity and input for every retry. Restarting this example resets its state.".into(),
    );

    // The same service that authenticates requests documents its security.
    use utoipa::openapi::{
        response::ResponseBuilder,
        security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme},
    };
    document
        .components
        .get_or_insert_with(Default::default)
        .add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
    for path in document.paths.paths.values_mut() {
        if let Some(operation) = path.post.as_mut() {
            operation.security = Some(vec![SecurityRequirement::new(
                "bearerAuth",
                Vec::<String>::new(),
            )]);
            for (code, description) in [
                ("401", "Authentication failed"),
                ("403", "Caller lacks write permission"),
            ] {
                operation.responses.responses.insert(
                    code.into(),
                    ResponseBuilder::new()
                        .description(description)
                        .build()
                        .into(),
                );
            }
        }
    }
    let router = Router::new()
        .merge(routes)
        .route(
            "/orders/total/resolve/{request_id}",
            post(recovery::resolve),
        )
        .route("/ready", get(auth::ready))
        .route(
            "/docs",
            get(|| async { Html(include_str!("../../openapi-ui.html")) }),
        )
        .route("/openapi.json", get(move || async move { Json(document) }))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(state);

    let bind = std::env::var("CELLULE_EXAMPLE_BIND").unwrap_or_else(|_| "127.0.0.1:3002".into());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!(
        "Orders service: http://{} (API docs: /docs)",
        listener.local_addr()?
    );
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let signal = shutdown_signal().await;
            println!("Stopping HTTP; waiting for accepted requests");
            let _ = signal_tx.send(signal);
        })
        .await?;
    signal_rx.await??;
    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    // Declare and compile once; the same artifact feeds node and OpenAPI.
    let compiled = Arc::new(OrdersApp::compile(BuildDescriptor {
        source_revision: format!("example-source:{:?}", application::source_digest()),
        cargo_lock_digest: Digest::from_bytes(
            *blake3::hash(include_bytes!("../Cargo.lock")).as_bytes(),
        ),
    })?);

    // This tutorial uses ephemeral authoritative storage and local credentials.
    // A real service supplies its durable Store and persistent state directory.
    let files = tempfile::tempdir()?;
    let journal = Journal::open(files.path().join("commands.sqlite"))?;
    let startup = ServiceNode::start(
        compiled,
        Store::new(Arc::new(InMemory::new())),
        NodeConfig {
            state_directory: files.path().join("node"),
            storage_prefix: Path::from("builder-service"),
            application_id: ApplicationId::from_bytes([7; 16]),
        },
        &[ALPHA, BETA],
    )
    .await;
    let node = match startup {
        Ok(node) => node,
        Err(error) => {
            let retained = files.keep();
            eprintln!(
                "startup failed; retained local evidence at {}",
                retained.display()
            );
            return Err(error);
        }
    };
    let state = Arc::new(ServiceState { node, journal });

    // This catches route construction, bind and serving errors as well as signals.
    let serving = serve(state.clone()).await;
    println!("Draining node with lease renewal still active");
    let shutdown = state.node.shutdown().await;
    if let Err(error) = &shutdown {
        eprintln!("node drain/withdrawal failed: {error}");
        let retained = files.keep();
        eprintln!("retained local evidence at {}", retained.display());
    } else {
        println!("Node drained; enrollment withdrawn");
    }
    serving?;
    shutdown?;
    Ok(())
}
