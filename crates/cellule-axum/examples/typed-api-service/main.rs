//! Typed registration, authorization, OpenAPI, evidence custody and recovery.
//! Run: cargo run -p cellule-axum --example typed-api-service --features openapi --locked

mod recovery;
mod support;

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, Path, Request, State},
    http::{StatusCode, request::Parts},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use cellule_app::ApplicationHandle;
use cellule_axum::{
    CellApi, CellEndpoint, CellJson, CommandEndpoint, EndpointSpec, HttpError, RequestCellule,
    utoipa,
};
use cellule_runtime::{
    CellTarget, PreparedCommand, Receipt, Resolution,
    cell::executor::StoredOutcome,
    client::{Committed, InvocationError},
    codec::{BoundedDecoder, WireValue},
};
use recovery::Journal;
use serde_json::json;
use support::{ExampleResult, ORDERS, OrdersApp, ReadTotal, SetTotal};
use utoipa::OpenApi as _;
use uuid::Uuid;

#[derive(utoipa::OpenApi)]
#[openapi(paths(scope, recover, ready))]
struct ManualRoutes;

#[derive(serde::Serialize, utoipa::ToSchema)]
struct AuthorizedScope {
    cell: [u8; 32],
}

#[derive(Clone)]
struct ServiceState {
    app: ApplicationHandle<OrdersApp>,
    journal: Journal,
}

#[derive(Clone)]
struct Authorized {
    app: ApplicationHandle<OrdersApp>,
    target: CellTarget,
    journal: Journal,
}

// In a product, verify a session/token and look up its permitted tenant. This
// local credential grants exactly the fixture tenant and Orders target; request
// headers cannot pick another tenant. Never expose this tutorial token publicly.
async fn authorize(
    State(state): State<ServiceState>,
    mut request: Request,
    next: Next,
) -> Response {
    let values = request.headers().get_all("authorization");
    if values.iter().count() != 1
        || values.iter().next().and_then(|value| value.to_str().ok()) != Some("Bearer local-orders")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let target = match state.app.target_for_scope(ORDERS, b"orders") {
        Ok(target) => target,
        Err(error) => return HttpError::from(error).into_response(),
    };
    request.extensions_mut().insert(state.app.clone());
    request.extensions_mut().insert(Authorized {
        app: state.app,
        target,
        journal: state.journal,
    });
    next.run(request).await
}

impl<S: Send + Sync> FromRequestParts<S> for Authorized {
    type Rejection = HttpError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, HttpError> {
        let Extension(context) = Extension::<Self>::from_request_parts(parts, state)
            .await
            .map_err(HttpError::internal)?;
        Ok(context)
    }
}

impl CellEndpoint<OrdersApp> for Authorized {
    fn application(&self) -> &ApplicationHandle<OrdersApp> {
        &self.app
    }
    fn target(&self) -> &CellTarget {
        &self.target
    }
}

impl CommandEndpoint<OrdersApp, SetTotal> for Authorized {
    async fn retain(&self, prepared: &PreparedCommand<SetTotal>) -> Result<(), HttpError> {
        self.journal.retain(prepared).await
    }
}

#[utoipa::path(
    get, path = "/scope", operation_id = "authorizedScope",
    responses(
        (status = 200, body = AuthorizedScope, description = "Cell selected by the authenticated scope"),
        (status = 401, description = "Application authentication failed")
    ),
    security(("bearerAuth" = []))
)]
async fn scope(app: RequestCellule<OrdersApp>) -> Result<Json<AuthorizedScope>, HttpError> {
    let target = app.target_for_scope(ORDERS, b"orders")?;
    Ok(Json(AuthorizedScope {
        cell: *target.cell_id().as_bytes(),
    }))
}

#[utoipa::path(
    post, path = "/recovery/{request_id}", operation_id = "recoverTotal",
    params(("request_id" = Uuid, Path, description = "Original command request ID")),
    responses(
        (status = 200, body = CellJson<i64>, description = "Committed original outcome or safely replayed command"),
        (status = 404, description = "No retained evidence in the authorized Cell"),
        (status = 409, description = "Durable rejection, request conflict, or expired original command"),
        (status = 503, description = "Outcome remains unknown or the node is unavailable")
    ),
    security(("bearerAuth" = []))
)]
async fn recover(context: Authorized, Path(request): Path<Uuid>) -> Result<Response, HttpError> {
    let Some((snapshot, input)) = context
        .journal
        .load(*context.target.cell_id().as_bytes(), request)
        .await?
    else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    // Restore enforces the same authorized tenant, operation, codec, original
    // incarnation, digest and bounds. Custody/authentication are our responsibility.
    let restored = context.app.restore_command::<SetTotal>(snapshot, input)?;
    let mut response = match context.app.resolve(restored.evidence()).await? {
        Resolution::Committed(outcome) => {
            let receipt = Receipt {
                cell: restored.evidence().target().cell_id(),
                incarnation: restored.evidence().incarnation(),
                commit_sequence: outcome.commit_sequence(),
            };
            let reply = CellJson {
                output: outcome.result(),
                receipt,
            }
            .try_map(|bytes| {
                let mut decoder = BoundedDecoder::new(bytes, 1024)?;
                let output = i64::decode(&mut decoder)?;
                decoder.finish()?;
                Ok::<_, cellule_runtime::codec::CodecError>(output)
            })?;
            match outcome {
                StoredOutcome::Success { .. } => reply.into_response(),
                StoredOutcome::Rejected { .. } => {
                    HttpError::from(InvocationError::Rejected(Box::new(Committed {
                        output: reply.output,
                        receipt,
                    })))
                    .into_response()
                }
            }
        }
        // Only authoritative absence permits execution with the original bytes.
        // execute() also checks original expiry and the pinned owner incarnation.
        Resolution::Absent => CellJson::from(restored.execute().await?).into_response(),
        Resolution::Unknown => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"state":"unknown"})),
        )
            .into_response(),
        Resolution::Expired => {
            (StatusCode::CONFLICT, Json(json!({"state":"expired"}))).into_response()
        }
    };
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

#[utoipa::path(
    get, path = "/ready", operation_id = "readiness",
    responses(
        (status = 204, description = "Initialized Cell can answer a query"),
        (status = 503, description = "Cell is unavailable")
    )
)]
async fn ready(State(state): State<ServiceState>) -> StatusCode {
    let Ok(target) = state.app.target_for_scope(ORDERS, b"orders") else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    match state.app.query::<ReadTotal>(&target, None, ()).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let node = support::start().await?;
    let result: ExampleResult<()> = async {
        let state = ServiceState {
            app: node.app.clone(),
            journal: Journal::open(node._files.path().join("recovery.sqlite"))?,
        };
        let (routes, mut document) = CellApi::<OrdersApp, ServiceState>::new(node.app.compiled())?
            .command::<SetTotal, Authorized>(
                ORDERS,
                EndpointSpec::new("/total", "setTotal")
                    .description("Set the total with a caller-created mutation identity. Retain the exact original body for retries and recovery. Negative totals are durable rejections."),
            )?
            .query::<ReadTotal, Authorized>(
                ORDERS,
                EndpointSpec::new("/total/read", "readTotal")
                    .description("Read the total using JSON null as the body. Supply x-cellule-receipt to require observation of a previous write."),
            )?
            .into_router()
            .split_for_parts();
        document.merge(ManualRoutes::openapi());
        document.info.title = "Typed Orders API".into();
        document.info.description = Some(
            "Local example: use Authorize with token `local-orders` (without the Bearer prefix). Queries take JSON `null`. Mutations require a UUID request_id and current issued_at_ms/expires_at_ms Unix timestamps in milliseconds; use a 60-second window and keep the same identity and input for every retry. Restarting this example resets its state.".into(),
        );
        // Authorization metadata belongs to the app, beside its middleware.
        use cellule_axum::utoipa::openapi::{
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
                operation.responses.responses.insert(
                    "401".into(),
                    ResponseBuilder::new()
                        .description("Application authentication failed.")
                        .build()
                        .into(),
                );
            }
        }
        let protected = routes
            .route("/scope", get(scope))
            .route("/recovery/{request_id}", post(recover))
            .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
        let router = Router::new()
            .merge(protected)
            .route("/ready", get(ready))
            .route("/docs", get(|| async { Html(include_str!("../openapi-ui.html")) }))
            .route(
                "/openapi.json",
                get(move || {
                    let document = document.clone();
                    async move { Json(document) }
                }),
            )
            .layer(DefaultBodyLimit::max(4096))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3001").await?;
        println!(
            "Typed API service: http://{} (API docs: /docs; Ctrl-C to drain)",
            listener.local_addr()?
        );
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = signal_tx.send(tokio::signal::ctrl_c().await);
            })
            .await?;
        signal_rx.await??;
        Ok(())
    }
    .await;
    // Drain accepted HTTP before runtime/SQLite. Setup and serve errors also clean up.
    let shutdown = node.runtime.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}
