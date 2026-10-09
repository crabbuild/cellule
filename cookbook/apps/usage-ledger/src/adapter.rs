use crate::{CloseRequest, engine::CloseEngine, wire};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, Response, StatusCode, header},
    routing::post,
};
use cellule_cookbook_support::LocalNode;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct AdapterState {
    engine: CloseEngine,
    token: String,
}

fn response(status: StatusCode, bytes: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

async fn close(
    State(state): State<AdapterState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {}", state.token));
    if !authorized {
        return response(
            StatusCode::UNAUTHORIZED,
            br#"{"error":"unauthorized"}"#.to_vec(),
        );
    }
    let request: CloseRequest = match wire::decode(&body, 4096) {
        Ok(request) => request,
        Err(source) => {
            tracing::warn!(%source,"invalid usage-ledger close Activity input");
            return response(
                StatusCode::BAD_REQUEST,
                br#"{"error":"invalid input"}"#.to_vec(),
            );
        }
    };
    match state.engine.close(request.period_id).await {
        Ok(completion) => match wire::encode(&completion, crate::MAX_WIRE_BYTES) {
            Ok(bytes) => response(StatusCode::OK, bytes),
            Err(source) => {
                tracing::error!(%source,"usage-ledger Activity result failed to encode");
                response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    br#"{"error":"close failed"}"#.to_vec(),
                )
            }
        },
        Err(source) => {
            tracing::error!(%source,"usage-ledger close engine failed");
            response(
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"error":"close failed"}"#.to_vec(),
            )
        }
    }
}

/// Starts the loopback-only internal Activity adapter as an owned node task.
pub(crate) async fn spawn(
    node: &LocalNode,
    engine: CloseEngine,
    endpoint: &str,
) -> Result<(), crate::BoxError> {
    let address = endpoint_address(endpoint)?;
    let token = crate::activity::adapter_token()?;
    if token.is_empty() || token.len() > 256 || !token.bytes().all(|value| value.is_ascii_graphic())
    {
        return Err("invalid usage-ledger adapter token".into());
    }
    let listener = TcpListener::bind(address).await?;
    node.spawn_worker(move |cancel: CancellationToken| async move {
        let app = Router::new()
            .route("/close", post(close))
            .with_state(AdapterState { engine, token });
        axum::serve(listener, app)
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await?;
        Ok::<_, std::io::Error>(())
    })?;
    Ok(())
}

fn endpoint_address(endpoint: &str) -> Result<SocketAddr, crate::BoxError> {
    let url = url::Url::parse(endpoint)?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || url.path() != "/"
        || url.as_str() != endpoint
    {
        return Err("usage-ledger adapter must listen on canonical numeric loopback".into());
    }
    Ok(SocketAddr::from((
        [127, 0, 0, 1],
        url.port().ok_or("adapter port absent")?,
    )))
}
