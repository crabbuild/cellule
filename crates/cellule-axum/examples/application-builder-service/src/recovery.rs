use axum::{
    Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cellule_axum::{CellEndpoint, CellJson, HttpError, utoipa};
use cellule_runtime::{
    Receipt, Resolution,
    cell::executor::StoredOutcome,
    client::{Committed, InvocationError},
    codec::{BoundedDecoder, WireValue},
};
use uuid::Uuid;

use crate::{application::SetTotal, auth::WriteOrders};

#[utoipa::path(
    post,
    path = "/orders/total/resolve/{request_id}",
    operation_id = "resolveTotal",
    params(("request_id" = Uuid, Path, description = "Original command request ID")),
    responses(
        (status = 200, body = CellJson<i64>, description = "Committed original outcome or safely replayed command"),
        (status = 404, description = "No retained evidence in the authorized Cell"),
        (status = 409, description = "Original command expired or conflicts"),
        (status = 503, description = "Outcome remains unknown")
    ),
    security(("bearerAuth" = []))
)]
pub async fn resolve(
    context: WriteOrders,
    Path(request): Path<Uuid>,
) -> Result<Response, HttpError> {
    let Some((snapshot, input)) = context
        .journal()
        .load(*context.target().cell_id().as_bytes(), request)
        .await?
    else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let restored = context
        .application()
        .restore_command::<SetTotal>(snapshot, input)?;
    let mut response = match context.application().resolve(restored.evidence()).await? {
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
        // Only authoritative absence permits replay of these original bytes.
        Resolution::Absent => CellJson::from(restored.execute().await?).into_response(),
        Resolution::Unknown => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"state": "unknown"})),
        )
            .into_response(),
        Resolution::Expired => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"state": "expired"})),
        )
            .into_response(),
    };
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}
