use axum::{
    Json,
    extract::{FromRequest, Request, rejection::JsonRejection},
    http::StatusCode,
};
use cellule_runtime::{MutationIdentity, identity::RequestId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::HttpError;

/// Caller-created mutation identity, retained unchanged across attempts.
///
/// Runtime preparation is the canonical authority for lifetime validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MutationIdentityDto {
    /// Stable identity of one logical command.
    pub request_id: Uuid,
    /// Original issuance time in Unix milliseconds.
    pub issued_at_ms: i64,
    /// Original expiry time in Unix milliseconds.
    pub expires_at_ms: i64,
}

impl From<MutationIdentityDto> for MutationIdentity {
    fn from(value: MutationIdentityDto) -> Self {
        Self {
            request_id: RequestId::from_bytes(*value.request_id.as_bytes()),
            issued_at_ms: value.issued_at_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}

impl From<MutationIdentity> for MutationIdentityDto {
    fn from(value: MutationIdentity) -> Self {
        Self {
            request_id: Uuid::from_bytes(*value.request_id.as_bytes()),
            issued_at_ms: value.issued_at_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}

/// Standard JSON command envelope: `{"identity": {...}, "input": ...}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBody<T> {
    /// Caller-created identity, never refreshed by the adapter.
    pub identity: MutationIdentityDto,
    /// Application-selected public command input.
    pub input: T,
}

/// Body extractor returning a native identity and typed command input.
///
/// Honors Axum's configured body limit. JSON failures share [`HttpError`]'s
/// safe public envelope and retain their original rejection as the source.
#[derive(Debug)]
pub struct MutationJson<T> {
    /// Exact identity supplied by the caller.
    pub identity: MutationIdentity,
    /// Deserialized input; domain validation belongs to the operation.
    pub input: T,
}

impl<S, T> FromRequest<S> for MutationJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = HttpError;
    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(body) = Json::<MutationBody<T>>::from_request(request, state)
            .await
            .map_err(json_error)?;
        Ok(Self {
            identity: body.identity.into(),
            input: body.input,
        })
    }
}

pub(crate) fn json_error(error: JsonRejection) -> HttpError {
    let (code, message) = match error.status() {
        StatusCode::PAYLOAD_TOO_LARGE => (
            "body_too_large",
            "Request body exceeds the configured limit.",
        ),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            ("unsupported_media_type", "A JSON content type is required.")
        }
        _ => ("invalid_request", "Invalid JSON request."),
    };
    HttpError::request(error.status(), code, message, error)
}
