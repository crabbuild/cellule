use std::{error::Error as StdError, fmt};

use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use cellule_runtime::{Error, client::InvocationError};
use serde::Serialize;

use crate::response::{ReceiptBody, hex};

/// An outcome-aware HTTP error that retains its original Rust source.
///
/// Convert runtime and typed invocation errors with `?` in a handler. Public
/// JSON contains a fixed `code` and `message`, plus a `request_id` for pending
/// commands or a `receipt` for published results. Source details are retained
/// for application logging and downcasting, but never included in the body.
///
/// `outcome_unknown` means resolve the original mutation before retrying.
/// `command_rejected` and `invalid_published_result` describe published
/// outcomes: do not issue a new command identity to recover their output.
/// Persist any recovery evidence your service needs before returning a reply.
#[derive(Debug)]
pub struct HttpError {
    status: StatusCode,
    body: Box<ErrorBody>,
    source: Box<dyn StdError + Send + Sync>,
}

impl HttpError {
    /// Returns the HTTP status chosen for this failure.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the machine-readable public error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.body.code
    }

    /// Returns the original error, including typed pending or published evidence.
    ///
    /// A converted `InvocationError<T>` can be recovered by downcasting this
    /// box to that same type. Conversion never discards its output or evidence.
    #[must_use]
    pub fn into_source(self) -> Box<dyn StdError + Send + Sync> {
        self.source
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, formatter)
    }
}

impl StdError for HttpError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<Error> for HttpError {
    fn from(error: Error) -> Self {
        let (status, body) = runtime_response(&error);
        Self {
            status,
            body: Box::new(body),
            source: Box::new(error),
        }
    }
}

impl<T: Send + Sync + 'static> From<InvocationError<T>> for HttpError {
    fn from(error: InvocationError<T>) -> Self {
        let (status, body) = match &error {
            InvocationError::NotStarted(source) => runtime_response(source),
            InvocationError::Pending(pending) => {
                let mut body = ErrorBody::unknown();
                body.request_id = Some(hex(pending.identity().request_id.as_bytes()));
                (StatusCode::SERVICE_UNAVAILABLE, body)
            }
            InvocationError::Rejected(committed) => {
                let mut body = ErrorBody::new("command_rejected", "Command was durably rejected.");
                body.receipt = Some(committed.receipt.into());
                (StatusCode::CONFLICT, body)
            }
            InvocationError::InvalidPublishedResult { receipt, .. } => {
                let mut body = ErrorBody::new(
                    "invalid_published_result",
                    "Published output could not be decoded; recover the original result before retrying.",
                );
                body.receipt = Some((*receipt).into());
                (StatusCode::INTERNAL_SERVER_ERROR, body)
            }
        };
        Self {
            status,
            body: Box::new(body),
            source: Box::new(error),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        // An uncertain outcome must not be reused as a cached failure or carry
        // an automatic retry instruction that skips mutation resolution.
        (
            self.status,
            [(header::CACHE_CONTROL, "no-store")],
            Json(self.body),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<ReceiptBody>,
}

impl ErrorBody {
    fn new(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message,
            request_id: None,
            receipt: None,
        }
    }

    fn unknown() -> Self {
        Self::new(
            "outcome_unknown",
            "Resolve the original mutation before retrying.",
        )
    }
}

fn runtime_response(error: &Error) -> (StatusCode, ErrorBody) {
    match error {
        Error::Identity(_) | Error::Command(_) => (
            StatusCode::BAD_REQUEST,
            ErrorBody::new("invalid_request", "Invalid Cell request."),
        ),
        Error::RequestConflict => (
            StatusCode::CONFLICT,
            ErrorBody::new(
                "request_conflict",
                "Request identity was already used for different command bytes.",
            ),
        ),
        Error::Deadline => (
            StatusCode::GATEWAY_TIMEOUT,
            ErrorBody::new("deadline_exceeded", "Cell invocation deadline exceeded."),
        ),
        Error::OutcomeUnknown { request_id, .. } => {
            let mut body = ErrorBody::unknown();
            body.request_id = Some(hex(request_id.as_bytes()));
            (StatusCode::SERVICE_UNAVAILABLE, body)
        }
        Error::EffectOutcomeUnknown { .. } | Error::PeerTransportUnknown { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, ErrorBody::unknown())
        }
        Error::Capacity(_)
        | Error::RuntimeClosed
        | Error::CellNotActive
        | Error::CellDraining
        | Error::Fenced
        | Error::PendingPublication
        | Error::ReplicaBehind { .. }
        | Error::ReplicaUnavailable
        | Error::PeerTransport { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorBody::new("unavailable", "Cell is temporarily unavailable."),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorBody::new("internal_error", "Cell operation failed."),
        ),
    }
}
