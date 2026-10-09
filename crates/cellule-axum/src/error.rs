use std::{error::Error as StdError, fmt};

use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use cellule_runtime::{Error, client::InvocationError};
use serde::Serialize;

use crate::{ReceiptDto, response::hex};

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
    body: Box<ErrorDto>,
    source: Box<dyn StdError + Send + Sync>,
}

impl HttpError {
    pub(crate) fn request<E: StdError + Send + Sync + 'static>(
        status: StatusCode,
        code: &'static str,
        message: &'static str,
        source: E,
    ) -> Self {
        Self {
            status,
            body: Box::new(ErrorDto::new(code, message)),
            source: Box::new(source),
        }
    }

    pub(crate) fn invalid_request<E: StdError + Send + Sync + 'static>(source: E) -> Self {
        Self::request(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Invalid Cell request.",
            source,
        )
    }

    /// Retains an application integration failure with a safe `internal_error` reply.
    ///
    /// Do not use this to replace an invocation failure: convert that failure
    /// directly so its pending or committed evidence stays in the public reply.
    pub fn internal<E: StdError + Send + Sync + 'static>(source: E) -> Self {
        Self::request(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Cell operation failed.",
            source,
        )
    }

    pub(crate) fn published<E: StdError + Send + Sync + 'static>(
        receipt: cellule_runtime::Receipt,
        source: E,
    ) -> Self {
        let mut error = Self::request(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_published_result",
            "Published output could not be converted; recover the original result before retrying.",
            source,
        );
        error.body.receipt = Some(receipt.into());
        error
    }
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
                let mut body = ErrorDto::unknown();
                body.request_id = Some(hex(pending.identity().request_id.as_bytes()));
                (StatusCode::SERVICE_UNAVAILABLE, body)
            }
            InvocationError::Rejected(committed) => {
                let mut body = ErrorDto::new("command_rejected", "Command was durably rejected.");
                body.receipt = Some(committed.receipt.into());
                (StatusCode::CONFLICT, body)
            }
            InvocationError::InvalidPublishedResult { receipt, .. } => {
                let mut body = ErrorDto::new(
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

/// Public HTTP error envelope; source errors remain available only in Rust.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorDto {
    /// Stable machine-readable outcome or request error code.
    pub code: &'static str,
    /// Safe public explanation, without source-error details.
    pub message: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Original mutation identity when the outcome is uncertain.
    #[cfg_attr(
        feature = "openapi",
        schema(nullable = false, pattern = "^[0-9a-f]{32}$")
    )]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Published observation when an outcome already exists.
    #[cfg_attr(feature = "openapi", schema(inline, nullable = false))]
    pub receipt: Option<ReceiptDto>,
}

impl ErrorDto {
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

fn runtime_response(error: &Error) -> (StatusCode, ErrorDto) {
    match error {
        Error::Identity(_) | Error::Command(_) => (
            StatusCode::BAD_REQUEST,
            ErrorDto::new("invalid_request", "Invalid Cell request."),
        ),
        Error::RequestConflict => (
            StatusCode::CONFLICT,
            ErrorDto::new(
                "request_conflict",
                "Request identity was already used for different command bytes.",
            ),
        ),
        Error::Deadline => (
            StatusCode::GATEWAY_TIMEOUT,
            ErrorDto::new("deadline_exceeded", "Cell invocation deadline exceeded."),
        ),
        Error::OutcomeUnknown { request_id, .. } => {
            let mut body = ErrorDto::unknown();
            body.request_id = Some(hex(request_id.as_bytes()));
            (StatusCode::SERVICE_UNAVAILABLE, body)
        }
        Error::EffectOutcomeUnknown { .. } | Error::PeerTransportUnknown { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, ErrorDto::unknown())
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
            ErrorDto::new("unavailable", "Cell is temporarily unavailable."),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorDto::new("internal_error", "Cell operation failed."),
        ),
    }
}
