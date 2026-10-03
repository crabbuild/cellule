//! Axum integration for existing typed Cellule application capabilities.
//!
//! [`Cellule`] extracts a scoped handle from router state, [`CellJson`] returns
//! an output with its receipt, and [`HttpError`] preserves invocation failures
//! while producing outcome-aware HTTP errors. The application owns routes,
//! authorization, tenant selection, listeners, and runtime shutdown.
//!
//! [`RequestCellule`] reads an authorized request-scoped capability,
//! [`MutationJson`] decodes caller-created identities and inputs, and
//! [`MinimumReceipt`] carries full observations into queries. The optional
//! `openapi` feature adds Utoipa schemas and typed endpoint registration.

#![deny(missing_docs)]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented
    )
)]

mod error;
mod extract;
mod mutation;
mod receipt;
mod response;

#[cfg(feature = "openapi")]
mod openapi;

pub use error::{ErrorDto, HttpError};
pub use extract::{Cellule, RequestCellule};
pub use mutation::{MutationBody, MutationIdentityDto, MutationJson};
pub use receipt::{MAX_RECEIPT_HEADER_BYTES, MINIMUM_RECEIPT_HEADER, MinimumReceipt, ReceiptDto};
pub use response::CellJson;

#[cfg(feature = "openapi")]
mod api;
#[cfg(feature = "openapi")]
pub use api::{CellApi, CellEndpoint, CommandEndpoint, EndpointSpec};
/// OpenAPI schema and document tooling used by the optional integration.
#[cfg(feature = "openapi")]
pub use utoipa;
/// Axum routers which can merge and nest generated OpenAPI documents.
#[cfg(feature = "openapi")]
pub use utoipa_axum;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod guide {}
