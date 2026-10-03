//! Axum integration for existing typed Cellule application capabilities.
//!
//! [`Cellule`] extracts a scoped handle from router state, [`CellJson`] returns
//! an output with its receipt, and [`HttpError`] preserves invocation failures
//! while producing outcome-aware HTTP errors. The application owns routes,
//! authorization, tenant selection, listeners, and runtime shutdown.

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
mod response;

pub use error::HttpError;
pub use extract::Cellule;
pub use response::CellJson;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod guide {}
