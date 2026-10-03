//! Application-owned storage, enrollment, and lifecycle assembly.
//!
//! This crate does not implement a second authority or durability path. Domain
//! applications compile their own modules and use the returned typed handles.

mod maintenance;
mod node;
mod peer;
mod signals;
mod storage;
#[cfg(test)]
mod tests;

pub use node::{LocalNode, NodeConfig, SiblingNodeFactory};
pub use peer::LocalPeer;
pub use signals::shutdown_signal;
pub use storage::local_s3_store;

use std::time::{SystemTime, UNIX_EPOCH};

use cellule_runtime::{MutationIdentity, identity::RequestId};

/// Errors retain their originating framework, provider, or operating-system cause.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Framework execution or lifecycle failure.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
    /// Supervised scheduler invocation failure, including pending evidence.
    #[error("application maintenance failed")]
    Maintenance(
        #[from]
        cellule_runtime::InvocationError<
            cellule_runtime::primitives::maintenance::MaintenanceTickOutcome,
        >,
    ),
    /// LTX preparation or verified reconstruction failure.
    #[error(transparent)]
    Ltx(#[from] cellule_ltx::LtxError),
    /// Provider construction or transport failure.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    /// Storage operation failure.
    #[error(transparent)]
    Storage(#[from] cellule_store::StorageError),
    /// Filesystem failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Invalid application configuration.
    #[error("invalid configuration: {0}")]
    Configuration(&'static str),
    /// Required provider capabilities were not proved.
    #[error("storage capability probe failed: {0:?}")]
    Probe(Vec<&'static str>),
    /// Wall clock precedes the Unix epoch.
    #[error(transparent)]
    Clock(#[from] std::time::SystemTimeError),
}

/// Result returned by application infrastructure.
pub type Result<T> = std::result::Result<T, Error>;

/// Returns a checked Unix timestamp in milliseconds.
pub fn now_ms() -> Result<i64> {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
        .map_err(|_| Error::Configuration("wall clock exceeds timestamp range"))
}

/// Creates an identity for a new logical command; retain it unchanged for retry.
pub fn new_identity() -> Result<MutationIdentity> {
    let issued_at_ms = now_ms()?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        issued_at_ms,
        expires_at_ms: issued_at_ms
            .checked_add(300_000)
            .ok_or(Error::Configuration("mutation expiry overflow"))?,
    })
}
