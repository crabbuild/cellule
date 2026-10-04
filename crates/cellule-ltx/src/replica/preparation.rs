//! Native verified derivation observed before immutable upload completion.
use super::*;
use std::{future::Future, pin::Pin};

/// Verified native root derivation whose immutable uploads may still be running.
///
/// Construction is private. This value allows retention of preparation metadata;
/// it grants no uploaded-root, authority, restore, serving or acknowledgement
/// rights. Only the later `PreparedRoot` proves all immutable uploads finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootPreparation {
    pub(super) root: RootRef,
    pub(super) predecessor: Option<RootRef>,
}
impl RootPreparation {
    /// Exact native root identity, without upload-completion rights.
    #[must_use]
    pub const fn root(&self) -> RootRef {
        self.root
    }
    /// Exact verified input, if any.
    #[must_use]
    pub const fn predecessor(&self) -> Option<RootRef> {
        self.predecessor
    }
}

/// Caller-owned metadata work joined with the same immutable preparation.
pub type RootPreparationFuture<'a> = Pin<
    Box<
        dyn Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'a,
    >,
>;

/// Caller-supplied storage of verified native preparation metadata.
///
/// This future may overlap immutable root-object uploads. Both must finish
/// successfully before a `PreparedRoot` escapes. The caller owns admission and
/// its finite work lifetime; the future shares the replica's origin I/O permit.
/// This starts no task, scheduler or authority effect.
/// Errors preserve their source and require caller reconciliation/classification.
pub trait RootPreparationMetadata: Send + Sync {
    /// Retains one exact verified derivation, without selecting Cell authority.
    fn retain(&self, preparation: RootPreparation) -> RootPreparationFuture<'_>;
}
