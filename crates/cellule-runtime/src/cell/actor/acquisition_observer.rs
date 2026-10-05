use std::{future::Future, pin::Pin};

use crate::control::Control;

/// A durable recording call at a canonical acquisition boundary.
pub type AcquisitionObservation<'a> = Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>>;

/// Trusted recorder for the exact input and restored position of acquisition.
///
/// The runtime waits for confirmation before ownership CAS and before actor
/// admission. The adapter must retain immutable original records and reject
/// incompatible repeats. These callbacks grant no ownership or authentication.
/// A lost write reply is an error, not permission to continue. The caller must
/// own this acquisition future independently of transport waiters.
pub trait AcquisitionObserver: Send + Sync + 'static {
    /// Records the exact canonical input before its ownership CAS. Takeover may
    /// retry a changed predecessor, so an adapter must explicitly accept or
    /// reject each input; it must never silently overwrite an earlier basis.
    /// Resuming an already claimed takeover reconfirms the original input here
    /// before materialization; it performs no additional ownership CAS.
    fn before_claim<'a>(&'a self, input: &'a Control) -> AcquisitionObservation<'a>;

    /// Records the exact published recovery/root position before actor activation.
    /// `input` is the confirmed input of the successful CAS, not a later reread.
    /// Failure follows canonical acquisition rollback and preserves its error.
    fn before_activation<'a>(
        &'a self,
        input: &'a Control,
        restored: &'a Control,
    ) -> AcquisitionObservation<'a>;
}
