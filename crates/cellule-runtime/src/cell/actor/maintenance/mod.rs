//! Busy maintenance uses the existing worker, coordination and release effects.

use super::*;
use crate::fleet::operations::{DrainBlocker, PublishedPosition};

mod inspection;
mod release;
pub(super) use inspection::inspect;
pub(super) use release::{begin, drive};

/// Result of a source-owned planned maintenance release.
#[derive(Debug)]
pub enum MaintenanceCellRelease {
    /// Canonical close and authority release produced this exact final root.
    Released(PublishedPosition),
    /// This request started no canonical release. Foreground closure, if
    /// installed, stays sticky; native completion remains available.
    Refused {
        /// The original preflight condition; it grants no release authority.
        blocker: DrainBlocker,
        /// Original inventory/admission failure, when one occurred.
        error: Option<Error>,
    },
}

pub(super) struct ReleaseRequest {
    pub(super) cell: CellId,
    pub(super) generation: u64,
    pub(super) incarnation: crate::identity::IncarnationId,
    pub(super) epoch: u64,
    pub(super) deadline: std::time::Instant,
    pub(super) reply: oneshot::Sender<crate::Result<MaintenanceCellRelease>>,
}

pub(super) struct ReleaseState {
    pub(super) deadline: std::time::Instant,
    pub(super) next_check: std::time::Instant,
    pub(super) closing: bool,
    pub(super) inventory_effect: Option<u64>,
}

pub(super) fn refuse(reply: DrainReply, blocker: DrainBlocker, error: Option<Error>) {
    match reply {
        DrainReply::Maintenance(reply) => {
            let _ = reply.send(Ok(MaintenanceCellRelease::Refused { blocker, error }));
        }
        reply => {
            let _ = reply.send(Err(match error {
                Some(error) => error,
                None => Error::CellDraining,
            }));
        }
    }
}

pub(super) fn reopen_completion(active: &mut ActiveCell) {
    active.coordination.step(CoordinationInput::AbortTransfer);
    if !active.coordination.is_fenced() && !active.draining() {
        // Semaphores remain open until ConfirmTransfer. No authority mutation
        // has started here; this restores only exact native completion because
        // the sticky foreground flag and kernel state remain installed.
        active.admission.draining.store(false, Ordering::Release);
    }
}
