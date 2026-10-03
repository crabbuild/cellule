//! Deterministic fleet operation records and transitions.
//!
//! A journal adapter publishes transitions by compare-and-swap. These records
//! allocate advisory work and account its resources; only the Cell actor and
//! existing authority path can acquire, release, or serve a Cell. All times
//! and observations are supplied by adapters, so replay needs no I/O or clock.

mod accepted;
mod acquisition;
mod actions;
mod attempt;
mod codec;
mod enrollment;
mod follower_evacuation;
pub use follower_evacuation::{
    FollowerEvacuationRecord, FollowerReplacementPolicy, FollowerReplacementWitness,
};
mod history;
mod inspection;
mod intent;
mod journal;
mod reader_evacuation;
mod records;
mod recovery;
pub use reader_evacuation::{
    ReaderEvacuationPage, ReaderEvacuationRecord, ReaderReplacementWitness,
};
mod registry;
pub use recovery::{RecoveredActivation, RecoveryBasis, RecoveryEvidence};

pub use accepted::AcceptedFleetAction;
pub use acquisition::AcquisitionBasis;
pub use actions::{
    FleetAction, FleetActionKind, FleetActionOutcome, FleetOutcome, MaintenanceAction,
};
pub use attempt::{
    ActivationEvidence, AttemptEvent, AttemptPhase, MoveAttempt, MoveAttemptSpec, MovementAction,
    PublishedPosition, ReceiverReservation, TransferCost,
};
pub use enrollment::{
    EnrollmentEndpoint, EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentSpec,
    EnrollmentStatus,
};
pub use history::{ProgressHead, ProgressPage};
pub use inspection::{FleetInspectionObservation, FleetInspectionRequest};
pub use intent::NodeIntent;
pub use journal::{ControllerLease, FleetHead, JournalTransition};
pub use records::{
    AttemptId, DrainBlocker, DrainEvidence, FleetProfile, FleetScope, MaintenanceEvent,
    MaintenanceOperation, MaintenancePhase, OperationId,
};
pub use registry::{EnrollmentPage, IntentPage, RegistryVersion};

/// Fleet journal and action format version.
pub const FORMAT_VERSION: u8 = 1;
/// Maximum encoded journal head or action envelope.
pub const MAX_RECORD_BYTES: u32 = 64 * 1024;
/// Maximum encoded observation or historical progress page.
pub const MAX_PAGE_BYTES: u32 = 1024 * 1024;
/// Maximum entries in an observation or historical progress page.
pub const MAX_PAGE_ENTRIES: usize = 128;
/// Hard initial bound on unresolved planned movement attempts.
pub const MAX_ACTIVE_ATTEMPTS: usize = 2;
/// Hard initial bound on disk demand reserved by unresolved attempts.
pub const MAX_RESTORE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Failure of a pure fleet operation transition or its bounded codec.
#[derive(Debug, thiserror::Error)]
pub enum OperationError {
    /// A supplied record, observation, or event violates the contract.
    #[error("invalid fleet operation: {0}")]
    Invalid(&'static str),
    /// The expected journal revision is no longer current.
    #[error("fleet journal revision changed")]
    Conflict,
    /// The controller lease is expired or belongs to a different epoch.
    #[error("fleet controller is fenced")]
    Fenced,
    /// The operation cannot admit new work beyond its deadline.
    #[error("fleet operation deadline exceeded")]
    Deadline,
    /// Unresolved attempts consume the fleet count or byte budget.
    #[error("fleet movement budget is exhausted")]
    Budget,
    /// Operator policy disables allocation of new planned attempts.
    #[error("fleet movement scheduling is stopped")]
    Stopped,
    /// The exact attempt is not present in the current head.
    #[error("fleet movement attempt is absent")]
    NotFound,
    /// A different maintenance request is still active.
    #[error("fleet maintenance operation is already active")]
    Busy,
    /// Bounded wire encoding failed, preserving the original codec error.
    #[error("fleet operation encoding failed")]
    Codec(#[from] crate::codec::CodecError),
    /// Cell target validation failed, preserving the original identity error.
    #[error("fleet Cell identity is invalid")]
    Identity(#[source] Box<crate::Error>),
    /// Canonical Cell control validation failed with its original source.
    #[error("fleet acquisition control is invalid")]
    Control(#[source] Box<crate::Error>),
}

/// Result of a pure fleet operation transition.
pub type Result<T> = std::result::Result<T, OperationError>;

fn nonzero(bytes: &[u8]) -> bool {
    bytes.iter().any(|byte| *byte != 0)
}

#[cfg(test)]
mod tests;
