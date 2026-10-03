//! Journal-bound fleet execution through the node's canonical runtime.
//!
//! Applications authenticate management requests and supply a strongly
//! consistent journal. Local execution owns accepted work independently of
//! transport waiters. Controller scheduling and deployment remain application
//! responsibilities.

mod actions;
mod cells;
mod controller;
mod enrollment;
mod inspection;
mod inventory;
mod journal;
mod maintenance;
mod movement;
mod reconciler;
mod references;
mod roster;
pub(crate) mod snapshot;
pub(crate) mod withdrawal;

pub use actions::FleetActionCompletion;
pub use cells::{FleetCellInputs, FleetCellProvider, FleetRecoveryInputs};
pub use controller::{FleetJournal, FleetJournalSnapshot};
pub use enrollment::{FleetBootObservation, FleetEnrollmentAcceptance, FleetEnrollmentJournal};
pub use inventory::{FleetNodeInventory, FleetNodeInventoryRecheck, FleetNodeInventoryScan};
pub use journal::{FleetActionAcceptance, FleetActionJournal, FleetAdapterFuture};
pub use reconciler::{
    FleetAttemptFailure, FleetObservation, FleetObserver, FleetOwnedCell, FleetReconcileReport,
    FleetReconciler, FleetTransport,
};
pub use references::FleetFollowerReferences;
pub use roster::{FleetRoster, FleetRosterBoot};
pub use snapshot::{
    FleetNodeSnapshot, FleetSnapshotBindings, FleetSnapshotNativePage, FleetSnapshotRequest,
    FleetSnapshotSubject,
};

pub(crate) use actions::FleetActionExecutor;
pub(crate) use actions::operation;

/// Stable name of the node-owned finite fleet-action executor.
pub const FLEET_ACTION_COMPONENT: &str = "fleet-actions";
