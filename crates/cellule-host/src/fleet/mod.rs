//! Journal-bound fleet execution through the node's canonical runtime.
//!
//! Applications authenticate management requests and supply a strongly
//! consistent journal. Local execution owns accepted work independently of
//! transport waiters. Controller scheduling and deployment remain application
//! responsibilities.

mod actions;
mod cells;
mod controller;
mod coverage;
mod enrollment;
mod failed_boot;
mod follower_evacuation;
pub use follower_evacuation::{
    FleetFollowerEvacuationCandidate, FleetFollowerEvacuationCheck, FleetFollowerEvacuationJournal,
    FleetFollowerEvacuationPublication, FleetFollowerEvacuationVerifier,
};
mod inspection;
mod inventory;
mod journal;
mod maintenance;
mod maintenance_enrollments;
mod maintenance_policies;
pub use maintenance_enrollments::FleetMaintenanceEnrollments;
pub use maintenance_policies::{
    FleetEnrollmentNonexecution, FleetEnrollmentNonexecutionEvidence,
    FleetEnrollmentNonexecutionRequest, FleetMaintenanceNonexecution,
    FleetMaintenancePolicyCoverage, FleetMaintenancePolicyObligation,
    FleetMaintenancePolicyProgress, FleetMaintenancePolicyStatus,
};
mod movement;
mod native_writer;
mod reader_evacuation;
mod reconciler;
mod recovered;
pub use reader_evacuation::{
    FleetReaderEvacuationCandidate, FleetReaderEvacuationCheck, FleetReaderEvacuationJournal,
    FleetReaderEvacuationPublication, FleetReaderEvacuationVerifier, FleetSourceReaderCheck,
    FleetSourceReaderInputs, FleetSourceReaderPolicies, FleetSourceReaderRetirement,
    FleetSourceReaderSuccessors,
};
mod references;
mod roster;
pub(crate) mod snapshot;
pub(crate) mod withdrawal;

pub use actions::{
    FleetActionCompletion, FleetActionWorkEntry, FleetActionWorkKind, FleetActionWorkSnapshot,
    FleetActionWorkState,
};
pub use cells::{FleetCellInputs, FleetCellProvider, FleetRecoveryInputs};
pub use controller::{FleetJournal, FleetJournalSnapshot};
pub use coverage::FleetRoleCoverage;
pub use enrollment::{FleetBootObservation, FleetEnrollmentAcceptance, FleetEnrollmentJournal};
pub use failed_boot::{
    FleetFailedBootClosure, FleetFailedBootProcessConfirmation, FleetFailedBootProcessEvidence,
    FleetFailedBootProcessRequest, FleetFailedBootProcesses, FleetFailedBootPublication,
    FleetFailedBootRetirement, FleetFailedReaderClosure, FleetFailedReaderPublication,
    FleetFailedReaderRetirement, FleetOriginalBootSuffixInventory, FleetOriginalCatalogSet,
    FleetOriginalCatalogSource, FleetOriginalCatalogs, FleetOriginalWriterCapture,
    FleetOriginalWriterInventory, FleetOriginalWriterJournal, FleetOriginalWriterSuccessorInputs,
    FleetOriginalWriterSuccessorInventory, FleetOriginalWriterSuccessorProof,
    FleetOriginalWriterSuccessors,
};
pub use inventory::{FleetNodeInventory, FleetNodeInventoryRecheck, FleetNodeInventoryScan};
pub use journal::{FleetActionAcceptance, FleetActionJournal, FleetAdapterFuture};
pub use reconciler::{
    FleetAttemptFailure, FleetObservation, FleetObserver, FleetOwnedCell, FleetReconcileReport,
    FleetReconciler, FleetTransport,
};
pub use recovered::{
    FleetRecoveredFollowerClosure, FleetRecoveredFollowerMember, FleetRecoveredFollowerPublication,
    FleetRecoveredFollowerRetirement,
};
pub use references::FleetFollowerReferences;
pub use roster::{FleetRoster, FleetRosterBoot};
pub use snapshot::{
    FleetNodeSnapshot, FleetSnapshotBindings, FleetSnapshotNativePage, FleetSnapshotRequest,
    FleetSnapshotSubject, FleetSnapshotTransport,
};

pub(crate) use actions::FleetActionExecutor;
pub(crate) use actions::operation;

/// Stable name of the node-owned finite fleet-action executor.
pub const FLEET_ACTION_COMPONENT: &str = "fleet-actions";
