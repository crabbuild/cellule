use cellule_runtime::fleet::operations::{
    EnrollmentEvent, EnrollmentRecord, EnrollmentSpec, FleetScope, NodeIntent, OperationId,
    RegistryVersion,
};
use cellule_runtime::identity::{Digest, NodeId, SessionId};

use super::FleetAdapterFuture;

/// Atomic pending enrollment or its original retained request.
pub enum FleetEnrollmentAcceptance {
    /// Pending acceptance committed with current intent checks. Only this
    /// result permits first execution of the ordinary enrollment protocol.
    New(EnrollmentRecord),
    /// Original request, including lost replies and terminal tombstones.
    /// Pending state does not authorize repeating an unobserved enrollment.
    Existing(EnrollmentRecord),
}

/// Enrollment and retained-intent transactions sharing the controller journal.
///
/// Authentication, bootstrap coverage and canonical role evidence are supplied
/// by the application. Successful writes must survive backend/client restart.
/// Advance the shared RegistryVersion atomically with every actual row change;
/// exact duplicates return original records without refreshing evidence time.
pub trait FleetEnrollmentJournal: Send + Sync + 'static {
    /// Creates an initial Active physical-node row only when absent. An existing
    /// identical row is idempotent; any different row conflicts. Never overwrite
    /// an older retained cordon with a boot's default configuration.
    fn register_initial_intent<'a>(
        &'a self,
        intent: &'a NodeIntent,
    ) -> FleetAdapterFuture<'a, NodeIntent>;

    /// Rebinds an Active intent after application-validated authoritative old
    /// withdrawal and new boot enrollment. Check the exact original intent and
    /// derive the successor with `rebind_active` inside the shared transaction.
    fn rebind_active_intent<'a>(
        &'a self,
        original: &'a NodeIntent,
        session: SessionId,
        revision: u64,
    ) -> FleetAdapterFuture<'a, NodeIntent>;

    /// Loads the exact retained completed operation and current physical-node
    /// intent together, then uses `return_to_service`. New boot validation and
    /// operator authorization precede this call. A different operation conflicts.
    fn return_to_service(
        &self,
        scope: FleetScope,
        node: NodeId,
        operation: OperationId,
        new_session: SessionId,
        revision: u64,
    ) -> FleetAdapterFuture<'_, NodeIntent>;

    /// Commits the controlled initial coverage barrier at the exact revision.
    /// The caller pauses all producers and imports all existing live/failed
    /// obligations first. Do not infer bootstrap from an empty live directory.
    fn bootstrap_registry(
        &self,
        expected: RegistryVersion,
    ) -> FleetAdapterFuture<'_, RegistryVersion>;

    /// First acceptance checks all spec intent revisions and Active mode for a
    /// new reader/follower role in the same commit as Pending. Boot enrollment
    /// must honor its exact retained mode and cannot open readiness. Existing
    /// requests compare full original
    /// inputs before current intent checks; a cordon cannot erase admitted work.
    fn accept_enrollment<'a>(
        &'a self,
        spec: &'a EnrollmentSpec,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FleetEnrollmentAcceptance>;

    /// Confirms an original acceptance and canonical result in one transaction.
    /// Compare immutable spec and original acceptance time, load current progress
    /// and apply the event. Evidence must cover the exact role/session/epoch.
    /// Lost replies leave pending work charged as an obligation until inspection
    /// proves a definite refusal, completion, or canonical closure/retirement.
    fn publish_enrollment_result<'a>(
        &'a self,
        original: &'a EnrollmentRecord,
        event: EnrollmentEvent,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, EnrollmentRecord>;

    /// Reads retained request progress, including failed-session tombstones.
    fn load_enrollment(
        &self,
        scope: FleetScope,
        key: Digest,
    ) -> FleetAdapterFuture<'_, Option<EnrollmentRecord>>;
}
