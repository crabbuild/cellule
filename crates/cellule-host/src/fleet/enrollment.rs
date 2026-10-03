use cellule_runtime::fleet::operations::{
    EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentSpec, EnrollmentStatus,
    FleetScope, NodeIntent, OperationError, OperationId, RegistryVersion,
};
use cellule_runtime::identity::{Digest, NodeId, SessionId};

use super::FleetAdapterFuture;

/// Current physical intent and established boot obligation read atomically.
/// Canonical advertisement evidence is verified by the trusted journal adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetBootObservation {
    intent: NodeIntent,
    enrollment: EnrollmentRecord,
}

impl FleetBootObservation {
    /// Checks an established boot against the same transaction's current intent.
    /// An Active enrollment may finish after a cordon, without reopening it.
    pub fn new(intent: NodeIntent, enrollment: EnrollmentRecord) -> Result<Self, OperationError> {
        intent.to_bytes()?;
        enrollment.to_bytes()?;
        let spec = enrollment.spec();
        let EnrollmentRole::Node { mode } = spec.role else {
            return Err(OperationError::Conflict);
        };
        if enrollment.status() != EnrollmentStatus::Established
            || spec.source.is_some()
            || spec.scope != intent.scope()
            || spec.target.node != intent.node()
            || spec.target.session != intent.session()
            || spec.target.intent_revision > intent.revision()
            || (spec.target.intent_revision == intent.revision() && mode != intent.mode())
        {
            return Err(OperationError::Conflict);
        }
        Ok(Self { intent, enrollment })
    }
    /// Returns the current retained physical-node intent.
    #[must_use]
    pub const fn intent(&self) -> &NodeIntent {
        &self.intent
    }
    /// Returns the established original boot obligation and canonical evidence.
    #[must_use]
    pub const fn enrollment(&self) -> &EnrollmentRecord {
        &self.enrollment
    }
}

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
    /// Atomically settles joined work whose native enrollment never started.
    /// Validate complete immutable inputs and trusted nonexecution evidence.
    /// If absent, retain `unexecuted_refusal`; if Pending, apply that same refusal.
    /// An existing identical refusal replays its original time. Established or
    /// differently settled rows conflict. Advance RegistryVersion in the same
    /// transaction. A delayed acceptance must return this terminal row, never New.
    /// This is not an absence read or authority to cancel an unobserved native CAS.
    fn refuse_unexecuted_enrollment<'a>(
        &'a self,
        spec: &'a EnrollmentSpec,
        evidence: Digest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, EnrollmentRecord>;

    /// Loads current intent and the exact established boot in one transaction.
    /// Missing rows return None. Pending/refused/retired, foreign-role/session
    /// or contradictory rows fail closed. Never return a cached older intent.
    fn load_boot(
        &self,
        scope: FleetScope,
        node: NodeId,
        key: Digest,
    ) -> FleetAdapterFuture<'_, Option<FleetBootObservation>>;

    /// Creates an initial Active physical-node row only when absent. An existing
    /// identical row is idempotent; any different row conflicts. Never overwrite
    /// an older retained cordon with a boot's default configuration.
    fn register_initial_intent<'a>(
        &'a self,
        intent: &'a NodeIntent,
    ) -> FleetAdapterFuture<'a, NodeIntent>;

    /// Rebinds an Active intent after application-validated authoritative old
    /// withdrawal or complete failed-boot closure, and new boot enrollment.
    /// Failed closure includes original process/accepted external work joining
    /// and every original role settled; expiry or takeover alone is insufficient.
    /// Check the exact original intent and
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
