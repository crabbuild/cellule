use cellule_runtime::fleet::operations::{
    EnrollmentPage, FleetHead, FleetScope, IntentPage, JournalTransition, MaintenanceOperation,
    OperationError, OperationId, ProgressPage, RegistryVersion,
};
use cellule_runtime::identity::{CellId, Digest, IncarnationId, NodeId, SessionId};

use super::{FleetActionJournal, FleetAdapterFuture, FleetEnrollmentJournal};

/// Head and registry metadata read together in one consistent transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetJournalSnapshot {
    head: FleetHead,
    registry: RegistryVersion,
}

impl FleetJournalSnapshot {
    /// Binds the same application scope without claiming observation coverage.
    pub fn new(head: FleetHead, registry: RegistryVersion) -> Result<Self, OperationError> {
        if head.scope() != registry.scope() {
            return Err(OperationError::Conflict);
        }
        Ok(Self { head, registry })
    }
    /// Returns the current charged attempts and controller fencing epoch.
    #[must_use]
    pub const fn head(&self) -> &FleetHead {
        &self.head
    }
    /// Returns retained intent/enrollment revision and operator scheduling mode.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.registry
    }
}

/// Controller and action transactions on one shared, durable application journal.
///
/// Implementations bind one validated FleetProfile at construction. Every method
/// preserves original backend errors. The action acceptance methods inherited
/// here must use the same transaction domain as these methods, not a separate
/// read-then-write cache. Applications own caller authorization and storage.
pub trait FleetJournal: FleetActionJournal + FleetEnrollmentJournal {
    /// Reads the head and registry metadata from one consistent transaction.
    fn load_snapshot(&self, scope: FleetScope) -> FleetAdapterFuture<'_, FleetJournalSnapshot>;

    /// CAS-acquires/renews the controller using `FleetHead::claim`. An ambiguous
    /// reply requires rereading the same head, never deleting its permits.
    fn claim_controller(
        &self,
        scope: FleetScope,
        expected_revision: u64,
        claimant: SessionId,
        now_ms: i64,
    ) -> FleetAdapterFuture<'_, FleetJournalSnapshot>;

    /// Atomically publishes one reducer transition against both current versions.
    ///
    /// For Allocate, load exact source/receiver intents and invoke the registry's
    /// `authorize_allocation` inside this transaction before the head transition.
    /// For maintenance, advance and retain that physical node's intent in the
    /// same commit. Retain all prior operations for return-to-service proofs.
    /// For Retire, publish the exact progress page with permit retirement; a
    /// failed CAS cannot expose committed history or release either budget.
    /// Finalization must compare the observed registry version again here.
    fn compare_exchange<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        controller_epoch: u64,
        now_ms: i64,
        transition: &'a JournalTransition,
    ) -> FleetAdapterFuture<'a, FleetJournalSnapshot>;

    /// Loads retained operations, including those older than the current head.
    fn load_operation(
        &self,
        scope: FleetScope,
        operation: OperationId,
    ) -> FleetAdapterFuture<'_, Option<MaintenanceOperation>>;

    /// Loads an immutable progress page. Verify scope and canonical page digest
    /// against the requested digest; a successful orphan PUT is not committed.
    fn load_progress(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<ProgressPage>>;

    /// Reads the greatest confirmed movement completion time for this exact
    /// Cell incarnation from history committed at `expected`. Cancelled attempts
    /// do not count as movement. Compare the full snapshot in the same read
    /// transaction; an orphan progress page cannot contribute to cooldown.
    /// An indexed backend may use an index updated atomically with retirement.
    fn last_moved_at<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        cell: CellId,
        incarnation: IncarnationId,
    ) -> FleetAdapterFuture<'a, Option<i64>>;

    /// Greatest committed movement completion time across the scope. Count
    /// balancing requires every signed sample to follow this post-batch barrier.
    /// Compare the full snapshot and exclude cancellation/orphan pages.
    fn last_movement_at<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<i64>>;

    /// Returns retained physical-node rows, including every earlier cordon.
    /// Require the exact version and a limit in 1..=128 before any allocation.
    /// Reject changed revisions rather than returning a filtered current page.
    fn intents_page(
        &self,
        version: RegistryVersion,
        after: Option<NodeId>,
        limit: usize,
    ) -> FleetAdapterFuture<'_, IntentPage>;

    /// Returns all enrollment rows, including failed, pending and retired boots.
    /// Require the exact version and bounded limit; never omit expired sessions.
    fn enrollments_page(
        &self,
        version: RegistryVersion,
        after: Option<Digest>,
        limit: usize,
    ) -> FleetAdapterFuture<'_, EnrollmentPage>;

    /// Atomically applies revision-checked stop/resume through the registry.
    /// Stop blocks new Allocate transactions and preserves accepted work, every
    /// charged permit, retained intent, and all immutable evidence/history.
    fn set_scheduling(
        &self,
        expected: RegistryVersion,
        enabled: bool,
    ) -> FleetAdapterFuture<'_, RegistryVersion>;
}
