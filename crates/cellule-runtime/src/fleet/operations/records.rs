use crate::identity::{ApplicationId, Digest, NodeId, SessionId};

use super::{MAX_ACTIVE_ATTEMPTS, MAX_RESTORE_BYTES, OperationError, Result, nonzero};

/// Nonzero application-assigned operation identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(pub(super) [u8; 16]);

impl OperationId {
    /// Validates a stable identity supplied by the application.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if !nonzero(&bytes) {
            return Err(OperationError::Invalid("zero operation identity"));
        }
        Ok(Self(bytes))
    }

    /// Returns the canonical identity bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// One never-reused journal allocation within an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AttemptId {
    /// Operation that requested the movement.
    pub operation: OperationId,
    /// Monotonic sequence allocated by the fleet head.
    pub sequence: u64,
}

impl AttemptId {
    pub(super) fn validate(self) -> Result<()> {
        if self.sequence == 0 || !nonzero(self.operation.as_bytes()) {
            return Err(OperationError::Invalid("invalid attempt identity"));
        }
        Ok(())
    }
}

/// Scope of one independent controller, journal, and movement budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetScope {
    /// Authenticated deployment fleet identity.
    pub fleet: Digest,
    /// Application whose Cells and sessions may be managed.
    pub application: ApplicationId,
}

impl FleetScope {
    pub(super) fn validate(self) -> Result<()> {
        if !nonzero(self.fleet.as_bytes()) || !nonzero(self.application.as_bytes()) {
            return Err(OperationError::Invalid("zero fleet operation scope"));
        }
        Ok(())
    }
}

/// Validated starting bounds for caller-driven fleet reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetProfile {
    /// Maximum unresolved planned attempts across all donors.
    pub max_inflight: usize,
    /// Maximum disk demand charged by unresolved planned attempts.
    pub max_restore_bytes: u64,
    /// Controller lease duration in logical milliseconds.
    pub controller_lease_ms: i64,
    /// Periodic observation interval; results may wake the caller earlier.
    pub reconcile_interval_ms: i64,
}

impl Default for FleetProfile {
    fn default() -> Self {
        Self {
            max_inflight: MAX_ACTIVE_ATTEMPTS,
            max_restore_bytes: MAX_RESTORE_BYTES,
            controller_lease_ms: 30_000,
            reconcile_interval_ms: 15_000,
        }
    }
}

impl FleetProfile {
    /// Rejects unbounded profiles before a driver or journal starts work.
    pub fn validate(self) -> Result<Self> {
        if self.max_inflight == 0
            || self.max_inflight > MAX_ACTIVE_ATTEMPTS
            || self.max_restore_bytes == 0
            || self.max_restore_bytes > MAX_RESTORE_BYTES
            || self.controller_lease_ms <= 0
            || self.controller_lease_ms > 30_000
            || self.reconcile_interval_ms <= 0
            || self.reconcile_interval_ms >= self.controller_lease_ms
        {
            return Err(OperationError::Invalid("invalid fleet operation profile"));
        }
        Ok(self)
    }
}

/// Bounded operational classification; detailed source errors remain in adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DrainBlocker {
    /// Required membership, inventory, or readiness evidence is incomplete.
    IncompleteObservation = 1,
    /// A sample is stale or its time/sequence regressed.
    StaleObservation = 2,
    /// Release, schema, or operational codec versions disagree.
    IncompatibleRelease = 3,
    /// No receiver can admit the measured Cell cost.
    ReceiverCapacity = 4,
    /// Foreground work or a transition is still executing.
    BusyExecution = 5,
    /// An issued external-work lease has not settled.
    ExternalLease = 6,
    /// An acknowledged state still needs object coverage.
    PendingPublication = 7,
    /// Foreign follower tails or epochs still require this node.
    FollowerObligation = 8,
    /// Primitive or role inventory has not been proven.
    UnknownInventory = 9,
    /// Existing planned attempts consume the movement budget.
    MovementBudget = 10,
    /// An accepted remote action has no confirmed result yet.
    OutcomeUnknown = 11,
    /// The request's deadline stops further admission.
    Deadline = 12,
    /// An owned facility has not completed its drain.
    FacilityFailure = 13,
    /// Required reader redundancy has not been restored.
    ReaderObligation = 14,
}

/// Durable phase of an explicit physical-node maintenance request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MaintenancePhase {
    /// Intent is persisted but local admission is not yet confirmed closed.
    Requested = 1,
    /// The current node session rejects new role acquisition.
    Cordoned = 2,
    /// Writers, readers, and follower obligations are being evacuated.
    Evacuating = 3,
    /// Relocation is proven and normal terminal host shutdown may run.
    Closing = 4,
    /// Host shutdown and exact session withdrawal are confirmed.
    Completed = 5,
}

/// Fresh node and relocation observations needed to close a maintenance operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainEvidence {
    /// Physical node whose durable intent is being reconciled.
    pub node: NodeId,
    /// Exact boot session observed by this evidence.
    pub session: SessionId,
    /// Source writers, including in-progress activation/release transitions.
    pub remaining_cells: u64,
    /// Planned attempts without terminal outcomes or reservation cleanup.
    pub unresolved_attempts: u32,
    /// Whether every affected Cell has verified serving evidence elsewhere.
    pub relocated: bool,
    /// Required reader replacements and local views are settled.
    pub readers_settled: bool,
    /// Complete foreign-tail inventory is proven no longer needed locally.
    pub followers_settled: bool,
    /// Required host facilities have joined and closed.
    pub facilities_closed: bool,
    /// The host has reached its successful terminal state.
    pub stopped: bool,
    /// The exact node session was authoritatively withdrawn.
    pub withdrawn: bool,
}

impl DrainEvidence {
    pub(super) fn ready_to_close(self) -> bool {
        self.remaining_cells == 0
            && self.unresolved_attempts == 0
            && self.relocated
            && self.readers_settled
            && self.followers_settled
    }
}

/// Replayable event of the maintenance lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceEvent {
    /// Local admission closure and advertised mode are confirmed.
    Cordoned,
    /// Begin bounded evacuation under the persisted cordon.
    BeginEvacuation,
    /// Prove all writer and role obligations before entering terminal shutdown.
    ReadyToClose(DrainEvidence),
    /// Prove the successful host shutdown and exact withdrawal.
    Stopped(DrainEvidence),
    /// Report a temporary blocker without losing the durable phase.
    Blocked(DrainBlocker),
    /// Adopt a rebooted session while retaining the physical-node cordon.
    SessionReplaced(SessionId),
    /// Extend a deadline through an authorized newer request revision.
    ExtendDeadline(i64),
}

/// Bounded durable maintenance intent; the operation never reopens acquisition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceOperation {
    pub(super) id: OperationId,
    pub(super) request_digest: Digest,
    pub(super) node: NodeId,
    pub(super) session: SessionId,
    pub(super) intent_revision: u64,
    pub(super) created_at_ms: i64,
    pub(super) deadline_ms: i64,
    pub(super) phase: MaintenancePhase,
    pub(super) blocker: Option<DrainBlocker>,
    pub(super) drain_evidence: Option<DrainEvidence>,
}

impl MaintenanceOperation {
    /// Constructs the persisted request before any node side effect.
    pub fn new(
        id: OperationId,
        request_digest: Digest,
        node: NodeId,
        session: SessionId,
        intent_revision: u64,
        created_at_ms: i64,
        deadline_ms: i64,
    ) -> Result<Self> {
        let operation = Self {
            id,
            request_digest,
            node,
            session,
            intent_revision,
            created_at_ms,
            deadline_ms,
            phase: MaintenancePhase::Requested,
            blocker: None,
            drain_evidence: None,
        };
        operation.validate()?;
        Ok(operation)
    }

    /// Returns the idempotent operation identity.
    #[must_use]
    pub const fn id(&self) -> OperationId {
        self.id
    }
    /// Returns the immutable original request identity across progress/adoption.
    #[must_use]
    pub const fn request_digest(&self) -> Digest {
        self.request_digest
    }
    /// Returns the original request time, independent of later deadlines.
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
    /// Returns the physical node to keep cordoned across sessions.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Returns the current targeted boot session.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the durable node-intent revision.
    #[must_use]
    pub const fn intent_revision(&self) -> u64 {
        self.intent_revision
    }
    /// Returns the operation's admission deadline.
    #[must_use]
    pub const fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }
    /// Returns the durable progress phase.
    #[must_use]
    pub const fn phase(&self) -> MaintenancePhase {
        self.phase
    }
    /// Returns the last temporary blocker.
    #[must_use]
    pub const fn blocker(&self) -> Option<DrainBlocker> {
        self.blocker
    }

    /// Returns the exact evidence retained by a closing or completed operation.
    #[must_use]
    pub const fn drain_evidence(&self) -> Option<DrainEvidence> {
        self.drain_evidence
    }

    pub(super) fn validate(&self) -> Result<()> {
        if !nonzero(self.id.as_bytes())
            || !nonzero(self.request_digest.as_bytes())
            || !nonzero(self.node.as_bytes())
            || !nonzero(self.session.as_bytes())
            || self.intent_revision == 0
            || self.created_at_ms < 0
            || self.deadline_ms <= self.created_at_ms
            || (self.phase == MaintenancePhase::Completed && self.blocker.is_some())
        {
            return Err(OperationError::Invalid("invalid maintenance record"));
        }
        if matches!(
            self.phase,
            MaintenancePhase::Closing | MaintenancePhase::Completed
        ) != self.drain_evidence.is_some()
        {
            return Err(OperationError::Invalid(
                "maintenance phase lacks drain evidence",
            ));
        }
        if let Some(evidence) = self.drain_evidence
            && (evidence.node != self.node
                || evidence.session != self.session
                || !evidence.ready_to_close()
                || (self.phase == MaintenancePhase::Completed
                    && !(evidence.facilities_closed && evidence.stopped && evidence.withdrawn)))
        {
            return Err(OperationError::Invalid("invalid stored drain evidence"));
        }
        Ok(())
    }

    pub(super) fn apply(&mut self, event: MaintenanceEvent, now_ms: i64) -> Result<()> {
        if now_ms < self.created_at_ms {
            return Err(OperationError::Invalid("maintenance time regressed"));
        }
        let matches = |e: DrainEvidence| e.node == self.node && e.session == self.session;
        match event {
            MaintenanceEvent::Cordoned if self.phase == MaintenancePhase::Requested => {
                self.phase = MaintenancePhase::Cordoned;
                self.blocker = None;
            }
            MaintenanceEvent::Cordoned if self.phase != MaintenancePhase::Completed => {}
            MaintenanceEvent::BeginEvacuation if self.phase == MaintenancePhase::Cordoned => {
                self.phase = MaintenancePhase::Evacuating;
                self.blocker = None;
            }
            MaintenanceEvent::BeginEvacuation if self.phase == MaintenancePhase::Evacuating => {}
            MaintenanceEvent::ReadyToClose(e)
                if matches(e)
                    && e.ready_to_close()
                    && matches!(
                        self.phase,
                        MaintenancePhase::Evacuating | MaintenancePhase::Closing
                    ) =>
            {
                self.phase = MaintenancePhase::Closing;
                self.drain_evidence = Some(e);
                self.blocker = None;
            }
            MaintenanceEvent::Stopped(e)
                if matches(e)
                    && e.ready_to_close()
                    && e.facilities_closed
                    && e.stopped
                    && e.withdrawn
                    && matches!(
                        self.phase,
                        MaintenancePhase::Closing | MaintenancePhase::Completed
                    ) =>
            {
                self.phase = MaintenancePhase::Completed;
                self.drain_evidence = Some(e);
                self.blocker = None;
            }
            MaintenanceEvent::Blocked(blocker) if self.phase != MaintenancePhase::Completed => {
                self.blocker = Some(blocker);
            }
            MaintenanceEvent::SessionReplaced(session)
                if nonzero(session.as_bytes()) && self.phase != MaintenancePhase::Completed =>
            {
                if session != self.session {
                    // A new boot invalidates every enrollment check made with
                    // the previous session, even when desired mode is unchanged.
                    self.intent_revision = self
                        .intent_revision
                        .checked_add(1)
                        .ok_or(OperationError::Invalid("maintenance revision overflow"))?;
                    self.session = session;
                    self.phase = MaintenancePhase::Requested;
                    self.drain_evidence = None;
                    self.blocker = Some(DrainBlocker::IncompleteObservation);
                }
            }
            MaintenanceEvent::ExtendDeadline(deadline)
                if deadline > self.deadline_ms
                    && deadline > now_ms
                    && self.phase != MaintenancePhase::Completed =>
            {
                self.deadline_ms = deadline;
                self.intent_revision = self
                    .intent_revision
                    .checked_add(1)
                    .ok_or(OperationError::Invalid("maintenance revision overflow"))?;
                self.blocker = None;
            }
            _ => return Err(OperationError::Invalid("unproven maintenance transition")),
        }
        self.validate()
    }
}
