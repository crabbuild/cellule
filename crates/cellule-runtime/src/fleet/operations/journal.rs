use crate::identity::SessionId;

use super::{
    AttemptEvent, AttemptId, FleetProfile, FleetScope, MAX_ACTIVE_ATTEMPTS, MAX_RESTORE_BYTES,
    MaintenanceEvent, MaintenanceOperation, MaintenancePhase, MoveAttempt, MoveAttemptSpec,
    MovementAction, NodeIntent, OperationError, ProgressHead, ProgressPage, Result, nonzero,
};

/// Journal controller lease; it never confers Cell ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControllerLease {
    /// Exact controller process that owns the journal epoch.
    pub claimant: SessionId,
    /// Monotonic fencing epoch, including same-process reacquisition after expiry.
    pub epoch: u64,
    /// Exclusive expiration time in adapter-supplied logical milliseconds.
    pub expires_at_ms: i64,
}

/// One CAS-published deterministic state change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalTransition {
    /// Atomically persist maintenance intent before cordoning its node.
    BeginMaintenance(MaintenanceOperation),
    /// Advance or diagnose the current maintenance operation.
    Maintenance(MaintenanceEvent),
    /// Allocate one exact attempt and its fleet count/byte permit.
    Allocate(MoveAttemptSpec),
    /// Persist a dispatch intent or confirmed exact-attempt outcome.
    Attempt {
        /// Attempt receiving this result or dispatch intent.
        id: AttemptId,
        /// Validated movement transition.
        event: AttemptEvent,
    },
    /// In the same CAS transaction, prove no acceptance exists for this exact
    /// effect, endpoint and attempt. Fence delayed old envelopes by advancing
    /// the head revision, then retry or cancel when admission has expired.
    /// A separate lookup cannot satisfy this precondition. Never use this to
    /// erase an accepted effect or reclaim its permit.
    ResolveUnaccepted {
        /// Still-charged exact movement attempt.
        id: AttemptId,
        /// Effect whose original acceptance is atomically proved absent.
        effect: MovementAction,
    },
    /// Publish exact terminal history and free its permits in the same CAS.
    /// The adapter persists this immutable page before publishing the head.
    Retire {
        /// All retired attempts must exactly match the current active records.
        progress: ProgressPage,
    },
}

/// Bounded atomically published controller state and unresolved movement permits.
///
/// History pages are persisted by the adapter before CAS publication. All
/// unresolved attempts live in this head so controller failover cannot forget
/// remote work or oversubscribe the fleet's budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetHead {
    pub(super) scope: FleetScope,
    pub(super) revision: u64,
    pub(super) last_observed_ms: i64,
    pub(super) controller: Option<ControllerLease>,
    pub(super) maintenance: Option<MaintenanceOperation>,
    pub(super) next_sequence: u64,
    pub(super) attempts: Vec<MoveAttempt>,
    pub(super) progress: Option<ProgressHead>,
}

impl FleetHead {
    /// Creates an empty journal; the adapter installs it with create-if-absent.
    pub fn new(scope: FleetScope, now_ms: i64) -> Result<Self> {
        scope.validate()?;
        if now_ms < 0 {
            return Err(OperationError::Invalid("negative journal time"));
        }
        Ok(Self {
            scope,
            revision: 0,
            last_observed_ms: now_ms,
            controller: None,
            maintenance: None,
            next_sequence: 1,
            attempts: Vec::new(),
            progress: None,
        })
    }

    /// Returns the exact authorization scope.
    #[must_use]
    pub const fn scope(&self) -> FleetScope {
        self.scope
    }
    /// Returns the revision an adapter must compare when publishing.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    /// Returns the current journal controller lease.
    #[must_use]
    pub const fn controller(&self) -> Option<ControllerLease> {
        self.controller
    }
    /// Returns the durable physical-node maintenance intent and progress.
    #[must_use]
    pub const fn maintenance(&self) -> Option<&MaintenanceOperation> {
        self.maintenance.as_ref()
    }
    /// Returns all unresolved attempts, including unknown remote outcomes.
    #[must_use]
    pub fn attempts(&self) -> &[MoveAttempt] {
        &self.attempts
    }
    /// Returns the next sequence the current controller may allocate once.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
    /// Returns the committed terminal-history chain, retained on controller expiry.
    #[must_use]
    pub const fn progress(&self) -> Option<ProgressHead> {
        self.progress
    }

    /// Derives the current maintenance intent for atomic publication by the adapter.
    /// Older nodes' intent must remain in the adapter's physical-node registry.
    pub fn node_intent(&self) -> Result<Option<NodeIntent>> {
        self.maintenance
            .as_ref()
            .map(|operation| NodeIntent::maintenance(self.scope, operation))
            .transpose()
    }

    /// Builds one immutable retirement page without changing the current head.
    /// The adapter writes it first, then CAS-publishes `JournalTransition::Retire`.
    pub fn retirement_page(&self, ids: &[AttemptId]) -> Result<ProgressPage> {
        if ids.is_empty() || ids.len() > MAX_ACTIVE_ATTEMPTS {
            return Err(OperationError::Invalid("invalid retirement batch"));
        }
        let entries = self
            .attempts
            .iter()
            .filter(|attempt| ids.contains(&attempt.spec.id))
            .cloned()
            .collect::<Vec<_>>();
        if entries.len() != ids.len() {
            return Err(OperationError::NotFound);
        }
        let operation = entries[0].spec.id.operation;
        ProgressPage::new(
            self.scope,
            operation,
            self.next_progress_sequence()?,
            self.progress.map(|head| head.digest),
            entries,
        )
    }

    fn next_progress_sequence(&self) -> Result<u64> {
        self.progress.map_or(Ok(1), |head| {
            head.sequence
                .checked_add(1)
                .ok_or(OperationError::Invalid("progress sequence overflow"))
        })
    }
    /// Returns restore demand still charged to unresolved planned attempts.
    #[must_use]
    pub fn reserved_restore_bytes(&self) -> u64 {
        self.attempts.iter().map(|a| a.spec.cost.disk_bytes).sum()
    }

    /// Calculates an exact CAS successor for acquiring or renewing the controller.
    /// Expiry never removes an attempt or frees its resource permit.
    pub fn claim(
        &self,
        profile: FleetProfile,
        expected_revision: u64,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<Self> {
        let profile = profile.validate()?;
        self.check_revision_and_time(expected_revision, now_ms)?;
        if !nonzero(claimant.as_bytes()) {
            return Err(OperationError::Invalid("zero controller claimant"));
        }
        let epoch = match self.controller {
            Some(current) if now_ms < current.expires_at_ms => {
                if current.claimant != claimant {
                    return Err(OperationError::Fenced);
                }
                current.epoch
            }
            Some(current) => current
                .epoch
                .checked_add(1)
                .ok_or(OperationError::Invalid("controller epoch overflow"))?,
            None => 1,
        };
        let mut next = self.clone();
        next.controller = Some(ControllerLease {
            claimant,
            epoch,
            expires_at_ms: now_ms
                .checked_add(profile.controller_lease_ms)
                .ok_or(OperationError::Invalid("controller lease time overflow"))?,
        });
        next.finish_transition(now_ms)?;
        Ok(next)
    }

    /// Calculates a pure successor; the adapter must CAS the exact current head.
    /// An event is accepted only by the live controller epoch and exact attempt.
    pub fn transition(
        &self,
        profile: FleetProfile,
        expected_revision: u64,
        controller_epoch: u64,
        now_ms: i64,
        transition: JournalTransition,
    ) -> Result<Self> {
        let profile = profile.validate()?;
        self.check_revision_and_time(expected_revision, now_ms)?;
        let controller = self.controller.ok_or(OperationError::Fenced)?;
        if controller.epoch != controller_epoch || now_ms >= controller.expires_at_ms {
            return Err(OperationError::Fenced);
        }
        // Absence resolution must fence delayed authorizations even when the
        // attempt already has no Unknown marker and otherwise stays identical.
        let fence_unaccepted = matches!(transition, JournalTransition::ResolveUnaccepted { .. });
        let mut next = self.clone();
        match transition {
            JournalTransition::BeginMaintenance(operation) => {
                operation.validate()?;
                if operation.phase != MaintenancePhase::Requested
                    || operation.created_at_ms > now_ms
                {
                    return Err(OperationError::Invalid(
                        "maintenance request is not initial",
                    ));
                }
                if let Some(old) = &self.maintenance {
                    if old.id == operation.id {
                        if old.request_digest != operation.request_digest
                            || old.node != operation.node
                            || old.created_at_ms != operation.created_at_ms
                        {
                            return Err(OperationError::Conflict);
                        }
                        return Ok(self.clone());
                    }
                    if old.phase != MaintenancePhase::Completed {
                        return Err(OperationError::Busy);
                    }
                }
                if now_ms >= operation.deadline_ms {
                    return Err(OperationError::Deadline);
                }
                next.maintenance = Some(operation);
            }
            JournalTransition::Maintenance(event) => {
                let operation = next.maintenance.as_mut().ok_or(OperationError::NotFound)?;
                if matches!(
                    event,
                    MaintenanceEvent::ReadyToClose(_) | MaintenanceEvent::Stopped(_)
                ) && next.attempts.iter().any(|a| {
                    a.spec.source_node == operation.node
                        || a.spec.destination_node == operation.node
                }) {
                    return Err(OperationError::Invalid(
                        "node still has unresolved movement",
                    ));
                }
                operation.apply(event, now_ms)?;
            }
            JournalTransition::Allocate(spec) => {
                spec.validate()?;
                if spec.target.application() != self.scope.application
                    || spec.id.sequence != self.next_sequence
                {
                    return Err(OperationError::Invalid(
                        "attempt sequence or application mismatch",
                    ));
                }
                if now_ms >= spec.deadline_ms {
                    return Err(OperationError::Deadline);
                }
                if let Some(operation) = &self.maintenance {
                    if spec.destination_node == operation.node {
                        return Err(OperationError::Invalid(
                            "receiver has durable maintenance intent",
                        ));
                    }
                    if spec.source_node == operation.node {
                        if spec.id.operation != operation.id
                            || spec.source != operation.session
                            || operation.phase != MaintenancePhase::Evacuating
                        {
                            return Err(OperationError::Invalid(
                                "maintenance source identity or phase mismatch",
                            ));
                        }
                        if now_ms >= operation.deadline_ms {
                            return Err(OperationError::Deadline);
                        }
                    }
                }
                if self
                    .attempts
                    .iter()
                    .any(|a| a.spec.target.cell_id() == spec.target.cell_id())
                {
                    return Err(OperationError::Busy);
                }
                let bytes = self
                    .reserved_restore_bytes()
                    .checked_add(spec.cost.disk_bytes)
                    .ok_or(OperationError::Budget)?;
                if self.attempts.len() >= profile.max_inflight || bytes > profile.max_restore_bytes
                {
                    return Err(OperationError::Budget);
                }
                next.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .ok_or(OperationError::Invalid("attempt sequence overflow"))?;
                next.attempts.push(MoveAttempt::new(spec)?);
            }
            JournalTransition::Attempt { id, event } => {
                if matches!(event, AttemptEvent::BeginMaintenanceRelease) {
                    let operation = self.maintenance.as_ref().ok_or(OperationError::Invalid(
                        "busy release lacks maintenance intent",
                    ))?;
                    let attempt = self
                        .attempts
                        .iter()
                        .find(|attempt| attempt.spec.id == id)
                        .ok_or(OperationError::NotFound)?;
                    if attempt.spec.id.operation != operation.id
                        || attempt.spec.source_node != operation.node
                        || attempt.spec.source != operation.session
                        || operation.phase != MaintenancePhase::Evacuating
                    {
                        return Err(OperationError::Invalid(
                            "busy release maintenance identity mismatch",
                        ));
                    }
                    if now_ms >= operation.deadline_ms {
                        return Err(OperationError::Deadline);
                    }
                }
                let attempt = next
                    .attempts
                    .iter_mut()
                    .find(|a| a.spec.id == id)
                    .ok_or(OperationError::NotFound)?;
                attempt.apply(event, now_ms)?;
            }
            JournalTransition::ResolveUnaccepted { id, effect } => {
                let attempt = next
                    .attempts
                    .iter_mut()
                    .find(|a| a.spec.id == id)
                    .ok_or(OperationError::NotFound)?;
                attempt.resolve_unaccepted(effect, now_ms)?;
            }
            JournalTransition::Retire { progress } => {
                progress.validate()?;
                if progress.scope != self.scope
                    || progress.sequence != self.next_progress_sequence()?
                    || progress.previous != self.progress.map(|head| head.digest)
                    || progress.entries.len() > MAX_ACTIVE_ATTEMPTS
                {
                    return Err(OperationError::Invalid(
                        "progress chain does not extend the current head",
                    ));
                }
                for entry in &progress.entries {
                    let current = self
                        .attempts
                        .iter()
                        .find(|attempt| attempt.spec.id == entry.spec.id)
                        .ok_or(OperationError::NotFound)?;
                    if current != entry {
                        return Err(OperationError::Invalid(
                            "historical attempt differs from active permit",
                        ));
                    }
                }
                next.progress = Some(ProgressHead {
                    digest: progress.digest()?,
                    sequence: progress.sequence,
                });
                next.attempts.retain(|attempt| {
                    !progress
                        .entries
                        .iter()
                        .any(|entry| entry.spec.id == attempt.spec.id)
                });
            }
        }
        if next == *self && !fence_unaccepted {
            return Ok(next);
        }
        next.finish_transition(now_ms)?;
        Ok(next)
    }

    fn check_revision_and_time(&self, expected_revision: u64, now_ms: i64) -> Result<()> {
        if expected_revision != self.revision {
            return Err(OperationError::Conflict);
        }
        if now_ms < self.last_observed_ms {
            return Err(OperationError::Invalid(
                "journal observation time regressed",
            ));
        }
        Ok(())
    }

    fn finish_transition(&mut self, now_ms: i64) -> Result<()> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(OperationError::Invalid("journal revision overflow"))?;
        self.last_observed_ms = now_ms;
        self.validate()
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self.last_observed_ms < 0
            || self.next_sequence == 0
            || self.attempts.len() > MAX_ACTIVE_ATTEMPTS
        {
            return Err(OperationError::Invalid("invalid fleet head"));
        }
        if let Some(lease) = self.controller {
            if !nonzero(lease.claimant.as_bytes())
                || lease.epoch == 0
                || lease.expires_at_ms <= self.last_observed_ms
                || self.revision == 0
            {
                return Err(OperationError::Invalid("invalid stored controller lease"));
            }
        } else if self.revision != 0
            || !self.attempts.is_empty()
            || self.maintenance.is_some()
            || self.next_sequence != 1
            || self.progress.is_some()
        {
            return Err(OperationError::Invalid(
                "journal state lacks controller epoch",
            ));
        }
        if let Some(progress) = self.progress {
            progress.validate()?;
            if progress.sequence >= self.next_sequence || progress.sequence > self.revision {
                return Err(OperationError::Invalid(
                    "progress chain is ahead of journal allocations",
                ));
            }
        }
        if let Some(operation) = &self.maintenance {
            operation.validate()?;
            if operation.created_at_ms > self.last_observed_ms {
                return Err(OperationError::Invalid(
                    "maintenance record is ahead of its head",
                ));
            }
        }
        let mut bytes = 0_u64;
        let mut previous_sequence = 0;
        for (i, attempt) in self.attempts.iter().enumerate() {
            attempt.validate()?;
            if attempt
                .recovered()
                .is_some_and(|e| e.recovery.basis().scope != self.scope)
            {
                return Err(OperationError::Invalid("recovery belongs to another fleet"));
            }
            if attempt
                .completed_at_ms
                .is_some_and(|at| at > self.last_observed_ms)
            {
                return Err(OperationError::Invalid(
                    "attempt result is ahead of its head",
                ));
            }
            if attempt.spec.target.application() != self.scope.application
                || attempt.spec.id.sequence >= self.next_sequence
                || attempt.spec.id.sequence <= previous_sequence
                || self.attempts[..i]
                    .iter()
                    .any(|a| a.spec.target.cell_id() == attempt.spec.target.cell_id())
            {
                return Err(OperationError::Invalid(
                    "duplicate or unordered active attempt",
                ));
            }
            previous_sequence = attempt.spec.id.sequence;
            bytes = bytes
                .checked_add(attempt.spec.cost.disk_bytes)
                .ok_or(OperationError::Budget)?;
        }
        if bytes > MAX_RESTORE_BYTES {
            return Err(OperationError::Budget);
        }
        Ok(())
    }
}
