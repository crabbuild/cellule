use crate::identity::{Digest, NodeId, SessionId};

use super::{
    ActivationEvidence, AttemptId, AttemptPhase, DrainBlocker, DrainEvidence, FleetHead,
    FleetScope, MaintenanceOperation, MaintenancePhase, MoveAttempt, MovementAction,
    OperationError, PublishedPosition, ReceiverReservation, RecoveredActivation, Result, nonzero,
};

/// Node lifecycle work recorded by one maintenance operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MaintenanceAction {
    /// Apply the persisted physical-node intent before new role admission.
    Cordon = 1,
    /// Reconcile reader replacements and foreign follower obligations.
    SettleRoles = 2,
    /// Complete the existing host drain after relocation is proven.
    Finalize = 3,
    /// Reconstruct actual progress without starting a new role transition.
    Inspect = 4,
}

/// Exact action payload cloned from the current committed journal state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetActionKind {
    /// Execute or inspect one immutable session/generation-bound movement.
    Movement {
        /// Remote effect or inspection; retirement is journal-local.
        action: MovementAction,
        /// Current charged attempt and its dispatch/evidence state.
        attempt: Box<MoveAttempt>,
    },
    /// Execute or inspect a physical-node maintenance operation.
    Maintenance {
        /// Lifecycle effect to reconcile.
        action: MaintenanceAction,
        /// Current committed operation and targeted session.
        operation: Box<MaintenanceOperation>,
    },
}

/// Bounded journal-bound action envelope. It is not an authentication capability.
///
/// The receiving application authenticates the caller and reads the journal
/// before `authorize_against`. Existing accepted actions may finish after a
/// controller change; accepting a new effect requires the current live epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetAction {
    pub(super) scope: FleetScope,
    pub(super) journal_revision: u64,
    pub(super) controller: SessionId,
    pub(super) controller_epoch: u64,
    pub(super) issued_at_ms: i64,
    pub(super) kind: FleetActionKind,
}

impl FleetAction {
    /// Returns the exact fleet/application authorization scope.
    #[must_use]
    pub const fn scope(&self) -> FleetScope {
        self.scope
    }
    /// Returns the committed revision authorizing first acceptance.
    #[must_use]
    pub const fn journal_revision(&self) -> u64 {
        self.journal_revision
    }
    /// Returns the controller boot identity covered by the journal lease.
    #[must_use]
    pub const fn controller(&self) -> SessionId {
        self.controller
    }
    /// Returns the controller fencing epoch.
    #[must_use]
    pub const fn controller_epoch(&self) -> u64 {
        self.controller_epoch
    }
    /// Returns the logical issuance time; receivers recheck their current time.
    #[must_use]
    pub const fn issued_at_ms(&self) -> i64 {
        self.issued_at_ms
    }
    /// Returns the immutable exact target/effect payload.
    #[must_use]
    pub const fn kind(&self) -> &FleetActionKind {
        &self.kind
    }

    /// Returns a stable deduplication key across controller adoption and retries.
    /// Authorization revision/time are deliberately excluded; the current
    /// journal is still required to accept an effect for the first time.
    pub fn key(&self) -> Result<Digest> {
        self.validate()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-action-key.v1\0");
        hash.update(self.scope.fleet.as_bytes());
        hash.update(self.scope.application.as_bytes());
        match &self.kind {
            FleetActionKind::Movement { action, attempt } => {
                return Ok(movement_key(self.scope, *action, &attempt.spec));
            }
            FleetActionKind::Maintenance { action, operation } => {
                hash.update(&[2, *action as u8]);
                hash.update(operation.id.as_bytes());
                hash.update(operation.node.as_bytes());
                hash.update(operation.session.as_bytes());
                hash.update(&operation.intent_revision.to_be_bytes());
            }
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }

    /// Verifies exact current journal state before first local acceptance.
    /// Caller identity and backend authenticity are checked by the application.
    pub fn authorize_against(&self, head: &FleetHead, now_ms: i64) -> Result<()> {
        self.validate()?;
        head.check_action_controller(now_ms)?;
        if self.scope != head.scope
            || self.journal_revision != head.revision
            || self.issued_at_ms > now_ms
        {
            return Err(OperationError::Conflict);
        }
        let expected = match &self.kind {
            FleetActionKind::Movement { action, attempt } => {
                head.movement_action(attempt.spec.id, *action, self.issued_at_ms)?
            }
            FleetActionKind::Maintenance { action, .. } => {
                head.maintenance_action(*action, self.issued_at_ms)?
            }
        };
        if *self != expected {
            return Err(OperationError::Fenced);
        }
        self.check_admission_deadline(now_ms)
    }

    pub(super) fn check_admission_deadline(&self, now_ms: i64) -> Result<()> {
        if let FleetActionKind::Movement { action, attempt } = &self.kind {
            if matches!(action, MovementAction::Prepare | MovementAction::Release)
                && now_ms >= attempt.spec.deadline_ms
            {
                return Err(OperationError::Deadline);
            }
            if *action == MovementAction::Release
                && attempt
                    .reservation
                    .is_none_or(|r| r.expires_at_ms <= now_ms)
            {
                return Err(OperationError::Invalid("release reservation expired"));
            }
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self.journal_revision == 0
            || self.controller_epoch == 0
            || !nonzero(self.controller.as_bytes())
            || self.issued_at_ms < 0
        {
            return Err(OperationError::Invalid(
                "invalid fleet action authorization",
            ));
        }
        match &self.kind {
            FleetActionKind::Movement { action, attempt } => {
                attempt.validate()?;
                if attempt.spec.target.application() != self.scope.application
                    || !movement_allowed(attempt, *action)
                {
                    return Err(OperationError::Invalid("movement dispatch phase mismatch"));
                }
            }
            FleetActionKind::Maintenance { action, operation } => {
                operation.validate()?;
                if !maintenance_allowed(operation, *action) {
                    return Err(OperationError::Invalid(
                        "maintenance dispatch phase mismatch",
                    ));
                }
            }
        }
        self.check_admission_deadline(self.issued_at_ms)
    }
}

impl FleetHead {
    fn check_action_controller(&self, now_ms: i64) -> Result<()> {
        if now_ms < self.last_observed_ms {
            return Err(OperationError::Invalid("action time regressed"));
        }
        if self
            .controller
            .is_none_or(|lease| now_ms >= lease.expires_at_ms)
        {
            return Err(OperationError::Fenced);
        }
        Ok(())
    }

    /// Builds a movement action only after its dispatch phase was CAS-published.
    pub fn movement_action(
        &self,
        id: AttemptId,
        action: MovementAction,
        now_ms: i64,
    ) -> Result<FleetAction> {
        let attempt = self
            .attempts
            .iter()
            .find(|attempt| attempt.spec.id == id)
            .ok_or(OperationError::NotFound)?;
        self.make_action(
            FleetActionKind::Movement {
                action,
                attempt: Box::new(attempt.clone()),
            },
            now_ms,
        )
    }

    /// Builds lifecycle work only from the current committed maintenance operation.
    pub fn maintenance_action(
        &self,
        action: MaintenanceAction,
        now_ms: i64,
    ) -> Result<FleetAction> {
        let operation = self.maintenance.as_ref().ok_or(OperationError::NotFound)?;
        self.make_action(
            FleetActionKind::Maintenance {
                action,
                operation: Box::new(operation.clone()),
            },
            now_ms,
        )
    }

    fn make_action(&self, kind: FleetActionKind, now_ms: i64) -> Result<FleetAction> {
        self.check_action_controller(now_ms)?;
        let controller = self.controller.ok_or(OperationError::Fenced)?;
        let action = FleetAction {
            scope: self.scope,
            journal_revision: self.revision,
            controller: controller.claimant,
            controller_epoch: controller.epoch,
            issued_at_ms: now_ms,
            kind,
        };
        action.validate()?;
        Ok(action)
    }
}

fn movement_allowed(attempt: &MoveAttempt, action: MovementAction) -> bool {
    if action == MovementAction::Inspect {
        return true;
    }
    if attempt.blocker == Some(DrainBlocker::OutcomeUnknown) {
        return false;
    }
    match action {
        MovementAction::Prepare => attempt.phase == AttemptPhase::Preparing,
        MovementAction::Release => attempt.phase == AttemptPhase::Releasing,
        MovementAction::Activate => attempt.phase == AttemptPhase::Activating,
        MovementAction::Recover => attempt.phase == AttemptPhase::Recovering,
        MovementAction::Cancel => {
            attempt.phase == AttemptPhase::Cancelling
                || attempt.phase == AttemptPhase::CleaningReceiver
                || (matches!(
                    attempt.phase,
                    AttemptPhase::Activated | AttemptPhase::Recovered
                ) && !attempt.receiver_cleaned)
        }
        MovementAction::Inspect => true,
        MovementAction::Retire => false,
    }
}

fn maintenance_allowed(operation: &MaintenanceOperation, action: MaintenanceAction) -> bool {
    match action {
        MaintenanceAction::Cordon => operation.phase != MaintenancePhase::Completed,
        MaintenanceAction::SettleRoles => operation.phase == MaintenancePhase::Evacuating,
        MaintenanceAction::Finalize => operation.phase == MaintenancePhase::Closing,
        MaintenanceAction::Inspect => true,
    }
}

/// Bounded remote status; transport/source errors remain on the adapter result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetOutcome {
    /// Exact preferred receiver resources have been admitted.
    Reserved(ReceiverReservation),
    /// Exact source release and object-covered position are proven.
    Released(PublishedPosition),
    /// A current owner and actor-backed serving position are proven.
    Activated(ActivationEvidence),
    /// Failed-source recovery, distinct from a clean release followed by activation.
    Recovered(Box<RecoveredActivation>),
    /// No effect was accepted; this is distinct from an ambiguous transport error.
    Rejected(DrainBlocker),
    /// Accepted work or an obligation still blocks progress; retain its permit.
    Blocked(DrainBlocker),
    /// Accepted remote work has no confirmed result yet.
    Unknown,
    /// Receiver work joined and its unused reservation no longer exists.
    ReceiverCleaned,
    /// The exact local node session has applied its persistent admission closure.
    Cordoned,
    /// Complete role inventory at the named barrier reports zero obligations.
    RolesSettled {
        /// Digest of the authoritative inventory used at the final barrier.
        inventory: Digest,
    },
    /// Complete relocation, runtime/facility shutdown, and withdrawal evidence.
    Stopped(DrainEvidence),
}

/// Canonical bounded action result for an authenticated origin session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetActionOutcome {
    /// Exact fleet/application scope checked by the receiving adapter.
    pub scope: FleetScope,
    /// Stable action identity, independent of controller adoption.
    pub action_key: Digest,
    /// Authenticated physical node reporting the result.
    pub node: NodeId,
    /// Exact boot session that performed or inspected the effect.
    pub session: SessionId,
    /// Logical time the evidence was inspected, not merely republished.
    pub observed_at_ms: i64,
    /// Typed bounded status; this record itself grants no Cell authority.
    pub outcome: FleetOutcome,
}

impl FleetActionOutcome {
    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if !nonzero(self.action_key.as_bytes())
            || !nonzero(self.node.as_bytes())
            || !nonzero(self.session.as_bytes())
            || self.observed_at_ms < 0
        {
            return Err(OperationError::Invalid("invalid fleet result identity"));
        }
        match &self.outcome {
            FleetOutcome::Reserved(r)
                if r.session != self.session || r.expires_at_ms <= self.observed_at_ms =>
            {
                Err(OperationError::Invalid(
                    "reservation result is not admitted",
                ))
            }
            FleetOutcome::Released(p) => p.validate(),
            FleetOutcome::Activated(e) => {
                e.position.validate()?;
                if e.node != self.node || e.session != self.session {
                    return Err(OperationError::Invalid("activation result origin mismatch"));
                }
                Ok(())
            }
            FleetOutcome::Recovered(e) => {
                e.validate()?;
                if e.serving.node != self.node
                    || e.serving.session != self.session
                    || e.recovery.recorded_at_ms() > self.observed_at_ms
                {
                    return Err(OperationError::Invalid(
                        "recovery result origin or time mismatch",
                    ));
                }
                Ok(())
            }
            FleetOutcome::RolesSettled { inventory } if !nonzero(inventory.as_bytes()) => {
                Err(OperationError::Invalid("role result lacks inventory"))
            }
            FleetOutcome::Stopped(e)
                if e.node != self.node
                    || e.session != self.session
                    || !e.ready_to_close()
                    || !e.facilities_closed
                    || !e.stopped
                    || !e.withdrawn =>
            {
                Err(OperationError::Invalid(
                    "stopped result lacks terminal proof",
                ))
            }
            FleetOutcome::Rejected(DrainBlocker::OutcomeUnknown)
            | FleetOutcome::Blocked(DrainBlocker::OutcomeUnknown) => Err(OperationError::Invalid(
                "an unknown result must use the unknown outcome",
            )),
            _ => Ok(()),
        }
    }

    /// Binds a reply to the exact issued action and authenticated local endpoint.
    /// Readiness/fresh authority checks remain required before counting relocation.
    pub fn validate_for(&self, action: &FleetAction) -> Result<()> {
        self.validate()?;
        if self.scope != action.scope || self.action_key != action.key()? {
            return Err(OperationError::Invalid("fleet reply action mismatch"));
        }
        let permitted = match &action.kind {
            FleetActionKind::Movement {
                action: kind,
                attempt,
            } => {
                let source =
                    self.node == attempt.spec.source_node && self.session == attempt.spec.source;
                let receiver = self.node == attempt.spec.destination_node
                    && self.session == attempt.spec.destination;
                match &self.outcome {
                    FleetOutcome::Reserved(_) => {
                        receiver
                            && matches!(kind, MovementAction::Prepare | MovementAction::Inspect)
                    }
                    FleetOutcome::Released(p) => {
                        source
                            && matches!(kind, MovementAction::Release | MovementAction::Inspect)
                            && p.incarnation == attempt.spec.incarnation
                            && p.epoch == attempt.spec.source_epoch
                    }
                    FleetOutcome::Activated(e) => {
                        matches!(kind, MovementAction::Activate | MovementAction::Inspect)
                            && e.position.incarnation == attempt.spec.incarnation
                            && e.position.epoch > attempt.spec.source_epoch
                            && e.session != attempt.spec.source
                            && (e.node != attempt.spec.destination_node || receiver)
                    }
                    FleetOutcome::Recovered(e) => {
                        receiver
                            && matches!(kind, MovementAction::Recover | MovementAction::Inspect)
                            && e.recovery.basis().spec() == attempt.spec()
                            && e.recovery.basis().scope == action.scope
                    }
                    FleetOutcome::ReceiverCleaned => {
                        receiver && matches!(kind, MovementAction::Cancel | MovementAction::Inspect)
                    }
                    FleetOutcome::Rejected(_)
                    | FleetOutcome::Blocked(_)
                    | FleetOutcome::Unknown => match kind {
                        MovementAction::Release => source,
                        MovementAction::Prepare
                        | MovementAction::Activate
                        | MovementAction::Cancel
                        | MovementAction::Recover => receiver,
                        MovementAction::Inspect => source || receiver,
                        MovementAction::Retire => false,
                    },
                    _ => false,
                }
            }
            FleetActionKind::Maintenance {
                action: kind,
                operation,
            } => {
                self.node == operation.node
                    && self.session == operation.session
                    && match self.outcome {
                        FleetOutcome::Cordoned => {
                            matches!(kind, MaintenanceAction::Cordon | MaintenanceAction::Inspect)
                        }
                        FleetOutcome::RolesSettled { .. } => matches!(
                            kind,
                            MaintenanceAction::SettleRoles | MaintenanceAction::Inspect
                        ),
                        FleetOutcome::Stopped(_) => matches!(
                            kind,
                            MaintenanceAction::Finalize | MaintenanceAction::Inspect
                        ),
                        FleetOutcome::Rejected(_)
                        | FleetOutcome::Blocked(_)
                        | FleetOutcome::Unknown => true,
                        _ => false,
                    }
            }
        };
        if !permitted {
            return Err(OperationError::Invalid(
                "fleet reply effect or target mismatch",
            ));
        }
        Ok(())
    }
}

// Preserve the existing action index bytes for all prior movement effects.
// Recovery basis decoders use this same function, not an independent key format.
pub(super) fn movement_key(
    scope: FleetScope,
    action: MovementAction,
    spec: &super::MoveAttemptSpec,
) -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-action-key.v1\0");
    hash.update(scope.fleet.as_bytes());
    hash.update(scope.application.as_bytes());
    hash.update(&[1, action as u8]);
    hash.update(spec.id.operation.as_bytes());
    hash.update(&spec.id.sequence.to_be_bytes());
    hash.update(spec.target.cell_id().as_bytes());
    hash.update(spec.incarnation.as_bytes());
    hash.update(spec.source.as_bytes());
    hash.update(&spec.generation.to_be_bytes());
    hash.update(spec.destination.as_bytes());
    Digest::from_bytes(*hash.finalize().as_bytes())
}
