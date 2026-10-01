use crate::control::RootRef;
use crate::identity::{CellTarget, Digest, IncarnationId, NodeId, SessionId};

use super::{AttemptId, DrainBlocker, OperationError, RecoveredActivation, Result, nonzero};

/// Conservative resources charged before a planned receive begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferCost {
    /// Reserved native memory and retained buffers in bytes.
    pub memory_bytes: u64,
    /// Conservative local restore and scratch disk demand in bytes.
    pub disk_bytes: u64,
    /// File descriptors needed by the incoming Cell and restoration.
    pub file_descriptors: u32,
    /// Worker/restore job credits needed by this attempt.
    pub job_credits: u32,
}

impl TransferCost {
    /// Rejects unknown cost represented as zero.
    pub fn validate(self) -> Result<Self> {
        if self.memory_bytes == 0
            || self.disk_bytes == 0
            || self.file_descriptors == 0
            || self.job_credits == 0
        {
            return Err(OperationError::Invalid("unknown transfer cost"));
        }
        Ok(self)
    }
}

/// Immutable exact identity and resource demand allocated by the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveAttemptSpec {
    /// Never-reused operation/sequence identity.
    pub id: AttemptId,
    /// Verified tenant/application/namespace partition target.
    pub target: CellTarget,
    /// Cell incarnation observed at the source.
    pub incarnation: IncarnationId,
    /// Physical donor node.
    pub source_node: NodeId,
    /// Exact source boot session; a reboot cannot adopt its actor handle.
    pub source: SessionId,
    /// Exact activation generation checked by source release.
    pub generation: u64,
    /// Source ownership epoch checked by release evidence.
    pub source_epoch: u64,
    /// Preferred physical receiver.
    pub destination_node: NodeId,
    /// Exact preferred receiver boot session.
    pub destination: SessionId,
    /// Cost reserved by both the fleet permit and local receiver.
    pub cost: TransferCost,
    /// Canonical digest of the planner's observation inputs.
    pub snapshot_digest: Digest,
    /// Latest time at which new source release may be dispatched.
    pub deadline_ms: i64,
}

impl MoveAttemptSpec {
    /// Validates all immutable attempt fields before allocating resources.
    pub fn validate(&self) -> Result<()> {
        self.id.validate()?;
        self.cost.validate()?;
        if !nonzero(self.incarnation.as_bytes())
            || !nonzero(self.source_node.as_bytes())
            || !nonzero(self.destination_node.as_bytes())
            || !nonzero(self.source.as_bytes())
            || !nonzero(self.destination.as_bytes())
            || !nonzero(self.snapshot_digest.as_bytes())
            || !nonzero(self.target.tenant().as_bytes())
            || !nonzero(self.target.application().as_bytes())
            || !nonzero(self.target.namespace().as_bytes())
            || self.source == self.destination
            || self.source_node == self.destination_node
            || self.generation == 0
            || self.source_epoch == 0
            || self.deadline_ms < 0
        {
            return Err(OperationError::Invalid("invalid movement identity"));
        }
        Ok(())
    }
}

/// Exact object-covered release or actor-backed serving position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedPosition {
    /// Cell incarnation this position belongs to.
    pub incarnation: IncarnationId,
    /// Cell authority epoch associated with the evidence.
    pub epoch: u64,
    /// Exact authoritative immutable root and sequence.
    pub root: RootRef,
}

impl PublishedPosition {
    pub(super) fn validate(&self) -> Result<()> {
        if !nonzero(self.incarnation.as_bytes())
            || self.epoch == 0
            || !nonzero(self.root.digest.as_bytes())
            || self.root.commit_sequence > i64::MAX as u64
            || self.root.checksum & cellule_ltx::types::CHECKSUM_FLAG == 0
        {
            return Err(OperationError::Invalid(
                "invalid published movement position",
            ));
        }
        Ok(())
    }
}

/// Admitted receiver reservation; it provides resources, never Cell authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiverReservation {
    /// Exact session owning the reservation for this attempt.
    pub session: SessionId,
    /// Time after which local cancellation may begin; not proof of cleanup.
    pub expires_at_ms: i64,
}

/// Verified current owner and actor readiness after normal acquisition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationEvidence {
    /// Actual serving node, which may differ from the preferred receiver.
    pub node: NodeId,
    /// Current serving boot session.
    pub session: SessionId,
    /// Current verified root and actor position.
    pub position: PublishedPosition,
}

/// Durable movement phase. Dispatch is recorded before starting remote work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum AttemptPhase {
    /// Fleet count/byte permit is allocated, with no remote effect yet.
    Planned = 1,
    /// Preparation may be accepted remotely; inspect before reallocation.
    Preparing = 2,
    /// Exact receiver resources have been admitted.
    Reserved = 3,
    /// Source release may be accepted and its outcome needs reconciliation.
    Releasing = 4,
    /// Source release is proven; serving elsewhere is not yet established.
    Released = 5,
    /// Ordinary receiver activation may be in progress.
    Activating = 6,
    /// Current actor-backed successor evidence is proven.
    Activated = 7,
    /// Remote cancellation/cleanup is requested and still charged.
    Cancelling = 8,
    /// No source release and no receiver work remain for this attempt.
    Cancelled = 9,
    /// Source release is proven; unused receiver cleanup is requested.
    /// This phase cannot turn relocation into pre-release cancellation.
    CleaningReceiver = 10,
    /// Canonical failed-source recovery is accepted or awaiting inspection.
    Recovering = 11,
    /// Recovery and current serving are proved; receiver cleanup remains separate.
    Recovered = 12,
}

/// Advisory next action selected from durable state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MovementAction {
    /// Prepare an exact-session admitted resource reservation.
    Prepare = 1,
    /// Quiesce and release the exact source generation through its actor.
    Release = 2,
    /// Activate by ordinary acquisition, consuming receiver reservation.
    Activate = 3,
    /// Inspect accepted work or authority before deciding what happened.
    Inspect = 4,
    /// Cancel and join unused receiver reservation/work.
    Cancel = 5,
    /// Terminal evidence permits removing this attempt from the active budget.
    Retire = 6,
    /// Recover an unresolved release using canonical failed-session proof.
    Recover = 7,
}

/// Replayable result or dispatch intent; input identity is supplied by the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptEvent {
    /// Persist preparation dispatch before a receiver side effect.
    BeginPrepare,
    /// Confirm exact receiver resource admission.
    Reserved(ReceiverReservation),
    /// Persist source release dispatch before a source side effect.
    BeginRelease,
    /// The source definitively refused without accepting a release.
    ReleaseRefused(DrainBlocker),
    /// Confirm exact source release and final authoritative root.
    Released(PublishedPosition),
    /// Persist ordinary activation dispatch.
    BeginActivate,
    /// Persist failed-source recovery dispatch without claiming a clean release.
    BeginRecover,
    /// Confirm pinned recovery and actor-backed serving independently of release.
    Recovered(Box<RecoveredActivation>),
    /// Confirm current actor-backed serving state at or after the release.
    Activated(ActivationEvidence),
    /// An accepted action needs observation; keep its phase and permit.
    OutcomeUnknown,
    /// Begin cancellation only before any source release could be accepted.
    /// After a proven release, request independent receiver cleanup instead.
    BeginCancel,
    /// Confirm receiver work is joined, reservation freed, and source unreleased.
    Cancelled,
    /// Confirm cleanup of an unused preferred reservation after another node won.
    ReceiverCleaned,
}

/// One durably charged attempt. Its private fields can change only by transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveAttempt {
    pub(super) spec: MoveAttemptSpec,
    pub(super) phase: AttemptPhase,
    pub(super) reservation: Option<ReceiverReservation>,
    pub(super) released: Option<PublishedPosition>,
    pub(super) activated: Option<ActivationEvidence>,
    pub(super) recovered: Option<Box<RecoveredActivation>>,
    pub(super) receiver_cleaned: bool,
    pub(super) blocker: Option<DrainBlocker>,
    pub(super) completed_at_ms: Option<i64>,
}

impl MoveAttempt {
    pub(super) fn new(spec: MoveAttemptSpec) -> Result<Self> {
        spec.validate()?;
        Ok(Self {
            spec,
            phase: AttemptPhase::Planned,
            reservation: None,
            released: None,
            activated: None,
            recovered: None,
            receiver_cleaned: false,
            blocker: None,
            completed_at_ms: None,
        })
    }

    /// Returns the immutable attempt scope and cost.
    #[must_use]
    pub const fn spec(&self) -> &MoveAttemptSpec {
        &self.spec
    }
    /// Returns the persisted phase.
    #[must_use]
    pub const fn phase(&self) -> AttemptPhase {
        self.phase
    }
    /// Returns a temporary blocker or unknown-outcome marker.
    #[must_use]
    pub const fn blocker(&self) -> Option<DrainBlocker> {
        self.blocker
    }
    /// Returns exact source release evidence, when proven.
    #[must_use]
    pub const fn released(&self) -> Option<&PublishedPosition> {
        self.released.as_ref()
    }
    /// Returns actual successor serving evidence, when proven.
    #[must_use]
    pub const fn activated(&self) -> Option<&ActivationEvidence> {
        self.activated.as_ref()
    }
    /// Returns failed-source recovery evidence, never a clean release position.
    #[must_use]
    pub fn recovered(&self) -> Option<&RecoveredActivation> {
        self.recovered.as_deref()
    }
    /// Returns receiver admission evidence, when proven.
    #[must_use]
    pub const fn reservation(&self) -> Option<ReceiverReservation> {
        self.reservation
    }

    /// Checks current actor-backed serving evidence against this exact release.
    /// This checks shape and required position, not authority or resource cleanup.
    pub fn validate_activation(&self, evidence: &ActivationEvidence) -> Result<()> {
        evidence.position.validate()?;
        let release = self
            .released
            .as_ref()
            .ok_or(OperationError::Invalid("activation without release"))?;
        if !nonzero(evidence.node.as_bytes())
            || !nonzero(evidence.session.as_bytes())
            || evidence.node == self.spec.source_node
            || evidence.session == self.spec.source
            || evidence.position.incarnation != release.incarnation
            || evidence.position.epoch <= release.epoch
            || !successor_position(&evidence.position, release)
            || (evidence.session == self.spec.destination
                && evidence.node != self.spec.destination_node)
        {
            return Err(OperationError::Invalid("successor activation mismatch"));
        }
        Ok(())
    }

    /// Returns the first proven activation or cancellation time for history.
    /// A duplicate reply cannot refresh the ordinary movement cooldown.
    #[must_use]
    pub const fn completed_at_ms(&self) -> Option<i64> {
        self.completed_at_ms
    }

    /// Selects work without assuming that an expired lease cancelled an effect.
    #[must_use]
    pub fn next_action(&self) -> MovementAction {
        if self.blocker == Some(DrainBlocker::OutcomeUnknown) {
            return MovementAction::Inspect;
        }
        match self.phase {
            AttemptPhase::Planned => MovementAction::Prepare,
            AttemptPhase::Reserved => MovementAction::Release,
            AttemptPhase::Released => MovementAction::Activate,
            AttemptPhase::Preparing
            | AttemptPhase::Releasing
            | AttemptPhase::Activating
            | AttemptPhase::Recovering => MovementAction::Inspect,
            AttemptPhase::Cancelling | AttemptPhase::CleaningReceiver => MovementAction::Cancel,
            AttemptPhase::Activated | AttemptPhase::Recovered if !self.receiver_cleaned => {
                MovementAction::Cancel
            }
            AttemptPhase::Activated | AttemptPhase::Recovered | AttemptPhase::Cancelled => {
                MovementAction::Retire
            }
        }
    }

    pub(super) fn can_retire(&self) -> bool {
        self.phase == AttemptPhase::Cancelled
            || (matches!(
                self.phase,
                AttemptPhase::Activated | AttemptPhase::Recovered
            ) && self.receiver_cleaned)
    }

    pub(super) fn apply(&mut self, event: AttemptEvent, now_ms: i64) -> Result<()> {
        let require_admission = || {
            if now_ms >= self.spec.deadline_ms {
                Err(OperationError::Deadline)
            } else {
                Ok(())
            }
        };
        match event {
            AttemptEvent::BeginPrepare if self.phase == AttemptPhase::Planned => {
                require_admission()?;
                self.phase = AttemptPhase::Preparing;
            }
            AttemptEvent::Reserved(reservation)
                if matches!(self.phase, AttemptPhase::Preparing | AttemptPhase::Reserved) =>
            {
                if reservation.session != self.spec.destination
                    || reservation.expires_at_ms <= now_ms
                    || self.reservation.is_some_and(|old| old != reservation)
                {
                    return Err(OperationError::Invalid("receiver reservation mismatch"));
                }
                self.reservation = Some(reservation);
                self.phase = AttemptPhase::Reserved;
                self.blocker = None;
            }
            AttemptEvent::BeginRelease
                if self.phase == AttemptPhase::Reserved
                    && self.blocker != Some(DrainBlocker::OutcomeUnknown) =>
            {
                require_admission()?;
                if self.reservation.is_none_or(|r| r.expires_at_ms <= now_ms) {
                    return Err(OperationError::Invalid("receiver reservation expired"));
                }
                self.phase = AttemptPhase::Releasing;
                self.blocker = None;
            }
            AttemptEvent::ReleaseRefused(blocker)
                if self.phase == AttemptPhase::Releasing
                    && blocker != DrainBlocker::OutcomeUnknown =>
            {
                self.phase = AttemptPhase::Reserved;
                self.blocker = Some(blocker);
            }
            AttemptEvent::Released(position)
                if matches!(self.phase, AttemptPhase::Releasing | AttemptPhase::Released) =>
            {
                position.validate()?;
                if position.incarnation != self.spec.incarnation
                    || position.epoch != self.spec.source_epoch
                    || self.released.as_ref().is_some_and(|old| old != &position)
                {
                    return Err(OperationError::Invalid("source release position mismatch"));
                }
                self.released = Some(position);
                self.phase = AttemptPhase::Released;
                self.blocker = None;
            }
            AttemptEvent::BeginRecover if self.phase == AttemptPhase::Releasing => {
                self.phase = AttemptPhase::Recovering;
                self.blocker = None;
            }
            AttemptEvent::Recovered(evidence)
                if matches!(
                    self.phase,
                    AttemptPhase::Recovering | AttemptPhase::Recovered
                ) =>
            {
                evidence.validate()?;
                if evidence.recovery.basis().spec() != &self.spec
                    || evidence.recovery.recorded_at_ms() > now_ms
                    || self.recovered.as_ref().is_some_and(|old| old != &evidence)
                {
                    return Err(OperationError::Invalid("recovery movement input mismatch"));
                }
                self.recovered = Some(evidence);
                self.phase = AttemptPhase::Recovered;
                self.completed_at_ms.get_or_insert(now_ms);
                self.blocker = None;
            }
            AttemptEvent::BeginActivate if self.phase == AttemptPhase::Released => {
                // Recovery of an accepted release is allowed beyond its admission deadline.
                self.phase = AttemptPhase::Activating;
                self.blocker = None;
            }
            AttemptEvent::Activated(evidence)
                if matches!(
                    self.phase,
                    AttemptPhase::Activating
                        | AttemptPhase::Activated
                        | AttemptPhase::CleaningReceiver
                ) =>
            {
                self.validate_activation(&evidence)?;
                if self.activated.as_ref().is_some_and(|old| old != &evidence) {
                    return Err(OperationError::Invalid("successor activation mismatch"));
                }
                // Serving on the preferred session may be ordinary acquisition.
                // Resource consumption/cleanup needs independent executor proof.
                self.activated = Some(evidence);
                self.phase = AttemptPhase::Activated;
                self.completed_at_ms.get_or_insert(now_ms);
                self.blocker = None;
            }
            AttemptEvent::OutcomeUnknown
                if matches!(
                    self.phase,
                    AttemptPhase::Preparing
                        | AttemptPhase::Releasing
                        | AttemptPhase::Recovering
                        | AttemptPhase::Activating
                        | AttemptPhase::Cancelling
                        | AttemptPhase::CleaningReceiver
                ) || (matches!(
                    self.phase,
                    AttemptPhase::Activated | AttemptPhase::Recovered
                ) && !self.receiver_cleaned) =>
            {
                self.blocker = Some(DrainBlocker::OutcomeUnknown);
            }
            AttemptEvent::BeginCancel
                if matches!(
                    self.phase,
                    AttemptPhase::Released
                        | AttemptPhase::Activating
                        | AttemptPhase::CleaningReceiver
                ) && !self.receiver_cleaned =>
            {
                self.phase = AttemptPhase::CleaningReceiver;
                self.blocker = None;
            }
            AttemptEvent::BeginCancel
                if matches!(
                    self.phase,
                    AttemptPhase::Planned
                        | AttemptPhase::Preparing
                        | AttemptPhase::Reserved
                        | AttemptPhase::Cancelling
                ) =>
            {
                self.phase = AttemptPhase::Cancelling;
                self.blocker = None;
            }
            AttemptEvent::Cancelled
                if matches!(
                    self.phase,
                    AttemptPhase::Cancelling | AttemptPhase::Cancelled
                ) =>
            {
                self.phase = AttemptPhase::Cancelled;
                self.completed_at_ms.get_or_insert(now_ms);
                self.receiver_cleaned = true;
                self.blocker = None;
            }
            AttemptEvent::ReceiverCleaned
                if matches!(
                    self.phase,
                    AttemptPhase::Activated | AttemptPhase::Recovered
                ) =>
            {
                self.receiver_cleaned = true;
                self.blocker = None;
            }
            AttemptEvent::ReceiverCleaned if self.phase == AttemptPhase::CleaningReceiver => {
                self.receiver_cleaned = true;
                self.phase = AttemptPhase::Released;
                self.blocker = None;
            }
            _ => return Err(OperationError::Invalid("unproven movement transition")),
        }
        self.validate()
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if matches!(
            self.phase,
            AttemptPhase::Activated | AttemptPhase::Recovered | AttemptPhase::Cancelled
        ) != self.completed_at_ms.is_some()
            || self.completed_at_ms.is_some_and(|at| at < 0)
        {
            return Err(OperationError::Invalid(
                "movement completion time disagrees with phase",
            ));
        }
        if matches!(self.phase, AttemptPhase::Planned | AttemptPhase::Preparing)
            && self.reservation.is_some()
        {
            return Err(OperationError::Invalid("reservation precedes admission"));
        }
        if let Some(r) = self.reservation
            && (r.session != self.spec.destination || r.expires_at_ms < 0)
        {
            return Err(OperationError::Invalid("invalid stored reservation"));
        }
        if matches!(
            self.phase,
            AttemptPhase::Reserved
                | AttemptPhase::Releasing
                | AttemptPhase::Released
                | AttemptPhase::Activating
                | AttemptPhase::Activated
                | AttemptPhase::CleaningReceiver
                | AttemptPhase::Recovering
                | AttemptPhase::Recovered
        ) && self.reservation.is_none()
        {
            return Err(OperationError::Invalid("movement lost reservation"));
        }
        if matches!(
            self.phase,
            AttemptPhase::Released
                | AttemptPhase::Activating
                | AttemptPhase::Activated
                | AttemptPhase::CleaningReceiver
        ) != self.released.is_some()
        {
            return Err(OperationError::Invalid(
                "movement release evidence disagrees with phase",
            ));
        }
        if let Some(p) = &self.released {
            p.validate()?;
            if p.incarnation != self.spec.incarnation || p.epoch != self.spec.source_epoch {
                return Err(OperationError::Invalid("stored release scope mismatch"));
            }
        }
        if (self.phase == AttemptPhase::Activated) != self.activated.is_some() {
            return Err(OperationError::Invalid(
                "movement activation evidence disagrees with phase",
            ));
        }
        if let Some(e) = &self.activated {
            e.position.validate()?;
            let p = self
                .released
                .as_ref()
                .ok_or(OperationError::Invalid("missing release evidence"))?;
            if !nonzero(e.node.as_bytes())
                || !nonzero(e.session.as_bytes())
                || e.node == self.spec.source_node
                || e.session == self.spec.source
                || e.position.incarnation != p.incarnation
                || e.position.epoch <= p.epoch
                || !successor_position(&e.position, p)
                || (e.session == self.spec.destination && e.node != self.spec.destination_node)
            {
                return Err(OperationError::Invalid("invalid stored activation"));
            }
        }
        if (self.phase == AttemptPhase::Recovered) != self.recovered.is_some() {
            return Err(OperationError::Invalid(
                "recovery evidence disagrees with phase",
            ));
        }
        if let Some(evidence) = &self.recovered {
            evidence.validate()?;
            if evidence.recovery.basis().spec() != &self.spec {
                return Err(OperationError::Invalid("stored recovery input mismatch"));
            }
            if self
                .completed_at_ms
                .is_none_or(|at| at < evidence.recovery.recorded_at_ms())
            {
                return Err(OperationError::Invalid(
                    "recovery completion precedes evidence",
                ));
            }
        }
        if self.receiver_cleaned
            && !matches!(
                self.phase,
                AttemptPhase::Released
                    | AttemptPhase::Activating
                    | AttemptPhase::Activated
                    | AttemptPhase::Cancelled
                    | AttemptPhase::Recovered
            )
        {
            return Err(OperationError::Invalid("premature reservation cleanup"));
        }
        if self.phase == AttemptPhase::Cancelled && !self.receiver_cleaned {
            return Err(OperationError::Invalid("cancelled work lacks cleanup"));
        }
        if self.blocker == Some(DrainBlocker::OutcomeUnknown)
            && !matches!(
                self.phase,
                AttemptPhase::Preparing
                    | AttemptPhase::Releasing
                    | AttemptPhase::Recovering
                    | AttemptPhase::Activating
                    | AttemptPhase::Cancelling
                    | AttemptPhase::CleaningReceiver
            )
            && !(matches!(
                self.phase,
                AttemptPhase::Activated | AttemptPhase::Recovered
            ) && !self.receiver_cleaned)
        {
            return Err(OperationError::Invalid(
                "unknown outcome has no pending action",
            ));
        }
        Ok(())
    }
}

pub(super) fn successor_position(
    current: &PublishedPosition,
    released: &PublishedPosition,
) -> bool {
    current.root.commit_sequence >= released.root.commit_sequence
        && current.root.txid >= released.root.txid
        && (current.root.commit_sequence != released.root.commit_sequence
            || current.root == released.root)
}
