use crate::control::{Control, ControlState, Transition};
use crate::identity::{Digest, NodeId, SessionId};
use crate::node::NodeTakeoverProof;

use super::{
    AcceptedFleetAction, ActivationEvidence, FleetActionKind, FleetScope, MoveAttemptSpec,
    MovementAction, OperationError, PublishedPosition, Result,
};

/// Original pinned input of recovery of an unresolved source release.
///
/// Construction requires canonical failed-session takeover proof. Decoding
/// validates only shape; the trusted journal supplies provenance. The accepted
/// action remains separately retained under `action_key`; this record grants
/// no takeover authority and cannot replace fresh actor/authority checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryBasis {
    pub(super) spec: MoveAttemptSpec,
    pub(super) scope: FleetScope,
    pub(super) action_key: Digest,
    pub(super) node: NodeId,
    pub(super) session: SessionId,
    pub(super) accepted_at_ms: i64,
    pub(super) control: Control,
    pub(super) observed_at_ms: i64,
}

impl RecoveryBasis {
    /// Captures the exact source-epoch input before canonical acquisition CAS.
    /// An Idle input covers source loss after an unobserved release CAS; it is
    /// still recovery evidence and never manufactures a clean release result.
    pub fn new(
        accepted: &AcceptedFleetAction,
        control: Control,
        takeover: NodeTakeoverProof,
        observed_at_ms: i64,
    ) -> Result<Self> {
        let FleetActionKind::Movement {
            action: MovementAction::Recover,
            attempt,
        } = accepted.action().kind()
        else {
            return Err(OperationError::Invalid(
                "recovery requires accepted recovery action",
            ));
        };
        if takeover.session() != attempt.spec().source || takeover.claimant() != accepted.session()
        {
            return Err(OperationError::Fenced);
        }
        let basis = Self {
            spec: attempt.spec().clone(),
            scope: accepted.action().scope(),
            action_key: accepted.action().key()?,
            node: accepted.node(),
            session: accepted.session(),
            accepted_at_ms: accepted.accepted_at_ms(),
            control,
            observed_at_ms,
        };
        basis.validate()?;
        basis.validate_acceptance(accepted)?;
        Ok(basis)
    }

    /// Checks the original acceptance without accepting another effect.
    pub fn validate_acceptance(&self, accepted: &AcceptedFleetAction) -> Result<()> {
        self.validate()?;
        let FleetActionKind::Movement {
            action: MovementAction::Recover,
            attempt,
        } = accepted.action().kind()
        else {
            return Err(OperationError::Invalid("recovery acceptance kind mismatch"));
        };
        if self.spec != *attempt.spec()
            || self.scope != accepted.action().scope()
            || self.action_key != accepted.action().key()?
            || self.node != accepted.node()
            || self.session != accepted.session()
            || self.accepted_at_ms != accepted.accepted_at_ms()
        {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }

    /// Returns all immutable movement inputs, including the failed source.
    #[must_use]
    pub const fn spec(&self) -> &MoveAttemptSpec {
        &self.spec
    }
    /// Returns the exact canonical input retained before CAS.
    #[must_use]
    pub const fn control(&self) -> &Control {
        &self.control
    }
    /// Returns the original action index, never a substitute for full identity.
    #[must_use]
    pub const fn action_key(&self) -> Digest {
        self.action_key
    }
    /// Returns the original observation time, never refreshed by republication.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        self.scope.validate()?;
        self.control
            .encode()
            .map_err(|e| OperationError::Control(Box::new(e)))?;
        if self.scope.application != self.spec.target.application()
            || self.action_key
                != super::actions::movement_key(self.scope, MovementAction::Recover, &self.spec)
            || self.node != self.spec.destination_node
            || self.session != self.spec.destination
            || self.accepted_at_ms < 0
            || self.observed_at_ms < self.accepted_at_ms
            || self.control.cell != self.spec.target.cell_id()
            || self.control.incarnation != self.spec.incarnation
            || self.control.epoch != self.spec.source_epoch
            || self.control.root.is_none()
        {
            return Err(OperationError::Invalid("recovery basis scope mismatch"));
        }
        match self.control.state {
            ControlState::Serving | ControlState::Recovering
                if self
                    .control
                    .owner
                    .as_ref()
                    .is_some_and(|o| o.session == self.spec.source) => {}
            ControlState::Idle
                if self.control.owner.is_none() && self.control.recovery.is_none() => {}
            _ => {
                return Err(OperationError::Invalid(
                    "recovery basis is not the failed source",
                ));
            }
        }
        PublishedPosition {
            incarnation: self.control.incarnation,
            epoch: self.control.epoch,
            root: self
                .control
                .root
                .clone()
                .ok_or(OperationError::Invalid("recovery basis lacks root"))?,
        }
        .validate()
    }
}

/// Immutable canonical recovery position confirmed before successor admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryEvidence {
    pub(super) basis: RecoveryBasis,
    pub(super) restored: Control,
    pub(super) recorded_at_ms: i64,
}

impl RecoveryEvidence {
    /// Checks the actual takeover and optional pinned-overlay publication.
    /// The recorder must durably confirm this value before actor activation.
    pub fn new(basis: RecoveryBasis, restored: Control, recorded_at_ms: i64) -> Result<Self> {
        let evidence = Self {
            basis,
            restored,
            recorded_at_ms,
        };
        evidence.validate()?;
        Ok(evidence)
    }
    /// Returns the retained pre-CAS input and exact accepted action binding.
    #[must_use]
    pub const fn basis(&self) -> &RecoveryBasis {
        &self.basis
    }
    /// Returns the canonical control after recovery publication, before activation.
    #[must_use]
    pub const fn restored(&self) -> &Control {
        &self.restored
    }
    /// Returns the original recording time, not a serving-freshness timestamp.
    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
    /// Returns the position a current successor must have restored or advanced.
    pub fn position(&self) -> Result<PublishedPosition> {
        Ok(PublishedPosition {
            incarnation: self.restored.incarnation,
            epoch: self.restored.epoch,
            root: self
                .restored
                .root
                .clone()
                .ok_or(OperationError::Invalid("recovery result lacks root"))?,
        })
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.basis.validate()?;
        self.restored
            .encode()
            .map_err(|e| OperationError::Control(Box::new(e)))?;
        let owner = self
            .restored
            .owner
            .clone()
            .ok_or(OperationError::Invalid("recovery lacks claimant"))?;
        if owner.session != self.basis.session || self.recorded_at_ms < self.basis.observed_at_ms {
            return Err(OperationError::Invalid(
                "recovery claimant or time mismatch",
            ));
        }
        let claimed = self
            .basis
            .control
            .takeover(owner)
            .map_err(|e| OperationError::Control(Box::new(e)))?;
        if claimed.recovery.is_some() {
            claimed
                .validate_transition(&self.restored, Transition::PublishRecovery)
                .map_err(|e| OperationError::Control(Box::new(e)))?;
        } else if claimed != self.restored {
            return Err(OperationError::Invalid(
                "recovery changed the pinned root without an overlay",
            ));
        }
        self.position()?.validate()
    }
}

/// Canonical failed-source recovery plus independently checked current serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredActivation {
    /// Immutable basis and exact materialized recovery position.
    pub recovery: RecoveryEvidence,
    /// Current actor-backed successor, potentially advanced beyond recovery.
    pub serving: ActivationEvidence,
}

impl RecoveredActivation {
    pub(super) fn validate(&self) -> Result<()> {
        self.recovery.validate()?;
        self.serving.position.validate()?;
        let spec = self.recovery.basis.spec();
        let required = self.recovery.position()?;
        if !super::nonzero(self.serving.node.as_bytes())
            || !super::nonzero(self.serving.session.as_bytes())
            || self.serving.node == spec.source_node
            || self.serving.session == spec.source
            || self.serving.position.incarnation != spec.incarnation
            || self.serving.position.epoch < required.epoch
            || (self.serving.position.epoch == required.epoch
                && (self.serving.node != self.recovery.basis.node
                    || self.serving.session != self.recovery.basis.session))
            || !super::attempt::successor_position(&self.serving.position, &required)
            || (self.serving.session == spec.destination
                && self.serving.node != spec.destination_node)
        {
            return Err(OperationError::Invalid(
                "recovered successor lacks required position",
            ));
        }
        Ok(())
    }
}
