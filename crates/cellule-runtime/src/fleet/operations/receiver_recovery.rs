//! Recovery of a process-closed receiver after a proven source release.
use super::*;
use crate::control::{Control, ControlState, Transition};
use crate::node::NodeTakeoverProof;

/// Exact failed-receiver input retained before canonical takeover.
/// This is separate from failed-source recovery and never erases clean release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverRecoveryBasis {
    pub(super) accepted: AcceptedFleetAction,
    pub(super) control: Control,
    pub(super) observed_at_ms: i64,
}

impl ReceiverRecoveryBasis {
    /// Requires canonical failed-session proof for a closed boot in this route.
    pub fn new(
        accepted: AcceptedFleetAction,
        control: Control,
        takeover: NodeTakeoverProof,
        observed_at_ms: i64,
    ) -> Result<Self> {
        if control
            .owner
            .as_ref()
            .is_none_or(|owner| owner.session != takeover.session())
            || takeover.claimant() != accepted.session()
        {
            return Err(OperationError::Fenced);
        }
        let basis = Self {
            accepted,
            control,
            observed_at_ms,
        };
        basis.validate()?;
        Ok(basis)
    }
    /// Returns the exact accepted routed activation.
    pub const fn accepted(&self) -> &AcceptedFleetAction {
        &self.accepted
    }
    /// Returns the failed ownership control read before takeover.
    pub const fn control(&self) -> &Control {
        &self.control
    }
    /// Returns the immutable input capture time.
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.accepted.validate()?;
        self.control
            .encode()
            .map_err(|e| OperationError::Control(Box::new(e)))?;
        let FleetActionKind::Movement {
            action: MovementAction::Activate,
            attempt,
        } = self.accepted.action().kind()
        else {
            return Err(OperationError::Invalid(
                "receiver recovery requires activation",
            ));
        };
        let route = self
            .accepted
            .action()
            .receiver_route()
            .ok_or(OperationError::Invalid(
                "receiver recovery lacks closed route",
            ))?;
        let owner = self.control.owner.as_ref().ok_or(OperationError::Invalid(
            "receiver recovery lacks failed owner",
        ))?;
        let released = attempt.released().ok_or(OperationError::Invalid(
            "receiver recovery lacks clean release",
        ))?;
        if !route
            .hops
            .iter()
            .any(|hop| hop.previous().1 == owner.session)
            || self.control.cell != attempt.spec().target.cell_id()
            || self.control.incarnation != attempt.spec().incarnation
            || self.control.epoch <= released.epoch
            || !matches!(
                self.control.state,
                ControlState::Serving | ControlState::Recovering
            )
            || self.control.root.is_none()
            || self.observed_at_ms < self.accepted.accepted_at_ms()
        {
            return Err(OperationError::Invalid(
                "receiver recovery scope or state mismatch",
            ));
        }
        let position = PublishedPosition {
            incarnation: self.control.incarnation,
            epoch: self.control.epoch,
            root: self
                .control
                .root
                .clone()
                .ok_or(OperationError::Invalid("receiver recovery root missing"))?,
        };
        if !super::attempt::successor_position(&position, released) {
            return Err(OperationError::Invalid(
                "receiver recovery precedes release",
            ));
        }
        position.validate()
    }
}

/// Canonical recovered receiver position retained before actor admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverRecoveryEvidence {
    pub(super) basis: ReceiverRecoveryBasis,
    pub(super) restored: Control,
    pub(super) recorded_at_ms: i64,
}
impl ReceiverRecoveryEvidence {
    /// Validates the actual takeover and optional pinned-overlay publication.
    pub fn new(
        basis: ReceiverRecoveryBasis,
        restored: Control,
        recorded_at_ms: i64,
    ) -> Result<Self> {
        let evidence = Self {
            basis,
            restored,
            recorded_at_ms,
        };
        evidence.validate()?;
        Ok(evidence)
    }
    /// Returns the exact original receiver recovery input.
    pub const fn basis(&self) -> &ReceiverRecoveryBasis {
        &self.basis
    }
    /// Returns the canonical pre-activation recovery result.
    pub const fn restored(&self) -> &Control {
        &self.restored
    }
    /// Returns the original evidence recording time.
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
    /// Checks an activation result against this immutable recovery record.
    pub fn validate_result(&self, result: &FleetActionOutcome) -> Result<()> {
        self.validate()?;
        self.basis.accepted.validate_result(result)?;
        let FleetOutcome::Activated(serving) = &result.outcome else {
            return Err(OperationError::Invalid(
                "receiver recovery result is not activated",
            ));
        };
        let required = PublishedPosition {
            incarnation: self.restored.incarnation,
            epoch: self.restored.epoch,
            root: self
                .restored
                .root
                .clone()
                .ok_or(OperationError::Invalid("receiver result lacks root"))?,
        };
        if result.observed_at_ms < self.recorded_at_ms
            || !super::attempt::successor_position(&serving.position, &required)
        {
            return Err(OperationError::Invalid(
                "activation precedes receiver recovery",
            ));
        }
        Ok(())
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
            .ok_or(OperationError::Invalid("receiver recovery lacks claimant"))?;
        if owner.session != self.basis.accepted.session()
            || self.recorded_at_ms < self.basis.observed_at_ms
        {
            return Err(OperationError::Invalid(
                "receiver recovery claimant or time mismatch",
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
                "receiver recovery changed root without overlay",
            ));
        }
        Ok(())
    }
}
