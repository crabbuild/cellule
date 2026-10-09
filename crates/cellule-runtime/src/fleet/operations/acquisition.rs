use crate::control::{Control, ControlState};

use super::{
    AcceptedFleetAction, FleetActionKind, FleetActionOutcome, FleetOutcome, MovementAction,
    OperationError, PublishedPosition, Result,
};

/// Immutable checked Idle input retained before a prepared acquisition CAS.
///
/// The host records its actual authority observation under the accepted action
/// before invoking canonical acquisition. This record proves neither CAS
/// success nor serving. It is distinct from failed-owner recovery evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquisitionBasis {
    pub(super) accepted: AcceptedFleetAction,
    pub(super) control: Control,
    pub(super) observed_at_ms: i64,
}

impl AcquisitionBasis {
    /// Checks the observed Idle control against an accepted receiver activation.
    pub fn new(
        accepted: AcceptedFleetAction,
        control: Control,
        observed_at_ms: i64,
    ) -> Result<Self> {
        let basis = Self {
            accepted,
            control,
            observed_at_ms,
        };
        basis.validate()?;
        Ok(basis)
    }

    /// Returns the exact original accepted receiver action.
    #[must_use]
    pub const fn accepted(&self) -> &AcceptedFleetAction {
        &self.accepted
    }
    /// Returns the canonical Idle control read before takeover.
    #[must_use]
    pub const fn control(&self) -> &Control {
        &self.control
    }
    /// Returns the capture time without refreshing the historical observation.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }

    /// Returns the exact immutable position of the acquisition input.
    pub fn position(&self) -> Result<PublishedPosition> {
        Ok(PublishedPosition {
            incarnation: self.control.incarnation,
            epoch: self.control.epoch,
            root: self
                .control
                .root
                .clone()
                .ok_or(OperationError::Invalid("acquisition lacks root"))?,
        })
    }

    /// Checks that a receiver's activation result follows this recorded basis.
    /// Fresh authority and actor checks still establish current serving.
    pub fn validate_result(&self, result: &FleetActionOutcome) -> Result<()> {
        self.validate()?;
        self.accepted.validate_result(result)?;
        if let FleetOutcome::Activated(evidence) = &result.outcome {
            let input = self.position()?;
            if result.observed_at_ms < self.observed_at_ms
                || evidence.position.epoch <= input.epoch
                || !super::attempt::successor_position(&evidence.position, &input)
            {
                return Err(OperationError::Invalid(
                    "activation does not follow acquisition basis",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.accepted.validate()?;
        self.control
            .encode()
            .map_err(|error| OperationError::Control(Box::new(error)))?;
        let FleetActionKind::Movement {
            action: MovementAction::Activate,
            attempt,
        } = self.accepted.action().kind()
        else {
            return Err(OperationError::Invalid(
                "acquisition requires accepted activation",
            ));
        };
        let spec = attempt.spec();
        if self.observed_at_ms < self.accepted.accepted_at_ms()
            || self.control.cell != spec.target.cell_id()
            || self.control.incarnation != spec.incarnation
            || self.control.state != ControlState::Idle
            || self.control.owner.is_some()
            || self.control.recovery.is_some()
            || self.control.epoch < spec.source_epoch
        {
            return Err(OperationError::Invalid(
                "acquisition control scope or state mismatch",
            ));
        }
        let input = self.position()?;
        input.validate()?;
        let released = attempt
            .released()
            .ok_or(OperationError::Invalid("acquisition lacks source position"))?;
        if !super::attempt::successor_position(&input, released)
            || (input.epoch == released.epoch && input.root != released.root)
        {
            return Err(OperationError::Invalid(
                "acquisition input precedes or changes release",
            ));
        }
        Ok(())
    }
}
