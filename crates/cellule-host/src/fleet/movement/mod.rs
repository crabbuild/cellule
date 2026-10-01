//! Canonical receiver admission, movement, and retained evidence.

use super::actions::{
    ActionResult, FleetActionCompletion, FleetActionExecutor, journal_error, operation,
    wall_time_ms,
};
use super::{FleetActionAcceptance, FleetCellInputs};
use cellule_runtime::Error;
use cellule_runtime::cell::actor::{CellInventoryEntry, PreparedCellReceiver, ReceiverState};
use cellule_runtime::control::{ControlState, authority::VersionedControl};
use cellule_runtime::fleet::operations::{
    AcceptedFleetAction, AcquisitionBasis, ActivationEvidence, AttemptPhase, DrainBlocker,
    FleetActionKind, FleetOutcome, MoveAttempt, MovementAction, PublishedPosition,
};

mod activation;
mod inspection;
mod receiver;
mod recovery;

impl FleetActionExecutor {
    pub(super) async fn perform_movement(
        &self,
        accepted: &AcceptedFleetAction,
    ) -> cellule_runtime::Result<ActionResult> {
        let FleetActionKind::Movement { action, attempt } = accepted.action().kind() else {
            return Err(Error::Control("fleet movement has no attempt"));
        };
        let spec = attempt.spec();
        let now = wall_time_ms()?;
        match action {
            MovementAction::Release => {
                if now >= spec.deadline_ms
                    || attempt.reservation().is_none_or(|r| now >= r.expires_at_ms)
                {
                    return Ok(ActionResult::checked(FleetOutcome::Rejected(
                        DrainBlocker::Deadline,
                    )));
                }
                self.runtime
                    .release_idle_cell_at(
                        spec.target.cell_id(),
                        spec.source,
                        spec.generation,
                        spec.incarnation,
                        spec.source_epoch,
                    )
                    .await
                    .map(FleetOutcome::Released)
                    .map(ActionResult::checked)
            }
            MovementAction::ReleaseMaintenance => {
                let until = attempt
                    .reservation()
                    .map_or(spec.deadline_ms, |r| r.expires_at_ms.min(spec.deadline_ms));
                if now >= until {
                    return Ok(ActionResult::checked(FleetOutcome::Rejected(
                        DrainBlocker::Deadline,
                    )));
                }
                let remaining = u64::try_from(until - now).map_err(|_| Error::Deadline)?;
                let deadline = tokio::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(remaining))
                    .ok_or(Error::Deadline)?;
                match self
                    .runtime
                    .release_maintenance_cell_at(
                        spec.target.cell_id(),
                        spec.source,
                        spec.generation,
                        spec.incarnation,
                        spec.source_epoch,
                        deadline,
                    )
                    .await?
                {
                    cellule_runtime::cell::actor::MaintenanceCellRelease::Released(position) => {
                        Ok(ActionResult::checked(FleetOutcome::Released(position)))
                    }
                    cellule_runtime::cell::actor::MaintenanceCellRelease::Refused {
                        blocker,
                        error,
                    } => Ok(ActionResult {
                        outcome: FleetOutcome::Rejected(blocker),
                        error,
                    }),
                }
            }
            MovementAction::Recover => self.recover(accepted, attempt).await,
            MovementAction::Prepare => self.prepare(attempt).await,
            MovementAction::Activate => self.activate(accepted, attempt).await,
            MovementAction::Cancel => self.cancel(attempt).await,
            MovementAction::Inspect => Err(Error::Control(
                "fleet Inspect requires request-bound inspection",
            )),
            MovementAction::Retire => Err(Error::Control("fleet retirement is journal-local")),
        }
    }

    pub(super) async fn inspect_accepted(
        &self,
        accepted: &AcceptedFleetAction,
    ) -> cellule_runtime::Result<ActionResult> {
        if matches!(
            accepted.action().kind(),
            FleetActionKind::Maintenance { .. }
        ) {
            return self.perform_maintenance(accepted);
        }
        let FleetActionKind::Movement { action, attempt } = accepted.action().kind() else {
            return Err(Error::Control("accepted fleet effect is not movement"));
        };
        // A runtime-owned Prepared state proves this session has not accepted
        // takeover. Repeating preparation is not required; activation's exact
        // credit state prevents a second accepted acquisition.
        match action {
            MovementAction::Recover => self.inspect_recovery(accepted, attempt).await,
            MovementAction::Prepare => self.inspect_preparation(attempt),
            MovementAction::Activate => {
                if self.runtime.prepared_receiver(attempt.spec().id)?.is_none()
                    && self.confirmed_credit_settlement(attempt).await?
                {
                    // Cleanup retired the receipt, but ordinary acquisition may
                    // already have served. Authority decides whether to inspect
                    // that actor or use the retained Idle input for acquisition.
                    return self.activate(accepted, attempt).await;
                }
                let prepared = self.prepared(attempt)?;
                match prepared.state()? {
                    ReceiverState::Prepared => self.activate(accepted, attempt).await,
                    ReceiverState::Cancelled
                        if self.confirmed_credit_settlement(attempt).await? =>
                    {
                        self.activate(accepted, attempt).await
                    }
                    ReceiverState::Activated => {
                        let basis = self
                            .journal
                            .load_acquisition_basis(accepted)
                            .await
                            .map_err(journal_error)?
                            .ok_or(Error::Peer(
                                "accepted activation has no retained acquisition basis",
                            ))?;
                        if basis.accepted() != accepted {
                            return Err(Error::Fenced);
                        }
                        let inputs = self.inputs(attempt).await?;
                        let outcome = self.serving(attempt, &inputs).await?;
                        let envelope = cellule_runtime::fleet::operations::FleetActionOutcome {
                            scope: accepted.action().scope(),
                            action_key: accepted.action().key().map_err(operation)?,
                            node: self.node,
                            session: self.session,
                            observed_at_ms: wall_time_ms()?,
                            outcome: outcome.clone(),
                        };
                        basis.validate_result(&envelope).map_err(operation)?;
                        Ok(ActionResult::checked(outcome))
                    }
                    _ => Ok(ActionResult::checked(FleetOutcome::Unknown)),
                }
            }
            MovementAction::Cancel => self.cancel(attempt).await,
            MovementAction::Inspect => Err(Error::Control(
                "fleet Inspect requires request-bound inspection",
            )),
            MovementAction::Release
            | MovementAction::ReleaseMaintenance
            | MovementAction::Retire => Err(Error::Peer(
                "accepted source effect has no provable retained result",
            )),
        }
    }
}
