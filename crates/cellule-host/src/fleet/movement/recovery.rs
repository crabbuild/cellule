use std::sync::Arc;

use cellule_runtime::cell::actor::{AcquisitionObservation, AcquisitionObserver};
use cellule_runtime::control::Control;
use cellule_runtime::fleet::operations::{RecoveredActivation, RecoveryBasis, RecoveryEvidence};
use cellule_runtime::node::NodeTakeoverProof;

use super::*;
use crate::fleet::FleetActionJournal;

struct RecoveryRecorder {
    accepted: AcceptedFleetAction,
    takeover: NodeTakeoverProof,
    journal: Arc<dyn FleetActionJournal>,
}

impl AcquisitionObserver for RecoveryRecorder {
    fn before_claim<'a>(&'a self, input: &'a Control) -> AcquisitionObservation<'a> {
        Box::pin(async move {
            let basis = RecoveryBasis::new(
                &self.accepted,
                input.clone(),
                self.takeover,
                wall_time_ms()?,
            )
            .map_err(operation)?;
            let retained = self
                .journal
                .record_recovery_basis(&self.accepted, &basis)
                .await
                .map_err(journal_error)?;
            retained
                .validate_acceptance(&self.accepted)
                .map_err(operation)?;
            if retained.control() != input || retained.observed_at_ms() > basis.observed_at_ms() {
                return Err(Error::Peer("journal changed checked recovery basis"));
            }
            Ok(())
        })
    }
    fn before_activation<'a>(
        &'a self,
        input: &'a Control,
        restored: &'a Control,
    ) -> AcquisitionObservation<'a> {
        Box::pin(async move {
            let basis = self
                .journal
                .load_recovery_basis(&self.accepted)
                .await
                .map_err(journal_error)?
                .ok_or(Error::Peer("recovery activation lacks retained input"))?;
            basis
                .validate_acceptance(&self.accepted)
                .map_err(operation)?;
            if basis.control() != input {
                return Err(Error::Fenced);
            }
            let evidence = RecoveryEvidence::new(basis, restored.clone(), wall_time_ms()?)
                .map_err(operation)?;
            let retained = self
                .journal
                .record_recovery_evidence(&self.accepted, &evidence)
                .await
                .map_err(journal_error)?;
            if retained.basis() != evidence.basis()
                || retained.restored() != restored
                || retained.recorded_at_ms() > evidence.recorded_at_ms()
            {
                return Err(Error::Peer("journal changed checked recovery result"));
            }
            Ok(())
        })
    }
}

impl FleetActionExecutor {
    pub(super) async fn recover(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        // Ordinary recovery must not compete with this attempt's unused affine
        // worker reservation. Only the tracked Prepared/Cancelled state proves
        // that no acquisition used it; never close an active writer for cleanup.
        if self.runtime.prepared_receiver(attempt.spec().id)?.is_some() {
            let credit = self.prepared(attempt)?;
            match credit.state()? {
                ReceiverState::Prepared | ReceiverState::Cancelled => {
                    self.runtime.cancel_prepared_receiver(&credit)?
                }
                _ => return Ok(ActionResult::checked(FleetOutcome::Unknown)),
            }
        } else if !self.confirmed_credit_settlement(attempt).await? {
            return Err(Error::Peer("recovery lacks unused-credit cleanup proof"));
        }
        let inputs = self.inputs(attempt).await?;
        let recovery = self
            .cells
            .recovery_inputs(attempt.spec())
            .await
            .map_err(|source| Error::Facility {
                name: "fleet-recovery-provider",
                source,
            })?;
        let observed = inputs
            .authority
            .load(attempt.spec().target.cell_id())
            .await?
            .ok_or(Error::Fenced)?;
        self.check_contract(attempt, &inputs, &observed)?;
        // Validate canonical fence/source binding before invoking any acquisition.
        RecoveryBasis::new(
            accepted,
            observed.value().clone(),
            recovery.takeover,
            wall_time_ms()?,
        )
        .map_err(operation)?;
        let recorder: Arc<dyn AcquisitionObserver> = Arc::new(RecoveryRecorder {
            accepted: accepted.clone(),
            takeover: recovery.takeover,
            journal: self.journal.clone(),
        });
        if observed.value().state == ControlState::Idle {
            self.runtime
                .acquire_idle_restored_observed(
                    inputs.catalog.clone(),
                    inputs.replica.clone(),
                    inputs.authority.clone(),
                    observed,
                    inputs.destination.clone(),
                    inputs.owner.clone(),
                    Some(recorder),
                )
                .await?;
        } else {
            self.runtime
                .takeover_restored_observed(
                    inputs.catalog.clone(),
                    inputs.replica.clone(),
                    inputs.authority.clone(),
                    observed,
                    recovery.takeover,
                    recovery.manifests,
                    inputs.destination.clone(),
                    inputs.owner.clone(),
                    Some(recorder),
                )
                .await?;
        }
        self.recovered_serving(accepted, attempt, &inputs).await
    }

    pub(super) async fn inspect_recovery(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        accepted
            .validate_replay(accepted.action(), self.node, self.session)
            .map_err(operation)?;
        let FleetActionKind::Movement {
            action: MovementAction::Recover,
            attempt: original,
        } = accepted.action().kind()
        else {
            return Err(Error::Fenced);
        };
        if original.spec() != attempt.spec() || accepted.action().scope() != self.scope {
            return Err(Error::Fenced);
        }
        let inputs = self.inputs(attempt).await?;
        if self
            .journal
            .load_recovery_evidence(accepted)
            .await
            .map_err(journal_error)?
            .is_some()
        {
            // The required position is historical; serving is checked again below.
            return self.recovered_serving(accepted, attempt, &inputs).await;
        }
        if let Some(basis) = self
            .journal
            .load_recovery_basis(accepted)
            .await
            .map_err(journal_error)?
        {
            basis.validate_acceptance(accepted).map_err(operation)?;
            let current = inputs
                .authority
                .load(attempt.spec().target.cell_id())
                .await?
                .ok_or(Error::Fenced)?;
            if current.value() != basis.control() {
                return Ok(ActionResult::checked(FleetOutcome::Unknown));
            }
            // Unchanged full control (including monotonic revision/epoch) proves
            // this retained input was not acquired; reconfirm it before CAS.
        }
        self.recover(accepted, attempt).await
    }

    pub(super) async fn recovered_serving(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
    ) -> cellule_runtime::Result<ActionResult> {
        let recovery = self
            .journal
            .load_recovery_evidence(accepted)
            .await
            .map_err(journal_error)?
            .ok_or(Error::Peer(
                "current successor lacks retained recovery evidence",
            ))?;
        recovery
            .basis()
            .validate_acceptance(accepted)
            .map_err(operation)?;
        let serving = self.serving_evidence(attempt, inputs).await?;
        let outcome = FleetOutcome::Recovered(Box::new(RecoveredActivation { recovery, serving }));
        let envelope = cellule_runtime::fleet::operations::FleetActionOutcome {
            scope: self.scope,
            action_key: accepted.action().key().map_err(operation)?,
            node: self.node,
            session: self.session,
            observed_at_ms: wall_time_ms()?,
            outcome: outcome.clone(),
        };
        accepted.validate_result(&envelope).map_err(operation)?;
        Ok(ActionResult::checked(outcome))
    }
}
