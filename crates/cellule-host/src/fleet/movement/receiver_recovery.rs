//! Failed receiver takeover reuses the ordinary observed acquisition path.
use super::*;
use crate::fleet::FleetActionJournal;
use cellule_runtime::cell::actor::{AcquisitionObservation, AcquisitionObserver};
use cellule_runtime::control::Control;
use cellule_runtime::fleet::operations::{ReceiverRecoveryBasis, ReceiverRecoveryEvidence};
use cellule_runtime::node::NodeTakeoverProof;
use std::sync::Arc;

struct ReceiverRecoveryRecorder {
    accepted: AcceptedFleetAction,
    takeover: NodeTakeoverProof,
    journal: Arc<dyn FleetActionJournal>,
}
impl AcquisitionObserver for ReceiverRecoveryRecorder {
    fn before_claim<'a>(&'a self, input: &'a Control) -> AcquisitionObservation<'a> {
        Box::pin(async move {
            let basis = ReceiverRecoveryBasis::new(
                self.accepted.clone(),
                input.clone(),
                self.takeover,
                wall_time_ms()?,
            )
            .map_err(operation)?;
            let retained = self
                .journal
                .record_receiver_recovery_basis(&basis)
                .await
                .map_err(journal_error)?;
            if retained.accepted() != &self.accepted
                || retained.control() != input
                || retained.observed_at_ms() > basis.observed_at_ms()
            {
                return Err(Error::Peer(
                    "journal changed checked receiver recovery basis",
                ));
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
                .load_receiver_recovery_basis(&self.accepted)
                .await
                .map_err(journal_error)?
                .ok_or(Error::Peer("receiver recovery lacks retained input"))?;
            if basis.accepted() != &self.accepted || basis.control() != input {
                return Err(Error::Fenced);
            }
            let evidence = ReceiverRecoveryEvidence::new(basis, restored.clone(), wall_time_ms()?)
                .map_err(operation)?;
            let retained = self
                .journal
                .record_receiver_recovery_evidence(&evidence)
                .await
                .map_err(journal_error)?;
            if retained.basis() != evidence.basis()
                || retained.restored() != restored
                || retained.recorded_at_ms() > evidence.recorded_at_ms()
            {
                return Err(Error::Peer(
                    "journal changed checked receiver recovery result",
                ));
            }
            Ok(())
        })
    }
}

impl FleetActionExecutor {
    pub(super) async fn recover_receiver(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        observed: VersionedControl,
    ) -> cellule_runtime::Result<ActionResult> {
        let Some(recovery) = self
            .cells
            .receiver_recovery_inputs(accepted, observed.value())
            .await
            .map_err(|source| Error::Facility {
                name: "fleet-receiver-recovery-provider",
                source,
            })?
        else {
            return Ok(ActionResult::checked(FleetOutcome::Unknown));
        };
        ReceiverRecoveryBasis::new(
            accepted.clone(),
            observed.value().clone(),
            recovery.takeover,
            wall_time_ms()?,
        )
        .map_err(operation)?;
        if let Some(original) = self
            .journal
            .load_receiver_recovery_basis(accepted)
            .await
            .map_err(journal_error)?
            && (original.accepted() != accepted || original.control() != observed.value())
        {
            return Ok(ActionResult::checked(FleetOutcome::Unknown));
        }
        // Prove the failed receiver still derives from the exact release before
        // takeover. Its pinned overlay is materialized by native recovery.
        let released = attempt.released().ok_or(Error::Fenced)?;
        self.verify_serving_prefix(
            attempt,
            inputs,
            ServingPrefix::Released(&released.root),
            observed.value().ltx_root().ok_or(Error::Fenced)?,
        )
        .await?;
        let recorder: Arc<dyn AcquisitionObserver> = Arc::new(ReceiverRecoveryRecorder {
            accepted: accepted.clone(),
            takeover: recovery.takeover,
            journal: self.journal.clone(),
        });
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
        self.receiver_recovered_serving(accepted, attempt, inputs)
            .await
    }

    pub(super) async fn receiver_recovered_serving(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
    ) -> cellule_runtime::Result<ActionResult> {
        let evidence = self
            .journal
            .load_receiver_recovery_evidence(accepted)
            .await
            .map_err(journal_error)?
            .ok_or(Error::Peer(
                "serving receiver lacks retained recovery evidence",
            ))?;
        if evidence.basis().accepted() != accepted {
            return Err(Error::Fenced);
        }
        let serving = self
            .serving_evidence(attempt, inputs, ServingPrefix::ReceiverRecovered(&evidence))
            .await?;
        let outcome = FleetOutcome::Activated(serving);
        let envelope = cellule_runtime::fleet::operations::FleetActionOutcome {
            scope: self.scope,
            action_key: accepted.action().key().map_err(operation)?,
            node: self.node,
            session: self.session,
            observed_at_ms: wall_time_ms()?,
            outcome: outcome.clone(),
        };
        evidence.validate_result(&envelope).map_err(operation)?;
        Ok(ActionResult::checked(outcome))
    }
}
