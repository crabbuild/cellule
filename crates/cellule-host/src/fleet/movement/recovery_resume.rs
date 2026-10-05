//! Preserve the original failed-source evidence across safe native rollback.
use super::*;
use cellule_runtime::fleet::operations::RecoveryEvidence;

impl FleetActionExecutor {
    pub(super) async fn resume_source_recovery(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        observed: VersionedControl,
    ) -> cellule_runtime::Result<ActionResult> {
        let evidence = self
            .confirm_source_recovery_evidence(accepted, inputs)
            .await?;
        if observed.value().state == ControlState::Idle {
            if observed.value().owner.is_some()
                || observed.value().epoch < evidence.restored().epoch
            {
                return Err(Error::Fenced);
            }
            // Prove native materialization and the original pinned suffix before
            // another ordinary claim. An Idle root or newer epoch alone cannot
            // replace the original acquisition and acknowledged history.
            self.verify_serving_prefix(
                attempt,
                inputs,
                ServingPrefix::Recovered(&evidence),
                observed.value().ltx_root().ok_or(Error::Fenced)?,
            )
            .await?;
            self.runtime
                .acquire_idle_restored(
                    inputs.catalog.clone(),
                    inputs.replica.clone(),
                    inputs.authority.clone(),
                    observed,
                    inputs.destination.clone(),
                    inputs.owner.clone(),
                )
                .await?;
        }
        self.recovered_serving(accepted, attempt, inputs).await
    }

    // Only accepted effect replay reaches this repair. Read-only inspection
    // keeps missing evidence blocking and never starts an acquisition.
    async fn confirm_source_recovery_evidence(
        &self,
        accepted: &AcceptedFleetAction,
        inputs: &FleetCellInputs,
    ) -> cellule_runtime::Result<RecoveryEvidence> {
        if let Some(evidence) = self
            .journal
            .load_recovery_evidence(accepted)
            .await
            .map_err(journal_error)?
        {
            evidence
                .basis()
                .validate_acceptance(accepted)
                .map_err(operation)?;
            return Ok(evidence);
        }
        let basis = self
            .journal
            .load_recovery_basis(accepted)
            .await
            .map_err(journal_error)?
            .ok_or(Error::Peer("recovery lacks retained original input"))?;
        basis.validate_acceptance(accepted).map_err(operation)?;
        let original = basis.control();
        let epoch = original
            .epoch
            .checked_add(1)
            .ok_or(Error::Control("recovery epoch overflow"))?;
        let canonical = inputs
            .authority
            .acquisition_record(original.cell, original.incarnation, epoch)
            .await?
            .ok_or(Error::AcquisitionHistoryIncomplete {
                cell: original.cell,
                incarnation: original.incarnation,
                epoch,
            })?;
        if canonical.input() != original {
            return Err(Error::Control(
                "recovery input differs from canonical acquisition",
            ));
        }
        let evidence =
            RecoveryEvidence::new(basis, canonical.materialized().clone(), wall_time_ms()?)
                .map_err(operation)?;
        let retained = self
            .journal
            .record_recovery_evidence(accepted, &evidence)
            .await
            .map_err(journal_error)?;
        if retained.basis() != evidence.basis()
            || retained.restored() != evidence.restored()
            || retained.recorded_at_ms() > evidence.recorded_at_ms()
        {
            return Err(Error::Peer("journal changed reconstructed recovery result"));
        }
        Ok(retained)
    }
}
