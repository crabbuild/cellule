//! Resume a safely rolled-back receiver through ordinary admitted acquisition.
use super::*;

impl FleetActionExecutor {
    pub(super) async fn resume_receiver_recovery(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        observed: VersionedControl,
    ) -> cellule_runtime::Result<ActionResult> {
        let evidence = self
            .confirm_receiver_recovery_evidence(accepted, inputs)
            .await?;
        if observed.value().state != ControlState::Idle
            || observed.value().owner.is_some()
            || observed.value().epoch < evidence.restored().epoch
        {
            return Ok(ActionResult::checked(FleetOutcome::Unknown));
        }
        // The original takeover remains immutable. Prove its materialization,
        // original suffix and exact release prefix before claiming the Idle
        // root. A later root/epoch alone cannot authorize continuation.
        self.verify_serving_prefix(
            attempt,
            inputs,
            ServingPrefix::ReceiverRecovered(&evidence),
            observed.value().ltx_root().ok_or(Error::Fenced)?,
        )
        .await?;
        // This exact accepted action already owns the journaled recovery basis.
        // Native acquisition retains the new Idle input before admission; do
        // not replace the original basis or mix it with AcquisitionBasis.
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
        self.receiver_recovered_serving(accepted, attempt, inputs)
            .await
    }
}
