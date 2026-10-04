use super::*;

impl FleetActionExecutor {
    pub(super) async fn inputs(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<FleetCellInputs> {
        let spec = attempt.spec();
        let inputs = self
            .cells
            .cell_inputs(spec)
            .await
            .map_err(|source| Error::Facility {
                name: "fleet-cell-provider",
                source,
            })?;
        // A verified catalog Cell id commits tenant/application identity. Check
        // its explicit namespace and partition as well without constructing a
        // second catalog proof or exposing the catalog's private target helper.
        if inputs.catalog.entry().cell() != spec.target.cell_id()
            || inputs.catalog.entry().namespace() != spec.target.namespace()
            || inputs.catalog.entry().partition() != spec.target.partition()
            || inputs.replica.scope()
                != (
                    *spec.target.cell_id().as_bytes(),
                    *spec.incarnation.as_bytes(),
                )
            || inputs.owner.session != self.session
        {
            return Err(Error::Fenced);
        }
        Ok(inputs)
    }

    pub(super) fn check_contract(
        &self,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        control: &VersionedControl,
    ) -> cellule_runtime::Result<()> {
        let value = control.value();
        let spec = attempt.spec();
        if value.cell != spec.target.cell_id() || value.incarnation != spec.incarnation {
            return Err(Error::Fenced);
        }
        if !self.registry.supports_cell(
            spec.target.namespace(),
            inputs.catalog.entry().role(),
            value.code,
            value.schema,
        ) {
            return Err(Error::Registry(
                "fleet receiver does not support current Cell contract",
            ));
        }
        Ok(())
    }

    pub(super) async fn prepare(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        let prepared = async {
            let inputs = self.inputs(attempt).await?;
            let spec = attempt.spec();
            let current = inputs
                .authority
                .load(spec.target.cell_id())
                .await?
                .ok_or(Error::Control("fleet source authority is absent"))?;
            self.check_contract(attempt, &inputs, &current)?;
            if current.value().epoch != spec.source_epoch
                || current.value().state != ControlState::Serving
                || current
                    .value()
                    .owner
                    .as_ref()
                    .is_none_or(|owner| owner.session != spec.source)
                || current.value().root.is_none()
            {
                return Err(Error::Fenced);
            }
            let prepared = self.runtime.prepare_receiver(
                spec.clone(),
                inputs.catalog,
                inputs.replica,
                inputs.destination,
                spec.deadline_ms,
                wall_time_ms()?,
            )?;
            Ok(FleetOutcome::Reserved(prepared.reservation()?))
        }
        .await;
        // Preparation cannot change Cell authority. A definite refusal has no
        // installed credit; partial runtime admission unwinds actual tokens.
        Ok(match prepared {
            Ok(outcome) => ActionResult::checked(outcome),
            Err(error) => {
                let blocker = match &error {
                    Error::Capacity(_) => DrainBlocker::ReceiverCapacity,
                    Error::Registry(_) => DrainBlocker::IncompatibleRelease,
                    Error::FleetOperation(e)
                        if matches!(
                            e.as_ref(),
                            cellule_runtime::fleet::operations::OperationError::Deadline
                        ) =>
                    {
                        DrainBlocker::Deadline
                    }
                    _ => DrainBlocker::IncompleteObservation,
                };
                ActionResult::refused(blocker, error)
            }
        })
    }

    pub(super) fn prepared(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<PreparedCellReceiver> {
        let prepared = self
            .runtime
            .prepared_receiver(attempt.spec().id)?
            .ok_or(Error::Peer("fleet receiver credit is absent"))?;
        let reservation = prepared.reservation()?;
        if prepared.spec()? != *attempt.spec()
            || attempt
                .reservation()
                .is_some_and(|expected| reservation != expected)
        {
            return Err(Error::Fenced);
        }
        Ok(prepared)
    }

    pub(super) fn inspect_preparation(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        let prepared = self.prepared(attempt)?;
        let outcome = match prepared.state()? {
            ReceiverState::Prepared if prepared.reservation()?.expires_at_ms > wall_time_ms()? => {
                FleetOutcome::Reserved(prepared.reservation()?)
            }
            ReceiverState::Prepared => FleetOutcome::Blocked(DrainBlocker::Deadline),
            _ => FleetOutcome::Unknown,
        };
        Ok(ActionResult::checked(outcome))
    }
}
