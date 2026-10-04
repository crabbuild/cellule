use super::*;

impl FleetActionExecutor {
    pub(super) async fn activate(
        &self,
        accepted: &AcceptedFleetAction,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        let prepared = match self.runtime.prepared_receiver(attempt.spec().id)? {
            Some(_) => {
                let prepared = self.prepared(attempt)?;
                match prepared.state()? {
                    ReceiverState::Prepared => Some(prepared),
                    ReceiverState::Cancelled
                        if self.confirmed_credit_settlement(attempt).await? =>
                    {
                        None
                    }
                    _ => return Ok(ActionResult::checked(FleetOutcome::Unknown)),
                }
            }
            None if self.confirmed_credit_settlement(attempt).await? => None,
            None => {
                return Err(Error::Peer(
                    "fleet receiver credit is absent without cleanup proof",
                ));
            }
        };
        let inputs = self.inputs(attempt).await?;
        let observed = inputs
            .authority
            .load(attempt.spec().target.cell_id())
            .await?
            .ok_or(Error::Control("fleet activation authority is absent"))?;
        self.check_contract(attempt, &inputs, &observed)?;
        if prepared.is_none()
            && observed.value().state == ControlState::Serving
            && observed
                .value()
                .owner
                .as_ref()
                .is_some_and(|owner| owner.session == self.session)
        {
            // Ordinary acquisition can win after unused credit was joined. It
            // needs current serving proof, not a second ownership CAS.
            return self
                .serving(attempt, &inputs)
                .await
                .map(ActionResult::checked);
        }
        let basis =
            AcquisitionBasis::new(accepted.clone(), observed.value().clone(), wall_time_ms()?)
                .map_err(operation)?;
        let retained = self
            .journal
            .record_acquisition_basis(&basis)
            .await
            .map_err(journal_error)?;
        if retained.accepted() != accepted
            || retained.control() != observed.value()
            || retained.observed_at_ms() > basis.observed_at_ms()
        {
            return Err(Error::Peer("journal changed checked acquisition basis"));
        }
        if let Some(prepared) = prepared {
            self.runtime
                .activate_prepared_receiver(
                    &prepared,
                    inputs.authority.clone(),
                    observed,
                    inputs.owner.clone(),
                    wall_time_ms()?,
                )
                .await?;
        } else {
            // After confirmed unused-credit cleanup, acquire through the normal
            // admitted restore path. The fleet permit and exact release remain
            // charged; this does not create another movement attempt.
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
        let outcome = self.serving(attempt, &inputs).await?;
        let envelope = cellule_runtime::fleet::operations::FleetActionOutcome {
            scope: accepted.action().scope(),
            action_key: accepted.action().key().map_err(operation)?,
            node: self.node,
            session: self.session,
            observed_at_ms: wall_time_ms()?,
            outcome: outcome.clone(),
        };
        retained.validate_result(&envelope).map_err(operation)?;
        Ok(ActionResult::checked(outcome))
    }

    pub(super) async fn serving(
        &self,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
    ) -> cellule_runtime::Result<FleetOutcome> {
        let release = attempt.released().ok_or_else(|| {
            operation(cellule_runtime::fleet::operations::OperationError::Invalid(
                "activation without release",
            ))
        })?;
        let evidence = self
            .serving_evidence(attempt, inputs, ServingPrefix::Released(&release.root))
            .await?;
        attempt.validate_activation(&evidence).map_err(operation)?;
        Ok(FleetOutcome::Activated(evidence))
    }

    pub(super) async fn serving_evidence(
        &self,
        attempt: &MoveAttempt,
        inputs: &FleetCellInputs,
        required: ServingPrefix<'_>,
    ) -> cellule_runtime::Result<ActivationEvidence> {
        let spec = attempt.spec();
        let current = inputs
            .authority
            .load(spec.target.cell_id())
            .await?
            .ok_or(Error::Fenced)?;
        self.check_contract(attempt, inputs, &current)?;
        if current.value().state != ControlState::Serving
            || current.value().epoch <= spec.source_epoch
            || current
                .value()
                .owner
                .as_ref()
                .is_none_or(|owner| owner.session != self.session)
        {
            return Err(Error::Fenced);
        }
        let before = self
            .runtime
            .observe_serving(
                &inputs.catalog,
                &inputs.authority,
                spec.incarnation,
                spec.source_epoch,
            )
            .await?;
        if before.owner().session != self.session {
            return Err(Error::Fenced);
        }
        let root = before
            .position()
            .root
            .to_ltx(spec.target.cell_id(), spec.incarnation);
        self.verify_serving_prefix(attempt, inputs, required, root)
            .await?;
        // Share the canonical native FIFO/authority/generation observation with
        // complete original-writer collection. Prefix I/O grants no serving.
        let after = self
            .runtime
            .observe_serving(
                &inputs.catalog,
                &inputs.authority,
                spec.incarnation,
                spec.source_epoch,
            )
            .await?;
        if !before.same_writer(&after) {
            return Err(Error::Fenced);
        }
        if !self.registry.supports_cell(
            spec.target.namespace(),
            inputs.catalog.entry().role(),
            after.native().code,
            after.native().schema,
        ) {
            return Err(Error::Registry(
                "fleet receiver does not support current Cell contract",
            ));
        }
        Ok(ActivationEvidence {
            node: self.node,
            session: self.session,
            position: after.position().clone(),
        })
    }
}
