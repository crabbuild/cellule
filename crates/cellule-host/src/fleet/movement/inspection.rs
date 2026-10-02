use super::*;

impl FleetActionExecutor {
    pub(super) async fn cancel(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        if self.runtime.prepared_receiver(attempt.spec().id)?.is_none() {
            return if self.confirmed_credit_settlement(attempt).await? {
                Ok(ActionResult::checked(FleetOutcome::ReceiverCleaned))
            } else {
                Err(Error::Peer(
                    "receiver cleanup has no retained resource proof",
                ))
            };
        }
        let prepared = self.prepared(attempt)?;
        match prepared.state()? {
            ReceiverState::Prepared | ReceiverState::Cancelled => {
                self.runtime.cancel_prepared_receiver(&prepared)?
            }
            ReceiverState::Activated
                if matches!(
                    attempt.phase(),
                    AttemptPhase::Activated | AttemptPhase::CleaningReceiver
                ) => {}
            _ => return Ok(ActionResult::checked(FleetOutcome::Unknown)),
        }
        Ok(ActionResult::checked(FleetOutcome::ReceiverCleaned))
    }

    pub(super) async fn confirmed_credit_settlement(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<bool> {
        for effect in [
            MovementAction::Cancel,
            MovementAction::Activate,
            MovementAction::Recover,
        ] {
            if self.confirmed_credit_result(attempt, effect).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) async fn confirmed_credit_result(
        &self,
        attempt: &MoveAttempt,
        effect: MovementAction,
    ) -> cellule_runtime::Result<bool> {
        let original = self
            .journal
            .load_movement_action(
                self.scope,
                attempt.spec().id,
                effect,
                self.node,
                self.session,
            )
            .await
            .map_err(journal_error)?;
        let Some(FleetActionAcceptance::Existing {
            accepted,
            result: Some(result),
        }) = original
        else {
            return Ok(false);
        };
        if accepted.action().scope() != self.scope
            || accepted.node() != self.node
            || accepted.session() != self.session
        {
            return Err(Error::Fenced);
        }
        accepted.validate_result(&result).map_err(operation)?;
        let FleetActionKind::Movement {
            action,
            attempt: original,
        } = accepted.action().kind()
        else {
            return Err(Error::Fenced);
        };
        if *action != effect || original.spec() != attempt.spec() {
            return Err(Error::Fenced);
        }
        if matches!(
            (&result.outcome, effect),
            (FleetOutcome::ReceiverCleaned, MovementAction::Cancel)
                | (FleetOutcome::Activated(_), MovementAction::Activate)
                | (FleetOutcome::Recovered(_), MovementAction::Recover)
        ) {
            return Ok(true);
        }
        Ok(false)
    }

    pub(in crate::fleet) async fn inspect(
        &self,
        attempt: &MoveAttempt,
    ) -> cellule_runtime::Result<ActionResult> {
        let spec = attempt.spec();
        if self.session == spec.source {
            let release_kinds: &[MovementAction] = match attempt.phase() {
                AttemptPhase::MaintenanceReleasing => &[MovementAction::ReleaseMaintenance],
                AttemptPhase::Releasing => &[MovementAction::Release],
                _ => &[MovementAction::ReleaseMaintenance, MovementAction::Release],
            };
            for &release_kind in release_kinds {
                let original = self
                    .journal
                    .load_movement_action(
                        self.scope,
                        spec.id,
                        release_kind,
                        self.node,
                        self.session,
                    )
                    .await
                    .map_err(journal_error)?;
                if let Some(FleetActionAcceptance::Existing {
                    accepted,
                    result: Some(result),
                }) = original
                {
                    if accepted.action().scope() != self.scope
                        || accepted.node() != self.node
                        || accepted.session() != self.session
                    {
                        return Err(Error::Fenced);
                    }
                    accepted.validate_result(&result).map_err(operation)?;
                    if let FleetActionKind::Movement {
                        action,
                        attempt: original,
                    } = accepted.action().kind()
                        && *action == release_kind
                        && original.spec() == spec
                        && matches!(result.outcome, FleetOutcome::Released(_))
                    {
                        return Ok(ActionResult::checked(result.outcome));
                    }
                }
            }
            return Ok(ActionResult::checked(FleetOutcome::Unknown));
        }
        if (self.node, self.session) != (spec.destination_node, spec.destination) {
            // The fresh request/journal boundary permits only known-release
            // successor reads here. Never acquire, inspect another endpoint's
            // retained effects, or infer settlement of its prepared resources.
            let inputs = self.inputs(attempt).await?;
            return self
                .serving(attempt, &inputs)
                .await
                .map(ActionResult::checked);
        }
        if matches!(
            attempt.phase(),
            AttemptPhase::Recovering | AttemptPhase::Recovered
        ) && let Some(FleetActionAcceptance::Existing { accepted, .. }) = self
            .journal
            .load_movement_action(
                self.scope,
                spec.id,
                MovementAction::Recover,
                self.node,
                self.session,
            )
            .await
            .map_err(journal_error)?
        {
            // Observation cannot start acquisition. Missing historical recovery
            // position stays Unknown; only replay of Recover may resume it.
            let inputs = self.inputs(attempt).await?;
            if self
                .journal
                .load_recovery_evidence(&accepted)
                .await
                .map_err(journal_error)?
                .is_some()
            {
                return self.recovered_serving(&accepted, attempt, &inputs).await;
            }
            return Ok(ActionResult::checked(FleetOutcome::Unknown));
        }
        if self.runtime.prepared_receiver(spec.id)?.is_some() {
            let prepared = self.prepared(attempt)?;
            match prepared.state()? {
                ReceiverState::Prepared => return self.inspect_preparation(attempt),
                ReceiverState::Cancelled => {
                    return Ok(ActionResult::checked(FleetOutcome::ReceiverCleaned));
                }
                ReceiverState::Activated if attempt.released().is_some() => {}
                _ => return Ok(ActionResult::checked(FleetOutcome::Unknown)),
            }
        }
        // The bounded local receipt can retire after result publication. Its
        // absence is not proof of resource settlement; use the retained action
        // and basis, then establish serving again through the actual actor.
        for effect in [MovementAction::Activate, MovementAction::Cancel] {
            let original = self
                .journal
                .load_movement_action(self.scope, spec.id, effect, self.node, self.session)
                .await
                .map_err(journal_error)?;
            let Some(FleetActionAcceptance::Existing {
                accepted,
                result: Some(result),
            }) = original
            else {
                continue;
            };
            if accepted.action().scope() != self.scope
                || accepted.node() != self.node
                || accepted.session() != self.session
            {
                return Err(Error::Fenced);
            }
            accepted.validate_result(&result).map_err(operation)?;
            let FleetActionKind::Movement {
                action,
                attempt: original,
            } = accepted.action().kind()
            else {
                return Err(Error::Fenced);
            };
            if *action != effect || original.spec() != spec {
                return Err(Error::Fenced);
            }
            match (&result.outcome, effect) {
                (FleetOutcome::Activated(_), MovementAction::Activate) => {
                    let basis = self
                        .journal
                        .load_acquisition_basis(&accepted)
                        .await
                        .map_err(journal_error)?;
                    if let Some(basis) = &basis {
                        if basis.accepted() != &accepted {
                            return Err(Error::Fenced);
                        }
                    } else if !self
                        .confirmed_credit_result(attempt, MovementAction::Cancel)
                        .await?
                    {
                        return Err(Error::Peer(
                            "activation inspection lacks acquisition or cleanup basis",
                        ));
                    }
                    let inputs = self.inputs(attempt).await?;
                    let outcome = self.serving(attempt, &inputs).await?;
                    let mut checked = *result;
                    checked.observed_at_ms = wall_time_ms()?;
                    checked.outcome = outcome.clone();
                    if let Some(basis) = basis {
                        basis.validate_result(&checked).map_err(operation)?;
                    } else {
                        accepted.validate_result(&checked).map_err(operation)?;
                    }
                    return Ok(ActionResult::checked(outcome));
                }
                (FleetOutcome::ReceiverCleaned, MovementAction::Cancel) => {
                    return Ok(ActionResult::checked(FleetOutcome::ReceiverCleaned));
                }
                _ => {}
            }
        }
        Ok(ActionResult::checked(FleetOutcome::Unknown))
    }

    pub(in crate::fleet) async fn retire_receiver_receipt(
        &self,
        completion: &FleetActionCompletion,
    ) -> cellule_runtime::Result<()> {
        if !matches!(
            completion.outcome.outcome,
            FleetOutcome::Activated(_) | FleetOutcome::Recovered(_) | FleetOutcome::ReceiverCleaned
        ) {
            return Ok(());
        }
        let FleetActionKind::Movement { attempt, .. } = completion.accepted.action().kind() else {
            return Ok(());
        };
        if self.session != attempt.spec().destination {
            return Ok(());
        }
        if let Some(prepared) = self.runtime.prepared_receiver(attempt.spec().id)? {
            if prepared.spec()? != *attempt.spec() {
                return Err(Error::Fenced);
            }
            self.runtime.retire_prepared_receiver(&prepared).await?;
        }
        Ok(())
    }
}
