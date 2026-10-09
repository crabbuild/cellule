use super::*;
use cellule_runtime::fleet::operations::{
    FleetOutcome, MaintenanceAction, MaintenanceEvent, MaintenanceOperation,
};

impl FleetReconciler {
    pub(super) async fn advance_maintenance(
        &self,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
        pass_observation: &mut Option<FleetObservation>,
    ) -> Result<()> {
        let Some(maintenance) = report.snapshot.head().maintenance().cloned() else {
            return Ok(());
        };
        match maintenance.phase() {
            MaintenancePhase::Requested => {
                // Cordon is monotonic even after the evacuation deadline. A
                // missing reply cannot clear the retained physical-node intent.
                // Replaying its stable key adopts the original acceptance.
                let action = report
                    .snapshot
                    .head()
                    .maintenance_action(MaintenanceAction::Cordon, clock.now()?)
                    .map_err(operation)?;
                report.dispatched += 1;
                let completion = call(
                    clock.deadline,
                    "fleet-transport",
                    self.transport.dispatch(&action, clock.deadline),
                )
                .await?;
                completion
                    .accepted
                    .validate_replay(&action, maintenance.node(), maintenance.session())
                    .map_err(operation)?;
                completion
                    .accepted
                    .validate_result(&completion.outcome)
                    .map_err(operation)?;
                report.maintenance_failure = completion.execution_error.clone();
                if !completion.committed {
                    if let Some(error) = &completion.journal_error {
                        report.maintenance_failure = Some(Arc::clone(error));
                    }
                    report.blocked(DrainBlocker::PendingPublication);
                    return Ok(());
                }
                match completion.outcome.outcome {
                    FleetOutcome::Cordoned => {
                        self.commit(
                            clock,
                            report,
                            JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
                        )
                        .await?;
                    }
                    FleetOutcome::Rejected(blocker) | FleetOutcome::Blocked(blocker) => {
                        report.blocked(blocker);
                    }
                    FleetOutcome::Unknown => {
                        report.blocked(DrainBlocker::OutcomeUnknown);
                    }
                    _ => return Err(operation(OperationError::Conflict)),
                }
            }
            MaintenancePhase::Cordoned => {
                // Only a previously committed endpoint proof reaches this phase.
                // Registry intent already excludes this node from receiving.
                self.commit(
                    clock,
                    report,
                    JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
                )
                .await?;
            }
            MaintenancePhase::Evacuating => {
                let roster =
                    FleetRoster::collect(self.journal.as_ref(), &report.snapshot, clock.deadline)
                        .await?;
                let observation = call(
                    clock.deadline,
                    "fleet-observer",
                    self.observer.observe(&roster, clock.now()?, clock.deadline),
                )
                .await?;
                if observation.scope != self.scope
                    || observation.registry != report.snapshot.registry()
                {
                    return Err(operation(OperationError::Conflict));
                }
                roster
                    .confirm(self.journal.as_ref(), clock.deadline)
                    .await?;
                let now = clock.now()?;
                let boot_complete = roster.covers_advertisements(&observation.nodes, now)?;
                let enrollment_settled = roster.enrollments().iter().all(|row| {
                    row.status() != cellule_runtime::fleet::operations::EnrollmentStatus::Pending
                });
                *pass_observation = Some(observation.with_roster(roster)?);
                let observation = pass_observation.as_ref().ok_or(Error::Control(
                    "fleet maintenance observation was not retained",
                ))?;
                report.maintenance_policy = observation
                    .maintenance_policy_coverage()
                    .map(|coverage| coverage.progress());
                let placements = observation.placements(now)?;
                if !observation.complete
                    || !boot_complete
                    || !enrollment_settled
                    || !observation.counts_match(&placements)
                {
                    report.blocked(DrainBlocker::IncompleteObservation);
                    return Ok(());
                }
                if report.snapshot.head().attempts().iter().any(|attempt| {
                    attempt.spec().source_node == maintenance.node()
                        || attempt.spec().destination_node == maintenance.node()
                }) {
                    report.blocked(DrainBlocker::PendingPublication);
                    return Ok(());
                }
                let action = report
                    .snapshot
                    .head()
                    .maintenance_action(MaintenanceAction::SettleRoles, now)
                    .map_err(operation)?;
                let settlement = match observation.role_settlement(&action, now) {
                    Ok(settlement) => settlement,
                    Err(_) => {
                        report.blocked(DrainBlocker::IncompleteObservation);
                        return Ok(());
                    }
                };
                report.dispatched += 1;
                let completion = if let Some(closure_digest) = settlement.failed_boot_closure() {
                    let closure = observation
                        .failed_boot_closures()
                        .and_then(|closures| {
                            closures
                                .iter()
                                .find(|closure| closure.digest() == closure_digest)
                        })
                        .ok_or(Error::Control(
                            "closed-boot settlement lost its process closure",
                        ))?;
                    settlement.validate_failed_boot_closure(&action, closure)?;
                    call(
                        clock.deadline,
                        "fleet-transport",
                        self.transport.settle_roles_after_process_closure(
                            &action,
                            &settlement,
                            closure,
                            clock.deadline,
                        ),
                    )
                    .await?
                } else {
                    call(
                        clock.deadline,
                        "fleet-transport",
                        self.transport
                            .settle_roles(&action, &settlement, clock.deadline),
                    )
                    .await?
                };
                completion
                    .accepted
                    .validate_replay(&action, maintenance.node(), maintenance.session())
                    .map_err(operation)?;
                completion
                    .accepted
                    .validate_result(&completion.outcome)
                    .map_err(operation)?;
                report.maintenance_failure = completion.execution_error.clone();
                if !completion.committed {
                    if let Some(error) = &completion.journal_error {
                        report.maintenance_failure = Some(Arc::clone(error));
                    }
                    report.blocked(DrainBlocker::PendingPublication);
                    return Ok(());
                }
                match completion.outcome.outcome {
                    FleetOutcome::RolesSettledAt {
                        inventory,
                        head_revision,
                        registry,
                    } if inventory == settlement.inventory()
                        && head_revision == settlement.head_revision()
                        && registry == settlement.registry() =>
                    {
                        self.commit(
                            clock,
                            report,
                            JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(
                                cellule_runtime::fleet::operations::DrainEvidence {
                                    node: maintenance.node(),
                                    session: maintenance.session(),
                                    remaining_cells: 0,
                                    unresolved_attempts: 0,
                                    relocated: true,
                                    readers_settled: true,
                                    followers_settled: true,
                                    facilities_closed: false,
                                    stopped: false,
                                    withdrawn: false,
                                },
                            )),
                        )
                        .await?;
                    }
                    FleetOutcome::Rejected(blocker) | FleetOutcome::Blocked(blocker) => {
                        report.blocked(blocker);
                    }
                    FleetOutcome::Unknown => {
                        report.blocked(DrainBlocker::OutcomeUnknown);
                    }
                    _ => return Err(operation(OperationError::Conflict)),
                }
            }
            MaintenancePhase::Closing => {
                // The committed Closing record carries the complete relocation
                // and role barrier. Finalize is a separately retained host task;
                // its action result is committed only after drain and withdrawal.
                let roster =
                    FleetRoster::collect(self.journal.as_ref(), &report.snapshot, clock.deadline)
                        .await?;
                let target_boot = roster
                    .enrollments()
                    .iter()
                    .find(|row| {
                        row.spec().target.node == maintenance.node()
                            && row.spec().target.session == maintenance.session()
                            && matches!(
                                row.spec().role,
                                cellule_runtime::fleet::operations::EnrollmentRole::Node { .. }
                            )
                    })
                    .cloned();
                let Some(target_boot) = target_boot else {
                    report.blocked(DrainBlocker::IncompleteObservation);
                    return Ok(());
                };
                let target_retired = target_boot.status()
                    == cellule_runtime::fleet::operations::EnrollmentStatus::Retired;
                let mut closed_observation = None;
                if target_retired {
                    let observation = call(
                        clock.deadline,
                        "fleet-observer",
                        self.observer.observe(&roster, clock.now()?, clock.deadline),
                    )
                    .await?;
                    if observation.scope != self.scope
                        || observation.registry != report.snapshot.registry()
                    {
                        return Err(operation(OperationError::Conflict));
                    }
                    let observation = observation.with_roster(roster)?;
                    observation
                        .roster()
                        .ok_or(Error::Control("closed-boot roster was not retained"))?
                        .confirm(self.journal.as_ref(), clock.deadline)
                        .await?;
                    let closure = observation.failed_boot_closures().and_then(|closures| {
                        closures
                            .iter()
                            .find(|closure| closure.boot() == &target_boot)
                    });
                    if closure.is_none_or(|closure| {
                        closure.snapshot() != &report.snapshot
                            || closure.boot().status()
                                != cellule_runtime::fleet::operations::EnrollmentStatus::Retired
                            || closure.canonical().node() != maintenance.node()
                            || closure.canonical().session() != maintenance.session()
                    }) {
                        report.blocked(DrainBlocker::IncompleteObservation);
                        return Ok(());
                    }
                    closed_observation = Some(observation);
                }
                let action = report
                    .snapshot
                    .head()
                    .maintenance_action(MaintenanceAction::Finalize, clock.now()?)
                    .map_err(operation)?;
                report.dispatched += 1;
                let completion = if let Some(observation) = &closed_observation {
                    let closure = observation
                        .failed_boot_closures()
                        .and_then(|closures| {
                            closures.iter().find(|closure| {
                                closure.boot().spec().target.node == maintenance.node()
                                    && closure.boot().spec().target.session == maintenance.session()
                            })
                        })
                        .ok_or(Error::Control(
                            "closed-boot finalization lost its process closure",
                        ))?;
                    call(
                        clock.deadline,
                        "fleet-transport",
                        self.transport.finalize_after_process_closure(
                            &action,
                            closure,
                            clock.deadline,
                        ),
                    )
                    .await?
                } else {
                    call(
                        clock.deadline,
                        "fleet-transport",
                        self.transport.dispatch(&action, clock.deadline),
                    )
                    .await?
                };
                completion
                    .accepted
                    .validate_replay(&action, maintenance.node(), maintenance.session())
                    .map_err(operation)?;
                completion
                    .accepted
                    .validate_result(&completion.outcome)
                    .map_err(operation)?;
                report.maintenance_failure = completion.execution_error.clone();
                if !completion.committed {
                    if let Some(error) = &completion.journal_error {
                        report.maintenance_failure = Some(Arc::clone(error));
                    }
                    report.blocked(DrainBlocker::PendingPublication);
                    return Ok(());
                }
                match completion.outcome.outcome {
                    FleetOutcome::Stopped(evidence) => {
                        // Exact boot retirement advances the registry inside
                        // the enrollment journal. Refresh the complete snapshot
                        // before the head CAS, then revalidate that the same
                        // Closing operation and evidence still own this result.
                        let latest = call(
                            clock.deadline,
                            "fleet-journal",
                            self.journal.load_snapshot(self.scope),
                        )
                        .await?;
                        let latest_epoch = self.controller_epoch(&latest, clock.now()?)?;
                        let dispatched_epoch = report
                            .snapshot
                            .head()
                            .controller()
                            .map(|lease| lease.epoch)
                            .ok_or_else(|| operation(OperationError::Fenced))?;
                        if latest_epoch != dispatched_epoch {
                            return Err(operation(OperationError::Fenced));
                        }
                        let current = latest
                            .head()
                            .maintenance()
                            .ok_or_else(|| operation(OperationError::NotFound))?;
                        if !same_maintenance_identity(current, &maintenance) {
                            return Err(operation(OperationError::Conflict));
                        }
                        match current.phase() {
                            MaintenancePhase::Closing
                                if current.drain_evidence() == maintenance.drain_evidence() =>
                            {
                                report.snapshot = latest;
                                self.commit(
                                    clock,
                                    report,
                                    JournalTransition::Maintenance(MaintenanceEvent::Stopped(
                                        evidence,
                                    )),
                                )
                                .await?;
                            }
                            MaintenancePhase::Completed
                                if current.drain_evidence() == Some(evidence) =>
                            {
                                // Another controller already published this
                                // exact terminal result while this waiter ran.
                                report.snapshot = latest;
                            }
                            _ => return Err(operation(OperationError::Conflict)),
                        }
                    }
                    FleetOutcome::Rejected(blocker) | FleetOutcome::Blocked(blocker) => {
                        report.blocked(blocker);
                    }
                    FleetOutcome::Unknown => {
                        report.blocked(DrainBlocker::OutcomeUnknown);
                    }
                    _ => return Err(operation(OperationError::Conflict)),
                }
            }
            MaintenancePhase::Completed => {}
        }
        if maintenance.phase() != MaintenancePhase::Completed
            && clock.now()? >= maintenance.deadline_ms()
        {
            report.blocked(DrainBlocker::Deadline);
        }
        Ok(())
    }
}

fn same_maintenance_identity(
    current: &MaintenanceOperation,
    dispatched: &MaintenanceOperation,
) -> bool {
    current.id() == dispatched.id()
        && current.request_digest() == dispatched.request_digest()
        && current.node() == dispatched.node()
        && current.session() == dispatched.session()
        && current.intent_revision() == dispatched.intent_revision()
}
