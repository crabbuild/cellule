use super::*;
use cellule_runtime::fleet::operations::{FleetOutcome, MaintenanceAction, MaintenanceEvent};

impl FleetReconciler {
    pub(super) async fn advance_maintenance(
        &self,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
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
            MaintenancePhase::Evacuating | MaintenancePhase::Closing => {
                // Full role inventory and joined facility/withdrawal evidence
                // are still required. Empty movement permits cannot prove them.
                report.blocked(DrainBlocker::IncompleteObservation);
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
