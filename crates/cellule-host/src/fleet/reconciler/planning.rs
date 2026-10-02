use super::*;
use cellule_runtime::fleet::operations::{
    AttemptId, FleetHead, MaintenanceOperation, MoveAttemptSpec, OperationId,
};
use cellule_runtime::fleet::placement::{CellTransferDemand, PlacementPlanner, PlacementPressure};

impl FleetReconciler {
    pub(super) async fn plan(
        &self,
        clock: &PassClock<'_>,
        report: &mut FleetReconcileReport,
    ) -> Result<()> {
        if report.snapshot.head().attempts().len() >= self.profile.max_inflight {
            report.blocked(DrainBlocker::MovementBudget);
            return Ok(());
        }
        if report.snapshot.registry().bootstrap_revision().is_none() {
            report.blocked(DrainBlocker::IncompleteObservation);
            return Ok(());
        }
        let roster = crate::fleet::FleetRoster::collect(
            self.journal.as_ref(),
            &report.snapshot,
            clock.deadline,
        )
        .await?;
        let observation = call(
            clock.deadline,
            "fleet-observer",
            self.observer.observe(&roster, clock.now()?, clock.deadline),
        )
        .await?;
        if observation.scope != self.scope || observation.registry != report.snapshot.registry() {
            return Err(operation(OperationError::Conflict));
        }
        roster
            .confirm(self.journal.as_ref(), clock.deadline)
            .await?;
        let now = clock.now()?;
        let boot_complete = roster.covers_advertisements(&observation.nodes, now)?;
        let enrollment_settled = roster.enrollments().iter().all(|record| {
            record.status() != cellule_runtime::fleet::operations::EnrollmentStatus::Pending
        });
        let observation = observation.with_roster(roster)?;
        let mut placements = observation.placements(now)?;
        let inventory_complete = observation.complete
            && boot_complete
            && enrollment_settled
            && observation.counts_match(&placements);
        // The most recently retired page is a post-batch sample barrier. Query
        // all earlier movement times as well: late retirement must not make
        // the count rule forget a more recent completion in an earlier page.
        let since = call(
            clock.deadline,
            "fleet-journal",
            self.journal.last_movement_at(&report.snapshot),
        )
        .await?
        .unwrap_or(-1);
        let planner = PlacementPlanner::default();
        let count_fresh = inventory_complete
            && report.snapshot.head().attempts().is_empty()
            && placements.iter().all(|node| node.observed_at_ms > since);
        if !inventory_complete {
            report.blocked(DrainBlocker::IncompleteObservation);
        } else if !count_fresh {
            report.blocked(DrainBlocker::StaleObservation);
        }
        // Retained intents take precedence over cached signed advertisements.
        for intent in observation
            .roster()
            .ok_or(Error::Control("fleet roster was not retained"))?
            .intents()
        {
            if let Some(node) = placements
                .iter_mut()
                .find(|node| node.node == intent.node())
            {
                if node.session != intent.session() {
                    return Err(operation(OperationError::Conflict));
                }
                node.draining |= intent.mode() != cellule_runtime::node::NodeMode::Active;
            }
        }
        let mut demands = Vec::new();
        for owned in &observation.cells {
            let row = &owned.observation;
            if report
                .snapshot
                .head()
                .attempts()
                .iter()
                .any(|attempt| attempt.spec().target.cell_id() == row.target.cell_id())
                || report
                    .snapshot
                    .head()
                    .maintenance()
                    .is_some_and(|operation| {
                        operation.node() == owned.node
                            && (operation.phase() != MaintenancePhase::Evacuating
                                || now >= operation.deadline_ms())
                    })
            {
                continue;
            }
            let maintenance = maintenance_for(report.snapshot.head(), owned, now).is_some();
            if maintenance && row.role == cellule_runtime::cell::catalog::CatalogRole::Blob {
                report.blocked(DrainBlocker::UnknownInventory);
                continue;
            }
            let Some(cost) = (if maintenance {
                row.maintenance_cost
            } else {
                row.cost
            }) else {
                continue;
            };
            cost.validate().map_err(operation)?;
            let settled = row.blockers.is_empty()
                && row.work_blocker.is_none()
                && row
                    .sampled_at_ms
                    .is_some_and(|at| at >= 0 && at <= now && now - at <= 30_000);
            // These local conditions are joined/rechecked by explicit busy
            // release after receiver preparation. Other role/fleet blockers
            // cannot be discharged by a Cell actor readiness read.
            if maintenance
                && let Some(blocker) = row.blockers.iter().find(|blocker| {
                    !matches!(
                        blocker,
                        DrainBlocker::BusyExecution
                            | DrainBlocker::ExternalLease
                            | DrainBlocker::PendingPublication
                            | DrainBlocker::UnknownInventory
                    )
                })
            {
                report.blocked(*blocker);
                continue;
            }
            if row.position.as_ref().is_none_or(|position| {
                position.incarnation != row.incarnation || position.epoch == 0
            }) || (!maintenance && !settled)
            {
                continue;
            }
            let source = placements
                .iter()
                .find(|node| node.session == owned.session)
                .ok_or(Error::Node("fleet donor observation absent"))?;
            if !count_fresh && !source.draining && source.pressure < PlacementPressure::Shedding {
                continue;
            }
            let moved = call(
                clock.deadline,
                "fleet-journal",
                self.journal
                    .last_moved_at(&report.snapshot, row.target.cell_id(), row.incarnation),
            )
            .await?;
            demands.push(CellTransferDemand {
                cell: row.target.cell_id(),
                source: owned.session,
                generation: row.generation,
                memory_bytes: cost.memory_bytes,
                disk_bytes: cost.disk_bytes,
                job_credits: cost.job_credits,
                resident_since_ms: row.resident_since_ms,
                last_moved_at_ms: moved,
                stable_observations: row.stable_observations,
                settled,
                maintenance,
            });
        }
        // Unknown receives stay charged independently of lagging advertisements.
        // Projection is conservative until permit retirement; pressure relief
        // can use the remaining count/byte budget without inventing count balance.
        for attempt in report.snapshot.head().attempts() {
            if let Some(receiver) = placements
                .iter_mut()
                .find(|node| node.session == attempt.spec().destination)
            {
                let cost = attempt.spec().cost;
                receiver.free_memory_bytes =
                    receiver.free_memory_bytes.saturating_sub(cost.memory_bytes);
                receiver.free_disk_bytes = receiver.free_disk_bytes.saturating_sub(cost.disk_bytes);
                receiver.active_cells = receiver.active_cells.saturating_add(1);
                receiver.running_jobs = receiver.running_jobs.saturating_add(cost.job_credits);
            }
        }
        // Recompute balance using the final eligibility inputs after intents.
        let balance = if count_fresh {
            planner.fleet_balance(now, &placements, since)?
        } else {
            None
        };
        let proposals = planner.plan_transfers(now, &placements, &demands, balance.as_ref())?;
        let digest = observation.digest(now)?;
        let operation_id =
            OperationId::from_bytes(*uuid::Uuid::now_v7().as_bytes()).map_err(operation)?;
        for proposal in proposals {
            let head = report.snapshot.head();
            if head.attempts().len() >= self.profile.max_inflight
                || head
                    .reserved_restore_bytes()
                    .checked_add(proposal.disk_bytes)
                    .is_none_or(|bytes| bytes > self.profile.max_restore_bytes)
            {
                report.blocked(DrainBlocker::MovementBudget);
                break;
            }
            let owned = observation
                .cells
                .iter()
                .find(|row| {
                    row.observation.target.cell_id() == proposal.cell
                        && row.session == proposal.source
                        && row.observation.generation == proposal.generation
                })
                .ok_or(Error::Node("fleet proposal lost its actor observation"))?;
            let row = &owned.observation;
            let destination = placements
                .iter()
                .find(|node| node.session == proposal.destination)
                .ok_or(Error::Node("fleet proposal receiver absent"))?;
            let position = row
                .position
                .as_ref()
                .ok_or(Error::Node("fleet proposal position absent"))?;
            let maintenance = maintenance_for(head, owned, now);
            let cost = if maintenance.is_some() {
                row.maintenance_cost
            } else {
                row.cost
            }
            .ok_or(Error::Node("fleet proposal cost absent"))?;
            let id = AttemptId {
                operation: maintenance.map_or(operation_id, |m| m.id()),
                sequence: head.next_sequence(),
            };
            let spec = MoveAttemptSpec {
                id,
                target: row.target.clone(),
                incarnation: row.incarnation,
                source_node: owned.node,
                source: owned.session,
                generation: row.generation,
                source_epoch: position.epoch,
                destination_node: destination.node,
                destination: destination.session,
                cost,
                snapshot_digest: digest,
                deadline_ms: now
                    .checked_add(self.profile.controller_lease_ms)
                    .ok_or(Error::Control("fleet movement deadline overflow"))?
                    .min(maintenance.map_or(i64::MAX, |m| m.deadline_ms())),
            };
            self.commit(clock, report, JournalTransition::Allocate(spec))
                .await?;
            report.allocated += 1;
        }
        Ok(())
    }
}

fn maintenance_for<'a>(
    head: &'a FleetHead,
    owned: &FleetOwnedCell,
    now: i64,
) -> Option<&'a MaintenanceOperation> {
    head.maintenance().filter(|operation| {
        operation.node() == owned.node
            && operation.session() == owned.session
            && operation.phase() == MaintenancePhase::Evacuating
            && now < operation.deadline_ms()
    })
}
