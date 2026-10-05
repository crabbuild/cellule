//! Background hydration, inventory, and compaction starters.

use super::*;

pub(in crate::cell::actor) fn start_background_hydration(
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    if node_lease.check().is_err() {
        for active in cells.values_mut() {
            active.coordination.step(CoordinationInput::Fence);
            fence_active(active);
        }
        return;
    }
    let resources = pool.resource_ledger();
    let now = std::time::Instant::now();
    let candidates = cells
        .iter_mut()
        .filter_map(|(cell, active)| {
            if now < active.hydration_retry_at {
                return None;
            }
            let reservation = resources
                .try_reserve(ResourceCost::zero().with_hydration_jobs(1))
                .ok()?;
            match active.coordination.step(CoordinationInput::BeginHydration {
                queue_empty: active.queue.is_empty(),
                publication_idle: active.coordination.publication_count() == 0,
                lease_live: node_lease.check().is_ok(),
            }) {
                CoordinationDecision::Started => {}
                CoordinationDecision::Fence => {
                    drop(reservation);
                    fence_active(active);
                    return None;
                }
                _ => {
                    drop(reservation);
                    return None;
                }
            }
            let effect_id = active.begin_task(CoordinationEffect::Hydration);
            Some((*cell, active.generation, effect_id, reservation))
        })
        .collect::<Vec<_>>();

    for (cell, generation, effect_id, reservation) in candidates {
        let pool = pool.clone();
        tasks.spawn(async move {
            let _reservation = reservation;
            let deadline = std::time::Instant::now() + SQL_WALL_DEADLINE;
            // The worker distinguishes abandoned fetches from uncertain local
            // installation. An outer timeout would erase that safety boundary.
            let result = pool.hydrate(cell, HYDRATION_PAGES_PER_STEP, deadline).await;
            if result.is_err() {
                let _ = pool.fence(cell).await;
            }
            TaskResult::Hydrated {
                cell,
                generation,
                effect_id,
                result,
            }
        });
    }
}

pub(in crate::cell::actor) fn start_background_inventory(
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    if node_lease.check().is_err() {
        for active in cells.values_mut() {
            fence_active(active);
        }
        return;
    }
    const MAX_INVENTORY_IN_FLIGHT: usize = 32;
    let in_flight = cells
        .values()
        .filter(|active| active.inventory_refreshing)
        .count();
    let slots = MAX_INVENTORY_IN_FLIGHT.saturating_sub(in_flight);
    if slots == 0 {
        return;
    }
    let now_ms = unix_millis();
    let mut candidates = cells
        .iter()
        .filter(|(_, active)| {
            !active.busy()
                && !active.draining()
                && !active.inventory_refreshing
                && active.queue.is_empty()
                && active.coordination.publication_count() == 0
                && active
                    .demand
                    .should_refresh(now_ms, active.persisted_work.is_unknown())
        })
        .map(|(cell, active)| (active.demand.refresh_priority(), *cell.as_bytes()))
        .collect::<Vec<_>>();
    // Oldest attempted samples get the next slots; a hot or failing Cell cannot
    // monopolize refresh. Shared SQL-job admission still bounds actual work.
    candidates.sort_unstable();
    candidates.truncate(slots);
    for (_, cell_bytes) in candidates {
        let cell = CellId::from_bytes(cell_bytes);
        let Some(active) = cells.get_mut(&cell) else {
            continue;
        };
        let decision = active.coordination.step(CoordinationInput::BeginInventory {
            queue_empty: active.queue.is_empty(),
            publication_idle: active.coordination.publication_count() == 0,
            inventory_unknown: true,
            refreshing: active.inventory_refreshing,
            lease_live: node_lease.check().is_ok(),
        });
        if matches!(decision, CoordinationDecision::Fence) {
            fence_active(active);
            continue;
        }
        if !matches!(decision, CoordinationDecision::Started) {
            continue;
        }
        let effect_id = active.begin_task(CoordinationEffect::Inventory);
        active.inventory_refreshing = true;
        let generation = active.generation;
        let role = active.role;
        let inventory_revision = active.inventory_revision;
        let pool = pool.clone();
        tasks.spawn(async move {
            let deadline = std::time::Instant::now() + SQL_WALL_DEADLINE;
            let sql_deadline = SqlDeadline::new(deadline);
            let result = tokio::time::timeout_at(
                deadline.into(),
                pool.fleet_inventory(cell, role, now_ms, sql_deadline.clone()),
            )
            .await
            .map_err(|_| {
                sql_deadline.cancel_queued();
                Error::Deadline
            })
            .and_then(|result| result);
            TaskResult::InventoryRefreshed {
                cell,
                generation,
                effect_id,
                inventory_revision,
                result,
            }
        });
    }
}

pub(in crate::cell::actor) fn start_background_compaction(
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    let now = std::time::Instant::now();
    for (cell, active) in cells {
        if now.duration_since(active.last_work_at) < COMPACTION_QUIET
            || now < active.compaction_retry_at
        {
            continue;
        }
        let due = active
            .publisher
            .as_ref()
            .is_some_and(CellPublisher::compaction_due);
        let decision = active
            .coordination
            .step(CoordinationInput::BeginCompaction {
                queue_empty: active.queue.is_empty(),
                publication_idle: active.coordination.publication_count() == 0,
                publisher_ready: active.publisher.is_some(),
                due,
                lease_live: node_lease.check().is_ok(),
            });
        if matches!(decision, CoordinationDecision::Fence) {
            fence_active(active);
            continue;
        }
        if !matches!(decision, CoordinationDecision::Started) {
            continue;
        }
        let admission = match active.publisher.as_ref().map_or_else(
            || Err(Error::Control("compaction publisher unavailable")),
            CellPublisher::try_admit_compaction,
        ) {
            Ok(Some(replica)) => Ok(replica),
            Ok(None) => {
                // Undo the synchronous grant before the actor accepts another
                // message. A waiter has no publisher token, task, or busy Cell.
                active
                    .coordination
                    .step(CoordinationInput::FinishCompaction { fenced: false });
                active.compaction_retry_at = now + COMPACTION_RETRY;
                continue;
            }
            Err(error) => Err(error),
        };
        let Some(mut publisher) = active.publisher.take() else {
            active
                .coordination
                .step(CoordinationInput::FinishCompaction { fenced: true });
            fence_active(active);
            continue;
        };
        let cell = *cell;
        let generation = active.generation;
        let effect_id = active.begin_task(CoordinationEffect::Compaction);
        let pool = pool.clone();
        tasks.spawn(async move {
            let started = std::time::Instant::now();
            let result = match admission {
                Ok(replica) => publisher.compact_one_quiet(replica).await,
                Err(error) => Err(error),
            };
            tracing::debug!(
                elapsed_ms = started.elapsed().as_millis(),
                promoted = matches!(result, Ok(Some(true))),
                retry = matches!(result, Ok(None)),
                succeeded = result.is_ok(),
                "Cell LTX quiet compaction completed"
            );
            if result.is_err() {
                let _ = pool.fence(cell).await;
            }
            TaskResult::Compacted {
                cell,
                generation,
                effect_id,
                publisher: Box::new(publisher),
                result,
            }
        });
    }
}
