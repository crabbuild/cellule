//! Inventory refresh, lease renewal, and deactivation completions.

use super::*;

/// Applies an inventory refresh result to the Cell's persisted work.
pub(super) fn handle_inventory_refreshed(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    inventory_revision: u64,
    result: crate::Result<crate::cell::worker::WorkerCellInventory>,
) {
    let TaskContext {
        pool,
        cells,
        transitioning,
        tasks,
        node_lease,
        ..
    } = context;
    let Some(active) = cells.get_mut(&cell) else {
        return;
    };
    if active.generation != generation
        || !active
            .coordination
            .effect_matches(effect_id, CoordinationEffect::Inventory)
    {
        return;
    }
    active.finish_task(effect_id, CoordinationEffect::Inventory);
    active.inventory_refreshing = false;
    // An inventory effect can finish after another foreground mutation starts.
    // Finish its effect, but do not let its older rows clear that mutation's
    // unknown-work marker or recreate obsolete demand.
    if active.inventory_revision != inventory_revision {
        continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
        return;
    }
    match result {
        Ok(sample) => {
            if let Err(error) =
                active
                    .demand
                    .record(sample, active.resource_limits, active.published_sequence)
            {
                active.persisted_work =
                    crate::primitives::maintenance::PersistedWorkInventory::unknown();
                tracing::debug!(cell = ?cell, error = ?error, "Cell demand remains unknown after inventory");
            } else {
                active.persisted_work = sample.persisted_work;
            }
        }
        Err(error) => {
            active.persisted_work =
                crate::primitives::maintenance::PersistedWorkInventory::unknown();
            active.demand.failed(unix_millis());
            tracing::debug!(cell = ?cell, error = ?error, "Cell demand inspection failed");
        }
    }
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}

/// Applies a lease renewal result and continues the Cell's schedule.
pub(super) fn handle_renewed(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    publisher: Box<CellPublisher>,
    mut result: crate::Result<()>,
) {
    let TaskContext {
        pool,
        cells,
        transitioning,
        tasks,
        node_lease,
        ..
    } = context;
    let Some(active) = cells.get_mut(&cell) else {
        return;
    };
    if active.generation != generation
        || !active
            .coordination
            .effect_matches(effect_id, CoordinationEffect::Renewal)
    {
        return;
    }
    active.finish_task(effect_id, CoordinationEffect::Renewal);
    if let Err(error) = &result {
        tracing::warn!(cell = ?cell, error = ?error, "Cell renewal fenced its owner");
    }
    if node_lease.check().is_err() {
        result = Err(Error::Fenced);
    }
    active.publisher = Some(*publisher);
    let decision = active.coordination.step(CoordinationInput::FinishRenewal {
        fenced: result.is_err(),
    });
    if matches!(decision, CoordinationDecision::Fence) {
        fence_active(active);
    }
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}

/// Applies a deactivation result, releasing movement permits and shutdown waiters.
pub(super) fn handle_deactivated(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    reply: Option<DrainReply>,
    shutdown_drain: bool,
    result: crate::Result<()>,
    released: Option<crate::fleet::operations::PublishedPosition>,
) {
    let TaskContext {
        cells,
        transitioning,
        shutdown,
        movement,
        movement_permits,
        ..
    } = context;
    if cells
        .get(&cell)
        .is_some_and(|active| active.generation != generation)
    {
        return;
    }
    if let Some(mut permit) = movement_permits.remove(&cell) {
        movement.complete(&mut permit);
    }
    transitioning.remove(&cell);
    let runtime_waiting = shutdown_drain || shutdown.draining;
    match reply {
        Some(reply) => {
            let failed = result.is_err();
            match reply.send_released(result, released) {
                Err(Err(error)) => fail_shutdown(shutdown, error),
                _ if runtime_waiting && failed => fail_shutdown(
                    shutdown,
                    Error::Control("one or more Cells failed to drain"),
                ),
                _ => {}
            }
        }
        None => {
            if let Err(error) = result {
                fail_shutdown(shutdown, error);
            }
        }
    }
}
