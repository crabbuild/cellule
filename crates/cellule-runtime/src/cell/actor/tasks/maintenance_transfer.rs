//! Fresh maintenance readiness and the canonical final release barrier.

use super::super::maintenance::{refuse, reopen_completion};
use super::*;
use crate::fleet::operations::DrainBlocker;
use crate::primitives::maintenance_readiness::MaintenanceWorkInventory;

pub(super) fn handle(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    result: crate::Result<MaintenanceWorkInventory>,
) {
    let TaskContext {
        pool,
        cells,
        transitioning,
        tasks,
        node_lease,
        movement,
        movement_permits,
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
    let Some(mut transfer) = active.transfer.take() else {
        continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
        return;
    };
    let Some(state) = transfer.maintenance.as_mut() else {
        // A stale completion cannot classify a different transfer as maintenance.
        active.transfer = Some(transfer);
        continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
        return;
    };
    if state.inventory_effect != Some(effect_id) {
        // A previous request may expire while its read is still owned. Complete
        // that effect, but never feed its snapshot/error to a newer request.
        active.transfer = Some(transfer);
        continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
        return;
    }
    state.inventory_effect = None;
    let now = std::time::Instant::now();
    let failure = if node_lease.check().is_err() || active.coordination.is_fenced() {
        super::super::admission::fence_active(active);
        let _ = transfer.reply.send(Err(Error::Fenced));
        true
    } else if now >= state.deadline {
        reopen_completion(active);
        refuse(transfer.reply, DrainBlocker::Deadline, None);
        true
    } else {
        match result {
            Err(Error::Fenced) => {
                super::super::admission::fence_active(active);
                let _ = transfer.reply.send(Err(Error::Fenced));
                true
            }
            Err(error) => {
                reopen_completion(active);
                refuse(transfer.reply, DrainBlocker::UnknownInventory, Some(error));
                true
            }
            Ok(inventory) if !inventory.is_transferable() => {
                reopen_completion(active);
                state.closing = false;
                state.next_check = now + HYDRATION_TICK;
                active.transfer = Some(transfer);
                false
            }
            Ok(_) if !state.closing => {
                let decision =
                    active
                        .coordination
                        .step(CoordinationInput::BeginTransferPreflight {
                            queue_empty: active.queue.is_empty(),
                            publication_idle: active.coordination.publication_count() == 0,
                            lease_live: node_lease.check().is_ok(),
                        });
                if decision == CoordinationDecision::Started {
                    // Close every new capability before joining accepted native
                    // work and refreshing readiness again. Keep semaphore owners
                    // intact until confirm, so a definite refusal can restore
                    // only native completion on this same exact activation.
                    active.admission.draining.store(true, Ordering::Release);
                    state.closing = true;
                }
                state.next_check = now + HYDRATION_TICK;
                active.transfer = Some(transfer);
                false
            }
            Ok(_) => {
                if active.queue.is_empty()
                    && active.coordination.can_deactivate()
                    && active.publisher.is_some()
                    && active.unpublished_node_logs == 0
                    && active.coordination.step(CoordinationInput::ConfirmTransfer)
                        == CoordinationDecision::ReadyToDeactivate
                {
                    active.admission.requests.close();
                    active.admission.bytes.close();
                    active.drain = Some(transfer.reply);
                    start_deactivate(cell, pool, cells, transitioning, tasks);
                    return;
                }
                state.next_check = now + HYDRATION_TICK;
                active.transfer = Some(transfer);
                false
            }
        }
    };
    if failure && let Some(mut permit) = movement_permits.remove(&cell) {
        movement.complete(&mut permit);
    }
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}
