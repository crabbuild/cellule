use super::*;

pub(in crate::cell::actor) fn inspect(
    cell: CellId,
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    let Some(active) = cells.get_mut(&cell) else {
        return;
    };
    let Some(state) = active
        .transfer
        .as_ref()
        .and_then(|transfer| transfer.maintenance.as_ref())
    else {
        return;
    };
    let now = std::time::Instant::now();
    if now < state.next_check || now >= state.deadline {
        return;
    }
    let deadline = state.deadline.min(now + SQL_WALL_DEADLINE);
    let decision = active
        .coordination
        .step(CoordinationInput::BeginMaintenanceInventory {
            refreshing: active.inventory_refreshing,
            finalizing: state.closing,
            queue_empty: active.queue.is_empty(),
            publisher_ready: active.publisher.is_some(),
            lease_live: node_lease.check().is_ok(),
        });
    if decision == CoordinationDecision::Fence {
        super::super::admission::fence_active(active);
        return;
    }
    if decision != CoordinationDecision::Started {
        return;
    }
    let effect_id = active.begin_task(CoordinationEffect::Inventory);
    active.inventory_refreshing = true;
    if let Some(state) = active
        .transfer
        .as_mut()
        .and_then(|transfer| transfer.maintenance.as_mut())
    {
        state.inventory_effect = Some(effect_id);
    }
    let generation = active.generation;
    let role = active.role;
    let pool = pool.clone();
    tasks.spawn(async move {
        let sql_deadline = SqlDeadline::new(deadline);
        let operation = pool.fleet_inventory(cell, role, unix_millis(), sql_deadline.clone());
        tokio::pin!(operation);
        let result = match tokio::time::timeout_at(deadline.into(), &mut operation).await {
            Ok(result) => result.map(|inventory| inventory.maintenance_work),
            Err(_) => {
                // A started read remains owned and joined. Queued reads may be
                // cancelled through the ordinary worker deadline capability.
                sql_deadline.cancel_queued();
                let _ = operation.await;
                Err(Error::Deadline)
            }
        };
        TaskResult::MaintenancePreflight {
            cell,
            generation,
            effect_id,
            result,
        }
    });
}
