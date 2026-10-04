use super::*;

pub(in crate::cell::actor) fn begin(
    request: ReleaseRequest,
    cells: &mut HashMap<CellId, ActiveCell>,
    movement: &mut MovementBudget,
    permits: &mut HashMap<CellId, MovementPermit>,
) -> Option<CellId> {
    let ReleaseRequest {
        cell,
        generation,
        incarnation,
        epoch,
        deadline,
        reply,
    } = request;
    let reply = DrainReply::Maintenance(reply);
    let Some(active) = cells.get_mut(&cell) else {
        let _ = reply.send(Err(Error::CellNotActive));
        return None;
    };
    if active.generation != generation || active.incarnation != incarnation {
        let _ = reply.send(Err(Error::Fenced));
        return None;
    }
    let Some(publisher) = active.publisher.as_ref() else {
        refuse(reply, DrainBlocker::BusyExecution, None);
        return None;
    };
    if publisher.control().value().epoch != epoch {
        let _ = reply.send(Err(Error::Fenced));
        return None;
    }
    if active.draining() || active.transfer.is_some() || active.drain.is_some() {
        refuse(reply, DrainBlocker::BusyExecution, None);
        return None;
    }
    if active.role == CatalogRole::Blob {
        // Foreground closure cannot inventory external stream/upload/pin owners.
        // Leave their completion path intact until that barrier is implemented.
        refuse(reply, DrainBlocker::UnknownInventory, None);
        return None;
    }
    let now = std::time::Instant::now();
    if now >= deadline {
        refuse(reply, DrainBlocker::Deadline, None);
        return None;
    }
    let mut permit = match movement.try_start_requested(unix_millis()) {
        Ok(permit) => permit,
        Err(error) => {
            refuse(reply, DrainBlocker::MovementBudget, Some(error));
            return None;
        }
    };
    match active
        .coordination
        .step(CoordinationInput::BeginMaintenanceQuiescence)
    {
        CoordinationDecision::Started => {}
        CoordinationDecision::Reject(reason) => {
            movement.complete(&mut permit);
            let _ = reply.send(Err(super::super::admission::rejection_error(reason)));
            return None;
        }
        _ => {
            movement.complete(&mut permit);
            refuse(reply, DrainBlocker::BusyExecution, None);
            return None;
        }
    }
    active
        .admission
        .maintenance_quiescing
        .store(true, Ordering::Release);
    active.transfer = Some(TransferPreflight {
        reply,
        maintenance: Some(ReleaseState {
            deadline,
            next_check: now,
            closing: false,
            inventory_effect: None,
        }),
    });
    permits.insert(cell, permit);
    Some(cell)
}

pub(in crate::cell::actor) fn drive(
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    transitioning: &mut HashSet<CellId>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
    movement: &mut MovementBudget,
    permits: &mut HashMap<CellId, MovementPermit>,
) {
    // Requested movement permits bound this scan. Do not scan the whole resident
    // fleet on every ingress message or add a second timer/rate limiter.
    let pending = permits
        .keys()
        .copied()
        .filter(|cell| {
            cells
                .get(cell)
                .and_then(|active| active.transfer.as_ref())
                .is_some_and(|transfer| transfer.maintenance.is_some())
        })
        .collect::<Vec<_>>();
    for cell in pending {
        let Some(active) = cells.get_mut(&cell) else {
            continue;
        };
        let expired = active
            .transfer
            .as_ref()
            .and_then(|transfer| transfer.maintenance.as_ref())
            .is_some_and(|state| std::time::Instant::now() >= state.deadline);
        if expired {
            if let Some(transfer) = active.transfer.take() {
                reopen_completion(active);
                refuse(transfer.reply, DrainBlocker::Deadline, None);
            }
            if let Some(mut permit) = permits.remove(&cell) {
                movement.complete(&mut permit);
            }
            continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
        } else {
            inspect(cell, pool, cells, tasks, node_lease);
        }
    }
}
