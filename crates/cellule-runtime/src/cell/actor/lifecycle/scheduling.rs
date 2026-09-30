//! Coordination decisions that schedule the loop's next step.

use super::*;

pub(in crate::cell::actor) fn schedule(
    active: &mut ActiveCell,
    lease_live: bool,
) -> CoordinationDecision {
    let publication_blocked = active.queue.front().is_some_and(|work| {
        matches!(work, QueuedWork::Command(_))
            && (active.coordination.publication_count() >= MAX_PENDING_PUBLICATIONS
                || active.publication_bytes >= PENDING_PUBLICATION_HIGH_WATER_BYTES)
    });
    active.coordination.step(CoordinationInput::Schedule {
        queue_empty: active.queue.is_empty(),
        publisher_ready: active.publisher.is_some(),
        publication_blocked,
        lease_live,
    })
}

pub(in crate::cell::actor) fn start_next(
    active: &mut ActiveCell,
    pool: &SqlWorkerPool,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    let decision = schedule(active, node_lease.check().is_ok());
    if matches!(decision, CoordinationDecision::Fence) {
        fence_active(active);
        return;
    }
    if !matches!(decision, CoordinationDecision::StartQueuedWork) {
        return;
    }
    let Some(work) = active.queue.pop_front() else {
        return;
    };
    if matches!(&work, QueuedWork::Command(_) | QueuedWork::Migration(_)) {
        // Durable command outcomes, effects, Queue rows, and Workflow runs
        // remain release obligations until a fresh inventory proves otherwise.
        active.persisted_work = crate::primitives::maintenance::PersistedWorkInventory::unknown();
    }
    let generation = active.generation;
    active.last_used_ms = unix_millis();
    active.last_work_at = std::time::Instant::now();
    let kind = match &work {
        QueuedWork::Command(_) => AdmissionKind::Command,
        QueuedWork::Query(_) => AdmissionKind::Query,
        QueuedWork::Resolve(_) => AdmissionKind::Resolve,
        QueuedWork::Migration(_) => AdmissionKind::Migration,
    };
    if !matches!(
        active.coordination.step(CoordinationInput::BeginWork {
            kind,
            publisher_ready: active.publisher.is_some(),
        }),
        CoordinationDecision::Started
    ) {
        active.queue.push_front(work);
        return;
    }
    let pool = pool.clone();
    let interrupt = active.interrupt.clone();
    match work {
        QueuedWork::Command(command) => {
            let durability = active.durability_submitter.clone();
            let effect_id = active.begin_task(CoordinationEffect::Work(kind));
            tasks.spawn(async move {
                execute_command(pool, durability, command, interrupt, generation, effect_id).await
            });
        }
        QueuedWork::Query(query) => {
            let effect_id = active.begin_task(CoordinationEffect::Work(kind));
            tasks.spawn(async move {
                execute_query(pool, query, interrupt, generation, effect_id).await
            });
        }
        QueuedWork::Resolve(resolve) => {
            let effect_id = active.begin_task(CoordinationEffect::Work(kind));
            tasks.spawn(async move {
                execute_resolve(pool, resolve, interrupt, generation, effect_id).await
            });
        }
        QueuedWork::Migration(migration) => {
            let Some(publisher) = active.publisher.take() else {
                let mut migration = migration;
                send_migration_reply(&mut migration, Err(Error::Fenced));
                finish_migration(active, true);
                return;
            };
            let effect_id = active.begin_task(CoordinationEffect::Work(kind));
            tasks.spawn(async move {
                execute_migration(
                    pool,
                    Box::new(publisher),
                    migration,
                    interrupt,
                    generation,
                    effect_id,
                )
                .await
            });
        }
    }
}
pub(in crate::cell::actor) fn start_transfer_inspection(
    cell: CellId,
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    let Some(active) = cells.get_mut(&cell) else {
        return;
    };
    if active.transfer.is_none()
        || active.inventory_refreshing
        || !active.queue.is_empty()
        || !active.coordination.can_deactivate()
    {
        return;
    }
    if node_lease.check().is_err() {
        fence_active(active);
        return;
    }
    let generation = active.generation;
    let role = active.role;
    let effect_id = active.begin_task(CoordinationEffect::Inventory);
    active.inventory_refreshing = true;
    let pool = pool.clone();
    tasks.spawn(async move {
        let deadline = std::time::Instant::now() + SQL_WALL_DEADLINE;
        let result = tokio::time::timeout_at(
            deadline.into(),
            pool.transfer_work_inventory(cell, role, unix_millis()),
        )
        .await
        .map_err(|_| Error::Deadline)
        .and_then(|result| result);
        TaskResult::TransferPreflight {
            cell,
            generation,
            effect_id,
            result,
        }
    });
}

/// Due owners are scanned once per tick, then drained in deadline order as
/// renewal I/O completes. Rebuilding the pending set bounds stale entries
/// when Cells depart or change generation while all I/O slots are occupied.
pub(in crate::cell::actor) struct RenewalScheduler {
    due: Vec<(std::time::Instant, [u8; 32], u64)>,
    in_flight: usize,
}

impl RenewalScheduler {
    pub(in crate::cell::actor) fn new() -> Self {
        Self {
            due: Vec::new(),
            in_flight: 0,
        }
    }

    pub(in crate::cell::actor) fn scan_due(&mut self, cells: &HashMap<CellId, ActiveCell>) {
        self.due.clear();
        let now = std::time::Instant::now();
        for (cell, active) in cells {
            let Some(publisher) = active.publisher.as_ref() else {
                continue;
            };
            if publisher.renewal_due(now) {
                self.due
                    .push((publisher.renewal_at(), *cell.as_bytes(), active.generation));
            }
        }
        // Pop the oldest deadline first. Identity breaks ties independently of
        // HashMap iteration order, so newly due Cells cannot overtake old debt.
        self.order_due();
    }

    fn order_due(&mut self) {
        self.due.sort_unstable_by(|left, right| right.cmp(left));
    }

    fn next_due(&mut self) -> Option<(CellId, u64)> {
        self.due
            .pop()
            .map(|(_, bytes, generation)| (CellId::from_bytes(bytes), generation))
    }

    pub(in crate::cell::actor) fn finished(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    pub(in crate::cell::actor) fn dispatch(
        &mut self,
        pool: &SqlWorkerPool,
        cells: &mut HashMap<CellId, ActiveCell>,
        tasks: &mut JoinSet<TaskResult>,
        node_lease: &RuntimeNodeLease,
    ) {
        let now = std::time::Instant::now();
        while self.in_flight < MAX_RENEWALS_IN_FLIGHT {
            let Some((cell, generation)) = self.next_due() else {
                break;
            };
            let Some(active) = cells.get_mut(&cell) else {
                continue;
            };
            if active.generation != generation
                || active
                    .publisher
                    .as_ref()
                    .is_none_or(|publisher| !publisher.renewal_due(now))
            {
                continue;
            }
            match active.coordination.step(CoordinationInput::BeginRenewal {
                queue_empty: active.queue.is_empty(),
                publication_idle: active.coordination.publication_count() == 0,
                lease_live: node_lease.check().is_ok(),
            }) {
                CoordinationDecision::Started => {}
                CoordinationDecision::Fence => {
                    fence_active(active);
                    continue;
                }
                _ => continue,
            }
            let Some(mut publisher) = active.publisher.take() else {
                active
                    .coordination
                    .step(CoordinationInput::FinishRenewal { fenced: true });
                continue;
            };
            let effect_id = active.begin_task(CoordinationEffect::Renewal);
            self.in_flight += 1;
            let pool = pool.clone();
            tasks.spawn(async move {
                let result = publisher.renew().await;
                if result.is_err() {
                    let _ = pool.fence(cell).await;
                }
                TaskResult::Renewed {
                    cell,
                    generation,
                    effect_id,
                    publisher: Box::new(publisher),
                    result,
                }
            });
        }
    }
}

pub(in crate::cell::actor) fn start_deactivate(
    cell: CellId,
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    transitioning: &mut HashSet<CellId>,
    tasks: &mut JoinSet<TaskResult>,
) {
    let Some(active) = cells.remove(&cell) else {
        return;
    };
    transitioning.insert(cell);
    let generation = active.generation;
    let pool = pool.clone();
    tasks.spawn(async move {
        let result = async {
            let mut publisher = active.publisher.ok_or(Error::Fenced)?;
            // A release that already published its root may leave a resume
            // record, so the next same-node activation can continue the local
            // database instead of restoring the exact root again.
            let control = publisher.control().value().clone();
            match control.root {
                Some(root) => pool.deactivate_resumable(cell, root, control.code).await?,
                None => pool.deactivate(cell).await?,
            }
            publisher.release().await?;
            // A released Cell that still has a deadline publishes one bounded
            // hint key, so the scheduler finds it without scanning every shard.
            // The hint is an accelerator: a failed write costs a later Tick
            // through the backstop and never a missed deadline.
            if let Some(due_ms) = control.next_due_ms
                && let Err(error) = crate::cell::due::publish(
                    publisher.layout(),
                    control.cell,
                    due_ms,
                    unix_millis(),
                )
                .await
            {
                tracing::debug!(error = %error, "Cell due hint was not published");
            }
            Ok(())
        }
        .await;
        TaskResult::Deactivated {
            cell,
            generation,
            reply: active.drain,
            shutdown_drain: active.coordination.is_shutdown(),
            result,
        }
    });
}

pub(in crate::cell::actor) fn start_fenced_deactivate(
    cell: CellId,
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    transitioning: &mut HashSet<CellId>,
    tasks: &mut JoinSet<TaskResult>,
    preserve_owner: bool,
) {
    let Some(active) = cells.remove(&cell) else {
        return;
    };
    transitioning.insert(cell);
    let generation = active.generation;
    let pool = pool.clone();
    tasks.spawn(async move {
        let result = async {
            pool.discard(cell).await?;
            // An unpublished node-log cut must keep its owner record so
            // takeover seals and replays it instead of treating the Cell as idle.
            if preserve_owner {
                return Ok(());
            }
            let mut publisher = active.publisher.ok_or(Error::Fenced)?;
            publisher.release_after_fence().await
        }
        .await;
        TaskResult::Deactivated {
            cell,
            generation,
            reply: active.drain,
            shutdown_drain: active.coordination.is_shutdown(),
            result,
        }
    });
}

pub(in crate::cell::actor) fn start_orphan_deactivate(
    cell: CellId,
    pool: &SqlWorkerPool,
    mut publisher: CellPublisher,
    transitioning: &mut HashSet<CellId>,
    tasks: &mut JoinSet<TaskResult>,
    shutdown_drain: bool,
    generation: u64,
) {
    let pool = pool.clone();
    tasks.spawn(async move {
        let result = async {
            let control = publisher.control().value().clone();
            match control.root {
                Some(root) => pool.deactivate_resumable(cell, root, control.code).await?,
                None => pool.deactivate(cell).await?,
            }
            publisher.release().await
        }
        .await;
        TaskResult::Deactivated {
            cell,
            generation,
            reply: None,
            shutdown_drain,
            result,
        }
    });
    transitioning.insert(cell);
}

#[cfg(test)]
mod renewal_tests {
    use super::*;

    #[test]
    fn due_owners_are_ordered_and_stale_entries_are_discarded_on_scan() {
        let mut scheduler = RenewalScheduler::new();
        let now = std::time::Instant::now();
        for index in (0_u64..1_000).rev() {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&index.to_be_bytes());
            scheduler
                .due
                .push((now + std::time::Duration::from_millis(index), bytes, index));
        }
        scheduler.order_due();
        for index in 0_u64..1_000 {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&index.to_be_bytes());
            assert_eq!(
                scheduler.next_due(),
                Some((CellId::from_bytes(bytes), index))
            );
        }
        scheduler.due.push((now, [7; 32], 1));
        scheduler.scan_due(&HashMap::new());
        assert!(scheduler.due.is_empty());
    }
}
