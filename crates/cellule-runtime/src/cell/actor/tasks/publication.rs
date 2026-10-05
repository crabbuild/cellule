//! Publication proof, publish, and compaction completions.

use super::*;

/// Applies a publication proof result and answers its waiter.
pub(super) fn handle_proven(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    mut command: Box<QueuedCommand>,
    mut result: crate::Result<StoredOutcome>,
    mut fenced: bool,
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
        // The publication task may fence and remove the actor first; proof owns the
        // caller's final result and must not be rewritten as CellNotActive.
        send_finished_command_reply(&mut command, result);
        return;
    };
    if active.generation != generation
        || !active
            .coordination
            .effect_matches(effect_id, CoordinationEffect::Proof)
    {
        send_finished_command_reply(&mut command, result);
        return;
    }
    active.finish_task(effect_id, CoordinationEffect::Proof);
    if node_lease.check().is_err() {
        result = Err(command.operation.unknown(Error::Fenced));
        fenced = true;
    }
    finish_work(active, fenced);
    send_finished_command_reply(&mut command, result);
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}

/// Selects the latest covered range after shared preparation admission.
pub(super) fn handle_publication_admitted(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    publisher: Box<CellPublisher>,
    result: crate::Result<Box<PublicationAdmission>>,
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
            .effect_matches(effect_id, CoordinationEffect::Publication)
    {
        return;
    }
    let result = result.and_then(|replica| {
        node_lease.check()?;
        if active.coordination.is_fenced() {
            return Err(Error::Fenced);
        }
        Ok(replica)
    });
    match result {
        Ok(replica) => super::super::requests::start_admitted_publication(
            cell, active, pool, tasks, publisher, replica, effect_id,
        ),
        Err(error) => {
            active.finish_task(effect_id, CoordinationEffect::Publication);
            if let Some(newest) = active.publications.back() {
                let elapsed = newest.submitted_at.elapsed();
                publisher.record_publication_timing(crate::fleet::telemetry::PublicationTiming {
                    queue_wait: elapsed,
                    preparation: std::time::Duration::ZERO,
                    authority: std::time::Duration::ZERO,
                    total: elapsed,
                    succeeded: false,
                    commit_sequence: newest.pending.outcome().commit_sequence(),
                });
            }
            active.publisher = Some(*publisher);
            let source = Arc::new(error);
            // Preserve admission's typed source for every original proof waiter.
            // These commits stay recoverable; failure grants no object coverage.
            while let Some(queued) = active.publications.pop_front() {
                active
                    .coordination
                    .step(CoordinationInput::FinishPublication {
                        fenced: true,
                        succeeded: false,
                    });
                active.publication_bytes = active
                    .publication_bytes
                    .saturating_sub(queued.pending.retained_bytes());
                let _ = queued.proof.send(Err(Error::Shared(source.clone())));
            }
            fence_active(active);
            continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
        }
    }
}

/// Applies a publish result, its byte accounting, and its failure cleanup.
#[expect(
    clippy::too_many_arguments,
    reason = "the actor loop hands each protocol facility and finished-task field to the handler explicitly"
)]
pub(super) fn handle_published(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    publisher: Box<CellPublisher>,
    retained_bytes: u64,
    node_log_bytes: u64,
    covered: u64,
    covered_node_logs: u64,
    next_due_ms: Option<i64>,
    commit_sequence: u64,
    mut result: crate::Result<()>,
    mut fenced: bool,
) {
    let TaskContext {
        pool,
        cells,
        transitioning,
        tasks,
        node_lease,
        unpublished_node_log_bytes,
        publications,
        ..
    } = context;
    let Some(active) = cells.get_mut(&cell) else {
        return;
    };
    if active.generation != generation
        || !active
            .coordination
            .effect_matches(effect_id, CoordinationEffect::Publication)
    {
        return;
    }
    active.finish_task(effect_id, CoordinationEffect::Publication);
    active.last_work_at = std::time::Instant::now();
    let object_published = result.is_ok();
    if object_published {
        // Control now names this commit, so the local mirror can answer a due
        // scan without reading the record back.
        active.next_due_ms = next_due_ms;
        active.published_sequence = commit_sequence;
    }
    if node_lease.check().is_err() {
        result = Err(Error::Fenced);
        fenced = true;
    }
    active.publisher = Some(*publisher);
    active.publication_bytes = active.publication_bytes.saturating_sub(retained_bytes);
    if object_published && covered_node_logs > 0 {
        active.unpublished_node_logs = active
            .unpublished_node_logs
            .saturating_sub(usize::try_from(covered_node_logs).unwrap_or(usize::MAX));
        subtract_unpublished_bytes(unpublished_node_log_bytes, node_log_bytes);
    }
    // One queued publication per covered commit was accounted at admission, so
    // each of them completes here.
    let mut fence = false;
    for _ in 0..covered {
        let decision = active
            .coordination
            .step(CoordinationInput::FinishPublication {
                fenced,
                succeeded: result.is_ok(),
            });
        fence |= matches!(decision, CoordinationDecision::Fence);
    }
    if fence {
        fence_active(active);
    } else {
        // Only the completed object path can wake snapshot readers. Fleet proof
        // may acknowledge earlier; hints never substitute for a published root.
        if result.is_ok() {
            let _ = publications.send(active.catalog.entry().clone());
        }
        start_publication(cell, active, tasks);
    }
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}

/// Rechecks the current Cell before converting shared admission into work.
pub(super) fn handle_compaction_admitted(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    result: crate::Result<Option<Box<cellule_ltx::CellReplica>>>,
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
    if active.generation != generation {
        return;
    }
    let Some(admission) = active.compaction_admission.as_mut() else {
        return;
    };
    if !admission.pending {
        return;
    }
    let now = std::time::Instant::now();
    let replica = match result {
        Ok(Some(replica)) if !admission.cancel.is_cancelled() => Some(replica),
        Err(error) => {
            tracing::warn!(cell = ?cell, error = ?error, "Cell compaction admission fenced its owner");
            fence_active(active);
            None
        }
        _ => None,
    };
    if let Some(replica) = replica {
        let due = now.duration_since(active.last_work_at) >= COMPACTION_QUIET
            && active
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
        if matches!(decision, CoordinationDecision::Started) {
            if let Some(admission) = &mut active.compaction_admission {
                admission.pending = false;
            }
            start_admitted_compaction(cell, active, *replica, pool, tasks);
            return;
        }
        if matches!(decision, CoordinationDecision::Fence) {
            fence_active(active);
        }
    }
    active.compaction_admission = None;
    active.compaction_retry_at = now + COMPACTION_RETRY;
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}

/// Applies a compaction result and returns the publisher to the Cell.
pub(super) fn handle_compacted(
    context: TaskContext<'_>,
    cell: CellId,
    generation: u64,
    effect_id: u64,
    publisher: Box<CellPublisher>,
    result: crate::Result<Option<bool>>,
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
            .effect_matches(effect_id, CoordinationEffect::Compaction)
    {
        return;
    }
    active.finish_task(effect_id, CoordinationEffect::Compaction);
    active.compaction_admission = None;
    if let Err(error) = &result {
        tracing::warn!(cell = ?cell, error = ?error, "Cell compaction fenced its owner");
    }
    let fenced = result.is_err() || node_lease.check().is_err();
    active.publisher = Some(*publisher);
    if matches!(result, Ok(None)) {
        active.compaction_retry_at = std::time::Instant::now() + COMPACTION_RETRY;
    }
    let decision = active
        .coordination
        .step(CoordinationInput::FinishCompaction { fenced });
    if matches!(decision, CoordinationDecision::Fence) {
        fence_active(active);
    }
    continue_cell(cell, pool, cells, transitioning, tasks, node_lease);
}
