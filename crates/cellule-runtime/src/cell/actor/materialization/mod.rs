//! Exact shared selection and fair, admitted Cell-root materialization.

use super::*;

pub(super) const CHECKPOINT_COMMANDS: u64 = 215;
const MAX_ROOT_AGE: std::time::Duration = std::time::Duration::from_secs(45);
const MAX_MATERIALIZERS: usize = 8;

pub(super) fn blocks_commands(active: &ActiveCell) -> bool {
    active.root_debt.as_ref().is_some_and(|debt| {
        debt.selected.proof.locator_count() >= CHECKPOINT_COMMANDS as usize
            || debt
                .selected
                .proof
                .native_suffix_bytes()
                .is_ok_and(|bytes| bytes >= 3 << 20)
    })
}

pub(super) fn start_selection(
    cell: CellId,
    active: &mut ActiveCell,
    pool: &SqlWorkerPool,
    tasks: &mut JoinSet<TaskResult>,
    publisher: CellPublisher,
) {
    let coverage: Vec<_> = active.publications.drain(..).collect();
    let covered = coverage.len() as u64;
    let retained_bytes = coverage
        .iter()
        .map(|queued| queued.pending.retained_bytes())
        .sum();
    let generation = active.generation;
    let effect_id = active.begin_task(CoordinationEffect::Publication);
    active.publishing_since = coverage.first().map(|queued| queued.submitted_at);
    // Worker cleanup is dispatched through the same owned pool as SQL. The
    // publisher token excludes root preparation throughout exact selection.
    let pool = pool.clone();
    tasks.spawn(async move {
        let result = async {
            let newest = coverage.last().ok_or(Error::PendingPublication)?;
            let mut captures = Vec::with_capacity(coverage.len());
            for queued in &coverage {
                let durability = queued
                    .durability
                    .as_ref()
                    .ok_or(Error::PendingPublication)?;
                let capture = durability
                    .selected_capture_prefix()
                    .await?
                    .ok_or(Error::Control("managed selection lacks capture"))?;
                captures.push(capture);
            }
            let selected = Arc::clone(captures.last().ok_or(Error::PendingPublication)?.selected());
            if publisher.control().value().bundle_binding != Some(selected.proof.binding())
                || selected.proof.commit_sequence() != newest.pending.outcome().commit_sequence()
                || selected.proof.position() != newest.pending.cuts().position
            {
                return Err(Error::Fenced);
            }
            let released = pool.release_bundle_captures(cell, captures).await?;
            if released.len() != coverage.len()
                || released
                    .iter()
                    .zip(&coverage)
                    .any(|(outcome, queued)| outcome != queued.pending.outcome())
            {
                return Err(Error::Control("selected result differs from queued commit"));
            }
            Ok(Box::new(SelectedPublication {
                covered,
                debt: RootDebt {
                    selected,
                    durability: newest
                        .durability
                        .as_ref()
                        .ok_or(Error::PendingPublication)?
                        .clone(),
                    submitted_at: coverage
                        .first()
                        .ok_or(Error::PendingPublication)?
                        .submitted_at,
                    next_due_ms: newest.pending.next_due_ms(),
                    node_log_bytes: retained_bytes,
                    covered_node_logs: covered,
                },
            }))
        };
        let result = tokio::time::timeout(FLEET_PUBLICATION_GRACE, result)
            .await
            .map_err(|_| Error::Deadline)
            .and_then(|result| result);
        // The original durability receipt owns the Bundle/Fleet ACK; these
        // senders represent only the ordinary root fallback, never a new proof.
        if let Err(error) = &result {
            tracing::warn!(cell = ?cell, error = ?error, "exact shared selection failed");
            let _ = pool.fence(cell).await;
        }
        drop(coverage);
        TaskResult::BundleSelected {
            cell,
            generation,
            effect_id,
            publisher: Box::new(publisher),
            covered,
            retained_bytes,
            result,
        }
    });
}

pub(super) fn dispatch(
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
) {
    let running = cells.values().filter(|active| active.materializing).count();
    let now = std::time::Instant::now();
    let mut ready: Vec<_> = cells
        .iter()
        .filter_map(|(cell, active)| {
            let debt = active.root_debt.as_ref()?;
            let forced = active.draining()
                || active
                    .queue
                    .front()
                    .is_some_and(|work| matches!(work, QueuedWork::Migration(_)))
                || !active.publications.is_empty()
                    && active.publications.iter().any(|queued| {
                        queued
                            .durability
                            .as_ref()
                            .is_none_or(|pending| !pending.has_managed_bundle_capture())
                    });
            (active.publisher.is_some()
                && !active.materializing
                && (forced
                    || blocks_commands(active)
                    || debt
                        .selected
                        .proof
                        .commit_sequence()
                        .saturating_sub(active.published_sequence)
                        >= CHECKPOINT_COMMANDS
                    || now.saturating_duration_since(debt.submitted_at) >= MAX_ROOT_AGE))
                .then_some((debt.submitted_at, *cell))
        })
        .collect();
    ready.sort_unstable_by(|(a, cell_a), (b, cell_b)| {
        a.cmp(b)
            .then_with(|| cell_a.as_bytes().cmp(cell_b.as_bytes()))
    });
    let mut available = MAX_MATERIALIZERS.saturating_sub(running);
    for (_, cell) in ready {
        if available == 0 {
            break;
        }
        let Some(active) = cells.get_mut(&cell) else {
            continue;
        };
        let Some(debt) = active.root_debt.as_ref() else {
            continue;
        };
        let reservation = debt
            .selected
            .proof
            .materialization_bytes()
            .and_then(|bytes| {
                bytes
                    .checked_add(
                        crate::node::bundle::MaterializedBundlePrefix::maximum_retained_bytes(),
                    )
                    .ok_or(Error::Capacity("checkpoint metadata admission"))
            })
            .and_then(|bytes| {
                pool.resource_ledger()
                    .try_reserve(ResourceCost::zero().with_retained_bytes(bytes))
            });
        let Ok(reservation) = reservation else {
            continue;
        };
        let Some(publisher) = active.publisher.take() else {
            continue;
        };
        let debt = debt.clone();
        active.materializing = true;
        available -= 1;
        let effect_id = active.begin_task(CoordinationEffect::Publication);
        let generation = active.generation;
        let published_sequence = active.published_sequence;
        let pool = pool.clone();
        tasks.spawn(async move {
            let mut reservation = reservation;
            let mut publisher = publisher;
            let started = std::time::Instant::now();
            let commit_sequence = debt.selected.proof.commit_sequence();
            let result = async {
                let deadline = started + FLEET_PUBLICATION_GRACE;
                let mut delay = std::time::Duration::from_millis(100);
                let root = loop {
                    match publisher.materialize_bundle_with_due(&debt.selected.proof, debt.next_due_ms).await {
                        Ok(root) => break root,
                        Err(error) if is_storage_publication_error(&error) && std::time::Instant::now() < deadline => {
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2).min(std::time::Duration::from_secs(2));
                        }
                        Err(error) => return Err(error),
                    }
                };
                // Checkpoint the authenticated locator prefix before admitting
                // more selection; both root and index now cover this exact cut.
                debt.durability.checkpoint_materialized(publisher.authority(), root).await?;
                // Transfer pre-admitted metadata to the worker. No admission
                // can fail after the canonical root and checkpoint are joined.
                let retained = reservation.split_retained(crate::node::bundle::MaterializedBundlePrefix::maximum_retained_bytes())?;
                pool.bind_bundle_materialized(cell, root, retained).await
            }.await;
            publisher.record_publication_timing(crate::fleet::telemetry::PublicationTiming {
                queue_wait: started.saturating_duration_since(debt.submitted_at),
                preparation: std::time::Duration::ZERO,
                authority: started.elapsed(), total: debt.submitted_at.elapsed(),
                succeeded: result.is_ok(), commit_sequence,
                covered_commits: commit_sequence.saturating_sub(published_sequence),
            });
            let fenced = result.is_err();
            if fenced {
                tracing::warn!(cell = ?cell, error = ?result.as_ref().err(), "root materialization failed");
                let _ = pool.fence(cell).await;
            }
            TaskResult::Published {
                cell, generation, effect_id, publisher: Box::new(publisher),
                retained_bytes: 0, node_log_bytes: debt.node_log_bytes,
                covered: 1, covered_node_logs: debt.covered_node_logs,
                next_due_ms: debt.next_due_ms, commit_sequence, result, fenced,
            }
        });
    }
}
