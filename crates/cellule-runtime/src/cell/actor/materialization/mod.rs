//! Exact shared selection and fair, admitted Cell-root materialization.

use super::*;

mod readiness;

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
) {
    if active.selecting {
        return;
    }
    let ready = active
        .publications
        .iter()
        .take_while(|queued| {
            queued
                .durability
                .as_ref()
                .is_some_and(PendingDurability::selection_ready)
        })
        .count();
    if ready == 0 {
        readiness::wait(cell, active, tasks);
        return;
    }
    let binding = active
        .publisher
        .as_ref()
        .and_then(|publisher| publisher.control().value().bundle_binding)
        .or_else(|| {
            active
                .materializing
                .as_ref()
                .map(|root| root.debt.selected.proof.binding())
        });
    let Some(binding) = binding else {
        return;
    };
    let materializing = active
        .materializing
        .as_ref()
        .map(|root| (Arc::clone(&root.debt.selected), root.joined.clone()));
    active.selecting = true;
    // Retire only a selected oldest prefix. The bounded unselected suffix
    // stays in the original queue, without excluding root preparation.
    let coverage: Vec<_> = active.publications.drain(..ready).collect();
    let covered = coverage.len() as u64;
    let retained_bytes = coverage
        .iter()
        .map(|queued| queued.pending.retained_bytes())
        .sum();
    let generation = active.generation;
    let effect_id = active.begin_task(CoordinationEffect::Publication);
    active.publishing_since = coverage.first().map(|queued| queued.submitted_at);
    // Cleanup uses immutable verified coverage, not the mutable publisher.
    // Keep one original FIFO cleanup task per Cell while root I/O continues.
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
            if binding != selected.proof.binding()
                || selected.proof.commit_sequence() != newest.pending.outcome().commit_sequence()
                || selected.proof.position() != newest.pending.cuts().position
            {
                return Err(Error::Fenced);
            }
            if let Some((original, joined)) = materializing
                && selected.proof.base()? != original.proof.base()?
            {
                // A proof rebased by the original checkpoint needs its native
                // prefix witness. Old-base proofs can retire throughout root
                // I/O; only the new-base handoff waits for that exact bind.
                joined.cancelled().await;
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
            covered,
            retained_bytes,
            result,
        }
    });
}

// Foreground events inspect only their affected Cell. The fleet scan uses this
// same predicate to retain age, forced-drain and checkpoint-bound semantics.
pub(super) fn ready_since(
    active: &ActiveCell,
    now: std::time::Instant,
) -> Option<std::time::Instant> {
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
        && active.materializing.is_none()
        && !active.selecting
        && (forced
            || blocks_commands(active)
            || debt
                .selected
                .proof
                .commit_sequence()
                .saturating_sub(active.published_sequence)
                >= CHECKPOINT_COMMANDS
            || now.saturating_duration_since(debt.submitted_at) >= MAX_ROOT_AGE))
        .then_some(debt.submitted_at)
}

pub(super) fn dispatch(
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    tasks: &mut JoinSet<TaskResult>,
) {
    let running = cells
        .values()
        .filter(|active| active.materializing.is_some())
        .count();
    let mut available = MAX_MATERIALIZERS.saturating_sub(running);
    // A completion revisits dispatch after releasing its slot. Walking and
    // sorting the fleet while every slot is occupied cannot start any work.
    if available == 0 {
        return;
    }
    let now = std::time::Instant::now();
    let mut ready: Vec<_> = cells
        .iter()
        .filter_map(|(cell, active)| ready_since(active, now).map(|since| (since, *cell)))
        .collect();
    ready.sort_unstable_by(|(a, cell_a), (b, cell_b)| {
        a.cmp(b)
            .then_with(|| cell_a.as_bytes().cmp(cell_b.as_bytes()))
    });
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
        let joined = tokio_util::sync::CancellationToken::new();
        active.root_debt = None;
        active.materializing = Some(MaterializingRoot {
            debt: debt.clone(),
            joined: joined.clone(),
        });
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
                // Root I/O/CAS has joined and its overlay/preparation buffers
                // have exited. Keep only pre-admitted prefix metadata across
                // the queued checkpoint; completed working credit must not
                // block replication or materialization for independent Cells.
                let retained = reservation.split_retained(crate::node::bundle::MaterializedBundlePrefix::maximum_retained_bytes())?;
                drop(reservation);
                // Checkpoint this exact cut. Later selected captures can retire
                // independently; a rebased proof waits for the native witness.
                debt.durability.checkpoint_materialized(publisher.authority(), root).await?;
                // Transfer pre-admitted metadata to the worker. No admission
                // can fail after the canonical root and checkpoint are joined.
                pool.bind_bundle_materialized(cell, root, Arc::clone(&debt.selected), retained).await
            }.await;
            joined.cancel();
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
