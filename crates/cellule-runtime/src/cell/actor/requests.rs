//! Request execution for one Cell actor.
//!
//! Every request path runs on the actor's own task: it drives one queued
//! command, query, migration, or resolve to a terminal coordination decision,
//! publishes the outcome, and hands the cell back through `continue_cell`.

use super::admission::{
    fence_active, fence_admission, send_command_reply, send_migration_reply, send_query_reply,
    send_resolve_reply,
};
use super::*;
use tracing::Instrument as _;

pub(super) async fn execute_migration(
    pool: SqlWorkerPool,
    mut publisher: Box<CellPublisher>,
    mut migration: Box<QueuedMigration>,
    interrupt: Arc<cellule_ltx::rusqlite::InterruptHandle>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    let mut preserve_owner = false;
    let mut unpublished_bytes = 0;
    let deadline = SqlDeadline::new(std::time::Instant::now() + SQL_WALL_DEADLINE);
    let operation = pool.migrate(
        migration.cell,
        migration.plan,
        migration.now_ms,
        deadline.clone(),
    );
    tokio::pin!(operation);
    let pending = match tokio::time::timeout_at(deadline.at().into(), &mut operation).await {
        Ok(result) => result,
        Err(_) => {
            deadline.cancel_queued();
            // Migration has already closed the old capability. Even a queued
            // timeout must recover ownership before exposing a usable handle.
            interrupt.interrupt();
            fence_admission(&migration.admission);
            send_migration_reply(&mut migration, Err(Error::Deadline));
            let _ = operation.await;
            let _ = pool.fence(migration.cell).await;
            return TaskResult::Migrated {
                cell: migration.cell,
                generation,
                effect_id,
                publisher,
                migration,
                result: Err(Error::Deadline),
                fenced: true,
                preserve_owner: false,
                unpublished_bytes: 0,
            };
        }
    };
    let result = match pending {
        Ok(pending) => {
            let durability_started = std::time::Instant::now();
            let durability = publisher.submit_migration_durability(&pending).await;
            match durability {
                Err(error) => Err(error),
                Ok(durability) => {
                    let node_logged = durability.is_some();
                    let retained_bytes = pending.retained_bytes();
                    let cell = migration.cell;
                    let early_outcome = MigrationOutcome {
                        code: pending.code(),
                        schema: pending.to_schema(),
                        commit_sequence: pending.commit_sequence(),
                    };
                    let object = async {
                        let prepared = publisher.prepare_migration(&pending).await?;
                        pool.bind_migration_prepared(cell, prepared.clone()).await?;
                        let root = publisher
                            .publish_migration(
                                &prepared,
                                pending.next_due_ms(),
                                pending.code(),
                                pending.to_schema(),
                            )
                            .await?;
                        if let Some(durability) = durability.as_ref() {
                            durability.prove_object().await?;
                        } else {
                            publisher.record_object_proof(durability_started.elapsed());
                        }
                        pool.confirm_migration_published(cell, root).await
                    };
                    tokio::pin!(object);
                    let result = match durability.as_ref() {
                        Some(durability) => {
                            let fleet = durability.prove_fleet();
                            tokio::pin!(fleet);
                            tokio::select! {
                                result = &mut object => result,
                                fleet = &mut fleet => {
                                    if fleet.is_ok() {
                                        let admission = Arc::clone(&migration.successor_admission);
                                        send_migration_reply(
                                            &mut migration,
                                            Ok(MigratedAdmission {
                                                admission,
                                                outcome: early_outcome,
                                            }),
                                        );
                                    }
                                    object.await
                                }
                            }
                        }
                        None => object.await,
                    };
                    preserve_owner = node_logged && result.is_err();
                    if preserve_owner {
                        unpublished_bytes = retained_bytes;
                    }
                    result
                }
            }
        }
        Err(error) => Err(error),
    };
    let fenced = result.is_err();
    if fenced {
        let _ = pool.fence(migration.cell).await;
    }
    TaskResult::Migrated {
        cell: migration.cell,
        generation,
        effect_id,
        publisher,
        migration,
        result,
        fenced,
        preserve_owner,
        unpublished_bytes,
    }
}

pub(super) async fn execute_command(
    pool: SqlWorkerPool,
    durability: CellDurabilitySubmitter,
    mut command: Box<QueuedCommand>,
    interrupt: Arc<cellule_ltx::rusqlite::InterruptHandle>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    let execution_started = std::time::Instant::now();
    if command.group.is_some() {
        return super::group::execute(pool, durability, command, interrupt, generation, effect_id)
            .await;
    }
    let queue_wait = command.queued_at.elapsed();
    tracing::debug!(
        target: "cellule_runtime::action",
        parent: &command.trace,
        event = "cell_execution_started",
        actor_queue_us = queue_wait.as_micros(),
    );
    let deadline = SqlDeadline::new(std::time::Instant::now() + SQL_WALL_DEADLINE);
    let execution = match command.handler.take() {
        Some(handler) => {
            let cell = command.cell;
            let queued_operation = command.operation;
            let now_ms = command.now_ms;
            let max_result_bytes = command.max_result_bytes;
            let worker_pool = pool.clone();
            let worker_deadline = deadline.clone();
            let operation = async move {
                match queued_operation {
                    QueuedOperation::Mutation {
                        identity,
                        operation_digest,
                    } => {
                        worker_pool
                            .execute_until(
                                cell,
                                identity,
                                operation_digest,
                                now_ms,
                                max_result_bytes,
                                worker_deadline,
                                handler,
                            )
                            .await
                    }
                    QueuedOperation::Effect { delivery } => {
                        worker_pool
                            .deliver_effect(
                                cell,
                                delivery,
                                now_ms,
                                max_result_bytes,
                                worker_deadline,
                                handler,
                            )
                            .await
                    }
                }
            };
            let operation = operation.instrument(command.trace.clone());
            tokio::pin!(operation);
            match tokio::time::timeout_at(deadline.at().into(), &mut operation).await {
                Ok(result) => result,
                Err(_) => {
                    command.telemetry.command_execution(
                        queue_wait,
                        execution_started.elapsed(),
                        false,
                    );
                    let fenced = !deadline.cancel_queued();
                    tracing::warn!(cell = ?command.cell, sql_started = fenced, "Cell SQL command deadline expired");
                    if fenced {
                        interrupt.interrupt();
                        fence_admission(&command.admission);
                    }
                    let error = if fenced {
                        command.operation.unknown(Error::Deadline)
                    } else {
                        Error::Deadline
                    };
                    send_command_reply(&mut command, Err(error));
                    let _ = operation.await;
                    if fenced {
                        let _ = pool.fence(command.cell).await;
                    }
                    return TaskResult::Executed {
                        cell: command.cell,
                        generation,
                        effect_id,
                        command,
                        result: Err(Error::Deadline),
                        fenced,
                    };
                }
            }
        }
        None => Err(Error::Fenced),
    };
    command
        .telemetry
        .command_execution(queue_wait, execution_started.elapsed(), execution.is_ok());
    tracing::debug!(
        target: "cellule_runtime::action",
        parent: &command.trace,
        event = "cell_execution_completed",
        worker_round_trip_us = execution_started.elapsed().as_micros(),
        succeeded = execution.is_ok(),
    );
    let (result, must_fence) = match execution {
        Ok(WorkerExecution::Recorded(outcome)) => (Ok(CommandTaskResult::Recorded(outcome)), false),
        Ok(WorkerExecution::Pending(pending)) => {
            let result = reserve_pending_publication(&pool, &pending);
            let result = match result {
                Ok(retained_reservation) => durability
                    .submit(pending.outcome().commit_sequence(), pending.cuts())
                    .await
                    .map(|durability| CommandTaskResult::Pending {
                        pending,
                        durability,
                        retained_reservation,
                    }),
                Err(error) => Err(error),
            };
            (result, true)
        }
        Err(error) => (
            Err(error),
            !deadline.cancelled()
                && !pool
                    .state(command.cell)
                    .await
                    .is_ok_and(WorkerState::is_reusable),
        ),
    };
    let fenced = must_fence && result.is_err();
    if fenced {
        tracing::warn!(cell = ?command.cell, error = ?result.as_ref().err(), "Cell command execution fenced its owner");
        let _ = pool.fence(command.cell).await;
    }
    let result = if fenced {
        result.map_err(|source| command.operation.unknown(source))
    } else {
        result
    };
    TaskResult::Executed {
        cell: command.cell,
        generation,
        effect_id,
        command,
        result,
        fenced,
    }
}

pub(super) fn reserve_pending_publication(
    pool: &SqlWorkerPool,
    pending: &PendingCommit,
) -> crate::Result<ResourceReservation> {
    // The host already owns the LTX file's disk reservation. Retain RAM for
    // shared indexes and live outcome/descriptor copies, not the on-disk body.
    // Physical backlog counters and their per-Cell limits remain unchanged.
    let bytes = usize::try_from(pending.retained_memory_bytes())
        .map_err(|_| Error::Capacity("pending publication bytes"))?;
    pool.resource_ledger()
        .try_reserve(ResourceCost::zero().with_retained_bytes(bytes))
        .map_err(|error| match error {
            Error::Capacity(_) => Error::Capacity("pending publication bytes"),
            error => error,
        })
}

pub(super) async fn prove_command(
    pool: SqlWorkerPool,
    mut command: Box<QueuedCommand>,
    outcome: StoredOutcome,
    commit_sequence: u64,
    durability: Option<PendingDurability>,
    mut object: oneshot::Receiver<crate::Result<()>>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    use crate::node::log::DurabilitySource;

    let proof_started = std::time::Instant::now();
    let proof = match durability {
        Some(durability) => {
            let fleet_or_object = durability.prove();
            tokio::pin!(fleet_or_object);
            tokio::select! {
                object = &mut object => match receive_publication_proof(object) {
                    Ok(()) => Ok(DurabilitySource::Object),
                    // Object publication failure does not invalidate an
                    // independently fsynced follower proof for this cut.
                    Err(_) => fleet_or_object.await,
                },
                result = &mut fleet_or_object => match result {
                    Ok(source) => Ok(source),
                    // Losing the follower path does not invalidate the same
                    // cut's object publication, which remains the fallback.
                    Err(_) => receive_publication_proof(object.await).map(|()| DurabilitySource::Object),
                }
            }
        }
        None => receive_publication_proof(object.await).map(|()| DurabilitySource::Object),
    };
    tracing::debug!(
        target: "cellule_runtime::action",
        parent: &command.trace,
        event = "cell_proof_completed",
        proof_wait_us = proof_started.elapsed().as_micros(),
        commit_sequence,
        succeeded = proof.is_ok(),
    );
    let result = match proof {
        Ok(source) => {
            let confirmation = std::time::Instant::now();
            pool.confirm_durable(command.cell, commit_sequence)
                .await
                .map(|()| {
                    command.response_proof = Some((source, confirmation.elapsed()));
                    outcome
                })
        }
        Err(error) => Err(error),
    };
    let fenced = result.is_err();
    if fenced {
        let _ = pool.fence(command.cell).await;
    }
    let result = if fenced {
        result.map_err(|source| command.operation.unknown(source))
    } else {
        result
    };
    TaskResult::Proven {
        cell: command.cell,
        generation,
        effect_id,
        command,
        result,
        fenced,
    }
}

pub(super) fn receive_publication_proof(
    result: std::result::Result<crate::Result<()>, oneshot::error::RecvError>,
) -> crate::Result<()> {
    result.map_err(|_| Error::RuntimeClosed)?
}

pub(super) fn start_publication(
    cell: CellId,
    active: &mut ActiveCell,
    tasks: &mut JoinSet<TaskResult>,
) {
    let Some(mut publisher) = active.publisher.take() else {
        return;
    };
    if active.publications.is_empty() {
        active.publisher = Some(publisher);
        return;
    }
    let generation = active.generation;
    let effect_id = active.begin_task(CoordinationEffect::Publication);
    let fleet_deadline = std::time::Instant::now() + FLEET_PUBLICATION_GRACE;
    tasks.spawn(async move {
        // Keep coverage in the bounded Cell queue while waiting. The publisher
        // token prevents another root or compaction from overtaking admission.
        let result = publisher.admit_publication().await.map(|replica| {
            Box::new(PublicationAdmission {
                replica,
                fleet_deadline,
            })
        });
        TaskResult::PublicationAdmitted {
            cell,
            generation,
            effect_id,
            publisher: Box::new(publisher),
            result,
        }
    });
}

pub(super) fn start_admitted_publication(
    cell: CellId,
    active: &mut ActiveCell,
    pool: &SqlWorkerPool,
    tasks: &mut JoinSet<TaskResult>,
    mut publisher: Box<CellPublisher>,
    admission: Box<PublicationAdmission>,
    effect_id: u64,
) {
    use crate::fleet::telemetry::PublicationTiming;

    // Coalescing publishes one root for every queued commit. It is only sound
    // once each covered commit reached its follower proof, because that proof is
    // what allowed the commits to queue behind an unpublished one.
    let coalesce = active.publications.len() > 1
        && active
            .publications
            .iter()
            .all(|queued| queued.durability.is_some());
    let coverage: Vec<QueuedPublication> = if coalesce {
        active.publications.drain(..).collect()
    } else {
        match active.publications.pop_front() {
            Some(queued) => vec![queued],
            None => {
                active.finish_task(effect_id, CoordinationEffect::Publication);
                active.publisher = Some(*publisher);
                return;
            }
        }
    };
    let Some(merged) =
        crate::cell::executor::merge_captures(coverage.iter().map(|queued| queued.pending.cuts()))
    else {
        active.finish_task(effect_id, CoordinationEffect::Publication);
        active.publisher = Some(*publisher);
        return;
    };
    let Some(newest) = coverage.last() else {
        active.finish_task(effect_id, CoordinationEffect::Publication);
        active.publisher = Some(*publisher);
        return;
    };
    let covered = coverage.len();
    // One physical native-group capture can represent several logical commands.
    // The serialized publisher advances the exact contiguous Cell sequence.
    let covered_commits = coverage.last().map_or(0, |queued| {
        queued
            .pending
            .outcome()
            .commit_sequence()
            .saturating_sub(active.published_sequence)
    });
    let retained_bytes: u64 = coverage
        .iter()
        .map(|queued| queued.pending.retained_bytes())
        .sum();
    let node_log_bytes: u64 = coverage
        .iter()
        .filter(|queued| queued.durability.is_some())
        .map(|queued| queued.pending.retained_bytes())
        .sum();
    let covered_node_logs = coverage
        .iter()
        .filter(|queued| queued.durability.is_some())
        .count() as u64;
    let node_logged = newest.durability.is_some();
    let root_sequence_lag = i128::from(newest.pending.outcome().commit_sequence())
        - i128::from(active.published_sequence);
    let incarnation = active.incarnation;
    let commit_sequence = newest.pending.outcome().commit_sequence();
    let newest_submitted_at = newest.submitted_at;
    let queue_wait = newest_submitted_at.elapsed();
    tracing::debug!(
        target: "cellule_runtime::action",
        event = "cell_publication_started",
        cell = ?cell,
        incarnation = ?incarnation,
        commit_sequence,
        covered,
        queue_wait_ms = newest_submitted_at.elapsed().as_millis(),
        pending_publications = active.coordination.publication_count(),
        publication_bytes = active.publication_bytes,
        root_sequence_lag = %root_sequence_lag,
        "Cell LTX publication started"
    );
    let generation = active.generation;
    // Moving the publisher out of ActiveCell is the serialization token for
    // root preparation and CAS; no second object publisher can overtake it.
    active.publishing_since = coverage.first().map(|queued| queued.submitted_at);
    let pool = pool.clone();
    let published_next_due_ms = newest.pending.next_due_ms();
    let published_commit_sequence = commit_sequence;
    let mut proofs = Vec::with_capacity(covered);
    let mut durabilities = Vec::with_capacity(covered);
    let mut expected = Vec::with_capacity(covered);
    let mut reservations = Vec::with_capacity(covered);
    let mut pendings = Vec::with_capacity(covered);
    for queued in coverage {
        proofs.push(queued.proof);
        durabilities.push(queued.durability);
        expected.push(queued.pending.outcome().clone());
        reservations.push(queued.retained_reservation);
        pendings.push(queued.pending);
    }
    tasks.spawn(async move {
        let mut admitted = Some(admission.replica);
        let _retained_reservations = reservations;
        let mut publication_proofs = Some(proofs);
        // Admission delay cannot restart or extend the existing retry grace.
        let fleet_deadline = admission.fleet_deadline;
        let mut retry_delay = std::time::Duration::from_millis(100);
        let mut preparation = std::time::Duration::ZERO;
        let mut authority = std::time::Duration::ZERO;
        let result = async {
            let preparation_started = std::time::Instant::now();
            let prepared = loop {
                let attempt = if let Some(replica) = admitted.take() {
                    publisher
                        .prepare_admitted_batch(replica, &merged, published_commit_sequence)
                        .await
                } else if covered == 1 {
                    publisher.prepare(&pendings[0]).await
                } else {
                    publisher
                        .prepare_batch(&merged, published_commit_sequence)
                        .await
                };
                match attempt {
                    Ok(prepared) => break prepared,
                    Err(error) if node_logged && is_storage_publication_error(&error) => {
                        if let Some(durability) = durabilities.iter().flatten().next() {
                            wait_for_fleet_proof(durability, fleet_deadline).await?;
                        }
                        // A lone covered commit learns about the retryable
                        // storage failure while the Cell keeps retrying; a
                        // coalesced range resolves together at the end.
                        if covered == 1
                            && let Some(mut proofs) = publication_proofs.take()
                            && let Some(proof) = proofs.pop()
                        {
                            let _ = proof.send(Err(error));
                        }
                        if std::time::Instant::now() >= fleet_deadline {
                            return Err(Error::Fenced);
                        }
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = retry_delay
                            .saturating_mul(2)
                            .min(std::time::Duration::from_secs(2));
                    }
                    Err(error) => return Err(error),
                }
            };
            preparation = preparation_started.elapsed();
            if covered == 1 {
                pool.bind_prepared(cell, prepared.clone()).await?;
            } else {
                pool.bind_prepared_all(cell, merged, prepared.clone()).await?;
            }
            let authority_started = std::time::Instant::now();
            let root = loop {
                match publisher
                    .publish_prepared(&prepared, published_next_due_ms)
                    .await
                {
                    Ok(root) => break root,
                    Err(error) if node_logged && is_storage_publication_error(&error) => {
                        if let Some(durability) = durabilities.iter().flatten().next() {
                            wait_for_fleet_proof(durability, fleet_deadline).await?;
                        }
                        if covered == 1
                            && let Some(mut proofs) = publication_proofs.take()
                            && let Some(proof) = proofs.pop()
                        {
                            let _ = proof.send(Err(error));
                        }
                        if std::time::Instant::now() >= fleet_deadline {
                            return Err(Error::Fenced);
                        }
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = retry_delay
                            .saturating_mul(2)
                            .min(std::time::Duration::from_secs(2));
                    }
                    Err(error) => return Err(error),
                }
            };
            authority = authority_started.elapsed();
            let logged = PendingDurability::prove_objects(&durabilities).await?;
            if !logged {
                publisher.record_object_proof(newest_submitted_at.elapsed());
            }
            // The published root covers every queued commit, so the range
            // confirmation releases exactly the outcomes it continues.
            let published = if covered == 1 {
                vec![pool.confirm_published(cell, root).await?]
            } else {
                pool.confirm_published_range(cell, root).await?
            };
            if published.len() != expected.len()
                || published
                    .iter()
                    .zip(expected.iter())
                    .any(|(released, queued)| released != queued)
            {
                return Err(Error::Control(
                    "published result does not match queued commit",
                ));
            }
            Ok(())
        }
        .await;
        publisher.record_publication_timing(PublicationTiming {
            queue_wait,
            preparation,
            authority,
            total: newest_submitted_at.elapsed(),
            succeeded: result.is_ok(),
            commit_sequence,
            covered_commits,
        });
        tracing::debug!(
            target: "cellule_runtime::action",
            event = "cell_publication_completed",
            cell = ?cell,
            incarnation = ?incarnation,
            commit_sequence,
            covered,
            publication_lag_ms = newest_submitted_at.elapsed().as_millis(),
            succeeded = result.is_ok(),
            "Cell LTX publication completed"
        );
        let fenced = result.is_err();
        if let Err(error) = &result {
            // The proof waiter receives an unknown outcome. Retain the cause
            // here so an operator can distinguish storage failure from fencing.
            tracing::warn!(cell = ?cell, commit_sequence, error = ?error, "Cell publication fenced its owner");
        }
        // Every covered commit waits on this root, so each proof is answered.
        // Keep the original failure available to every waiter; replacing it
        // with Fenced would hide the storage or local confirmation failure.
        let result = result.map_err(Arc::new);
        if let Some(proofs) = publication_proofs {
            for proof in proofs {
                let proof_result = result.as_ref().copied().map_err(|source| {
                    match source.as_ref() {
                        Error::Fenced => Error::Fenced,
                        _ => Error::Shared(Arc::clone(source)),
                    }
                });
                let _ = proof.send(proof_result);
            }
        }
        let result = result.map_err(|source| match Arc::try_unwrap(source) {
            Ok(error) => error,
            Err(source) => Error::Shared(source),
        });
        if fenced {
            let _ = pool.fence(cell).await;
        }
        TaskResult::Published {
            cell,
            generation,
            effect_id,
            publisher,
            retained_bytes,
            node_log_bytes,
            covered: covered as u64,
            covered_node_logs,
            next_due_ms: published_next_due_ms,
            commit_sequence: published_commit_sequence,
            result,
            fenced,
        }
    });
}

pub(super) fn is_storage_publication_error(error: &Error) -> bool {
    if let Error::Shared(source) = error {
        return is_storage_publication_error(source);
    }
    matches!(
        error,
        Error::Storage(_) | Error::Ltx(cellule_ltx::LtxError::Storage(_))
    )
}

pub(super) async fn wait_for_fleet_proof(
    durability: &PendingDurability,
    deadline: std::time::Instant,
) -> crate::Result<()> {
    tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        durability.prove_fleet(),
    )
    .await
    .map_err(|_| Error::Fenced)?
}

pub(super) async fn execute_query(
    pool: SqlWorkerPool,
    mut query: Box<QueuedQuery>,
    interrupt: Arc<cellule_ltx::rusqlite::InterruptHandle>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    let queue_wait = query.queued_at.elapsed();
    let execution_started = std::time::Instant::now();
    let deadline = SqlDeadline::new(std::time::Instant::now() + SQL_WALL_DEADLINE);
    let result = match query.handler.take() {
        Some(handler) => {
            let operation = pool.query(
                query.cell,
                query.max_result_bytes,
                deadline.clone(),
                handler,
            );
            tokio::pin!(operation);
            match tokio::time::timeout_at(deadline.at().into(), &mut operation).await {
                Ok(result) => result,
                Err(_) => {
                    let fenced = !deadline.cancel_queued();
                    tracing::warn!(cell = ?query.cell, sql_started = fenced, "Cell SQL query deadline expired");
                    query
                        .telemetry
                        .query_execution(queue_wait, execution_started.elapsed(), false);
                    if fenced {
                        interrupt.interrupt();
                        fence_admission(&query.admission);
                    }
                    send_query_reply(&mut query, Err(Error::Deadline));
                    let _ = operation.await;
                    if fenced {
                        let _ = pool.fence(query.cell).await;
                    }
                    return TaskResult::Queried {
                        cell: query.cell,
                        generation,
                        effect_id,
                        query,
                        result: Err(Error::Deadline),
                        fenced,
                    };
                }
            }
        }
        None => Err(Error::Fenced),
    };
    query
        .telemetry
        .query_execution(queue_wait, execution_started.elapsed(), result.is_ok());
    let fenced = result.is_err()
        && !deadline.cancelled()
        && !pool
            .state(query.cell)
            .await
            .is_ok_and(WorkerState::is_reusable);
    if fenced {
        let _ = pool.fence(query.cell).await;
    }
    TaskResult::Queried {
        cell: query.cell,
        generation,
        effect_id,
        query,
        result,
        fenced,
    }
}

pub(super) async fn execute_resolve(
    pool: SqlWorkerPool,
    mut resolve: Box<QueuedResolve>,
    interrupt: Arc<cellule_ltx::rusqlite::InterruptHandle>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    let deadline = SqlDeadline::new(std::time::Instant::now() + SQL_WALL_DEADLINE);
    let cell = resolve.cell;
    let resolve_operation = resolve.operation;
    let now_ms = resolve.now_ms;
    let max_result_bytes = resolve.max_result_bytes;
    let worker_pool = pool.clone();
    let worker_deadline = deadline.clone();
    let operation = async move {
        match resolve_operation {
            ResolveOperation::Mutation {
                identity,
                operation_digest,
            } => {
                worker_pool
                    .resolve(
                        cell,
                        identity,
                        operation_digest,
                        now_ms,
                        max_result_bytes,
                        worker_deadline,
                    )
                    .await
            }
            ResolveOperation::Effect { delivery } => {
                worker_pool
                    .resolve_effect(cell, delivery, now_ms, max_result_bytes, worker_deadline)
                    .await
            }
        }
    };
    tokio::pin!(operation);
    let result = match tokio::time::timeout_at(deadline.at().into(), &mut operation).await {
        Ok(result) => result,
        Err(_) => {
            let fenced = !deadline.cancel_queued();
            tracing::warn!(cell = ?resolve.cell, sql_started = fenced, "Cell SQL resolution deadline expired");
            if fenced {
                interrupt.interrupt();
                fence_admission(&resolve.admission);
            }
            send_resolve_reply(&mut resolve, Ok(Resolution::Unknown));
            let _ = operation.await;
            if fenced {
                let _ = pool.fence(resolve.cell).await;
            }
            return TaskResult::Resolved {
                cell: resolve.cell,
                generation,
                effect_id,
                resolve,
                result: Ok(Resolution::Unknown),
                fenced,
            };
        }
    };
    let timed_out = matches!(result, Err(Error::Deadline));
    let result = if timed_out {
        Ok(Resolution::Unknown)
    } else {
        result
    };
    let fenced = timed_out && !deadline.cancelled()
        || result.is_err()
            && !pool
                .state(resolve.cell)
                .await
                .is_ok_and(WorkerState::is_reusable);
    if fenced {
        let _ = pool.fence(resolve.cell).await;
    }
    TaskResult::Resolved {
        cell: resolve.cell,
        generation,
        effect_id,
        resolve,
        result,
        fenced,
    }
}

pub(super) fn continue_cell(
    cell: CellId,
    pool: &SqlWorkerPool,
    cells: &mut HashMap<CellId, ActiveCell>,
    transitioning: &mut HashSet<CellId>,
    tasks: &mut JoinSet<TaskResult>,
    node_lease: &RuntimeNodeLease,
) {
    if cells
        .get(&cell)
        .is_some_and(|active| active.transfer.is_some())
    {
        let ready = cells.get(&cell).is_some_and(|active| {
            !active.inventory_refreshing
                && active.queue.is_empty()
                && active.coordination.can_deactivate()
        });
        if ready {
            start_transfer_inspection(cell, pool, cells, tasks, node_lease);
            return;
        }
        if let Some(active) = cells.get_mut(&cell)
            && !active.inventory_refreshing
            && !active.queue.is_empty()
        {
            start_next(active, pool, tasks, node_lease);
        }
        return;
    }
    let Some(active) = cells.get_mut(&cell) else {
        return;
    };
    let decision = schedule(active, node_lease.check().is_ok());
    match decision {
        CoordinationDecision::ReadyToDeactivateFenced => {
            let preserve_owner = active.unpublished_node_logs != 0;
            start_fenced_deactivate(cell, pool, cells, transitioning, tasks, preserve_owner);
        }
        CoordinationDecision::ReadyToDeactivate => {
            start_deactivate(cell, pool, cells, transitioning, tasks);
        }
        CoordinationDecision::StartQueuedWork => {
            start_next(active, pool, tasks, node_lease);
        }
        CoordinationDecision::Ignored
        | CoordinationDecision::Admit
        | CoordinationDecision::ResolveUnknown
        | CoordinationDecision::LocalHandle
        | CoordinationDecision::Reject(_)
        | CoordinationDecision::Started
        | CoordinationDecision::EffectCompleted
        | CoordinationDecision::StaleEffect => {}
        CoordinationDecision::Fence => {
            fence_active(active);
        }
    }
}
