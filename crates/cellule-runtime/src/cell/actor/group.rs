//! One bounded native worker job, released through its exact range proof.

use super::admission::{fence_admission, send_command_reply};
use super::*;
use tracing::Instrument as _;

pub(super) async fn execute(
    pool: SqlWorkerPool,
    durability: CellDurabilitySubmitter,
    mut command: Box<QueuedCommand>,
    interrupt: Arc<cellule_ltx::rusqlite::InterruptHandle>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    let started = std::time::Instant::now();
    let mut waits = vec![command.queued_at.elapsed()];
    if let Some(group) = &command.group {
        waits.extend(
            group
                .members
                .iter()
                .map(|member| member.queued_at.elapsed()),
        );
    }
    let native = take_native(&mut command).and_then(|first| {
        let group = command.group.as_mut().ok_or(Error::Fenced)?;
        let mut native = vec![first];
        for member in &mut group.members {
            native.push(take_native(member)?);
        }
        Ok(native)
    });
    let deadline = SqlDeadline::new(started + SQL_WALL_DEADLINE);
    let execution = match native {
        Ok(native) => {
            let operation = pool.execute_group(command.cell, native, deadline.clone());
            let operation = operation.instrument(command.trace.clone());
            tokio::pin!(operation);
            match tokio::time::timeout_at(deadline.at().into(), &mut operation).await {
                Ok(result) => result,
                Err(_) => {
                    record_execution(&command, &waits, started.elapsed(), None);
                    let fenced = !deadline.cancel_queued();
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
                    // Retain every member's admission until dispatched SQL exits.
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
        Err(error) => Err(error),
    };
    record_execution(&command, &waits, started.elapsed(), execution.as_ref().ok());
    let (result, must_fence) = match execution {
        Ok(NativeGroupExecution {
            outcomes,
            pending,
            base_sequence,
        }) => {
            let highest = outcomes
                .iter()
                .filter_map(|result| result.as_ref().ok())
                .filter(|outcome| outcome.commit_sequence() > base_sequence)
                .max_by_key(|outcome| outcome.commit_sequence());
            let valid = command
                .group
                .as_ref()
                .is_some_and(|group| outcomes.len() == group.members.len() + 1)
                && match (&pending, highest) {
                    (Some(pending), Some(outcome)) => pending.outcome() == outcome,
                    (None, None) => true,
                    _ => false,
                };
            if !valid {
                (
                    Err(Error::Control(
                        "native group capture does not cover its outcomes",
                    )),
                    true,
                )
            } else {
                if let Some(group) = &mut command.group {
                    group.execution = Some(GroupOutcomes {
                        outcomes,
                        base_sequence,
                    });
                }
                match pending {
                    Some(pending) => {
                        let result = match reserve_pending_publication(&pool, &pending) {
                            Ok(retained_reservation) => {
                                let first = base_sequence.checked_add(1).ok_or(Error::Fenced);
                                match first {
                                    Ok(first) => durability
                                        .submit_range(
                                            first,
                                            pending.outcome().commit_sequence(),
                                            pending.cuts(),
                                        )
                                        .await
                                        .map(|durability| CommandTaskResult::Pending {
                                            pending,
                                            durability,
                                            retained_reservation,
                                        }),
                                    Err(error) => Err(error),
                                }
                            }
                            Err(error) => Err(error),
                        };
                        // One verified physical cut includes every result in
                        // this logical range, including durable rejections.
                        (result, true)
                    }
                    None => (Ok(CommandTaskResult::GroupRecorded), false),
                }
            }
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
        let _ = pool.fence(command.cell).await;
    }
    let result = if fenced {
        result.map_err(|error| command.operation.unknown(error))
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

fn take_native(command: &mut QueuedCommand) -> crate::Result<NativeCommand<Handler>> {
    let QueuedOperation::Mutation {
        identity,
        operation_digest,
    } = command.operation
    else {
        return Err(Error::Control("native group contains an effect"));
    };
    Ok(NativeCommand {
        identity,
        operation_digest,
        now_ms: command.now_ms,
        max_result_bytes: command.max_result_bytes,
        handler: command.handler.take().ok_or(Error::Fenced)?,
    })
}

fn record_execution(
    command: &QueuedCommand,
    waits: &[std::time::Duration],
    elapsed: std::time::Duration,
    execution: Option<&NativeGroupExecution>,
) {
    let members =
        std::iter::once(command).chain(command.group.iter().flat_map(|group| group.members.iter()));
    for (index, (member, wait)) in members.zip(waits).enumerate() {
        let succeeded = execution
            .and_then(|execution| execution.outcomes.get(index))
            .is_some_and(crate::Result::is_ok);
        member
            .telemetry
            .command_execution(*wait, elapsed, succeeded);
    }
}

pub(super) fn reply(command: &mut QueuedCommand, result: crate::Result<Option<StoredOutcome>>) {
    let Some(group) = command.group.take() else {
        return;
    };
    let proof = command.response_proof.take();
    let execution = group.execution;
    let result = match (result, &execution) {
        (Ok(representative), Some(execution)) => {
            let highest = execution
                .outcomes
                .iter()
                .filter_map(|result| result.as_ref().ok())
                .filter(|outcome| outcome.commit_sequence() > execution.base_sequence)
                .max_by_key(|outcome| outcome.commit_sequence());
            let valid = execution.outcomes.len() == group.members.len() + 1
                && match highest {
                    Some(highest) => {
                        representative.as_ref() == Some(highest)
                            && matches!(
                                proof,
                                Some((
                                    crate::node::log::DurabilitySource::Object
                                        | crate::node::log::DurabilitySource::Fleet
                                        | crate::node::log::DurabilitySource::Bundle,
                                    _
                                ))
                            )
                    }
                    None => representative.is_none(),
                };
            if valid {
                Ok(())
            } else {
                Err(command.operation.unknown(Error::Fenced))
            }
        }
        (Ok(_), None) => Err(command.operation.unknown(Error::Fenced)),
        (Err(error), _) => Err(error),
    };
    let failure = result.err().map(|error| match error {
        Error::OutcomeUnknown { source, .. } => (Arc::new(*source), true),
        error => (Arc::new(error), false),
    });
    let (mut outcomes, base_sequence) = match execution {
        Some(execution) if execution.outcomes.len() == group.members.len() + 1 => (
            Some(execution.outcomes.into_iter()),
            execution.base_sequence,
        ),
        _ => (None, 0),
    };
    let mut tails = group.members;
    let members = std::iter::once(&mut *command).chain(tails.iter_mut());
    // Members retain their own admissions and reply channels through this gate.
    for member in members {
        let result = match outcomes.as_mut().and_then(Iterator::next) {
            Some(Err(error)) => Err(error),
            Some(Ok(outcome)) if outcome.commit_sequence() <= base_sequence => Ok(outcome),
            Some(Ok(outcome)) if failure.is_none() => {
                member.response_proof = proof;
                Ok(outcome)
            }
            Some(Ok(_)) => Err(member.operation.unknown(shared_failure(&failure))),
            None => match &failure {
                Some((error, true)) => {
                    Err(member.operation.unknown(Error::Shared(Arc::clone(error))))
                }
                _ => Err(shared_failure(&failure)),
            },
        };
        send_command_reply(member, result);
    }
    // A deadline may send replies before a dispatched worker exits. Keep the
    // tail work reservations owned by the task until that exit is observed.
    command.group = Some(CommandGroup {
        members: tails,
        execution: None,
    });
}

fn shared_failure(failure: &Option<(Arc<Error>, bool)>) -> Error {
    let Some((error, _)) = failure else {
        return Error::Fenced;
    };
    match error.as_ref() {
        Error::Deadline => Error::Deadline,
        Error::Fenced => Error::Fenced,
        Error::Capacity(name) => Error::Capacity(name),
        Error::RuntimeClosed => Error::RuntimeClosed,
        Error::PendingPublication => Error::PendingPublication,
        Error::CellNotActive => Error::CellNotActive,
        _ => Error::Shared(Arc::clone(error)),
    }
}
