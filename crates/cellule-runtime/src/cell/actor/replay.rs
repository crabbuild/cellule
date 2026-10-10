//! Read-only command lookup under publication pressure, using the original gate.
use super::admission::{fence_admission, send_command_reply};
use super::*;
use tracing::Instrument as _;

pub(super) async fn execute(
    pool: SqlWorkerPool,
    mut command: Box<QueuedCommand>,
    interrupt: Arc<cellule_ltx::DbInterruptHandle>,
    generation: u64,
    effect_id: u64,
) -> TaskResult {
    let started = std::time::Instant::now();
    let queue_wait = command.queued_at.elapsed();
    let deadline = SqlDeadline::new(started + SQL_WALL_DEADLINE);
    let operation = command.operation;
    let cell = command.cell;
    let now_ms = command.now_ms;
    let max_result_bytes = command.max_result_bytes;
    let worker_pool = pool.clone();
    let worker_deadline = deadline.clone();
    let lookup = async move {
        let QueuedOperation::Mutation {
            identity,
            operation_digest,
        } = operation
        else {
            return Err(Error::Control("publication lookup contains an effect"));
        };
        identity.validate(now_ms)?;
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
    };
    let lookup = lookup.instrument(command.trace.clone());
    tokio::pin!(lookup);
    let resolution = match tokio::time::timeout_at(deadline.at().into(), &mut lookup).await {
        Ok(result) => result,
        Err(_) => {
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
            // The original command admission survives cancellation until its
            // dispatched read exits, just as it does for normal command SQL.
            let _ = lookup.as_mut().await;
            if fenced {
                let _ = pool.fence(command.cell).await;
            }
            Err(Error::Deadline)
        }
    };
    let result = match resolution {
        Ok(Resolution::Committed(outcome)) => Ok(CommandTaskResult::Recorded(outcome)),
        Ok(Resolution::Absent) if command.refused_mutation.is_none() => {
            Ok(CommandTaskResult::AwaitPublication)
        }
        Ok(Resolution::Absent | Resolution::Unknown) => Err(command
            .refused_mutation
            .take()
            .unwrap_or(Error::PendingPublication)),
        Ok(Resolution::Expired) => Err(Error::Command("invalid mutation identity lifetime")),
        Err(error) => Err(error),
    };
    command
        .telemetry
        .command_execution(queue_wait, started.elapsed(), result.is_ok());
    let fenced = result.is_err()
        && !deadline.cancelled()
        && !pool
            .state(command.cell)
            .await
            .is_ok_and(WorkerState::is_reusable);
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
