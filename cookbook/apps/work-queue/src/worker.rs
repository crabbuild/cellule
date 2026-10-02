use crate::{
    DEAD, DeadLetters, Delivery, Error, JOBS, Job, Jobs, RECEIVER, Receiver, Record, RecordOutcome,
    Result, WorkQueue, target,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, new_identity};
use cellule_runtime::{
    CellTarget, InvocationError,
    codec::{BoundedDecoder, WireValue},
    primitives::{
        effects::{EffectRunOutcome, EffectSupervisor},
        queue::{QueueClaimRequest, QueueLeaseOutcome, QueueModule, QueueNamespace, QueueState},
    },
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// Publication and receipt validation consume lease time before external work.
// Keep the same delivery margin for consumers and native effect runners.
const DELIVERY_LEASE_MS: u32 = 15_000;

/// Bounded observable checkpoints; delivery completion precedes queue acknowledgement.
#[derive(Clone, Debug)]
pub enum WorkerProgress {
    /// Receiver publication succeeded; safe to redeliver using the same job key.
    ReceiverPublished {
        /// Permanent business identity.
        job: String,
        /// Queue delivery attempt.
        attempt: u32,
        /// Receiver outcome.
        outcome: RecordOutcome,
        /// Whether the action recorded a dead letter.
        dead: bool,
    },
}
/// Explicit bounded worker settings and an optional exercised crash checkpoint.
#[derive(Clone)]
pub struct WorkerOptions {
    /// Delay after published receiver success and before acknowledgement, at most 10 seconds.
    pub before_ack: Duration,
    /// Optional bounded nonblocking observer; a slow observer never blocks settlement.
    pub progress: Option<mpsc::Sender<WorkerProgress>>,
}
impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            before_ack: Duration::ZERO,
            progress: None,
        }
    }
}
fn failure<E: std::error::Error + Send + Sync + 'static>(error: E) -> Error {
    Error::Worker(Box::new(error))
}
async fn idle(cancel: &CancellationToken) {
    tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(100))=>{}}
}
async fn consume<M: QueueModule>(
    queue: QueueNamespace<M>,
    shard: u32,
    handle: ApplicationHandle<WorkQueue>,
    receiver: CellTarget,
    dead: bool,
    options: WorkerOptions,
    cancel: CancellationToken,
) -> Result<()> {
    while !cancel.is_cancelled() {
        // Never cancel an accepted claim or receiver command halfway through
        // publication. Unknown outcomes fail readiness; expiry permits a new
        // lease, and permanent receiver idempotency permits safe redelivery.
        let claimed = queue
            .claim(
                new_identity()?,
                shard,
                QueueClaimRequest {
                    limit: 1,
                    lease_ms: DELIVERY_LEASE_MS,
                },
            )
            .await
            .map_err(failure)?;
        if claimed.output.is_empty() {
            idle(&cancel).await;
            continue;
        }
        if !queue
            .validate_claim(shard, claimed.output.clone(), Some(claimed.receipt))
            .await
            .map_err(failure)?
            .output
        {
            continue;
        }
        for message in claimed.output {
            let mut decoder = BoundedDecoder::new(&message.payload, 1024)?;
            let job = Job::decode(&mut decoder)?;
            decoder.finish()?;
            let result = handle
                .command::<Record>(
                    &receiver,
                    new_identity()?,
                    Delivery {
                        job: job.clone(),
                        dead,
                    },
                )
                .await;
            let outcome = match result {
                Ok(result) => result.output,
                Err(InvocationError::Rejected(result)) => result.output,
                Err(error) => return Err(failure(error)),
            };
            let success = matches!(outcome, RecordOutcome::Recorded | RecordOutcome::Duplicate);
            if success {
                if let Some(progress) = &options.progress {
                    let _ = progress.try_send(WorkerProgress::ReceiverPublished {
                        job: job.id,
                        attempt: message.attempt,
                        outcome,
                        dead,
                    });
                }
                // Delay is bounded below the claim duration. On graceful drain
                // the completed action is settled immediately, before exit.
                tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(options.before_ack)=>{}}
            }
            let settled = if success {
                queue
                    .ack(new_identity()?, shard, message.message_id, message.token)
                    .await
            } else {
                queue
                    .retry(
                        new_identity()?,
                        shard,
                        message.message_id,
                        message.token,
                        100,
                    )
                    .await
            };
            let settled = match settled {
                Ok(result) => result,
                Err(InvocationError::Rejected(result))
                    if result.output == QueueLeaseOutcome::LeaseLost =>
                {
                    *result
                }
                Err(error) => return Err(failure(error)),
            };
            if let QueueLeaseOutcome::Applied { state, .. } = settled.output
                && success
                && state != QueueState::Acked
            {
                return Err(cellule_runtime::Error::Command("ack did not settle job").into());
            }
        }
    }
    Ok(())
}
/// Opens all declared Cells, then supervises two consumers, two effect runners,
/// and a dead-letter inspection consumer in the node's owned task group.
///
/// All tasks stop admission on cancellation and finish accepted publication and
/// settlement before drain. The application owns the local peer trust adapter.
pub async fn spawn_workers(
    node: &LocalNode,
    handle: ApplicationHandle<WorkQueue>,
    tenant: cellule_runtime::TenantId,
    application: cellule_runtime::ApplicationId,
    options: WorkerOptions,
) -> Result<()> {
    if options.before_ack > Duration::from_secs(10) {
        return Err(cellule_runtime::Error::Command("before-ack delay exceeds 10 seconds").into());
    }
    let receiver = target(tenant, application, RECEIVER, 0)?;
    node.open_cell(&receiver, &Receiver).await?;
    let destination = target(tenant, application, DEAD, 0)?;
    let destination_handle = node.open_cell(&destination, &DeadLetters).await?;
    for shard in 0..2 {
        node.open_cell(&target(tenant, application, JOBS, shard)?, &Jobs)
            .await?;
    }
    let peer = crate::peer::client(
        handle.compiled().registry(),
        destination,
        destination_handle,
    );
    for shard in 0..2 {
        let source = handle.effects::<Jobs>(target(tenant, application, JOBS, shard)?)?;
        let supervisor = Arc::new(EffectSupervisor::new(
            source,
            peer.clone(),
            DELIVERY_LEASE_MS,
        )?);
        node.spawn_worker(move |cancel| async move {
            while !cancel.is_cancelled() {
                if let EffectRunOutcome::Failed { receipt } =
                    supervisor.run_once().await.map_err(failure)?
                {
                    return Err(Error::DeadLetterFailed { receipt });
                }
                idle(&cancel).await;
            }
            Ok::<_, Error>(())
        })?;
        let queue = handle.queue::<Jobs>()?;
        let handle = handle.clone();
        let receiver = receiver.clone();
        let options = options.clone();
        node.spawn_worker(move |cancel| {
            consume(queue, shard, handle, receiver, false, options, cancel)
        })?;
    }
    let queue = handle.queue::<DeadLetters>()?;
    node.spawn_worker(move |cancel| {
        consume(
            queue,
            0,
            handle,
            receiver,
            true,
            WorkerOptions {
                before_ack: Duration::ZERO,
                ..options
            },
            cancel,
        )
    })?;
    Ok(())
}
