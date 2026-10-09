use crate::*;
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, new_identity};
use cellule_runtime::{
    Error, InvocationError,
    primitives::queue::{QueueClaimRequest, QueueLeaseOutcome, QueueNamespace, QueueState},
};
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[derive(Debug, thiserror::Error)]
#[error("telemetry consumer failed: {0}")]
struct ConsumerError(#[source] BoxError);
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
/// A checkpoint after actual publication; sending observations never blocks native settlement.
#[derive(Clone, Debug, Serialize)]
pub enum ConsumerProgress {
    /// One device decision is durable, while its enclosing cross-Cell batch remains incomplete.
    EventPublished {
        /// Exact permanent producer batch identity.
        batch: BatchId,
        /// Physical Queue identity, reused on redelivery.
        message: MessageId,
        /// Native claim attempt.
        attempt: u32,
        /// Zero-based payload entry position.
        index: usize,
        /// Original source answer and receipt.
        result: Box<EntryResult>,
    },
    /// Complete batch outcomes are durable before independent Queue acknowledgment.
    AuditPublished {
        /// Complete original processing evidence.
        completion: Completion,
        /// Exact audit-local source position.
        commit_sequence: u64,
        /// Native claim attempt.
        attempt: u32,
    },
}
/// Bounded exercised interruption controls for one declared immutable batch.
#[derive(Clone, Default)]
pub struct ConsumerOptions {
    /// Controls apply only to this batch, or to the first claimed batch when absent.
    pub controlled_batch: Option<BatchId>,
    /// Delay after first durable device result and before the batch audit, at most ten seconds.
    pub after_event: Duration,
    /// Delay after complete audit publication and before ack, at most ten seconds.
    pub before_ack: Duration,
    /// Optional bounded observations; losing diagnostics does not change processing.
    pub progress: Option<mpsc::Sender<ConsumerProgress>>,
}
async fn idle(cancel: &CancellationToken) {
    tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(Duration::from_millis(100))=>{}}
}
async fn consume(
    queue: QueueNamespace<Ingress>,
    handle: ApplicationHandle<TelemetryIngest>,
    audit: AuditClient,
    keys: Vec<DeviceKey>,
    options: ConsumerOptions,
    used: Arc<AtomicBool>,
    cancel: CancellationToken,
) -> Result<(), BoxError> {
    while !cancel.is_cancelled() {
        // Accepted native commands are never dropped halfway through publication. Unknown outcomes
        // stop readiness and leave the physical message available for lease-expiry recovery.
        let claimed = queue
            .claim(
                new_identity()?,
                0,
                QueueClaimRequest {
                    limit: 1,
                    lease_ms: 30000,
                },
            )
            .await?;
        if claimed.output.is_empty() {
            idle(&cancel).await;
            continue;
        }
        if !queue
            .validate_claim(0, claimed.output.clone(), Some(claimed.receipt))
            .await?
            .output
        {
            continue;
        }
        for message in claimed.output {
            let batch: Batch = wire::decode(&message.payload, 4096)?;
            batch.validate()?;
            let id = MessageId::from_bytes(message.message_id)?;
            let controlled = options.controlled_batch.is_none_or(|v| v == batch.id)
                && !used.swap(true, Ordering::SeqCst);
            let existing = audit.get(id, None).await?;
            let (completion, audit_sequence) = if let Some(value) = existing.output {
                if value.batch != batch {
                    return Err(Error::Identity(
                        "Queue message differs from permanent telemetry audit",
                    )
                    .into());
                }
                (value, existing.receipt.commit_sequence)
            } else {
                let mut results = Vec::with_capacity(batch.events.len());
                let mut paused_publication = false;
                for (index, event) in batch.events.iter().enumerate() {
                    let result = if keys.contains(&event.device) {
                        let device = DeviceClient::new(handle.clone(), event.device.clone())?;
                        let committed = match device.record(new_identity()?, event.clone()).await {
                            Ok(v) => v,
                            Err(InvocationError::Rejected(v)) => *v,
                            Err(source) => return Err(source.into()),
                        };
                        let result = EntryResult {
                            event: event.clone(),
                            outcome: committed.output,
                            source: Some(committed.receipt.into()),
                        };
                        if let Some(progress) = &options.progress {
                            let _ = progress.try_send(ConsumerProgress::EventPublished {
                                batch: batch.id,
                                message: id,
                                attempt: message.attempt,
                                index,
                                result: Box::new(result.clone()),
                            });
                        }
                        if controlled && !paused_publication {
                            paused_publication = true;
                            tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(options.after_event)=>{}}
                        }
                        result
                    } else {
                        EntryResult {
                            event: event.clone(),
                            outcome: DeviceOutcome {
                                decision: Decision::NotInRoster,
                                version: None,
                            },
                            source: None,
                        }
                    };
                    results.push(result);
                }
                let published = audit
                    .complete(
                        new_identity()?,
                        Completion {
                            message: id,
                            batch: batch.clone(),
                            results,
                        },
                    )
                    .await?;
                let AuditOutcome::Complete(value) = published.output else {
                    return Err(Error::Command(
                        "telemetry audit did not return complete outcomes; Queue remains unacked",
                    )
                    .into());
                };
                (value, published.receipt.commit_sequence)
            };
            if let Some(progress) = &options.progress {
                let _ = progress.try_send(ConsumerProgress::AuditPublished {
                    completion,
                    commit_sequence: audit_sequence,
                    attempt: message.attempt,
                });
            }
            if controlled {
                tokio::select! {()=cancel.cancelled()=>{},()=tokio::time::sleep(options.before_ack)=>{}}
            }
            let settled = match queue
                .ack(new_identity()?, 0, message.message_id, message.token)
                .await
            {
                Ok(v) => v,
                Err(InvocationError::Rejected(v)) if v.output == QueueLeaseOutcome::LeaseLost => *v,
                Err(source) => return Err(source.into()),
            };
            if let QueueLeaseOutcome::Applied { state, .. } = settled.output
                && state != QueueState::Acked
            {
                return Err(Error::Command("telemetry ack did not settle ingress message").into());
            }
        }
    }
    Ok(())
}
/// Opens the Queue, audit, and complete 1..2-device roster before starting two owned consumers.
/// Unknown keys receive an explicit roster exclusion; source absence is never inferred from a page.
pub async fn spawn_consumers(
    node: &LocalNode,
    handle: ApplicationHandle<TelemetryIngest>,
    keys: &[DeviceKey],
    options: ConsumerOptions,
) -> Result<(), BoxError> {
    if keys.is_empty()
        || keys.len() > 2
        || keys
            .iter()
            .enumerate()
            .any(|(index, key)| keys[..index].contains(key))
        || options.after_event > Duration::from_secs(10)
        || options.before_ack > Duration::from_secs(10)
    {
        return Err(Error::Command(
            "telemetry consumers require 1..2 distinct devices and delays at most ten seconds",
        )
        .into());
    }
    let producer = Producer::new(handle.clone())?;
    let audit = AuditClient::new(handle.clone())?;
    node.open_cell(producer.target(), &Ingress).await?;
    node.open_cell(audit.target(), &Audit).await?;
    for key in keys {
        let client = DeviceClient::new(handle.clone(), key.clone())?;
        node.open_cell(client.target(), &Devices).await?;
    }
    let used = Arc::new(AtomicBool::new(false));
    for _ in 0..2 {
        let queue = handle.queue::<Ingress>()?;
        let handle = handle.clone();
        let audit = audit.clone();
        let keys = keys.to_vec();
        let options = options.clone();
        let used = used.clone();
        node.spawn_worker(move |cancel| async move {
            consume(queue, handle, audit, keys, options, used, cancel)
                .await
                .map_err(ConsumerError)
        })?;
    }
    Ok(())
}
