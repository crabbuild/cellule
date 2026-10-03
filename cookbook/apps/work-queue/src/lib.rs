//! Leased job delivery with permanent receiver idempotency and native dead letters.
mod application;
mod model;
mod peer;
mod receiver;
mod worker;
pub use application::{DEAD, DeadLetters, JOBS, Jobs, RECEIVER, Receiver, WorkQueue, compile};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    ApplicationId, CellTarget, Committed, InvocationError, MutationIdentity, PreparedCommand,
    Resolution, TenantId,
    codec::{BoundedEncoder, WireValue},
    partition_for_shard,
    primitives::queue::{QueueSendCommand, QueueSendOutcome, QueueSendRequest},
};
pub use model::{
    Delivery, Inspection, InspectionCursor, InspectionPageRequest, Job, RecordOutcome,
};
pub use receiver::{Inspect, Record, SetEnabled};
pub use worker::{WorkerOptions, WorkerProgress, spawn_workers};

/// Errors retain published invocation evidence and their originating source.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Application assembly or time failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Invalid bounded application message.
    #[error(transparent)]
    Codec(#[from] cellule_runtime::codec::CodecError),
    /// Framework preparation or decoding failure.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
    /// Native dead-letter intent exhausted its attempts; retain the source receipt.
    #[error("dead-letter delivery failed at source receipt {receipt:?}")]
    DeadLetterFailed {
        /// Source transition that durably recorded terminal failure.
        receipt: cellule_runtime::Receipt,
    },
    /// Supervised worker or effect failure.
    #[error("worker operation failed: {0}")]
    Worker(#[source] Box<dyn std::error::Error + Send + Sync>),
}
/// Domain and worker result.
pub type Result<T> = std::result::Result<T, Error>;
/// Derives the declared fixed-shard identity; rejects out-of-range shards.
pub fn target(
    tenant: TenantId,
    application: ApplicationId,
    namespace: cellule_runtime::NamespaceId,
    shard: u32,
) -> cellule_runtime::Result<CellTarget> {
    let shards = if namespace == JOBS {
        2
    } else if namespace == DEAD || namespace == RECEIVER {
        1
    } else {
        return Err(cellule_runtime::Error::Identity(
            "unknown queue application namespace",
        ));
    };
    if shard >= shards {
        return Err(cellule_runtime::Error::Identity(
            "queue application shard out of range",
        ));
    }
    CellTarget::new(tenant, application, namespace, &partition_for_shard(shard))
}
/// Reusable producer capability bound to an authenticated application and tenant.
#[derive(Clone)]
pub struct Producer {
    handle: ApplicationHandle<WorkQueue>,
}
impl Producer {
    /// Accepts an application-selected tenant handle.
    pub fn new(handle: ApplicationHandle<WorkQueue>) -> Self {
        Self { handle }
    }
    /// Freezes the exact native request for retained evidence and unchanged retries.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        job: &Job,
        available_at_ms: i64,
    ) -> std::result::Result<
        PreparedCommand<QueueSendCommand<Jobs>>,
        InvocationError<QueueSendOutcome>,
    > {
        let producer_id = job
            .validate()
            .map_err(|e| InvocationError::NotStarted(e.into()))?;
        let mut encoder =
            BoundedEncoder::new(1024).map_err(|e| InvocationError::NotStarted(e.into()))?;
        job.encode(&mut encoder)
            .map_err(|e| InvocationError::NotStarted(e.into()))?;
        let target = self
            .handle
            .target_for_scope(JOBS, &producer_id)
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<QueueSendCommand<Jobs>>(
                &target,
                identity,
                QueueSendRequest {
                    producer_id,
                    payload: encoder.finish(),
                    available_at_ms,
                },
            )
            .await
    }
    /// Sends an exact prepared job; preserve its identity and scheduling timestamp.
    pub async fn send(
        &self,
        identity: MutationIdentity,
        job: &Job,
        available_at_ms: i64,
    ) -> std::result::Result<Committed<QueueSendOutcome>, InvocationError<QueueSendOutcome>> {
        self.prepare(identity, job, available_at_ms)
            .await?
            .execute()
            .await
    }
    /// Resolves retained producer evidence without dispatching another command.
    pub async fn resolve(
        &self,
        pending: &cellule_runtime::PendingMutation,
    ) -> std::result::Result<Resolution, InvocationError<Vec<u8>>> {
        self.handle.resolve(pending).await
    }
}
