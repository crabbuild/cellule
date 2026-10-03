use crate::*;
use cellule_runtime::primitives::effects::EffectState;
use serde::Serialize;
/// Native source-ledger status; missing or failed evidence is never treated as summary completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionState {
    /// Latest transactional intent is ready or leased.
    Pending,
    /// The receiver accepted the summary, including harmless stale-state delivery.
    Delivered,
    /// Receiver rejection, exhausted attempts, or expiry requires investigation.
    Failed,
    /// Source ledger evidence is unavailable; this proves no receiver absence.
    Unavailable,
}
/// Coherent source state and its latest asynchronous projection evidence.
#[derive(Clone, Debug, Serialize)]
pub struct ProjectionProgress {
    /// Latest source revision and transactional intent.
    pub version: Version,
    /// Native ledger classification.
    pub state: ProjectionState,
    /// Retained native attempts, when evidence exists.
    pub attempts: Option<u32>,
}
/// Progress errors retain their query or ledger source.
#[derive(Debug, thiserror::Error)]
pub enum ProgressError {
    /// Source query failure.
    #[error(transparent)]
    Source(#[from] InvocationError<Option<DeviceState>>),
    /// Native ledger query failure.
    #[error(transparent)]
    Ledger(#[from] InvocationError<Option<cellule_runtime::primitives::effects::EffectStatus>>),
    /// Registry, codec, or receiver evidence contract failure.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Three observations raced with edits; retry this read rather than infer completion.
    #[error("device changed during bounded projection observation")]
    Changed,
}

use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution,
    primitives::queue::{QueueSendCommand, QueueSendOutcome, QueueSendRequest},
};
/// Authorized device capability; the embedding application authenticates before choosing its tenant/key.
#[derive(Clone)]
pub struct DeviceClient {
    handle: ApplicationHandle<TelemetryIngest>,
    key: DeviceKey,
    target: CellTarget,
}
impl DeviceClient {
    /// Binds one canonical entity key to an already-authorized application handle.
    pub fn new(
        handle: ApplicationHandle<TelemetryIngest>,
        key: DeviceKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(DEVICES, key.as_bytes())?;
        Ok(Self {
            handle,
            key,
            target,
        })
    }
    /// Stable source target for explicit enrollment and ownership.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Freezes immutable registration evidence before dispatch.
    pub async fn prepare_registration(
        &self,
        identity: MutationIdentity,
        input: Registration,
    ) -> Result<PreparedCommand<RegisterDevice>, InvocationError<DeviceOutcome>> {
        input
            .window
            .validate()
            .map_err(InvocationError::NotStarted)?;
        self.check(&input.device)?;
        self.handle
            .prepare_command::<RegisterDevice>(&self.target, identity, input)
            .await
    }
    /// Registers an immutable window and publishes its initial native summary intent.
    pub async fn register(
        &self,
        identity: MutationIdentity,
        window: Window,
    ) -> Result<Committed<DeviceOutcome>, InvocationError<DeviceOutcome>> {
        self.prepare_registration(
            identity,
            Registration {
                device: self.key.clone(),
                window,
            },
        )
        .await?
        .execute()
        .await
    }
    fn check(&self, key: &DeviceKey) -> Result<(), InvocationError<DeviceOutcome>> {
        if *key != self.key {
            return Err(InvocationError::NotStarted(Error::Identity(
                "telemetry event targets a different device",
            )));
        }
        Ok(())
    }
    /// Freezes exact source event bytes and native command identity before dispatch.
    pub async fn prepare_event(
        &self,
        identity: MutationIdentity,
        event: Event,
    ) -> Result<PreparedCommand<RecordEvent>, InvocationError<DeviceOutcome>> {
        event.validate().map_err(InvocationError::NotStarted)?;
        self.check(&event.device)?;
        self.handle
            .prepare_command::<RecordEvent>(&self.target, identity, event)
            .await
    }
    /// Applies or rejects one permanent sequence binding; no fresh identity bypasses that binding.
    pub async fn record(
        &self,
        identity: MutationIdentity,
        event: Event,
    ) -> Result<Committed<DeviceOutcome>, InvocationError<DeviceOutcome>> {
        self.prepare_event(identity, event).await?.execute().await
    }
    /// Reads complete history at or beyond a receipt from this exact source Cell.
    pub async fn get(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<DeviceState>>, InvocationError<Option<DeviceState>>> {
        self.handle
            .query::<GetDevice>(&self.target, minimum, self.key.clone())
            .await
    }
    /// Resolves original source evidence without refreshing its input or identity.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign telemetry device evidence",
            )));
        }
        self.handle.resolve(pending).await
    }
}
impl DeviceClient {
    /// Reads one historical native intent in this device Cell without exposing its lease token.
    /// Missing evidence proves no summary absence; a source receipt remains source-local.
    pub async fn effect_status(
        &self,
        effect_id: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<
        Observed<Option<cellule_runtime::primitives::effects::EffectStatus>>,
        InvocationError<Option<cellule_runtime::primitives::effects::EffectStatus>>,
    > {
        if effect_id == [0; 32] {
            return Err(InvocationError::NotStarted(Error::Identity(
                "zero telemetry effect identity",
            )));
        }
        self.handle
            .effects::<Devices>(self.target.clone())
            .map_err(InvocationError::NotStarted)?
            .status(effect_id, minimum)
            .await
    }
    /// Observes state and its native ledger coherently, retrying at most three concurrent edits.
    pub async fn progress(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ProjectionProgress>>, ProgressError> {
        for _ in 0..3 {
            let read = self.get(minimum).await?;
            let Some(state) = read.output else {
                return Ok(Observed {
                    output: None,
                    receipt: read.receipt,
                });
            };
            let version = Version {
                snapshot: state.snapshot()?,
                effect_id: state.effect_id,
            };
            let ledger = self
                .handle
                .effects::<Devices>(self.target.clone())?
                .status(state.effect_id, Some(read.receipt))
                .await?;
            let verified = self.get(Some(ledger.receipt)).await?;
            if verified.output.as_ref() != Some(&state) {
                continue;
            }
            let (state, attempts) = match ledger.output {
                None => (ProjectionState::Unavailable, None),
                Some(status) => {
                    let state = match status.state {
                        EffectState::Ready | EffectState::Leased => ProjectionState::Pending,
                        EffectState::Failed => ProjectionState::Failed,
                        EffectState::Delivered => match wire::decode::<ProjectionOutcome>(
                            status.result.as_deref().ok_or(Error::Command(
                                "telemetry settled projection has no receiver result",
                            ))?,
                            16,
                        )? {
                            ProjectionOutcome::Applied
                            | ProjectionOutcome::Duplicate
                            | ProjectionOutcome::Stale => ProjectionState::Delivered,
                            ProjectionOutcome::Conflict | ProjectionOutcome::Capacity => {
                                ProjectionState::Failed
                            }
                        },
                    };
                    (state, Some(status.attempt))
                }
            };
            return Ok(Observed {
                output: Some(ProjectionProgress {
                    version,
                    state,
                    attempts,
                }),
                receipt: ledger.receipt,
            });
        }
        Err(ProgressError::Changed)
    }
}
/// Native producer bound to the embedding application's authorized tenant.
#[derive(Clone)]
pub struct Producer {
    handle: ApplicationHandle<TelemetryIngest>,
    target: CellTarget,
}
impl Producer {
    /// Selects the declared single ingress shard.
    pub fn new(handle: ApplicationHandle<TelemetryIngest>) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(INGRESS, b"ingress")?;
        Ok(Self { handle, target })
    }
    /// Stable Queue target for explicit enrollment.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Freezes exact payload and admission time; producer deduplication has native bounded retention.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        batch: &Batch,
        available_at_ms: i64,
    ) -> Result<PreparedCommand<QueueSendCommand<Ingress>>, InvocationError<QueueSendOutcome>> {
        batch.validate().map_err(InvocationError::NotStarted)?;
        let payload = wire::encode(batch, 4096).map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<QueueSendCommand<Ingress>>(
                &self.target,
                identity,
                QueueSendRequest {
                    producer_id: batch.id.bytes(),
                    payload,
                    available_at_ms,
                },
            )
            .await
    }
    /// Publishes one immutable batch; a Queue receipt alone does not prove device processing.
    pub async fn send(
        &self,
        identity: MutationIdentity,
        batch: &Batch,
        available_at_ms: i64,
    ) -> Result<Committed<QueueSendOutcome>, InvocationError<QueueSendOutcome>> {
        self.prepare(identity, batch, available_at_ms)
            .await?
            .execute()
            .await
    }
    /// Resolves only the original Queue-scoped evidence.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign telemetry producer evidence",
            )));
        }
        self.handle.resolve(pending).await
    }
}
/// Independently committed permanent batch processing audit.
#[derive(Clone)]
pub struct AuditClient {
    handle: ApplicationHandle<TelemetryIngest>,
    target: CellTarget,
}
impl AuditClient {
    /// Selects the single audit Cell inside an authorized tenant scope.
    pub fn new(handle: ApplicationHandle<TelemetryIngest>) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(AUDITS, b"audit")?;
        Ok(Self { handle, target })
    }
    /// Stable audit target for explicit enrollment.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Freezes complete outcomes before publishing their audit.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        completion: Completion,
    ) -> Result<PreparedCommand<CompleteBatch>, InvocationError<AuditOutcome>> {
        completion.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<CompleteBatch>(&self.target, identity, completion)
            .await
    }
    /// Commits complete original outcomes before the consumer acknowledges its Queue lease.
    pub async fn complete(
        &self,
        identity: MutationIdentity,
        completion: Completion,
    ) -> Result<Committed<AuditOutcome>, InvocationError<AuditOutcome>> {
        self.prepare(identity, completion).await?.execute().await
    }
    /// Observes one physical message's permanent completion at an audit-local receipt.
    pub async fn get(
        &self,
        message: MessageId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Completion>>, InvocationError<Option<Completion>>> {
        self.handle
            .query::<GetBatch>(&self.target, minimum, message)
            .await
    }
    /// Resolves retained audit evidence at its exact target.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign telemetry audit evidence",
            )));
        }
        self.handle.resolve(pending).await
    }
}
/// Independently committed summary shard capability; pages carry shard-local receipts.
#[derive(Clone)]
pub struct SummaryClient {
    handle: ApplicationHandle<TelemetryIngest>,
    target: CellTarget,
}
impl SummaryClient {
    /// Selects one of the two declared fixed summary shards.
    pub fn new(
        handle: ApplicationHandle<TelemetryIngest>,
        shard: u32,
    ) -> cellule_runtime::Result<Self> {
        if shard >= 2 {
            return Err(Error::Identity("telemetry summary shard out of range"));
        }
        let scope = handle.target_for_scope(AUDITS, b"audit")?;
        let target = CellTarget::new(
            scope.tenant(),
            scope.application(),
            SUMMARIES,
            &cellule_runtime::partition_for_shard(shard),
        )?;
        Ok(Self { handle, target })
    }
    /// Selects the summary shard using the canonical device key.
    pub fn for_device(
        handle: ApplicationHandle<TelemetryIngest>,
        key: &DeviceKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(SUMMARIES, key.as_bytes())?;
        Ok(Self { handle, target })
    }
    /// Stable independent receiver target for explicit enrollment.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Reads a device projection; source and receiver receipts remain distinct.
    pub async fn get(
        &self,
        key: DeviceKey,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<DeviceSnapshot>>, InvocationError<Option<DeviceSnapshot>>> {
        let expected = self
            .handle
            .target_for_scope(SUMMARIES, key.as_bytes())
            .map_err(InvocationError::NotStarted)?;
        if expected != self.target {
            return Err(InvocationError::NotStarted(Error::Identity(
                "device belongs to a different telemetry summary shard",
            )));
        }
        self.handle
            .query::<GetSummary>(&self.target, minimum, key)
            .await
    }
    /// Queries bounded grouped minute totals; repeated pages do not establish a cross-shard snapshot.
    pub async fn list(
        &self,
        page: BucketPageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<BucketPage>, InvocationError<BucketPage>> {
        self.handle
            .query::<ListBuckets>(&self.target, minimum, page)
            .await
    }
}
