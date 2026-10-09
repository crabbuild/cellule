//! Durable recurring reminders with native ticks, signed effects, and a SQL inbox.
mod application;
mod inbox;
mod model;
mod service;
pub use application::{INBOX, Inbox, IntervalScheduler, SCHEDULES, Schedules, compile};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, Error as RuntimeError, InvocationError, MutationIdentity, Observed,
    PendingMutation, PreparedCommand, Receipt, Resolution,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    partition_for_shard,
    primitives::cron::{
        CronCommand, CronMutation, CronMutationOutcome, CronQueryResult, CronSchedule,
    },
};
pub use inbox::{ListDeliveries, RecordReminder};
pub use model::{
    Change, Delivery, DeliveryPage, DeliveryPageRequest, RecordOutcome, Reminder, Schedule,
    ScheduleId, SchedulePage,
};
pub use service::{DeliveryOptions, DeliveryProgress, ServiceError, open, spawn_delivery};

/// Domain inspection errors preserve their native query or codec source.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Native schedule query failure, including receipt and outcome evidence.
    #[error(transparent)]
    Query(#[from] InvocationError<CronQueryResult>),
    /// Invalid domain mapping or scope.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Invalid stored reminder content.
    #[error(transparent)]
    Codec(#[from] cellule_runtime::codec::CodecError),
}
/// Application-scoped typed reminder operations, with no caller-selected delivery target.
#[derive(Clone)]
pub struct SchedulerClient {
    handle: ApplicationHandle<IntervalScheduler>,
}
impl SchedulerClient {
    /// Binds an already-authenticated tenant/application capability.
    pub fn new(handle: ApplicationHandle<IntervalScheduler>) -> Self {
        Self { handle }
    }
    /// Returns the schedule's deterministic native Cron shard target.
    pub fn target(&self, id: ScheduleId) -> cellule_runtime::Result<CellTarget> {
        self.handle.target_for_scope(SCHEDULES, id.as_bytes())
    }
    /// Prepares an exact request; retain the identity, content, and absolute due time.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<PreparedCommand<CronCommand<Schedules>>, InvocationError<CronMutationOutcome>> {
        change
            .validate(identity.issued_at_ms)
            .map_err(|e| InvocationError::NotStarted(e.into()))?;
        let target = self
            .target(change.id())
            .map_err(InvocationError::NotStarted)?;
        let mutation = match change {
            Change::Upsert {
                id,
                reminder,
                interval_ms,
                next_due_ms,
            } => {
                let definition = model::Definition {
                    id: ScheduleId::from_bytes(*identity.request_id.as_bytes())
                        .map_err(|e| InvocationError::NotStarted(e.into()))?,
                    reminder,
                };
                let mut encoder =
                    BoundedEncoder::new(1024).map_err(|e| InvocationError::NotStarted(e.into()))?;
                definition
                    .encode(&mut encoder)
                    .map_err(|e| InvocationError::NotStarted(e.into()))?;
                CronMutation::Upsert {
                    schedule_id: *id.as_bytes(),
                    target_index: 0,
                    target_partition: partition_for_shard(0).to_vec(),
                    payload: encoder.finish(),
                    interval_ms,
                    next_due_ms,
                }
            }
            Change::Pause { id } => CronMutation::Pause {
                schedule_id: *id.as_bytes(),
            },
            Change::Resume { id, next_due_ms } => CronMutation::Resume {
                schedule_id: *id.as_bytes(),
                next_due_ms,
            },
            Change::Delete { id } => CronMutation::Delete {
                schedule_id: *id.as_bytes(),
            },
        };
        self.handle
            .prepare_command::<CronCommand<Schedules>>(&target, identity, mutation)
            .await
    }
    /// Applies a new logical change; unchanged retries return its original publication.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<Committed<CronMutationOutcome>, InvocationError<CronMutationOutcome>> {
        self.prepare(identity, change).await?.execute().await
    }
    /// Resolves retained evidence without dispatching or creating a replacement identity.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        self.handle.resolve(evidence).await
    }
    /// Reads one native schedule at an optional minimum source-Cell receipt.
    pub async fn get(
        &self,
        id: ScheduleId,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Schedule>>, ReadError> {
        let observed = self
            .handle
            .cron::<Schedules>()?
            .get(*id.as_bytes(), minimum)
            .await?;
        let CronQueryResult::Get(schedule) = observed.output else {
            return Err(RuntimeError::Command("unexpected schedule get result").into());
        };
        Ok(Observed {
            receipt: observed.receipt,
            output: schedule.map(map_schedule).transpose()?,
        })
    }
    /// Lists 1–100 schedules on one explicit shard using the native continuation.
    /// Pagination is a series of current reads, not a cross-command snapshot.
    pub async fn list(
        &self,
        shard: u32,
        after: Option<ScheduleId>,
        limit: u32,
        minimum: Option<Receipt>,
    ) -> Result<Observed<SchedulePage>, ReadError> {
        if shard >= 2 || !(1..=100).contains(&limit) {
            return Err(RuntimeError::Command(
                "schedule page requires shard 0..1 and limit 1..100",
            )
            .into());
        }
        let observed = self
            .handle
            .cron::<Schedules>()?
            .list_shard(shard, after.map(|id| *id.as_bytes()), limit, minimum)
            .await?;
        let CronQueryResult::List { schedules, next } = observed.output else {
            return Err(RuntimeError::Command("unexpected schedule list result").into());
        };
        Ok(Observed {
            receipt: observed.receipt,
            output: SchedulePage {
                schedules: schedules
                    .into_iter()
                    .map(map_schedule)
                    .collect::<Result<_, _>>()?,
                next: next.map(ScheduleId::from_bytes).transpose()?,
            },
        })
    }
    /// Reads bounded delivered occurrences at an optional receiver-Cell receipt.
    /// A source receipt cannot be substituted for a destination progress proof.
    pub async fn deliveries(
        &self,
        page: DeliveryPageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<DeliveryPage>, InvocationError<DeliveryPage>> {
        let target = self
            .handle
            .target_for_scope(INBOX, &[0])
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .query::<ListDeliveries>(&target, minimum, page)
            .await
    }
}
fn map_schedule(value: CronSchedule) -> Result<Schedule, ReadError> {
    if value.target_index != 0
        || value.target_partition != partition_for_shard(0)
        || value.generation == 0
    {
        return Err(RuntimeError::Command("stored reminder target or generation differs").into());
    }
    let mut decoder = BoundedDecoder::new(&value.payload, 1024)?;
    let definition = model::Definition::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(Schedule {
        id: ScheduleId::from_bytes(value.schedule_id)?,
        definition: definition.id,
        reminder: definition.reminder,
        interval_ms: value.interval_ms,
        next_due_ms: value.next_due_ms,
        occurrence: value.occurrence,
        enabled: value.enabled,
        generation: value.generation,
    })
}
