//! Scheduled external observations, atomic incident transitions, and signed notification delivery.
mod activity;
mod alerts;
mod application;
mod checks;
mod definition;
mod model;
mod schedules;
mod service;
mod sql;
mod wire;
pub use activity::probe;
pub use alerts::RecordAlert;
pub use application::{
    ALERTS, Alerts, CHECKS, Checks, MonitorApplication, PROBES, Probes, SCHEDULES, Schedules,
    compile,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, InvocationError, MutationIdentity, Observed, PendingMutation, PreparedCommand,
    Receipt, Resolution,
    primitives::{cron::CronQueryResult, workflow::WorkflowStatus},
};
pub use checks::RecordCheck;
pub use definition::StartProbe;
pub use model::{
    AlertOutcome, AlertPage, Change, Check, Definition, Edge, EdgeKind, Health, Id, Inspection,
    MAX_CHECKS, MAX_DEFINITIONS, MAX_MONITORS, MonitorState, PROBE_LIFETIME_MS, PageRequest, Probe,
    ProbeState, RecordOutcome, Schedule, ScheduleOutcome, StartOutcome, StoredCheck, Ticket,
    validate_endpoint,
};
pub use schedules::ChangeSchedule;
pub use service::{DeliveryOptions, Progress, ServiceError, open, spawn_workers};
/// Retained native invocation, provider, HTTP, codec, and task source errors.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Typed capability for the four fixed transaction domains of one authenticated installation.
#[derive(Clone)]
pub struct MonitorClient {
    handle: ApplicationHandle<MonitorApplication>,
}
impl MonitorClient {
    /// Binds the embedding's authenticated tenant/application capability.
    pub fn new(handle: ApplicationHandle<MonitorApplication>) -> Self {
        Self { handle }
    }
    /// Resolves one declared fixed-shard namespace for opening and receipt comparisons.
    pub fn target(
        &self,
        namespace: cellule_runtime::NamespaceId,
    ) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(namespace, b"fixed-monitor-shard")
    }
    /// Prepares exact absolute scheduling input; persist its identity before dispatch.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<PreparedCommand<ChangeSchedule>, InvocationError<ScheduleOutcome>> {
        change
            .validate(identity.issued_at_ms)
            .map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<ChangeSchedule>(
                &self
                    .target(SCHEDULES)
                    .map_err(InvocationError::NotStarted)?,
                identity,
                change,
            )
            .await
    }
    /// Resolves only original scheduling evidence, without dispatch.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if evidence.target()
            != &self
                .target(SCHEDULES)
                .map_err(InvocationError::NotStarted)?
        {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign monitor scheduling evidence"),
            ));
        }
        self.handle.resolve(evidence).await
    }
    /// Reads native source scheduling state at its own optional minimum receipt.
    pub async fn schedule(
        &self,
        monitor: Id,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Schedule>>, BoxError> {
        let observed = self
            .handle
            .cron::<Schedules>()?
            .get(monitor.bytes(), minimum)
            .await?;
        let CronQueryResult::Get(value) = observed.output else {
            return Err("unexpected monitor schedule result".into());
        };
        let output = value
            .map(|value| -> Result<Schedule, BoxError> {
                if value.target_index != 0
                    || value.target_partition != cellule_runtime::partition_for_shard(0)
                    || value.generation == 0
                {
                    return Err("stored probe schedule target differs".into());
                }
                Ok(Schedule {
                    monitor: Id::from_bytes(value.schedule_id)?,
                    definition: schedules::definition(&value.payload)?,
                    generation: value.generation,
                    occurrence: value.occurrence,
                    next_due_ms: value.next_due_ms,
                    interval_ms: value.interval_ms,
                    enabled: value.enabled,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output,
        })
    }
    /// Reads coherent incident state and bounded check history from the SQL projection.
    pub async fn inspect(
        &self,
        page: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Inspection>, BoxError> {
        page.validate()?;
        Ok(self
            .handle
            .query::<checks::Inspect>(&self.target(CHECKS)?, minimum, page)
            .await?)
    }
    /// Reads one permanent observation independently of native Workflow visibility.
    pub async fn check(
        &self,
        ticket: Ticket,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<StoredCheck>>, BoxError> {
        ticket.validate()?;
        Ok(self
            .handle
            .query::<checks::ReadCheck>(&self.target(CHECKS)?, minimum, ticket)
            .await?)
    }
    /// Reads bounded notification history; a check receipt is not a notification receipt.
    pub async fn alerts(
        &self,
        page: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<AlertPage>, BoxError> {
        page.validate()?;
        Ok(self
            .handle
            .query::<alerts::ListAlerts>(&self.target(ALERTS)?, minimum, page)
            .await?)
    }
    /// Reads a permanently bound native probe run and retained external observation.
    pub async fn workflow(
        &self,
        ticket: &Ticket,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ProbeView>>, BoxError> {
        ticket.validate()?;
        let observed = self
            .handle
            .workflow::<Probes>()?
            .state(ticket.key().to_vec(), minimum)
            .await?;
        let output = observed
            .output
            .map(|run| -> Result<ProbeView, BoxError> {
                let state: ProbeState = model::decode(&run.state)?;
                if state.ticket != *ticket {
                    return Err("native probe ticket differs".into());
                }
                Ok(ProbeView {
                    run_id: run.run_id,
                    status: match run.status {
                        WorkflowStatus::Running => "running",
                        WorkflowStatus::Paused => "paused",
                        WorkflowStatus::Completed => "completed",
                        WorkflowStatus::Failed => "failed",
                        WorkflowStatus::Cancelled => "cancelled",
                    }
                    .into(),
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output,
        })
    }
}
/// Native probe execution view; completed does not imply SQL result delivery has settled.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProbeView {
    /// Native run identity.
    pub run_id: [u8; 16],
    /// Native status.
    pub status: String,
    /// Permanent occurrence and retained observation.
    pub state: ProbeState,
}
