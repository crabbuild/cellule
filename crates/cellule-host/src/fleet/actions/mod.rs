use std::sync::{Arc, Mutex};

use cellule_runtime::Error;
use cellule_runtime::cell::actor::{CellRuntime, NodeByteReservation};
use cellule_runtime::fleet::operations::{
    AcceptedFleetAction, DrainBlocker, FleetAction, FleetActionKind, FleetActionOutcome,
    FleetInspectionObservation, FleetInspectionRequest, FleetOutcome, FleetScope,
    MAX_ACTIVE_ATTEMPTS, MAX_RECORD_BYTES, MaintenanceAction, OperationError,
};
use cellule_runtime::identity::{Digest, NodeId, SessionId};
use tokio::{sync::watch, task::JoinHandle};

use super::snapshot::{FleetNodeSnapshot, FleetSnapshotRequest, SnapshotOwners};
use super::{FleetActionAcceptance, FleetActionJournal, FleetCellProvider, FleetRoleSettlement};

mod work;
pub use work::{
    FleetActionWorkEntry, FleetActionWorkKind, FleetActionWorkSnapshot, FleetActionWorkState,
};

/// Retained completion of one accepted canonical effect and journal write.
#[derive(Debug)]
pub struct FleetActionCompletion {
    /// Exact original acceptance. Its record does not confer Cell authority.
    pub accepted: AcceptedFleetAction,
    /// Checked canonical result, or Unknown when an effect cannot be proved.
    pub outcome: FleetActionOutcome,
    /// True only after the journal confirmed durable result publication.
    pub committed: bool,
    /// Original runtime error, independently of result publication failure.
    pub execution_error: Option<Arc<Error>>,
    /// Original result-publication error. Native effects retry the same retained
    /// evidence without reexecution. A failed retry of joined read-only role
    /// evidence retires that local proof; a fresh full capture must replace it.
    pub journal_error: Option<Arc<Error>>,
}

pub(super) struct ActionResult {
    pub(super) outcome: FleetOutcome,
    pub(super) error: Option<Error>,
}

/// Result of durably accepting a host Finalize action outside the ordinary
/// finite action bank.
pub(crate) enum FinalizeActionAdmission {
    /// The local terminal owner must run or resume the canonical host drain.
    Accepted(AcceptedFleetAction),
    /// The exact action already has a durable non-Unknown result.
    Completed(Arc<FleetActionCompletion>),
}

impl ActionResult {
    pub(super) fn checked(outcome: FleetOutcome) -> Self {
        Self {
            outcome,
            error: None,
        }
    }
    pub(super) fn refused(blocker: DrainBlocker, error: Error) -> Self {
        Self {
            outcome: FleetOutcome::Rejected(blocker),
            error: Some(error),
        }
    }
}

type EffectCompletion = Result<Arc<FleetActionCompletion>, Arc<Error>>;
type Completion = Result<Arc<JobCompletion>, Arc<Error>>;

enum JobCompletion {
    Effect(Arc<FleetActionCompletion>),
    Inspection(Arc<FleetInspectionObservation>),
    Snapshot(Arc<FleetNodeSnapshot>),
}

#[derive(Clone)]
enum JobRequest {
    Effect {
        action: FleetAction,
        now_ms: i64,
        settlement: Option<Arc<FleetRoleSettlement>>,
    },
    Inspection(FleetInspectionRequest),
    Snapshot {
        request: Box<FleetSnapshotRequest>,
        owners: Arc<SnapshotOwners>,
    },
}

impl JobRequest {
    fn scope(&self) -> FleetScope {
        match self {
            Self::Effect { action, .. } => action.scope(),
            Self::Inspection(request) => request.action().scope(),
            Self::Snapshot { request, .. } => request.expected().head().scope(),
        }
    }
    fn validate_endpoint(&self, node: NodeId, session: SessionId) -> Result<(), OperationError> {
        match self {
            Self::Effect { action, .. } => action.validate_endpoint(node, session),
            Self::Inspection(request) => request.validate_endpoint(node, session),
            Self::Snapshot { request, .. }
                if request.node() == node && request.session() == session =>
            {
                Ok(())
            }
            Self::Snapshot { .. } => Err(OperationError::Fenced),
        }
    }
    fn key(&self) -> Result<Digest, OperationError> {
        match self {
            Self::Effect { action, .. } => action.key(),
            Self::Inspection(request) => request.key(),
            Self::Snapshot { request, .. } => request.key(),
        }
    }
    fn validate_replay(&self, other: &Self) -> Result<(), OperationError> {
        match (self, other) {
            (
                Self::Effect {
                    action, settlement, ..
                },
                Self::Effect {
                    action: replay,
                    settlement: replay_settlement,
                    ..
                },
            ) if settlement == replay_settlement => action.validate_replay(replay),
            (Self::Inspection(original), Self::Inspection(replay)) if original == replay => Ok(()),
            (
                Self::Snapshot {
                    request: original, ..
                },
                Self::Snapshot {
                    request: replay, ..
                },
            ) if original == replay => Ok(()),
            _ => Err(OperationError::Conflict),
        }
    }
}

pub(crate) struct FleetActionExecutor {
    pub(super) runtime: CellRuntime,
    pub(super) scope: FleetScope,
    pub(super) node: NodeId,
    pub(super) session: SessionId,
    pub(super) journal: Arc<dyn FleetActionJournal>,
    pub(super) cells: Arc<dyn FleetCellProvider>,
    pub(super) registry: Arc<cellule_runtime::registry::Registry>,
    bank: Mutex<ActionBank>,
}

#[derive(Default)]
struct ActionBank {
    draining: bool,
    // Read-only snapshots do not advance this counter; other accepted jobs and
    // their removal must remain visible even between two empty observations.
    work_revision: u64,
    // Includes captures for the before/after check within one native interval;
    // it is excluded from cross-capture fingerprints so fresh reads converge.
    capture_revision: u64,
    jobs: Vec<Arc<ActionJob>>,
    failure: Option<Arc<Error>>,
}

struct ActionJob {
    key: Digest,
    request: JobRequest,
    completion: watch::Receiver<Option<Completion>>,
    task: tokio::sync::Mutex<ActionJoin>,
    _retained: NodeByteReservation,
}

struct ActionJoin {
    task: Option<JoinHandle<()>>,
    failure: Option<Arc<Error>>,
}

impl ActionJoin {
    async fn join(&mut self) -> Result<(), Arc<Error>> {
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            if let Err(source) = result {
                self.failure = Some(Arc::new(Error::Facility {
                    name: "fleet-action-task",
                    source: Box::new(source),
                }));
            }
        }
        match &self.failure {
            Some(error) => Err(Arc::clone(error)),
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
struct RetainedActionFailure(Arc<Error>);

impl std::fmt::Display for RetainedActionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}

impl std::error::Error for RetainedActionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn retained_action_failure(error: Arc<Error>) -> Error {
    Error::Facility {
        name: "fleet-action-task",
        source: Box::new(RetainedActionFailure(error)),
    }
}

pub(crate) fn operation(error: OperationError) -> Error {
    Error::FleetOperation(Box::new(error))
}

pub(super) fn journal_error(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-action-journal",
        source,
    }
}

impl FleetActionExecutor {
    pub(crate) fn new(
        runtime: CellRuntime,
        scope: FleetScope,
        node: NodeId,
        session: SessionId,
        journal: Arc<dyn FleetActionJournal>,
        cells: Arc<dyn FleetCellProvider>,
        registry: Arc<cellule_runtime::registry::Registry>,
    ) -> cellule_runtime::Result<Self> {
        cellule_runtime::fleet::operations::FleetHead::new(scope, 0).map_err(operation)?;
        if !node.as_bytes().iter().any(|byte| *byte != 0)
            || !session.as_bytes().iter().any(|byte| *byte != 0)
        {
            return Err(Error::Node("invalid fleet executor binding"));
        }
        Ok(Self {
            runtime,
            scope,
            node,
            session,
            journal,
            cells,
            registry,
            bank: Mutex::new(ActionBank::default()),
        })
    }

    pub(crate) fn reserve_finalize_retention(
        &self,
    ) -> cellule_runtime::Result<NodeByteReservation> {
        self.runtime
            .try_reserve_node_bytes(3 * MAX_RECORD_BYTES as usize)
    }

    pub(crate) async fn apply(
        self: &Arc<Self>,
        action: FleetAction,
        now_ms: i64,
    ) -> EffectCompletion {
        if matches!(action.kind(), FleetActionKind::Maintenance { action, .. } if *action != MaintenanceAction::Cordon)
        {
            return Err(Arc::new(Error::Control(
                "fleet role settlement and finalization require their host barriers",
            )));
        }
        if matches!(
            action.kind(),
            FleetActionKind::Movement {
                action: cellule_runtime::fleet::operations::MovementAction::Inspect,
                ..
            }
        ) {
            return Err(Arc::new(Error::Control(
                "fleet Inspect requires request-bound inspection",
            )));
        }
        if now_ms < action.issued_at_ms() {
            return Err(Arc::new(operation(OperationError::Invalid(
                "fleet action time regressed",
            ))));
        }
        let completion = self
            .submit(JobRequest::Effect {
                action,
                now_ms,
                settlement: None,
            })
            .await?;
        match completion.as_ref() {
            JobCompletion::Effect(result) => Ok(Arc::clone(result)),
            _ => Err(Arc::new(Error::Control(
                "fleet action completion kind mismatch",
            ))),
        }
    }

    pub(crate) async fn apply_role_settlement(
        self: &Arc<Self>,
        action: FleetAction,
        settlement: FleetRoleSettlement,
        now_ms: i64,
    ) -> EffectCompletion {
        if action.scope() != self.scope
            || !matches!(
                action.kind(),
                FleetActionKind::Maintenance {
                    action: MaintenanceAction::SettleRoles,
                    ..
                }
            )
        {
            return Err(Arc::new(Error::Fenced));
        }
        action
            .validate_endpoint(self.node, self.session)
            .map_err(operation)
            .map_err(Arc::new)?;
        settlement.validate_for(&action).map_err(Arc::new)?;
        if now_ms < action.issued_at_ms() {
            return Err(Arc::new(operation(OperationError::Invalid(
                "fleet action time regressed",
            ))));
        }
        let completion = self
            .submit(JobRequest::Effect {
                action,
                now_ms,
                settlement: Some(Arc::new(settlement)),
            })
            .await?;
        match completion.as_ref() {
            JobCompletion::Effect(result) => Ok(Arc::clone(result)),
            _ => Err(Arc::new(Error::Control(
                "fleet role settlement completion kind mismatch",
            ))),
        }
    }

    /// Accepts the terminal action without adding it to the bank that the
    /// canonical node drain joins. Closing admission after acceptance makes
    /// every previously admitted sibling part of that drain's join set.
    pub(crate) async fn accept_finalize(
        &self,
        action: &FleetAction,
        now_ms: i64,
    ) -> Result<FinalizeActionAdmission, Arc<Error>> {
        let maintenance = match action.kind() {
            FleetActionKind::Maintenance {
                action: MaintenanceAction::Finalize,
                operation,
            } => operation,
            _ => {
                return Err(Arc::new(Error::Control(
                    "host finalization requires a Finalize maintenance action",
                )));
            }
        };
        let evidence = maintenance.drain_evidence().ok_or_else(|| {
            Arc::new(operation(OperationError::Invalid(
                "fleet Finalize lacks committed drain evidence",
            )))
        })?;
        if action.scope() != self.scope
            || maintenance.node() != self.node
            || maintenance.session() != self.session
            || maintenance.phase() != cellule_runtime::fleet::operations::MaintenancePhase::Closing
            || evidence.node != self.node
            || evidence.session != self.session
            || evidence.remaining_cells != 0
            || evidence.unresolved_attempts != 0
            || !evidence.relocated
            || !evidence.readers_settled
            || !evidence.followers_settled
        {
            return Err(Arc::new(operation(OperationError::Invalid(
                "fleet Finalize lacks exact ready-to-close evidence",
            ))));
        }
        action
            .validate_endpoint(self.node, self.session)
            .map_err(operation)
            .map_err(Arc::new)?;
        if now_ms < action.issued_at_ms() {
            return Err(Arc::new(operation(OperationError::Invalid(
                "fleet Finalize time regressed",
            ))));
        }

        let acceptance = self
            .journal
            .accept_action(action, self.node, self.session, now_ms)
            .await
            .map_err(journal_error)
            .map_err(Arc::new)?;
        let accepted = match acceptance {
            FleetActionAcceptance::New(accepted) => {
                accepted
                    .validate_replay(action, self.node, self.session)
                    .map_err(operation)
                    .map_err(Arc::new)?;
                if accepted.action() != action || accepted.accepted_at_ms() != now_ms {
                    return Err(Arc::new(operation(OperationError::Conflict)));
                }
                accepted
            }
            FleetActionAcceptance::Existing { accepted, result } => {
                accepted
                    .validate_replay(action, self.node, self.session)
                    .map_err(operation)
                    .map_err(Arc::new)?;
                if let Some(result) = result {
                    accepted
                        .validate_result(&result)
                        .map_err(operation)
                        .map_err(Arc::new)?;
                    if !matches!(&result.outcome, FleetOutcome::Unknown) {
                        return Ok(FinalizeActionAdmission::Completed(Arc::new(
                            FleetActionCompletion {
                                accepted,
                                outcome: *result,
                                committed: true,
                                execution_error: None,
                                journal_error: None,
                            },
                        )));
                    }
                }
                accepted
            }
        };
        self.close_admission().map_err(Arc::new)?;
        Ok(FinalizeActionAdmission::Accepted(accepted))
    }

    fn close_admission(&self) -> cellule_runtime::Result<()> {
        let mut bank = self
            .bank
            .lock()
            .map_err(|_| Error::Control("fleet action bank poisoned"))?;
        bank.draining = true;
        Ok(())
    }

    /// Publishes terminal evidence retained by the host Finalize owner.
    pub(crate) async fn publish_terminal(
        &self,
        accepted: AcceptedFleetAction,
        outcome: FleetActionOutcome,
        execution_error: Option<Arc<Error>>,
    ) -> FleetActionCompletion {
        self.publish(accepted, outcome, execution_error).await
    }

    pub(crate) async fn observe(
        self: &Arc<Self>,
        request: FleetInspectionRequest,
    ) -> Result<Arc<FleetInspectionObservation>, Arc<Error>> {
        if request.node() != self.node || request.session() != self.session {
            return Err(Arc::new(Error::Fenced));
        }
        if !matches!(
            request.action().kind(),
            FleetActionKind::Movement { .. }
                | FleetActionKind::Maintenance {
                    action: MaintenanceAction::Inspect,
                    ..
                }
        ) {
            return Err(Arc::new(Error::Control(
                "fleet inspection requires an Inspect action",
            )));
        }
        let completion = self.submit(JobRequest::Inspection(request)).await?;
        match completion.as_ref() {
            JobCompletion::Inspection(result) => Ok(Arc::clone(result)),
            _ => Err(Arc::new(Error::Control(
                "fleet inspection completion kind mismatch",
            ))),
        }
    }

    pub(crate) async fn snapshot(
        self: &Arc<Self>,
        request: FleetSnapshotRequest,
        owners: SnapshotOwners,
    ) -> Result<Arc<FleetNodeSnapshot>, Arc<Error>> {
        let completion = self
            .submit(JobRequest::Snapshot {
                request: Box::new(request),
                owners: Arc::new(owners),
            })
            .await?;
        match completion.as_ref() {
            JobCompletion::Snapshot(snapshot) => Ok(Arc::clone(snapshot)),
            _ => Err(Arc::new(Error::Control(
                "fleet native snapshot completion kind mismatch",
            ))),
        }
    }

    async fn submit(self: &Arc<Self>, request: JobRequest) -> Completion {
        if request.scope() != self.scope {
            return Err(Arc::new(Error::PeerAuthorization(
                "fleet action scope mismatch",
            )));
        }
        request
            .validate_endpoint(self.node, self.session)
            .map_err(operation)
            .map_err(Arc::new)?;
        self.reap().await.map_err(Arc::new)?;
        let key = request.key().map_err(operation).map_err(Arc::new)?;
        let (mut response, retained_job) = {
            let mut bank = self
                .bank
                .lock()
                .map_err(|_| Arc::new(Error::Control("fleet action bank poisoned")))?;
            if bank.draining {
                return Err(Arc::new(Error::CellDraining));
            }
            if let Some(job) = bank.jobs.iter().find(|job| job.key == key) {
                job.request
                    .validate_replay(&request)
                    .map_err(operation)
                    .map_err(Arc::new)?;
                (job.completion.clone(), Arc::clone(job))
            } else {
                if bank.jobs.len() >= MAX_ACTIVE_ATTEMPTS {
                    return Err(Arc::new(Error::Capacity("fleet action receipt bound")));
                }
                let revision = bank.next_work_revision(&request).map_err(Arc::new)?;
                let capture_revision = bank.next_capture_revision().map_err(Arc::new)?;
                // Leave room to remove every accepted sibling, including this
                // job. Counter exhaustion must refuse before spawning work.
                bank.work_revision
                    .checked_add(MAX_ACTIVE_ATTEMPTS as u64 + 1)
                    .ok_or_else(|| {
                        Arc::new(Error::Capacity("fleet action work revision exhausted"))
                    })?;
                bank.capture_revision
                    .checked_add(MAX_ACTIVE_ATTEMPTS as u64 + 1)
                    .ok_or_else(|| {
                        Arc::new(Error::Capacity("fleet action capture revision exhausted"))
                    })?;
                // Both the accepted envelope and its checked result remain owned
                // after an RPC waiter disappears, using the shared node ledger.
                let retained = self
                    .runtime
                    .try_reserve_node_bytes(3 * MAX_RECORD_BYTES as usize)
                    .map_err(Arc::new)?;
                let (sender, response) = watch::channel(None);
                let executor = Arc::clone(self);
                let issued = request.clone();
                let task = tokio::spawn(async move {
                    let result = match issued {
                        JobRequest::Effect {
                            action,
                            now_ms,
                            settlement,
                        } => executor
                            .execute(action, now_ms, settlement)
                            .await
                            .map(JobCompletion::Effect),
                        JobRequest::Inspection(request) => executor
                            .execute_inspection(request)
                            .await
                            .map(JobCompletion::Inspection),
                        JobRequest::Snapshot { request, owners } => executor
                            .execute_snapshot(*request, owners)
                            .await
                            .map(JobCompletion::Snapshot),
                    }
                    .map(Arc::new);
                    let _ = sender.send(Some(result));
                });
                let job = Arc::new(ActionJob {
                    key,
                    request,
                    completion: response.clone(),
                    task: tokio::sync::Mutex::new(ActionJoin {
                        task: Some(task),
                        failure: None,
                    }),
                    _retained: retained,
                });
                bank.jobs.push(Arc::clone(&job));
                bank.work_revision = revision;
                bank.capture_revision = capture_revision;
                (response, job)
            }
        };
        loop {
            let completed = response.borrow().clone();
            if let Some(result) = completed {
                // Sending completion precedes the task's exit. Join that short
                // epilogue so an immediate retry can reap an unconfirmed result
                // and retry publication instead of joining the stale receipt.
                // The job stays owned if this waiter is dropped during the join.
                retained_job.task.lock().await.join().await?;
                return result;
            }
            if response.changed().await.is_err() {
                // The waiter retains this exact job even if a concurrent drain
                // removes its settled bank receipt. Preserve its own task error.
                retained_job.task.lock().await.join().await?;
                return Err(Arc::new(Error::Control(
                    "fleet action completion task ended",
                )));
            }
        }
    }

    async fn execute(
        &self,
        action: FleetAction,
        now_ms: i64,
        settlement: Option<Arc<FleetRoleSettlement>>,
    ) -> EffectCompletion {
        let acceptance = self
            .journal
            .accept_action(&action, self.node, self.session, now_ms)
            .await
            .map_err(journal_error)
            .map_err(Arc::new)?;
        let (accepted, result) = match acceptance {
            FleetActionAcceptance::Existing { accepted, result } => {
                accepted
                    .validate_replay(&action, self.node, self.session)
                    .map_err(operation)
                    .map_err(Arc::new)?;
                let refresh_roles = matches!(
                    accepted.action().kind(),
                    FleetActionKind::Maintenance {
                        action: MaintenanceAction::SettleRoles,
                        ..
                    }
                ) && result.as_ref().is_some_and(|result| {
                    matches!(
                        &result.outcome,
                        FleetOutcome::RolesSettled { .. } | FleetOutcome::RolesSettledAt { .. }
                    )
                });
                if let Some(result) = result
                    && !matches!(result.outcome, FleetOutcome::Unknown)
                    && !(refresh_roles && settlement.is_some())
                {
                    accepted
                        .validate_result(&result)
                        .map_err(operation)
                        .map_err(Arc::new)?;
                    return Ok(Arc::new(FleetActionCompletion {
                        accepted,
                        outcome: *result,
                        committed: true,
                        execution_error: None,
                        journal_error: None,
                    }));
                }
                let result = if let Some(settlement) = settlement {
                    self.perform_action(&accepted, &action, Some(settlement))
                        .await
                } else {
                    self.inspect_accepted(&accepted).await
                };
                (accepted, result)
            }
            FleetActionAcceptance::New(accepted) => {
                accepted
                    .validate_replay(&action, self.node, self.session)
                    .map_err(operation)
                    .map_err(Arc::new)?;
                if accepted.action() != &action || accepted.accepted_at_ms() != now_ms {
                    return Err(Arc::new(operation(OperationError::Conflict)));
                }
                let result = self.perform_action(&accepted, &action, settlement).await;
                (accepted, result)
            }
        };
        let (outcome, mut execution_error) = match result {
            Ok(result) => (result.outcome, result.error.map(Arc::new)),
            Err(error) => (FleetOutcome::Unknown, Some(Arc::new(error))),
        };
        // Keep canonical proof even if the clock fails after release. An older
        // timestamp is conservative, and the original clock error is retained.
        let observed_at_ms = match wall_time_ms() {
            Ok(now) if now >= accepted.accepted_at_ms() => now,
            clock => {
                if execution_error.is_none() {
                    execution_error = Some(Arc::new(match clock {
                        Err(error) => error,
                        Ok(_) => Error::Control("fleet action clock regressed"),
                    }));
                }
                accepted.accepted_at_ms()
            }
        };
        let outcome = FleetActionOutcome {
            scope: action.scope(),
            action_key: action.key().map_err(operation).map_err(Arc::new)?,
            node: self.node,
            session: self.session,
            observed_at_ms,
            outcome,
        };
        accepted
            .validate_result(&outcome)
            .map_err(operation)
            .map_err(Arc::new)?;
        if let Some(error) = &execution_error {
            tracing::warn!(action_key = ?outcome.action_key, node = ?self.node,
                session = ?self.session, error = ?error, "fleet action retained execution error");
        }
        Ok(Arc::new(
            self.publish(accepted, outcome, execution_error).await,
        ))
    }

    async fn publish(
        &self,
        accepted: AcceptedFleetAction,
        outcome: FleetActionOutcome,
        execution_error: Option<Arc<Error>>,
    ) -> FleetActionCompletion {
        let journal_error = self
            .journal
            .publish_action_result(&accepted, &outcome)
            .await
            .err()
            .map(journal_error)
            .map(Arc::new);
        FleetActionCompletion {
            accepted,
            outcome,
            committed: journal_error.is_none(),
            execution_error,
            journal_error,
        }
    }

    async fn reap(&self) -> cellule_runtime::Result<()> {
        let (jobs, mut first_error) = {
            let bank = self
                .bank
                .lock()
                .map_err(|_| Error::Control("fleet action bank poisoned"))?;
            (
                bank.jobs.clone(),
                bank.failure
                    .as_ref()
                    .map(|error| retained_action_failure(Arc::clone(error))),
            )
        };
        for job in jobs {
            let mut task = job.task.lock().await;
            if task.task.as_ref().is_some_and(|task| !task.is_finished()) {
                continue;
            }
            let result = self.finish_job(&job, &mut task).await;
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn drain(&self) -> cellule_runtime::Result<()> {
        {
            let mut bank = self
                .bank
                .lock()
                .map_err(|_| Error::Control("fleet action bank poisoned"))?;
            bank.draining = true;
        }
        let (jobs, mut first_error) = {
            let bank = self
                .bank
                .lock()
                .map_err(|_| Error::Control("fleet action bank poisoned"))?;
            (
                bank.jobs.clone(),
                bank.failure
                    .as_ref()
                    .map(|error| retained_action_failure(Arc::clone(error))),
            )
        };
        for job in jobs {
            let mut task = job.task.lock().await;
            // A failed job must not short-circuit the join of a sibling whose
            // accepted work is still owned. Keep its original failure separately
            // from the settled receipt, then continue all joins/publications.
            let result = self.finish_job(&job, &mut task).await;
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn remember_failure(&self, error: Arc<Error>) -> cellule_runtime::Result<()> {
        let mut bank = self
            .bank
            .lock()
            .map_err(|_| Error::Control("fleet action bank poisoned"))?;
        if bank.failure.is_none() {
            bank.failure = Some(error);
        }
        Ok(())
    }

    async fn finish_job(
        &self,
        job: &Arc<ActionJob>,
        task: &mut ActionJoin,
    ) -> cellule_runtime::Result<()> {
        match task.join().await {
            Err(error) => {
                self.remember_failure(Arc::clone(&error))?;
                self.remove_job(job)?;
                Err(retained_action_failure(error))
            }
            Ok(()) => self.settle_job(job).await,
        }
    }

    fn remove_job(&self, job: &Arc<ActionJob>) -> cellule_runtime::Result<()> {
        let mut bank = self
            .bank
            .lock()
            .map_err(|_| Error::Control("fleet action bank poisoned"))?;
        if bank.jobs.iter().any(|entry| Arc::ptr_eq(entry, job)) {
            let revision = bank.next_work_revision(&job.request)?;
            let capture_revision = bank.next_capture_revision()?;
            bank.jobs.retain(|entry| !Arc::ptr_eq(entry, job));
            bank.work_revision = revision;
            bank.capture_revision = capture_revision;
        }
        Ok(())
    }

    async fn settle_job(&self, job: &Arc<ActionJob>) -> cellule_runtime::Result<()> {
        let completion = job.completion.borrow().clone();
        if let Some(Ok(completion)) = completion
            && let JobCompletion::Effect(completion) = completion.as_ref()
        {
            if !completion.committed {
                // Retry the retained proof, never the canonical effect. Native
                // effect receipts stay retained across every publication failure.
                if let Err(source) = self
                    .journal
                    .publish_action_result(&completion.accepted, &completion.outcome)
                    .await
                {
                    if matches!(&job.request, JobRequest::Effect {
                        action, settlement: Some(_), ..
                    } if matches!(action.kind(), FleetActionKind::Maintenance {
                        action: MaintenanceAction::SettleRoles, ..
                    })) {
                        // SettleRoles only validates a captured observation; it
                        // starts no physical effect. A joined proof cannot be
                        // restamped after its journal barrier changes. Return the
                        // publication error and retire this local read so the
                        // next pass can collect fresh opaque evidence. Its durable
                        // acceptance remains unresolved until that proof commits.
                        // Movement/native-effect receipts keep their original
                        // evidence and retained owner on every publication error.
                        self.remove_job(job)?;
                    }
                    return Err(journal_error(source));
                }
            }
            self.retire_receiver_receipt(completion).await?;
        }
        self.remove_job(job)?;
        Ok(())
    }
}

impl ActionBank {
    fn next_capture_revision(&self) -> cellule_runtime::Result<u64> {
        self.capture_revision
            .checked_add(1)
            .ok_or(Error::Capacity("fleet action capture revision exhausted"))
    }
    fn next_work_revision(&self, request: &JobRequest) -> cellule_runtime::Result<u64> {
        if matches!(request, JobRequest::Snapshot { .. }) {
            return Ok(self.work_revision);
        }
        self.work_revision
            .checked_add(1)
            .ok_or(Error::Capacity("fleet action work revision exhausted"))
    }
}

pub(crate) fn wall_time_ms() -> cellule_runtime::Result<i64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|source| Error::Facility {
            name: "fleet-action-clock",
            source: Box::new(source),
        })?;
    i64::try_from(elapsed.as_millis()).map_err(|source| Error::Facility {
        name: "fleet-action-clock",
        source: Box::new(source),
    })
}
