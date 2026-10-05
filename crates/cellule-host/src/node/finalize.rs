//! Retained fleet terminal action ownership outside the finite action bank.

use std::sync::Arc;

use cellule_runtime::Error;
use cellule_runtime::cell::actor::NodeByteReservation;
use cellule_runtime::fleet::operations::{
    AcceptedFleetAction, DrainEvidence, FleetAction, FleetActionKind, FleetActionOutcome,
    FleetOutcome, MaintenanceAction, MaintenancePhase, OperationError,
};
use cellule_runtime::identity::Digest;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::fleet::{
    FinalizeActionAdmission, FleetActionCompletion, FleetActionExecutor, operation, wall_time_ms,
};

use super::drain::DrainOwner;

/// One canonical Finalize action retained per node boot.
pub(crate) struct FleetFinalizeOwner {
    admission: Mutex<()>,
    current: Mutex<Option<FinalizeSlot>>,
}

#[derive(Clone)]
enum FinalizeSlot {
    Running(Arc<FinalizeAttempt>),
    /// Durable results are reloaded from the journal, avoiding a second retained
    /// copy of the terminal result after publication succeeds. An Unknown result
    /// keeps the original bounded reservation so a retry remains admissible even
    /// if the runtime has already closed.
    Settled {
        key: Digest,
        executor: Arc<FleetActionExecutor>,
        retained: Arc<Mutex<Option<NodeByteReservation>>>,
    },
}

struct FinalizeAttempt {
    key: Digest,
    accepted: AcceptedFleetAction,
    executor: Arc<FleetActionExecutor>,
    completion: watch::Receiver<Option<Arc<FleetActionCompletion>>>,
    completed: watch::Sender<Option<Arc<FleetActionCompletion>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    resolution: Mutex<()>,
    retained: Arc<Mutex<Option<NodeByteReservation>>>,
}

enum StartOutcome {
    Attempt(Arc<FinalizeAttempt>),
    Completed(Arc<FleetActionCompletion>),
}

impl FleetFinalizeOwner {
    pub(crate) fn new() -> Self {
        Self {
            admission: Mutex::new(()),
            current: Mutex::new(None),
        }
    }

    pub(crate) async fn apply(
        self: &Arc<Self>,
        action: FleetAction,
        now_ms: i64,
        executor: Option<Arc<FleetActionExecutor>>,
        drain_owner: Arc<DrainOwner>,
        shutdown_lock: Arc<Mutex<()>>,
        allow_new: bool,
    ) -> Result<Arc<FleetActionCompletion>, Arc<Error>> {
        let key = action.key().map_err(operation).map_err(Arc::new)?;
        let _admission = self.admission.lock().await;
        let mut current = self.current.lock().await;
        let existing = current.clone();

        let started = match existing {
            Some(FinalizeSlot::Running(attempt)) => {
                if attempt.key != key {
                    return Err(Arc::new(operation(OperationError::Conflict)));
                }
                attempt
                    .accepted
                    .validate_replay(&action, attempt.accepted.node(), attempt.accepted.session())
                    .map_err(operation)
                    .map_err(Arc::new)?;
                StartOutcome::Attempt(attempt)
            }
            Some(FinalizeSlot::Settled {
                key: settled_key,
                executor,
                retained,
            }) => {
                if settled_key != key {
                    return Err(Arc::new(operation(OperationError::Conflict)));
                }
                match executor.accept_finalize(&action, now_ms).await? {
                    FinalizeActionAdmission::Completed(completion) => {
                        retained.lock().await.take();
                        StartOutcome::Completed(completion)
                    }
                    FinalizeActionAdmission::Accepted(accepted) => {
                        if retained.lock().await.is_none() {
                            return Err(Arc::new(Error::Control(
                                "fleet Finalize retry lost its retained reservation",
                            )));
                        }
                        self.start_accepted(
                            &mut current,
                            key,
                            accepted,
                            executor,
                            Arc::clone(&drain_owner),
                            Arc::clone(&shutdown_lock),
                            retained,
                        )?
                    }
                }
            }
            None => {
                if !allow_new {
                    return Err(Arc::new(Error::CellDraining));
                }
                let executor = executor.ok_or_else(|| {
                    Arc::new(Error::Control("fleet action executor is not installed"))
                })?;
                self.accept_or_start(
                    &mut current,
                    key,
                    action,
                    now_ms,
                    executor,
                    Arc::clone(&drain_owner),
                    Arc::clone(&shutdown_lock),
                )
                .await?
            }
        };

        drop(current);
        drop(_admission);
        match started {
            StartOutcome::Completed(completion) => {
                verify_stopped(&completion, &drain_owner)?;
                Ok(completion)
            }
            StartOutcome::Attempt(attempt) => {
                let completion = attempt.resolve().await?;
                if completion.committed {
                    self.mark_settled(&attempt).await;
                    verify_stopped(&completion, &drain_owner)?;
                }
                Ok(completion)
            }
        }
    }

    async fn accept_or_start(
        self: &Arc<Self>,
        current: &mut Option<FinalizeSlot>,
        key: Digest,
        action: FleetAction,
        now_ms: i64,
        executor: Arc<FleetActionExecutor>,
        drain_owner: Arc<DrainOwner>,
        shutdown_lock: Arc<Mutex<()>>,
    ) -> Result<StartOutcome, Arc<Error>> {
        if !drain_owner.has_boot_withdrawal().map_err(Arc::new)? {
            return Err(Arc::new(Error::Control(
                "fleet Finalize requires a bound managed-boot withdrawal",
            )));
        }
        // Reserve the retained accepted/result envelope before the journal can
        // commit acceptance. The fixed slot shares this charge across retries.
        let retained = Arc::new(Mutex::new(Some(
            executor.reserve_finalize_retention().map_err(Arc::new)?,
        )));
        match executor.accept_finalize(&action, now_ms).await? {
            FinalizeActionAdmission::Completed(completion) => {
                retained.lock().await.take();
                *current = Some(FinalizeSlot::Settled {
                    key,
                    executor,
                    retained,
                });
                Ok(StartOutcome::Completed(completion))
            }
            FinalizeActionAdmission::Accepted(accepted) => self.start_accepted(
                current,
                key,
                accepted,
                executor,
                drain_owner,
                shutdown_lock,
                retained,
            ),
        }
    }

    fn start_accepted(
        self: &Arc<Self>,
        current: &mut Option<FinalizeSlot>,
        key: Digest,
        accepted: AcceptedFleetAction,
        executor: Arc<FleetActionExecutor>,
        drain_owner: Arc<DrainOwner>,
        shutdown_lock: Arc<Mutex<()>>,
        retained: Arc<Mutex<Option<NodeByteReservation>>>,
    ) -> Result<StartOutcome, Arc<Error>> {
        let attempt = Self::spawn(
            self,
            key,
            accepted,
            executor,
            drain_owner,
            shutdown_lock,
            retained,
        )?;
        *current = Some(FinalizeSlot::Running(Arc::clone(&attempt)));
        Ok(StartOutcome::Attempt(attempt))
    }

    fn spawn(
        owner: &Arc<Self>,
        key: Digest,
        accepted: AcceptedFleetAction,
        executor: Arc<FleetActionExecutor>,
        drain_owner: Arc<DrainOwner>,
        shutdown_lock: Arc<Mutex<()>>,
        retained: Arc<Mutex<Option<NodeByteReservation>>>,
    ) -> Result<Arc<FinalizeAttempt>, Arc<Error>> {
        let (completed, completion) = watch::channel(None);
        let attempt = Arc::new(FinalizeAttempt {
            key,
            accepted: accepted.clone(),
            executor: Arc::clone(&executor),
            completion,
            completed: completed.clone(),
            task: Mutex::new(None),
            resolution: Mutex::new(()),
            retained: Arc::clone(&retained),
        });
        let owner = Arc::downgrade(owner);
        let weak_attempt = Arc::downgrade(&attempt);
        let settled_executor = Arc::clone(&executor);
        let task = tokio::spawn(async move {
            let completion =
                run_finalize(key, accepted, executor, drain_owner, shutdown_lock).await;
            completed.send_replace(Some(Arc::clone(&completion)));
            if completion.committed {
                if !matches!(completion.outcome.outcome, FleetOutcome::Unknown) {
                    retained.lock().await.take();
                }
                if let (Some(owner), Some(attempt)) = (owner.upgrade(), weak_attempt.upgrade()) {
                    owner
                        .mark_settled_with_executor(
                            &attempt,
                            settled_executor,
                            Arc::clone(&retained),
                        )
                        .await;
                }
            }
        });
        match attempt.task.try_lock() {
            Ok(mut slot) => *slot = Some(task),
            Err(_) => {
                task.abort();
                return Err(Arc::new(Error::Control(
                    "fleet Finalize task slot was unexpectedly busy",
                )));
            }
        }
        Ok(attempt)
    }

    async fn mark_settled(&self, attempt: &Arc<FinalizeAttempt>) {
        self.mark_settled_with_executor(
            attempt,
            Arc::clone(&attempt.executor),
            Arc::clone(&attempt.retained),
        )
        .await;
    }

    async fn mark_settled_with_executor(
        &self,
        attempt: &Arc<FinalizeAttempt>,
        executor: Arc<FleetActionExecutor>,
        retained: Arc<Mutex<Option<NodeByteReservation>>>,
    ) {
        let mut current = self.current.lock().await;
        if let Some(FinalizeSlot::Running(active)) = current.as_ref()
            && Arc::ptr_eq(active, attempt)
        {
            *current = Some(FinalizeSlot::Settled {
                key: attempt.key,
                executor,
                retained,
            });
        }
    }
}

impl FinalizeAttempt {
    async fn resolve(self: &Arc<Self>) -> Result<Arc<FleetActionCompletion>, Arc<Error>> {
        let _resolution = self.resolution.lock().await;
        let mut completion = self.wait().await?;
        if !completion.committed {
            completion = Arc::new(
                self.executor
                    .publish_terminal(
                        completion.accepted.clone(),
                        completion.outcome.clone(),
                        completion.execution_error.clone(),
                    )
                    .await,
            );
            self.completed.send_replace(Some(Arc::clone(&completion)));
        }
        if completion.committed && !matches!(completion.outcome.outcome, FleetOutcome::Unknown) {
            self.retained.lock().await.take();
        }
        Ok(completion)
    }

    async fn wait(self: &Arc<Self>) -> Result<Arc<FleetActionCompletion>, Arc<Error>> {
        let mut completion = self.completion.clone();
        loop {
            let returned = { completion.borrow().clone() };
            if let Some(result) = returned {
                // The result was constructed and published before the task
                // signalled. A task epilogue failure cannot invalidate it.
                let _ = self.join().await;
                return Ok(result);
            }
            if completion.changed().await.is_err() {
                let error = match self.join().await {
                    Err(error) => error,
                    Ok(()) => Arc::new(Error::Control(
                        "fleet Finalize task ended without publishing its result",
                    )),
                };
                let returned = { completion.borrow().clone() };
                if let Some(result) = returned {
                    return Ok(result);
                }
                let result =
                    unknown_completion(self.key, &self.accepted, Arc::clone(&self.executor), error)
                        .await;
                self.completed.send_replace(Some(Arc::clone(&result)));
                return Ok(result);
            }
        }
    }

    async fn join(&self) -> Result<(), Arc<Error>> {
        let mut slot = self.task.lock().await;
        if let Some(task) = slot.as_mut() {
            let result = task.await;
            *slot = None;
            if let Err(source) = result {
                return Err(Arc::new(Error::Facility {
                    name: "fleet-finalize-task",
                    source: Box::new(source),
                }));
            }
        }
        Ok(())
    }
}

async fn run_finalize(
    key: Digest,
    accepted: AcceptedFleetAction,
    executor: Arc<FleetActionExecutor>,
    drain_owner: Arc<DrainOwner>,
    shutdown_lock: Arc<Mutex<()>>,
) -> Arc<FleetActionCompletion> {
    let base = match ready_evidence(&accepted) {
        Ok(evidence) => evidence,
        Err(error) => {
            return unknown_completion(key, &accepted, executor, Arc::new(error)).await;
        }
    };
    let shutdown = Arc::clone(&shutdown_lock).lock_owned().await;
    let drain = drain_owner.drain(shutdown, None).await;
    let terminal = match drain {
        Ok(()) => drain_owner
            .confirms_fleet_terminal()
            .map_err(Arc::new)
            .and_then(|confirmed| {
                confirmed.then_some(()).ok_or_else(|| {
                    Arc::new(Error::Control(
                        "canonical node drain returned without withdrawal proof",
                    ))
                })
            }),
        Err(error) => Err(Arc::new(error)),
    };
    match terminal {
        Ok(()) => {
            let mut evidence = base;
            evidence.facilities_closed = true;
            evidence.stopped = true;
            evidence.withdrawn = true;
            let (result, time_error) = outcome(&accepted, key, FleetOutcome::Stopped(evidence));
            match accepted.validate_result(&result) {
                Ok(()) => {
                    let execution_error = time_error.map(Arc::new);
                    Arc::new(
                        executor
                            .publish_terminal(accepted, result, execution_error)
                            .await,
                    )
                }
                Err(error) => {
                    unknown_completion(key, &accepted, executor, Arc::new(operation(error))).await
                }
            }
        }
        Err(error) => unknown_completion(key, &accepted, executor, error).await,
    }
}

async fn unknown_completion(
    key: Digest,
    accepted: &AcceptedFleetAction,
    executor: Arc<FleetActionExecutor>,
    error: Arc<Error>,
) -> Arc<FleetActionCompletion> {
    let (result, time_error) = outcome(accepted, key, FleetOutcome::Unknown);
    let execution_error = Some(match time_error {
        None => error,
        Some(clock) => Arc::new(Error::Facility {
            name: "fleet-finalize-clock",
            source: Box::new(FinalizeFailure {
                primary: error,
                clock,
            }),
        }),
    });
    Arc::new(
        executor
            .publish_terminal(accepted.clone(), result, execution_error)
            .await,
    )
}

fn outcome(
    accepted: &AcceptedFleetAction,
    key: Digest,
    outcome: FleetOutcome,
) -> (FleetActionOutcome, Option<Error>) {
    let (observed_at_ms, time_error) = match wall_time_ms() {
        Ok(now) if now >= accepted.accepted_at_ms() => (now, None),
        Ok(_) => (
            accepted.accepted_at_ms(),
            Some(Error::Control("fleet Finalize clock regressed")),
        ),
        Err(error) => (accepted.accepted_at_ms(), Some(error)),
    };
    (
        FleetActionOutcome {
            scope: accepted.action().scope(),
            action_key: key,
            node: accepted.node(),
            session: accepted.session(),
            observed_at_ms,
            outcome,
        },
        time_error,
    )
}

fn ready_evidence(accepted: &AcceptedFleetAction) -> cellule_runtime::Result<DrainEvidence> {
    let FleetActionKind::Maintenance {
        action: MaintenanceAction::Finalize,
        operation,
    } = accepted.action().kind()
    else {
        return Err(Error::Control("accepted action is not fleet Finalize"));
    };
    let evidence = operation
        .drain_evidence()
        .ok_or(Error::Control("fleet Finalize lacks drain evidence"))?;
    if operation.phase() != MaintenancePhase::Closing
        || evidence.node != accepted.node()
        || evidence.session != accepted.session()
        || evidence.remaining_cells != 0
        || evidence.unresolved_attempts != 0
        || !evidence.relocated
        || !evidence.readers_settled
        || !evidence.followers_settled
        || evidence.facilities_closed
        || evidence.stopped
        || evidence.withdrawn
    {
        return Err(Error::Control(
            "fleet Finalize evidence is not ready to close",
        ));
    }
    Ok(evidence)
}

fn verify_stopped(
    completion: &FleetActionCompletion,
    drain_owner: &DrainOwner,
) -> Result<(), Arc<Error>> {
    if matches!(completion.outcome.outcome, FleetOutcome::Stopped(_))
        && !drain_owner.confirms_fleet_terminal().map_err(Arc::new)?
    {
        return Err(Arc::new(Error::Control(
            "committed fleet Finalize result lacks local terminal drain proof",
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct FinalizeFailure {
    primary: Arc<Error>,
    clock: Error,
}

impl std::fmt::Display for FinalizeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}; result clock failed: {}",
            self.primary, self.clock
        )
    }
}

impl std::error::Error for FinalizeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
