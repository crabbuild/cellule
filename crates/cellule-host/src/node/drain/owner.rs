//! Retain the complete host attempt without moving the shared drain lane.
use super::*;
use tokio::sync::{OwnedMutexGuard, watch};

type DrainResult = std::result::Result<(), Arc<Error>>;

pub(crate) struct DrainOwner {
    resources: Arc<DrainResources>,
    bank: Mutex<DrainBank>,
    history: Arc<Mutex<DrainHistory>>,
}

#[derive(Default)]
struct DrainBank {
    serial: u64,
    current: Option<Arc<DrainAttempt>>,
}

#[derive(Default)]
struct DrainHistory {
    first: Option<Arc<Error>>,
    latest: Option<Arc<Error>>,
}

struct DrainAttempt {
    serial: u64,
    task: tokio::sync::Mutex<DrainJoin>,
    returned: watch::Sender<Option<DrainResult>>,
    joined: AtomicBool,
}

enum DrainJoin {
    Running(JoinHandle<DrainResult>),
    Finished {
        result: DrainResult,
        task_failure: Option<Arc<Error>>,
    },
}

impl DrainHistory {
    fn record(&mut self, source: Arc<Error>) {
        self.first.get_or_insert_with(|| Arc::clone(&source));
        self.latest = Some(source);
    }
}

impl DrainOwner {
    pub(crate) fn new(
        runtime: CellRuntime,
        state: Arc<Mutex<NodeState>>,
        facilities: Arc<Mutex<Vec<CellNodeFacility>>>,
        task_group: Arc<Mutex<Option<Arc<CellNodeTaskGroup>>>>,
    ) -> Self {
        Self {
            resources: Arc::new(DrainResources {
                runtime,
                state,
                facilities,
                task_group,
                runtime_drain: tokio::sync::Mutex::new(RuntimeDrain::Idle),
                boot_withdrawal: Mutex::new(None),
            }),
            bank: Mutex::new(DrainBank::default()),
            history: Arc::new(Mutex::new(DrainHistory::default())),
        }
    }

    pub(crate) fn bind_boot_withdrawal(
        &self,
        withdrawal: crate::fleet::withdrawal::FleetBootWithdrawal,
    ) -> cellule_runtime::Result<()> {
        let mut binding = self
            .resources
            .boot_withdrawal
            .lock()
            .map_err(|_| Error::Control("CellNode boot withdrawal lock poisoned"))?;
        if binding.is_some() {
            return Err(Error::Control("CellNode boot withdrawal already installed"));
        }
        *binding = Some(Arc::new(withdrawal));
        Ok(())
    }

    pub(crate) async fn drain(
        &self,
        shutdown: OwnedMutexGuard<()>,
        deadline: Option<Instant>,
    ) -> cellule_runtime::Result<()> {
        let previous = self
            .bank
            .lock()
            .map_err(|_| Error::Control("CellNode drain bank poisoned"))?
            .current
            .clone();
        if let Some(previous) = previous {
            // Acquiring the lane proves the old resource sequence released its
            // guard. Join its actual epilogue before replacing this fixed slot.
            // A task panic remains fatal; it is not a retryable phase timeout.
            previous.join(&self.history).await.map_err(shared_error)?;
        }
        {
            let mut state = self
                .resources
                .state
                .lock()
                .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
            if *state == NodeState::Stopped {
                return Ok(());
            }
            *state = NodeState::Draining;
        }
        let attempt = {
            let mut bank = self
                .bank
                .lock()
                .map_err(|_| Error::Control("CellNode drain bank poisoned"))?;
            let serial = bank
                .serial
                .checked_add(1)
                .ok_or(Error::Capacity("CellNode drain serial exhausted"))?;
            let resources = Arc::clone(&self.resources);
            let history = Arc::clone(&self.history);
            let (returned, _) = watch::channel(None);
            let result = returned.clone();
            // Capture resources and diagnostic history, never this owner or its
            // slot. The retained task cannot form an owner/join-handle cycle.
            let task = tokio::spawn(async move {
                let _shutdown = shutdown;
                let outcome = resources.run(deadline).await.map_err(Arc::new);
                if let Err(source) = &outcome {
                    record_failure(&history, Arc::clone(source));
                }
                result.send_replace(Some(outcome.clone()));
                outcome
            });
            let attempt = Arc::new(DrainAttempt {
                serial,
                task: tokio::sync::Mutex::new(DrainJoin::Running(task)),
                returned,
                joined: AtomicBool::new(false),
            });
            bank.serial = serial;
            bank.current = Some(Arc::clone(&attempt));
            attempt
        };
        attempt.join(&self.history).await.map_err(shared_error)?;
        attempt.result().await?.map_err(shared_error)
    }

    pub(crate) fn observe(&self) -> cellule_runtime::Result<Option<NodeDrainObservation>> {
        let current = self
            .bank
            .lock()
            .map_err(|_| Error::Control("CellNode drain bank poisoned"))?
            .current
            .clone();
        let Some(current) = current else {
            return Ok(None);
        };
        let joined = current.joined.load(Ordering::Acquire);
        let result = current.returned.borrow().clone();
        let phase = if joined {
            NodeDrainPhase::Joined
        } else if result.is_some() {
            NodeDrainPhase::Returned
        } else {
            NodeDrainPhase::Running
        };
        let history = self
            .history
            .lock()
            .map_err(|_| Error::Control("CellNode drain history poisoned"))?;
        Ok(Some(NodeDrainObservation {
            serial: current.serial,
            phase,
            result,
            first_failure: history.first.clone(),
            latest_failure: history.latest.clone(),
        }))
    }
}

impl DrainAttempt {
    async fn join(&self, history: &Mutex<DrainHistory>) -> DrainResult {
        let mut joining = self.task.lock().await;
        if let DrainJoin::Running(task) = &mut *joining {
            let (result, task_failure) = match task.await {
                Ok(result) => (result, None),
                Err(source) => {
                    let source = Arc::new(Error::Facility {
                        name: "cell-node-drain-task",
                        source: Box::new(source),
                    });
                    record_failure(history, Arc::clone(&source));
                    self.returned.send_replace(Some(Err(Arc::clone(&source))));
                    (Err(Arc::clone(&source)), Some(source))
                }
            };
            // No await after consuming the handle. A cancelled waiter leaves
            // the original handle in Running; later callers join it in place.
            *joining = DrainJoin::Finished {
                result,
                task_failure,
            };
            self.joined.store(true, Ordering::Release);
        }
        match &*joining {
            DrainJoin::Finished { task_failure, .. } => match task_failure {
                Some(source) => Err(Arc::clone(source)),
                None => Ok(()),
            },
            DrainJoin::Running(_) => {
                Err(Arc::new(Error::Control("CellNode drain join incomplete")))
            }
        }
    }

    async fn result(&self) -> cellule_runtime::Result<DrainResult> {
        match &*self.task.lock().await {
            DrainJoin::Finished { result, .. } => Ok(result.clone()),
            DrainJoin::Running(_) => Err(Error::Control("CellNode drain result is unjoined")),
        }
    }
}

fn record_failure(history: &Mutex<DrainHistory>, source: Arc<Error>) {
    // History contains diagnostics only. Poison recovery preserves the known
    // original failure and does not confer authority or declare shutdown safe.
    let mut history = match history.lock() {
        Ok(history) => history,
        Err(poisoned) => poisoned.into_inner(),
    };
    history.record(source);
}

fn shared_error(source: Arc<Error>) -> Error {
    let name = match source.as_ref() {
        Error::Facility { name, .. } => *name,
        Error::Control(message) => return Error::Control(message),
        _ => "cell-node-drain",
    };
    Error::Facility {
        name,
        source: Box::new(RetainedHostFailure(source)),
    }
}

#[derive(Debug)]
struct RetainedHostFailure(Arc<Error>);
impl std::fmt::Display for RetainedHostFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The outer error already retains the original facility name. Keep
        // its presentation while preserving the complete original source.
        match self.0.as_ref() {
            Error::Facility { source, .. } => std::fmt::Display::fmt(source, formatter),
            source => std::fmt::Display::fmt(source, formatter),
        }
    }
}
impl std::error::Error for RetainedHostFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
