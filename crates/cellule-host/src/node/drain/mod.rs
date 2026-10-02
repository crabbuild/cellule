//! One canonical resource sequence, retained by the original host closing task.
use super::*;
mod owner;
pub(crate) use owner::DrainOwner;

pub(super) struct DrainResources {
    runtime: CellRuntime,
    state: Arc<Mutex<NodeState>>,
    facilities: Arc<Mutex<Vec<CellNodeFacility>>>,
    task_group: Arc<Mutex<Option<Arc<CellNodeTaskGroup>>>>,
    runtime_drain: tokio::sync::Mutex<RuntimeDrain>,
    boot_withdrawal: Mutex<Option<Arc<crate::fleet::withdrawal::FleetBootWithdrawal>>>,
}

#[derive(Default)]
enum RuntimeDrain {
    #[default]
    Idle,
    Running(JoinHandle<cellule_runtime::Result<()>>),
    Finished(Result<(), Arc<Error>>),
}

#[derive(Debug)]
struct RetainedDrainFailure(Arc<Error>);

impl std::fmt::Display for RetainedDrainFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for RetainedDrainFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl DrainResources {
    pub(super) async fn run(&self, deadline: Option<Instant>) -> cellule_runtime::Result<()> {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
            if *state == NodeState::Stopped {
                return Ok(());
            }
            *state = NodeState::Draining;
        }
        let task_group = self
            .task_group
            .lock()
            .map(|task_group| task_group.clone())
            .map_err(|_| Error::Control("CellNode task group lock poisoned"));
        let facilities = self
            .facilities
            .lock()
            .map(|facilities| {
                facilities
                    .iter()
                    .rev()
                    .map(|facility| (facility.name, Arc::clone(&facility.drain)))
                    .collect::<Vec<_>>()
            })
            .map_err(|_| Error::Control("CellNode facility lock poisoned"));
        let mut first_error = None;
        if let Some(error) = task_group.as_ref().err().map(|error| match error {
            Error::Control(message) => Error::Control(message),
            _ => Error::Control("CellNode task group unavailable during drain"),
        }) {
            first_error = Some(error);
        }
        if let Ok(Some(task_group)) = task_group.as_ref() {
            task_group.cancel_work();
        }
        match facilities {
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
            Ok(facilities) => {
                for (name, drain) in facilities {
                    let result = if name == "cell-coordination-tasks" {
                        // The task group is a retained owner, so its join must share the
                        // node deadline; an unbounded callback could strand shutdown.
                        match task_group.as_ref() {
                            Ok(Some(task_group)) => task_group.drain_work_until(deadline).await,
                            _ => drain().await,
                        }
                    } else {
                        match deadline {
                            Some(deadline) => {
                                match tokio::time::timeout_at(deadline.into(), drain()).await {
                                    Ok(result) => result,
                                    Err(_) => Err(Box::new(std::io::Error::new(
                                        std::io::ErrorKind::TimedOut,
                                        "CellNode facility drain deadline exceeded",
                                    ))
                                        as Box<dyn std::error::Error + Send + Sync>),
                                }
                            }
                            None => drain().await,
                        }
                    };
                    if let Err(source) = result
                        && first_error.is_none()
                    {
                        first_error = Some(Error::Facility { name, source });
                    }
                }
            }
        }
        let runtime_result = match deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline.into(), self.join_runtime_drain()).await {
                    Ok(result) => result,
                    Err(_) => Err(Error::Control("CellNode runtime drain deadline exceeded")),
                }
            }
            None => self.join_runtime_drain().await,
        };
        let runtime_closed = runtime_result.is_ok();
        if first_error.is_none() {
            first_error = runtime_result.err();
        }
        // Session withdrawal fences log authority. An owned enrollment may
        // still return a committed generation after runtime closure, so retain
        // lease maintenance until every required facility join also succeeds.
        if runtime_closed
            && first_error.is_none()
            && let Ok(Some(task_group)) = task_group
            && let Err(source) = task_group.drain_until(deadline).await
            && first_error.is_none()
        {
            first_error = Some(Error::Facility {
                name: "cell-coordination-tasks",
                source,
            });
        }
        let result = match first_error {
            Some(error) => Err(error),
            None => self.withdraw_boot(deadline).await,
        };
        let result = if result.is_ok() {
            match self.facilities.lock() {
                Ok(mut facilities) => {
                    facilities.clear();
                    Ok(())
                }
                Err(_) => Err(Error::Control("CellNode facility lock poisoned")),
            }
        } else {
            result
        };
        if result.is_ok()
            && let Ok(mut state) = self.state.lock()
        {
            *state = NodeState::Stopped;
        }
        result
    }

    async fn withdraw_boot(&self, deadline: Option<Instant>) -> cellule_runtime::Result<()> {
        let withdrawal = self
            .boot_withdrawal
            .lock()
            .map_err(|_| Error::Control("CellNode boot withdrawal lock poisoned"))?
            .clone();
        let Some(withdrawal) = withdrawal else {
            return Ok(());
        };
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), withdrawal.withdraw())
                .await
                .map_err(|source| Error::Facility {
                    name: "fleet-boot-withdrawal-deadline",
                    source: Box::new(source),
                })?,
            None => withdrawal.withdraw().await,
        }
    }

    async fn join_runtime_drain(&self) -> cellule_runtime::Result<()> {
        let mut drain = self.runtime_drain.lock().await;
        if matches!(*drain, RuntimeDrain::Idle) {
            let runtime = self.runtime.clone();
            // The canonical runtime barrier is invoked once. A caller deadline
            // drops only this join waiter; the host retains the task and result.
            *drain = RuntimeDrain::Running(tokio::spawn(async move { runtime.shutdown().await }));
        }
        let result = match &mut *drain {
            RuntimeDrain::Running(task) => {
                let result = match task.await {
                    Ok(result) => result.map_err(Arc::new),
                    Err(source) => Err(Arc::new(Error::Facility {
                        name: "cell-runtime-task",
                        source: Box::new(source),
                    })),
                };
                *drain = RuntimeDrain::Finished(result.clone());
                result
            }
            RuntimeDrain::Finished(result) => result.clone(),
            RuntimeDrain::Idle => {
                return Err(Error::Control("CellNode runtime drain did not start"));
            }
        };
        result.map_err(|source| Error::Facility {
            name: "cell-runtime-drain",
            source: Box::new(RetainedDrainFailure(source)),
        })
    }
}
