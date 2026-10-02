//! Retain the one supervisor join across cancelled waiters and host deadlines.
use super::*;
use requests::RotationRequests;

type SharedFailure = Arc<dyn std::error::Error + Send + Sync>;
type SupervisorFuture = Pin<Box<dyn Future<Output = FacilityResult> + Send>>;
enum SupervisorJoin {
    Unstarted(Option<SupervisorFuture>),
    Running(JoinHandle<FacilityResult>),
    Finished(std::result::Result<(), SharedFailure>),
}

pub(crate) struct DurabilitySupervisor {
    pub(crate) requests: Arc<RotationRequests>,
    pub(crate) cancellation: CancellationToken,
    join: tokio::sync::Mutex<SupervisorJoin>,
}

impl DurabilitySupervisor {
    pub(crate) fn new<P: NodeDurabilityProvider>(
        provider: Arc<P>,
        runtime: CellRuntime,
        configuration: NodeDurabilitySupervisorConfig,
        session: SessionId,
        cancellation: CancellationToken,
    ) -> Self {
        let requests = Arc::new(RotationRequests::new(configuration.application));
        let task_requests = Arc::clone(&requests);
        let token = cancellation.clone();
        let future = Box::pin(async move {
            run_node_durability_supervisor(
                provider,
                runtime,
                configuration,
                session,
                token,
                task_requests,
            )
            .await
        });
        Self {
            requests,
            cancellation,
            join: tokio::sync::Mutex::new(SupervisorJoin::Unstarted(Some(future))),
        }
    }
    pub(crate) async fn join(&self) -> FacilityResult {
        let mut joining = self.join.lock().await;
        if let SupervisorJoin::Unstarted(future) = &mut *joining {
            let task = future.take().ok_or_else(|| {
                Box::new(Error::Control("node-log supervisor future missing"))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;
            *joining = if self.cancellation.is_cancelled() {
                self.requests.stop(None)?;
                drop(task);
                SupervisorJoin::Finished(Ok(()))
            } else {
                SupervisorJoin::Running(tokio::spawn(task))
            };
        }
        if let SupervisorJoin::Running(task) = &mut *joining {
            let result = match task.await {
                Ok(result) => result.map_err(SharedFailure::from),
                Err(source) => Err(Arc::new(source) as SharedFailure),
            };
            // No await after a terminal join: a cancelled join waiter leaves
            // the same handle in Running; every later caller joins it in place.
            self.requests.stop(result.as_ref().err().cloned())?;
            *joining = SupervisorJoin::Finished(result);
        }
        match &*joining {
            SupervisorJoin::Finished(Ok(())) => Ok(()),
            SupervisorJoin::Finished(Err(source)) => {
                Err(Box::new(SharedSupervisorError(Arc::clone(source))))
            }
            _ => Err(Box::new(Error::Control(
                "node-log supervisor join incomplete",
            ))),
        }
    }
    pub(crate) async fn drain(&self) -> FacilityResult {
        self.cancellation.cancel();
        self.join().await
    }
}
impl Drop for DurabilitySupervisor {
    fn drop(&mut self) {
        if let SupervisorJoin::Running(task) = self.join.get_mut() {
            task.abort();
        }
    }
}
#[derive(Debug)]
struct SharedSupervisorError(SharedFailure);
impl std::fmt::Display for SharedSupervisorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedSupervisorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
