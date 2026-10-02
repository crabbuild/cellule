//! Retain the one supervisor join across cancelled waiters and host deadlines.
use super::*;
use observation::{SharedFailure, SupervisorProgress};
use requests::RotationRequests;

type SupervisorFuture = Pin<Box<dyn Future<Output = Result<(), SharedFailure>> + Send>>;
enum SupervisorJoin {
    Unstarted(Option<SupervisorFuture>),
    Running(JoinHandle<Result<(), SharedFailure>>),
    Finished(std::result::Result<(), SharedFailure>),
}

pub(crate) struct DurabilitySupervisor {
    pub(crate) requests: Arc<RotationRequests>,
    pub(crate) cancellation: CancellationToken,
    join: tokio::sync::Mutex<SupervisorJoin>,
    application: ApplicationId,
    session: SessionId,
    progress: Arc<SupervisorProgress>,
}

impl DurabilitySupervisor {
    pub(crate) fn new<P: NodeDurabilityProvider>(
        provider: Arc<P>,
        runtime: CellRuntime,
        configuration: NodeDurabilitySupervisorConfig,
        session: SessionId,
        cancellation: CancellationToken,
    ) -> cellule_runtime::Result<Self> {
        let requests = Arc::new(RotationRequests::new(configuration.application));
        let progress = Arc::new(SupervisorProgress::new(&runtime)?);
        let returned = Arc::clone(&progress);
        let task_requests = Arc::clone(&requests);
        let token = cancellation.clone();
        let future = Box::pin(async move {
            let result = run_node_durability_supervisor(
                provider,
                runtime,
                configuration,
                session,
                token,
                task_requests,
            )
            .await
            .map_err(SharedFailure::from);
            let captured = returned.returned(&result);
            match (result, captured) {
                (Err(source), _) => Err(source),
                (Ok(()), Err(error)) => Err(Arc::new(error) as SharedFailure),
                (Ok(()), Ok(())) => Ok(()),
            }
        });
        Ok(Self {
            requests,
            cancellation,
            join: tokio::sync::Mutex::new(SupervisorJoin::Unstarted(Some(future))),
            application: configuration.application,
            session,
            progress,
        })
    }
    pub(crate) fn observe(
        &self,
        now_ms: i64,
    ) -> cellule_runtime::Result<NodeDurabilitySupervisorObservation> {
        self.progress.capture(
            self.application,
            self.session,
            now_ms,
            self.cancellation.is_cancelled(),
            &self.requests,
        )
    }
    pub(crate) async fn join(&self) -> FacilityResult {
        let mut joining = self.join.lock().await;
        if let SupervisorJoin::Unstarted(future) = &mut *joining {
            let task = future.take().ok_or_else(|| {
                Box::new(Error::Control("node-log supervisor future missing"))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;
            *joining = if self.cancellation.is_cancelled() {
                drop(task);
                SupervisorJoin::Finished(Ok(()))
            } else {
                SupervisorJoin::Running(self.progress.start(task)?)
            };
        }
        if let SupervisorJoin::Running(task) = &mut *joining {
            let result = match task.await {
                Ok(result) => result,
                Err(source) => Err(Arc::new(source) as SharedFailure),
            };
            // No await after a terminal join: a cancelled join waiter leaves
            // the same handle in Running; every later caller joins it in place.
            *joining = SupervisorJoin::Finished(result);
        }
        match &*joining {
            SupervisorJoin::Finished(result) => {
                // Commit the consumed join before bookkeeping. A stop failure
                // cannot leave a Ready JoinHandle to be polled a second time.
                let stopped = self.requests.stop(result.as_ref().err().cloned());
                let stop_error = stopped.err().map(Arc::new);
                let captured = self.progress.joined(result, stop_error);
                if let Some(source) = result.as_ref().err() {
                    return Err(Box::new(SharedSupervisorError(Arc::clone(source))));
                }
                match captured? {
                    Some(source) => Err(Box::new(SharedSupervisorError(source))),
                    None => Ok(()),
                }
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
