//! Tasks internals for the Cell node host.

use super::*;

struct AbortOnDrop<T> {
    pub(super) handle: JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    pub(super) fn new(handle: JoinHandle<T>) -> Self {
        Self { handle }
    }

    pub(super) async fn join(&mut self) -> std::result::Result<T, tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Bounded task supervisor owned by a [`CellNode`] facility.
pub struct CellNodeTaskGroup {
    pub(super) cancellation: CancellationToken,
    pub(super) node_shutdown: CancellationToken,
    tasks: Mutex<Vec<Arc<NodeTask>>>,
    pub(super) failed: Arc<AtomicBool>,
    pub(super) draining: AtomicBool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskPhase {
    Work,
    RetainedWork,
    Lease,
}

struct NodeTask {
    phase: TaskPhase,
    abort: tokio::task::AbortHandle,
    supervisor: tokio::task::AbortHandle,
    join: tokio::sync::Mutex<TaskJoin>,
}

type TaskFailure = Arc<dyn std::error::Error + Send + Sync>;

enum TaskJoin {
    Running(JoinHandle<FacilityResult>),
    Finished(std::result::Result<(), TaskFailure>),
}

impl NodeTask {
    async fn join(&self) -> std::result::Result<(), TaskFailure> {
        let mut joining = self.join.lock().await;
        if let TaskJoin::Running(handle) = &mut *joining {
            let result = match handle.await {
                Ok(result) => result.map_err(TaskFailure::from),
                Err(source) => Err(Arc::new(source) as TaskFailure),
            };
            // Commit the consumed handle without another await. Cancellation
            // before this point drops only the waiter and keeps the same join.
            *joining = TaskJoin::Finished(result);
        }
        match &*joining {
            TaskJoin::Finished(result) => result.clone(),
            TaskJoin::Running(_) => Err(Arc::new(Error::Control("node task join incomplete"))),
        }
    }
}

impl Drop for CellNodeTaskGroup {
    fn drop(&mut self) {
        let tasks = match self.tasks.lock() {
            Ok(tasks) => tasks,
            Err(poisoned) => poisoned.into_inner(),
        };
        for task in tasks.iter() {
            task.abort.abort();
            task.supervisor.abort();
        }
    }
}

impl CellNodeTaskGroup {
    pub(super) fn cancel_work(&self) {
        self.draining.store(true, Ordering::Release);
        self.cancellation.cancel();
    }

    /// Creates a task group whose cancellation tokens are controlled by the product host.
    #[must_use]
    pub fn new(cancellation: CancellationToken, node_shutdown: CancellationToken) -> Self {
        Self {
            cancellation,
            node_shutdown,
            tasks: Mutex::new(Vec::new()),
            failed: Arc::new(AtomicBool::new(false)),
            draining: AtomicBool::new(false),
        }
    }

    /// Returns the cancellation signal that stops owned tasks during node drain.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(super) fn is_healthy(&self) -> bool {
        if self.failed.load(Ordering::Acquire) || self.draining.load(Ordering::Acquire) {
            return false;
        }
        self.tasks
            .lock()
            .map(|tasks| tasks.iter().all(|task| !task.supervisor.is_finished()))
            .unwrap_or(false)
    }

    pub(super) fn ensure_accepting_tasks(&self) -> cellule_runtime::Result<()> {
        if self.draining.load(Ordering::Acquire) {
            return Err(Error::CellDraining);
        }
        Ok(())
    }

    /// Spawns one bounded node task and retains its join handle for drain.
    pub fn spawn<F, E>(&self, task: F) -> cellule_runtime::Result<()>
    where
        F: Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.spawn_task(
            async move {
                task.await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
            },
            TaskPhase::Work,
        )
    }

    /// Retains lease renewal until the node has drained its runtime and closed its log.
    ///
    /// This task must stop on the node-shutdown token rather than the work
    /// cancellation token. It shares the ordinary task limit and supervision.
    pub fn spawn_lease_maintenance<F, E>(&self, task: F) -> cellule_runtime::Result<()>
    where
        F: Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.spawn_task(
            async move {
                task.await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
            },
            TaskPhase::Lease,
        )
    }

    /// Spawns one task that already uses the node's boxed facility error type.
    pub fn spawn_boxed<F>(&self, task: F) -> cellule_runtime::Result<()>
    where
        F: Future<Output = FacilityResult> + Send + 'static,
    {
        self.spawn_task(task, TaskPhase::Work)
    }

    /// Watch an independently retained facility join without aborting its
    /// waiter on a deadline. The facility still owns and joins accepted work.
    pub(super) fn spawn_retained<F>(&self, task: F) -> cellule_runtime::Result<()>
    where
        F: Future<Output = FacilityResult> + Send + 'static,
    {
        self.spawn_task(task, TaskPhase::RetainedWork)
    }

    fn spawn_task<F>(&self, task: F, phase: TaskPhase) -> cellule_runtime::Result<()>
    where
        F: Future<Output = FacilityResult> + Send + 'static,
    {
        self.ensure_accepting_tasks()?;
        let mut tasks = self
            .tasks
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?;
        self.ensure_accepting_tasks()?;
        if tasks.len() >= MAX_NODE_TASKS {
            return Err(Error::Capacity("CellNode task limit reached"));
        }
        let failed = Arc::clone(&self.failed);
        let task = tokio::spawn(task);
        // Deadline abortion targets the work, not its supervisor. The latter
        // stays owned until it joins the work's cancellation and destructor.
        let abort = task.abort_handle();
        let handle = tokio::spawn(async move {
            let mut task = AbortOnDrop::new(task);
            match task.join().await {
                Ok(result) => {
                    if result.is_err() {
                        failed.store(true, Ordering::Release);
                    }
                    result
                }
                Err(error) => {
                    failed.store(true, Ordering::Release);
                    Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
                }
            }
        });
        tasks.push(Arc::new(NodeTask {
            phase,
            abort,
            supervisor: handle.abort_handle(),
            join: tokio::sync::Mutex::new(TaskJoin::Running(handle)),
        }));
        Ok(())
    }

    pub(super) async fn drain_work_until(&self, deadline: Option<Instant>) -> FacilityResult {
        self.cancel_work();
        self.join_until(deadline, false).await
    }

    /// Cancels admission and joins tasks in reverse registration order.
    ///
    /// Cancelling the caller leaves the original joins owned by this group.
    /// Every subsequent drain preserves original task failures as error sources.
    pub async fn drain(&self) -> FacilityResult {
        self.drain_until(None).await
    }

    /// Cancels admission and joins tasks until an optional absolute deadline.
    ///
    /// A deadline aborts unfinished ordinary tasks but retains their handles.
    /// A later drain joins those tasks and reports their original cancellation
    /// errors. Watchers of retained facility work continue until that join.
    pub async fn drain_until(&self, deadline: Option<Instant>) -> FacilityResult {
        self.cancel_work();
        self.node_shutdown.cancel();
        self.join_until(deadline, true).await
    }

    async fn join_until(&self, deadline: Option<Instant>, include_lease: bool) -> FacilityResult {
        let tasks = match self.tasks.lock() {
            // Keep the handles and settled results in the same bounded bank.
            // Multiple drain callers share each join rather than taking it away.
            Ok(tasks) => tasks
                .iter()
                .filter(|task| include_lease || task.phase != TaskPhase::Lease)
                .cloned()
                .collect::<Vec<_>>(),
            Err(poisoned) => {
                for task in poisoned.into_inner().iter() {
                    if task.phase != TaskPhase::RetainedWork
                        && (include_lease || task.phase != TaskPhase::Lease)
                    {
                        task.abort.abort();
                    }
                }
                return Err(Box::new(std::io::Error::other(
                    "CellNode task group lock poisoned",
                )));
            }
        };
        let mut first_error = None;
        for task in tasks.iter().rev() {
            let result = match deadline {
                Some(deadline) => {
                    match tokio::time::timeout_at(deadline.into(), task.join()).await {
                        Ok(result) => result,
                        Err(_) => {
                            // Preserve the existing deadline abort policy, but
                            // retain ownership until a later caller joins it.
                            for task in &tasks {
                                if task.phase != TaskPhase::RetainedWork {
                                    task.abort.abort();
                                }
                            }
                            first_error.get_or_insert_with(|| {
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "CellNode task group drain deadline exceeded",
                                ))
                                    as Box<dyn std::error::Error + Send + Sync>
                            });
                            break;
                        }
                    }
                }
                None => task.join().await,
            };
            if let Err(source) = result
                && first_error.is_none()
            {
                first_error = Some(Box::new(RetainedTaskFailure(source))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[derive(Debug)]
struct RetainedTaskFailure(TaskFailure);

impl std::fmt::Display for RetainedTaskFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for RetainedTaskFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
