//! Fixed-size observation of the original supervisor and bounded rotation bank.
use super::*;
use tokio::task::AbortHandle;

pub(super) type SharedFailure = Arc<dyn std::error::Error + Send + Sync>;

/// Lifecycle of the one retained durability supervisor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeDurabilitySupervisorState {
    /// The retained future has not been started or joined.
    NotStarted,
    /// The original task is running, including accepted provider/native work.
    Running,
    /// The task finished without a returned result; its original join is required.
    FinishedUnobserved,
    /// The supervisor future returned; its task and request stop are not yet joined.
    Returned,
    /// The task was joined but stopping its request bank failed.
    JoinedUnsettled,
    /// The original task was joined and request-stop bookkeeping succeeded.
    Joined,
}

/// Original epoch and its local progress, without a new rotation request.
#[derive(Clone, Debug)]
pub struct NodeLogRotationEntry {
    /// Exact epoch named at original acceptance.
    pub epoch: u64,
    /// Original errors, retirement and replacement references at capture.
    pub progress: NodeLogRotationObservation,
}

/// The supervisor's existing bounded bank, including automatic rotation.
#[derive(Clone, Debug)]
pub struct NodeLogRotationInventory {
    /// New requests are permanently closed after supervisor join.
    pub stopped: bool,
    /// Claimed epoch, including automatic work without a requested receipt.
    pub running_epoch: Option<u64>,
    /// At most one original pending request; Interrupted does not prove absence.
    pub pending: Option<NodeLogRotationEntry>,
    /// At most one retained local completion; eviction does not prove settlement.
    pub completed: Option<NodeLogRotationEntry>,
}

/// Local interval observation; it cannot certify role absence or fleet completion.
///
/// Capture only clones fixed metadata and original Arcs. It never awaits the
/// supervisor's join lane, provider I/O or native retirement. The installed
/// owner retains four KiB of metadata admission until that owner is released.
/// Returned is distinct from Joined, and a joined error remains visible.
#[derive(Clone, Debug)]
pub struct NodeDurabilitySupervisorObservation {
    /// Compiled application bound at installation.
    pub application: ApplicationId,
    /// Exact original host boot session.
    pub session: SessionId,
    /// Supplied local observation time, without restamping remote evidence.
    pub observed_at_ms: i64,
    /// Work cancellation was requested; accepted calls may still be running.
    pub cancellation_requested: bool,
    /// Original supervisor lifecycle, independent of producer row counts.
    pub state: NodeDurabilitySupervisorState,
    /// Original returned or task-join failure, preserved across repeated captures.
    pub supervisor_error: Option<Arc<dyn std::error::Error + Send + Sync>>,
    /// Original request-stop failure, independently of the supervisor's result.
    pub requests_error: Option<Arc<Error>>,
    /// Every original bank entry, or an explicit capture error supplying no coverage.
    pub rotations: Result<NodeLogRotationInventory, Arc<Error>>,
}

struct Status {
    state: NodeDurabilitySupervisorState,
    task: Option<AbortHandle>,
    supervisor_error: Option<SharedFailure>,
    requests_error: Option<Arc<Error>>,
}

pub(super) struct SupervisorProgress {
    status: Mutex<Status>,
    _bytes: cellule_runtime::cell::actor::NodeByteReservation,
}

impl SupervisorProgress {
    pub(super) fn new(runtime: &CellRuntime) -> cellule_runtime::Result<Self> {
        Ok(Self {
            status: Mutex::new(Status {
                state: NodeDurabilitySupervisorState::NotStarted,
                task: None,
                supervisor_error: None,
                requests_error: None,
            }),
            _bytes: runtime.try_reserve_node_metadata_bytes(4 * 1024)?,
        })
    }

    fn lock(&self) -> cellule_runtime::Result<std::sync::MutexGuard<'_, Status>> {
        self.status
            .lock()
            .map_err(|_| Error::Control("node-log supervisor observation lock poisoned"))
    }

    pub(super) fn start(
        &self,
        future: Pin<Box<dyn Future<Output = Result<(), SharedFailure>> + Send>>,
    ) -> cellule_runtime::Result<JoinHandle<Result<(), SharedFailure>>> {
        let mut status = self.lock()?;
        if status.state != NodeDurabilitySupervisorState::NotStarted {
            return Err(Error::Control("node-log supervisor already started"));
        }
        // Hold only this short metadata lock through spawn. A task that returns
        // immediately cannot publish Returned before Running overwrites it.
        let task = tokio::spawn(future);
        status.task = Some(task.abort_handle());
        status.state = NodeDurabilitySupervisorState::Running;
        Ok(task)
    }

    pub(super) fn returned(
        &self,
        result: &Result<(), SharedFailure>,
    ) -> cellule_runtime::Result<()> {
        let mut status = self.lock()?;
        status.state = NodeDurabilitySupervisorState::Returned;
        status.supervisor_error = result.as_ref().err().cloned();
        Ok(())
    }

    pub(super) fn joined(
        &self,
        result: &Result<(), SharedFailure>,
        requests_error: Option<Arc<Error>>,
    ) -> cellule_runtime::Result<Option<Arc<Error>>> {
        let mut status = self.lock()?;
        status.supervisor_error = result.as_ref().err().cloned();
        let failed = requests_error.is_some();
        if let Some(error) = requests_error {
            status.requests_error.get_or_insert(error);
            status.state = NodeDurabilitySupervisorState::JoinedUnsettled;
        } else {
            status.state = NodeDurabilitySupervisorState::Joined;
        }
        Ok(failed.then(|| status.requests_error.clone()).flatten())
    }

    pub(super) fn capture(
        &self,
        application: ApplicationId,
        session: SessionId,
        now_ms: i64,
        cancellation_requested: bool,
        requests: &requests::RotationRequests,
    ) -> cellule_runtime::Result<NodeDurabilitySupervisorObservation> {
        let status = self.lock()?;
        // Status precedes the bank; join releases the bank before publishing
        // Joined. A Joined capture therefore cannot carry pre-stop bank state.
        let rotations = requests.inventory().map_err(|error| {
            status
                .requests_error
                .clone()
                .unwrap_or_else(|| Arc::new(error))
        });
        let state = if status.state == NodeDurabilitySupervisorState::Running
            && status.task.as_ref().is_some_and(AbortHandle::is_finished)
        {
            NodeDurabilitySupervisorState::FinishedUnobserved
        } else {
            status.state
        };
        Ok(NodeDurabilitySupervisorObservation {
            application,
            session,
            observed_at_ms: now_ms,
            cancellation_requested,
            state,
            supervisor_error: status.supervisor_error.clone(),
            requests_error: status.requests_error.clone(),
            rotations,
        })
    }
}

#[cfg(test)]
mod tests;
