//! One admission word and original native owner shared by every store capability.
use super::*;
use std::{
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, oneshot};

const CLOSED: usize = 1 << (usize::BITS - 1);
const MAX_JOBS: usize = 64;

/// Local closure of one original artifact-store owner. This does not prove
/// global Blob references, remote success, retention pins or Cell authority.
#[derive(Clone, Debug)]
pub struct BlobArtifactLifecycleObservation {
    closed: bool,
    accepted_jobs: usize,
    first_failure: Option<Arc<Error>>,
}
impl BlobArtifactLifecycleObservation {
    /// Whether the original shared admission word is irreversibly closed.
    #[must_use]
    pub const fn admission_closed(&self) -> bool {
        self.closed
    }
    /// Original native jobs still running, including cancelled callers' work.
    #[must_use]
    pub const fn accepted_jobs(&self) -> usize {
        self.accepted_jobs
    }
    /// Closed plus zero is stable; operation success still needs its result.
    #[must_use]
    pub const fn locally_joined(&self) -> bool {
        self.closed && self.accepted_jobs == 0
    }
    /// Original first native failure, retained after waiter loss and closure.
    /// This diagnostic cannot classify an ambiguous remote result as absent.
    #[must_use]
    pub fn first_failure(&self) -> Option<&Arc<Error>> {
        self.first_failure.as_ref()
    }
}

#[derive(Default)]
pub(super) struct ArtifactLifetime {
    state: AtomicUsize,
    changed: Notify,
    first_failure: Mutex<Option<Arc<Error>>>,
}
struct Accepted(Arc<ArtifactLifetime>);
impl Drop for Accepted {
    fn drop(&mut self) {
        self.0.state.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
impl ArtifactLifetime {
    fn accept(self: &Arc<Self>) -> Result<Accepted> {
        self.state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & CLOSED == 0 && state < MAX_JOBS).then_some(state + 1)
            })
            .map_err(|state| {
                if state & CLOSED != 0 {
                    Error::CellDraining
                } else {
                    Error::Capacity("Blob artifact jobs")
                }
            })?;
        Ok(Accepted(self.clone()))
    }
    pub(super) fn close(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
    }
    pub(super) async fn join(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.load(Ordering::Acquire) & !CLOSED == 0 {
                return;
            }
            changed.await;
        }
    }
    pub(super) fn observe(&self) -> Result<BlobArtifactLifecycleObservation> {
        // Closed-plus-zero acquires every final result write before reading
        // diagnostics; reading the failure first could miss the last job's error.
        let state = self.state.load(Ordering::Acquire);
        let first_failure = self
            .first_failure
            .lock()
            .map_err(|_| Error::Control("Blob artifact failure lock poisoned"))?
            .clone();
        Ok(BlobArtifactLifecycleObservation {
            closed: state & CLOSED != 0,
            accepted_jobs: state & !CLOSED,
            first_failure,
        })
    }
    pub(super) fn retain_failure(&self, error: Error) -> Error {
        let original = match error {
            Error::Shared(original) => original,
            error => Arc::new(error),
        };
        match self.first_failure.lock() {
            Ok(mut first) => {
                first.get_or_insert_with(|| original.clone());
            }
            Err(poisoned) => {
                poisoned
                    .into_inner()
                    .get_or_insert_with(|| original.clone());
            }
        }
        Error::Shared(original)
    }
    pub(super) async fn run<F, T>(self: &Arc<Self>, work: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let runtime = tokio::runtime::Handle::try_current().map_err(Error::RuntimeStart)?;
        let accepted = self.accept()?;
        let (reply, waiter) = oneshot::channel();
        // The retained supervisor joins the original native task even if its
        // caller cancels. This also preserves a provider panic's JoinError.
        runtime.spawn(async move {
            let result = match tokio::spawn(work).await {
                Ok(result) => result,
                Err(source) => Err(Error::Facility {
                    name: "blob-artifacts",
                    source: Box::new(source),
                }),
            };
            let result = result.map_err(|error| accepted.0.retain_failure(error));
            let _ = reply.send(result);
            // Inputs, native I/O and undelivered output have all completed or
            // dropped before waking an irreversible closed-plus-zero waiter.
            drop(accepted);
        });
        waiter.await.map_err(|_| Error::RuntimeClosed)?
    }
}
