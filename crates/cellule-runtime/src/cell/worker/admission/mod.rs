//! A bounded pending descriptor does not own a native execution slot.

use super::*;
use crate::fleet::telemetry::SqlJobKind;
use std::{
    future::Future,
    task::{Context, Poll, Wake, Waker},
};

pub(super) struct ExecutionSlots {
    closed: Mutex<bool>,
    resources: ResourceLedger,
    pub(super) probe: slot::Probe,
    pub(super) permits: Vec<Arc<Semaphore>>,
    queued_permits: Vec<Arc<Semaphore>>,
}

pub(super) struct QueuedJob {
    requested: Option<u64>,
    kind: SqlJobKind,
    retained: ResourceReservation,
    queue: OwnedSemaphorePermit,
}

impl QueuedJob {
    pub(super) const fn retained_bytes(traced: bool) -> usize {
        // Request/result payloads retain their existing caller reservations.
        // Cover the command box and one linked FIFO node. Nodes free on pop;
        // no high-water buffer survives its last descriptor's charge. Request
        // and result payloads retain their existing caller reservations.
        2 * std::mem::size_of::<WorkerCommand>()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<Self>()
            + if traced {
                std::mem::size_of::<slot::JobTrace>()
            } else {
                0
            }
    }
}

impl ExecutionSlots {
    pub(super) fn new(workers: usize, resources: ResourceLedger) -> Self {
        Self {
            closed: Mutex::new(false),
            resources,
            probe: slot::Probe::new(workers),
            permits: (0..workers).map(|_| Arc::new(Semaphore::new(1))).collect(),
            queued_permits: (0..workers)
                .map(|_| Arc::new(Semaphore::new(WORKER_QUEUE)))
                .collect(),
        }
    }

    pub(super) async fn queued(&self, shard: usize, kind: SqlJobKind) -> Result<QueuedJob> {
        let requested = self.probe.request();
        let queue = self.queued_permits[shard]
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        let retained = self.resources.try_reserve(
            ResourceCost::zero()
                .with_retained_bytes(QueuedJob::retained_bytes(requested.is_some())),
        )?;
        Ok(QueuedJob {
            requested,
            kind,
            retained,
            queue,
        })
    }

    pub(super) fn finish(
        &self,
        permit: OwnedSemaphorePermit,
        requested: Option<u64>,
        shard: usize,
        kind: SqlJobKind,
        queued: Option<ResourceReservation>,
    ) -> Result<WorkerJobReservation> {
        let closed = self.closed.lock().map_err(|_| Error::RuntimeClosed)?;
        if *closed {
            return Err(Error::RuntimeClosed);
        }
        // Snapshot and native admission use the same close barrier and the
        // same one-per-shard permit. Queueing cannot enlarge the native cap.
        let trace_bytes = if requested.is_some() && queued.is_none() {
            std::mem::size_of::<slot::JobTrace>()
        } else {
            0
        };
        let reservation = self.resources.try_reserve(
            ResourceCost::zero()
                .with_worker_jobs(1)
                .with_retained_bytes(trace_bytes),
        )?;
        let trace = self.probe.acquired(requested, shard, kind)?;
        Ok(WorkerJobReservation {
            reservation: Some(reservation),
            queued,
            permit: Some(permit),
            trace,
        })
    }

    pub(super) fn close(&self) {
        // Cleanup must also wake parked native workers after lock poisoning.
        let mut closed = match self.closed.lock() {
            Ok(closed) => closed,
            Err(poisoned) => poisoned.into_inner(),
        };
        *closed = true;
        for permit in &self.permits {
            permit.close();
        }
        for permit in &self.queued_permits {
            permit.close();
        }
    }
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub(super) struct NativeAdmission {
    slots: Arc<ExecutionSlots>,
    shard: usize,
    waker: Waker,
}

impl NativeAdmission {
    pub(super) fn new(slots: Arc<ExecutionSlots>, shard: usize) -> Self {
        Self {
            slots,
            shard,
            waker: Waker::from(Arc::new(ThreadWake(std::thread::current()))),
        }
    }

    pub(super) fn reserve(
        &self,
        queued: QueuedJob,
        mut service_control: impl FnMut(&mut Context<'_>) -> bool,
    ) -> Result<WorkerJobReservation> {
        let permit = self.slots.permits[self.shard].clone().acquire_owned();
        let mut permit = std::pin::pin!(permit);
        let mut context = Context::from_waker(&self.waker);
        let permit = loop {
            // Service a finite burst before slot acquisition, including fences
            // sent while an immutable snapshot holds this worker's slot.
            let mut serviced = false;
            for _ in 0..16 {
                if !service_control(&mut context) {
                    break;
                }
                serviced = true;
            }
            match permit.as_mut().poll(&mut context) {
                Poll::Ready(result) => break result.map_err(|_| Error::RuntimeClosed)?,
                // Thread::unpark retains a token when release races this park.
                // Only this OS worker parks; the Tokio runtime stays available.
                Poll::Pending if serviced => continue,
                Poll::Pending => std::thread::park(),
            }
        };
        // Pending admission ends at execution; retained bytes follow completion.
        drop(queued.queue);
        self.slots.finish(
            permit,
            queued.requested,
            self.shard,
            queued.kind,
            Some(queued.retained),
        )
    }
}

impl WorkerCommand {
    pub(super) fn is_abandoned(&self) -> bool {
        // Direct pool callers can abandon pending admission. Actor-owned
        // operations keep their internal receiver through reconciliation,
        // even after the author has stopped waiting for the final response.
        match self {
            Self::Execute { reply, .. } | Self::DeliverEffect { reply, .. } => reply.is_closed(),
            Self::ExecuteGroup { reply, .. } => reply.is_closed(),
            Self::Migrate { reply, .. } => reply.is_closed(),
            Self::Query { reply, .. } => reply.is_closed(),
            Self::PrepareHydration { reply, .. } => reply.is_closed(),
            Self::InstallHydration { reply, .. } => reply.is_closed(),

            Self::TransferWork { reply, .. } => reply.is_closed(),
            Self::Resolve { reply, .. } | Self::ResolveEffect { reply, .. } => reply.is_closed(),
            _ => false,
        }
    }

    pub(super) fn reject_admission(self, error: Error) {
        // Preserve the source admission error through the existing result
        // channel; dropping its sender would replace it with RuntimeClosed.
        match self {
            Self::Queued { command, .. } | Self::Reserved { command, .. } => {
                command.reject_admission(error)
            }
            Self::Activate { reply, .. }
            | Self::ActivateRestored { reply, .. }
            | Self::BindPrepared { reply, .. }
            | Self::BindMigrationPrepared { reply, .. }
            | Self::ConfirmDurable { reply, .. }
            | Self::ConfirmBootstrapPublished { reply, .. }
            | Self::Fence { reply, .. }
            | Self::Deactivate { reply, .. }
            | Self::DeactivateResumable { reply, .. }
            | Self::Discard { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Bootstrap(bootstrap) => {
                let _ = bootstrap.reply.send(Err(error));
            }
            Self::Execute { reply, .. } | Self::DeliverEffect { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ExecuteGroup { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::FleetInventory { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Migrate { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Query { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::PrepareHydration { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::InstallHydration { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Hydration { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::TransferWork { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Resolve { reply, .. } | Self::ResolveEffect { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::BindPreparedAll { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ConfirmPublishedRange { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Pending { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ConfirmPublished { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ConfirmMigrationPublished { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::State { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::InterruptHandle { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

#[cfg(test)]
mod tests;
