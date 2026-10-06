//! Optional monotonic phase stamps shared with the actor's one reply gate.

use crate::CellId;
use crate::fleet::resource::{ResourceCost, ResourceLedger, ResourceReservation};
use crate::fleet::telemetry::{CellTelemetryHandle, QueryActorState, QueryTiming};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(crate) struct QueryTrace {
    origin: Instant,
    telemetry: CellTelemetryHandle,
    ingress: AtomicU64,
    dispatch: AtomicU64,
    actor_state: AtomicU64,
    actor: AtomicU64,
    admission: AtomicU64,
    dequeue: AtomicU64,
    completion: AtomicU64,
    job_id: AtomicU64,
    _retained: ResourceReservation,
}

impl QueryTrace {
    pub(crate) fn new(
        telemetry: &CellTelemetryHandle,
        resources: &ResourceLedger,
    ) -> crate::Result<Option<Arc<Self>>> {
        if !telemetry.is_enabled() {
            return Ok(None);
        }
        // Charge the shared payload and Arc counters through the last worker
        // reference, even if the caller has already received a deadline reply.
        let retained =
            resources.try_reserve(ResourceCost::zero().with_retained_bytes(
                std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
            ))?;
        Ok(Some(Arc::new(Self {
            origin: Instant::now(),
            telemetry: telemetry.clone(),
            ingress: AtomicU64::new(0),
            dispatch: AtomicU64::new(0),
            actor_state: AtomicU64::new(0),
            actor: AtomicU64::new(0),
            admission: AtomicU64::new(0),
            dequeue: AtomicU64::new(0),
            completion: AtomicU64::new(0),
            job_id: AtomicU64::new(0),
            _retained: retained,
        })))
    }

    fn stamp(&self, target: &AtomicU64) {
        // Zero is absent; elapsed + 1 represents a reached boundary, even in
        // the first nanosecond. SQL deadlines are far below this saturation.
        let nanos = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        target.store(nanos.saturating_add(1), Ordering::Release);
    }

    pub(crate) fn started(&self) {
        self.stamp(&self.actor);
    }
    pub(crate) fn received(&self) {
        self.stamp(&self.ingress);
    }
    pub(crate) fn enqueued(&self, state: QueryActorState) {
        let state = match state {
            QueryActorState::Ready => 1,
            QueryActorState::Renewal => 2,
            QueryActorState::Busy => 3,
            QueryActorState::Queued => 4,
            QueryActorState::Inventory => 5,
        };
        self.actor_state.store(state, Ordering::Release);
    }
    pub(crate) fn dispatched(&self) {
        self.stamp(&self.dispatch);
    }
    pub(crate) fn admitted(&self, job_id: Option<u64>) {
        self.job_id.store(job_id.unwrap_or(0), Ordering::Release);
        self.stamp(&self.admission);
    }
    pub(crate) fn dequeued(&self) {
        self.stamp(&self.dequeue);
    }
    pub(crate) fn completed(&self) {
        self.stamp(&self.completion);
    }

    pub(crate) fn snapshot(&self, succeeded: bool) -> QueryTiming {
        // Read later boundaries first: observing completion also observes all
        // earlier Release stores. A deadline may race an unfinished callback;
        // absent boundaries stay absent and never masquerade as zero-duration.
        let load = |value: &AtomicU64| {
            value
                .load(Ordering::Acquire)
                .checked_sub(1)
                .map(Duration::from_nanos)
        };
        let completion = load(&self.completion);
        let dequeue = load(&self.dequeue);
        let admission = load(&self.admission);
        let actor = load(&self.actor);
        let dispatch = load(&self.dispatch);
        let ingress = load(&self.ingress);
        let total = self.origin.elapsed();
        let between = |start: Option<Duration>, end: Option<Duration>| {
            start
                .zip(end)
                .and_then(|(start, end)| end.checked_sub(start))
        };
        QueryTiming {
            // Admission's release store publishes the ID. A deadline can
            // snapshot between those stores; only bind a reached boundary.
            job_id: admission.and_then(|_| match self.job_id.load(Ordering::Acquire) {
                0 => None,
                id => Some(id),
            }),
            actor_queue: actor,
            actor_ingress: ingress,
            cell_queue: between(ingress, dispatch),
            task_start: between(dispatch, actor),
            actor_state: match self.actor_state.load(Ordering::Acquire) {
                1 => Some(QueryActorState::Ready),
                2 => Some(QueryActorState::Renewal),
                3 => Some(QueryActorState::Busy),
                4 => Some(QueryActorState::Queued),
                5 => Some(QueryActorState::Inventory),
                _ => None,
            },
            worker_admission: between(actor, admission),
            worker_queue: between(admission, dequeue),
            execution: between(dequeue, completion),
            reply_queue: completion.and_then(|completion| total.checked_sub(completion)),
            total,
            succeeded,
            delivered: false,
        }
    }

    pub(crate) fn emit(&self, cell: CellId, timing: QueryTiming) {
        self.telemetry.query_completed(cell, timing);
    }
}

#[cfg(test)]
mod tests;
