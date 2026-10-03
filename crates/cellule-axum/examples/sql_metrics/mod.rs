//! Finite, nonblocking read-path measurements for this example application.
use cellule_runtime::fleet::telemetry::{
    CellTelemetry, PrimitiveOperationKind, PrimitiveOperationOutcome,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Default)]
pub(super) struct QueryMetrics {
    queries: AtomicU64,
    failures: AtomicU64,
    queue_ns: AtomicU64,
    worker_ns: AtomicU64,
    primitives: AtomicU64,
    primitive_ns: AtomicU64,
}

fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

impl CellTelemetry for QueryMetrics {
    fn query_execution(&self, queue: Duration, worker: Duration, succeeded: bool) {
        self.queue_ns.fetch_add(nanos(queue), Ordering::Relaxed);
        self.worker_ns.fetch_add(nanos(worker), Ordering::Relaxed);
        self.failures
            .fetch_add(u64::from(!succeeded), Ordering::Relaxed);
        self.queries.fetch_add(1, Ordering::Relaxed);
    }
    fn primitive_operation(
        &self,
        _module: &'static str,
        kind: PrimitiveOperationKind,
        _outcome: PrimitiveOperationOutcome,
        elapsed: Duration,
    ) {
        if kind == PrimitiveOperationKind::Query {
            self.primitive_ns
                .fetch_add(nanos(elapsed), Ordering::Relaxed);
            self.primitives.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl QueryMetrics {
    // Called after HTTP and runtime drain, when the totals are stable.
    pub(super) fn snapshot(&self) -> serde_json::Value {
        let queries = self.queries.load(Ordering::Relaxed);
        let primitives = self.primitives.load(Ordering::Relaxed);
        let mean_us = |total: &AtomicU64, count: u64| {
            if count == 0 {
                None
            } else {
                Some(total.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0)
            }
        };
        serde_json::json!({
            "queries": queries,
            "failures": self.failures.load(Ordering::Relaxed),
            "mean_actor_queue_us": mean_us(&self.queue_ns, queries),
            "mean_worker_round_trip_us": mean_us(&self.worker_ns, queries),
            "primitive_queries": primitives,
            "mean_primitive_query_us": mean_us(&self.primitive_ns, primitives)
        })
    }
}
