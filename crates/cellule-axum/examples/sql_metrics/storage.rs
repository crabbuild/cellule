//! Fixed object families chosen by the application, without identity labels.
use super::*;
use std::sync::Arc;

const FAMILIES: [&str; 6] = [
    "immutable",
    "cell_authority",
    "node_authority",
    "other",
    "owner_enrollment",
    "receiver_enrollment",
];

tokio::task_local! {
    static ENROLLMENT_FAMILY: usize;
}

pub(super) async fn enrollment<T>(
    receiver: bool,
    future: impl std::future::Future<Output = T>,
) -> T {
    ENROLLMENT_FAMILY
        .scope(if receiver { 5 } else { 4 }, future)
        .await
}

#[derive(Default)]
struct Counts {
    started: AtomicU64,
    outcomes: [AtomicU64; StorageOutcome::ALL.len()],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

struct Family {
    total: Arc<[StorageMetrics; StorageOperation::ALL.len()]>,
    counts: [Counts; StorageOperation::ALL.len()],
}

impl StorageObserver for Family {
    fn started(&self, operation: StorageOperation) {
        self.total[operation.index()]
            .started
            .fetch_add(1, Ordering::Relaxed);
        self.counts[operation.index()]
            .started
            .fetch_add(1, Ordering::Relaxed);
    }

    fn finished(&self, observation: StorageObservation) {
        record(&self.total[observation.operation.index()], observation);
        let counts = &self.counts[observation.operation.index()];
        counts.outcomes[observation.outcome.index()].fetch_add(1, Ordering::Relaxed);
        counts
            .bytes_read
            .fetch_add(observation.bytes_read, Ordering::Relaxed);
        counts
            .bytes_written
            .fetch_add(observation.bytes_written, Ordering::Relaxed);
    }
}

pub(super) struct Accounting {
    pub total: Arc<[StorageMetrics; StorageOperation::ALL.len()]>,
    families: [Arc<Family>; FAMILIES.len()],
}

impl Default for Accounting {
    fn default() -> Self {
        let total = Arc::new(std::array::from_fn(|_| StorageMetrics::default()));
        Self {
            families: std::array::from_fn(|_| {
                Arc::new(Family {
                    total: total.clone(),
                    counts: std::array::from_fn(|_| Counts::default()),
                })
            }),
            total,
        }
    }
}

impl Accounting {
    pub fn started(&self, operation: StorageOperation) {
        self.families[3].started(operation);
    }
    pub fn finished(&self, observation: StorageObservation) {
        self.families[3].finished(observation);
    }

    pub fn observer(
        &self,
        location: Option<&object_store::path::Path>,
    ) -> Arc<dyn StorageObserver> {
        let index = location.map_or(3, |path| {
            let name = path.as_ref();
            if [".ltx", ".index", ".dir", ".root", ".bundle", ".pack"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
            {
                0
            } else if name
                .split('/')
                .any(|part| matches!(part, "nodes" | "node-logs"))
            {
                // Select once before dispatch. The returned observer owns the
                // role through streamed completion, beyond this task scope.
                ENROLLMENT_FAMILY.try_with(|index| *index).unwrap_or(2)
            } else if name.split('/').any(|part| part == "cells") {
                1
            } else {
                3
            }
        });
        self.families[index].clone()
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let families: serde_json::Map<String, serde_json::Value> = FAMILIES
            .iter()
            .zip(&self.families)
            .map(|(label, family)| {
                let operations: serde_json::Map<String, serde_json::Value> = StorageOperation::ALL
                    .iter()
                    .map(|operation| {
                        let counts = &family.counts[operation.index()];
                        let outcomes: serde_json::Map<String, serde_json::Value> =
                            StorageOutcome::ALL
                                .iter()
                                .map(|outcome| {
                                    (
                                        outcome.label().into(),
                                        counts.outcomes[outcome.index()]
                                            .load(Ordering::Relaxed)
                                            .into(),
                                    )
                                })
                                .collect();
                        (
                            operation.label().into(),
                            serde_json::json!({
                                "started": counts.started.load(Ordering::Relaxed),
                                "outcomes": outcomes,
                                "bytes_read": counts.bytes_read.load(Ordering::Relaxed),
                                "bytes_written": counts.bytes_written.load(Ordering::Relaxed),
                            }),
                        )
                    })
                    .collect();
                ((*label).into(), operations.into())
            })
            .collect();
        families.into()
    }
}

pub(super) fn record(metrics: &StorageMetrics, observation: StorageObservation) {
    metrics.duration.observe(observation.duration);
    metrics.outcomes[observation.outcome.index()].fetch_add(1, Ordering::Relaxed);
    metrics
        .bytes_read
        .fetch_add(observation.bytes_read, Ordering::Relaxed);
    metrics
        .bytes_written
        .fetch_add(observation.bytes_written, Ordering::Relaxed);
}
