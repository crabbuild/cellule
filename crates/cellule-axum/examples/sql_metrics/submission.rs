//! Fixed phases for the same successfully assigned native capture cohort.
use super::*;

const LABELS: [&str; 8] = [
    "submission_validation",
    "submission_native_bytes",
    "submission_shipping_slot",
    "submission_local_load",
    "submission_ordered_lane",
    "submission_publication_slot",
    "submission_assignment",
    "submission_total",
];

#[derive(Default)]
pub(super) struct SubmissionMetrics {
    successes: AtomicU64,
    failures: AtomicU64,
    cancelled: AtomicU64,
    frames: AtomicU64,
    bytes: AtomicU64,
    phases: [Histogram; LABELS.len()],
}

impl SubmissionMetrics {
    pub(super) fn observe(&self, timing: NodeLogSubmissionTiming) {
        if timing.cancelled {
            self.cancelled.fetch_add(1, Ordering::Relaxed);
        } else if !timing.succeeded {
            self.failures.fetch_add(1, Ordering::Relaxed);
        } else {
            self.successes.fetch_add(1, Ordering::Relaxed);
            self.frames.fetch_add(timing.frames, Ordering::Relaxed);
            self.bytes.fetch_add(timing.bytes, Ordering::Relaxed);
            // Every phase, including zero waits, describes this same complete
            // assignment. Failed/cancelled submissions have separate counters.
            for (histogram, duration) in self.phases.iter().zip([
                timing.validation,
                timing.native_bytes,
                timing.shipping_slot,
                timing.local_load,
                timing.ordered_lane,
                timing.publication_slot,
                timing.assignment,
                timing.total,
            ]) {
                histogram.observe(duration);
            }
        }
    }

    pub(super) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "successes": self.successes.load(Ordering::Relaxed),
            "failures": self.failures.load(Ordering::Relaxed),
            "cancelled": self.cancelled.load(Ordering::Relaxed),
            "assigned_frames": self.frames.load(Ordering::Relaxed),
            "assigned_bytes": self.bytes.load(Ordering::Relaxed),
        })
    }

    pub(super) fn window_snapshot(&self) -> serde_json::Map<String, serde_json::Value> {
        LABELS
            .iter()
            .zip(&self.phases)
            .map(|(label, histogram)| ((*label).into(), histogram.raw()))
            .collect()
    }
}
