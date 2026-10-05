//! Scheduled latency includes load-generator queueing; HTTP latency starts at dispatch.
use std::time::Duration;
const BUCKET_US: u64 = 100;
const BUCKETS: usize = 100_002;

pub(super) struct Metrics {
    pub warmup_attempts: u64,
    pub warmup_errors: u64,
    pub errors: u64,
    attempts: u64,
    successes: u64,
    completed_in_window: u64,
    bytes: u64,
    per_cell_successes: Vec<u64>,
    scheduled: Vec<u32>,
    request: Vec<u32>,
    max_scheduled_ms: f64,
    max_request_ms: f64,
}

fn observe(histogram: &mut [u32], latency: Duration) {
    let bucket = latency.as_micros().div_ceil(u128::from(BUCKET_US));
    let index = usize::try_from(bucket)
        .unwrap_or(usize::MAX)
        .min(BUCKETS - 1);
    histogram[index] += 1;
}

fn percentile(histogram: &[u32], attempts: u64, percent: u64) -> Option<f64> {
    if attempts == 0 {
        return None;
    }
    let rank = (attempts * percent).div_ceil(100);
    let mut count = 0_u64;
    for (index, bucket) in histogram.iter().enumerate() {
        count += u64::from(*bucket);
        if count >= rank {
            return (index < BUCKETS - 1).then_some(index as f64 * BUCKET_US as f64 / 1_000.0);
        }
    }
    None
}

impl Metrics {
    pub fn new(cells: usize) -> Self {
        Self {
            warmup_attempts: 0,
            warmup_errors: 0,
            errors: 0,
            attempts: 0,
            successes: 0,
            completed_in_window: 0,
            bytes: 0,
            per_cell_successes: vec![0; cells],
            scheduled: vec![0; BUCKETS],
            request: vec![0; BUCKETS],
            max_scheduled_ms: 0.0,
            max_request_ms: 0.0,
        }
    }

    pub fn observe(
        &mut self,
        shard: usize,
        success: bool,
        in_window: bool,
        scheduled: Duration,
        request: Duration,
        bytes: usize,
    ) {
        self.attempts += 1;
        self.errors += u64::from(!success);
        self.successes += u64::from(success);
        self.completed_in_window += u64::from(success && in_window);
        if success {
            self.per_cell_successes[shard] += 1;
            self.bytes += bytes as u64;
        }
        observe(&mut self.scheduled, scheduled);
        observe(&mut self.request, request);
        self.max_scheduled_ms = self.max_scheduled_ms.max(scheduled.as_secs_f64() * 1_000.0);
        self.max_request_ms = self.max_request_ms.max(request.as_secs_f64() * 1_000.0);
    }

    pub fn merge(&mut self, other: Self) {
        self.warmup_attempts += other.warmup_attempts;
        self.warmup_errors += other.warmup_errors;
        self.attempts += other.attempts;
        self.successes += other.successes;
        self.errors += other.errors;
        self.completed_in_window += other.completed_in_window;
        self.bytes += other.bytes;
        for (a, b) in self
            .per_cell_successes
            .iter_mut()
            .zip(other.per_cell_successes)
        {
            *a += b;
        }
        for (a, b) in self.scheduled.iter_mut().zip(other.scheduled) {
            *a += b;
        }
        for (a, b) in self.request.iter_mut().zip(other.request) {
            *a += b;
        }
        self.max_scheduled_ms = self.max_scheduled_ms.max(other.max_scheduled_ms);
        self.max_request_ms = self.max_request_ms.max(other.max_request_ms);
    }

    pub fn summary(
        &self,
        seconds: u64,
        rate: u64,
        offered: u64,
        dropped: u64,
        warmup_dropped: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "target_requests_per_second": rate, "planned_offers": rate*seconds,
            "generated_offers": offered, "producer_unissued": (rate*seconds).saturating_sub(offered),
            "queue_dropped": dropped, "warmup_queue_dropped": warmup_dropped,
            "warmup_attempts": self.warmup_attempts, "warmup_errors": self.warmup_errors,
            "attempts": self.attempts, "successes_including_drain": self.successes, "errors": self.errors,
            "successes_in_window": self.completed_in_window, "successful_requests_per_second": self.completed_in_window as f64 / seconds as f64,
            "successes_after_deadline": self.successes - self.completed_in_window,
            "payload_bytes_including_drain": self.bytes, "per_cell_successes_including_drain": self.per_cell_successes,
            "scheduled_latency_ms_all_attempts": {"p50": percentile(&self.scheduled,self.attempts,50), "p95": percentile(&self.scheduled,self.attempts,95), "p99": percentile(&self.scheduled,self.attempts,99), "max":self.max_scheduled_ms},
            "request_latency_ms_all_attempts": {"p50": percentile(&self.request,self.attempts,50), "p95": percentile(&self.request,self.attempts,95), "p99": percentile(&self.request,self.attempts,99), "max":self.max_request_ms},
            "histogram_resolution_us": BUCKET_US, "scheduled_histogram_overflow": self.scheduled[BUCKETS-1], "request_histogram_overflow": self.request[BUCKETS-1],
        })
    }
}

#[cfg(test)]
mod tests;
