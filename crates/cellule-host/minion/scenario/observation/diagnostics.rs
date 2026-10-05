//! Temporary CI deadline probes; remove after the hosted observer is diagnosed.
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

pub(in crate::scenario) struct Trace {
    detail: String,
    started: Instant,
    deadline: Instant,
    stage: &'static str,
    stage_started: Instant,
    stages: BTreeMap<&'static str, (usize, Duration)>,
    completed: bool,
}

impl Trace {
    pub(in crate::scenario) fn new(detail: String, deadline: Instant, stage: &'static str) -> Self {
        let started = Instant::now();
        Self {
            detail,
            started,
            deadline,
            stage,
            stage_started: started,
            stages: BTreeMap::new(),
            completed: false,
        }
    }

    pub(in crate::scenario) fn stage(&mut self, stage: &'static str) {
        self.record_stage();
        self.stage = stage;
        self.stage_started = Instant::now();
    }

    fn record_stage(&mut self) {
        let entry = self.stages.entry(self.stage).or_default();
        entry.0 += 1;
        entry.1 += self.stage_started.elapsed();
    }

    pub(in crate::scenario) fn finish(&mut self) {
        self.completed = true;
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        let budget = self.deadline.saturating_duration_since(self.started);
        if !self.completed || (self.detail == "collector" && elapsed >= budget.mul_f32(0.75)) {
            self.record_stage();
            eprintln!(
                "[DEBUG-fleet-57] completed={} {} stage={} elapsed={:?} stage_elapsed={:?} original_budget={:?} stages={:?}",
                self.completed,
                self.detail,
                self.stage,
                elapsed,
                self.stage_started.elapsed(),
                budget,
                self.stages,
            );
        }
    }
}
