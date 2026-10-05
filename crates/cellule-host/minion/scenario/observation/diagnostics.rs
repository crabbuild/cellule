//! Temporary CI deadline probes; remove after the hosted observer is diagnosed.
use tokio::time::Instant;

pub(in crate::scenario) struct Trace {
    detail: String,
    started: Instant,
    deadline: Instant,
    stage: &'static str,
    stage_started: Instant,
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
            completed: false,
        }
    }

    pub(in crate::scenario) fn stage(&mut self, stage: &'static str) {
        self.stage = stage;
        self.stage_started = Instant::now();
    }

    pub(in crate::scenario) fn finish(&mut self) {
        self.completed = true;
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        if !self.completed {
            eprintln!(
                "[DEBUG-fleet-57] failed-or-cancelled {} stage={} elapsed={:?} stage_elapsed={:?} original_budget={:?}",
                self.detail,
                self.stage,
                self.started.elapsed(),
                self.stage_started.elapsed(),
                self.deadline.saturating_duration_since(self.started),
            );
        }
    }
}
