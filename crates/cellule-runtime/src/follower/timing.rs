//! Optional append measurements; callbacks run only after storage locks drop.

use std::time::{Duration, Instant};

use crate::fleet::telemetry::{CellTelemetryHandle, FollowerAppendTiming};

pub(super) struct AppendObservation {
    sink: CellTelemetryHandle,
    started: Option<Instant>,
    pub timing: FollowerAppendTiming,
}

impl AppendObservation {
    pub fn new(
        sink: CellTelemetryHandle,
        leader: crate::SessionId,
        epoch: u64,
        frames: u64,
        encoded_bytes: u64,
    ) -> Self {
        let started = sink.is_enabled().then(Instant::now);
        Self {
            sink,
            started,
            timing: FollowerAppendTiming {
                leader: Some(leader),
                epoch,
                frames,
                encoded_bytes,
                ..FollowerAppendTiming::default()
            },
        }
    }

    pub fn unobserved(lane: super::Lane) -> Self {
        Self::new(Default::default(), lane.leader, lane.epoch, 0, 0)
    }

    pub fn mark(&self) -> Option<Instant> {
        self.started.map(|_| Instant::now())
    }

    pub fn elapsed(mark: Option<Instant>) -> Duration {
        mark.map_or(Duration::ZERO, |started| started.elapsed())
    }

    pub fn worker_started(&mut self) {
        self.timing.worker_queue = Self::elapsed(self.started);
    }

    pub fn sync_data(&mut self, file: &std::fs::File) -> crate::Result<()> {
        let started = self.mark();
        let result = file.sync_data();
        self.timing.data_sync += Self::elapsed(started);
        self.timing.data_sync_calls += 1;
        result.map_err(crate::Error::from)
    }

    pub fn sync_directory(&mut self, path: &std::path::Path) -> crate::Result<()> {
        let started = self.mark();
        let result = super::sync_directory(path);
        self.timing.directory_sync += Self::elapsed(started);
        self.timing.directory_sync_calls += 1;
        result.map_err(crate::Error::from)
    }

    pub fn finish(mut self, succeeded: bool) {
        self.timing.total = Self::elapsed(self.started);
        self.timing.succeeded = succeeded;
        self.timing.worker = self.timing.total.saturating_sub(self.timing.worker_queue);
        self.sink.follower_append(self.timing);
    }
}
