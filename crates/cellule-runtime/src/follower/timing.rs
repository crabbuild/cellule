//! Optional append measurements; callbacks run only after storage locks drop.

use std::time::{Duration, Instant};

use crate::fleet::telemetry::{CellTelemetryHandle, FollowerAppendTiming};

pub(super) struct AppendObservation {
    sink: CellTelemetryHandle,
    started: Option<Instant>,
    leader: crate::SessionId,
    epoch: u64,
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
            leader,
            epoch,
            timing: FollowerAppendTiming {
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
        self.timing.blocking_queue = Self::elapsed(self.started);
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
        self.sink
            .follower_append(self.leader, self.epoch, self.timing);
    }
}
