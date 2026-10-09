//! Optional, pool-local slot lifetime observations; no change to admission.

use super::*;
use crate::fleet::telemetry::{CellTelemetryHandle, SqlJobKind, SqlSlotTiming};
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct Release {
    id: AtomicU64,
    at: AtomicU64,
}

pub(super) struct Probe {
    telemetry: CellTelemetryHandle,
    origin: Instant,
    next_id: AtomicU64,
    releases: Vec<Arc<Release>>,
}

impl Probe {
    pub(super) fn new(workers: usize) -> Self {
        Self {
            telemetry: CellTelemetryHandle::default(),
            origin: Instant::now(),
            next_id: AtomicU64::new(1),
            releases: (0..workers).map(|_| Arc::new(Release::default())).collect(),
        }
    }

    pub(super) fn telemetry(&self) -> CellTelemetryHandle {
        self.telemetry.clone()
    }

    pub(super) fn request(&self) -> Option<u64> {
        self.telemetry.is_enabled().then(|| elapsed(self.origin))
    }

    pub(super) fn acquired(
        &self,
        requested: Option<u64>,
        shard: usize,
        kind: SqlJobKind,
    ) -> Result<Option<JobTrace>> {
        let Some(requested) = requested else {
            return Ok(None);
        };
        let id = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::Capacity("SQL slot trace IDs"))?;
        // The caller exclusively owns this shard's permit. The previous holder
        // wrote both markers before releasing that permit, and no other holder
        // can overwrite them until this reservation itself releases.
        let release = self.releases[shard].clone();
        let previous_job = release.id.load(Ordering::Acquire);
        let previous_at = release.at.load(Ordering::Relaxed);
        Ok(Some(JobTrace {
            id,
            previous_job,
            previous_at,
            shard,
            kind,
            requested,
            acquired: elapsed(self.origin),
            started: None,
            origin: self.origin,
            release,
            telemetry: self.telemetry.clone(),
        }))
    }
}

pub(super) struct JobTrace {
    pub(super) id: u64,
    previous_job: u64,
    previous_at: u64,
    shard: usize,
    kind: SqlJobKind,
    requested: u64,
    acquired: u64,
    started: Option<u64>,
    origin: Instant,
    release: Arc<Release>,
    telemetry: CellTelemetryHandle,
}

impl JobTrace {
    pub(super) fn started(&mut self) {
        self.started = Some(elapsed(self.origin));
    }

    pub(super) fn release(&self) -> SqlSlotTiming {
        let released = elapsed(self.origin);
        self.release.at.store(released, Ordering::Relaxed);
        self.release.id.store(self.id, Ordering::Release);
        SqlSlotTiming {
            id: self.id,
            previous_job: (self.previous_job != 0).then_some(self.previous_job),
            shard: self.shard,
            kind: self.kind,
            admission: Duration::from_nanos(self.acquired.saturating_sub(self.requested)),
            after_last_release: (self.previous_job != 0).then(|| {
                Duration::from_nanos(
                    self.acquired
                        .saturating_sub(self.previous_at.max(self.requested)),
                )
            }),
            handoff: self
                .started
                .map(|started| Duration::from_nanos(started.saturating_sub(self.acquired))),
            native: self
                .started
                .map(|started| Duration::from_nanos(released.saturating_sub(started))),
            held: Duration::from_nanos(released.saturating_sub(self.acquired)),
            requested_ns: self.requested,
            acquired_ns: self.acquired,
            started_ns: self.started,
            released_ns: released,
        }
    }

    pub(super) fn emit(&self, timing: SqlSlotTiming) {
        self.telemetry.sql_slot_released(timing);
    }
}

fn elapsed(origin: Instant) -> u64 {
    u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
