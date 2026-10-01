//! Generation-local demand samples. Page reads cannot manufacture stability.

use crate::cell::worker::{ACTIVE_CELL_PAGE_CACHE_BYTES, WorkerCellInventory};
use crate::fleet::operations::TransferCost;
use crate::fleet::resource::{
    ACTIVE_CELL_FILE_DESCRIPTORS, ACTIVE_CELL_NATIVE_BYTES, READ_REPLICA_FILE_DESCRIPTORS,
    READ_REPLICA_NATIVE_BYTES,
};
use crate::{Error, Result};

const REFRESH_MS: i64 = 15_000;
const FRESH_MS: i64 = 30_000;
const RETRY_MS: i64 = 1_000;
const SECOND_SAMPLE_MS: i64 = 100;

#[derive(Default)]
pub(in crate::cell::actor) struct CellDemandState {
    sample: Option<WorkerCellInventory>,
    stable_observations: u8,
    last_measurement_ms: Option<i64>,
    last_attempt_ms: i64,
    refresh_after_ms: i64,
}

impl CellDemandState {
    pub(in crate::cell::actor) fn clear(&mut self) {
        self.sample = None;
        self.stable_observations = 0;
        self.refresh_after_ms = 0;
    }

    pub(in crate::cell::actor) fn failed(&mut self, now_ms: i64) {
        self.clear();
        self.last_attempt_ms = self.last_attempt_ms.max(now_ms);
        self.refresh_after_ms = now_ms.saturating_add(RETRY_MS);
    }

    pub(in crate::cell::actor) fn record(
        &mut self,
        sample: WorkerCellInventory,
        limits: cellule_ltx::Limits,
        published_sequence: u64,
    ) -> Result<()> {
        self.last_attempt_ms = self.last_attempt_ms.max(sample.observed_at_ms);
        let validation = || {
            if sample.observed_at_ms < 0
                || sample.persisted_work.is_unknown()
                || self
                    .last_measurement_ms
                    .is_some_and(|at| sample.observed_at_ms < at)
            {
                return Err(Error::Control(
                    "Cell demand measurement regressed or is unknown",
                ));
            }
            if sample.commit_sequence != published_sequence {
                return Err(Error::PendingPublication);
            }
            transfer_cost(limits, sample.database_bytes).map(|_| ())
        };
        if let Err(error) = validation() {
            self.failed(sample.observed_at_ms);
            return Err(error);
        }
        let unchanged = self.sample.is_some_and(|old| {
            old.commit_sequence == sample.commit_sequence
                && old.database_bytes == sample.database_bytes
                && old.persisted_work == sample.persisted_work
                && old.transfer_work == sample.transfer_work
                && old.maintenance_work == sample.maintenance_work
        });
        self.stable_observations = if unchanged {
            if self
                .sample
                .is_some_and(|old| sample.observed_at_ms > old.observed_at_ms)
            {
                self.stable_observations.saturating_add(1).min(2)
            } else {
                self.stable_observations
            }
        } else {
            1
        };
        self.last_measurement_ms = Some(sample.observed_at_ms);
        self.sample = Some(sample);
        self.refresh_after_ms =
            sample
                .observed_at_ms
                .saturating_add(if self.stable_observations < 2 {
                    SECOND_SAMPLE_MS
                } else {
                    REFRESH_MS
                });
        Ok(())
    }

    pub(in crate::cell::actor) fn should_refresh(&self, now_ms: i64, work_unknown: bool) -> bool {
        now_ms >= self.refresh_after_ms
            && (work_unknown
                || self.sample.is_none()
                || self.stable_observations < 2
                || self
                    .sample
                    .is_some_and(|s| now_ms.saturating_sub(s.observed_at_ms) >= REFRESH_MS))
    }

    pub(in crate::cell::actor) fn refresh_priority(&self) -> i64 {
        self.last_attempt_ms
    }

    pub(in crate::cell::actor) fn fresh_sample(
        &self,
        now_ms: i64,
        published_sequence: u64,
    ) -> Option<WorkerCellInventory> {
        self.sample.filter(|s| {
            now_ms >= s.observed_at_ms
                && now_ms.saturating_sub(s.observed_at_ms) < FRESH_MS
                && s.commit_sequence == published_sequence
        })
    }

    pub(in crate::cell::actor) fn stable_observations(&self) -> u8 {
        self.stable_observations
    }
}

pub(in crate::cell::actor) fn transfer_cost(
    limits: cellule_ltx::Limits,
    database_bytes: u64,
) -> Result<TransferCost> {
    if database_bytes == 0
        || database_bytes > limits.max_database_bytes
        || limits.max_database_bytes < 512
        || limits.max_file_bytes < 128
        || limits.max_plan_bytes < limits.max_file_bytes
    {
        return Err(Error::Capacity(
            "Cell demand exceeds its validated LTX bounds",
        ));
    }
    // Bound full-image restore, retained plan inputs, and the canonical restore's
    // 64-MiB headroom. Use the validated database ceiling rather than today's
    // sparse-file length, so growth before release does not underreserve disk.
    let disk_bytes = limits
        .max_database_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(limits.max_plan_bytes))
        .and_then(|bytes| bytes.checked_add(64 << 20))
        .ok_or(Error::Capacity("Cell transfer disk cost overflow"))?;
    Ok(TransferCost {
        // Reuse the framework's conservative cache/native accounting. This is
        // admission demand, not a measured process RSS guarantee.
        memory_bytes: ACTIVE_CELL_NATIVE_BYTES as u64
            + ACTIVE_CELL_PAGE_CACHE_BYTES
            + READ_REPLICA_NATIVE_BYTES as u64,
        disk_bytes,
        file_descriptors: (ACTIVE_CELL_FILE_DESCRIPTORS + READ_REPLICA_FILE_DESCRIPTORS) as u32,
        job_credits: 1,
    })
}
