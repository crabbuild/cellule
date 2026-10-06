//! Follower lanes: per-leader record streams that back fleet durability proofs.
use std::collections::{BTreeMap, HashMap, btree_map::Entry};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

use crate::identity::SessionId;
use crate::{Error, Result};

mod accounting;
mod directory;
mod records;
mod timing;

use accounting::{DiskAccounting, LaneAccounting};
use directory::*;
use records::*;
use timing::AppendObservation;

const RECORD_MAGIC: &[u8; 4] = b"CFR1";
const RECORD_HEADER_BYTES: usize = 52;
const ROTATE_BYTES: u64 = 64 << 20;
const MAX_APPEND_FRAMES: usize = 64;
const MAX_TAIL_PAGE_BYTES: usize = 1 << 20;
const MAX_TAIL_PAGE_FRAMES: usize = 4096;
const MAX_RETIRED_LANES: usize = 1_024;
const FOLLOWER_QUARANTINE: &str = "followers-quarantine";
const INDEX_BYTES_PER_RECORD: u64 = 128;
const MAX_FOLLOWER_INDEX_BYTES: u64 = 256 << 20;

#[cfg(test)]
type ScanCounter = Arc<AtomicUsize>;
#[cfg(not(test))]
#[derive(Clone, Copy)]
struct ScanCounter;

#[cfg(test)]
fn new_scan_counter() -> ScanCounter {
    Arc::new(AtomicUsize::new(0))
}

#[cfg(not(test))]
const fn new_scan_counter() -> ScanCounter {
    ScanCounter
}

#[cfg(test)]
fn clone_scan_counter(counter: &ScanCounter) -> ScanCounter {
    Arc::clone(counter)
}

#[cfg(not(test))]
const fn clone_scan_counter(counter: &ScanCounter) -> ScanCounter {
    *counter
}

#[cfg(test)]
fn count_scan(counter: &ScanCounter) {
    counter.fetch_add(1, Ordering::Relaxed);
}

#[cfg(not(test))]
const fn count_scan(_: &ScanCounter) {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Lane {
    leader: SessionId,
    epoch: u64,
}

type LaneState = Arc<Mutex<Option<LaneMemory>>>;
type LaneMap = Arc<Mutex<HashMap<Lane, LaneState>>>;

/// Durable contiguous range retained by one follower lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FollowerReceipt {
    /// First sequence the lane retained before the append.
    pub base_sequence: u64,
    /// Highest sequence the lane has made durable.
    pub durable_through: u64,
}

/// One bounded page from a sealed follower lane.
pub struct FollowerTailPage {
    /// Frames in sequence order.
    pub frames: Vec<Bytes>,
    /// Sequence to continue from when the page filled its bound.
    pub next_sequence: Option<u64>,
}

/// Exact retired follower lane eligible for authority-checked collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetiredFollowerLane {
    leader: SessionId,
    epoch: u64,
    covered_through: u64,
    retired_at_ms: i64,
}

impl RetiredFollowerLane {
    /// Returns the leader whose lane was retired.
    #[must_use]
    pub const fn leader(&self) -> SessionId {
        self.leader
    }

    /// Returns the node-log epoch the lane belonged to.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the highest sequence object storage covered.
    #[must_use]
    pub const fn covered_through(&self) -> u64 {
        self.covered_through
    }

    /// Returns the logical time the lane was retired.
    #[must_use]
    pub const fn retired_at_ms(&self) -> i64 {
        self.retired_at_ms
    }
}

/// Local SSD store for checksum-verified follower fragments.
///
/// The bytes are durability obligations until the leader's contiguous object
/// watermark permits whole-chunk deletion. They are never cache-evicted.
#[derive(Clone)]
pub struct FollowerStore {
    root: PathBuf,
    limits: cellule_ltx::Limits,
    lanes: LaneMap,
    disk: cellule_ltx::DiskBudget,
    retained: Arc<Mutex<DiskAccounting>>,
    // Collection can remove a leader directory shared by several epochs.
    // Normal lane work holds a shared barrier; collection takes it exclusively.
    maintenance: Arc<RwLock<()>>,
    namespace: Arc<Mutex<()>>,
    index_used: Arc<Mutex<u64>>,
    quarantined_entries: usize,
    scan_counter: ScanCounter,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
}

impl FollowerStore {
    /// Opens the `followers` namespace beneath a durable node data directory.
    pub fn open(
        root: PathBuf,
        limits: cellule_ltx::Limits,
        disk: cellule_ltx::DiskBudget,
    ) -> Result<Self> {
        Self::open_with_telemetry(root, limits, disk, Default::default())
    }

    /// Opens the same durable store with the node's bounded operational sink.
    pub fn open_with_telemetry(
        root: PathBuf,
        limits: cellule_ltx::Limits,
        disk: cellule_ltx::DiskBudget,
        telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    ) -> Result<Self> {
        let existed = root.exists();
        std::fs::create_dir_all(&root).map_err(cellule_ltx::LtxError::from)?;
        if !existed {
            let parent = root
                .parent()
                .ok_or(Error::Node("follower root has no parent"))?;
            sync_directory(parent).map_err(cellule_ltx::LtxError::from)?;
        }
        sync_directory(&root).map_err(cellule_ltx::LtxError::from)?;
        scrub_followers(&root, limits)?;
        let retained = disk.try_reserve(follower_bytes(&root)?)?;
        let quarantined_entries = quarantine_entry_count(&root)?;
        Ok(Self {
            root,
            limits,
            lanes: Arc::new(Mutex::new(HashMap::new())),
            disk,
            retained: Arc::new(Mutex::new(DiskAccounting::new(retained))),
            maintenance: Arc::new(RwLock::new(())),
            namespace: Arc::new(Mutex::new(())),
            index_used: Arc::new(Mutex::new(0)),
            quarantined_entries,
            scan_counter: new_scan_counter(),
            telemetry,
        })
    }

    #[cfg(test)]
    pub(crate) fn scan_count(&self) -> usize {
        self.scan_counter.load(Ordering::Relaxed)
    }

    /// Returns retained bytes plus conservative charges for dispatched work.
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        match self.retained.lock() {
            Ok(retained) => retained.bytes(),
            Err(poisoned) => poisoned.into_inner().bytes(),
        }
    }

    /// Returns the bytes the store may still write.
    #[must_use]
    pub fn available_bytes(&self) -> u64 {
        self.disk.available()
    }

    /// Reports diagnostic entries isolated by this or an earlier startup scrub.
    #[must_use]
    pub const fn quarantined_entries(&self) -> usize {
        self.quarantined_entries
    }

    /// Appends one ordered batch and acknowledges only after `sync_data`.
    pub async fn append(
        &self,
        leader: SessionId,
        epoch: u64,
        frames: Vec<Bytes>,
        covered_through: u64,
    ) -> Result<FollowerReceipt> {
        let encoded_bytes = frames
            .iter()
            .try_fold(0_u64, |total, frame| total.checked_add(frame.len() as u64));
        if frames.is_empty()
            || frames.len() > MAX_APPEND_FRAMES
            || encoded_bytes
                .is_none_or(|bytes| bytes > self.limits.max_capture_bytes.saturating_add(64 * 240))
        {
            return Err(Error::Node("invalid follower append batch"));
        }
        let lane = Lane { leader, epoch };
        let lock = self.lane_lock(lane)?;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let maintenance = Arc::clone(&self.maintenance);
        let namespace = Arc::clone(&self.namespace);
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        let growth = encoded_bytes
            .and_then(|bytes| bytes.checked_add((frames.len() * RECORD_HEADER_BYTES) as u64))
            .ok_or(Error::Node("follower append byte count overflow"))?;
        let mut observation = AppendObservation::new(
            self.telemetry.clone(),
            leader,
            epoch,
            frames.len() as u64,
            encoded_bytes.ok_or(Error::Node("follower append byte count overflow"))?,
        );
        tokio::task::spawn_blocking(move || {
            observation.worker_started();
            let result = (|| {
                let _maintenance = maintenance
                    .read()
                    .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
                let waiting = observation.mark();
                let state = lock.lock();
                observation.timing.lane_wait = AppendObservation::elapsed(waiting);
                let mut state = state.map_err(|_| Error::Node("follower lane lock poisoned"))?;
                let accounting =
                    LaneAccounting::begin(retained, &root, lane, growth, &mut observation)?;
                let started = observation.mark();
                let result = append_sync(
                    &root,
                    lane,
                    frames,
                    covered_through,
                    limits,
                    &accounting,
                    &namespace,
                    &index_used,
                    &mut state,
                    &scan_counter,
                    &mut observation,
                );
                observation.timing.append = AppendObservation::elapsed(started);
                let result = accounting.finish(result, &mut observation);
                if result.is_err() {
                    *state = None;
                }
                result
            })();
            observation.finish(result.is_ok());
            result
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Seals a lane against future appends and returns its retained range.
    pub async fn seal(&self, leader: SessionId, epoch: u64) -> Result<FollowerReceipt> {
        let lane = Lane { leader, epoch };
        let lock = self.lane_lock(lane)?;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let maintenance = Arc::clone(&self.maintenance);
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let _maintenance = maintenance
                .read()
                .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
            let mut observation = AppendObservation::unobserved(lane);
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let directory = lane_directory(&root, lane);
            let growth =
                if !directory.join("sealed").exists() && !directory.join("retired").exists() {
                    8
                } else {
                    0
                };
            let accounting =
                LaneAccounting::begin(retained, &root, lane, growth, &mut observation)?;
            let result = seal_sync(
                &root,
                lane,
                limits,
                &accounting,
                &index_used,
                &mut state,
                &scan_counter,
            );
            let result = accounting.finish(result, &mut observation);
            if result.is_err() {
                *state = None;
            }
            result
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Retires one fully object-covered lane and keeps a durable append fence.
    pub async fn retire(
        &self,
        leader: SessionId,
        epoch: u64,
        covered_through: u64,
    ) -> Result<FollowerReceipt> {
        let lane = Lane { leader, epoch };
        let lock = self.lane_lock(lane)?;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let maintenance = Arc::clone(&self.maintenance);
        let namespace = Arc::clone(&self.namespace);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let _maintenance = maintenance
                .read()
                .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
            let mut observation = AppendObservation::unobserved(lane);
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let growth = if !lane_directory(&root, lane).join("retired").exists() {
                8
            } else {
                0
            };
            let accounting =
                LaneAccounting::begin(retained, &root, lane, growth, &mut observation)?;
            accounting.invalidate();
            let result = retire_sync(
                &root,
                lane,
                covered_through,
                limits,
                &namespace,
                &scan_counter,
            );
            let result = accounting.finish(result, &mut observation);
            *state = None;
            result
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Reads a sealed, verified tail in node-sequence order.
    pub async fn read_tail(
        &self,
        leader: SessionId,
        epoch: u64,
        first_sequence: u64,
    ) -> Result<Vec<Bytes>> {
        let lane = Lane { leader, epoch };
        let lock = self.lane_lock(lane)?;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let maintenance = Arc::clone(&self.maintenance);
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let _maintenance = maintenance
                .read()
                .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
            let mut observation = AppendObservation::unobserved(lane);
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let accounting = LaneAccounting::begin(retained, &root, lane, 0, &mut observation)?;
            if state.is_none() {
                accounting.invalidate();
            }
            let result = read_tail_sync(
                &root,
                lane,
                first_sequence,
                limits,
                &index_used,
                &mut state,
                usize::MAX,
                usize::MAX,
                &scan_counter,
            )
            .map(|page| page.frames);
            let result = accounting.finish(result, &mut observation);
            if result.is_err() {
                *state = None;
            }
            result
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Reads one network-sized page from a sealed, verified tail.
    ///
    /// A single frame may exceed the page target and is returned alone because
    /// node frames are the independently checksummed transport unit.
    pub async fn read_tail_page(
        &self,
        leader: SessionId,
        epoch: u64,
        first_sequence: u64,
    ) -> Result<FollowerTailPage> {
        let lane = Lane { leader, epoch };
        let lock = self.lane_lock(lane)?;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let maintenance = Arc::clone(&self.maintenance);
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let _maintenance = maintenance
                .read()
                .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
            let mut observation = AppendObservation::unobserved(lane);
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let accounting = LaneAccounting::begin(retained, &root, lane, 0, &mut observation)?;
            if state.is_none() {
                accounting.invalidate();
            }
            let result = read_tail_sync(
                &root,
                lane,
                first_sequence,
                limits,
                &index_used,
                &mut state,
                MAX_TAIL_PAGE_BYTES,
                MAX_TAIL_PAGE_FRAMES,
                &scan_counter,
            );
            let result = accounting.finish(result, &mut observation);
            if result.is_err() {
                *state = None;
            }
            result
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Lists bounded retired lanes whose marker predates the caller's grace cutoff.
    pub async fn retired_lanes(
        &self,
        retired_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<RetiredFollowerLane>> {
        if retired_before_ms < 0 || !(1..=MAX_RETIRED_LANES).contains(&limit) {
            return Err(Error::Node("retired follower scan bound is invalid"));
        }
        let root = self.root.clone();
        let maintenance = Arc::clone(&self.maintenance);
        tokio::task::spawn_blocking(move || {
            let _maintenance = maintenance
                .read()
                .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
            retired_lanes_sync(&root, retired_before_ms, limit)
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Deletes one exact, grace-aged retired lane after external authority proof.
    pub async fn remove_retired(
        &self,
        candidate: RetiredFollowerLane,
        retired_before_ms: i64,
    ) -> Result<bool> {
        if candidate.retired_at_ms > retired_before_ms {
            return Err(Error::Node("retired follower grace period has not elapsed"));
        }
        let lane = Lane {
            leader: candidate.leader,
            epoch: candidate.epoch,
        };
        let lock = self.lane_lock(lane)?;
        let cleanup_lock = Arc::clone(&lock);
        let root = self.root.clone();
        let retained = Arc::clone(&self.retained);
        let maintenance = Arc::clone(&self.maintenance);
        let removed = tokio::task::spawn_blocking(move || {
            let _maintenance = maintenance
                .write()
                .map_err(|_| Error::Node("follower maintenance lock poisoned"))?;
            let mut observation = AppendObservation::unobserved(lane);
            let _lane = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let accounting = LaneAccounting::begin(retained, &root, lane, 0, &mut observation)?;
            let result = remove_retired_sync(&root, lane, candidate, retired_before_ms);
            if matches!(result, Ok(true)) {
                accounting.emptied();
            }
            accounting.finish(result, &mut observation)
        })
        .await
        .map_err(Error::FollowerWorkerJoin)??;
        if removed {
            let mut lanes = self
                .lanes
                .lock()
                .map_err(|_| Error::Node("follower store lock poisoned"))?;
            if lanes
                .get(&lane)
                .is_some_and(|current| Arc::ptr_eq(current, &cleanup_lock))
                && Arc::strong_count(&cleanup_lock) == 2
            {
                lanes.remove(&lane);
            }
        }
        Ok(removed)
    }

    fn lane_lock(&self, lane: Lane) -> Result<Arc<Mutex<Option<LaneMemory>>>> {
        let mut lanes = self
            .lanes
            .lock()
            .map_err(|_| Error::Node("follower store lock poisoned"))?;
        Ok(lanes
            .entry(lane)
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone())
    }
}

#[cfg(test)]
mod tests;
