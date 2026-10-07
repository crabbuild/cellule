//! Follower lanes: per-leader record streams that back fleet durability proofs.
use std::collections::{BTreeMap, HashMap, btree_map::Entry};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::fleet::telemetry::{CellTelemetryHandle, FollowerAppendTiming};
use crate::identity::SessionId;
use crate::{Error, Result};
use std::time::Instant;

mod grant;
pub use grant::{AppendGrantIssuer, AppendGrantPeer, GrantedFollowerAppend};
mod directory;
mod inventory;
mod records;

pub use inventory::{
    FollowerInventoryCursor, FollowerInventoryPage, FollowerLaneObservation, FollowerLaneState,
};

use directory::*;
use records::*;

const RECORD_MAGIC: &[u8; 4] = b"CFR1";
const RECORD_HEADER_BYTES: usize = 52;
// Warm scans and coverage rewrites touch the live chunk. Keep that work
// bounded; a single larger valid frame still occupies its own chunk.
const ROTATE_BYTES: u64 = 1 << 20;
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

enum RetirementWatermark {
    Covered(u64),
    Recovered { active: bool },
}

struct LaneSlot {
    memory: Mutex<Option<LaneMemory>>,
    grant: Arc<tokio::sync::Mutex<grant::GrantState>>,
}
impl LaneSlot {
    fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, Option<LaneMemory>>> {
        self.memory.lock()
    }
}
type LaneState = Arc<LaneSlot>;
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
    retained: Arc<Mutex<cellule_ltx::DiskReservation>>,
    index_used: Arc<Mutex<u64>>,
    quarantined_entries: usize,
    scan_counter: ScanCounter,
    admission: crate::fleet::admission::NodeAdmission,
    inventory_scope: [u8; 16],
    telemetry: CellTelemetryHandle,
    grant_receiver: Option<SessionId>,
    grant_slots: Arc<tokio::sync::Semaphore>,
}

impl FollowerStore {
    /// Opens the `followers` namespace beneath a durable node data directory.
    pub fn open(
        root: PathBuf,
        limits: cellule_ltx::Limits,
        disk: cellule_ltx::DiskBudget,
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
            retained: Arc::new(Mutex::new(retained)),
            index_used: Arc::new(Mutex::new(0)),
            quarantined_entries,
            scan_counter: new_scan_counter(),
            admission: crate::fleet::admission::NodeAdmission::default(),
            inventory_scope: rand::random(),
            telemetry: CellTelemetryHandle::default(),
            grant_receiver: None,
            grant_slots: Arc::new(tokio::sync::Semaphore::new(grant::MAX_APPEND_GRANTS)),
        })
    }

    /// Installs the runtime's shared gate before the store is shared or serves
    /// traffic. Cordon blocks entirely new lanes while existing acknowledged
    /// tails remain appendable under their normal epoch authorization.
    #[must_use]
    pub fn with_node_admission(
        mut self,
        admission: crate::fleet::admission::NodeAdmission,
    ) -> Self {
        self.admission = admission;
        self
    }

    /// Attaches a bounded telemetry sink before sharing the native store.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: CellTelemetryHandle) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[cfg(test)]
    pub(crate) fn scan_count(&self) -> usize {
        self.scan_counter.load(Ordering::Relaxed)
    }

    /// Returns the bytes the store currently retains.
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
        self.append_inner(leader, epoch, frames, covered_through, None)
            .await
    }

    async fn append_inner(
        &self,
        leader: SessionId,
        epoch: u64,
        frames: Vec<Bytes>,
        covered_through: u64,
        grant: Option<grant::GrantAppend>,
    ) -> Result<FollowerReceipt> {
        let encoded_bytes = frames
            .iter()
            .try_fold(0_u64, |total, frame| total.checked_add(frame.len() as u64));
        if frames.is_empty()
            || frames.len() > MAX_APPEND_FRAMES
            || encoded_bytes.is_none_or(|bytes| {
                bytes
                    > self
                        .limits
                        .max_capture_bytes
                        .saturating_add(64 * cellule_ltx::MAX_NODE_FRAME_HEADER_BYTES as u64)
            })
        {
            return Err(Error::Node("invalid follower append batch"));
        }
        let lane = Lane { leader, epoch };
        validate_lane(lane)?;
        if !lane_directory(&self.root, lane).exists() {
            self.admission.check_new_role()?;
        }
        let lock = if grant.is_some() {
            // An invalid/replayed token cannot allocate a new lane. Only fresh
            // grant issuance installs its locally charged registry entry.
            self.lanes
                .lock()
                .map_err(|_| Error::Node("follower store lock poisoned"))?
                .get(&lane)
                .cloned()
                .ok_or(Error::Fenced)?
        } else {
            self.lane_lock(lane)?
        };
        let grant_state = lock.grant.clone().lock_owned().await;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        let growth = encoded_bytes
            .and_then(|bytes| bytes.checked_add((frames.len() * RECORD_HEADER_BYTES) as u64))
            .ok_or(Error::Node("follower append byte count overflow"))?;
        let admission = self.admission.clone();
        let lanes = Arc::clone(&self.lanes);
        let queued_at = Instant::now();
        let telemetry = self.telemetry.clone();
        let frame_count = frames.len() as u64;
        tokio::task::spawn_blocking(move || {
            // Dispatched work owns the lifecycle gate through fsync and proof
            // revalidation even when the HTTP or runtime waiter disconnects.
            let grant_state = grant_state;
            let authorization = grant
                .as_ref()
                .map(|request| grant_state.authorize(request, epoch))
                .transpose()?;
            let covered_through = authorization.as_ref().map_or(
                covered_through,
                crate::node::append_grant::NodeAppendGrant::covered_through,
            );
            let mut observation = AppendObservation {
                started: Instant::now(),
                telemetry,
                timing: FollowerAppendTiming {
                    worker_queue: queued_at.elapsed(),
                    frames: frame_count,
                    ..FollowerAppendTiming::default()
                },
            };
            let retained = retained
                .lock()
                .map_err(|_| Error::Node("follower disk reservation lock poisoned"))?;
            retained.try_grow(growth)?;
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let result = (|| {
                if !lane_directory(&root, lane).exists() {
                    admission.admit(|| ensure_lane_directories(&root, lane))?;
                }
                append_sync(
                    &root,
                    lane,
                    frames,
                    covered_through,
                    limits,
                    &index_used,
                    &mut state,
                    &scan_counter,
                    &mut observation.timing,
                    authorization.as_ref(),
                )
            })();
            let resize =
                follower_bytes(&root).and_then(|bytes| retained.resize(bytes).map_err(Error::from));
            let result = settle_disk_reservation(result, resize);
            let result = result.and_then(|receipt| {
                if let Some(request) = &grant {
                    grant_state.authorize(request, epoch)?;
                }
                Ok(receipt)
            });
            if result.is_err() {
                if let Some(memory) = state.as_mut() {
                    memory.invalidate();
                }
                if !lane_directory(&root, lane).exists()
                    && state.as_ref().is_none_or(LaneMemory::is_empty)
                    && let Ok(mut lanes) = lanes.lock()
                {
                    // Cordon may win after the precheck but before enrollment.
                    // Rejected transient lanes must not accumulate in memory.
                    if lanes.get(&lane).is_some_and(|registered| {
                        Arc::ptr_eq(registered, &lock) && Arc::strong_count(registered) == 2
                    }) {
                        *state = None;
                        lanes.remove(&lane);
                    }
                }
            }
            observation.timing.succeeded = result.is_ok();
            result
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }

    /// Seals a lane against future appends and returns its retained range.
    pub async fn seal(&self, leader: SessionId, epoch: u64) -> Result<FollowerReceipt> {
        let lane = Lane { leader, epoch };
        let lock = self.lane_lock(lane)?;
        let grant_state = lock.grant.clone().lock_owned().await;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let mut grant_state = grant_state;
            let retained = retained
                .lock()
                .map_err(|_| Error::Node("follower disk reservation lock poisoned"))?;
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let directory = lane_directory(&root, lane);
            if !directory.join("sealed").exists() && !directory.join("retired").exists() {
                retained.try_grow(8)?;
            }
            let result = seal_sync(&root, lane, limits, &index_used, &mut state, &scan_counter);
            let resize =
                follower_bytes(&root).and_then(|bytes| retained.resize(bytes).map_err(Error::from));
            let result = settle_disk_reservation(result, resize);
            let directory = lane_directory(&root, lane);
            if directory.join("sealed").exists() || directory.join("retired").exists() {
                grant_state.close();
            }
            if result.is_err()
                && let Some(memory) = state.as_mut()
            {
                memory.invalidate();
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
        self.retire_lane(lane, RetirementWatermark::Covered(covered_through))
            .await
    }

    /// Retires a canonically recovered lane through its existing durable fence.
    /// The application binds `member` to this receiver and authenticates the
    /// requester before obtaining fresh directory authorization. Active lanes
    /// must retain their actual native seal; no caller watermark is accepted.
    pub async fn retire_recovered(
        &self,
        member: crate::identity::NodeId,
        authorization: crate::node::RecoveredLogRetirementAuthorization,
    ) -> Result<FollowerReceipt> {
        if authorization.member() != member {
            return Err(Error::Fenced);
        }
        let sealed = authorization.sealed();
        let lane = Lane {
            leader: sealed.session(),
            epoch: sealed.log().epoch(),
        };
        self.retire_lane(
            lane,
            RetirementWatermark::Recovered {
                active: sealed.log().active(),
            },
        )
        .await
    }

    async fn retire_lane(
        &self,
        lane: Lane,
        watermark: RetirementWatermark,
    ) -> Result<FollowerReceipt> {
        let lock = self.lane_lock(lane)?;
        let grant_state = lock.grant.clone().lock_owned().await;
        let root = self.root.clone();
        let limits = self.limits;
        let retained = Arc::clone(&self.retained);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let mut grant_state = grant_state;
            let retained = retained
                .lock()
                .map_err(|_| Error::Node("follower disk reservation lock poisoned"))?;
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            if !lane_directory(&root, lane).join("retired").exists() {
                retained.try_grow(8)?;
            }
            let result = retire_sync(&root, lane, watermark, limits, &scan_counter);
            let resize =
                follower_bytes(&root).and_then(|bytes| retained.resize(bytes).map_err(Error::from));
            let result = settle_disk_reservation(result, resize);
            let directory = lane_directory(&root, lane);
            if directory.join("sealed").exists() || directory.join("retired").exists() {
                grant_state.close();
            }
            if result.is_ok() {
                *state = None;
            } else if let Some(memory) = state.as_mut() {
                memory.invalidate();
            }
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
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
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
            if result.is_err()
                && let Some(memory) = state.as_mut()
            {
                memory.invalidate();
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
        let index_used = Arc::clone(&self.index_used);
        let scan_counter = clone_scan_counter(&self.scan_counter);
        tokio::task::spawn_blocking(move || {
            let mut state = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
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
            if result.is_err()
                && let Some(memory) = state.as_mut()
            {
                memory.invalidate();
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
        tokio::task::spawn_blocking(move || retired_lanes_sync(&root, retired_before_ms, limit))
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
        let grant_state = lock.grant.clone().lock_owned().await;
        let cleanup_lock = Arc::clone(&lock);
        let root = self.root.clone();
        let retained = Arc::clone(&self.retained);
        let removed = tokio::task::spawn_blocking(move || {
            let mut grant_state = grant_state;
            let retained = retained
                .lock()
                .map_err(|_| Error::Node("follower disk reservation lock poisoned"))?;
            let _lane = lock
                .lock()
                .map_err(|_| Error::Node("follower lane lock poisoned"))?;
            let removed = remove_retired_sync(&root, lane, candidate, retired_before_ms)?;
            if removed {
                grant_state.close();
            }
            retained.resize(follower_bytes(&root)?)?;
            Ok::<bool, Error>(removed)
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

    fn lane_lock(&self, lane: Lane) -> Result<LaneState> {
        let mut lanes = self
            .lanes
            .lock()
            .map_err(|_| Error::Node("follower store lock poisoned"))?;
        Ok(lanes
            .entry(lane)
            .or_insert_with(|| {
                Arc::new(LaneSlot {
                    memory: Mutex::new(None),
                    grant: Arc::new(tokio::sync::Mutex::new(grant::GrantState::default())),
                })
            })
            .clone())
    }
}

struct AppendObservation {
    started: Instant,
    telemetry: CellTelemetryHandle,
    timing: FollowerAppendTiming,
}

impl Drop for AppendObservation {
    fn drop(&mut self) {
        self.timing.worker = self.started.elapsed();
        self.telemetry.follower_append(self.timing);
    }
}

#[cfg(test)]
mod tests;
