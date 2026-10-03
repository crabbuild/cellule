//! Canonical full native traversal beneath the application's fleet observer.
use super::{
    FleetJournalSnapshot, FleetNodeSnapshot, FleetOwnedCell, FleetRoster, FleetSnapshotBindings,
    FleetSnapshotNativePage, FleetSnapshotRequest, FleetSnapshotSubject, operation,
};
use crate::read_replicas::{ReaderEnrollmentCompletion, ReaderEnrollmentJobs};
use crate::{NodeDurabilitySupervisorObservation, NodeState};
use cellule_runtime::{
    Error, Result,
    client::ReadReplicaLifecycleObservation,
    follower::FollowerLaneObservation,
    identity::{CellId, Digest, NodeId, SessionId},
    node::NodeMode,
};
use std::collections::HashSet;

mod pages;
mod recheck;
mod roles;
mod traversal;
pub use recheck::FleetNodeInventoryRecheck;

const MAX_ENTRIES: usize = 10_000;
const CATEGORIES: usize = 7;
// Four 10,000-row categories, at most 32 producer epochs, Host/supervisor,
// and both local and fleet-wide rechecks. Further bounded rounds may consume
// spare capacity; exhaustion requires a fresh traversal, never nonce eviction.
const MAX_CAPTURES: usize = 4 * MAX_ENTRIES + 32 + 2 + 2 * CATEGORIES;

/// Fully traversed local owners at an exact durable roster barrier.
///
/// This is native coverage, not fleet settlement. Authentication, unexpected
/// directory-record discovery, current Cell/log authority, failed-process
/// closure and replacement policy remain required. Unbound categories retain
/// their missing-binding identity; they are never interpreted as empty roles.
/// Applications account these bounded copied collector buffers. Native page
/// charges stay with their original responses and can be released after accept.
pub struct FleetNodeInventory {
    node: NodeId,
    session: SessionId,
    roster: Digest,
    snapshot: FleetJournalSnapshot,
    nonces: HashSet<Digest>,
    started_at_ms: i64,
    finished_at_ms: i64,
    collected_at_ms: i64,
    rechecked: Option<(i64, i64)>,
    host: Host,
    headers: [Option<Header>; CATEGORIES],
    cells: Vec<FleetOwnedCell>,
    transitioning: Vec<CellId>,
    role_state: RoleState,
    readers: Vec<ReadReplicaLifecycleObservation>,
    reader_enrollments: Vec<ReaderEnrollmentCompletion>,
    follower_lanes: Vec<FollowerLaneObservation>,
    follower_enrollments: Vec<crate::FollowerEnrollmentProgress>,
    supervisor: Option<NodeDurabilitySupervisorObservation>,
}

impl FleetNodeInventory {
    /// Exact authenticated physical endpoint selected by the collector.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Original boot; a successor cannot replace it during this traversal.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Original fully traversed journal inputs, including terminal records.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster
    }
    /// Actual first-to-last capture interval, including all final rechecks.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
    pub(crate) fn coverage_checkpoint(&self) -> (i64, Option<(i64, i64)>) {
        (self.collected_at_ms, self.rechecked)
    }
    pub(crate) fn coverage_digest(&self) -> Result<Digest> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-native-coverage.v1\0");
        hash.update(self.roster.as_bytes());
        hash.update(self.node.as_bytes());
        hash.update(self.session.as_bytes());
        hash.update(&[self.host.state as u8, self.host.mode as u8]);
        for bound in [
            self.host.bindings.managed_startup,
            self.host.bindings.readers,
            self.host.bindings.follower_store,
            self.host.bindings.follower_producer,
            self.host.bindings.durability_supervisor,
        ] {
            hash.update(&[u8::from(bound)]);
        }
        hash.update(&[u8::from(self.host.node_log.is_some())]);
        if let Some((session, node, epoch)) = self.host.node_log {
            hash.update(session.as_bytes());
            hash.update(node.as_bytes());
            hash.update(&epoch.to_be_bytes());
        }
        for header in self.headers {
            let header = header.ok_or(Error::Fenced)?;
            hash.update(&[u8::from(header.bound)]);
            hash.update(&(header.total as u64).to_be_bytes());
            hash.update(header.extra.as_bytes());
            hash.update(&[u8::from(header.topology.is_some())]);
            if let Some(topology) = header.topology {
                hash.update(topology.as_bytes());
            }
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
    /// Sealed host composition observed throughout this traversal.
    #[must_use]
    pub const fn bindings(&self) -> FleetSnapshotBindings {
        self.host.bindings
    }
    /// Shared irreversible admission mode observed throughout capture.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.host.mode
    }
    /// Local active node-log binding; remote authority must still be loaded.
    #[must_use]
    pub const fn node_log(&self) -> Option<(SessionId, NodeId, u64)> {
        self.host.node_log
    }
    /// Generation-bound writers. Transitional obligations are retained separately.
    #[must_use]
    pub fn cells(&self) -> &[FleetOwnedCell] {
        &self.cells
    }
    /// Outstanding actor activation/close/release keys, never absent writers.
    #[must_use]
    pub fn transitioning_cells(&self) -> &[CellId] {
        &self.transitioning
    }
    /// Actual reader admission closure; None retains a missing native binding.
    #[must_use]
    pub const fn readers_closed(&self) -> Option<bool> {
        self.role_state.readers_closed
    }
    /// Original accepted producer jobs, including preparation without a request.
    #[must_use]
    pub fn reader_jobs(&self) -> Option<&ReaderEnrollmentJobs> {
        self.role_state.reader_jobs.as_ref()
    }
    /// Counts unretired lanes and quarantined files. None means unbound.
    #[must_use]
    pub const fn follower_store_state(&self) -> Option<(usize, usize)> {
        self.role_state.follower_store
    }
    /// Protocol busy, draining and original pending epoch. None means unbound.
    #[must_use]
    pub const fn follower_producer_state(&self) -> Option<(bool, bool, Option<u64>)> {
        self.role_state.follower_producer
    }
    /// Begins all-category revalidation after remote authority/membership scans.
    /// The returned collector must finish; its interval starts at the original
    /// traversal. Repeated nonces are rejected across all revalidation rounds.
    pub fn recheck(&mut self) -> FleetNodeInventoryRecheck<'_> {
        FleetNodeInventoryRecheck::new(self)
    }
    /// Original shared native lifecycle observations for every managed view.
    #[must_use]
    pub fn readers(&self) -> &[ReadReplicaLifecycleObservation] {
        &self.readers
    }
    /// Every original producer request and its retained native/publication errors.
    #[must_use]
    pub fn reader_enrollments(&self) -> &[ReaderEnrollmentCompletion] {
        &self.reader_enrollments
    }
    /// All persisted lanes, including sealed, retired and cold lanes.
    #[must_use]
    pub fn follower_lanes(&self) -> &[FollowerLaneObservation] {
        &self.follower_lanes
    }
    /// Original selected ensembles and producer progress, including unknown CAS.
    #[must_use]
    pub fn follower_enrollments(&self) -> &[crate::FollowerEnrollmentProgress] {
        &self.follower_enrollments
    }
    /// Original supervisor lifecycle and accepted automatic/requested rotations.
    #[must_use]
    pub fn supervisor(&self) -> Option<&NodeDurabilitySupervisorObservation> {
        self.supervisor.as_ref()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Host {
    state: NodeState,
    mode: NodeMode,
    bindings: FleetSnapshotBindings,
    node_log: Option<(SessionId, NodeId, u64)>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct Header {
    topology: Option<Digest>,
    total: usize,
    minimum: usize,
    extra: Digest,
    bound: bool,
}

#[derive(Default)]
struct RoleState {
    actor_counts: Option<(usize, usize)>,
    readers_closed: Option<bool>,
    reader_jobs: Option<ReaderEnrollmentJobs>,
    follower_store: Option<(usize, usize)>,
    follower_producer: Option<(bool, bool, Option<u64>)>,
}

/// Streams every canonical native category and then rechecks its full fingerprint.
///
/// Use fresh authenticated requests for `next_subject`, validate responses with
/// the exact request, and release each original page after acceptance. A failed
/// acceptance poisons this scan; restart against a fresh full journal barrier.
/// Matching fingerprints are interval evidence. They do not make open native
/// work atomic or establish remote authority and maintenance finalization.
pub struct FleetNodeInventoryScan<'a> {
    roster: &'a FleetRoster,
    node: NodeId,
    session: SessionId,
    stage: usize,
    next: Option<FleetSnapshotSubject>,
    headers: [Option<Header>; CATEGORIES],
    counts: [usize; CATEGORIES],
    nonces: HashSet<Digest>,
    host: Option<Host>,
    started_at_ms: Option<i64>,
    finished_at_ms: i64,
    poisoned: bool,
    cells: Vec<FleetOwnedCell>,
    transitioning: Vec<CellId>,
    last_cell: Option<CellId>,
    role_state: RoleState,
    readers: Vec<ReadReplicaLifecycleObservation>,
    reader_enrollments: Vec<ReaderEnrollmentCompletion>,
    follower_lanes: Vec<FollowerLaneObservation>,
    follower_enrollments: Vec<crate::FollowerEnrollmentProgress>,
    supervisor: Option<NodeDurabilitySupervisorObservation>,
}
