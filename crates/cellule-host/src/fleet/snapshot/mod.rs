//! Native page collection through the existing retained fleet job owner.
use super::FleetJournalSnapshot;
use crate::NodeState;
use crate::read_replicas::ReadReplicaManager;
use cellule_runtime::{
    Error,
    identity::{Digest, NodeId, SessionId},
    node::NodeMode,
};
use std::sync::{Arc, Mutex};

mod capture;
mod request;
pub use request::{FleetSnapshotRequest, FleetSnapshotSubject};

/// Exact local installed owner bindings. Unbound owners supply no role coverage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetSnapshotBindings {
    /// A matching managed startup intent was confirmed before capture admission.
    pub managed_startup: bool,
    /// The canonical reader manager is installed.
    pub readers: bool,
    /// The canonical inbound follower store is installed.
    pub follower_store: bool,
    /// The retained original supervisor owner is installed.
    pub durability_supervisor: bool,
    /// Its managed follower producer is installed.
    pub follower_producer: bool,
}

/// One original native page, retaining its existing allocation token and errors.
pub enum FleetSnapshotNativePage {
    /// Fixed host metadata lives in the enclosing snapshot.
    Host,
    /// Original generation-bound actor inventory.
    Cells(cellule_runtime::cell::actor::CellInventoryPage),
    /// Original managed reader inventory.
    Readers(crate::read_replicas::ReaderInventoryPage),
    /// Original reader producer progress, including accepted preparation jobs.
    ReaderEnrollments(crate::read_replicas::ReaderEnrollmentInventoryPage),
    /// Original persisted inbound lane inventory.
    FollowerLanes(cellule_runtime::follower::FollowerInventoryPage),
    /// Original managed leader producer inventory.
    FollowerEnrollments(crate::FollowerEnrollmentInventoryPage),
    /// Original supervisor join state and bounded request bank.
    DurabilitySupervisor(crate::NodeDurabilitySupervisorObservation),
    /// The requested owner/binding is missing. This is never an empty page or absence proof.
    Unbound,
}

/// Request-bound local interval evidence. Authentication, stable full traversal,
/// current remote authority and replacement-policy proof remain adapter duties.
/// Native pages retain their original bounded charges until this response drops.
pub struct FleetNodeSnapshot {
    request: FleetSnapshotRequest,
    started_at_ms: i64,
    finished_at_ms: i64,
    state_before: NodeState,
    state_after: NodeState,
    mode: NodeMode,
    bindings: FleetSnapshotBindings,
    node_log: Option<(SessionId, NodeId, u64)>,
    page: FleetSnapshotNativePage,
}
impl FleetNodeSnapshot {
    /// Returns the exact original request and journal barrier.
    #[must_use]
    pub fn request(&self) -> &FleetSnapshotRequest {
        &self.request
    }
    /// Returns the actual capture start after pre-authorization.
    #[must_use]
    pub const fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }
    /// Returns the actual completion after post-authorization.
    #[must_use]
    pub const fn finished_at_ms(&self) -> i64 {
        self.finished_at_ms
    }
    /// Returns lifecycle before the original native read.
    #[must_use]
    pub const fn state_before(&self) -> NodeState {
        self.state_before
    }
    /// Returns lifecycle after the original native read.
    #[must_use]
    pub const fn state_after(&self) -> NodeState {
        self.state_after
    }
    /// Returns shared admission mode at completion, independently of counts.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Returns installed owner bindings; no missing owner supplies role coverage.
    #[must_use]
    pub const fn bindings(&self) -> FleetSnapshotBindings {
        self.bindings
    }
    /// Returns the current local log binding, without remote membership/closure authority.
    #[must_use]
    pub const fn node_log(&self) -> Option<(SessionId, NodeId, u64)> {
        self.node_log
    }
    /// Returns the original native page, never a cached movement result.
    #[must_use]
    pub fn page(&self) -> &FleetSnapshotNativePage {
        &self.page
    }
    /// Validates complete request equality and the original non-restamped interval.
    /// This supplies no transport authentication or finalization permission.
    pub fn validate(
        &self,
        request: &FleetSnapshotRequest,
        now_ms: i64,
    ) -> cellule_runtime::Result<()> {
        if &self.request != request
            || self.started_at_ms < request.issued_at_ms()
            || self.finished_at_ms < self.started_at_ms
            || self.finished_at_ms >= request.deadline_ms()
            || now_ms < self.finished_at_ms
            || now_ms >= request.deadline_ms()
        {
            return Err(Error::Node("native snapshot request or interval differs"));
        }
        let scope = request.expected().head().scope();
        let (tag, observed, identity) = match &self.page {
            FleetSnapshotNativePage::Host => (1, self.started_at_ms, true),
            FleetSnapshotNativePage::Cells(page) => (
                2,
                page.observed_at_ms(),
                page.session() == request.session() && page.entries().len() <= request.limit(),
            ),
            FleetSnapshotNativePage::Readers(page) => (
                3,
                page.observed_at_ms(),
                page.session() == request.session() && page.entries().len() <= request.limit(),
            ),
            FleetSnapshotNativePage::ReaderEnrollments(page) => (
                4,
                page.observed_at_ms(),
                page.session() == request.session()
                    && page.node() == request.node()
                    && page.scope() == scope
                    && page.entries().len() <= request.limit(),
            ),
            FleetSnapshotNativePage::FollowerLanes(page) => (
                5,
                page.observed_at_ms(),
                page.entries().len() <= request.limit(),
            ),
            FleetSnapshotNativePage::FollowerEnrollments(page) => (
                6,
                page.observed_at_ms(),
                page.session() == request.session()
                    && page.node() == request.node()
                    && page.scope() == scope
                    && page.entries().len() <= request.limit(),
            ),
            FleetSnapshotNativePage::DurabilitySupervisor(observed) => (
                7,
                observed.observed_at_ms,
                observed.session == request.session() && observed.application == scope.application,
            ),
            FleetSnapshotNativePage::Unbound => (request.subject().tag(), self.started_at_ms, true),
        };
        if tag != request.subject().tag()
            || !identity
            || observed < self.started_at_ms
            || observed > self.finished_at_ms
        {
            return Err(Error::Node(
                "native snapshot page identity or capture differs",
            ));
        }
        if self.node_log.is_some_and(|(session, node, epoch)| {
            session != request.session() || node != request.node() || epoch == 0
        }) {
            return Err(Error::Fenced);
        }
        Ok(())
    }
}

pub(crate) struct SnapshotOwners {
    pub(crate) state: Arc<Mutex<NodeState>>,
    pub(crate) managed_startup: bool,
    pub(crate) readers: Option<Arc<ReadReplicaManager>>,
    pub(crate) followers: Option<Arc<cellule_runtime::follower::FollowerStore>>,
    pub(crate) supervisor: Option<Arc<crate::durability::DurabilitySupervisor>>,
    pub(crate) producer: Option<Arc<crate::durability::enrollment::FleetFollowerEnrollment>>,
}
impl SnapshotOwners {
    fn state(&self) -> cellule_runtime::Result<NodeState> {
        self.state
            .lock()
            .map(|s| *s)
            .map_err(|_| Error::Control("native snapshot lifecycle lock poisoned"))
    }
    fn bindings(&self) -> FleetSnapshotBindings {
        FleetSnapshotBindings {
            managed_startup: self.managed_startup,
            readers: self.readers.is_some(),
            follower_store: self.followers.is_some(),
            durability_supervisor: self.supervisor.is_some(),
            follower_producer: self.producer.is_some(),
        }
    }
}

#[cfg(test)]
mod tests;
