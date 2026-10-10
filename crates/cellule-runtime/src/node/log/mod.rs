//! Node log: durability gate, rotation barrier, and recovery overlays.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::identity::NodeId;
use crate::identity::{ApplicationId, CellId, IncarnationId, SessionId};
use crate::node::log_transport::NodeLogTransport;
use crate::node::{NodeDirectory, VersionedNodeAdvertisement};
use crate::{Error, Result};

mod recovery;
mod retirement;
pub(crate) use retirement::retire_node_log;
pub use retirement::{
    NodeLogMemberRetirement, NodeLogRetirementObservation, NodeLogRetirementProof,
};

pub use recovery::*;

pub(crate) const MAX_TICKET_FRAMES: u64 = 1_024;
static NEXT_GATE_INSTANCE: AtomicU64 = AtomicU64::new(1);

/// Exact Cell writer identity carried by the ordered native frame lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CellLogScope {
    /// Application that owns the Cell.
    pub application: ApplicationId,
    /// Cell whose issuance will be closed.
    pub cell: CellId,
    /// Original incarnation.
    pub incarnation: IncarnationId,
    /// Original writer epoch.
    pub cell_epoch: u64,
}

/// Frozen complete issued endpoint, including frames acknowledged by followers
/// above the selected object prefix. Only the ordered gate can construct it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellIssuedRange {
    leader_session: SessionId,
    log_epoch: u64,
    scope: CellLogScope,
    last_node_sequence: u64,
    commit_sequence: u64,
    first_commit_sequence: u64,
    position: cellule_ltx::Position,
}

/// Exact complete capture assigned by the ordered native lane. A frame prefix
/// cannot substitute for this capability even when it carries the final logical
/// command number. Its digest binds every assigned native frame in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AssignedCommitRange {
    gate_instance: u64,
    ticket: CommitTicket,
    scope: CellLogScope,
    first_commit: u64,
    commit: u64,
    position: cellule_ltx::Position,
    digest: [u8; 32],
    capture_digest: [u8; 32],
}
impl AssignedCommitRange {
    pub(crate) const fn endpoint(&self) -> (u64, u64, cellule_ltx::Position) {
        (self.first_commit, self.commit, self.position)
    }

    pub(crate) const fn scope(&self) -> CellLogScope {
        self.scope
    }

    /// Exact original native ticket.
    pub const fn ticket(&self) -> CommitTicket {
        self.ticket
    }

    pub(crate) fn matches_capture(
        &self,
        cell: CellId,
        incarnation: IncarnationId,
        commit: u64,
        cuts: &cellule_ltx::CaptureBatch,
    ) -> bool {
        self.scope.cell == cell
            && self.scope.incarnation == incarnation
            && self.commit == commit
            && self.position == cuts.position
            && cuts.segments.len() as u64
                == self.ticket.last_sequence - self.ticket.first_sequence + 1
            && self.capture_digest
                == capture_digest(cuts.segments.iter().map(cellule_ltx::LocalSegment::info))
    }
    pub(crate) fn verify(&self, frames: &[cellule_ltx::VerifiedNodeFrame]) -> Result<()> {
        if frames.is_empty()
            || frames.len() as u64 != self.ticket.last_sequence - self.ticket.first_sequence + 1
        {
            return Err(Error::Node("bundle omits part of an assigned capture"));
        }
        let mut hash = blake3::Hasher::new();
        for (offset, frame) in frames.iter().enumerate() {
            let scope = frame.scope();
            if scope.leader_session != *self.ticket.leader_session.as_bytes()
                || scope.log_epoch != self.ticket.log_epoch
                || scope.node_sequence != self.ticket.first_sequence + offset as u64
                || scope.application != *self.scope.application.as_bytes()
                || scope.cell != *self.scope.cell.as_bytes()
                || scope.incarnation != *self.scope.incarnation.as_bytes()
                || scope.cell_epoch != self.scope.cell_epoch
                || frame.first_commit_sequence() != self.first_commit
                || scope.commit_sequence != self.commit
            {
                return Err(Error::Node("bundle assigned capture scope differs"));
            }
            hash.update(&frame.digest());
        }
        if *hash.finalize().as_bytes() != self.digest
            || frames
                .last()
                .is_none_or(|frame| frame.segment().position() != self.position)
        {
            return Err(Error::Node("bundle assigned capture digest differs"));
        }
        Ok(())
    }
}

fn capture_digest<'a>(segments: impl Iterator<Item = &'a cellule_ltx::SegmentInfo>) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-assigned-capture-v1");
    for segment in segments {
        hash.update(&segment.min_txid.to_le_bytes());
        hash.update(&segment.max_txid.to_le_bytes());
        hash.update(&segment.page_size.to_le_bytes());
        hash.update(&segment.database_pages.to_le_bytes());
        hash.update(&segment.pre_checksum.to_le_bytes());
        hash.update(&segment.post_checksum.to_le_bytes());
        hash.update(&segment.size_bytes.to_le_bytes());
        hash.update(&segment.blake3);
    }
    *hash.finalize().as_bytes()
}

impl CellIssuedRange {
    /// Original lane session.
    pub const fn leader_session(&self) -> SessionId {
        self.leader_session
    }
    /// Original lane epoch.
    pub const fn log_epoch(&self) -> u64 {
        self.log_epoch
    }
    /// Exact original writer.
    pub const fn scope(&self) -> CellLogScope {
        self.scope
    }
    /// Complete assigned native endpoint, rather than sampled follower coverage.
    pub const fn last_node_sequence(&self) -> u64 {
        self.last_node_sequence
    }
    /// Complete logical command endpoint.
    pub const fn commit_sequence(&self) -> u64 {
        self.commit_sequence
    }
    /// Exact SQLite position of the complete issued range.
    pub const fn position(&self) -> cellule_ltx::Position {
        self.position
    }
}

/// One actor-issued consecutive frame range awaiting a durability proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitTicket {
    leader_session: SessionId,
    log_epoch: u64,
    first_sequence: u64,
    last_sequence: u64,
}

impl CommitTicket {
    /// Returns the leader session that issued the ticket.
    #[must_use]
    pub const fn leader_session(&self) -> SessionId {
        self.leader_session
    }

    /// Returns the node-log epoch the commit was written under.
    #[must_use]
    pub const fn log_epoch(&self) -> u64 {
        self.log_epoch
    }

    /// Returns the first node-log sequence the ticket covers.
    #[must_use]
    pub const fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    /// Returns the last node-log sequence the ticket covers.
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
}

/// Durable path that authorized release of one committed result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilitySource {
    /// The commit was covered by the enrolled node-log lane.
    Fleet,
    /// The commit was covered by an exact materialized Cell root in object storage.
    Object,
    /// A verified node bundle is selected in origin; the Cell root may lag.
    Bundle,
}

/// Non-forgeable proof issued by the gate after one complete path wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurabilityProof {
    ticket: CommitTicket,
    source: DurabilitySource,
}

/// Proof that one gate stopped issuance after every old-epoch ticket became object-covered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLogRotationBarrier {
    leader_session: SessionId,
    log_epoch: u64,
    members: Vec<NodeId>,
    covered_through: u64,
}

/// New authoritative enrollment and inactive durability gate after rotation.
pub struct RotatedNodeLog {
    /// Enrollment the rotation installed.
    pub enrollment: VersionedNodeAdvertisement,
    /// Inactive durability gate the rotated log starts from.
    pub gate: DurabilityGate,
}

impl NodeLogRotationBarrier {
    /// Returns the session the barrier was issued for.
    #[must_use]
    pub const fn leader_session(&self) -> SessionId {
        self.leader_session
    }

    /// Returns the sealed log epoch.
    #[must_use]
    pub const fn log_epoch(&self) -> u64 {
        self.log_epoch
    }

    /// Returns the members the rotation enrolled.
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// Returns the highest sequence the sealed log covered.
    #[must_use]
    pub const fn covered_through(&self) -> u64 {
        self.covered_through
    }
}

impl DurabilityProof {
    /// Returns the ticket this proof covers.
    #[must_use]
    pub const fn ticket(&self) -> CommitTicket {
        self.ticket
    }

    /// Returns which durability path issued the proof.
    #[must_use]
    pub const fn source(&self) -> DurabilitySource {
        self.source
    }
}

/// Coordinates object and follower durability without exposing forgeable ACKs.
///
/// The gate owns one leader session and one log epoch. Tickets are issued in
/// strict node-sequence order. Fleet proof is disabled until the session's
/// `active=false -> active=true` CAS has been observed by the caller.
#[derive(Clone)]
pub struct DurabilityGate {
    inner: Arc<Mutex<GateState>>,
    changed: Arc<Notify>,
}

/// One consistent observation of an epoch's durability frontiers.
///
/// Observations grant no durability, recovery, rotation, or deletion authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeLogProgress {
    /// Epoch these frontiers describe.
    pub log_epoch: u64,
    /// Highest issued node-log sequence.
    pub issued_through: u64,
    /// Highest sequence fsynced by every enrolled follower.
    pub follower_proven_through: u64,
    /// Contiguous prefix covered by authoritative object publication.
    pub tiered_through: u64,
    /// Issued sequences beyond the contiguous object-covered prefix.
    pub pending_object_sequences: u64,
    /// Whether authoritative activation enabled fleet proofs.
    pub fleet_active: bool,
    /// Whether issuance has stopped for rotation.
    pub rotating: bool,
    /// Whether the gate has been fenced.
    pub fenced: bool,
}

struct GateState {
    instance: u64,
    leader_session: SessionId,
    leader_node: NodeId,
    log_epoch: u64,
    members: HashSet<NodeId>,
    follower_through: HashMap<NodeId, u64>,
    // Keep only completed sequences above the contiguous prefix. The prefix
    // proves older tickets without one allocation per historical frame.
    object_covered: BTreeSet<u64>,
    // Merge adjacent exact confirmations. A dense selected prefix occupies
    // one entry without retaining one source tag per historical native frame.
    bundle_covered: BTreeMap<u64, u64>,
    tiered_through: u64,
    next_sequence: u64,
    fleet_active: bool,
    rotating: bool,
    fenced: bool,
    cell_issued: HashMap<CellLogScope, CellIssuedRange>,
    closed_cells: HashSet<CellLogScope>,
    untracked_issuance: bool,
}

impl DurabilityGate {
    /// Samples all frontiers under one lock without issuing a proof.
    pub fn progress(&self) -> Result<NodeLogProgress> {
        let state = self.lock()?;
        let issued_through = state.next_sequence.saturating_sub(1);
        Ok(NodeLogProgress {
            log_epoch: state.log_epoch,
            issued_through,
            follower_proven_through: state
                .members
                .iter()
                .map(|member| state.follower_through.get(member).copied().unwrap_or(0))
                .min()
                .unwrap_or(0),
            tiered_through: state.tiered_through,
            pending_object_sequences: issued_through.saturating_sub(state.tiered_through),
            fleet_active: state.fleet_active,
            rotating: state.rotating,
            fenced: state.fenced,
        })
    }

    /// Creates one inactive gate for the exact recruited follower ensemble.
    pub fn new(
        leader_session: SessionId,
        leader_node: NodeId,
        log_epoch: u64,
        members: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self> {
        let members = members.into_iter().collect::<HashSet<_>>();
        if leader_session.as_bytes().iter().all(|byte| *byte == 0)
            || leader_node.as_bytes().iter().all(|byte| *byte == 0)
            || log_epoch == 0
            || members.is_empty()
            || members.contains(&leader_node)
            || members
                .iter()
                .any(|member| member.as_bytes().iter().all(|byte| *byte == 0))
        {
            return Err(Error::Node("invalid node-log ensemble"));
        }
        let follower_through = members.iter().map(|member| (*member, 0)).collect();
        // Not a persisted identity: exact assignments cannot be confirmed by a
        // second in-process gate even when boot, epoch and ticket numbers match.
        let mut instance = NEXT_GATE_INSTANCE.load(Ordering::Relaxed);
        loop {
            let next = instance
                .checked_add(1)
                .ok_or(Error::Node("durability gate instance overflow"))?;
            match NEXT_GATE_INSTANCE.compare_exchange_weak(
                instance,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => instance = current,
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(GateState {
                instance,
                leader_session,
                leader_node,
                log_epoch,
                members,
                follower_through,
                object_covered: BTreeSet::new(),
                bundle_covered: BTreeMap::new(),
                tiered_through: 0,
                next_sequence: 1,
                fleet_active: false,
                rotating: false,
                fenced: false,
                cell_issued: HashMap::new(),
                closed_cells: HashSet::new(),
                untracked_issuance: false,
            })),
            changed: Arc::new(Notify::new()),
        })
    }

    /// Allocates the next consecutive frame range after SQLite capture.
    pub fn issue(&self, frame_count: u64) -> Result<CommitTicket> {
        if !(1..=MAX_TICKET_FRAMES).contains(&frame_count) {
            return Err(Error::Node("invalid node-log ticket size"));
        }
        let mut state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if state.rotating {
            return Err(Error::Node("node log is rotating"));
        }
        let first_sequence = state.next_sequence;
        let last_sequence = first_sequence
            .checked_add(frame_count - 1)
            .ok_or(Error::Node("node sequence overflow"))?;
        state.next_sequence = last_sequence
            .checked_add(1)
            .ok_or(Error::Node("node sequence overflow"))?;
        // This legacy API has no Cell identity. Its ranges cannot establish a
        // complete per-Cell closure endpoint; the canonical shipper uses the
        // verified commit_frames path instead.
        state.untracked_issuance = true;
        Ok(CommitTicket {
            leader_session: state.leader_session,
            log_epoch: state.log_epoch,
            first_sequence,
            last_sequence,
        })
    }

    pub(crate) fn preview(&self, frame_count: u64) -> Result<CommitTicket> {
        if !(1..=MAX_TICKET_FRAMES).contains(&frame_count) {
            return Err(Error::Node("invalid node-log ticket size"));
        }
        let state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if state.rotating {
            return Err(Error::Node("node log is rotating"));
        }
        let first_sequence = state.next_sequence;
        let last_sequence = first_sequence
            .checked_add(frame_count - 1)
            .ok_or(Error::Node("node sequence overflow"))?;
        Ok(CommitTicket {
            leader_session: state.leader_session,
            log_epoch: state.log_epoch,
            first_sequence,
            last_sequence,
        })
    }

    pub(crate) fn commit_frames(
        &self,
        ticket: CommitTicket,
        frames: &[cellule_ltx::VerifiedNodeFrame],
    ) -> Result<Option<AssignedCommitRange>> {
        let mut state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if state.rotating {
            return Err(Error::Node("node log is rotating"));
        }
        if ticket.leader_session != state.leader_session
            || ticket.log_epoch != state.log_epoch
            || ticket.first_sequence != state.next_sequence
            || ticket.first_sequence == 0
            || ticket.first_sequence > ticket.last_sequence
            || ticket.last_sequence.saturating_sub(ticket.first_sequence) >= MAX_TICKET_FRAMES
        {
            return Err(Error::Node("node-log ticket reservation changed"));
        }
        // One submission is one complete Cell capture. Stage its endpoint on
        // the stack, then install it only after every frame passed validation.
        let mut issued: Option<CellIssuedRange> = None;
        let mut assignment = None;
        if !frames.is_empty()
            && frames.len() as u64 != ticket.last_sequence - ticket.first_sequence + 1
        {
            return Err(Error::Node("node-log assigned frame range differs"));
        }
        for (index, frame) in frames.iter().enumerate() {
            let scope = frame.scope();
            let cell_scope = CellLogScope {
                application: ApplicationId::from_bytes(scope.application),
                cell: CellId::from_bytes(scope.cell),
                incarnation: IncarnationId::from_bytes(scope.incarnation),
                cell_epoch: scope.cell_epoch,
            };
            match &mut assignment {
                None => {
                    assignment = Some(AssignedCommitRange {
                        gate_instance: state.instance,
                        ticket,
                        scope: cell_scope,
                        first_commit: frame.first_commit_sequence(),
                        commit: scope.commit_sequence,
                        position: frame.segment().position(),
                        digest: [0; 32],
                        capture_digest: [0; 32],
                    })
                }
                Some(assignment) => {
                    if assignment.scope != cell_scope
                        || assignment.first_commit != frame.first_commit_sequence()
                        || assignment.commit != scope.commit_sequence
                    {
                        return Err(Error::Node(
                            "assigned capture changes Cell or command range",
                        ));
                    }
                    assignment.position = frame.segment().position();
                }
            }
            if state.closed_cells.contains(&cell_scope) {
                return Err(Error::Fenced);
            }
            if scope.leader_session != *state.leader_session.as_bytes()
                || scope.log_epoch != state.log_epoch
                || scope.node_sequence != ticket.first_sequence + index as u64
            {
                return Err(Error::Node("node-log assigned frame scope differs"));
            }
            if let Some(previous) = issued.as_ref() {
                let continuing_group = scope.commit_sequence == previous.commit_sequence;
                if (continuing_group
                    && frame.first_commit_sequence() != previous.first_commit_sequence)
                    || (!continuing_group
                        && frame.first_commit_sequence()
                            != previous
                                .commit_sequence
                                .checked_add(1)
                                .ok_or(Error::Node("Cell commit overflow"))?)
                    || frame.segment().min_txid
                        != previous
                            .position
                            .txid
                            .checked_add(1)
                            .ok_or(Error::Node("Cell TXID overflow"))?
                    || frame.segment().pre_checksum != previous.position.checksum
                {
                    return Err(Error::Node("node-log Cell issuance has a gap"));
                }
            } else if state.cell_issued.get(&cell_scope).is_some_and(|previous| {
                // Separate captures may have intervening object-only commands.
                // Require forward issuance here; only the bundle verifier can
                // establish exact continuity from its authority-pinned base.
                scope.commit_sequence <= previous.commit_sequence
                    || frame.first_commit_sequence() <= previous.commit_sequence
                    || frame.segment().max_txid <= previous.position.txid
            }) {
                return Err(Error::Node("node-log Cell issuance did not advance"));
            }
            if !state.cell_issued.contains_key(&cell_scope) && state.cell_issued.len() >= 4_096 {
                return Err(Error::Capacity("node-log Cell issuance bindings"));
            }
            issued = Some(CellIssuedRange {
                leader_session: state.leader_session,
                log_epoch: state.log_epoch,
                scope: cell_scope,
                last_node_sequence: scope.node_sequence,
                commit_sequence: scope.commit_sequence,
                first_commit_sequence: frame.first_commit_sequence(),
                position: frame.segment().position(),
            });
        }
        state.next_sequence = ticket
            .last_sequence
            .checked_add(1)
            .ok_or(Error::Node("node sequence overflow"))?;
        if let Some(issued) = issued {
            state.cell_issued.insert(issued.scope, issued);
        }
        if let Some(assignment) = &mut assignment {
            let mut hash = blake3::Hasher::new();
            for frame in frames {
                hash.update(&frame.digest());
            }
            assignment.digest = *hash.finalize().as_bytes();
            assignment.capture_digest = capture_digest(frames.iter().map(|frame| frame.segment()));
        }
        Ok(assignment)
    }

    /// Closes this writer's sequence assignment in the same lock as native
    /// ticket commit. Call after joining its accepted SQL/capture/submission
    /// jobs. Late assignment fails without consuming a global sequence.
    pub fn close_cell_issuance(
        &self,
        scope: CellLogScope,
        base: cellule_ltx::RootRef,
    ) -> Result<CellIssuedRange> {
        let mut state = self.lock()?;
        if state.fenced || state.untracked_issuance {
            return Err(Error::Fenced);
        }
        if scope.cell_epoch == 0
            || scope.cell.as_bytes().iter().all(|byte| *byte == 0)
            || base.cell != *scope.cell.as_bytes()
            || base.incarnation != *scope.incarnation.as_bytes()
        {
            return Err(Error::Node("invalid Cell issuance closure"));
        }
        if !state.closed_cells.contains(&scope) && state.closed_cells.len() >= 4_096 {
            return Err(Error::Capacity("node-log closed Cell bindings"));
        }
        state.closed_cells.insert(scope);
        Ok(state
            .cell_issued
            .get(&scope)
            .copied()
            .unwrap_or(CellIssuedRange {
                leader_session: state.leader_session,
                log_epoch: state.log_epoch,
                scope,
                last_node_sequence: 0,
                first_commit_sequence: base.commit_sequence,
                commit_sequence: base.commit_sequence,
                position: base.position,
            }))
    }

    /// Returns this gate's immutable enrolled epoch, including after rotation.
    pub fn log_epoch(&self) -> Result<u64> {
        Ok(self.lock()?.log_epoch)
    }

    /// Returns the immutable configured boot, physical node and epoch. This
    /// metadata is not a signed authority observation or durability proof.
    pub fn identity(&self) -> Result<(SessionId, NodeId, u64)> {
        let state = self.lock()?;
        Ok((state.leader_session, state.leader_node, state.log_epoch))
    }

    pub(crate) fn shipping_scope(&self) -> Result<(SessionId, u64, Vec<NodeId>)> {
        let state = self.lock()?;
        if state.fenced || state.rotating {
            return Err(Error::Fenced);
        }
        let mut members = state.members.iter().copied().collect::<Vec<_>>();
        members.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        Ok((state.leader_session, state.log_epoch, members))
    }

    pub(crate) fn stop_shipping(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.fleet_active = false;
            state.rotating = true;
        }
        self.changed.notify_waiters();
    }

    /// Enables fleet proofs only after the authoritative active CAS succeeds.
    pub fn activate_fleet(&self) -> Result<()> {
        let mut state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if state.rotating {
            return Err(Error::Node("node log is rotating"));
        }
        state.fleet_active = true;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    /// Records one authenticated follower's fsynced contiguous watermark.
    pub fn acknowledge(&self, member: NodeId, durable_through: u64) -> Result<()> {
        let mut state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if state.rotating {
            return Err(Error::Node("node log is rotating"));
        }
        let next_sequence = state.next_sequence;
        let current = state
            .follower_through
            .get_mut(&member)
            .ok_or(Error::Node("node-log ACK came from a non-member"))?;
        if durable_through < *current || durable_through >= next_sequence {
            return Err(Error::Node("node-log ACK watermark is invalid"));
        }
        *current = durable_through;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    /// Waits until every enrolled follower has fsynced the complete ticket.
    ///
    /// This does not enable fleet durability. The caller must first complete
    /// the authoritative inactive-to-active CAS and then call `activate_fleet`.
    pub async fn wait_followers(&self, ticket: CommitTicket) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.lock()?;
                validate_ticket(&state, ticket)?;
                if state.fenced {
                    return Err(Error::Fenced);
                }
                if state.rotating {
                    return Err(Error::Node("node log is rotating"));
                }
                if state.members.iter().all(|member| {
                    state
                        .follower_through
                        .get(member)
                        .is_some_and(|through| *through >= ticket.last_sequence)
                }) {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// Marks exactly the frame range now reachable through an authoritative root.
    pub fn prove_object(&self, ticket: CommitTicket) -> Result<u64> {
        self.prove_objects(&[ticket])
    }

    pub(crate) fn objects_are_covered(&self, tickets: &[CommitTicket]) -> Result<bool> {
        let state = self.lock()?;
        for ticket in tickets {
            validate_ticket(&state, *ticket)?;
        }
        if state.fenced {
            return Err(Error::Fenced);
        }
        Ok(tickets.iter().all(|ticket| object_covers(&state, *ticket)))
    }

    pub(crate) fn uncovered_objects(&self, tickets: &[CommitTicket]) -> Result<Vec<CommitTicket>> {
        let state = self.lock()?;
        for ticket in tickets {
            validate_ticket(&state, *ticket)?;
        }
        if state.fenced {
            return Err(Error::Fenced);
        }
        Ok(tickets
            .iter()
            .copied()
            .filter(|ticket| !object_covers(&state, *ticket))
            .collect())
    }

    pub(crate) fn prove_objects(&self, tickets: &[CommitTicket]) -> Result<u64> {
        let mut state = self.lock()?;
        // Validate the entire batch before changing any proof. One bad scope
        // cannot release valid siblings before the caller observes an error.
        for ticket in tickets {
            validate_ticket(&state, *ticket)?;
        }
        if state.fenced {
            return Err(Error::Fenced);
        }
        mark_object_coverage(&mut state, tickets.iter().copied());
        let tiered_through = state.tiered_through;
        drop(state);
        self.changed.notify_waiters();
        Ok(tiered_through)
    }

    pub(crate) fn confirm_bundle_ranges(&self, assignments: &[AssignedCommitRange]) -> Result<u64> {
        let mut state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        for assignment in assignments {
            validate_ticket(&state, assignment.ticket)?;
            if assignment.gate_instance != state.instance {
                return Err(Error::Node(
                    "selected capture belongs to another durability gate",
                ));
            }
        }
        for assignment in assignments {
            mark_bundle_coverage(&mut state, assignment.ticket);
        }
        mark_object_coverage(
            &mut state,
            assignments.iter().map(|assignment| assignment.ticket),
        );
        let through = state.tiered_through;
        drop(state);
        self.changed.notify_waiters();
        Ok(through)
    }

    pub(crate) fn confirmed_object_proof(&self, ticket: CommitTicket) -> Result<DurabilityProof> {
        let state = self.lock()?;
        validate_ticket(&state, ticket)?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if !object_covers(&state, ticket) {
            return Err(Error::Node("object ticket remains unconfirmed"));
        }
        // This caller completed an exact Cell root CAS. A bundle that won the
        // earlier generic race must not turn materialization into an error.
        Ok(DurabilityProof {
            ticket,
            source: DurabilitySource::Object,
        })
    }

    pub(crate) fn preview_objects(&self, tickets: &[CommitTicket]) -> Result<u64> {
        let state = self.lock()?;
        for ticket in tickets {
            validate_ticket(&state, *ticket)?;
        }
        if state.fenced {
            return Err(Error::Fenced);
        }
        let covered = tickets
            .iter()
            .flat_map(|ticket| ticket.first_sequence..=ticket.last_sequence)
            .collect::<BTreeSet<_>>();
        let mut tiered_through = state.tiered_through;
        while let Some(next) = tiered_through.checked_add(1) {
            if state.object_covered.contains(&next) || covered.contains(&next) {
                tiered_through = next;
            } else {
                break;
            }
        }
        Ok(tiered_through)
    }

    /// Returns the contiguous authoritative object prefix, including selected bundles.
    #[must_use]
    pub fn tiered_through(&self) -> u64 {
        self.lock().map_or(0, |state| state.tiered_through)
    }

    /// Returns the highest sequence issued to the follower lane.
    #[must_use]
    pub fn issued_through(&self) -> u64 {
        self.lock()
            .map_or(0, |state| state.next_sequence.saturating_sub(1))
    }

    /// Stops ticket issuance after the entire old epoch is object-covered.
    pub fn begin_rotation(&self) -> Result<NodeLogRotationBarrier> {
        let mut state = self.lock()?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        let issued_through = state.next_sequence.saturating_sub(1);
        if state.tiered_through != issued_through {
            return Err(Error::PendingPublication);
        }
        state.rotating = true;
        state.fleet_active = false;
        let mut members = state.members.iter().copied().collect::<Vec<_>>();
        members.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let barrier = NodeLogRotationBarrier {
            leader_session: state.leader_session,
            log_epoch: state.log_epoch,
            members,
            covered_through: state.tiered_through,
        };
        drop(state);
        self.changed.notify_waiters();
        Ok(barrier)
    }

    /// Waits until either complete durability path covers the whole ticket.
    pub async fn prove(&self, ticket: CommitTicket) -> Result<DurabilityProof> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(proof) = self.proof(ticket)? {
                return Ok(proof);
            }
            notified.await;
        }
    }

    /// Permanently rejects new tickets and wakes every waiting command.
    pub fn fence(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.fenced = true;
        }
        self.changed.notify_waiters();
    }

    fn proof(&self, ticket: CommitTicket) -> Result<Option<DurabilityProof>> {
        let state = self.lock()?;
        validate_ticket(&state, ticket)?;
        if state.fenced {
            return Err(Error::Fenced);
        }
        if object_covers(&state, ticket) {
            return Ok(Some(DurabilityProof {
                ticket,
                source: if state
                    .bundle_covered
                    .range(..=ticket.first_sequence)
                    .next_back()
                    .is_some_and(|(_, through)| *through >= ticket.last_sequence)
                {
                    DurabilitySource::Bundle
                } else {
                    DurabilitySource::Object
                },
            }));
        }
        if state.fleet_active
            && state.members.iter().all(|member| {
                state
                    .follower_through
                    .get(member)
                    .is_some_and(|through| *through >= ticket.last_sequence)
            })
        {
            return Ok(Some(DurabilityProof {
                ticket,
                source: DurabilitySource::Fleet,
            }));
        }
        Ok(None)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, GateState>> {
        self.inner
            .lock()
            .map_err(|_| Error::Node("node-log durability gate poisoned"))
    }
}

/// Best-effort retires old follower lanes before CASing a newly selected log epoch.
///
/// Unreachable followers may retain inert data, but cannot block rotation.
/// A CAS failure leaves the old gate closed to new tickets. Retrying is safe:
/// the barrier and follower retire markers are exact and idempotent.
pub async fn rotate_node_log(
    directory: &NodeDirectory,
    transport: Arc<dyn NodeLogTransport>,
    observed: &VersionedNodeAdvertisement,
    gate: &DurabilityGate,
    required_follower_bytes: u64,
    live_node_limit: usize,
    now_ms: i64,
) -> Result<RotatedNodeLog> {
    let barrier = gate.begin_rotation()?;
    retire_node_log(transport, &barrier).await?;
    let enrollment = directory
        .rotate_log(
            observed,
            &barrier,
            required_follower_bytes,
            live_node_limit,
            now_ms,
        )
        .await?;
    let log = enrollment
        .advertisement()
        .log()
        .ok_or(Error::Node("rotated node session lost its log"))?;
    let gate = DurabilityGate::new(
        enrollment.advertisement().session(),
        enrollment.advertisement().node(),
        log.epoch(),
        log.members().iter().copied(),
    )?;
    Ok(RotatedNodeLog { enrollment, gate })
}

/// Best-effort retires a covered epoch and CAS-clears it for clean withdrawal.
///
/// Unreachable followers may retain inert bytes. The authoritative clear
/// rejects every later append before the session can be withdrawn.
pub async fn close_node_log(
    directory: &NodeDirectory,
    transport: Arc<dyn NodeLogTransport>,
    observed: &VersionedNodeAdvertisement,
    gate: &DurabilityGate,
    now_ms: i64,
) -> Result<VersionedNodeAdvertisement> {
    let barrier = gate.begin_rotation()?;
    retire_node_log(transport, &barrier).await?;
    directory.close_log(observed, &barrier, now_ms).await
}

fn mark_bundle_coverage(state: &mut GateState, ticket: CommitTicket) {
    let mut first = ticket.first_sequence;
    let mut last = ticket.last_sequence;
    if let Some((&before, &through)) = state.bundle_covered.range(..=first).next_back()
        && through >= first.saturating_sub(1)
    {
        first = before;
        last = last.max(through);
        state.bundle_covered.remove(&before);
    }
    while let Some((&after, &through)) = state.bundle_covered.range(first..).next() {
        if after > last.saturating_add(1) {
            break;
        }
        last = last.max(through);
        state.bundle_covered.remove(&after);
    }
    state.bundle_covered.insert(first, last);
}

fn mark_object_coverage(state: &mut GateState, tickets: impl Iterator<Item = CommitTicket>) {
    for ticket in tickets {
        for sequence in ticket.first_sequence..=ticket.last_sequence {
            if sequence > state.tiered_through {
                state.object_covered.insert(sequence);
            }
        }
    }
    while let Some(next) = state.tiered_through.checked_add(1) {
        if !state.object_covered.remove(&next) {
            break;
        }
        state.tiered_through = next;
    }
}

fn object_covers(state: &GateState, ticket: CommitTicket) -> bool {
    ticket.last_sequence <= state.tiered_through
        || (ticket.first_sequence..=ticket.last_sequence).all(|sequence| {
            sequence <= state.tiered_through || state.object_covered.contains(&sequence)
        })
}

fn validate_ticket(state: &GateState, ticket: CommitTicket) -> Result<()> {
    if ticket.leader_session != state.leader_session
        || ticket.log_epoch != state.log_epoch
        || ticket.first_sequence == 0
        || ticket.first_sequence > ticket.last_sequence
        || ticket.last_sequence >= state.next_sequence
    {
        return Err(Error::Node("node-log ticket is not owned by this gate"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
