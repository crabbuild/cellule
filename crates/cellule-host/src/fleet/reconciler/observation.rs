use std::collections::HashSet;

use cellule_runtime::cell::actor::OwnedCellObservation;
use cellule_runtime::fleet::operations::{FleetScope, RegistryVersion};
use cellule_runtime::fleet::placement::PlacementObservation;
use cellule_runtime::identity::{Digest, NodeId, SessionId};
use cellule_runtime::node::NodeAdvertisement;
use cellule_runtime::{Error, Result};

/// Generation-bound actor observation from an authenticated exact node boot.
#[derive(Clone, Debug)]
pub struct FleetOwnedCell {
    /// Physical origin authenticated by the observation adapter.
    pub node: NodeId,
    /// Exact boot whose actor supplied this page entry.
    pub session: SessionId,
    /// Existing actor inventory, including measured costs and blockers.
    pub observation: OwnedCellObservation,
}

/// Bounded aggregate of authenticated paginated observations for one barrier.
///
/// This is an in-process adapter value, not a wire or persisted format. Each
/// transport page remains bounded to 128 rows and one MiB. Aggregation is capped
/// at the planner's 10,000 nodes/Cells; applications account collector buffers.
/// The adapter proves roster coverage and signing-key enrollment independently
/// of self-signature verification. A digest identifies inputs, not atomicity.
pub struct FleetObservation {
    pub(super) scope: FleetScope,
    pub(super) registry: RegistryVersion,
    pub(super) membership_revision: u64,
    pub(super) capture_started_at_ms: i64,
    pub(super) capture_finished_at_ms: i64,
    pub(super) complete: bool,
    pub(super) nodes: Vec<NodeAdvertisement>,
    pub(super) cells: Vec<FleetOwnedCell>,
}

impl FleetObservation {
    /// Retains the original collection interval. `complete` asserts a stable,
    /// fully scanned roster and ownership pages, including busy/transitional
    /// entries in the signed counts. It is an adapter proof obligation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scope: FleetScope,
        registry: RegistryVersion,
        membership_revision: u64,
        capture_started_at_ms: i64,
        capture_finished_at_ms: i64,
        complete: bool,
        nodes: Vec<NodeAdvertisement>,
        cells: Vec<FleetOwnedCell>,
    ) -> Result<Self> {
        let observation = Self {
            scope,
            registry,
            membership_revision,
            capture_started_at_ms,
            capture_finished_at_ms,
            complete,
            nodes,
            cells,
        };
        observation.placements(capture_finished_at_ms)?;
        Ok(observation)
    }

    pub(super) fn placements(&self, now_ms: i64) -> Result<Vec<PlacementObservation>> {
        if self.registry.scope() != self.scope
            || self.membership_revision == 0
            || self.capture_started_at_ms < 0
            || self.capture_finished_at_ms < self.capture_started_at_ms
            || self.capture_finished_at_ms > now_ms
            || now_ms - self.capture_started_at_ms > 30_000
            || self.nodes.len() > 10_000
            || self.cells.len() > 10_000
        {
            return Err(Error::Node("invalid fleet observation barrier or bounds"));
        }
        let mut nodes = HashSet::new();
        let mut sessions = HashSet::new();
        let mut placements = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            if node.fleet() != self.scope.fleet
                || !nodes.insert(node.node())
                || !sessions.insert(node.session())
            {
                return Err(Error::Node("fleet observation duplicates or crosses scope"));
            }
            placements.push(PlacementObservation::from_signed_advertisement(
                node, now_ms, false,
            )?);
        }
        let mut cells = HashSet::new();
        for owned in &self.cells {
            let row = &owned.observation;
            if !cells.insert(row.target.cell_id())
                || row.target.application() != self.scope.application
                || !placements
                    .iter()
                    .any(|node| node.node == owned.node && node.session == owned.session)
                || row.generation == 0
                || row.resident_since_ms < 0
                || row.resident_since_ms > now_ms
            {
                return Err(Error::Node("fleet ownership observation identity mismatch"));
            }
        }
        placements.sort_by_key(|node| (*node.node.as_bytes(), *node.session.as_bytes()));
        Ok(placements)
    }

    /// Identifies the canonical planner inputs after signature/shape validation.
    /// Full envelope/role transport codecs remain distinct from this local value.
    pub(super) fn digest(&self, now_ms: i64) -> Result<Digest> {
        let nodes = self.placements(now_ms)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-planner-inputs.v1\0");
        hash.update(self.scope.fleet.as_bytes());
        hash.update(self.scope.application.as_bytes());
        hash.update(&self.registry.to_bytes().map_err(super::operation)?);
        for time in [
            self.membership_revision,
            self.capture_started_at_ms as u64,
            self.capture_finished_at_ms as u64,
        ] {
            hash.update(&time.to_be_bytes());
        }
        hash.update(&[u8::from(self.complete)]);
        hash.update(&(nodes.len() as u64).to_be_bytes());
        for node in nodes {
            hash.update(node.node.as_bytes());
            hash.update(node.session.as_bytes());
            for n in [
                node.observed_at_ms as u64,
                node.memory_capacity_bytes,
                node.free_memory_bytes,
                node.disk_capacity_bytes,
                node.free_disk_bytes,
                u64::from(node.active_cells),
                u64::from(node.max_active_cells),
                u64::from(node.running_jobs),
                u64::from(node.job_capacity),
                u64::from(node.publication_backlog),
                u64::from(node.hydration_backlog),
                u64::from(node.primitive_backlog),
            ] {
                hash.update(&n.to_be_bytes());
            }
            hash.update(&[node.pressure as u8, u8::from(node.draining)]);
        }
        let mut cells = self.cells.iter().collect::<Vec<_>>();
        cells.sort_by_key(|row| *row.observation.target.cell_id().as_bytes());
        hash.update(&(cells.len() as u64).to_be_bytes());
        for owned in cells {
            let row = &owned.observation;
            hash.update(owned.node.as_bytes());
            hash.update(owned.session.as_bytes());
            hash.update(row.target.cell_id().as_bytes());
            hash.update(row.incarnation.as_bytes());
            hash.update(&row.generation.to_be_bytes());
            hash.update(&row.resident_since_ms.to_be_bytes());
            hash.update(&row.sampled_at_ms.unwrap_or(-1).to_be_bytes());
            hash.update(&[row.stable_observations]);
            hash.update(&[u8::from(
                row.blockers.is_empty() && row.work_blocker.is_none(),
            )]);
            hash.update(&[u8::from(row.cost.is_some())]);
            if let Some(cost) = row.cost {
                hash.update(&cost.memory_bytes.to_be_bytes());
                hash.update(&cost.disk_bytes.to_be_bytes());
                hash.update(&cost.file_descriptors.to_be_bytes());
                hash.update(&cost.job_credits.to_be_bytes());
            }
            hash.update(&[u8::from(row.position.is_some())]);
            if let Some(position) = &row.position {
                hash.update(&position.epoch.to_be_bytes());
                hash.update(position.root.digest.as_bytes());
                hash.update(&position.root.txid.to_be_bytes());
                hash.update(&position.root.checksum.to_be_bytes());
                hash.update(&position.root.commit_sequence.to_be_bytes());
            }
        }
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
}
