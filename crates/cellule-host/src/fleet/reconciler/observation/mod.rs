use std::collections::{HashMap, HashSet};

use crate::fleet::{
    FleetFailedBootClosure, FleetMaintenanceEnrollments, FleetMaintenanceNonexecution,
    FleetMaintenancePolicyCoverage, FleetOriginalWriterSuccessorInventory, FleetRoleCoverage,
    FleetRoster,
};
use cellule_runtime::cell::actor::OwnedCellObservation;
use cellule_runtime::fleet::operations::{FleetScope, RegistryVersion};
use cellule_runtime::fleet::placement::PlacementObservation;
use cellule_runtime::identity::{Digest, NodeId, SessionId};
use cellule_runtime::node::NodeAdvertisement;
use cellule_runtime::{Error, Result};
use evacuations::RoleEvacuations;

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
/// The reconciler traverses the durable roster. The adapter proves native-role
/// coverage, unexpected-live discovery and signing-key enrollment independently
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
    roster: Option<FleetRoster>,
    role_coverage: Option<FleetRoleCoverage>,
    original_writer_successors: Option<FleetOriginalWriterSuccessorInventory>,
    failed_boot_closures: Option<Vec<FleetFailedBootClosure>>,
    role_evacuations: Option<RoleEvacuations>,
    maintenance_enrollments: Option<FleetMaintenanceEnrollments>,
    maintenance_policies: Option<FleetMaintenancePolicyCoverage>,
    maintenance_nonexecution: Option<FleetMaintenanceNonexecution>,
}

impl FleetObservation {
    /// Retains the original collection interval. `complete` asserts a stable,
    /// fully scanned native-role and ownership inventory, including busy/transitional
    /// entries in the signed counts. The reconciler separately validates durable
    /// boot coverage, Pending enrollment and matching signed writer counts.
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
            roster: None,
            role_coverage: None,
            original_writer_successors: None,
            failed_boot_closures: None,
            role_evacuations: None,
            maintenance_enrollments: None,
            maintenance_policies: None,
            maintenance_nonexecution: None,
        };
        observation.placements(capture_finished_at_ms)?;
        Ok(observation)
    }

    /// Retains the checked native/foreign graph inside this original capture.
    /// This cannot upgrade `complete`: authentication, membership discovery,
    /// current Cell authority, policy and failed-process evidence remain the
    /// adapter's duties. The reconciler compares the exact full roster again.
    pub fn with_role_coverage(mut self, coverage: FleetRoleCoverage) -> Result<Self> {
        if self.role_coverage.is_some() {
            return Err(Error::Control("fleet role coverage already retained"));
        }
        self.role_coverage = Some(coverage);
        self.validate_role_coverage()?;
        Ok(self)
    }

    /// Original role graph retained in the planner inputs; never restamped.
    #[must_use]
    pub fn role_coverage(&self) -> Option<&FleetRoleCoverage> {
        self.role_coverage.as_ref()
    }

    fn validate_role_coverage(&self) -> Result<()> {
        self.validate_maintenance_policies()?;
        self.validate_maintenance_enrollments()?;
        self.validate_maintenance_nonexecution()?;
        self.validate_original_writer_successors()?;
        self.validate_failed_boot_closures()?;
        self.validate_role_evacuations()?;
        if let Some(coverage) = &self.role_coverage {
            let (started, finished) = coverage.interval();
            if coverage.snapshot().head().scope() != self.scope
                || coverage.snapshot().registry() != self.registry
                || started < self.capture_started_at_ms
                || finished > self.capture_finished_at_ms
            {
                return Err(Error::Node("fleet role coverage barrier differs"));
            }
            if let Some(roster) = &self.roster
                && (coverage.snapshot() != roster.snapshot()
                    || roster.digest()? != coverage.roster_digest())
            {
                return Err(Error::Node("fleet role coverage roster differs"));
            }
        }
        Ok(())
    }

    /// Retains a fully traversed durable roster in these planner inputs. The
    /// adapter must recheck it after native capture. Attaching rows alone cannot
    /// establish native-role coverage or upgrade an incomplete observation.
    pub(super) fn with_roster(mut self, roster: FleetRoster) -> Result<Self> {
        if roster.snapshot().registry() != self.registry
            || roster.snapshot().head().scope() != self.scope
        {
            return Err(Error::Node("fleet observation roster barrier differs"));
        }
        self.roster = Some(roster);
        self.validate_role_coverage()?;
        Ok(self)
    }

    /// Returns the original roster retained by the reconciler after capture.
    #[must_use]
    pub fn roster(&self) -> Option<&FleetRoster> {
        self.roster.as_ref()
    }

    pub(super) fn counts_match(&self, placements: &[PlacementObservation]) -> bool {
        let mut counts = HashMap::new();
        for owned in &self.cells {
            *counts.entry(owned.session).or_insert(0_u32) += 1;
        }
        // A transitioning actor remains in the signed count. Omitting its row
        // cannot turn a partial inventory into a complete count barrier.
        placements
            .iter()
            .all(|node| counts.get(&node.session).copied().unwrap_or(0) == node.active_cells)
    }

    pub(super) fn placements(&self, now_ms: i64) -> Result<Vec<PlacementObservation>> {
        self.validate_role_coverage()?;
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
        hash.update(b"cellule.fleet-planner-inputs.v11\0");
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
        hash.update(&[u8::from(self.roster.is_some())]);
        if let Some(roster) = &self.roster {
            hash.update(roster.digest()?.as_bytes());
        }
        hash.update(&[u8::from(self.role_coverage.is_some())]);
        if let Some(coverage) = &self.role_coverage {
            hash.update(coverage.digest().as_bytes());
        }
        hash.update(&[u8::from(self.original_writer_successors.is_some())]);
        if let Some(inventory) = &self.original_writer_successors {
            hash.update(inventory.digest()?.as_bytes());
        }
        hash.update(&[u8::from(self.failed_boot_closures.is_some())]);
        if let Some(closures) = &self.failed_boot_closures {
            let mut ordered = closures.iter().collect::<Vec<_>>();
            ordered.sort_by_key(|closure| {
                let boot = closure.boot().spec().target;
                (*boot.node.as_bytes(), *boot.session.as_bytes())
            });
            hash.update(&(ordered.len() as u64).to_be_bytes());
            for closure in ordered {
                hash.update(closure.digest().as_bytes());
                for bytes in [
                    closure
                        .snapshot()
                        .head()
                        .to_bytes()
                        .map_err(super::operation)?,
                    closure
                        .snapshot()
                        .registry()
                        .to_bytes()
                        .map_err(super::operation)?,
                ] {
                    hash.update(&(bytes.len() as u64).to_be_bytes());
                    hash.update(&bytes);
                }
                for time in [closure.interval().0, closure.interval().1] {
                    hash.update(&time.to_be_bytes());
                }
            }
        }
        self.hash_role_evacuations(&mut hash)?;
        hash.update(&[u8::from(self.maintenance_nonexecution.is_some())]);
        if let Some(nonexecution) = &self.maintenance_nonexecution {
            hash.update(nonexecution.digest().as_bytes());
        }
        hash.update(&[u8::from(self.maintenance_enrollments.is_some())]);
        if let Some(original) = &self.maintenance_enrollments {
            hash.update(original.digest()?.as_bytes());
        }
        hash.update(&[u8::from(self.maintenance_policies.is_some())]);
        if let Some(coverage) = &self.maintenance_policies {
            hash.update(coverage.digest().as_bytes());
        }
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
            hash.update(row.code.as_bytes());
            hash.update(&row.schema.to_be_bytes());
            // Role now controls busy-maintenance eligibility. Use explicit tags
            // in this producer domain rather than enum declaration order.
            use cellule_runtime::cell::catalog::CatalogRole;
            hash.update(&[match row.role {
                CatalogRole::Application => 1,
                CatalogRole::Sql => 2,
                CatalogRole::Kv => 3,
                CatalogRole::Queue => 4,
                CatalogRole::Workflow => 5,
                CatalogRole::Blob => 6,
                CatalogRole::Cron => 7,
            }]);
            hash.update(&row.generation.to_be_bytes());
            hash.update(&row.resident_since_ms.to_be_bytes());
            hash.update(&row.sampled_at_ms.unwrap_or(-1).to_be_bytes());
            hash.update(&[row.stable_observations]);
            hash.update(&[
                u8::from(row.quiescing),
                u8::from(row.maintenance_work.is_some()),
            ]);
            if let Some(inventory) = row.maintenance_work {
                use cellule_runtime::primitives::maintenance_readiness::MaintenanceWorkBlocker;
                for blocker in [
                    MaintenanceWorkBlocker::EffectLease,
                    MaintenanceWorkBlocker::QueueLease,
                    MaintenanceWorkBlocker::ActivityLease,
                    MaintenanceWorkBlocker::BlobInventory,
                ] {
                    hash.update(&[u8::from(inventory.has_blocker(blocker))]);
                }
            }

            hash.update(&[u8::from(
                row.blockers.is_empty() && row.work_blocker.is_none(),
            )]);
            hash.update(&(row.blockers.len() as u64).to_be_bytes());
            for blocker in &row.blockers {
                hash.update(&[*blocker as u8]);
            }
            for cost in [row.cost, row.maintenance_cost] {
                hash.update(&[u8::from(cost.is_some())]);
                if let Some(cost) = cost {
                    hash.update(&cost.memory_bytes.to_be_bytes());
                    hash.update(&cost.disk_bytes.to_be_bytes());
                    hash.update(&cost.file_descriptors.to_be_bytes());
                    hash.update(&cost.job_credits.to_be_bytes());
                }
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

mod evacuations;
mod failed_boots;
mod maintenance_enrollments;
mod maintenance_policies;
mod nonexecution;
mod original_writers;
#[cfg(test)]
mod tests;
