//! Bounded actor-owned advisory inventory. It cannot authorize a Cell release.

use super::*;
use crate::fleet::operations::{DrainBlocker, MAX_PAGE_ENTRIES, PublishedPosition, TransferCost};
use crate::identity::IncarnationId;

mod demand;
pub(super) use demand::{CellDemandState, transfer_cost};

/// Continuation of a sorted ownership-topology scan, scoped to this runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellInventoryCursor {
    topology: Digest,
    after: CellId,
}

impl CellInventoryCursor {
    /// Returns the ownership topology fingerprint; it confers no authority.
    #[must_use]
    pub const fn topology(self) -> Digest {
        self.topology
    }
    /// Returns the last Cell ID emitted by the preceding page.
    #[must_use]
    pub const fn after(self) -> CellId {
        self.after
    }
    /// Encodes the fixed-width opaque continuation for application transports.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 64] {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(self.topology.as_bytes());
        bytes[32..].copy_from_slice(self.after.as_bytes());
        bytes
    }
    /// Restores exactly one fixed-width continuation; the actor revalidates it.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let bytes: &[u8; 64] = bytes
            .try_into()
            .map_err(|_| Error::Node("invalid Cell inventory cursor width"))?;
        let mut topology = [0; 32];
        topology.copy_from_slice(&bytes[..32]);
        let mut after = [0; 32];
        after.copy_from_slice(&bytes[32..]);
        Ok(Self {
            topology: Digest::from_bytes(topology),
            after: CellId::from_bytes(after),
        })
    }
}

/// One generation-bound owner, including busy or draining Cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedCellObservation {
    /// Verified catalog target; actor observations do not replace its proof.
    pub target: CellTarget,
    /// Local generation required by the canonical release path.
    pub generation: u64,
    /// Authority-pinned incarnation loaded by this activation.
    pub incarnation: IncarnationId,
    /// Immutable activation identity, retained while a publication owns the
    /// publisher. This is advisory and grants no release or durability proof.
    pub owner_fence: crate::control::OwnerFence,
    /// Current executable contract identity.
    pub code: Digest,
    /// Current schema version.
    pub schema: u32,
    /// Namespace's persistent role.
    pub role: CatalogRole,
    /// Activation time, unaffected by later use or publication.
    pub resident_since_ms: i64,
    /// Most recent admitted work time; separate from residence.
    pub last_used_ms: i64,
    /// Actor's last published authority position, if available locally.
    pub position: Option<PublishedPosition>,
    /// Conservative receiver cost from a fresh worker sample; required by idle movement.
    pub cost: Option<TransferCost>,
    /// Configured peak receiver envelope for explicit busy maintenance. This is
    /// independent of worker settlement and grants no release authority. Blob
    /// owners remain unsupported until their external barriers are implemented.
    pub maintenance_cost: Option<TransferCost>,
    /// Logical SQLite size measured by the serialized worker, distinct from restore cost.
    pub database_bytes: Option<u64>,
    /// Time of the worker measurement; reading this page does not refresh it.
    pub sampled_at_ms: Option<i64>,
    /// Consecutive unchanged worker observations, capped at two.
    pub stable_observations: u8,
    /// First executable work class blocking ordinary idle movement, if known.
    pub work_blocker: Option<crate::primitives::maintenance::TransferWorkClass>,
    /// Sticky foreground closure installed for this exact local activation.
    /// Native lease completion and validation remain available until release.
    pub quiescing: bool,
    /// Fresh primitive readiness from the same serialized worker measurement.
    /// Absence is unknown; a transferable value alone proves no actor barrier.
    pub maintenance_work:
        Option<crate::primitives::maintenance_readiness::MaintenanceWorkInventory>,
    /// Local advisory blockers; action acceptance rechecks them.
    pub blockers: Vec<DrainBlocker>,
}

/// All local ownership obligations appear even when metadata is unavailable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellInventoryEntry {
    /// A live actor whose catalog and generation are known.
    Owned(Box<OwnedCellObservation>),
    /// Activation/close/release is outstanding; do not interpret it as absent.
    Transitioning {
        /// Stable Cell ID whose lifecycle task is still owned.
        cell: CellId,
    },
}

impl CellInventoryEntry {
    /// Returns the stable key used for bounded sorted pagination.
    #[must_use]
    pub fn cell(&self) -> CellId {
        match self {
            Self::Owned(owner) => owner.target.cell_id(),
            Self::Transitioning { cell } => *cell,
        }
    }
}

/// A bounded page holding its native-memory reservation until dropped.
pub struct CellInventoryPage {
    session: SessionId,
    topology: Digest,
    observed_at_ms: i64,
    owned_cells: usize,
    transitioning_cells: usize,
    entries: Vec<CellInventoryEntry>,
    next: Option<CellInventoryCursor>,
    _retained: ResourceReservation,
}

impl CellInventoryPage {
    /// Returns the exact runtime boot identity shared by its owner observations.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the fingerprint shared by every page of a stable topology scan.
    #[must_use]
    pub const fn topology(&self) -> Digest {
        self.topology
    }
    /// Returns the time this actor page was captured, without refreshing old rows.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
    /// Counts all active actors, including busy and draining ones.
    #[must_use]
    pub const fn owned_cells(&self) -> usize {
        self.owned_cells
    }
    /// Counts all outstanding activation/close/release tasks.
    #[must_use]
    pub const fn transitioning_cells(&self) -> usize {
        self.transitioning_cells
    }
    /// Returns at most 128 entries in ascending Cell-ID order.
    #[must_use]
    pub fn entries(&self) -> &[CellInventoryEntry] {
        &self.entries
    }
    /// Returns a continuation, or None only after the full topology was scanned.
    #[must_use]
    pub const fn next(&self) -> Option<CellInventoryCursor> {
        self.next
    }
}

pub(super) fn validate_limit(limit: usize) -> crate::Result<()> {
    if limit == 0 || limit > MAX_PAGE_ENTRIES {
        return Err(Error::Node("invalid Cell inventory page limit"));
    }
    Ok(())
}

pub(super) fn collect_page(
    cells: &HashMap<CellId, ActiveCell>,
    transitioning: &HashSet<CellId>,
    next_generation: u64,
    session: SessionId,
    cursor: Option<CellInventoryCursor>,
    limit: usize,
    retained: ResourceReservation,
) -> crate::Result<CellInventoryPage> {
    validate_limit(limit)?;
    // SQL pool admission bounds total ownership to 10,000. Copy fixed IDs only,
    // within the page's one-MiB reservation; clone catalog rows for this page.
    let key_count = cells
        .len()
        .checked_add(transitioning.len())
        .filter(|count| *count <= crate::cell::worker::MAX_ACTIVE_CELLS)
        .ok_or(Error::Capacity(
            "Cell inventory topology exceeds bounded scan",
        ))?;
    let mut keys = Vec::with_capacity(key_count);
    keys.extend(
        cells
            .keys()
            .chain(transitioning.iter())
            .map(|cell| *cell.as_bytes()),
    );
    keys.sort_unstable();
    keys.dedup();
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.actor-inventory.v1\0");
    hash.update(session.as_bytes());
    hash.update(&next_generation.to_be_bytes());
    for key in &keys {
        let cell = CellId::from_bytes(*key);
        hash.update(key);
        hash.update(&cells.get(&cell).map_or(0, |a| a.generation).to_be_bytes());
        hash.update(&[u8::from(transitioning.contains(&cell))]);
    }
    let topology = Digest::from_bytes(*hash.finalize().as_bytes());
    let first = match cursor {
        Some(cursor) => {
            if cursor.topology != topology {
                return Err(Error::Node("Cell inventory topology changed; restart scan"));
            }
            keys.binary_search(cursor.after.as_bytes())
                .map_err(|_| Error::Node("Cell inventory cursor key is absent"))?
                + 1
        }
        None => 0,
    };
    let end = first.saturating_add(limit).min(keys.len());
    let mut entries = Vec::with_capacity(end - first);
    for key in &keys[first..end] {
        let cell = CellId::from_bytes(*key);
        let entry = match cells.get(&cell).filter(|_| !transitioning.contains(&cell)) {
            Some(active) => CellInventoryEntry::Owned(Box::new(observe_owned(active)?)),
            None => CellInventoryEntry::Transitioning { cell },
        };
        entries.push(entry);
    }
    let next = if end < keys.len() {
        Some(CellInventoryCursor {
            topology,
            after: CellId::from_bytes(keys[end - 1]),
        })
    } else {
        None
    };
    Ok(CellInventoryPage {
        session,
        topology,
        observed_at_ms: unix_millis(),
        owned_cells: cells.len(),
        transitioning_cells: transitioning.len(),
        entries,
        next,
        _retained: retained,
    })
}

fn observe_owned(active: &ActiveCell) -> crate::Result<OwnedCellObservation> {
    let mut blockers = Vec::new();
    if active.busy() || active.draining() || !active.queue.is_empty() {
        blockers.push(DrainBlocker::BusyExecution);
    }
    if !active.publications.is_empty() || active.unpublished_node_logs != 0 {
        blockers.push(DrainBlocker::PendingPublication);
    }
    let sample = active
        .demand
        .fresh_sample(unix_millis(), active.published_sequence);
    if sample.is_none() {
        blockers.push(DrainBlocker::UnknownInventory);
    }
    let cost = sample
        .map(|sample| demand::transfer_cost(active.resource_limits, sample.database_bytes))
        .transpose()?;
    let maintenance_cost = (active.role != CatalogRole::Blob)
        .then(|| {
            demand::transfer_cost(
                active.resource_limits,
                active.resource_limits.max_database_bytes,
            )
        })
        .transpose()?;
    let work_blocker = sample.and_then(|sample| sample.transfer_work.first_blocker());
    if work_blocker.is_some() && !blockers.contains(&DrainBlocker::BusyExecution) {
        blockers.push(DrainBlocker::BusyExecution);
    }
    let position = active.publisher.as_ref().and_then(|publisher| {
        let control = publisher.control().value();
        control.root.clone().map(|root| PublishedPosition {
            incarnation: control.incarnation,
            epoch: control.epoch,
            root,
        })
    });
    Ok(OwnedCellObservation {
        target: active.catalog.target()?,
        generation: active.generation,
        incarnation: active.incarnation,
        owner_fence: active.admission.owner_fence,
        code: active.code,
        schema: active.schema,
        role: active.role,
        resident_since_ms: active.resident_since_ms,
        last_used_ms: active.last_used_ms,
        position,
        cost,
        maintenance_cost,
        database_bytes: sample.map(|sample| sample.database_bytes),
        sampled_at_ms: sample.map(|sample| sample.observed_at_ms),
        stable_observations: sample.map_or(0, |_| active.demand.stable_observations()),
        work_blocker,
        quiescing: active.coordination.is_maintenance_quiescing(),
        maintenance_work: sample.map(|sample| sample.maintenance_work),
        blockers,
    })
}

#[cfg(test)]
mod tests;
