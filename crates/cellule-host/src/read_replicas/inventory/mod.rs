//! Node-owned reader diagnostics through the existing activation barrier.

use super::*;
use cellule_runtime::cell::actor::NodeByteReservation;
use cellule_runtime::client::ReadReplicaLifecycleObservation;
use cellule_runtime::identity::Digest;
use cellule_runtime::node::NodeMode;

const PAGE_BYTES: usize = 1 << 20;
const MAX_PAGE_ENTRIES: usize = 128;

/// Opaque continuation for one manager session and active-view topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderInventoryCursor {
    topology: Digest,
    after: CellId,
}

impl ReaderInventoryCursor {
    /// Encodes a fixed-width application continuation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 64] {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(self.topology.as_bytes());
        bytes[32..].copy_from_slice(self.after.as_bytes());
        bytes
    }
    /// Decodes the fixed width; topology is rechecked when the manager scans.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 64] = bytes
            .try_into()
            .map_err(|_| Error::Node("invalid reader inventory cursor width"))?;
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

/// Bounded managed views, retaining their observation's native-byte admission.
pub struct ReaderInventoryPage {
    session: SessionId,
    topology: Digest,
    mode: NodeMode,
    observed_at_ms: i64,
    closed: bool,
    total_views: usize,
    entries: Vec<ReadReplicaLifecycleObservation>,
    next: Option<ReaderInventoryCursor>,
    _memory: NodeByteReservation,
}

impl ReaderInventoryPage {
    /// Returns the manager's exact node boot session.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the opaque manager and active-view topology identity.
    #[must_use]
    pub const fn topology(&self) -> Digest {
        self.topology
    }
    /// Returns current shared admission mode, independent of reader count.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Returns the supplied capture time; receipts retain their own positions.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
    /// Reports terminal manager activation closure.
    #[must_use]
    pub const fn closed(&self) -> bool {
        self.closed
    }
    /// Counts every managed view, including views outside this page.
    #[must_use]
    pub const fn total_views(&self) -> usize {
        self.total_views
    }
    /// Returns sorted positions and original native lifetimes, without readiness proof.
    #[must_use]
    pub fn entries(&self) -> &[ReadReplicaLifecycleObservation] {
        &self.entries
    }
    /// Continues until activation, removal, replacement, or shutdown changes topology.
    #[must_use]
    pub const fn next(&self) -> Option<ReaderInventoryCursor> {
        self.next
    }
}

impl ReadReplicaManager {
    /// Observes managed reader obligations after the current activation completes.
    ///
    /// Each page retains one MiB from the existing runtime byte ledger. No remote
    /// I/O is performed and no new scheduling task is created. A changed topology
    /// requires restarting pagination. Receipts are advisory local positions:
    /// canonical lifetime guards retain accepted query/refresh work, including
    /// cancelled native jobs. Local joining fences every retained clone; remote
    /// authority, producer retirement, replacement policy and host facilities
    /// still require independent settlement before taking a node offline.
    pub async fn fleet_readers_page(
        &self,
        cursor: Option<ReaderInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> Result<ReaderInventoryPage> {
        if !(1..=MAX_PAGE_ENTRIES).contains(&limit) || now_ms < 0 {
            return Err(Error::Node("invalid reader inventory bounds"));
        }
        let memory = self.runtime.try_reserve_node_bytes(PAGE_BYTES)?;
        // Activation and removal already use this lane. Observation cannot miss
        // an accepted open that is about to enter the managed collection.
        let _activation = self.activation.lock().await;
        let active = self.active.read().await;
        if active.views.len() > MAX_READ_VIEWS {
            return Err(Error::Capacity("node read-view inventory bound"));
        }
        let mut identity = [0; 32];
        identity[..16].copy_from_slice(active.topology.as_bytes());
        identity[16..].copy_from_slice(self.session.as_bytes());
        let topology = Digest::from_bytes(identity);
        let mut cells = active.views.keys().copied().collect::<Vec<_>>();
        cells.sort_unstable_by_key(|cell| *cell.as_bytes());
        let start = match cursor {
            None => 0,
            Some(cursor) if cursor.topology == topology => {
                cells
                    .binary_search_by_key(cursor.after.as_bytes(), |cell| *cell.as_bytes())
                    .map_err(|_| Error::Node("reader inventory cursor key is absent"))?
                    + 1
            }
            Some(_) => {
                return Err(Error::Node(
                    "reader inventory topology changed; restart scan",
                ));
            }
        };
        let end = start.saturating_add(limit).min(cells.len());
        let mut entries = Vec::with_capacity(end - start);
        for cell in &cells[start..end] {
            let reader = active.views.get(cell).ok_or(Error::Control(
                "reader inventory view disappeared under activation lane",
            ))?;
            entries.push(reader.lifecycle_observation().await);
        }
        let next = if end < cells.len() {
            entries.last().map(|last| ReaderInventoryCursor {
                topology,
                after: last.receipt().cell,
            })
        } else {
            None
        };
        Ok(ReaderInventoryPage {
            session: self.session,
            topology,
            mode: self.runtime.node_admission().mode()?,
            observed_at_ms: now_ms,
            closed: self.closed.is_cancelled(),
            total_views: cells.len(),
            entries,
            next,
            _memory: memory,
        })
    }
}
