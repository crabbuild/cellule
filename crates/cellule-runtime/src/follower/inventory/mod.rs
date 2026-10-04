//! Advisory persisted-lane inventory. It never retires or seals a tail.

use super::*;
use crate::identity::{Digest, encode_hex};
use crate::node::NodeMode;

const MAX_INVENTORY_LANES: usize = 10_000;
const INVENTORY_BYTES: u64 = 1 << 20;
const MAX_PAGE_ENTRIES: usize = 128;

/// Local continuation bound to one opened store and its persisted lane topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FollowerInventoryCursor {
    topology: Digest,
    leader: SessionId,
    epoch: u64,
}

impl FollowerInventoryCursor {
    /// Encodes one fixed-width continuation for application transports.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 56] {
        let mut bytes = [0; 56];
        bytes[..32].copy_from_slice(self.topology.as_bytes());
        bytes[32..48].copy_from_slice(self.leader.as_bytes());
        bytes[48..].copy_from_slice(&self.epoch.to_le_bytes());
        bytes
    }

    /// Decodes a continuation; the store validates its topology on use.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 56] = bytes
            .try_into()
            .map_err(|_| Error::Node("invalid follower inventory cursor width"))?;
        let mut topology = [0; 32];
        topology.copy_from_slice(&bytes[..32]);
        let mut leader = [0; 16];
        leader.copy_from_slice(&bytes[32..48]);
        let mut epoch = [0; 8];
        epoch.copy_from_slice(&bytes[48..]);
        let cursor = Self {
            topology: Digest::from_bytes(topology),
            leader: SessionId::from_bytes(leader),
            epoch: u64::from_le_bytes(epoch),
        };
        validate_lane(Lane {
            leader: cursor.leader,
            epoch: cursor.epoch,
        })?;
        Ok(cursor)
    }
}

/// Persisted local append fence; it is separate from remote epoch authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FollowerLaneState {
    /// The lane can still receive authorized appends.
    Open = 1,
    /// A seal prevents appends, but retained data may still be required.
    Sealed = 2,
    /// A retirement fence exists; current authority must still be inspected.
    Retired = 3,
}

/// One persisted lane, including lanes not yet loaded into the in-memory index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FollowerLaneObservation {
    /// Boot session that originally enrolled this lane, even if now expired.
    pub leader: SessionId,
    /// Exact node-log epoch; identities are never inferred from live membership.
    pub epoch: u64,
    /// Persisted append-fence state at the local scan barrier.
    pub state: FollowerLaneState,
    /// Highest sequence recorded in the seal marker, if sealed.
    pub sealed_through: Option<u64>,
    /// Object-coverage watermark recorded by retirement, if retired.
    pub retired_through: Option<u64>,
}

impl FollowerLaneObservation {
    fn key(self) -> ([u8; 16], u64) {
        (*self.leader.as_bytes(), self.epoch)
    }
}

/// One bounded local scan page retaining its follower-index memory admission.
pub struct FollowerInventoryPage {
    topology: Digest,
    mode: NodeMode,
    observed_at_ms: i64,
    total_lanes: usize,
    unretired_lanes: usize,
    quarantined_entries: usize,
    entries: Vec<FollowerLaneObservation>,
    next: Option<FollowerInventoryCursor>,
    _memory: IndexReservation,
}

impl FollowerInventoryPage {
    /// Returns the store and persisted topology fingerprint shared by all pages.
    #[must_use]
    pub const fn topology(&self) -> Digest {
        self.topology
    }
    /// Returns the shared local admission mode observed at the scan barrier.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Returns the caller's capture time; it does not refresh remote evidence.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
    /// Counts all persisted lanes, including lanes outside this page.
    #[must_use]
    pub const fn total_lanes(&self) -> usize {
        self.total_lanes
    }
    /// Counts all open or sealed lanes. Zero is not remote retirement proof.
    #[must_use]
    pub const fn unretired_lanes(&self) -> usize {
        self.unretired_lanes
    }
    /// Counts isolated filesystem entries that still require diagnosis.
    #[must_use]
    pub const fn quarantined_entries(&self) -> usize {
        self.quarantined_entries
    }
    /// Returns at most 128 observations, sorted by leader session and epoch.
    #[must_use]
    pub fn entries(&self) -> &[FollowerLaneObservation] {
        &self.entries
    }
    /// Continues only while the opened store and persisted topology still match.
    #[must_use]
    pub const fn next(&self) -> Option<FollowerInventoryCursor> {
        self.next
    }
}

impl FollowerStore {
    /// Observes every persisted lane in bounded pages without changing its fences.
    ///
    /// Each page retains one MiB from the existing follower-index memory budget.
    /// The scan includes cold lanes after reopen and reports quarantine. A topology
    /// change requires restarting pagination. Callers must separately inspect all
    /// authoritative log references, including dead owners, before shutdown.
    /// Cordon must be committed before using a scan as an enrollment barrier.
    pub async fn fleet_lanes_page(
        &self,
        cursor: Option<FollowerInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> Result<FollowerInventoryPage> {
        if !(1..=MAX_PAGE_ENTRIES).contains(&limit) || now_ms < 0 {
            return Err(Error::Node("invalid follower inventory page bounds"));
        }
        let memory = IndexReservation::new(&self.index_used, INVENTORY_BYTES)?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            // All lane creation, append, seal, retire, and collection mutate under
            // this same disk lane. A completed cordon plus this barrier includes
            // enrollments accepted before cordon; queued new lanes cannot appear.
            let _disk = store
                .retained
                .lock()
                .map_err(|_| Error::Node("follower disk reservation lock poisoned"))?;
            collect_page(&store, cursor, limit, now_ms, memory)
        })
        .await
        .map_err(Error::FollowerWorkerJoin)?
    }
}

fn collect_page(
    store: &FollowerStore,
    cursor: Option<FollowerInventoryCursor>,
    limit: usize,
    now_ms: i64,
    memory: IndexReservation,
) -> Result<FollowerInventoryPage> {
    // Preallocate the hard cap: geometric Vec growth must not exceed admission.
    let mut lanes = Vec::with_capacity(MAX_INVENTORY_LANES);
    let followers = store.root.join("followers");
    let exists = match std::fs::symlink_metadata(&followers) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if exists {
        require_directory(&followers)?;
        let mut leaders = 0;
        for leader in std::fs::read_dir(&followers)? {
            leaders += 1;
            if leaders > MAX_INVENTORY_LANES {
                return Err(Error::Capacity("follower inventory leader bound"));
            }
            let path = leader?.path();
            require_directory(&path)?;
            let leader = parse_session_directory(&path)?;
            if path.file_name().and_then(|name| name.to_str())
                != Some(encode_hex(leader.as_bytes()).as_str())
            {
                return Err(Error::Node("noncanonical follower leader directory"));
            }
            for epoch in std::fs::read_dir(&path)? {
                if lanes.len() == MAX_INVENTORY_LANES {
                    return Err(Error::Capacity("follower inventory lane bound"));
                }
                let path = epoch?.path();
                require_directory(&path)?;
                let epoch = parse_epoch_directory(&path)?;
                if path.file_name().and_then(|name| name.to_str())
                    != Some(epoch.to_string().as_str())
                {
                    return Err(Error::Node("noncanonical follower epoch directory"));
                }
                let sealed_through = marker(&path.join("sealed"))?;
                let retired_through = marker(&path.join("retired"))?;
                // Canonical retirement removes chunks after persisting its fence.
                // A crash can retain the covered chunks; absence is legal only
                // when that durable retirement marker exists.
                match std::fs::symlink_metadata(path.join("chunks")) {
                    Ok(metadata) if metadata.file_type().is_dir() => {}
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound
                            && retired_through.is_some() => {}
                    Err(error) => return Err(error.into()),
                    Ok(_) => {
                        return Err(Error::Node("follower inventory chunks are not a directory"));
                    }
                }
                for entry in std::fs::read_dir(&path)? {
                    let entry = entry?;
                    if !matches!(
                        entry.file_name().to_str(),
                        Some("chunks" | "sealed" | "retired")
                    ) {
                        return Err(Error::Node("follower inventory lane has an unknown entry"));
                    }
                }
                if matches!((sealed_through, retired_through), (Some(sealed), Some(retired)) if sealed > retired)
                {
                    return Err(Error::Node("follower retirement does not cover seal"));
                }
                lanes.push(FollowerLaneObservation {
                    leader,
                    epoch,
                    state: if retired_through.is_some() {
                        FollowerLaneState::Retired
                    } else if sealed_through.is_some() {
                        FollowerLaneState::Sealed
                    } else {
                        FollowerLaneState::Open
                    },
                    sealed_through,
                    retired_through,
                });
            }
        }
    }
    lanes.sort_unstable_by_key(|lane| lane.key());
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-follower-inventory-v1");
    hash.update(&store.inventory_scope);
    for lane in &lanes {
        hash.update(lane.leader.as_bytes());
        hash.update(&lane.epoch.to_le_bytes());
        hash.update(&[lane.state as u8]);
    }
    let topology = Digest::from_bytes(*hash.finalize().as_bytes());
    let start = match cursor {
        None => 0,
        Some(cursor) if cursor.topology == topology => {
            let position = lanes
                .binary_search_by_key(&(*cursor.leader.as_bytes(), cursor.epoch), |lane| {
                    lane.key()
                })
                .map_err(|_| Error::Node("follower inventory cursor key is absent"))?;
            position + 1
        }
        Some(_) => {
            return Err(Error::Node(
                "follower inventory topology changed; restart scan",
            ));
        }
    };
    let end = start.saturating_add(limit).min(lanes.len());
    let entries = lanes[start..end].to_vec();
    let next = if end < lanes.len() {
        entries.last().map(|last| FollowerInventoryCursor {
            topology,
            leader: last.leader,
            epoch: last.epoch,
        })
    } else {
        None
    };
    Ok(FollowerInventoryPage {
        topology,
        mode: store.admission.mode()?,
        observed_at_ms: now_ms,
        total_lanes: lanes.len(),
        unretired_lanes: lanes
            .iter()
            .filter(|lane| lane.state != FollowerLaneState::Retired)
            .count(),
        quarantined_entries: quarantine_entry_count(&store.root)?,
        entries,
        next,
        _memory: memory,
    })
}

fn require_directory(path: &Path) -> Result<()> {
    if !std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(Error::Node(
            "follower inventory contains a special directory",
        ));
    }
    Ok(())
}

fn marker(path: &Path) -> Result<Option<u64>> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == 8 => Ok(Some(
            read_watermark(path, "invalid follower inventory marker")?,
        )),
        Ok(_) => Err(Error::Node(
            "follower inventory marker is not an eight-byte file",
        )),
    }
}

#[cfg(test)]
mod tests;
