//! Bounded discovery of authoritative log references, including failed owners.

use std::collections::{BTreeMap, HashSet};

use super::*;

const MAX_PAGE_ENTRIES: usize = 128;

/// Whether the authoritative log belongs to a live, expired, or fenced session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLeaderState {
    /// A signed advertisement remains within its declared lifetime.
    Live,
    /// The signed advertisement expired; its log remains an obligation.
    Expired,
    /// An authoritative tombstone fences the original owner.
    Fenced,
}

/// One authoritative current epoch naming the inspected physical follower node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerLogObservation {
    /// Original leader boot session, retained through tombstone recovery.
    pub leader: SessionId,
    /// Physical node that enrolled the log.
    pub leader_node: NodeId,
    /// Liveness classification is advisory; recovery requires its ordinary claim.
    pub leader_state: LogLeaderState,
    /// Exact current log authority, including phase, members, and coverage.
    pub log: NodeLogStatus,
}

/// Continuation bound to one directory instance, member, and discovered topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogInventoryCursor {
    topology: Digest,
    after: SessionId,
}

impl LogInventoryCursor {
    /// Encodes one fixed-width application continuation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 48] {
        let mut bytes = [0; 48];
        bytes[..32].copy_from_slice(self.topology.as_bytes());
        bytes[32..].copy_from_slice(self.after.as_bytes());
        bytes
    }
    /// Decodes the fixed width; the directory rechecks topology during use.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 48] = bytes
            .try_into()
            .map_err(|_| Error::Node("invalid log inventory cursor width"))?;
        let mut topology = [0; 32];
        topology.copy_from_slice(&bytes[..32]);
        let mut after = [0; 16];
        after.copy_from_slice(&bytes[32..]);
        if after == [0; 16] {
            return Err(Error::Node("zero log inventory continuation session"));
        }
        Ok(Self {
            topology: Digest::from_bytes(topology),
            after: SessionId::from_bytes(after),
        })
    }
}

/// Bounded discovered references. This is not an atomic membership snapshot.
pub struct LogInventoryPage {
    member: NodeId,
    topology: Digest,
    observed_at_ms: i64,
    total_logs: usize,
    entries: Vec<FollowerLogObservation>,
    next: Option<LogInventoryCursor>,
}

impl LogInventoryPage {
    /// Returns the physical follower node whose references were discovered.
    #[must_use]
    pub const fn member(&self) -> NodeId {
        self.member
    }
    /// Returns the fingerprint for this directory and discovered log topology.
    #[must_use]
    pub const fn topology(&self) -> Digest {
        self.topology
    }
    /// Returns the caller's capture time, without renewing any owner lease.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
    /// Counts all matching current logs, including logs outside this page.
    #[must_use]
    pub const fn total_logs(&self) -> usize {
        self.total_logs
    }
    /// Returns at most 128 references, sorted by leader session.
    #[must_use]
    pub fn entries(&self) -> &[FollowerLogObservation] {
        &self.entries
    }
    /// Continues a scan only while discovered epoch membership still matches.
    #[must_use]
    pub const fn next(&self) -> Option<LogInventoryCursor> {
        self.next
    }
}

impl NodeDirectory {
    /// Discovers current log references to a follower, including expired records
    /// and fenced/recovering tombstones that `live` intentionally omits.
    ///
    /// Scans stream at most 10,000 records and retain only the requested window.
    /// Cursor changes require a restart. Object listings alone are not atomic:
    /// the fleet observer must additionally prove its membership/enrollment
    /// barrier and reread exact epoch authority before maintenance finalization.
    /// Wrap the call in the caller's deadline; failure yields no partial page.
    pub async fn follower_logs_page(
        &self,
        member: NodeId,
        cursor: Option<LogInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> Result<LogInventoryPage> {
        if member.as_bytes() == &[0; 16] || !(1..=MAX_PAGE_ENTRIES).contains(&limit) || now_ms < 0 {
            return Err(Error::Node("invalid follower log inventory bounds"));
        }
        let prefix = self.layout.node_directory_path();
        let mut objects = self.layout.store().inner().list(Some(&prefix));
        let mut seen = HashSet::with_capacity(MAX_LIVE_NODE_RECORDS);
        let mut window = BTreeMap::new();
        let mut combined = [0_u8; 32];
        let mut total_logs = 0;
        let mut cursor_found = cursor.is_none();
        while let Some(object) = objects.next().await {
            if seen.len() == MAX_LIVE_NODE_RECORDS {
                return Err(Error::Capacity("follower log inventory record bound"));
            }
            let meta = object.map_err(|error| map_object_store_error(error, prefix.as_ref()))?;
            let Some((record, _)) = self.load_record_at(&meta.location).await? else {
                // A disappearing record does not establish a complete scan.
                return Err(Error::Node("log inventory record changed during scan"));
            };
            let session = record.session();
            validate_record_path(&self.layout, session, &meta.location)?;
            if !seen.insert(session) {
                return Err(Error::Node("duplicate log inventory session"));
            }
            let (node, leader_state) = match &record {
                NodeRecord::Advertisement(advertisement) => {
                    advertisement.validate_shape()?;
                    advertisement.verify_signature()?;
                    if advertisement.fleet != self.fleet
                        || advertisement.issued_at_ms > now_ms.saturating_add(MAX_CLOCK_SKEW_MS)
                    {
                        return Err(Error::Node("log inventory fleet or issue time differs"));
                    }
                    (
                        advertisement.node,
                        if advertisement.expires_at_ms > now_ms {
                            LogLeaderState::Live
                        } else {
                            LogLeaderState::Expired
                        },
                    )
                }
                NodeRecord::Tombstone(tombstone) => (tombstone.node, LogLeaderState::Fenced),
            };
            let Some(log) = record.log().filter(|log| log.members().contains(&member)) else {
                continue;
            };
            total_logs += 1;
            // Commutative digest makes arbitrary object listing order harmless.
            // Duplicate sessions are rejected separately. Volatile coverage and
            // heartbeat times do not reset topology; exact actions recheck them.
            let mut hash = blake3::Hasher::new();
            hash.update(session.as_bytes());
            hash.update(node.as_bytes());
            hash.update(&log.epoch().to_le_bytes());
            hash.update(&[match log.phase() {
                NodeLogPhase::Open => 1,
                NodeLogPhase::Recovering => 2,
                NodeLogPhase::Sealed => 3,
                NodeLogPhase::Retired => 4,
            }]);
            for enrolled in log.members() {
                hash.update(enrolled.as_bytes());
            }
            for (combined, byte) in combined.iter_mut().zip(hash.finalize().as_bytes()) {
                *combined ^= byte;
            }
            if cursor.is_some_and(|cursor| cursor.after == session) {
                cursor_found = true;
            }
            if cursor.is_some_and(|cursor| session.as_bytes() <= cursor.after.as_bytes()) {
                continue;
            }
            window.insert(
                *session.as_bytes(),
                FollowerLogObservation {
                    leader: session,
                    leader_node: node,
                    leader_state,
                    log: log.clone(),
                },
            );
            if window.len() > limit + 1 {
                window.pop_last();
            }
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule-authoritative-log-inventory-v1");
        hash.update(&self.inventory_scope);
        hash.update(member.as_bytes());
        hash.update(&(total_logs as u64).to_le_bytes());
        hash.update(&combined);
        let topology = Digest::from_bytes(*hash.finalize().as_bytes());
        if !cursor_found || cursor.is_some_and(|cursor| cursor.topology != topology) {
            return Err(Error::Node("log inventory topology changed; restart scan"));
        }
        let more = window.len() > limit;
        if more {
            window.pop_last();
        }
        let entries = window.into_values().collect::<Vec<_>>();
        let next = if more {
            entries.last().map(|last| LogInventoryCursor {
                topology,
                after: last.leader,
            })
        } else {
            None
        };
        Ok(LogInventoryPage {
            member,
            topology,
            observed_at_ms: now_ms,
            total_logs,
            entries,
            next,
        })
    }
}

#[cfg(test)]
mod tests;
