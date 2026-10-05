//! Bounded discovery of authoritative log references, including failed owners.

use std::collections::HashSet;

mod batch;

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
    /// Traverses the directory once for bounded physical-follower windows.
    /// Returns pages in request order with the same cursor/digest contract as
    /// `follower_logs_page`. The sum of requested page limits is at most 128;
    /// each window retains at most one additional lookahead row. Any invalid
    /// record or cursor rejects the entire batch. This is interval evidence,
    /// not atomic membership or permission to finalize a node.
    pub async fn follower_logs_pages(
        &self,
        requests: &[(NodeId, Option<LogInventoryCursor>)],
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<LogInventoryPage>> {
        if requests.is_empty()
            || requests.len() > MAX_PAGE_ENTRIES
            || limit == 0
            || limit > MAX_PAGE_ENTRIES / requests.len()
            || now_ms < 0
        {
            return Err(Error::Node("invalid follower log inventory batch bounds"));
        }
        let mut members = HashSet::new();
        for (member, _) in requests {
            if member.as_bytes() == &[0; 16] || !members.insert(*member) {
                return Err(Error::Node("invalid follower log inventory batch member"));
            }
        }
        batch::collect(self, requests, limit, now_ms).await
    }

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
        self.follower_logs_pages(&[(member, cursor)], limit, now_ms)
            .await?
            .into_iter()
            .next()
            .ok_or(Error::Node("follower log inventory page missing"))
    }
}

#[cfg(test)]
mod tests;
