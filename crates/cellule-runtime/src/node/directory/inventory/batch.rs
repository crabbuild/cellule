//! One fresh canonical traversal feeds independently bound follower windows.
use super::*;
use std::collections::BTreeMap;

pub(super) async fn collect(
    directory: &NodeDirectory,
    requests: &[(NodeId, Option<LogInventoryCursor>)],
    limit: usize,
    now_ms: i64,
) -> Result<Vec<LogInventoryPage>> {
    let mut windows = requests
        .iter()
        .map(|(member, cursor)| Window::new(*member, *cursor))
        .collect::<Vec<_>>();
    let prefix = directory.layout.node_directory_path();
    let mut objects = directory.layout.store().inner().list(Some(&prefix));
    let mut seen = HashSet::with_capacity(MAX_LIVE_NODE_RECORDS);
    while let Some(object) = objects.next().await {
        if seen.len() == MAX_LIVE_NODE_RECORDS {
            return Err(Error::Capacity("follower log inventory record bound"));
        }
        let meta = object.map_err(|error| map_object_store_error(error, prefix.as_ref()))?;
        let Some((record, _)) = directory.load_record_at(&meta.location).await? else {
            return Err(Error::Node("log inventory record changed during scan"));
        };
        let session = record.session();
        validate_record_path(&directory.layout, session, &meta.location)?;
        if !seen.insert(session) {
            return Err(Error::Node("duplicate log inventory session"));
        }
        let (node, leader_state) = match &record {
            NodeRecord::Advertisement(advertisement) => {
                // Authenticate every fresh body before filtering any follower,
                // including expired and foreign-release log obligations.
                advertisement.validate_shape()?;
                if advertisement.fleet != directory.fleet
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
        if let Some(log) = record.log() {
            for window in &mut windows {
                window.accept(session, node, leader_state, log, limit);
            }
        }
    }
    // A malformed cursor in any window rejects the entire traversal; no caller
    // can consume a successful prefix as proof that the full request completed.
    windows
        .into_iter()
        .map(|window| window.finish(directory.inventory_scope, limit, now_ms))
        .collect()
}

struct Window {
    member: NodeId,
    cursor: Option<LogInventoryCursor>,
    entries: BTreeMap<[u8; 16], FollowerLogObservation>,
    combined: [u8; 32],
    total_logs: usize,
    cursor_found: bool,
}
impl Window {
    fn new(member: NodeId, cursor: Option<LogInventoryCursor>) -> Self {
        Self {
            member,
            cursor,
            entries: BTreeMap::new(),
            combined: [0; 32],
            total_logs: 0,
            cursor_found: cursor.is_none(),
        }
    }
    fn accept(
        &mut self,
        session: SessionId,
        node: NodeId,
        state: LogLeaderState,
        log: &NodeLogStatus,
        limit: usize,
    ) {
        if !log.members().contains(&self.member) {
            return;
        }
        self.total_logs += 1;
        // Preserve the existing commutative topology digest byte-for-byte.
        // Coverage/heartbeat changes remain visible in exact recheck rows.
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
        for member in log.members() {
            hash.update(member.as_bytes());
        }
        for (combined, byte) in self.combined.iter_mut().zip(hash.finalize().as_bytes()) {
            *combined ^= byte;
        }
        if self.cursor.is_some_and(|cursor| cursor.after == session) {
            self.cursor_found = true;
        }
        if self
            .cursor
            .is_some_and(|cursor| session.as_bytes() <= cursor.after.as_bytes())
        {
            return;
        }
        self.entries.insert(
            *session.as_bytes(),
            FollowerLogObservation {
                leader: session,
                leader_node: node,
                leader_state: state,
                log: log.clone(),
            },
        );
        if self.entries.len() > limit + 1 {
            self.entries.pop_last();
        }
    }
    fn finish(mut self, scope: [u8; 16], limit: usize, now_ms: i64) -> Result<LogInventoryPage> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule-authoritative-log-inventory-v1");
        hash.update(&scope);
        hash.update(self.member.as_bytes());
        hash.update(&(self.total_logs as u64).to_le_bytes());
        hash.update(&self.combined);
        let topology = Digest::from_bytes(*hash.finalize().as_bytes());
        if !self.cursor_found
            || self
                .cursor
                .is_some_and(|cursor| cursor.topology != topology)
        {
            return Err(Error::Node("log inventory topology changed; restart scan"));
        }
        let more = self.entries.len() > limit;
        if more {
            self.entries.pop_last();
        }
        let entries = self.entries.into_values().collect::<Vec<_>>();
        let next = if more {
            entries.last().map(|last| LogInventoryCursor {
                topology,
                after: last.leader,
            })
        } else {
            None
        };
        Ok(LogInventoryPage {
            member: self.member,
            topology,
            observed_at_ms: now_ms,
            total_logs: self.total_logs,
            entries,
            next,
        })
    }
}
