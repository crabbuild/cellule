//! Permanent session fencing with no outstanding leader-log retirement.
use super::*;

/// Canonical permanent fence for one original physical boot and its leader log.
/// A Retired log retains its original ensemble and pinned manifest. This proves
/// neither process termination nor Cell relocation, foreign roles or withdrawal.
/// Applications must separately prove those before completing maintenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSessionClosure {
    node: NodeId,
    session: SessionId,
    expires_at_ms: i64,
    retired_at_ms: i64,
    log: Option<NodeLogStatus>,
}

impl NodeSessionClosure {
    /// Physical identity retained by the canonical tombstone.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Original fenced session, never a replacement boot.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Original advertised expiry; expiry alone supplies no closure.
    #[must_use]
    pub const fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }
    /// First permanent fencing time, independent of recovery claim renewal.
    #[must_use]
    pub const fn retired_at_ms(&self) -> i64 {
        self.retired_at_ms
    }
    /// Exact terminal original log, or no enrolled log in the canonical fence.
    #[must_use]
    pub const fn log(&self) -> Option<&NodeLogStatus> {
        self.log.as_ref()
    }
}

impl NodeDirectory {
    /// Freshly confirms the exact physical boot's permanent fence and terminal
    /// leader log. Missing/live/expired advertisements and Open/Recovering/Sealed
    /// logs refuse; even an inactive enrolled log must complete retirement.
    /// Authenticate the live claimant before calling. The retained claimant and
    /// its claim expiry are excluded from this immutable session closure.
    pub async fn closed_session(
        &self,
        node: NodeId,
        session: SessionId,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<NodeSessionClosure> {
        if now_ms < 0 || claimant == session {
            return Err(Error::Fenced);
        }
        self.load(claimant, now_ms).await?.ok_or(Error::Fenced)?;
        let path = self.layout.node_path(session.as_bytes());
        let Some((NodeRecord::Tombstone(current), _)) = self.load_record_at(&path).await? else {
            return Err(Error::Control("node session has no permanent fence"));
        };
        if current.node != node || current.session != session || current.retired_at_ms > now_ms {
            return Err(Error::Fenced);
        }
        if current
            .log
            .as_ref()
            .is_some_and(|log| log.phase() != NodeLogPhase::Retired)
        {
            return Err(Error::Control("node session log retirement is incomplete"));
        }
        Ok(NodeSessionClosure {
            node: current.node,
            session: current.session,
            expires_at_ms: current.expires_at_ms,
            retired_at_ms: current.retired_at_ms,
            log: current.log.clone(),
        })
    }
}
