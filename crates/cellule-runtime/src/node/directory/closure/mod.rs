//! Permanent session fencing with no outstanding leader-log retirement.
use super::*;

mod fence;
pub use fence::NodeSessionFence;

/// Canonical original boot fence with every enrolled log recovered or retired.
/// The optional manifest names the complete sealed suffix set, including other
/// applications. This grants no process closure, bundle availability, successor
/// serving, follower settlement or permission to stop a physical node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSessionRecovery {
    fence: NodeSessionFence,
    log: Option<NodeLogStatus>,
}
impl NodeSessionRecovery {
    /// Exact permanent physical boot fence.
    #[must_use]
    pub const fn fence(&self) -> &NodeSessionFence {
        &self.fence
    }
    /// Complete canonical sealed/retired log, or explicitly no enrolled log.
    #[must_use]
    pub const fn log(&self) -> Option<&NodeLogStatus> {
        self.log.as_ref()
    }
}

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
    /// The same original permanent fence, without terminal-log assertions.
    #[must_use]
    pub fn fence(&self) -> NodeSessionFence {
        NodeSessionFence::from_closure(self)
    }
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
    /// Reads the exact original fence and complete recovered log boundary.
    /// Open/Recovering logs refuse even when inactive; missing/live sessions are
    /// not interpreted as empty suffix sets. Authenticate the live claimant and
    /// canonical backend independently. This starts no recovery or retirement.
    pub async fn recovered_session(
        &self,
        node: NodeId,
        session: SessionId,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<NodeSessionRecovery> {
        let current = self
            .fenced_session_record(node, session, claimant, now_ms)
            .await?;
        if current
            .log
            .as_ref()
            .is_some_and(|log| !matches!(log.phase(), NodeLogPhase::Sealed | NodeLogPhase::Retired))
        {
            return Err(Error::Control("node session log recovery is incomplete"));
        }
        crate::node::bundle::store::ensure_session_drained(
            &self.layout,
            current.session,
            current.bundle,
        )
        .await?;
        Ok(NodeSessionRecovery {
            fence: NodeSessionFence::from_record(&current),
            log: current.log.clone(),
        })
    }

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
        let current = self
            .fenced_session_record(node, session, claimant, now_ms)
            .await?;
        if current
            .log
            .as_ref()
            .is_some_and(|log| log.phase() != NodeLogPhase::Retired)
        {
            return Err(Error::Control("node session log retirement is incomplete"));
        }
        crate::node::bundle::store::ensure_session_drained(
            &self.layout,
            current.session,
            current.bundle,
        )
        .await?;
        Ok(NodeSessionClosure {
            node: current.node,
            session: current.session,
            expires_at_ms: current.expires_at_ms,
            retired_at_ms: current.retired_at_ms,
            log: current.log.clone(),
        })
    }
}
