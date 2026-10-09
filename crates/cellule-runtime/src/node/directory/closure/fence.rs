use super::*;

/// Permanent canonical fence of an exact physical boot, independent of recovery.
///
/// This proves no process termination, accepted-work joining, log retirement,
/// Cell relocation or withdrawal. Applications obtain original process evidence
/// separately before using a fence to retain affected Cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSessionFence {
    node: NodeId,
    session: SessionId,
    expires_at_ms: i64,
    retired_at_ms: i64,
}

impl NodeSessionFence {
    /// Physical node retained in the permanent tombstone.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Original boot session that cannot renew its advertisement.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Original advertised expiry, without claiming process closure.
    #[must_use]
    pub const fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }
    /// First permanent fencing time, unaffected by claims or recovery.
    #[must_use]
    pub const fn retired_at_ms(&self) -> i64 {
        self.retired_at_ms
    }

    pub(super) fn from_record(record: &NodeTombstone) -> Self {
        Self {
            node: record.node,
            session: record.session,
            expires_at_ms: record.expires_at_ms,
            retired_at_ms: record.retired_at_ms,
        }
    }

    pub(super) fn from_closure(closure: &NodeSessionClosure) -> Self {
        Self {
            node: closure.node,
            session: closure.session,
            expires_at_ms: closure.expires_at_ms,
            retired_at_ms: closure.retired_at_ms,
        }
    }
}

impl NodeDirectory {
    /// Reads the permanent original boot fence while its leader log may still
    /// need recovery. It starts no claim, recovery, retirement or process effect.
    /// Authenticate the live claimant and original signing identity separately.
    pub async fn fenced_session(
        &self,
        node: NodeId,
        session: SessionId,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<NodeSessionFence> {
        let current = self
            .fenced_session_record(node, session, claimant, now_ms)
            .await?;
        Ok(NodeSessionFence::from_record(&current))
    }

    pub(super) async fn fenced_session_record(
        &self,
        node: NodeId,
        session: SessionId,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<Box<NodeTombstone>> {
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
        Ok(current)
    }
}
