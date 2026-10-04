//! Complete receiver fence evidence after canonical recovery has pinned every tail.
use super::*;
use crate::node::log::NodeLogMemberRetirement;
use crate::node::log_transport::{RecoveredNodeLogTransport, RecoveredRetireRequest};

/// Joined responses from every original member, retaining individual failures.
pub struct RecoveredNodeLogRetirement {
    sealed: SealedNodeLog,
    members: Vec<NodeLogMemberRetirement>,
}
impl RecoveredNodeLogRetirement {
    /// Original canonical recovery completion used by all requests.
    #[must_use]
    pub const fn sealed(&self) -> &SealedNodeLog {
        &self.sealed
    }
    /// Every original member's response, including failed and ambiguous results.
    #[must_use]
    pub fn members(&self) -> &[NodeLogMemberRetirement] {
        &self.members
    }
    /// Confirms only a complete original ensemble of durable native fences.
    pub fn confirmed(&self) -> Result<RecoveredNodeLogRetirementProof> {
        for member in &self.members {
            if let Err(source) = member.result() {
                return Err(Error::Facility {
                    name: "recovered-log-member-retirement",
                    source: Box::new(RetainedError(source)),
                });
            }
        }
        Ok(RecoveredNodeLogRetirementProof {
            sealed: self.sealed.clone(),
            members: self.members.clone(),
        })
    }
}

/// Opaque complete native retirement evidence; canonical directory CAS is separate.
pub struct RecoveredNodeLogRetirementProof {
    sealed: SealedNodeLog,
    members: Vec<NodeLogMemberRetirement>,
}
impl RecoveredNodeLogRetirementProof {
    /// Exact recovered epoch, ensemble and original pinned manifest identity.
    #[must_use]
    pub const fn sealed(&self) -> &SealedNodeLog {
        &self.sealed
    }
    /// Original authenticated native fence responses retained by confirmation.
    #[must_use]
    pub fn members(&self) -> &[NodeLogMemberRetirement] {
        &self.members
    }
}

#[derive(Debug)]
struct RetainedError(Arc<Error>);
impl std::fmt::Display for RetainedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Retires the complete original ensemble after canonical recovery completion.
/// Transport receivers authenticate the requester and reread canonical authority.
/// Every member is joined even after a sibling fails. Cancelled/lost waiters
/// supply no proof; replay the same recovered epoch to adopt its native fences.
pub async fn retire_recovered_members(
    transport: Arc<dyn RecoveredNodeLogTransport>,
    sealed: &SealedNodeLog,
) -> Result<RecoveredNodeLogRetirement> {
    if !matches!(
        sealed.log().phase(),
        NodeLogPhase::Sealed | NodeLogPhase::Retired
    ) {
        return Err(Error::Fenced);
    }
    let responses = join_all(sealed.log().members().iter().map(|member| {
        let transport = Arc::clone(&transport);
        let request = RecoveredRetireRequest {
            sealed: sealed.clone(),
        };
        let member = *member;
        async move {
            let result = transport
                .retire_recovered(member, request)
                .await
                .and_then(|receipt| {
                    if receipt.base_sequence != receipt.durable_through.saturating_add(1) {
                        return Err(Error::Node("recovered follower retirement receipt differs"));
                    }
                    Ok(receipt)
                });
            NodeLogMemberRetirement::new(member, result)
        }
    }))
    .await;
    Ok(RecoveredNodeLogRetirement {
        sealed: sealed.clone(),
        members: responses,
    })
}
