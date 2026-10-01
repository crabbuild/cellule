//! Complete member retirement evidence for maintenance, preserving ordinary rotation.
use super::*;
use crate::follower::FollowerReceipt;
use crate::node::log_transport::RetireRequest;
use futures_util::future::join_all;

/// One authenticated member's original retirement response or transport error.
#[derive(Clone, Debug)]
pub struct NodeLogMemberRetirement {
    member: NodeId,
    result: std::result::Result<FollowerReceipt, Arc<Error>>,
}
impl NodeLogMemberRetirement {
    /// Returns the exact member addressed by the retirement request.
    #[must_use]
    pub const fn member(&self) -> NodeId {
        self.member
    }
    /// Returns the original checked receipt or error, independently of siblings.
    pub fn result(&self) -> std::result::Result<FollowerReceipt, Arc<Error>> {
        self.result.clone()
    }
}

/// Joined retirement responses for one object-covered, issuance-stopped epoch.
/// Failed members remain obligations; ordinary epoch closure is a separate fact.
#[derive(Clone, Debug)]
pub struct NodeLogRetirementObservation {
    barrier: NodeLogRotationBarrier,
    members: Vec<NodeLogMemberRetirement>,
}
impl NodeLogRetirementObservation {
    /// Returns the exact original epoch and contiguous object-coverage barrier.
    #[must_use]
    pub const fn barrier(&self) -> &NodeLogRotationBarrier {
        &self.barrier
    }
    /// Returns every member in barrier order, including every original failure.
    #[must_use]
    pub fn members(&self) -> &[NodeLogMemberRetirement] {
        &self.members
    }
    /// Issues a proof only after every addressed member confirmed its exact fence.
    /// The proof does not by itself assert leader-directory closure or lane deletion.
    pub fn confirmed(&self) -> Result<NodeLogRetirementProof> {
        for member in &self.members {
            if let Err(source) = &member.result {
                return Err(Error::Facility {
                    name: "node-log-member-retirement",
                    source: Box::new(RetainedRetirementError(Arc::clone(source))),
                });
            }
        }
        Ok(NodeLogRetirementProof {
            barrier: self.barrier.clone(),
        })
    }
}

/// Opaque confirmation that every old member fsynced its exact append fence.
/// Produced only from joined, scope-bound transport responses after object coverage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLogRetirementProof {
    barrier: NodeLogRotationBarrier,
}
impl NodeLogRetirementProof {
    /// Returns the exact original leader, epoch, complete member set and watermark.
    #[must_use]
    pub const fn barrier(&self) -> &NodeLogRotationBarrier {
        &self.barrier
    }
}

#[derive(Debug)]
struct RetainedRetirementError(Arc<Error>);
impl std::fmt::Display for RetainedRetirementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
impl std::error::Error for RetainedRetirementError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub(crate) async fn retire_node_log(
    transport: Arc<dyn NodeLogTransport>,
    barrier: &NodeLogRotationBarrier,
) -> Result<NodeLogRetirementObservation> {
    // Join healthy siblings even after another member fails. Cancellation may
    // lose a reply, but never creates a proof or changes the complete member set.
    let retirements = join_all(barrier.members().iter().map(|member| {
        let transport = Arc::clone(&transport);
        let member = *member;
        let request = RetireRequest {
            leader_session: barrier.leader_session(),
            log_epoch: barrier.log_epoch(),
            covered_through: barrier.covered_through(),
        };
        async move { (member, transport.retire(member, request).await) }
    }))
    .await;
    let expected_base = barrier.covered_through().saturating_add(1);
    let mut members = Vec::with_capacity(retirements.len());
    for (member, result) in retirements {
        if let Ok(receipt) = &result
            && (receipt.base_sequence != expected_base
                || receipt.durable_through != barrier.covered_through())
        {
            // Preserve the ordinary protocol-error contract: even best-effort
            // rotation cannot advance authority after a contradictory receipt.
            return Err(Error::Node("follower retire receipt differs"));
        }
        members.push(NodeLogMemberRetirement {
            member,
            result: result.map_err(Arc::new),
        });
    }
    Ok(NodeLogRetirementObservation {
        barrier: barrier.clone(),
        members,
    })
}
