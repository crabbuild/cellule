//! The same authenticated native member transport for recovery fault fixtures.
use super::*;

pub(in crate::scenario) struct Members {
    pub(super) peers: Vec<(NodeId, LocalRecoveredFollowerTransport)>,
    pub(super) retirements: AtomicUsize,
}
impl Members {
    pub(in crate::scenario) fn new(peers: Vec<(NodeId, LocalRecoveredFollowerTransport)>) -> Self {
        Self {
            peers,
            retirements: AtomicUsize::new(0),
        }
    }

    fn peer(&self, member: NodeId) -> &LocalRecoveredFollowerTransport {
        &self
            .peers
            .iter()
            .find(|(node, _)| *node == member)
            .unwrap()
            .1
    }
}
impl NodeLogTransport for Members {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.peer(member).append(member, request)
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.peer(member).seal(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.peer(member).retire(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<Vec<Bytes>>> {
        self.peer(member).tail(member, request)
    }
}
impl RecoveredNodeLogTransport for Members {
    fn retire_recovered<'a>(
        &'a self,
        member: NodeId,
        request: RecoveredRetireRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.retirements.fetch_add(1, Ordering::AcqRel);
        self.peer(member).retire_recovered(member, request)
    }
}
