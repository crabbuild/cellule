//! Finite live-owner embedding adapter; all effects use the canonical directory.
use super::*;
use bytes::Bytes;
use cellule_host::{FacilityResult, FleetNodeDurabilityProvider, FleetNodeLogRecruitment};
use cellule_runtime::{
    Error,
    follower::FollowerReceipt,
    node::{
        VersionedNodeAdvertisement,
        durability::NodeLogAuthority,
        log::{NodeLogRetirementObservation, NodeLogRotationBarrier},
        log_transport::{
            AppendRequest, LocalFollowerTransport, NodeLogTransport, RetireRequest, SealRequest,
            TailRequest,
        },
    },
};
use futures_util::future::BoxFuture;
use std::sync::atomic::AtomicBool;

struct CoverageGate {
    held: AtomicBool,
    changed: tokio::sync::Notify,
}

// Hold coverage publication until maintenance observes a real retained tail.
// Otherwise a fast root checkpoint can prune the fixture before its first check.
// Drop opens the gate on every failed setup or scenario exit so shutdown joins.
pub(super) struct CoverageHold(Arc<CoverageGate>);
impl CoverageHold {
    pub(super) fn release(&self) {
        self.0.held.store(false, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
impl Drop for CoverageHold {
    fn drop(&mut self) {
        self.release();
    }
}
impl CoverageGate {
    async fn wait(&self) {
        while self.held.load(Ordering::Acquire) {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.held.load(Ordering::Acquire) {
                changed.await;
            }
        }
    }
}

pub(super) struct LiveFollowers {
    directory: NodeDirectory,
    locals: Vec<(NodeId, LocalFollowerTransport)>,
    pub appends: AtomicUsize,
    coverage_gate: Arc<CoverageGate>,
}
impl LiveFollowers {
    pub fn new(
        directory: NodeDirectory,
        locals: Vec<(NodeId, LocalFollowerTransport)>,
    ) -> (Self, CoverageHold) {
        let coverage_gate = Arc::new(CoverageGate {
            held: AtomicBool::new(true),
            changed: tokio::sync::Notify::new(),
        });
        (
            Self {
                directory,
                locals,
                appends: AtomicUsize::new(0),
                coverage_gate: coverage_gate.clone(),
            },
            CoverageHold(coverage_gate),
        )
    }
    fn local(&self, member: NodeId) -> cellule_runtime::Result<&LocalFollowerTransport> {
        self.locals
            .iter()
            .find(|(node, _)| *node == member)
            .map(|(_, local)| local)
            .ok_or(Error::Fenced)
    }
}
impl NodeLogTransport for LiveFollowers {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async move {
            self.directory
                .authorize_log_append(
                    request.leader_session,
                    member,
                    request.log_epoch,
                    request.covered_through,
                    clock()?,
                )
                .await?;
            let receipt = self.local(member)?.append(member, request).await?;
            self.appends.fetch_add(1, Ordering::AcqRel);
            Ok(receipt)
        })
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async move {
            self.directory
                .authorize_log_retire(
                    request.leader_session,
                    member,
                    request.log_epoch,
                    request.covered_through,
                    clock()?,
                )
                .await?;
            self.local(member)?.retire(member, request).await
        })
    }
    fn seal<'a>(
        &'a self,
        _member: NodeId,
        _request: SealRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async {
            Err(Error::PeerAuthorization(
                "live maintenance adapter has no recovery claimant",
            ))
        })
    }
    fn tail<'a>(
        &'a self,
        _member: NodeId,
        _request: TailRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<Vec<Bytes>>> {
        Box::pin(async {
            Err(Error::PeerAuthorization(
                "live maintenance adapter has no recovery claimant",
            ))
        })
    }
}

#[derive(Default)]
struct History {
    members: HashMap<u64, Vec<NodeId>>,
    closed: HashMap<u64, NodeLogRotationBarrier>,
}
pub(super) struct Authority {
    directory: NodeDirectory,
    history: tokio::sync::Mutex<History>,
    coverage_gate: Arc<CoverageGate>,
}
impl Authority {
    fn new(directory: NodeDirectory, coverage_gate: Arc<CoverageGate>) -> Self {
        Self {
            directory,
            history: tokio::sync::Mutex::new(History::default()),
            coverage_gate,
        }
    }
    async fn current(
        &self,
        epoch: u64,
        history: &History,
    ) -> cellule_runtime::Result<VersionedNodeAdvertisement> {
        let observed = self
            .directory
            .load_if_live(session(0), clock()?)
            .await?
            .ok_or(Error::Fenced)?;
        if observed.advertisement().node() != node_id(0)
            || observed.advertisement().log().is_none_or(|log| {
                log.epoch() != epoch
                    || history
                        .members
                        .get(&epoch)
                        .is_none_or(|members| log.members() != members)
            })
        {
            return Err(Error::Fenced);
        }
        Ok(observed)
    }
}
impl NodeLogAuthority for Authority {
    fn activate<'a>(&'a self, epoch: u64) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let history = self.history.lock().await;
            let observed = self.current(epoch, &history).await?;
            self.directory.activate_log(&observed, clock()?).await?;
            Ok(())
        })
    }
    fn advance_coverage<'a>(
        &'a self,
        epoch: u64,
        through: u64,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            // Bootstrap may publish its empty prefix. Later coverage waits
            // outside the authority lock, then rechecks the current generation.
            if through != 0 {
                self.coverage_gate.wait().await;
            }
            let history = self.history.lock().await;
            let observed = self.current(epoch, &history).await?;
            self.directory
                .advance_log_coverage(&observed, through, clock()?)
                .await?;
            Ok(())
        })
    }
    fn close<'a>(
        &'a self,
        retirement: &'a NodeLogRetirementObservation,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            retirement.confirmed()?;
            let mut history = self.history.lock().await;
            let barrier = retirement.barrier();
            if let Some(original) = history.closed.get(&barrier.log_epoch()) {
                return if original == barrier {
                    Ok(())
                } else {
                    Err(Error::Fenced)
                };
            }
            let observed = self.current(barrier.log_epoch(), &history).await?;
            self.directory
                .close_log(&observed, barrier, clock()?)
                .await?;
            // Retain the exact checked barrier before returning the close reply.
            // Mere absence of the log cannot prove a particular original close.
            history.closed.insert(barrier.log_epoch(), barrier.clone());
            Ok(())
        })
    }
}

pub(super) struct Provider {
    directory: NodeDirectory,
    transport: Arc<LiveFollowers>,
    authority: Arc<Authority>,
    lease: NodeLeaseGuard,
    prepared: AtomicU64,
}
impl Provider {
    pub fn new(
        directory: NodeDirectory,
        transport: Arc<LiveFollowers>,
        lease: NodeLeaseGuard,
    ) -> Self {
        Self {
            authority: Arc::new(Authority::new(
                directory.clone(),
                transport.coverage_gate.clone(),
            )),
            directory,
            transport,
            lease,
            prepared: AtomicU64::new(0),
        }
    }
}
impl FleetNodeDurabilityProvider for Provider {
    fn rotation_required(
        self: Arc<Self>,
        _live_node_limit: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = FacilityResult<bool>> + Send>> {
        Box::pin(async { Ok(false) })
    }
    fn prepare(
        self: Arc<Self>,
        _limits: Limits,
        bytes: u64,
        live: usize,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = FacilityResult<Option<FleetNodeLogRecruitment>>>
                + Send,
        >,
    > {
        Box::pin(async move {
            // This finite command owns exactly two epochs. Failed preparation
            // still consumes its epoch; native attempts are never restamped.
            let index = match self
                .prepared
                .try_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                    (old < 2).then_some(old + 1)
                }) {
                Ok(index) => index,
                Err(_) => return Ok(None),
            };
            let now = clock()?;
            let source = self
                .directory
                .load_if_live(session(0), now)
                .await?
                .ok_or(Error::Fenced)?;
            let prepared = self
                .directory
                .prepare_log_enrollment(&source, index + 1, bytes, live, now)
                .await?
                .ok_or(Error::Fenced)?;
            let attempt = self
                .directory
                .prepare_log_enrollment_attempt(&prepared, now)
                .await?;
            self.authority
                .history
                .lock()
                .await
                .members
                .insert(prepared.log().epoch(), prepared.log().members().to_vec());
            Ok(Some(FleetNodeLogRecruitment::new(
                self.directory.clone(),
                attempt,
                self.transport.clone(),
                self.authority.clone(),
                self.lease.clone(),
                Default::default(),
            )?))
        })
    }
}
