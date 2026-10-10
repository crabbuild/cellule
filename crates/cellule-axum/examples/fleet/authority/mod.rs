//! Ordered catalog mutations with a separate node-record/heartbeat lock.
use super::*;
use cellule_runtime::node::durability::NodeLogAuthority;
use cellule_runtime::node::durability::{
    BundleCheckpoint, NodeBundleAuthority, NodeBundlePublicationAuthority,
};
use cellule_runtime::node::log::{NodeLogRetirementObservation, NodeLogRotationBarrier};
use cellule_runtime::node::{
    NodeAdvertisement, NodeCapacity, NodeFailureDomain, VersionedNodeAdvertisement,
};
use futures_util::future::BoxFuture;
use tokio::sync::{Mutex, watch};

pub(super) struct Authority {
    pub directory: NodeDirectory,
    pub lease: NodeLeaseGuard,
    // Catalog changes always acquire this before state. Heartbeats and native
    // coverage preserve the catalog head and need only state, so a slow bundle
    // preparation/PUT cannot prevent them from renewing the original lease.
    catalog: Mutex<()>,
    state: Mutex<State>,
    tls: Arc<LoadedPeerTls>,
    code: Digest,
    follower: Option<cellule_runtime::follower::FollowerStore>,
    shutdown_error: std::sync::OnceLock<Arc<Error>>,
}

struct State {
    observed: VersionedNodeAdvertisement,
    closed: Option<NodeLogRotationBarrier>,
}

pub(super) struct Enrollment {
    pub authority: Arc<Authority>,
    stop: watch::Sender<bool>,
    heartbeat: tokio::task::JoinHandle<Result<()>>,
}

impl Enrollment {
    pub async fn start(
        directory: NodeDirectory,
        tls: Arc<LoadedPeerTls>,
        index: u8,
        session: SessionId,
        endpoint: String,
        code: Digest,
        follower: Option<cellule_runtime::follower::FollowerStore>,
    ) -> Result<Self> {
        let now = clock()?;
        let advertisement = NodeAdvertisement::sign(
            node(index),
            session,
            endpoint,
            tls.fleet(),
            tls.certificate(),
            code,
            code,
            tls.signing_key(),
            1,
            now,
            now + 30_000,
            vec![code],
            vec![1],
            NodeFailureDomain::default(),
            capacity(follower.as_ref()),
        )?;
        let observed = directory.create(advertisement, now).await?;
        // Monotonic admission starts only after the authoritative create returned.
        let lease = NodeLeaseGuard::new(clock()?, observed.advertisement().expires_at_ms())?;
        let authority = Arc::new(Authority {
            directory,
            lease,
            catalog: Mutex::new(()),
            state: Mutex::new(State {
                observed,
                closed: None,
            }),
            tls,
            code,
            follower,
            shutdown_error: std::sync::OnceLock::new(),
        });
        let (stop, mut stopping) = watch::channel(false);
        let running = authority.clone();
        let heartbeat = tokio::spawn(async move {
            let result = async {
                loop {
                    tokio::select! {
                        _ = stopping.changed() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(10)) => running.refresh().await?,
                    }
                }
            }
            .await;
            if let Err(error) = &result {
                eprintln!("Node heartbeat failed: {error:?}");
                running.lease.fence();
            }
            result
        });
        Ok(Self {
            authority,
            stop,
            heartbeat,
        })
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop.send(true);
        let joined = self.heartbeat.await.map_err(Error::FollowerWorkerJoin);
        let withdrawn = async {
            let _catalog = self.authority.catalog.lock().await;
            let state = self.authority.state.lock().await;
            self.authority
                .directory
                .withdraw_after_drain(&state.observed, clock()?)
                .await
        }
        .await;
        self.authority.lease.fence();
        joined.and_then(|result| result).and(withdrawn)
    }
}

impl Authority {
    async fn refresh(&self) -> Result<()> {
        let _state = self.live_state().await?;
        Ok(())
    }

    async fn live_state(&self) -> Result<tokio::sync::MutexGuard<'_, State>> {
        self.lease.check()?;
        let mut state = self.state.lock().await;
        self.renew_if_due(&mut state).await?;
        Ok(state)
    }

    async fn renew_if_due(&self, state: &mut State) -> Result<()> {
        // FIFO close/checkpoint queues can outlast the heartbeat's renewal
        // window. Whichever operation acquires the state maintains the same
        // authoritative lease before doing more work. Expiry stays terminal,
        // including time spent waiting for this mutex or the refresh CAS.
        self.lease.check()?;
        if self.lease.remaining() > Duration::from_secs(20) {
            return Ok(());
        }
        let previous = state.observed.advertisement();
        let now = clock()?;
        let next = NodeAdvertisement::sign(
            previous.node(),
            previous.session(),
            previous.endpoint().into(),
            self.tls.fleet(),
            self.tls.certificate(),
            self.code,
            self.code,
            self.tls.signing_key(),
            previous
                .progress()
                .checked_add(1)
                .ok_or(Error::Node("capacity heartbeat progress overflow"))?,
            now,
            now + 30_000,
            vec![self.code],
            vec![1],
            previous.failure_domain().clone(),
            capacity(self.follower.as_ref()),
        )?;
        state.observed = self.directory.refresh(&state.observed, next, now).await?;
        self.lease
            .renew(clock()?, state.observed.advertisement().expires_at_ms())
    }

    pub async fn recruit(&self) -> Result<Vec<NodeAdvertisement>> {
        self.lease.check()?;
        let _catalog = self.catalog.lock().await;
        let mut state = self.live_state().await?;
        state.observed = self
            .directory
            .recruit_log(&state.observed, 1, 16 << 20, 3, clock()?)
            .await?;
        state.observed = self
            .directory
            .initialize_bundle_lane(&state.observed, 1, clock()?)
            .await?;
        let members = state
            .observed
            .advertisement()
            .log()
            .ok_or(Error::Node("capacity log enrollment missing"))?
            .members();
        if members.len() != 2 {
            return Err(Error::Node("capacity benchmark requires two followers"));
        }
        let mut peers = Vec::new();
        for member in members {
            peers.push(
                self.directory
                    .resolve_node(*member, clock()?)
                    .await?
                    .ok_or(Error::Node("capacity follower enrollment missing"))?,
            );
        }
        Ok(peers)
    }

    async fn select_frames(
        &self,
        frames: &[cellule_ltx::VerifiedNodeFrame],
        assignments: &[cellule_runtime::node::log::AssignedCommitRange],
        checkpoints: &[BundleCheckpoint],
        prefixes: &[&cellule_runtime::node::bundle::BundleCoverageProof],
        lease: &NodeLeaseGuard,
    ) -> Result<Vec<cellule_runtime::node::bundle::BundleCoverageProof>> {
        let _catalog = self.catalog.lock().await;
        let current = {
            let state = self.live_state().await?;
            self.current(&state, 1).await?
        };
        let ready = ready_checkpoints(checkpoints).await?;
        let prepared = self
            .directory
            .prepare_node_bundle_with_checkpoints(
                &current,
                frames,
                assignments,
                &ready,
                cellule_ltx::Limits::default(),
                clock()?,
            )
            .await?;
        // Preparation owns immutable bytes and the catalog predecessor, not
        // the heartbeat state. Recheck the original live lease after upload and
        // preserve intervening renewals/coverage in the final serialized CAS.
        let mut state = self.live_state().await?;
        let current = self.current(&state, 1).await?;
        let (next, proofs) = self
            .directory
            .select_node_bundle_extending(
                &current,
                &prepared,
                lease,
                prefixes,
                cellule_ltx::Limits::default(),
                clock()?,
            )
            .await?;
        state.observed = next;
        self.lease.check()?;
        Ok(proofs)
    }

    async fn current(&self, state: &State, epoch: u64) -> Result<VersionedNodeAdvertisement> {
        self.lease.check()?;
        let original = state.observed.advertisement();
        let current = self
            .directory
            .load_if_live(original.session(), clock()?)
            .await?
            .ok_or(Error::Fenced)?;
        let actual = current.advertisement();
        if actual.node() != original.node()
            || actual.certificate() != original.certificate()
            || actual.endpoint() != original.endpoint()
            || actual.verifying_key()? != original.verifying_key()?
            || actual.log().is_none_or(|log| {
                log.epoch() != epoch
                    || original
                        .log()
                        .is_none_or(|before| log.members() != before.members())
            })
        {
            return Err(Error::Fenced);
        }
        self.lease.check()?;
        Ok(current)
    }
}

impl NodeBundleAuthority for Authority {
    fn bind<'a>(
        &'a self,
        authority: &'a cellule_runtime::control::authority::CellAuthority,
        observed: &'a cellule_runtime::control::authority::VersionedControl,
    ) -> BoxFuture<'a, Result<cellule_runtime::control::authority::VersionedControl>> {
        Box::pin(async move {
            let _catalog = self.catalog.lock().await;
            let mut state = self.live_state().await?;
            let current = self.current(&state, 1).await?;
            let (next, pinned) = self
                .directory
                .bind_bundle_cell(&current, authority, observed, clock()?)
                .await?;
            state.observed = next;
            self.lease.check()?;
            Ok(pinned)
        })
    }

    fn close<'a>(
        &'a self,
        authority: &'a cellule_runtime::control::authority::CellAuthority,
        observed: &'a cellule_runtime::control::authority::VersionedControl,
        issued: cellule_runtime::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Runtime joins the complete issued producer prefix before entering
            // this mutex, including captures whose Fleet ACK preceded selection.
            let _catalog = self.catalog.lock().await;
            let mut state = self.live_state().await?;
            let mut current = self.current(&state, issued.log_epoch()).await?;
            let proof = self
                .directory
                .load_bundle_coverage(authority, observed, cellule_ltx::Limits::default())
                .await?;
            current = self
                .directory
                .checkpoint_bundle_cell(
                    &current,
                    authority,
                    &proof,
                    cellule_ltx::Limits::default(),
                    clock()?,
                )
                .await?;
            state.observed = current;
            state.observed = self
                .directory
                .begin_bundle_close(&state.observed, proof.binding(), issued, clock()?)
                .await?;
            state.observed = self
                .directory
                .finish_bundle_close(&state.observed, proof.binding(), issued, clock()?)
                .await?;
            self.lease.check()
        })
    }
}

impl NodeBundlePublicationAuthority for Authority {
    fn select<'a>(
        &'a self,
        captures: &'a [cellule_runtime::node::log_shipper::AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        prefixes: &'a [&'a cellule_runtime::node::bundle::BundleCoverageProof],
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<cellule_runtime::node::bundle::BundleCoverageProof>>> {
        Box::pin(async move {
            let frames = captures
                .iter()
                .flat_map(|c| c.frames().iter().cloned())
                .collect::<Vec<_>>();
            let assignments = captures.iter().map(|c| c.assignment()).collect::<Vec<_>>();
            self.select_frames(&frames, &assignments, checkpoints, prefixes, lease)
                .await
        })
    }

    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let _catalog = self.catalog.lock().await;
            let mut state = self.live_state().await?;
            let current = self.current(&state, 1).await?;
            let ready = ready_checkpoints(checkpoints).await?;
            state.observed = if ready.is_empty() {
                current
            } else {
                self.directory
                    .checkpoint_bundle_cells(
                        &current,
                        &ready,
                        cellule_ltx::Limits::default(),
                        clock()?,
                    )
                    .await?
            };
            self.lease.check()
        })
    }
}

fn capacity(follower: Option<&cellule_runtime::follower::FollowerStore>) -> NodeCapacity {
    // Memory/disk hints are fixture admission ceilings, not measured physical
    // headroom. Follower retention and free budget come from the canonical store.
    NodeCapacity {
        free_memory_bytes: 256 << 20,
        free_disk_bytes: follower.map_or(1 << 30, |store| store.available_bytes()),
        follower_free_bytes: follower.map_or(0, |store| store.available_bytes()),
        follower_retained_bytes: follower.map_or(0, |store| store.retained_bytes()),
        job_credits: 1,
        log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
    }
}

impl NodeLogAuthority for Authority {
    fn observe_shutdown_failure(&self, error: Arc<Error>) -> Result<()> {
        // Retain and report the original blocker without logging every retry.
        // This observation grants no authority to skip coverage or retirement.
        if self.shutdown_error.set(error.clone()).is_ok() {
            eprintln!("Capacity node log drain is retrying: {error:?}");
        }
        Ok(())
    }

    fn requires_confirmed_retirement(&self) -> bool {
        true
    }

    fn activate<'a>(&'a self, epoch: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut state = self.live_state().await?;
            let current = self.current(&state, epoch).await?;
            state.observed = self.directory.activate_log(&current, clock()?).await?;
            self.lease.check()
        })
    }

    fn advance_coverage<'a>(&'a self, epoch: u64, through: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut state = self.live_state().await?;
            let current = self.current(&state, epoch).await?;
            state.observed = self
                .directory
                .advance_log_coverage(&current, through, clock()?)
                .await?;
            self.lease.check()
        })
    }

    fn close<'a>(
        &'a self,
        retirement: &'a NodeLogRetirementObservation,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            retirement.confirmed()?;
            let _catalog = self.catalog.lock().await;
            let mut state = self.state.lock().await;
            if let Some(closed) = &state.closed {
                return if closed == retirement.barrier() {
                    Ok(())
                } else {
                    Err(Error::Fenced)
                };
            }
            self.renew_if_due(&mut state).await?;
            let current = self
                .current(&state, retirement.barrier().log_epoch())
                .await?;
            state.observed = self
                .directory
                .close_log(&current, retirement.barrier(), clock()?)
                .await?;
            // Retain the original checked closure; arbitrary later absence is no proof.
            state.closed = Some(retirement.barrier().clone());
            self.lease.check()
        })
    }
}

async fn ready_checkpoints(
    checkpoints: &[BundleCheckpoint],
) -> Result<
    Vec<(
        &cellule_runtime::control::authority::CellAuthority,
        &cellule_runtime::node::bundle::BundleCoverageProof,
    )>,
> {
    let mut ready = Vec::new();
    for checkpoint in checkpoints {
        let root = checkpoint.root();
        let observed = checkpoint
            .authority()
            .load(cellule_runtime::CellId::from_bytes(root.cell))
            .await?
            .ok_or(Error::Fenced)?;
        let actual = observed.value().ltx_root().ok_or(Error::Fenced)?;
        if observed.value().bundle_binding == Some(checkpoint.proof().binding())
            && actual.cell == root.cell
            && actual.incarnation == root.incarnation
            && actual.commit_sequence > root.commit_sequence
        {
            // The later root's original notification is retained by its
            // joined publisher. This observation releases no locators.
            continue;
        }
        ready.push((checkpoint.authority(), checkpoint.proof()));
    }
    Ok(ready)
}

#[cfg(test)]
mod publication_tests;
#[cfg(test)]
mod renewal_tests;
