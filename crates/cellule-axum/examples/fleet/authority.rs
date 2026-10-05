//! One serialized directory view for heartbeats and durability CAS callbacks.
use super::*;
use cellule_runtime::node::durability::NodeLogAuthority;
use cellule_runtime::node::log::{NodeLogRetirementObservation, NodeLogRotationBarrier};
use cellule_runtime::node::{
    NodeAdvertisement, NodeCapacity, NodeFailureDomain, VersionedNodeAdvertisement,
};
use futures_util::future::BoxFuture;
use tokio::sync::{Mutex, watch};

pub(super) struct Authority {
    pub directory: NodeDirectory,
    pub lease: NodeLeaseGuard,
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
            if result.is_err() {
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
        self.lease.check()?;
        let mut state = self.state.lock().await;
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
        let mut state = self.state.lock().await;
        state.observed = self
            .directory
            .recruit_log(&state.observed, 1, 16 << 20, 3, clock()?)
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

fn capacity(follower: Option<&cellule_runtime::follower::FollowerStore>) -> NodeCapacity {
    // Memory/disk hints are fixture admission ceilings, not measured physical
    // headroom. Follower retention and free budget come from the canonical store.
    NodeCapacity {
        free_memory_bytes: 256 << 20,
        free_disk_bytes: follower.map_or(1 << 30, |store| store.available_bytes()),
        follower_free_bytes: follower.map_or(0, |store| store.available_bytes()),
        follower_retained_bytes: follower.map_or(0, |store| store.retained_bytes()),
        job_credits: 1,
        log_protocol: 1,
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
            let mut state = self.state.lock().await;
            let current = self.current(&state, epoch).await?;
            state.observed = self.directory.activate_log(&current, clock()?).await?;
            self.lease.check()
        })
    }

    fn advance_coverage<'a>(&'a self, epoch: u64, through: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
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
            let mut state = self.state.lock().await;
            if let Some(closed) = &state.closed {
                return if closed == retirement.barrier() {
                    Ok(())
                } else {
                    Err(Error::Fenced)
                };
            }
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
