//! Bounded startup and bucket-only crash takeover through canonical runtime APIs.
use std::{collections::BTreeMap, future::Future, path::Path, time::Duration};

use cellule_ltx::{CellReplica, Limits};
use cellule_runtime::{
    CatalogRole, CellRuntime, CellTarget, Digest, Error,
    cell::{
        actor::CellHandle,
        catalog::{CatalogEntry, CellCatalog},
    },
    control::{ControlState, Owner, authority::CellAuthority},
    identity::{IncarnationId, SessionId},
    ltx::CellStorageLayout,
    node::{NodeDirectory, NodeTakeoverProof},
    recovery::manifest::RecoveryManifestStore,
};
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use tokio::sync::Mutex;

// Overlap independent bucket waits without multiplying SQLite threads or
// bypassing the runtime's shared memory, disk, job and I/O admission.
const IN_FLIGHT: usize = 8;

pub(super) struct Startup<'a> {
    pub catalog: &'a CellCatalog,
    pub authority: &'a CellAuthority,
    pub layout: &'a CellStorageLayout,
    pub runtime: &'a CellRuntime,
    pub directory: Option<&'a NodeDirectory>,
    pub owner: &'a Owner,
    pub code: Digest,
    pub destination: &'a Path,
}

pub(super) struct Started {
    pub handles: Vec<CellHandle>,
    pub restored: usize,
    pub taken_over: usize,
}

impl Startup<'_> {
    pub async fn run(&self, targets: &[CellTarget]) -> super::ExampleResult<Started> {
        let proofs = Mutex::new(BTreeMap::new());
        let mut cells = bounded(targets.iter().enumerate(), |(shard, target)| {
            self.activate(shard, target, &proofs)
        })
        .await?;
        cells.sort_unstable_by_key(|(shard, _, _, _)| *shard);
        let restored = cells.iter().filter(|(_, _, restored, _)| *restored).count();
        let taken_over = cells.iter().filter(|(_, _, _, takeover)| *takeover).count();
        Ok(Started {
            handles: cells.into_iter().map(|(_, handle, _, _)| handle).collect(),
            restored,
            taken_over,
        })
    }

    async fn activate(
        &self,
        shard: usize,
        target: &CellTarget,
        proofs: &Mutex<BTreeMap<[u8; 16], NodeTakeoverProof>>,
    ) -> super::ExampleResult<(usize, CellHandle, bool, bool)> {
        let proof = self
            .catalog
            .provision(CatalogEntry::new(target, CatalogRole::Sql, self.code, 1)?)
            .await?;
        let observed = match self.authority.load(target.cell_id()).await? {
            Some(observed) => observed,
            None => {
                self.authority
                    .create_initial(
                        &proof,
                        IncarnationId::from_bytes([4; 16]),
                        self.owner.clone(),
                    )
                    .await?
            }
        };
        let replica = CellReplica::new(
            self.layout.clone(),
            *target.cell_id().as_bytes(),
            *observed.value().incarnation.as_bytes(),
            Limits::default(),
        )?;
        let destination = self.destination.join(format!("orders-{shard}.sqlite"));
        let restored = observed.value().root.is_some();
        let predecessor = observed
            .value()
            .owner
            .as_ref()
            .filter(|owner| owner.session != self.owner.session)
            .map(|owner| owner.session);
        let taken_over = predecessor.is_some();
        let handle = if let Some(predecessor) = predecessor {
            let takeover = self.takeover(predecessor, proofs).await?;
            if restored {
                self.runtime
                    .takeover_restored(
                        proof,
                        replica,
                        self.authority.clone(),
                        observed,
                        takeover,
                        RecoveryManifestStore::new(self.layout.clone(), Limits::default()),
                        destination,
                        self.owner.clone(),
                    )
                    .await?
            } else {
                self.runtime
                    .takeover_unpublished(
                        proof,
                        replica,
                        self.authority.clone(),
                        observed,
                        takeover,
                        destination,
                        self.owner.clone(),
                        initialize,
                    )
                    .await?
            }
        } else if observed.value().state == ControlState::Idle {
            self.runtime
                .acquire_idle_restored(
                    proof,
                    replica,
                    self.authority.clone(),
                    observed,
                    destination,
                    self.owner.clone(),
                )
                .await?
        } else {
            self.runtime
                .bootstrap(
                    proof,
                    replica,
                    self.authority.clone(),
                    observed,
                    destination,
                    initialize,
                )
                .await?
        };
        Ok((shard, handle, restored, taken_over))
    }

    async fn takeover(
        &self,
        predecessor: SessionId,
        proofs: &Mutex<BTreeMap<[u8; 16], NodeTakeoverProof>>,
    ) -> super::ExampleResult<NodeTakeoverProof> {
        let directory = self.directory.ok_or(Error::Node(
            "crash takeover requires real directory enrollment",
        ))?;
        // One permanent fence covers this failed boot's quiet Cells. Hold only
        // this startup cache lock; the independent node heartbeat must progress.
        let mut proofs = proofs.lock().await;
        if let Some(proof) = proofs.get(predecessor.as_bytes()) {
            return Ok(*proof);
        }
        let started = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(60), async {
            while directory
                .is_live(predecessor, super::fleet::clock()?)
                .await?
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok::<_, Error>(())
        })
        .await??;
        // Rechecks expiry and claimant liveness at a durable CAS. Active
        // follower logs refuse here and require the separate recovery coordinator.
        let proof = directory
            .claim_expired_for_takeover(predecessor, self.owner.session, super::fleet::clock()?)
            .await?;
        println!(
            "Node takeover: {}",
            serde_json::json!({"predecessor": uuid::Uuid::from_bytes(*predecessor.as_bytes()).to_string(), "wait_ms": started.elapsed().as_millis()})
        );
        proofs.insert(*predecessor.as_bytes(), proof);
        Ok(proof)
    }
}

fn initialize(transaction: &cellule_ltx::rusqlite::Transaction<'_>) -> cellule_runtime::Result<()> {
    transaction.execute_batch(super::SCHEMA)?;
    Ok(())
}

async fn bounded<I, F, T>(
    items: I,
    mut task: impl FnMut(I::Item) -> F,
) -> super::ExampleResult<Vec<T>>
where
    I: IntoIterator,
    F: Future<Output = super::ExampleResult<T>>,
{
    let mut items = items.into_iter();
    let mut pending = FuturesUnordered::new();
    for item in items.by_ref().take(IN_FLIGHT) {
        pending.push(task(item));
    }
    let mut results = Vec::new();
    let mut failure = None;
    while let Some(result) = pending.next().await {
        match result {
            Ok(result) => results.push(result),
            Err(error) if failure.is_none() => failure = Some(error),
            Err(_) => {}
        }
        if failure.is_none()
            && let Some(item) = items.next()
        {
            pending.push(task(item));
        }
    }
    // Never drop accepted activation futures at the first error. Their claims,
    // worker jobs and rollback must join before outer runtime shutdown begins.
    match failure {
        Some(error) => Err(error),
        None => Ok(results),
    }
}

#[cfg(test)]
mod tests;
