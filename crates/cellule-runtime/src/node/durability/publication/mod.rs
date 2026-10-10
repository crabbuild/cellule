//! One bounded producer, exact root checkpoint queue, and joined epoch drain.
use super::*;
use crate::node::bundle::BundleCoverageProof;
use crate::node::log_shipper::{AssignedCapture, NodePublicationFeed, SelectedBundle};
use std::sync::{Mutex as StdMutex, Weak};
use tokio::sync::{mpsc, oneshot, watch};

mod checkpoints;

const MAX_CAPTURES: usize = 64;
const MAX_FRAMES: usize = 64;
const MAX_NATIVE_BYTES: usize = crate::node::bundle::MAX_BUNDLE_BYTES as usize;
const MAX_CHECKPOINTS: usize = 512;
const ASSEMBLY: std::time::Duration = std::time::Duration::from_millis(1);

/// Exact original selected metadata retained after its materialized root CAS.
/// Construction is restricted to the canonical actor publisher.
pub struct BundleCheckpoint {
    pub(super) authority: crate::control::authority::CellAuthority,
    pub(super) root: cellule_ltx::RootRef,
    pub(super) selected: Arc<SelectedBundle>,
}

impl BundleCheckpoint {
    /// Original Cell authority, including its application and origin scope.
    pub fn authority(&self) -> &crate::control::authority::CellAuthority {
        &self.authority
    }
    /// Exact complete-capture endpoint already materialized by the actor.
    pub const fn root(&self) -> cellule_ltx::RootRef {
        self.root
    }
    /// Original dependency-verified selection proof for this exact endpoint.
    pub fn proof(&self) -> &BundleCoverageProof {
        &self.selected.proof
    }
}

/// Serialized node origin operations for the runtime-owned publication task.
/// Implement with the same original state/heartbeat mutex as binding and close.
pub trait NodeBundlePublicationAuthority: NodeBundleAuthority {
    /// Selects the complete ordered cohort with its native coverage in one CAS.
    /// Includes ready exact materialized checkpoints in that same catalog/CAS.
    /// Return original live proofs only after dependency verification and CAS;
    /// success also joins every supplied checkpoint obligation.
    fn select<'a>(
        &'a self,
        captures: &'a [AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>>;
    /// Checkpoints exact materialized roots together. An obsolete notification
    /// may be skipped only after observing a newer original materialized root;
    /// the newer actor notification remains an independent joined obligation.
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>>;
}

#[derive(Clone, Default)]
struct Progress {
    through: u64,
    terminal: Option<std::result::Result<(), Arc<Error>>>,
}

type PublicationResult = std::result::Result<(), Arc<Error>>;
type PublicationTask = tokio::task::JoinHandle<PublicationResult>;

pub(super) struct Publisher {
    checkpoints: StdMutex<Option<mpsc::Sender<CheckpointRequest>>>,
    progress: watch::Receiver<Progress>,
    worker: StdMutex<Option<PublicationTask>>,
    lease: NodeLeaseGuard,
}

struct CheckpointRequest {
    checkpoint: BundleCheckpoint,
    completed: oneshot::Sender<std::result::Result<(), Arc<Error>>>,
}

impl Publisher {
    pub(super) fn reserve_working(
        durability: &NodeDurability,
    ) -> Result<crate::fleet::resource::ResourceReservation> {
        let resources = durability
            .selection_resources
            .get()
            .ok_or(Error::PendingPublication)?;
        resources.try_reserve(
            crate::fleet::resource::ResourceCost::zero()
                // The fifth buffer covers the fresh cohort origin read. After
                // matching the proposal, its allocation is released for 2 MiB
                // of historical scratch and bounded operation-local facts.
                .with_retained_bytes((5 * crate::node::bundle::MAX_BUNDLE_BYTES) as usize),
        )
    }

    pub(super) fn start(
        durability: &Arc<NodeDurability>,
        authority: Arc<dyn NodeBundlePublicationAuthority>,
        feed: NodePublicationFeed,
        working: crate::fleet::resource::ResourceReservation,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        // Preparation is admitted before SQL can consume the remainder of the
        // ledger. Bounded catalog decode/encode copies cannot starve their own
        // selected-capture cleanup while all foreground memory is retained.
        let (sender, receiver) = mpsc::channel(MAX_CHECKPOINTS);
        let (progress, observed) = watch::channel(Progress::default());
        let lease = durability.node_lease.clone();
        let running_lease = lease.clone();
        let weak = Arc::downgrade(durability);
        let worker = runtime.spawn(async move {
            let _working = working;
            let result = run(weak, authority, feed, receiver, &progress, &running_lease)
                .await
                .map_err(|error| match error {
                    Error::Shared(source) => source,
                    error => Arc::new(error),
                });
            progress.send_modify(|state| state.terminal = Some(result.clone()));
            if result.is_err() {
                running_lease.fence();
            }
            result
        });
        Self {
            checkpoints: StdMutex::new(Some(sender)),
            progress: observed,
            worker: StdMutex::new(Some(worker)),
            lease,
        }
    }

    pub(super) async fn checkpoint(&self, checkpoint: BundleCheckpoint) -> Result<()> {
        self.lease.check()?;
        if checkpoint.root.commit_sequence != checkpoint.proof().commit_sequence()
            || checkpoint.root.position != checkpoint.proof().position()
        {
            return Err(Error::Node("bundle checkpoint endpoint differs"));
        }
        let sender = self
            .checkpoints
            .lock()
            .map_err(|_| Error::Node("bundle checkpoint lock poisoned"))?
            .clone()
            .ok_or(Error::RuntimeClosed)?;
        let (completed, completion) = oneshot::channel();
        tokio::select! {
            result = sender.send(CheckpointRequest { checkpoint, completed }) => result.map_err(|_| Error::RuntimeClosed)?,
            () = self.lease.wait_fenced() => return self.terminal_error(),
        }
        // Cell departure is allowed only after this original callback joins,
        // not after enqueue. Otherwise closing/detaching its pin could overtake
        // a still-running catalog checkpoint from the accepted root task.
        tokio::select! {
            result = completion => result.map_err(|_| Error::RuntimeClosed)?.map_err(Error::Shared),
            () = self.lease.wait_fenced() => self.terminal_error(),
        }
    }

    fn terminal_error(&self) -> Result<()> {
        match &self.progress.borrow().terminal {
            Some(Err(error)) => Err(Error::Shared(Arc::clone(error))),
            _ => Err(Error::Fenced),
        }
    }

    pub(super) async fn wait_through(&self, through: u64) -> Result<()> {
        let mut progress = self.progress.clone();
        loop {
            {
                let state = progress.borrow_and_update();
                if let Some(Err(error)) = &state.terminal {
                    return Err(Error::Shared(Arc::clone(error)));
                }
                if state.through >= through {
                    return self.lease.check();
                }
                if state.terminal.is_some() {
                    return Err(Error::Node("bundle producer ended before issued range"));
                }
            }
            tokio::select! {
                result = progress.changed() => result.map_err(|_| Error::RuntimeClosed)?,
                () = self.lease.wait_fenced() => return self.terminal_error(),
            }
        }
    }

    pub(super) async fn join(&self) -> Result<()> {
        self.checkpoints
            .lock()
            .map_err(|_| Error::Node("bundle checkpoint lock poisoned"))?
            .take();
        let worker = self
            .worker
            .lock()
            .map_err(|_| Error::Node("bundle producer lock poisoned"))?
            .take();
        if let Some(worker) = worker {
            return worker
                .await
                .map_err(Error::FollowerWorkerJoin)?
                .map_err(Error::Shared);
        }
        // Cancellation of a joining caller detaches the retained worker; it
        // does not cancel accepted I/O or manufacture a successful later join.
        let mut progress = self.progress.clone();
        loop {
            if let Some(result) = progress.borrow_and_update().terminal.clone() {
                return result.map_err(Error::Shared);
            }
            progress.changed().await.map_err(|_| Error::RuntimeClosed)?;
        }
    }
}

async fn run(
    durability: Weak<NodeDurability>,
    authority: Arc<dyn NodeBundlePublicationAuthority>,
    mut feed: NodePublicationFeed,
    mut checkpoints: mpsc::Receiver<CheckpointRequest>,
    progress: &watch::Sender<Progress>,
    lease: &NodeLeaseGuard,
) -> Result<()> {
    let mut carry = None;
    let mut checkpoints_open = true;
    loop {
        lease.check()?;
        // Retain one ready root notification so native work can share its
        // catalog/CAS. Idle publication still joins checkpoints immediately.
        let mut first_checkpoint = checkpoints.try_recv().ok();
        let capture = if carry.is_some() {
            carry.take()
        } else if let Some(capture) = feed.try_recv() {
            Some(capture)
        } else if let Some(first) = first_checkpoint.take() {
            tokio::select! {
                result = checkpoint_cohort(&authority, &mut checkpoints, first) => result?,
                () = lease.wait_fenced() => return Err(Error::Fenced),
            }
            continue;
        } else {
            tokio::select! {
                capture = feed.recv() => capture,
                checkpoint = checkpoints.recv(), if checkpoints_open => {
                    if let Some(first) = checkpoint {
                        tokio::select! {
                            result = checkpoint_cohort(&authority, &mut checkpoints, first) => result?,
                            () = lease.wait_fenced() => return Err(Error::Fenced),
                        }
                    }
                    else { checkpoints_open = false; }
                    continue;
                }
                () = lease.wait_fenced() => return Err(Error::Fenced),
            }
        };
        let Some(first) = capture else {
            if let Some(first) = first_checkpoint.take() {
                checkpoint_cohort(&authority, &mut checkpoints, first).await?;
            }
            break;
        };
        let mut captures = vec![first];
        let mut frames = captures[0].frames().len();
        let mut bytes = capture_bytes(&captures[0])?;
        if frames > MAX_FRAMES || bytes > MAX_NATIVE_BYTES {
            return Err(Error::Capacity(
                "complete capture exceeds native bundle bounds",
            ));
        }
        if frames == MAX_FRAMES
            && let Some(first) = first_checkpoint.take()
        {
            tokio::select! {
                result = checkpoint_cohort(&authority, &mut checkpoints, first) => result?,
                () = lease.wait_fenced() => return Err(Error::Fenced),
            }
        }
        let reserved_checkpoint = usize::from(first_checkpoint.is_some());
        let deadline = tokio::time::Instant::now() + ASSEMBLY;
        while captures.len() < MAX_CAPTURES {
            let next = tokio::select! {
                capture = feed.recv() => capture,
                _ = tokio::time::sleep_until(deadline) => break,
                () = lease.wait_fenced() => return Err(Error::Fenced),
            };
            let Some(next) = next else {
                break;
            };
            let next_bytes = capture_bytes(&next)?;
            if frames + next.frames().len() + reserved_checkpoint > MAX_FRAMES
                || bytes + next_bytes > MAX_NATIVE_BYTES
            {
                carry = Some(next);
                break;
            }
            frames += next.frames().len();
            bytes += next_bytes;
            captures.push(next);
        }
        let ready_checkpoints =
            checkpoints::Cohort::gather(&mut checkpoints, first_checkpoint, MAX_FRAMES - frames)?;
        let original = durability.upgrade().ok_or(Error::RuntimeClosed)?;
        let result = tokio::select! {
            proofs = authority.select(&captures, ready_checkpoints.values(), lease) => proofs,
            () = lease.wait_fenced() => Err(Error::Fenced),
        }
        .map_err(Arc::new);
        // Root tasks can release their original credit before receipt admission.
        // Completion follows the combined canonical CAS, never enqueue/upload.
        ready_checkpoints.complete(result.as_ref().map(|_| ()).map_err(Arc::clone));
        let proofs = result.map_err(Error::Shared)?;
        let cohort = receipts::SelectedCaptures::new(&original, &captures, proofs)?;
        let memory = {
            let admission = cohort.resources(&original)?.reserve(cohort.cost());
            tokio::pin!(admission);
            loop {
                tokio::select! {
                    // A ready original credit must win over an unrelated root
                    // callback. Only actual pressure requires standalone work;
                    // otherwise that callback can share the next native CAS.
                    biased;
                    () = lease.wait_fenced() => return Err(Error::Fenced),
                    result = &mut admission => break result?,
                    checkpoint = checkpoints.recv(), if checkpoints_open => {
                        if let Some(first) = checkpoint {
                            // Root tasks retain their admission until this callback
                            // joins. Servicing it while receipt credit is exhausted
                            // releases credit without repeating the durable CAS.
                            tokio::select! {
                                result = checkpoint_cohort(&authority, &mut checkpoints, first) => result?,
                                () = lease.wait_fenced() => return Err(Error::Fenced),
                            }
                        } else { checkpoints_open = false; }
                    }
                }
            }
        };
        let selected = cohort.confirm(&original, memory)?;
        progress.send_modify(|state| state.through = selected.selected_through());
        drop(selected);
        drop(captures);
        drop(original);
    }
    checkpoints.close();
    while let Some(first) = checkpoints.recv().await {
        tokio::select! {
            result = checkpoint_cohort(&authority, &mut checkpoints, first) => result?,
            () = lease.wait_fenced() => return Err(Error::Fenced),
        }
    }
    Ok(())
}

fn capture_bytes(capture: &AssignedCapture) -> Result<usize> {
    capture.frames().iter().try_fold(0_usize, |sum, frame| {
        sum.checked_add(frame.encoded().len())
            .ok_or(Error::Capacity("native bundle bytes"))
    })
}

async fn checkpoint_cohort(
    authority: &Arc<dyn NodeBundlePublicationAuthority>,
    receiver: &mut mpsc::Receiver<CheckpointRequest>,
    first: CheckpointRequest,
) -> Result<()> {
    let cohort = checkpoints::Cohort::gather(receiver, Some(first), MAX_CAPTURES)?;
    let result = authority
        .checkpoint(cohort.values())
        .await
        .map_err(Arc::new);
    cohort.complete(result.clone());
    result.map_err(Error::Shared)
}
