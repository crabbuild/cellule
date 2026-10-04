//! Explicit snapshot read path, gated against current Cell authority.

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use cellule_ltx::{CellReplica, ReadOnlyRoot};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};

use super::*;
use crate::cell::actor::CellRuntime;
use crate::control::authority::CellAuthority;
use crate::control::{Control, ControlState, Owner, RootRef};
use crate::fleet::resource::ResourceReservation;
use crate::identity::SessionId;
use crate::node::NodeDirectory;

const QUERY_DEADLINE: Duration = Duration::from_secs(5);

mod observation;
pub use observation::ReadReplicaLifecycleObservation;

/// One immutable replica snapshot that serves explicit, position-tagged reads.
///
/// The caller owns routing and authorization. Every successful
/// query checks authoritative control and the owner's live session after SQL
/// execution; a stale epoch or unavailable authority releases no output.
#[derive(Clone)]
pub struct CellReadReplica {
    runtime: CellRuntime,
    registry: Arc<Registry>,
    authority: CellAuthority,
    directory: NodeDirectory,
    replica: CellReplica,
    target: CellTarget,
    expected: CellDescription,
    snapshot: Arc<RwLock<ReplicaState>>,
    lifetime: Arc<ReplicaLifetime>,
    refresh_gate: Arc<Mutex<()>>,
    query_gate: Arc<Semaphore>,
}

struct ReplicaState {
    receipt: Receipt,
    snapshot: Option<Arc<ReplicaSnapshot>>,
}

/// Opaque exact reader source observed through canonical Cell authority.
///
/// Preparing this value creates no local reader or resource reservation. An
/// application can journal its exact responsibility before opening the view.
/// It does not grant admission or prove current serving; opening checks the
/// owner again and verifies every dependency of this pinned root.
#[derive(Clone)]
pub struct ReadReplicaSource {
    target: CellTarget,
    description: CellDescription,
    owner: Owner,
    epoch: u64,
    root: RootRef,
    node: crate::identity::NodeId,
    fleet: Digest,
}

impl ReadReplicaSource {
    /// Returns the original Cell target.
    #[must_use]
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Returns the catalog scope, code and schema of the observed source.
    #[must_use]
    pub const fn description(&self) -> CellDescription {
        self.description
    }
    /// Returns the original owner boot and endpoint.
    #[must_use]
    pub fn owner(&self) -> &Owner {
        &self.owner
    }
    /// Returns the original authority epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Returns the exact authority-pinned immutable root to open.
    #[must_use]
    pub fn root(&self) -> &RootRef {
        &self.root
    }
    /// Returns the physical source node from its verified boot advertisement.
    #[must_use]
    pub const fn node(&self) -> crate::identity::NodeId {
        self.node
    }
    /// Returns the source boot's signed fleet scope.
    #[must_use]
    pub const fn fleet(&self) -> Digest {
        self.fleet
    }
}

struct ReplicaSnapshot {
    owner: Owner,
    epoch: u64,
    node: crate::identity::NodeId,
    fleet: Digest,
    view: Arc<ReadOnlyRoot>,
    _admission: Arc<ResourceReservation>,
    // Fields drop in declaration order: the last root's resource charges must
    // be released before its lifetime wakes a close waiter.
    _lifetime: LifetimeGuard,
}

struct SnapshotInputs {
    owner: Owner,
    epoch: u64,
    node: crate::identity::NodeId,
    fleet: Digest,
    admission: Arc<ResourceReservation>,
    operation: Arc<LifetimeGuard>,
}

struct ReaderOpening {
    source: ReadReplicaSource,
    admission: Arc<ResourceReservation>,
}

const CLOSED: usize = 1 << (usize::BITS - 1);

#[derive(Default)]
struct ReplicaLifetime {
    // Closure and admission share one CAS word: a close waiter cannot observe
    // zero and then miss a newly accepted operation. Snapshots are retained by
    // an already accepted open/refresh; their jobs own the same operation guard.
    state: AtomicUsize,
    changed: Notify,
}

struct LifetimeGuard(Arc<ReplicaLifetime>);

impl ReplicaLifetime {
    fn acquire(self: &Arc<Self>, new_operation: bool) -> Result<LifetimeGuard> {
        self.state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                let count = state & !CLOSED;
                if (state & CLOSED != 0 && (new_operation || count == 0)) || count == CLOSED - 1 {
                    None
                } else {
                    Some(state + 1)
                }
            })
            .map_err(|state| {
                if state & CLOSED != 0 && (new_operation || state & !CLOSED == 0) {
                    Error::Fenced
                } else {
                    Error::Capacity("read replica lifetime count")
                }
            })?;
        Ok(LifetimeGuard(Arc::clone(self)))
    }

    async fn join(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.load(Ordering::Acquire) & !CLOSED == 0 {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for LifetimeGuard {
    fn drop(&mut self) {
        self.0.state.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

impl CellReadReplica {
    pub(crate) fn session(&self) -> SessionId {
        self.runtime.session()
    }

    pub(crate) fn description(&self) -> CellDescription {
        self.expected
    }

    /// Opens the exact S3 root currently named by one live serving owner.
    ///
    /// The caller supplies a fresh private destination and an admitting node
    /// runtime. Source Cell control must stay serving under the same owner
    /// epoch through installation.
    pub async fn open(
        runtime: CellRuntime,
        registry: Arc<Registry>,
        authority: CellAuthority,
        directory: NodeDirectory,
        replica: CellReplica,
        target: CellTarget,
        destination: &Path,
    ) -> Result<Self> {
        runtime.node_admission().check_new_role()?;
        // Ordinary opening retains its existing admission-before-provider-I/O
        // order. Explicit source preparation alone creates no local obligation.
        let admission = Arc::new(runtime.reserve_read_view()?);
        let source = Self::prepare_source(&registry, &authority, &directory, target).await?;
        Self::open_admitted(
            runtime,
            registry,
            authority,
            directory,
            replica,
            ReaderOpening { source, admission },
            destination,
        )
        .await
    }

    /// Observes an exact source without opening a view or reserving resources.
    /// Journal enrollment before calling `open_source`; a later publication
    /// cannot silently replace the root named by this value.
    pub async fn prepare_source(
        registry: &Registry,
        authority: &CellAuthority,
        directory: &NodeDirectory,
        target: CellTarget,
    ) -> Result<ReadReplicaSource> {
        let cell = target.cell_id();
        let observed = authority.load(cell).await?.ok_or(Error::CellNotActive)?;
        let control = observed.value();
        let owner = control.owner.as_ref().ok_or(Error::Fenced)?.clone();
        if control.state != ControlState::Serving || control.recovery.is_some() {
            return Err(Error::Fenced);
        }
        let (module, _) = registry
            .namespace_contract(target.namespace())
            .ok_or(Error::Registry("replica namespace is not registered"))?;
        if !registry.supports_module_code(module, control.code, control.schema) {
            return Err(Error::Registry("replica module or code is unsupported"));
        }
        let boot = directory
            .load_if_live(owner.session, unix_time_ms()?)
            .await?
            .ok_or(Error::Fenced)?;
        Ok(ReadReplicaSource {
            target,
            description: CellDescription {
                cell,
                incarnation: control.incarnation,
                code: control.code,
                schema: control.schema,
            },
            owner,
            epoch: control.epoch,
            root: control.root.clone().ok_or(Error::Fenced)?,
            node: boot.advertisement().node(),
            fleet: boot.advertisement().fleet(),
        })
    }

    /// Opens a previously checked exact source through the ordinary read path.
    ///
    /// The same owner/epoch must still be serving at installation. Newer roots
    /// under that owner are allowed, but this view opens the original pinned
    /// root. Cordon and closed runtime admission still reject a new view.
    pub async fn open_source(
        runtime: CellRuntime,
        registry: Arc<Registry>,
        authority: CellAuthority,
        directory: NodeDirectory,
        replica: CellReplica,
        source: ReadReplicaSource,
        destination: &Path,
    ) -> Result<Self> {
        runtime.node_admission().check_new_role()?;
        let admission = Arc::new(runtime.reserve_read_view()?);
        Self::open_admitted(
            runtime,
            registry,
            authority,
            directory,
            replica,
            ReaderOpening { source, admission },
            destination,
        )
        .await
    }

    async fn open_admitted(
        runtime: CellRuntime,
        registry: Arc<Registry>,
        authority: CellAuthority,
        directory: NodeDirectory,
        replica: CellReplica,
        opening: ReaderOpening,
        destination: &Path,
    ) -> Result<Self> {
        runtime.node_admission().check_new_role()?;
        let ReaderOpening { source, admission } = opening;
        let ReadReplicaSource {
            target,
            description: expected,
            owner,
            epoch,
            root,
            node,
            fleet,
        } = source;
        let (module, _) = registry
            .namespace_contract(target.namespace())
            .ok_or(Error::Registry("replica namespace is not registered"))?;
        if !registry.supports_module_code(module, expected.code, expected.schema) {
            return Err(Error::Registry("replica module or code is unsupported"));
        }
        let lifetime = Arc::new(ReplicaLifetime::default());
        let operation = Arc::new(lifetime.acquire(true)?);
        let replica = runtime.replica_for_read(replica);
        let verified = replica
            .open_root(&root.to_ltx(expected.cell, expected.incarnation))
            .await?;
        if verified.schema() != expected.schema {
            return Err(Error::Fenced);
        }
        let snapshot = open_view(
            &runtime,
            verified,
            destination,
            SnapshotInputs {
                owner,
                epoch,
                node,
                fleet,
                admission,
                operation,
            },
        )
        .await?;
        let position = receipt(expected, snapshot.view.root().commit_sequence);
        let opened = Self {
            runtime,
            registry,
            authority,
            directory,
            replica,
            target,
            expected,
            snapshot: Arc::new(RwLock::new(ReplicaState {
                receipt: position,
                snapshot: Some(snapshot.clone()),
            })),
            lifetime,
            refresh_gate: Arc::new(Mutex::new(())),
            query_gate: Arc::new(Semaphore::new(1)),
        };
        opened.confirm_authority(&snapshot).await?;
        Ok(opened)
    }

    /// Returns the last installed exact snapshot position, including after close.
    #[must_use]
    pub async fn receipt(&self) -> Receipt {
        self.snapshot.read().await.receipt
    }

    /// Returns the verified position and whether the original owner is still live.
    ///
    /// A false readiness bit is advisory warm state only; it never permits a
    /// query or takeover. Changed authority or closed admission rejects it.
    pub async fn readiness(&self) -> Result<(Receipt, bool)> {
        self.runtime.ensure_running()?;
        let _operation = self.lifetime.acquire(true)?;
        let snapshot = self.current_snapshot().await?;
        self.confirm_snapshot(&snapshot).await?;
        let boot = self
            .directory
            .load_if_live(snapshot.owner.session, unix_time_ms()?)
            .await?;
        if boot.as_ref().is_some_and(|boot| {
            boot.advertisement().node() != snapshot.node
                || boot.advertisement().fleet() != snapshot.fleet
        }) {
            return Err(Error::Fenced);
        }
        let live = boot.is_some();
        if self.query_gate.is_closed() {
            return Err(Error::Fenced);
        }
        Ok((self.snapshot_receipt(&snapshot), live))
    }

    /// Closes reader admission across every clone before eviction or writable activation.
    pub fn close(&self) {
        self.lifetime.state.fetch_or(CLOSED, Ordering::AcqRel);
        self.query_gate.close();
    }

    /// Closes admission, detaches snapshots from every retained peer clone,
    /// and joins accepted queries, native SQL and refresh work. The returned
    /// receipt is the last installed position, not current authority/readiness.
    /// A cancelled waiter leaves closure installed; another waiter can join the
    /// same retained work. Provider failures still belong to the original work.
    pub async fn close_and_join(&self) -> Receipt {
        self.close();
        let receipt = {
            let _refresh = self.refresh_gate.lock().await;
            let mut state = self.snapshot.write().await;
            state.snapshot.take();
            state.receipt
        };
        self.lifetime.join().await;
        receipt
    }

    async fn current_snapshot(&self) -> Result<Arc<ReplicaSnapshot>> {
        self.snapshot
            .read()
            .await
            .snapshot
            .clone()
            .ok_or(Error::Fenced)
    }

    /// Installs a newer exact root without disrupting queries using the old view.
    ///
    /// The destination must be fresh and private. Concurrent refreshes are
    /// serialized; a failed or stale refresh leaves the serving view intact.
    pub async fn refresh(&self, destination: &Path) -> Result<Receipt> {
        self.runtime.ensure_running()?;
        let operation = Arc::new(self.lifetime.acquire(true)?);
        let _refresh = self.refresh_gate.lock().await;
        let current = self.current_snapshot().await?;
        self.confirm_authority(&current).await?;
        let observed = self
            .authority
            .load(self.expected.cell)
            .await?
            .ok_or(Error::Fenced)?;
        let control = observed.value();
        if !self.same_owner_and_code(control, &current) {
            return Err(Error::Fenced);
        }
        let root = control.ltx_root().ok_or(Error::Fenced)?;
        if root.commit_sequence < current.view.root().commit_sequence {
            return Err(Error::Fenced);
        }
        if root == current.view.root() {
            return Ok(self.snapshot_receipt(&current));
        }
        let admission = Arc::new(self.runtime.reserve_read_view()?);
        let verified = self.replica.open_root(&root).await?;
        if verified.schema() != self.expected.schema {
            return Err(Error::Fenced);
        }
        let replacement = open_view(
            &self.runtime,
            verified,
            destination,
            SnapshotInputs {
                owner: current.owner.clone(),
                epoch: current.epoch,
                node: current.node,
                fleet: current.fleet,
                admission,
                operation: Arc::clone(&operation),
            },
        )
        .await?;
        self.confirm_authority(&replacement).await?;
        let receipt = self.snapshot_receipt(&replacement);
        let mut state = self.snapshot.write().await;
        // Close may have raced the final provider read. A closed reader cannot
        // install a replacement behind the detachment barrier.
        if self.query_gate.is_closed() {
            return Err(Error::Fenced);
        }
        state.receipt = receipt;
        state.snapshot = Some(replacement);
        Ok(receipt)
    }

    /// Executes one compiled typed query against this read-only snapshot.
    ///
    /// A minimum newer than this view fails rather than returning an older
    /// value. Authority is checked after SQL before any result is released.
    pub async fn query<Q: Query>(
        &self,
        minimum: Option<Receipt>,
        input: Q::Input,
    ) -> Result<Observed<Q::Output>> {
        let operation = self.registry.query_contract::<Q>(self.target.namespace())?;
        validate_description(&self.registry, Q::MODULE, self.expected, operation)?;
        let input = encode_wire(&input, operation.input_limit)?;
        let observed = self
            .query_encoded(EncodedQuery {
                target: self.target.clone(),
                expected: self.expected,
                minimum,
                now_ms: unix_time_ms()?,
                module: Q::MODULE,
                operation_id: Q::ID,
                codec_version: Q::CODEC_VERSION,
                input,
                input_limit: operation.input_limit,
                output_limit: operation.output_limit,
            })
            .await?;
        Ok(Observed {
            output: decode_wire(&observed.output, operation.output_limit)?,
            receipt: observed.receipt,
        })
    }

    pub(crate) async fn query_encoded(&self, query: EncodedQuery) -> Result<EncodedObservation> {
        self.runtime.ensure_running()?;
        let accepted_work = Arc::new(self.lifetime.acquire(true)?);
        if self.query_gate.is_closed() {
            return Err(Error::Fenced);
        }
        if query.target != self.target || query.expected != self.expected {
            return Err(Error::Fenced);
        }
        validate_minimum(self.expected, query.minimum)?;
        let (module, operation) = self.registry.routed_query_contract(
            self.target.namespace(),
            query.operation_id,
            query.codec_version,
        )?;
        if module != query.module
            || operation.input_limit != query.input_limit
            || operation.output_limit != query.output_limit
        {
            return Err(Error::Registry("replica query contract changed"));
        }
        validate_description(&self.registry, module, self.expected, operation)?;
        let snapshot = self.current_snapshot().await?;
        let observed = self.snapshot_receipt(&snapshot);
        if let Some(minimum) = query
            .minimum
            .filter(|minimum| observed.commit_sequence < minimum.commit_sequence)
        {
            return Err(Error::ReplicaBehind {
                observed_sequence: observed.commit_sequence,
                minimum_sequence: minimum.commit_sequence,
            });
        }
        let input = query.input;
        let deadline = Instant::now() + QUERY_DEADLINE;
        let permit = tokio::time::timeout_at(
            deadline.into(),
            Arc::clone(&self.query_gate).acquire_owned(),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|_| Error::Fenced)?;
        let job = tokio::time::timeout_at(deadline.into(), self.runtime.reserve_sql_job())
            .await
            .map_err(|_| Error::Deadline)??;
        let interrupt = snapshot.view.connection()?.get_interrupt_handle();
        let active_snapshot = snapshot.clone();
        let registry = Arc::clone(&self.registry);
        let cell = self.expected.cell;
        let schema = self.expected.schema;
        let sequence = observed.commit_sequence;
        let now_ms = unix_time_ms()?;
        let native_operation = Arc::clone(&accepted_work);
        let mut task = tokio::task::spawn_blocking(move || {
            // Declare this first so cancellation cannot wake close before SQL
            // view, job and semaphore charges have all been released.
            let _operation = native_operation;
            let _permit = permit;
            let _job = job;
            // Caller cancellation can drop the reader while SQL is running.
            // Keep its view and admission together, releasing them before the
            // job charge that node drain waits on.
            let snapshot = active_snapshot;
            let view = &snapshot.view;
            let connection = view.connection()?;
            view.take_io_error();
            cellule_ltx::with_paged_io_deadline(deadline, || {
                let (observed_sequence, now_ms) = local::current_metadata(&connection, now_ms)?;
                if observed_sequence != sequence {
                    return Err(Error::Fenced);
                }
                registry.execute_query(
                    &connection,
                    QueryInvocation {
                        module,
                        operation_id: query.operation_id,
                        codec_version: query.codec_version,
                        schema,
                        cell,
                        commit_sequence: sequence,
                        now_ms,
                        input: &input,
                    },
                )
            })
            .map_err(|error| match view.take_io_error() {
                Some(cellule_ltx::LtxError::Deadline) => Error::Deadline,
                Some(source) => Error::from(source),
                None => error,
            })
        });
        let result = match tokio::time::timeout_at(deadline.into(), &mut task).await {
            Ok(result) => result.map_err(Error::WorkerJoin)?,
            Err(_) => {
                interrupt.interrupt();
                let _ = task.await;
                return Err(Error::Deadline);
            }
        }?;
        tokio::time::timeout_at(deadline.into(), self.confirm_authority(&snapshot))
            .await
            .map_err(|_| Error::Deadline)??;
        self.runtime.ensure_running()?;
        Ok(EncodedObservation {
            output: result,
            receipt: observed,
        })
    }

    fn snapshot_receipt(&self, snapshot: &ReplicaSnapshot) -> Receipt {
        receipt(self.expected, snapshot.view.root().commit_sequence)
    }

    async fn confirm_snapshot(&self, snapshot: &ReplicaSnapshot) -> Result<()> {
        if self.query_gate.is_closed() {
            return Err(Error::Fenced);
        }
        let current = self
            .authority
            .load(self.expected.cell)
            .await?
            .ok_or(Error::Fenced)?;
        if !self.same_owner_and_code(current.value(), snapshot) {
            return Err(Error::Fenced);
        }
        Ok(())
    }

    /// Confirms the exact snapshot against live authority and the signed lease.
    ///
    /// Both observations are required before a result is released. They are
    /// intentionally read on every release rather than cached: this gate is
    /// what stops a replica fenced by a takeover, a drained query gate, or a
    /// retired owner session from answering. The two reads are independent, so
    /// they run together and cost one provider round trip instead of two.
    async fn confirm_authority(&self, snapshot: &ReplicaSnapshot) -> Result<()> {
        if self.query_gate.is_closed() {
            return Err(Error::Fenced);
        }
        let now_ms = unix_time_ms()?;
        let (current, live) = tokio::try_join!(
            self.authority.load(self.expected.cell),
            self.directory.load_if_live(snapshot.owner.session, now_ms)
        )?;
        let current = current.ok_or(Error::Fenced)?;
        // Either provider read may stall beyond the observed lease. Admission
        // and expiry must still hold when the result is actually released.
        let release_ms = unix_time_ms()?;
        let live = live.is_some_and(|node| {
            node.advertisement().expires_at_ms() > release_ms
                && node.advertisement().node() == snapshot.node
                && node.advertisement().fleet() == snapshot.fleet
        });
        if self.query_gate.is_closed()
            || !self.same_owner_and_code(current.value(), snapshot)
            || !live
        {
            return Err(Error::Fenced);
        }
        Ok(())
    }

    fn same_owner_and_code(&self, current: &Control, snapshot: &ReplicaSnapshot) -> bool {
        current.state == ControlState::Serving
            && current.recovery.is_none()
            && current.epoch == snapshot.epoch
            && current.incarnation == self.expected.incarnation
            && current.code == self.expected.code
            && current.schema == self.expected.schema
            && current.owner.as_ref() == Some(&snapshot.owner)
            && current
                .root
                .as_ref()
                .is_some_and(|root| root.commit_sequence >= snapshot.view.root().commit_sequence)
    }
}

async fn open_view(
    runtime: &CellRuntime,
    verified: cellule_ltx::VerifiedRoot,
    destination: &Path,
    inputs: SnapshotInputs,
) -> Result<Arc<ReplicaSnapshot>> {
    let job = runtime.reserve_sql_job().await?;
    let destination = destination.to_owned();
    // VFS faults need LTX's blocking pool for directory-cache I/O. SQLite must
    // use separate SQL admission, retaining both charges if its waiter cancels.
    tokio::task::spawn_blocking(move || -> Result<Arc<ReplicaSnapshot>> {
        let operation = inputs.operation;
        let _job = job;
        let admission = inputs.admission;
        let owner = inputs.owner;
        let checked_root = verified;
        let destination_path = destination;
        cellule_ltx::with_paged_io_deadline(Instant::now() + QUERY_DEADLINE, || {
            let view = Arc::new(checked_root.open_read_only(&destination_path)?);
            Ok(Arc::new(ReplicaSnapshot {
                owner,
                epoch: inputs.epoch,
                node: inputs.node,
                fleet: inputs.fleet,
                view,
                _admission: admission,
                _lifetime: operation.0.acquire(false)?,
            }))
        })
    })
    .await
    .map_err(Error::WorkerJoin)?
}
