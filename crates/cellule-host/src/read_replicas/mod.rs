//! Node-owned read-only snapshots selected by current Cell policy.

use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_runtime::{
    Error, Result,
    cell::actor::CellRuntime,
    client::{CellReadReplica, ReadReplicaSource, Receipt},
    control::{ControlState, authority::CellAuthority},
    identity::{CellId, CellTarget, Digest, IncarnationId, SessionId},
    ltx::{CellReplica, CellStorageLayout, Limits},
    node::NodeDirectory,
    peer::{PeerReplicaControl, PeerReplicaResolver},
    read_policy::{ReadPolicy, ReadPolicyStore},
    registry::Registry,
};
use futures_util::future::BoxFuture;
use futures_util::{StreamExt, stream};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) mod enrollment;
mod inventory;
use enrollment::{ActivationRequest, ReaderEnrollment};
pub use enrollment::{
    ReaderEnrollmentCompletion, ReaderEnrollmentInventoryCursor, ReaderEnrollmentInventoryPage,
    ReaderEnrollmentJobs,
};
mod reconciliation;
mod recruitment;
pub use inventory::{ReaderInventoryCursor, ReaderInventoryPage};
pub use recruitment::ReadReplicaRecruiter;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const RECONCILE_BATCH: usize = 64;
const RECONCILE_DEADLINE: Duration = Duration::from_secs(30);
const MAX_LIVE_NODES: usize = 10_000;
const MAX_READ_VIEWS: usize = 10_000;
const CLOSE_CONCURRENCY: usize = 16;

struct ActiveReaders {
    views: HashMap<CellId, CellReadReplica>,
    topology: Uuid,
}

impl Default for ActiveReaders {
    fn default() -> Self {
        Self {
            views: HashMap::new(),
            topology: Uuid::now_v7(),
        }
    }
}

/// Admitted immutable readers sharing one node runtime and current placement policy.
///
/// Product adapters authorize activation hints and policy changes before calling
/// this manager; selection never grants write ownership.
#[derive(Clone)]
pub struct ReadReplicaManager {
    runtime: CellRuntime,
    registry: Arc<Registry>,
    layout: CellStorageLayout,
    authority: CellAuthority,
    directory: NodeDirectory,
    policy: ReadPolicyStore,
    session: SessionId,
    root: PathBuf,
    limits: Limits,
    closed: CancellationToken,
    activation: Arc<Mutex<()>>,
    active: Arc<RwLock<ActiveReaders>>,
    enrollment: Arc<std::sync::Mutex<Option<Arc<ReaderEnrollment>>>>,
    enrollment_required: Arc<std::sync::atomic::AtomicBool>,
    activation_started: Arc<std::sync::atomic::AtomicBool>,
}

impl ReadReplicaManager {
    /// Creates a reader manager for an existing node runtime.
    ///
    /// The caller must supervise `run` and invoke `shutdown` before runtime drain.
    /// Prefer `CellNode::install_read_replicas` for automatic lifecycle ownership.
    #[must_use]
    pub fn new(
        runtime: CellRuntime,
        registry: Arc<Registry>,
        layout: CellStorageLayout,
        directory: NodeDirectory,
        session: SessionId,
        root: PathBuf,
        limits: Limits,
    ) -> Self {
        let authority = CellAuthority::with_telemetry(layout.clone(), runtime.telemetry_handle());
        Self {
            runtime,
            registry,
            authority,
            policy: ReadPolicyStore::new(layout.clone()),
            layout,
            directory,
            session,
            root,
            limits,
            closed: CancellationToken::new(),
            activation: Arc::new(Mutex::new(())),
            active: Arc::new(RwLock::new(ActiveReaders::default())),
            enrollment: Arc::new(std::sync::Mutex::new(None)),
            enrollment_required: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            activation_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(crate) fn require_enrollment(&self) {
        self.enrollment_required
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn bind_enrollment(&self, binding: Arc<ReaderEnrollment>) -> Result<()> {
        let mut current = self
            .enrollment
            .lock()
            .map_err(|_| Error::Control("reader enrollment binding poisoned"))?;
        if self
            .activation_started
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Control(
                "reader enrollment must precede the first activation",
            ));
        }
        if current.is_some() {
            return Err(Error::Control("reader enrollment already installed"));
        }
        *current = Some(binding);
        Ok(())
    }

    fn bound_enrollment(&self) -> Result<Option<Arc<ReaderEnrollment>>> {
        Ok(self
            .enrollment
            .lock()
            .map_err(|_| Error::Control("reader enrollment binding poisoned"))?
            .clone())
    }

    fn activation_enrollment(&self) -> Result<Option<Arc<ReaderEnrollment>>> {
        let binding = self
            .enrollment
            .lock()
            .map_err(|_| Error::Control("reader enrollment binding poisoned"))?;
        // Serialize the first activation with binding. A pre-binding caller must
        // never enter a cancellable path after durable enrollment is installed.
        if binding.is_none()
            && self
                .enrollment_required
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Control("fleet reader enrollment is not installed"));
        }
        self.activation_started
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(binding.clone())
    }

    fn enrollment(&self) -> Result<Option<Arc<ReaderEnrollment>>> {
        let binding = self.bound_enrollment()?;
        if binding.is_none()
            && self
                .enrollment_required
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Control("fleet reader enrollment is not installed"));
        }
        Ok(binding)
    }

    /// Inspects retained original acceptance, evidence and errors for a local
    /// reader obligation. This is diagnostic state, not current serving proof.
    pub async fn enrollment_completion(
        &self,
        cell: CellId,
    ) -> Result<Option<ReaderEnrollmentCompletion>> {
        match self.bound_enrollment()? {
            Some(enrollment) => enrollment.completion(cell),
            None => Ok(None),
        }
    }

    /// Loads the current Cell incarnation and advisory desired-reader policy.
    pub async fn target(&self, target: &CellTarget) -> Result<(IncarnationId, Option<ReadPolicy>)> {
        let control = self
            .authority
            .load(target.cell_id())
            .await?
            .ok_or(Error::CellNotActive)?;
        if control.value().state == ControlState::Tombstoned {
            return Err(Error::CellNotActive);
        }
        let policy = self
            .policy
            .load(target.cell_id())
            .await?
            .map(|observed| observed.value());
        Ok((control.value().incarnation, policy))
    }

    /// Conditionally updates the reader target after caller authorization.
    pub async fn set_target(
        &self,
        target: &CellTarget,
        expected_revision: u64,
        desired_readers: u16,
    ) -> Result<Option<ReadPolicy>> {
        let control = self
            .authority
            .load(target.cell_id())
            .await?
            .ok_or(Error::CellNotActive)?;
        if control.value().state == ControlState::Tombstoned {
            return Err(Error::CellNotActive);
        }
        let incarnation = control.value().incarnation;
        let observed = self.policy.load(target.cell_id()).await?;
        let updated = match observed {
            None if expected_revision == 0 => {
                self.policy
                    .create(target.cell_id(), incarnation, desired_readers)
                    .await?
            }
            Some(observed) if observed.value().revision() == expected_revision => {
                if observed.value().incarnation() == incarnation {
                    if observed.value().desired_readers() == desired_readers {
                        observed
                    } else {
                        self.policy.update(&observed, desired_readers).await?
                    }
                } else {
                    self.policy
                        .replace_incarnation(&observed, incarnation, desired_readers)
                        .await?
                }
            }
            _ => return Ok(None),
        };
        let current = self
            .authority
            .load(target.cell_id())
            .await?
            .ok_or(Error::Fenced)?;
        if current.value().incarnation != incarnation
            || current.value().state == ControlState::Tombstoned
        {
            return Err(Error::Fenced);
        }
        Ok(Some(updated.value()))
    }

    /// Admits or refreshes a selected snapshot after an authenticated owner hint.
    pub async fn activate(&self, target: CellTarget, origin: SessionId) -> Result<Receipt> {
        if let Some(enrollment) = self.activation_enrollment()? {
            return enrollment
                .activate(self.clone(), ActivationRequest::Hint(target, origin))
                .await;
        }
        tokio::select! {
            () = self.closed.cancelled() => Err(Error::RuntimeClosed),
            result = self.activate_open(target, origin) => result,
        }
    }

    async fn activate_open(&self, target: CellTarget, origin: SessionId) -> Result<Receipt> {
        let _activation = self.activation.lock().await;
        self.ensure_open()?;
        let source = CellReadReplica::prepare_source(
            &self.registry,
            &self.authority,
            &self.directory,
            target,
        )
        .await?;
        if source.owner().session != origin {
            return Err(Error::Fenced);
        }
        self.activate_source_locked(source).await
    }

    /// Observes a selected exact source without opening a local reader.
    ///
    /// A fleet adapter can journal Pending using this value's root, epoch and
    /// owner before `activate_source`. Selection and admission are rechecked
    /// at activation; preparation itself grants no enrollment permit.
    pub async fn prepare_source(
        &self,
        target: CellTarget,
        origin: SessionId,
    ) -> Result<ReadReplicaSource> {
        self.ensure_open()?;
        self.runtime.node_admission().check_new_role()?;
        let source = CellReadReplica::prepare_source(
            &self.registry,
            &self.authority,
            &self.directory,
            target,
        )
        .await?;
        if source.owner().session != origin || !self.selected_source(&source).await? {
            return Err(Error::Fenced);
        }
        Ok(source)
    }

    /// Initially opens an adapter's journaled exact source through canonical activation.
    /// Newer publication cannot replace the supplied root. An installed fleet
    /// enrollment binding owns Pending, checked publication and joined retirement
    /// across waiter cancellation. Without a binding, the adapter must retain
    /// this future after acceptance. Once it enters the lane, manager closure
    /// joins opening rather than cancelling it.
    pub async fn activate_source(&self, source: ReadReplicaSource) -> Result<Receipt> {
        if let Some(enrollment) = self.activation_enrollment()? {
            return enrollment
                .activate(self.clone(), ActivationRequest::Source(Box::new(source)))
                .await;
        }
        self.activate_initial(source).await
    }

    async fn activate_initial(&self, source: ReadReplicaSource) -> Result<Receipt> {
        let _activation = tokio::select! {
            () = self.closed.cancelled() => return Err(Error::RuntimeClosed),
            activation = self.activation.lock() => activation,
        };
        self.ensure_open()?;
        if self
            .active
            .read()
            .await
            .views
            .contains_key(&source.description().cell)
        {
            return Err(Error::Control("read view is already installed"));
        }
        self.activate_source_locked(source).await
    }

    async fn activate_source_locked(&self, source: ReadReplicaSource) -> Result<Receipt> {
        self.ensure_open()?;
        if !self.selected_source(&source).await? {
            return Err(Error::Fenced);
        }
        let expected = source.description();
        let cell = expected.cell;
        let path = self.destination(cell).await?;
        let existing = { self.active.read().await.views.get(&cell).cloned() };
        if let Some(existing) = existing {
            if let Some(enrollment) = self.enrollment()? {
                enrollment.established(cell).await?;
            }
            match existing.refresh(&path).await {
                Ok(receipt) if self.still_selected(cell).await? => return Ok(receipt),
                Ok(_) => {
                    self.remove_locked(cell).await?;
                    return Err(Error::Fenced);
                }
                Err(Error::Fenced) => {
                    self.remove_locked(cell).await?;
                }
                Err(error) => return Err(error),
            }
        }
        self.runtime.node_admission().check_new_role()?;
        if let Some(enrollment) = self.enrollment()? {
            return enrollment.open(self, source, path).await;
        }
        self.open_source_locked(source, path).await
    }

    async fn open_source_locked(
        &self,
        source: ReadReplicaSource,
        path: PathBuf,
    ) -> Result<Receipt> {
        let expected = source.description();
        let cell = expected.cell;
        if self.active.read().await.views.len() >= MAX_READ_VIEWS {
            return Err(Error::Capacity("node read-view inventory bound"));
        }
        let replica = CellReplica::new(
            self.layout.clone(),
            *cell.as_bytes(),
            *expected.incarnation.as_bytes(),
            self.limits,
        )?;
        let reader = CellReadReplica::open_source(
            self.runtime.clone(),
            Arc::clone(&self.registry),
            self.authority.clone(),
            self.directory.clone(),
            replica,
            source,
            &path,
        )
        .await?;
        let receipt = reader.receipt().await;
        let selection = self.still_selected(cell).await;
        if self.closed.is_cancelled() || !matches!(selection, Ok(true)) {
            // Once native opening succeeded, even a provider failure must join
            // that view before publishing closure or returning its error.
            reader.close_and_join().await;
            return Err(if self.closed.is_cancelled() {
                Error::RuntimeClosed
            } else {
                selection.err().unwrap_or(Error::Fenced)
            });
        }
        let mut active = self.active.write().await;
        active.topology = Uuid::now_v7();
        active.views.insert(cell, reader);
        Ok(receipt)
    }

    /// Returns the selected snapshot receipt and live-owner readiness.
    pub async fn status(&self, target: CellTarget) -> Result<(Receipt, bool)> {
        if !self.still_selected(target.cell_id()).await? {
            return Err(Error::ReplicaUnavailable);
        }
        self.resolve(target).await?.readiness().await
    }

    async fn destination(&self, cell: CellId) -> Result<PathBuf> {
        let directory = self.root.join(format!("{cell:?}"));
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|source| Error::Facility {
                name: "Cell read replica directory",
                source: Box::new(source),
            })?;
        Ok(directory.join(format!("{}.sqlite", Uuid::now_v7())))
    }

    async fn selected_source(&self, source: &ReadReplicaSource) -> Result<bool> {
        let expected = source.description();
        self.selected(
            expected.cell,
            expected.incarnation,
            expected.code,
            source.owner().session,
        )
        .await
    }

    async fn selected(
        &self,
        cell: CellId,
        incarnation: IncarnationId,
        code: Digest,
        origin: SessionId,
    ) -> Result<bool> {
        let Some(policy) = self.policy.load(cell).await? else {
            return Ok(false);
        };
        let policy = policy.value();
        if policy.incarnation() != incarnation || policy.desired_readers() == 0 {
            return Ok(false);
        }
        let candidates = self
            .directory
            .select_readers(
                cell,
                origin,
                code,
                usize::from(policy.desired_readers()),
                now_ms()?,
                MAX_LIVE_NODES,
            )
            .await?;
        let enrollment = self.bound_enrollment()?;
        Ok(candidates.iter().any(|candidate| {
            candidate.session() == self.session
                && enrollment
                    .as_ref()
                    .is_none_or(|binding| binding.matches_boot(candidate))
        }))
    }

    async fn still_selected(&self, cell: CellId) -> Result<bool> {
        let Some(control) = self.authority.load(cell).await? else {
            return Ok(false);
        };
        let control = control.value();
        if control.state != ControlState::Serving || control.recovery.is_some() {
            return Ok(false);
        }
        let Some(owner) = control.owner.as_ref() else {
            return Ok(false);
        };
        self.selected(
            control.cell,
            control.incarnation,
            control.code,
            owner.session,
        )
        .await
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.is_cancelled() {
            return Err(Error::RuntimeClosed);
        }
        Ok(())
    }

    /// Closes and removes one read view before eviction or writable activation.
    pub async fn remove(&self, cell: CellId) -> Result<()> {
        let _activation = self.activation.lock().await;
        self.remove_locked(cell).await
    }

    async fn remove_locked(&self, cell: CellId) -> Result<()> {
        let reader = self.active.read().await.views.get(&cell).cloned();
        let receipt = match &reader {
            Some(reader) => Some(reader.close_and_join().await),
            None => None,
        };
        if let Some(enrollment) = self.bound_enrollment()? {
            enrollment.retire(cell, receipt).await?;
        }
        if reader.is_some() {
            // Retain a fenced view and its enrollment until durable retirement.
            // Cancellation or publication failure cannot erase this obligation.
            let mut active = self.active.write().await;
            active.views.remove(&cell);
            active.topology = Uuid::now_v7();
        }
        Ok(())
    }

    /// Permanently closes activation, joins owned enrollment jobs and retires views.
    /// A journal failure retains fenced inventory for a later shutdown attempt.
    pub async fn shutdown(&self) -> Result<()> {
        self.closed.cancel();
        let enrollment = self.bound_enrollment()?;
        let mut failure = None;
        if let Some(enrollment) = &enrollment {
            enrollment.close_admission()?;
            if let Err(error) = enrollment.join().await {
                failure = Some(error);
            }
        }
        let _activation = self.activation.lock().await;
        let views = {
            let mut active = self.active.write().await;
            active.topology = Uuid::now_v7();
            for reader in active.views.values() {
                reader.close();
            }
            active
                .views
                .iter()
                .map(|(cell, reader)| (*cell, reader.clone()))
                .collect::<Vec<_>>()
        };
        let closing = stream::iter(views)
            .map(|(cell, reader)| async move { (cell, reader.close_and_join().await) })
            .buffer_unordered(CLOSE_CONCURRENCY);
        tokio::pin!(closing);
        let mut joined = Vec::new();
        while let Some(item) = closing.next().await {
            joined.push(item);
        }
        // Preserve the complete ownership collection across cancellation while
        // any native sibling is still joining, as the ordinary manager does.
        for (cell, receipt) in joined {
            let retirement = match &enrollment {
                Some(enrollment) => enrollment.retire(cell, Some(receipt)).await,
                None => Ok(()),
            };
            match retirement {
                Ok(()) => {
                    self.active.write().await.views.remove(&cell);
                }
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        if let Some(enrollment) = &enrollment {
            for cell in enrollment.unresolved_cells()? {
                if let Err(error) = enrollment.retire(cell, None).await
                    && failure.is_none()
                {
                    failure = Some(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Command("system clock precedes Unix epoch"))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| Error::Command("system clock overflow"))
}

impl PeerReplicaResolver for ReadReplicaManager {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellReadReplica>> + Send + 'static>> {
        let manager = self.clone();
        Box::pin(async move {
            manager.ensure_open()?;
            manager
                .active
                .read()
                .await
                .views
                .get(&target.cell_id())
                .cloned()
                .ok_or(Error::ReplicaUnavailable)
        })
    }
}

impl PeerReplicaControl for ReadReplicaManager {
    fn activate(
        &self,
        target: CellTarget,
        origin: SessionId,
    ) -> BoxFuture<'static, Result<Receipt>> {
        let manager = self.clone();
        Box::pin(async move { manager.activate(target, origin).await })
    }

    fn status(&self, target: CellTarget) -> BoxFuture<'static, Result<(Receipt, bool)>> {
        let manager = self.clone();
        Box::pin(async move { manager.status(target).await })
    }
}
