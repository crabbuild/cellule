use std::{path::PathBuf, sync::Arc, time::Duration};

use cellule_app::{ApplicationHandle, CellApplication, CompiledApplication};
use cellule_host::{CellNode, CellNodeBuilder, CellNodeTaskGroup};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, CatalogRole, CellClient, CellModule, CellTarget, Digest, Error as RuntimeError,
    SessionId, SqlWorkerPool, TenantId,
    cell::catalog::{CatalogEntry, CellCatalog},
    control::{ControlState, Owner, authority::CellAuthority},
    identity::{IncarnationId, NodeId},
    node::{
        NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain, lease::NodeLeaseGuard,
    },
    recovery::manifest::RecoveryManifestStore,
};
use cellule_store::{Store, probe_storage};
use ed25519_dalek::SigningKey;
use object_store::path::Path;
use rand::RngCore as _;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, now_ms};

const LEASE_MS: i64 = 30_000;

/// Explicit local resource and persistence configuration.
#[derive(Clone)]
pub struct NodeConfig {
    /// SQLite and cache files; authoritative objects remain in the provider.
    pub state_directory: PathBuf,
    /// Private provider prefix for this application installation.
    pub storage_prefix: Path,
    /// Stable application identity across restarts.
    pub application_id: ApplicationId,
}

/// Explicit factory for another leased node in the same installation.
/// The application bounds concurrent starts and owns every returned node's drain.
#[derive(Clone)]
pub struct SiblingNodeFactory {
    application: Arc<CompiledApplication>,
    store: Store,
    config: NodeConfig,
}
impl SiblingNodeFactory {
    /// Probes, enrolls and starts an independent boot with the original identities.
    pub async fn start(&self) -> Result<LocalNode> {
        LocalNode::start(
            self.application.clone(),
            self.store.clone(),
            self.config.clone(),
        )
        .await
    }
}

/// A leased single-node embedding with persistent storage and exact-root restore.
pub struct LocalNode {
    node: CellNode,
    tasks: Arc<CellNodeTaskGroup>,
    layout: cellule_ltx::CellStorageLayout,
    directory: NodeDirectory,
    lease: NodeLeaseGuard,
    state: PathBuf,
    application_id: ApplicationId,
    session: SessionId,
    acquisition: Arc<Mutex<()>>,
    sibling: SiblingNodeFactory,
}

impl LocalNode {
    /// Probes storage, enrolls and supervises a lease, then opens readiness.
    pub async fn start(
        application: Arc<CompiledApplication>,
        store: Store,
        config: NodeConfig,
    ) -> Result<Self> {
        let mut sibling = SiblingNodeFactory {
            application: application.clone(),
            store: store.clone(),
            config: config.clone(),
        };
        tokio::fs::create_dir_all(&config.state_directory).await?;
        let report = probe_storage(
            &store,
            &config.storage_prefix.clone().join("probe"),
            now_ms()?,
        )
        .await?;
        if !report.passed() {
            return Err(Error::Probe(report.failed_checks()));
        }
        let layout = cellule_ltx::CellStorageLayout::new(
            store,
            config.storage_prefix,
            *config.application_id.as_bytes(),
        );
        let session = SessionId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let registry = application.registry();
        let fleet = Digest::from_bytes(*blake3::hash(config.application_id.as_bytes()).as_bytes());
        let image = registry.release_digest();
        let directory = NodeDirectory::new(layout.clone(), fleet, image, image);
        let mut key = [0; 32];
        rand::rng().fill_bytes(&mut key);
        let signer = SigningKey::from_bytes(&key);
        let advertisement = move |now: i64, progress| {
            NodeAdvertisement::sign(
                NodeId::from_bytes(*session.as_bytes()),
                session,
                "https://cookbook.local".into(),
                fleet,
                Digest::from_bytes(*blake3::hash(signer.verifying_key().as_bytes()).as_bytes()),
                image,
                image,
                &signer,
                progress,
                now,
                now.checked_add(LEASE_MS)
                    .ok_or(RuntimeError::Node("lease expiry overflow"))?,
                registry.module_digests(),
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity {
                    free_memory_bytes: 64 << 20,
                    free_disk_bytes: 1 << 30,
                    job_credits: 32,
                    ..NodeCapacity::default()
                },
            )
        };
        // Validate/build resources before creating external enrollment. Any
        // later startup error must drain this runtime and retire its session.
        let node = CellNodeBuilder::new(application)
            .with_runtime(
                SqlWorkerPool::new(4, 32)?.with_native_memory_limit(64 << 20)?,
                64 << 20,
            )
            .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
            .with_session(session)
            .build()?;
        let enrollment: Result<_> = async {
            let now = now_ms()?;
            Ok(directory.create(advertisement(now, 1)?, now).await?)
        }
        .await;
        let observed = match enrollment {
            Ok(observed) => observed,
            Err(error) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(%cleanup, "unenrolled startup drain failed");
                }
                return Err(error);
            }
        };
        let lease_result: Result<_> = now_ms().and_then(|now| {
            Ok(NodeLeaseGuard::new(
                now,
                observed.advertisement().expires_at_ms(),
            )?)
        });
        let lease = match lease_result {
            Ok(lease) => lease,
            Err(error) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(%cleanup, "startup drain failed before lease installation");
                }
                if let Err(cleanup) = directory.withdraw(&observed, now_ms()?).await {
                    tracing::error!(%cleanup, "startup enrollment withdrawal failed");
                }
                return Err(error);
            }
        };
        let startup_observation = observed.clone();
        let shutdown = CancellationToken::new();
        let acquisition = Arc::new(Mutex::new(()));
        let setup = async {
            node.install_node_lease_for_startup(lease.clone())?;
            let tasks = node.install_task_group(CancellationToken::new(), shutdown.clone())?;
            let renewal_directory = directory.clone();
            let renewal_lease = lease.clone();
            tasks.spawn_lease_maintenance(async move {
                let mut observed = observed;
                let renewal: Result<()> = async {
                    loop {
                        tokio::select! {
                            () = shutdown.cancelled() => break,
                            () = tokio::time::sleep(Duration::from_secs(5)) => {}
                        }
                        let now = now_ms()?;
                        let progress = observed
                            .advertisement()
                            .progress()
                            .checked_add(1)
                            .ok_or(RuntimeError::Node("session progress overflow"))?;
                        let next = advertisement(now, progress)?;
                        // Finish an in-flight CAS before honoring shutdown; withdrawal
                        // must use the latest acknowledged advertisement generation.
                        observed = tokio::time::timeout(
                            renewal_lease.remaining(),
                            renewal_directory.refresh(&observed, next, now),
                        )
                        .await
                        .map_err(|_| RuntimeError::Deadline)??;
                        renewal_lease.renew(now_ms()?, observed.advertisement().expires_at_ms())?;
                    }
                    renewal_directory
                        .withdraw_after_drain(&observed, now_ms()?)
                        .await?;
                    Ok(())
                }
                .await;
                renewal_lease.fence();
                renewal
            })?;
            node.start()?;
            let cancellation = tasks.cancellation_token();
            tasks.spawn(crate::maintenance::run(
                node.runtime().clone(),
                CellClient::local_runtime(
                    node.application().registry(),
                    node.runtime(),
                    layout.clone(),
                ),
                node.application().registry(),
                cancellation,
                acquisition.clone(),
            ))?;
            Ok::<_, Error>(tasks)
        }
        .await;
        let tasks = match setup {
            Ok(tasks) => tasks,
            Err(error) => {
                let cleanup = node.shutdown().await;
                if let Err(cleanup) = cleanup {
                    tracing::error!(%cleanup, "startup drain failed");
                }
                lease.fence();
                if let Err(cleanup) = directory
                    .withdraw_after_drain(&startup_observation, now_ms()?)
                    .await
                {
                    tracing::error!(%cleanup, "startup enrollment withdrawal failed");
                }
                return Err(error);
            }
        };
        let state = config
            .state_directory
            .join(uuid::Uuid::from_bytes(*session.as_bytes()).to_string());
        // Callback receivers live under this boot, and must finish their owned
        // drain before the primary task group permits boot-directory cleanup.
        sibling.config.state_directory = state.join("receivers");
        Ok(Self {
            node,
            tasks,
            layout,
            directory,
            lease,
            state,
            application_id: config.application_id,
            session,
            acquisition,
            sibling,
        })
    }

    /// Returns same-installation construction data without sharing a live session.
    /// Returned receiver nodes require explicit shutdown before this node drains.
    pub fn sibling_factory(&self) -> SiblingNodeFactory {
        self.sibling.clone()
    }

    /// Returns serving readiness, including live task and lease health.
    pub fn is_ready(&self) -> bool {
        self.node.is_ready() && self.lease.check().is_ok()
    }

    /// Supervises application work and retains its task for cancellation and drain.
    pub fn spawn_worker<F, E>(&self, make_task: impl FnOnce(CancellationToken) -> F) -> Result<()>
    where
        F: std::future::Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        if !self.is_ready() {
            return Err(RuntimeError::CellDraining.into());
        }
        Ok(self
            .tasks
            .spawn(make_task(self.tasks.cancellation_token()))?)
    }

    /// Binds a typed client to an application-selected tenant.
    pub fn application_handle<A: CellApplication>(
        &self,
        tenant: TenantId,
    ) -> Result<ApplicationHandle<A>> {
        Ok(self.node.application_handle(
            CellClient::local_runtime(
                self.node.application().registry(),
                self.node.runtime(),
                self.layout.clone(),
            ),
            tenant,
            self.application_id,
        )?)
    }

    /// Provisions or restores a declared Cell, serializing concurrent local acquisition.
    pub async fn open_cell<M: CellModule>(
        &self,
        target: &CellTarget,
        module: &M,
    ) -> Result<cellule_runtime::cell::actor::CellHandle> {
        self.open_cell_inner(target, module, None).await
    }

    /// Explicitly publishes a retained, schema-one predecessor's code upgrade.
    ///
    /// The predecessor application must compile to the release that served this
    /// Cell. Drain its owner before upgrading; a crashed predecessor must first
    /// be recovered and drained under its own release. This path verifies rolling
    /// compatibility, restores the authority-pinned root, and uses the runtime's
    /// compiled migration. It never rewrites the original catalog entry. Retain
    /// all definitions and Activity handlers required by live predecessor runs.
    /// Repeating an acknowledged upgrade returns its current handle rather than
    /// migrating twice. On an uncertain error, drain and recover before retrying.
    pub async fn open_cell_after_rollout<M: CellModule>(
        &self,
        target: &CellTarget,
        module: &M,
        predecessor: &CompiledApplication,
    ) -> Result<cellule_runtime::cell::actor::CellHandle> {
        let application = self.node.application();
        if predecessor.name() != application.name() {
            return Err(RuntimeError::Registry("rollout application names differ").into());
        }
        if predecessor
            .cell_types()
            .iter()
            .any(|previous| !application.cell_types().contains(previous))
        {
            return Err(
                RuntimeError::Registry("code-only rollout changes predecessor topology").into(),
            );
        }
        let previous = predecessor.registry();
        application
            .registry()
            .verify_rolling_from(previous.release_bytes())?;
        self.open_cell_inner(target, module, Some(previous)).await
    }

    async fn open_cell_inner<M: CellModule>(
        &self,
        target: &CellTarget,
        module: &M,
        predecessor: Option<Arc<cellule_runtime::Registry>>,
    ) -> Result<cellule_runtime::cell::actor::CellHandle> {
        let _acquisition = self.acquisition.lock().await;
        if !self.is_ready() {
            return Err(RuntimeError::CellDraining.into());
        }
        let application = self.node.application();
        let cell_type = application
            .cell_types()
            .iter()
            .find(|entry| entry.namespace() == target.namespace())
            .ok_or(RuntimeError::Identity("undeclared namespace"))?;
        if target.application() != self.application_id {
            return Err(RuntimeError::Identity("foreign application target").into());
        }
        if cell_type.module() != M::NAME {
            return Err(RuntimeError::Identity("target module differs").into());
        }
        let registry = application.registry();
        let role = cell_type.role();
        if !matches!(
            role,
            CatalogRole::Sql
                | CatalogRole::Kv
                | CatalogRole::Blob
                | CatalogRole::Queue
                | CatalogRole::Cron
                | CatalogRole::Workflow
        ) {
            return Err(RuntimeError::Registry(
                "cookbook node does not yet install this primitive schema",
            )
            .into());
        }
        let code = registry
            .module_code(cell_type.module())
            .ok_or(RuntimeError::Registry("module is missing"))?;
        if registry.current_cell_version(target.namespace(), role) != Some((code, 1)) {
            return Err(RuntimeError::Registry(
                "cookbook activation supports schema version one only",
            )
            .into());
        }
        // Catalog entries pin bootstrap identity forever. After a migration,
        // current code belongs in authority, never in a replacement catalog.
        let catalog = CellCatalog::new(self.layout.clone(), target.tenant());
        let proof = match catalog.lookup(target.cell_id()).await? {
            Some(proof) => proof,
            None => {
                catalog
                    .provision(CatalogEntry::new(target, role, code, 1)?)
                    .await?
            }
        };
        if proof.entry().namespace() != target.namespace()
            || proof.entry().partition() != target.partition()
            || proof.entry().role() != role
        {
            return Err(RuntimeError::CatalogCollision.into());
        }
        let authority = CellAuthority::new(self.layout.clone());
        let runtime = self.node.runtime();
        let owner = Owner {
            session: self.session,
            endpoint: "https://cookbook.local".into(),
        };
        let observed = match authority.load(target.cell_id()).await? {
            Some(observed) => observed,
            None => {
                authority
                    .create_initial(
                        &proof,
                        IncarnationId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
                        owner.clone(),
                    )
                    .await?
            }
        };
        let migration = if observed.value().code == code && observed.value().schema == 1 {
            None
        } else {
            let previous = predecessor.as_ref().ok_or(RuntimeError::Registry(
                "stored application schema or code requires a migration",
            ))?;
            if observed.value().schema != 1
                || !previous.supports_cell(
                    target.namespace(),
                    role,
                    observed.value().code,
                    observed.value().schema,
                )
            {
                return Err(RuntimeError::Registry(
                    "stored code/schema is not served by the selected predecessor",
                )
                .into());
            }
            if observed.value().state != ControlState::Idle {
                return Err(RuntimeError::Registry(
                    "predecessor owner must recover and drain before rollout",
                )
                .into());
            }
            let plan = registry
                .next_migration(target.namespace(), observed.value().code, 1)?
                .ok_or(RuntimeError::Registry("rollout migration is missing"))?;
            if plan.to_schema() != 1 || plan.sql().is_some() {
                return Err(
                    RuntimeError::Registry("rollout requires a code-only migration").into(),
                );
            }
            Some(plan)
        };
        if let Some(handle) = runtime.local_handle(proof.clone(), &observed).await? {
            return Ok(handle);
        }
        let limits = Limits {
            max_database_bytes: cell_type.database_limit_bytes(),
            max_capture_bytes: cell_type.capture_limit_bytes(),
            ..Limits::default()
        };
        let replica = CellReplica::new(
            self.layout.clone(),
            *target.cell_id().as_bytes(),
            *observed.value().incarnation.as_bytes(),
            limits,
        )?;
        // Exact-root activation creates new files. A boot must never reuse a
        // previous session's SQLite or checksum sidecars as writable state.
        tokio::fs::create_dir_all(&self.state).await?;
        let destination = self.state.join(format!(
            "{}.sqlite",
            blake3::Hash::from_bytes(*target.cell_id().as_bytes()).to_hex()
        ));
        let migrations = module.descriptor().migrations;
        let initialize = move |transaction: &cellule_ltx::rusqlite::Transaction<'_>| {
            if role == CatalogRole::Kv {
                cellule_runtime::primitives::kv::install_kv_schema(transaction)?;
            }
            if role == CatalogRole::Cron {
                cellule_runtime::primitives::cron::install_cron_schema(transaction)?;
            }
            if role == CatalogRole::Workflow {
                cellule_runtime::primitives::workflow::install_workflow_schema(transaction)?;
            }
            if role == CatalogRole::Queue {
                cellule_runtime::primitives::queue::install_queue_schema(transaction)?;
            }
            if role == CatalogRole::Blob {
                cellule_runtime::primitives::blob::install_blob_schema(transaction)?;
            }
            for migration in migrations {
                transaction.execute_batch(migration.sql)?;
            }
            Ok(())
        };
        let handle = if observed
            .value()
            .owner
            .as_ref()
            .is_some_and(|value| value.session == owner.session)
        {
            runtime
                .bootstrap(proof, replica, authority, observed, destination, initialize)
                .await?
        } else if observed.value().state == ControlState::Idle {
            runtime
                .acquire_idle_restored(proof, replica, authority, observed, destination, owner)
                .await?
        } else {
            let previous = observed
                .value()
                .owner
                .as_ref()
                .ok_or(RuntimeError::Fenced)?
                .session;
            let takeover = self
                .directory
                .claim_expired_for_takeover(previous, owner.session, now_ms()?)
                .await?;
            if observed.value().root.is_none() {
                runtime
                    .takeover_unpublished(
                        proof,
                        replica,
                        authority,
                        observed,
                        takeover,
                        destination,
                        owner,
                        initialize,
                    )
                    .await?
            } else {
                runtime
                    .takeover_restored(
                        proof,
                        replica,
                        authority,
                        observed,
                        takeover,
                        RecoveryManifestStore::new(self.layout.clone(), limits),
                        destination,
                        owner,
                    )
                    .await?
            }
        };
        match migration {
            Some(plan) => Ok(handle.migrate(plan, now_ms()?).await?.handle),
            None => Ok(handle),
        }
    }

    /// Drains accepted commands while renewal remains live, then withdraws the session.
    pub async fn shutdown(&self) -> Result<()> {
        if let Err(error) = self
            .node
            .shutdown_until(std::time::Instant::now() + Duration::from_secs(20))
            .await
        {
            // Report task failures even when ingress already returned a readiness
            // error; keep the original owned error and its chain for the caller.
            tracing::error!(error=%error, "cookbook node drain failed");
            let mut cause = std::error::Error::source(&error);
            while let Some(source) = cause {
                tracing::error!(cause=%source, "cookbook node drain cause");
                cause = source.source();
            }
            return Err(error.into());
        }
        // Only our own session directory is disposable, and only after the
        // runtime has released every handle. Failed drains retain evidence.
        match tokio::fs::remove_dir_all(&self.state).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for LocalNode {
    fn drop(&mut self) {
        // Drop cannot await publication. Fence admission if an embedding caller
        // forgets the explicit drain; a successor must still prove takeover.
        self.lease.fence();
    }
}
