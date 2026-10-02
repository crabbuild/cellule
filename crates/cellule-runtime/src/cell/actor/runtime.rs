//! Cell-runtime construction, configuration, and node administration.
//!
//! Everything here configures one runtime or reports and reserves against
//! the node ledger it owns; the activation and takeover paths stay in the
//! actor root.

use super::*;

/// Converts one node ledger snapshot into the pressure observation the actor
/// classifies.
///
/// Memory is the resident and retained reservations against their own ceilings,
/// disk is the replica reservation against the replica budget, and jobs are the
/// same worker/primitive/hydration aggregate the placement block advertises, so
/// a node cannot look calm locally and pressed to the fleet.
pub(super) fn pressure_sample(
    snapshot: ResourceSnapshot,
    at_ms: i64,
) -> crate::Result<PressureSample> {
    let used = snapshot.used;
    let limit = snapshot.limit;
    let memory_used = used
        .resident_bytes()
        .checked_add(used.retained_bytes())
        .ok_or(Error::Capacity("pressure sample"))?;
    let memory_limit = limit
        .resident_bytes()
        .checked_add(limit.retained_bytes())
        .ok_or(Error::Capacity("pressure sample"))?;
    let memory_used = u64::try_from(memory_used).map_err(|_| Error::Capacity("pressure sample"))?;
    let memory_limit =
        u64::try_from(memory_limit).map_err(|_| Error::Capacity("pressure sample"))?;
    let jobs_used = used
        .worker_jobs()
        .checked_add(used.primitive_jobs())
        .and_then(|jobs| jobs.checked_add(used.hydration_jobs()))
        .ok_or(Error::Capacity("pressure sample"))?;
    let jobs_limit = limit
        .worker_jobs()
        .checked_add(limit.primitive_jobs())
        .and_then(|jobs| jobs.checked_add(limit.hydration_jobs()))
        .ok_or(Error::Capacity("pressure sample"))?;
    let jobs_used = u64::try_from(jobs_used).map_err(|_| Error::Capacity("pressure sample"))?;
    let jobs_limit = u64::try_from(jobs_limit).map_err(|_| Error::Capacity("pressure sample"))?;
    Ok(PressureSample {
        at_ms,
        memory_used_permille: permille(memory_used, memory_limit)?,
        disk_used_permille: permille(used.disk_bytes(), limit.disk_bytes())?,
        jobs_used_permille: permille(jobs_used, jobs_limit)?,
        // The ledger is read synchronously, so no sample can be late.
        stale: false,
    })
}

/// Scales `used / limit` to permille, saturating at full utilization.
fn permille(used: u64, limit: u64) -> crate::Result<u16> {
    if limit == 0 {
        return Err(Error::Capacity("pressure sample limit"));
    }
    let scaled = u128::from(used).saturating_mul(1_000) / u128::from(limit);
    Ok(u16::try_from(scaled.min(1_000)).unwrap_or(1_000))
}

impl CellRuntime {
    pub(crate) fn session(&self) -> SessionId {
        self.inner.session
    }

    /// Starts one dispatcher on the current Tokio runtime.
    pub fn new(
        pool: SqlWorkerPool,
        node_retained_bytes: usize,
        session: SessionId,
    ) -> crate::Result<Self> {
        Self::new_with_replica_host(
            pool,
            node_retained_bytes,
            session,
            cellule_ltx::Host::default(),
        )
    }

    /// Starts one dispatcher with caller-sized shared replica job admission.
    pub fn new_with_replica_host(
        pool: SqlWorkerPool,
        node_retained_bytes: usize,
        session: SessionId,
        replica_host: cellule_ltx::Host,
    ) -> crate::Result<Self> {
        Self::new_inner(
            pool,
            node_retained_bytes,
            session,
            replica_host,
            RuntimeNodeLease::ObjectOnly,
        )
    }

    /// Starts one dispatcher that remains fenced until its node lease is installed.
    pub fn new_with_replica_host_requiring_node_lease(
        pool: SqlWorkerPool,
        node_retained_bytes: usize,
        session: SessionId,
        replica_host: cellule_ltx::Host,
    ) -> crate::Result<Self> {
        Self::new_inner(
            pool,
            node_retained_bytes,
            session,
            replica_host,
            RuntimeNodeLease::Required(OnceLock::new()),
        )
    }

    /// Returns the budget shared by this runtime's local replica artifacts.
    ///
    /// Recovery admission must use this same ledger so streamed bundle bytes
    /// cannot bypass WAL, cache, or sparse-page reservations.
    #[must_use]
    pub fn local_disk_budget(&self) -> cellule_ltx::DiskBudget {
        self.inner.replica_host.local_disk_budget()
    }

    fn new_inner(
        pool: SqlWorkerPool,
        node_retained_bytes: usize,
        session: SessionId,
        replica_host: cellule_ltx::Host,
        node_lease: RuntimeNodeLease,
    ) -> crate::Result<Self> {
        if node_retained_bytes == 0 || node_retained_bytes > Semaphore::MAX_PERMITS {
            return Err(Error::Capacity("node retained bytes"));
        }
        pool.configure_retained_capacity(node_retained_bytes)?;
        let resources = pool.resource_ledger();
        let primitive_jobs = Arc::new(Semaphore::new(resources.snapshot()?.limit.primitive_jobs()));
        resources.set_disk_limit(replica_host.local_disk_capacity())?;
        resources.set_host_limits(
            replica_host.io_capacity(),
            replica_host.job_capacity(),
            replica_host.recovery_capacity(),
            replica_host.dirty_capacity(),
            replica_host.scratch_capacity() as usize,
        )?;
        let telemetry = crate::fleet::telemetry::CellTelemetryHandle::default();
        let mut replica_host = replica_host.with_ltx_telemetry(Arc::new(telemetry.clone()));
        replica_host.install_resource_admission(Arc::new(LedgerHostResourceAdmission::new(
            session, &resources,
        )));
        replica_host
            .install_disk_admission(Arc::new(LedgerDiskAdmission::new(session, &resources)))?;
        let runtime = tokio::runtime::Handle::try_current().map_err(Error::RuntimeStart)?;
        let (sender, receiver) = mpsc::channel(INGRESS_REQUESTS);
        let publications = broadcast::Sender::new(PUBLICATION_NOTIFICATIONS);
        let node_lease = Arc::new(node_lease);
        let unpublished_node_log_bytes = Arc::new(AtomicU64::new(0));
        let node_admission = NodeAdmission::default();
        runtime.spawn(run(
            receiver,
            pool.clone(),
            Arc::clone(&node_lease),
            Arc::clone(&unpublished_node_log_bytes),
            telemetry.clone(),
            publications.clone(),
            node_admission.clone(),
        ));
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                sender,
                publications,
                resources,
                primitive_jobs,
                shutting_down: AtomicBool::new(false),
                node_admission,
                session,
                pool,
                replica_host,
                receivers: std::sync::Mutex::new(receiver::ReceiverRegistry::default()),
                application_limits: OnceLock::new(),
                node_lease,
                node_durability: Arc::new(std::sync::RwLock::new(None)),
                telemetry,
                unpublished_node_log_bytes,
            }),
        })
    }

    /// Subscribes to bounded advisory hints after Cell activation or object publication.
    ///
    /// Hints grant no read or ownership capability. Receivers must reload authority
    /// and periodically reconcile missed hints, including broadcast lag. Command
    /// acknowledgement never waits for receivers; fleet-only proofs send no hint.
    #[must_use]
    pub fn subscribe_publications(&self) -> broadcast::Receiver<CatalogEntry> {
        self.inner.publications.subscribe()
    }

    /// Binds declared per-namespace LTX limits before a compiled application starts Cell work.
    ///
    /// Every later activation must use matching database and capture bounds.
    pub fn install_application_limits(
        &self,
        limits: impl IntoIterator<Item = (crate::identity::NamespaceId, u64, u64)>,
    ) -> crate::Result<()> {
        let mut by_namespace = HashMap::new();
        for (namespace, database, capture) in limits {
            if database < 512
                || capture < 128
                || by_namespace
                    .insert(namespace, (database, capture))
                    .is_some()
            {
                return Err(Error::Control("invalid application Cell limits"));
            }
        }
        if by_namespace.is_empty() {
            return Err(Error::Control("application has no Cell types"));
        }
        self.inner
            .application_limits
            .set(by_namespace)
            .map_err(|_| Error::Control("application Cell limits already installed"))
    }

    /// Installs the process telemetry sink before Cell work begins.
    pub fn install_telemetry(
        &self,
        telemetry: Arc<dyn crate::fleet::telemetry::CellTelemetry>,
    ) -> crate::Result<()> {
        self.inner.telemetry.install(telemetry)
    }

    /// Returns the shared sink used by node-log components for this runtime.
    #[must_use]
    pub fn telemetry_handle(&self) -> crate::fleet::telemetry::CellTelemetryHandle {
        self.inner.telemetry.clone()
    }

    /// Lists resident Cells whose published due time has passed.
    ///
    /// The actor answers from its own map, so a scheduler can tick due work it
    /// already owns without reading the Cell's catalog entry or control
    /// record. Callers must treat the list as a hint: the Tick itself fences
    /// against the publish sequence and re-derives what is due inside the
    /// Cell's transaction.
    pub async fn due_resident(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> crate::Result<Vec<super::DueResident>> {
        self.ensure_running()?;
        if limit == 0 || now_ms < 0 {
            return Ok(Vec::new());
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::DueResident {
                now_ms,
                limit,
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        let due = response.await.map_err(|_| Error::RuntimeClosed)?;
        Ok(due
            .into_iter()
            .map(|cell| {
                super::DueResident::new(
                    CellHandle {
                        cell: cell.cell,
                        incarnation: cell.incarnation,
                        code: cell.code,
                        schema: cell.schema,
                        catalog: cell.catalog,
                        inner: Arc::clone(&self.inner),
                        admission: cell.admission,
                    },
                    cell.expected_commit_sequence,
                    cell.next_due_ms,
                )
            })
            .collect())
    }

    /// Installs the successfully published process lease before Cell admission opens.
    pub fn install_node_lease(&self, guard: NodeLeaseGuard) -> crate::Result<()> {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Err(Error::RuntimeClosed);
        }
        self.inner.node_lease.install(guard)
    }

    /// Installs the one recruited node-log epoch used by newly activated Cells.
    pub fn install_node_durability(
        &self,
        application: ApplicationId,
        durability: Arc<NodeDurability>,
    ) -> crate::Result<()> {
        self.ensure_running()?;
        if durability.identity()?.0 != self.inner.session {
            return Err(Error::Control("Cell runtime node durability boot differs"));
        }
        let mut slot = self
            .inner
            .node_durability
            .write()
            .map_err(|_| Error::Control("Cell runtime node durability lock poisoned"))?;
        if slot.is_some() {
            return Err(Error::Control(
                "Cell runtime node durability was initialized twice",
            ));
        }
        *slot = Some((application, durability));
        Ok(())
    }

    /// Returns the currently installed node-log durability binding.
    #[must_use]
    pub fn node_durability(&self) -> Option<(ApplicationId, Arc<NodeDurability>)> {
        self.inner
            .node_durability
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Replaces the expected node-log binding after an epoch close. The identity
    /// check and replacement share one write lock; stale supervisors cannot
    /// overwrite a different binding installed while they awaited provider I/O.
    pub fn replace_node_durability(
        &self,
        application: ApplicationId,
        expected: &Arc<NodeDurability>,
        durability: Arc<NodeDurability>,
    ) -> crate::Result<Arc<NodeDurability>> {
        self.ensure_running()?;
        let mut slot = self
            .inner
            .node_durability
            .write()
            .map_err(|_| Error::Control("Cell runtime node durability lock poisoned"))?;
        let Some((installed_application, installed)) = slot.as_ref() else {
            return Err(Error::Control(
                "Cell runtime node durability is not installed",
            ));
        };
        if *installed_application != application {
            return Err(Error::Control(
                "Cell runtime node durability application changed",
            ));
        }
        if !Arc::ptr_eq(installed, expected) {
            return Err(Error::Control(
                "Cell runtime node durability binding changed",
            ));
        }
        let previous_identity = installed.identity()?;
        let identity = durability.identity()?;
        if identity.0 != previous_identity.0 || identity.1 != previous_identity.1 {
            return Err(Error::Control(
                "Cell runtime node durability identity changed",
            ));
        }
        if identity.2 <= previous_identity.2 {
            return Err(Error::Control(
                "Cell runtime node durability epoch did not advance",
            ));
        }
        let (_, previous) = slot
            .replace((application, durability))
            .ok_or(Error::Control("Cell runtime node durability disappeared"))?;
        Ok(previous)
    }

    /// Stops admission, drains accepted work, closes every Cell, and releases ownership.
    ///
    /// Returns the first unobserved background release failure, including one
    /// completed before shutdown was requested, after closing the remaining work.
    pub async fn shutdown(&self) -> crate::Result<()> {
        self.inner.node_admission.begin_drain()?;
        if self
            .inner
            .shutting_down
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::RuntimeClosed);
        }
        self.inner.primitive_jobs.close();
        // Cancel unused receiver credit and join accepted takeover before the
        // actor/worker close barrier. Retained caller handles are weak references.
        let receivers = self.drain_prepared_receivers().await;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::Shutdown { reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        let drain = response.await.map_err(|_| Error::RuntimeClosed)?;
        let workers = self.inner.pool.shutdown().await;
        let durability = match self.node_durability() {
            Some((_, durability)) => durability.shutdown().await,
            None => Ok(()),
        };
        // Admission and replica work are stopped. Optional fills outlive their
        // readers, so keep artifacts/executors until accepted fills complete.
        self.inner.replica_host.drain_cache_fills().await;
        receivers.and(drain).and(workers).and(durability)
    }

    /// Stops new Cell acquisition while existing owners continue serving.
    pub fn stop_acquiring(&self) -> crate::Result<()> {
        self.ensure_running()?;
        self.inner.node_admission.cordon()
    }

    /// Reports whether a new owner may be acquired on this node.
    #[must_use]
    pub fn is_acquiring(&self) -> bool {
        !self.inner.shutting_down.load(Ordering::Acquire)
            && self.inner.node_admission.check_new_role().is_ok()
    }

    /// Shares the node's lifecycle and pressure gate with local role facilities.
    /// The host installs this gate on its follower store before readiness.
    #[must_use]
    pub fn node_admission(&self) -> NodeAdmission {
        self.inner.node_admission.clone()
    }

    /// Returns the stable local classifier sample without refreshing its time.
    pub fn operational_sample(&self) -> crate::Result<Option<crate::node::NodeOperationalSample>> {
        self.inner.node_admission.sample()
    }

    /// Starts bounded, actor-owned eviction of safe idle Cells.
    ///
    /// The returned count is the number of drains started. Resource
    /// reservations are released only after the worker closes and ownership
    /// release completes; callers must not treat this as immediate capacity.
    pub async fn evict_idle(&self, limit: usize) -> crate::Result<usize> {
        self.ensure_running()?;
        if limit == 0 {
            return Ok(0);
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::EvictIdle { limit, reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Lists currently settled local Cells as advisory transfer candidates.
    /// The exact generation and eligibility are rechecked by `release_idle_cell`.
    pub async fn idle_transfer_candidates(
        &self,
    ) -> crate::Result<Vec<(CellId, u64, i64, CatalogRole)>> {
        self.ensure_running()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::IdleTransferCandidates { reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Lists active, non-draining catalog entries for bounded owner maintenance.
    pub async fn active_catalog_entries(
        &self,
    ) -> crate::Result<Vec<crate::cell::catalog::CatalogEntry>> {
        self.ensure_running()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::ActiveCatalogEntries { reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Observes a bounded page of all local owners and lifecycle transitions.
    ///
    /// The cursor pins ownership topology, not SQL state or authority. Changed
    /// topology rejects the cursor; restart the scan. Busy/draining Cells are
    /// retained, and each release still needs fresh generation/authority checks.
    /// The page owns retained-byte admission until dropped, including when its
    /// caller cancels before receiving the actor's response.
    pub async fn fleet_cells_page(
        &self,
        cursor: Option<CellInventoryCursor>,
        limit: usize,
    ) -> crate::Result<CellInventoryPage> {
        self.ensure_running()?;
        inventory::validate_limit(limit)?;
        let retained = self.inner.resources.try_reserve(
            ResourceCost::zero()
                .with_retained_bytes(crate::fleet::operations::MAX_PAGE_BYTES as usize),
        )?;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::FleetCellsPage {
                session: self.inner.session,
                cursor,
                limit,
                retained,
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Lists tenant-scoped targets of active, non-draining owners for maintenance.
    ///
    /// Targets come from verified activation proofs, including owners whose
    /// callers cancelled after activation. This advisory snapshot cannot authorize release.
    pub async fn active_cell_targets(&self) -> crate::Result<Vec<crate::identity::CellTarget>> {
        self.ensure_running()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::ActiveCellTargets { reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Counts live and transitioning Cells until their release has completed.
    pub async fn unreleased_cell_count(&self) -> crate::Result<usize> {
        self.ensure_running()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::UnreleasedCellCount { reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Releases one exact local generation only after a fresh settled-work
    /// preflight, actor gate, worker close, and authoritative release complete.
    pub async fn release_idle_cell(
        &self,
        cell: CellId,
        source: SessionId,
        generation: u64,
    ) -> crate::Result<()> {
        let (reply, response) = oneshot::channel();
        self.request_idle_release(cell, source, generation, None, DrainReply::Unit(reply))
            .await?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Stops new foreground work for one exact maintenance source.
    ///
    /// Already actor-admitted work and native exact-lease completion/validation
    /// continue through the normal transaction and durability gates. This
    /// transition is sticky for this activation and returns once installed;
    /// it does not prove primitive settlement, release, or fleet relocation.
    /// Applications authorize maintenance and retain its durable node intent
    /// before invoking this local boundary for a selected Cell.
    pub async fn quiesce_cell_at(
        &self,
        cell: CellId,
        source: SessionId,
        generation: u64,
        incarnation: crate::identity::IncarnationId,
        epoch: u64,
    ) -> crate::Result<()> {
        self.ensure_running()?;
        if source != self.inner.session || generation == 0 || epoch == 0 {
            return Err(Error::Fenced);
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::QuiesceCell {
                cell,
                generation,
                incarnation,
                epoch,
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Quiesces and releases a busy maintenance source through canonical publication.
    ///
    /// The deadline bounds preflight, not an already confirmed release. A refused
    /// preflight proves this request started no canonical release. Foreground
    /// closure remains sticky; native completion remains available after refusal.
    /// The runtime owns accepted work independently of the caller's waiter.
    pub async fn release_maintenance_cell_at(
        &self,
        cell: CellId,
        source: SessionId,
        generation: u64,
        incarnation: crate::identity::IncarnationId,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> crate::Result<MaintenanceCellRelease> {
        self.ensure_running()?;
        if source != self.inner.session || generation == 0 || epoch == 0 {
            return Err(Error::Fenced);
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::ReleaseMaintenanceCell(
                maintenance::ReleaseRequest {
                    cell,
                    generation,
                    incarnation,
                    epoch,
                    deadline: deadline.into_std(),
                    reply,
                },
            ))
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Releases an exact source identity and returns the canonical final position.
    ///
    /// The actor checks incarnation and ownership epoch before closing admission,
    /// then its existing worker-close/publication path captures the exact released
    /// root. A later authority read cannot replace this proof with a newer root.
    pub async fn release_idle_cell_at(
        &self,
        cell: CellId,
        source: SessionId,
        generation: u64,
        incarnation: crate::identity::IncarnationId,
        epoch: u64,
    ) -> crate::Result<crate::fleet::operations::PublishedPosition> {
        if epoch == 0 {
            return Err(Error::Fenced);
        }
        let (reply, response) = oneshot::channel();
        self.request_idle_release(
            cell,
            source,
            generation,
            Some((incarnation, epoch)),
            DrainReply::Position(reply),
        )
        .await?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    async fn request_idle_release(
        &self,
        cell: CellId,
        source: SessionId,
        generation: u64,
        expected: Option<(crate::identity::IncarnationId, u64)>,
        reply: DrainReply,
    ) -> crate::Result<()> {
        self.ensure_running()?;
        if source != self.inner.session || generation == 0 {
            return Err(Error::Fenced);
        }
        self.inner
            .sender
            .send(Message::ReleaseIdleCell {
                cell,
                generation,
                expected,
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)
    }

    /// Feeds one measured node sample into the actor-owned hysteretic pressure
    /// controller. Sustained shedding starts the same bounded idle-eviction
    /// path exposed by [`Self::evict_idle`].
    ///
    /// Samples are ordered by `at_ms`, and the actor samples its own ledger on
    /// the wall clock, so a caller must not observe an older node time than the
    /// node itself already has.
    pub async fn observe_pressure(&self, sample: PressureSample) -> crate::Result<PressureState> {
        self.ensure_running()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::ObservePressure { sample, reply })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    /// Reports whether node-wide admission has entered its terminal drain.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::Acquire)
    }

    /// Samples node-wide admission usage without waiting for actor work.
    #[must_use]
    pub fn stats(&self) -> CellRuntimeStats {
        let (
            retained,
            retained_capacity,
            resident,
            resident_capacity,
            file_descriptors,
            file_descriptor_capacity,
            worker_jobs,
            worker_job_capacity,
            primitive_jobs,
            primitive_job_capacity,
            hydration_jobs,
            hydration_job_capacity,
            io_slots,
            io_slot_capacity,
            blocking_jobs,
            blocking_job_capacity,
            recovery_jobs,
            recovery_job_capacity,
            dirty_jobs,
            dirty_job_capacity,
            scratch_units,
            scratch_unit_capacity,
            disk_bytes,
            disk_capacity_bytes,
        ) = self
            .inner
            .resources
            .snapshot()
            .map(|snapshot| {
                (
                    snapshot.used.retained_bytes(),
                    snapshot.limit.retained_bytes(),
                    snapshot.used.resident_bytes(),
                    snapshot.limit.resident_bytes(),
                    snapshot.used.file_descriptors(),
                    snapshot.limit.file_descriptors(),
                    snapshot.used.worker_jobs(),
                    snapshot.limit.worker_jobs(),
                    snapshot.used.primitive_jobs(),
                    snapshot.limit.primitive_jobs(),
                    snapshot.used.hydration_jobs(),
                    snapshot.limit.hydration_jobs(),
                    snapshot.used.io_slots(),
                    snapshot.limit.io_slots(),
                    snapshot.used.blocking_jobs(),
                    snapshot.limit.blocking_jobs(),
                    snapshot.used.recovery_jobs(),
                    snapshot.limit.recovery_jobs(),
                    snapshot.used.dirty_jobs(),
                    snapshot.limit.dirty_jobs(),
                    snapshot.used.scratch_units(),
                    snapshot.limit.scratch_units(),
                    snapshot.used.disk_bytes(),
                    snapshot.limit.disk_bytes(),
                )
            })
            .unwrap_or((
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ));
        CellRuntimeStats {
            active_cells: self.inner.pool.active_cells(),
            active_cell_capacity: self.inner.pool.active_cell_capacity(),
            resident_bytes: resident,
            resident_capacity_bytes: resident_capacity,
            file_descriptors,
            file_descriptor_capacity,
            retained_bytes: retained,
            retained_capacity_bytes: retained_capacity,
            worker_jobs,
            worker_job_capacity,
            primitive_jobs,
            primitive_job_capacity,
            hydration_jobs,
            hydration_job_capacity,
            io_slots,
            io_slot_capacity,
            blocking_jobs,
            blocking_job_capacity,
            recovery_jobs,
            recovery_job_capacity,
            dirty_jobs,
            dirty_job_capacity,
            scratch_units,
            scratch_unit_capacity,
            local_disk_reserved_bytes: disk_bytes,
            local_disk_capacity_bytes: disk_capacity_bytes,
            unpublished_node_log_bytes: self
                .inner
                .unpublished_node_log_bytes
                .load(Ordering::Acquire),
        }
    }

    /// Reserves node-wide bytes for native work retained outside a Cell mailbox.
    ///
    /// Returns a capacity error without waiting when the shared budget is full,
    /// and returns `RuntimeClosed` once terminal drain begins.
    pub fn try_reserve_node_bytes(&self, bytes: usize) -> crate::Result<NodeByteReservation> {
        self.ensure_running()?;
        if bytes == 0 {
            return Err(Error::Capacity("node retained bytes"));
        }
        let reservation = self
            .inner
            .resources
            .try_reserve(ResourceCost::zero().with_retained_bytes(bytes))
            .map_err(|error| match error {
                Error::Capacity(_) => Error::Capacity("node retained bytes"),
                error => error,
            })?;
        Ok(NodeByteReservation {
            _reservation: reservation,
        })
    }

    pub(crate) fn reserve_read_view(&self) -> crate::Result<ResourceReservation> {
        self.ensure_running()?;
        self.inner.resources.try_reserve(
            ResourceCost::zero()
                .with_resident_bytes(crate::fleet::resource::READ_REPLICA_NATIVE_BYTES)
                .with_file_descriptors(crate::fleet::resource::READ_REPLICA_FILE_DESCRIPTORS),
        )
    }

    pub(crate) fn replica_for_read(
        &self,
        replica: cellule_ltx::CellReplica,
    ) -> cellule_ltx::CellReplica {
        replica.with_host(self.inner.replica_host.clone())
    }

    pub(crate) async fn reserve_sql_job(
        &self,
    ) -> crate::Result<crate::cell::worker::WorkerJobReservation> {
        self.ensure_running()?;
        self.inner.pool.reserve_snapshot_job().await
    }

    /// Tries to reserve one worker-job slot from the same ledger as SQL work.
    ///
    /// A full ledger returns `Ok(None)` so schedulers can leave durable work
    /// unclaimed and retry on the next scan. Other failures preserve their
    /// original runtime error.
    pub fn try_reserve_worker_job(&self) -> crate::Result<Option<NodeJobReservation>> {
        self.ensure_running()?;
        match Arc::clone(&self.inner.primitive_jobs).try_acquire_owned() {
            Ok(permit) => self.worker_job_reservation(permit).map(Some),
            Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => Err(Error::RuntimeClosed),
        }
    }

    /// Waits for one primitive-job slot until the deadline or runtime shutdown.
    ///
    /// Callers must bound memory retained by waiters. Cancellation before
    /// admission reserves nothing; dispatched work must retain the returned
    /// reservation until it completes.
    pub async fn reserve_worker_job(
        &self,
        deadline: std::time::Instant,
    ) -> crate::Result<NodeJobReservation> {
        self.ensure_running()?;
        if std::time::Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let permit = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            Arc::clone(&self.inner.primitive_jobs).acquire_owned(),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|_| Error::RuntimeClosed)?;
        // Tokio polls a ready permit before its timer. Recheck after waking so
        // a released slot cannot revive work whose admission budget expired.
        if std::time::Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        self.worker_job_reservation(permit)
    }

    fn worker_job_reservation(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> crate::Result<NodeJobReservation> {
        self.ensure_running()?;
        Ok(NodeJobReservation {
            _reservation: self
                .inner
                .resources
                .try_reserve(ResourceCost::zero().with_primitive_jobs(1))?,
            _permit: permit,
        })
    }
}
