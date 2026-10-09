//! Resolving, acquiring, activating, and taking over a local Cell.
//!
//! Every entry point here turns a catalog proof, a restored root, or a
//! takeover proof into a running local handle, and refuses when the
//! authority, capacity ledger, or node lease does not agree.

use super::acquire_resume::TakeoverActivation;
use super::*;

impl CellRuntime {
    /// Checks actor-owned admission before a peer route reads Cell metadata.
    ///
    /// A miss only means this process has no currently dispatchable handle;
    /// the peer transport remains responsible for resolving remote authority.
    pub(crate) async fn has_local_owner(&self, cell: CellId) -> crate::Result<bool> {
        self.ensure_running()?;
        if self.inner.pool.active_cells() == 0 {
            // Activation reserves a Cell before enqueueing it and holds that
            // charge through worker teardown. Zero therefore proves a local
            // miss without waking the dispatcher. A concurrent activation can
            // change the destination hint; the peer still verifies ownership.
            self.ensure_running()?;
            if self.inner.sender.is_closed() {
                return Err(Error::RuntimeClosed);
            }
            return Ok(false);
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::Lookup {
                cell,
                require_resident: false,
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        let local = response.await.map_err(|_| Error::RuntimeClosed)?;
        self.ensure_running()?;
        Ok(local.is_some())
    }

    /// Resolves an active local owner without exposing the dispatcher's Cell map.
    pub async fn local_handle(
        &self,
        catalog: CatalogProof,
        control: &VersionedControl,
    ) -> crate::Result<Option<CellHandle>> {
        self.ensure_running()?;
        let value = control.value();
        if catalog.entry().cell() != value.cell {
            return Err(Error::Control("scheduler catalog and control differ"));
        }
        if value
            .owner
            .as_ref()
            .is_none_or(|owner| owner.session != self.inner.session)
            || value.root.is_none()
        {
            return Ok(None);
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .sender
            .send(Message::Lookup {
                cell: value.cell,
                require_resident: false,
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        let Some(local) = response.await.map_err(|_| Error::RuntimeClosed)? else {
            return Ok(None);
        };
        if local.incarnation != value.incarnation
            || local.code != value.code
            || local.schema != value.schema
        {
            return Ok(None);
        }
        Ok(Some(CellHandle {
            cell: value.cell,
            incarnation: value.incarnation,
            code: value.code,
            schema: value.schema,
            catalog,
            inner: self.inner.clone(),
            admission: local.admission,
        }))
    }

    /// Creates, initializes and publishes a new Cell before returning a handle.
    pub async fn bootstrap<F>(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        destination: PathBuf,
        initialize: F,
    ) -> crate::Result<CellHandle>
    where
        F: for<'connection> FnOnce(
                &cellule_ltx::rusqlite::Transaction<'connection>,
            ) -> crate::Result<()>
            + Send
            + 'static,
    {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        self.activation_cell(&catalog, &observed)?;
        if observed.value().state != crate::control::ControlState::Recovering
            || observed.value().root.is_some()
        {
            return Err(Error::Control("bootstrap requires an unpublished control"));
        }
        let replica = self
            .replica_with_directory_cache(replica, &destination)
            .await?;
        let reservation = self.inner.pool.reserve_activation()?;
        let incarnation = observed.value().incarnation;
        let schema = observed.value().schema;
        self.activate_inner(
            catalog,
            Activation::Bootstrap(Box::new(BootstrapActivation {
                replica: replica.clone(),
                destination,
                incarnation,
                schema,
                initialize: Box::new(initialize),
                reservation,
            })),
            replica,
            authority,
            observed,
        )
        .await
    }

    /// Takes over an unchanged unpublished owner, initializes and publishes the Cell.
    #[expect(
        clippy::too_many_arguments,
        reason = "the takeover boundary keeps every authority and activation input explicit"
    )]
    pub async fn takeover_unpublished<F>(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        mut observed: VersionedControl,
        takeover: crate::node::NodeTakeoverProof,
        destination: PathBuf,
        owner: Owner,
        initialize: F,
    ) -> crate::Result<CellHandle>
    where
        F: for<'connection> FnOnce(
                &cellule_ltx::rusqlite::Transaction<'connection>,
            ) -> crate::Result<()>
            + Send
            + 'static,
    {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        let cell = self.claiming_cell(&catalog, &observed, &owner)?;
        if owner.session != takeover.claimant() {
            return Err(Error::Fenced);
        }
        let replica = self
            .replica_with_directory_cache(replica, &destination)
            .await?;
        loop {
            if observed.value().state != crate::control::ControlState::Recovering
                || observed.value().owner.is_none()
                || observed.value().root.is_some()
            {
                return Err(Error::Control(
                    "unpublished takeover requires an active rootless control",
                ));
            }
            if observed.value().owner.as_ref().map(|owner| owner.session)
                != Some(takeover.session())
            {
                return Err(Error::Fenced);
            }
            self.ensure_running()?;
            let current = authority.load(cell).await?.ok_or(Error::Fenced)?;
            if current.value() != observed.value() {
                self.claiming_cell(&catalog, &current, &owner)?;
                observed = current;
                continue;
            }
            let reservation = self.inner.pool.reserve_activation()?;
            let successor = current.value().takeover(owner.clone())?;
            let claimed = match authority
                .transition(&current, successor.clone(), Transition::Takeover)
                .await
            {
                Ok(claimed) => claimed,
                Err(error) => {
                    let latest = authority.load(cell).await?.ok_or(Error::Fenced)?;
                    if latest.value() == &successor {
                        latest
                    } else if matches!(
                        &error,
                        Error::Storage(cellule_store::StorageError::StateConflict { .. })
                    ) {
                        observed = latest;
                        continue;
                    } else {
                        return Err(error);
                    }
                }
            };
            // Keep the actual successful input before initialization can admit
            // an actor. Rootless takeover has no acknowledged database prefix.
            authority
                .retain_acquisition(current.value(), claimed.value())
                .await?;
            let incarnation = claimed.value().incarnation;
            let schema = claimed.value().schema;
            return self
                .activate_inner(
                    catalog,
                    Activation::Bootstrap(Box::new(BootstrapActivation {
                        replica: replica.clone(),
                        destination,
                        incarnation,
                        schema,
                        initialize: Box::new(initialize),
                        reservation,
                    })),
                    replica,
                    authority,
                    claimed,
                )
                .await;
        }
    }

    /// Cold-opens the exact authoritative root on the Cell's SQL worker.
    pub async fn activate_restored(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        recovery_store: crate::recovery::manifest::RecoveryManifestStore,
        destination: PathBuf,
    ) -> crate::Result<CellHandle> {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        self.check_application_limits(&catalog, recovery_store.limits())?;
        self.activation_cell(&catalog, &observed)?;
        let replica = self
            .replica_with_directory_cache(replica, &destination)
            .await?;
        let observed = self
            .publish_attached_recovery(&replica, &authority, observed, &recovery_store)
            .await?;
        let reservation = self.inner.pool.reserve_activation()?;
        self.activate_restored_reserved(
            catalog,
            replica,
            authority,
            observed,
            destination,
            reservation,
            None,
        )
        .await
    }

    /// Acquires an idle published Cell and restores its exact immutable root.
    pub async fn acquire_idle_restored(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        destination: PathBuf,
        owner: Owner,
    ) -> crate::Result<CellHandle> {
        self.acquire_idle_restored_observed(
            catalog,
            replica,
            authority,
            observed,
            destination,
            owner,
            None,
        )
        .await
    }

    /// Acquires an Idle root with confirmed durable recording before CAS and
    /// before actor admission. Uses the same canonical acquisition and rollback.
    pub async fn acquire_idle_restored_observed(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        destination: PathBuf,
        owner: Owner,
        observer: Option<Arc<dyn AcquisitionObserver>>,
    ) -> crate::Result<CellHandle> {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        self.claiming_cell(&catalog, &observed, &owner)?;
        if observed.value().state != crate::control::ControlState::Idle
            || observed.value().owner.is_some()
            || observed.value().root.is_none()
        {
            return Err(Error::Control(
                "idle acquisition requires a published idle control",
            ));
        }
        let replica = self
            .replica_with_directory_cache(replica, &destination)
            .await?;
        let reservation = self.inner.pool.reserve_activation()?;
        self.acquire_idle_reserved(
            catalog,
            replica,
            authority,
            observed,
            destination,
            owner,
            reservation,
            None,
            observer,
        )
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "canonical idle acquisition consumes its explicit prepaid resources"
    )]
    pub(super) async fn acquire_idle_reserved(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        destination: PathBuf,
        owner: Owner,
        reservation: CellReservation,
        job: Option<crate::cell::worker::WorkerJobReservation>,
        observer: Option<Arc<dyn AcquisitionObserver>>,
    ) -> crate::Result<CellHandle> {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        let rollback_node_lease = self.inner.node_lease.guard()?;
        self.claiming_cell(&catalog, &observed, &owner)?;
        if observed.value().state != crate::control::ControlState::Idle
            || observed.value().owner.is_some()
            || observed.value().root.is_none()
        {
            return Err(Error::Control(
                "idle acquisition requires a published idle control",
            ));
        }
        let successor = observed.value().takeover(owner)?;
        if let Some(observer) = &observer {
            observer.before_claim(observed.value()).await?;
        }
        self.ensure_acquiring()?;
        let ownership_started = std::time::Instant::now();
        let claimed = match authority
            .transition(&observed, successor.clone(), Transition::Takeover)
            .await
        {
            Ok(claimed) => claimed,
            Err(error) => {
                let current = authority
                    .load(observed.value().cell)
                    .await?
                    .ok_or(Error::Fenced)?;
                if current.value() != &successor {
                    return Err(error);
                }
                current
            }
        };
        self.inner.telemetry.activation_phase(
            crate::fleet::telemetry::ActivationPhase::Ownership,
            ownership_started.elapsed(),
        );
        let rollback_authority = authority.clone();
        let rollback_claim = claimed.clone();
        let rollback_replica = replica.clone();
        let activation = async {
            authority
                .retain_acquisition(observed.value(), claimed.value())
                .await?;
            if let Some(observer) = &observer {
                observer
                    .before_activation(observed.value(), claimed.value())
                    .await?;
            }
            self.activate_restored_reserved(
                catalog,
                replica,
                authority,
                claimed,
                destination,
                reservation,
                job,
            )
            .await
        }
        .await;
        match activation {
            Ok(handle) => Ok(handle),
            Err(error) => {
                match rollback_failed_acquisition(
                    &rollback_authority,
                    &rollback_claim,
                    &rollback_replica,
                    rollback_node_lease,
                )
                .await
                {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(cleanup),
                }
            }
        }
    }

    /// Takes over an unchanged owner after its exact node session is fenced.
    #[expect(
        clippy::too_many_arguments,
        reason = "the takeover boundary keeps every authority, recovery and activation input explicit"
    )]
    pub async fn takeover_restored(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        takeover: crate::node::NodeTakeoverProof,
        recovery_store: crate::recovery::manifest::RecoveryManifestStore,
        destination: PathBuf,
        owner: Owner,
    ) -> crate::Result<CellHandle> {
        self.takeover_restored_observed(
            catalog,
            replica,
            authority,
            observed,
            takeover,
            recovery_store,
            destination,
            owner,
            None,
        )
        .await
    }

    /// Takes over a fenced predecessor with durable input and recovery-position
    /// recording. Callback failure cannot admit an actor; post-CAS failure uses
    /// the ordinary rollback path. A lost waiter must remain caller-owned.
    #[expect(
        clippy::too_many_arguments,
        reason = "the recorder supplements explicit recovery and authority inputs"
    )]
    pub async fn takeover_restored_observed(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        mut observed: VersionedControl,
        takeover: crate::node::NodeTakeoverProof,
        recovery_store: crate::recovery::manifest::RecoveryManifestStore,
        destination: PathBuf,
        owner: Owner,
        observer: Option<Arc<dyn AcquisitionObserver>>,
    ) -> crate::Result<CellHandle> {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        self.check_application_limits(&catalog, recovery_store.limits())?;
        let replica = self
            .replica_with_directory_cache(replica, &destination)
            .await?;
        let rollback_node_lease = self.inner.node_lease.guard()?;
        let cell = self.claiming_cell(&catalog, &observed, &owner)?;
        if owner.session != takeover.claimant() {
            return Err(Error::Fenced);
        }
        loop {
            if !matches!(
                observed.value().state,
                crate::control::ControlState::Recovering | crate::control::ControlState::Serving
            ) || observed.value().owner.is_none()
                || observed.value().root.is_none()
            {
                return Err(Error::Control(
                    "takeover requires a published control with an active owner",
                ));
            }
            if observed.value().owner.as_ref().map(|owner| owner.session)
                != Some(takeover.session())
            {
                return Err(Error::Fenced);
            }
            self.ensure_running()?;
            let current = authority.load(cell).await?.ok_or(Error::Fenced)?;
            if current.value() != observed.value() {
                self.claiming_cell(&catalog, &current, &owner)?;
                observed = current;
                continue;
            }
            let reservation = self.inner.pool.reserve_activation()?;
            let successor = current.value().takeover(owner.clone())?;
            if let Some(observer) = &observer {
                observer.before_claim(current.value()).await?;
            }
            self.ensure_acquiring()?;
            let claimed = match authority
                .transition(&current, successor.clone(), Transition::Takeover)
                .await
            {
                Ok(claimed) => claimed,
                Err(error) => {
                    let latest = authority.load(cell).await?.ok_or(Error::Fenced)?;
                    if latest.value() == &successor {
                        latest
                    } else if matches!(
                        &error,
                        Error::Storage(cellule_store::StorageError::StateConflict { .. })
                    ) {
                        observed = latest;
                        continue;
                    } else {
                        return Err(error);
                    }
                }
            };
            return self
                .finish_takeover_restored(TakeoverActivation {
                    catalog,
                    replica,
                    authority,
                    input: current.value().clone(),
                    claimed,
                    recovery_store,
                    destination,
                    reservation,
                    node_lease: rollback_node_lease,
                    observer,
                })
                .await;
        }
    }

    pub(super) async fn publish_attached_recovery(
        &self,
        replica: &cellule_ltx::CellReplica,
        authority: &CellAuthority,
        observed: VersionedControl,
        recovery_store: &crate::recovery::manifest::RecoveryManifestStore,
    ) -> crate::Result<VersionedControl> {
        let Some(recovery) = observed.value().recovery.as_ref() else {
            return Ok(observed);
        };
        let overlay = recovery_store
            .load_overlay(
                observed.value().cell,
                observed.value().incarnation,
                recovery,
            )
            .await?;
        let prepared = replica
            .prepare_recovered_overlay(&overlay, observed.value().schema)
            .await?;
        self.ensure_running()?;
        let successor = observed
            .value()
            .publish_recovery(&prepared, observed.value().next_due_ms)?;
        authority.retain_root_lineage(&prepared).await?;
        match authority
            .transition(&observed, successor.clone(), Transition::PublishRecovery)
            .await
        {
            Ok(published) => Ok(published),
            Err(error) => {
                let current = authority
                    .load(observed.value().cell)
                    .await?
                    .ok_or(Error::Fenced)?;
                if current.value() == &successor {
                    Ok(current)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(super) async fn activate_restored_reserved(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
        destination: PathBuf,
        reservation: CellReservation,
        job: Option<crate::cell::worker::WorkerJobReservation>,
    ) -> crate::Result<CellHandle> {
        self.ensure_acquiring()?;
        // Acquisition installed one cache owner before recovery. Keep that
        // replica through root verification, SQLite and publisher activation.
        let cell = self.activation_cell(&catalog, &observed)?;
        let control = observed.value();
        let root = control
            .ltx_root()
            .ok_or(Error::Control("activation requires a published root"))?;
        let incarnation = control.incarnation;
        let schema = control.schema;
        // A resume record this node wrote on a clean release still names this
        // exact root, so the local image can be continued instead of restored.
        // The record is an accelerator: every failure falls back to the origin.
        let database = match crate::cell::resume::take_matching(&destination, control, &replica) {
            Some(source) => {
                let resumed_started = std::time::Instant::now();
                match replica.open_resumed(&source, &destination) {
                    Ok(db) => {
                        self.inner.telemetry.activation_phase(
                            crate::fleet::telemetry::ActivationPhase::Resume,
                            resumed_started.elapsed(),
                        );
                        crate::cell::worker::RestoredDatabase::Local(Box::new(db))
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "Cell resume record did not continue");
                        let _ = replica.discard_resumed(&destination);
                        let _ = replica.discard_resumed(&source);
                        self.restore_exact(&replica, &root, schema, &destination)
                            .await?
                    }
                }
            }
            None => {
                self.restore_exact(&replica, &root, schema, &destination)
                    .await?
            }
        };
        self.ensure_acquiring()?;
        let current = authority.load(cell).await?.ok_or(Error::Fenced)?;
        if !current.value().is_same_or_pure_renewal_of(observed.value()) {
            return Err(Error::Fenced);
        }
        let activate_started = std::time::Instant::now();
        let handle = self
            .activate_inner(
                catalog,
                Activation::Restored(Box::new(RestoredActivation {
                    database,
                    destination,
                    incarnation,
                    schema,
                    root,
                    reservation,
                    job,
                })),
                replica,
                authority,
                current,
            )
            .await?;
        self.inner.telemetry.activation_phase(
            crate::fleet::telemetry::ActivationPhase::Activate,
            activate_started.elapsed(),
        );
        Ok(handle)
    }

    /// Verifies the immutable root and materializes it into the destination.
    async fn restore_exact(
        &self,
        replica: &cellule_ltx::CellReplica,
        root: &cellule_ltx::RootRef,
        schema: u32,
        destination: &Path,
    ) -> crate::Result<crate::cell::worker::RestoredDatabase> {
        let root_open_started = std::time::Instant::now();
        let verified = replica.open_root(root).await?;
        self.inner.telemetry.activation_phase(
            crate::fleet::telemetry::ActivationPhase::RootOpen,
            root_open_started.elapsed(),
        );
        if verified.schema() != schema {
            return Err(Error::Control(
                "immutable root schema does not match control",
            ));
        }
        let restore_started = std::time::Instant::now();
        let database = verified.paged().prepare_writable(destination).await?;
        self.inner.telemetry.activation_phase(
            crate::fleet::telemetry::ActivationPhase::Restore,
            restore_started.elapsed(),
        );
        Ok(crate::cell::worker::RestoredDatabase::Paged(Box::new(
            database,
        )))
    }

    fn claiming_cell(
        &self,
        catalog: &CatalogProof,
        observed: &VersionedControl,
        owner: &Owner,
    ) -> crate::Result<CellId> {
        let cell = catalog.entry().cell();
        if observed.value().cell != cell {
            return Err(Error::Control("ownership control changed Cell"));
        }
        if owner.session != self.inner.session {
            return Err(Error::Fenced);
        }
        if observed
            .value()
            .owner
            .as_ref()
            .is_some_and(|current| current.session == owner.session)
        {
            return Err(Error::CellAlreadyActive);
        }
        Ok(cell)
    }

    pub(super) fn check_application_limits(
        &self,
        catalog: &CatalogProof,
        limits: cellule_ltx::Limits,
    ) -> crate::Result<()> {
        let Some(application_limits) = self.inner.application_limits.get() else {
            return Ok(());
        };
        let Some(&(database, capture)) = application_limits.get(&catalog.entry().namespace())
        else {
            return Err(Error::Control(
                "Cell namespace is not declared by application",
            ));
        };
        if limits.max_database_bytes != database || limits.max_capture_bytes != capture {
            return Err(Error::Control(
                "Cell storage limits differ from application",
            ));
        }
        Ok(())
    }

    pub(super) fn activation_cell(
        &self,
        catalog: &CatalogProof,
        observed: &VersionedControl,
    ) -> crate::Result<CellId> {
        let cell = catalog.entry().cell();
        if observed.value().cell != cell {
            return Err(Error::Control("activation control changed Cell"));
        }
        if observed
            .value()
            .owner
            .as_ref()
            .is_none_or(|owner| owner.session != self.inner.session)
        {
            return Err(Error::Fenced);
        }
        Ok(cell)
    }

    pub(crate) fn ensure_running(&self) -> crate::Result<()> {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return Err(Error::RuntimeClosed);
        }
        self.inner.node_lease.check()
    }

    pub(super) fn ensure_acquiring(&self) -> crate::Result<()> {
        self.ensure_running()?;
        self.inner.node_admission.check_new_role()
    }

    async fn activate_inner(
        &self,
        catalog: CatalogProof,
        activation: Activation,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        observed: VersionedControl,
    ) -> crate::Result<CellHandle> {
        let cell = self.activation_cell(&catalog, &observed)?;
        let incarnation = observed.value().incarnation;
        let code = observed.value().code;
        let schema = observed.value().schema;
        let scratch_directory = match &activation {
            Activation::Restored(activation) => activation.destination.parent(),
            Activation::Bootstrap(activation) => activation.destination.parent(),
        }
        .ok_or(Error::Control("Cell activation destination has no parent"))?
        .to_owned();
        let (reply, response) = oneshot::channel();
        let mut publisher = CellPublisher::new(replica, authority, observed, scratch_directory);
        if let Some(node_lease) = self.inner.node_lease.guard()? {
            publisher = publisher.with_node_lease(node_lease);
        }
        publisher = publisher.with_node_durability_slot(Arc::clone(&self.inner.node_durability));
        publisher = publisher.with_telemetry(self.inner.telemetry.clone());
        self.inner
            .sender
            .send(Message::Activate {
                cell,
                role: catalog.entry().role(),
                catalog: catalog.clone(),
                activation,
                publisher: Box::new(publisher),
                reply,
            })
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        let admission = response.await.map_err(|_| Error::RuntimeClosed)??;
        Ok(CellHandle {
            cell,
            incarnation,
            code,
            schema,
            catalog,
            inner: self.inner.clone(),
            admission,
        })
    }

    pub(super) async fn replica_with_directory_cache(
        &self,
        replica: cellule_ltx::CellReplica,
        destination: &Path,
    ) -> crate::Result<cellule_ltx::CellReplica> {
        Self::replica_with_host_cache(replica, self.inner.replica_host.clone(), destination).await
    }

    pub(super) async fn replica_with_host_cache(
        replica: cellule_ltx::CellReplica,
        host: cellule_ltx::Host,
        destination: &Path,
    ) -> crate::Result<cellule_ltx::CellReplica> {
        let scratch_directory = destination
            .parent()
            .ok_or(Error::Control("Cell activation destination has no parent"))?;
        let host = host
            .with_directory_cache(scratch_directory.join(".cellule-directory-cache"))
            .await?;
        Ok(replica.with_host(host))
    }
}
