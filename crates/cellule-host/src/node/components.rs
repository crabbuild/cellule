//! Telemetry, lease, task-group, facility, and component installation.

use super::*;

impl CellNode {
    /// Installs the product's metrics adapter before the node is advertised.
    pub fn install_telemetry(
        &self,
        telemetry: Arc<dyn cellule_runtime::fleet::telemetry::CellTelemetry>,
    ) -> cellule_runtime::Result<()> {
        self.runtime.install_telemetry(telemetry)
    }

    /// Installs the authoritative node lease before readiness is exposed.
    ///
    /// A coordination task group must already be installed. This convenience
    /// path is intentionally fail-closed so a node can never advertise while
    /// its runtime-wide supervisors are unowned.
    pub fn install_node_lease(
        &self,
        lease: cellule_runtime::NodeLeaseGuard,
    ) -> cellule_runtime::Result<()> {
        self.require_task_group()?;
        self.install_node_lease_for_startup(lease)?;
        self.start()
    }

    /// Installs the lease without opening readiness to the product boundary.
    ///
    /// Servers use this during startup, then call [`Self::start`] only
    /// after their listeners and owned facilities have been started.
    pub fn install_node_lease_for_startup(
        &self,
        lease: cellule_runtime::NodeLeaseGuard,
    ) -> cellule_runtime::Result<()> {
        self.runtime.install_node_lease(lease)?;
        self.lease_installed.store(true, Ordering::Release);
        Ok(())
    }

    /// Declares the typed production components that must be retained before
    /// readiness can open. The declaration is immutable after startup begins;
    /// this keeps a product adapter from accidentally starting a node with a
    /// missing owner and discovering the gap only on its first request.
    pub fn require_owned_components(
        &self,
        names: impl IntoIterator<Item = &'static str>,
    ) -> cellule_runtime::Result<()> {
        if self.state() != NodeState::Starting {
            return Err(Error::CellDraining);
        }
        let mut required = self
            .required_components
            .lock()
            .map_err(|_| Error::Control("CellNode required-component lock poisoned"))?;
        append_required_components(&mut required, names)
    }

    /// Creates and retains the bounded coordination task group for this node.
    pub fn install_task_group(
        &self,
        cancellation: CancellationToken,
        node_shutdown: CancellationToken,
    ) -> cellule_runtime::Result<Arc<CellNodeTaskGroup>> {
        let task_group = Arc::new(CellNodeTaskGroup::new(cancellation, node_shutdown));
        let mut installed = self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?;
        if installed.is_some() {
            return Err(Error::Control("CellNode task group already installed"));
        }
        let drain_group = Arc::clone(&task_group);
        let facility = CellNodeFacility::new("cell-coordination-tasks", move || {
            let drain_group = Arc::clone(&drain_group);
            async move { drain_group.drain().await }
        })?;
        self.install_facility(facility)?;
        *installed = Some(Arc::clone(&task_group));
        Ok(task_group)
    }

    pub(super) fn require_task_group(&self) -> cellule_runtime::Result<()> {
        let installed = self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?;
        if installed.is_none() {
            return Err(Error::Control(
                "CellNode cannot become ready before its task group is installed",
            ));
        }
        Ok(())
    }

    /// Installs the provider enrollment adapter and moves node-log recruitment
    /// and rotation into the host-owned task group.
    pub fn install_node_durability_provider<P>(
        &self,
        provider: Arc<P>,
        configuration: NodeDurabilitySupervisorConfig,
    ) -> cellule_runtime::Result<()>
    where
        P: NodeDurabilityProvider,
    {
        let task_group = self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?
            .clone()
            .ok_or(Error::Control(
                "CellNode durability provider requires an installed task group",
            ))?;
        task_group.ensure_accepting_tasks()?;
        let supervisor = Arc::new(DurabilitySupervisor::new(
            provider.clone(),
            self.runtime.clone(),
            configuration,
            self.session,
            task_group.cancellation.clone(),
        ));
        let drained = Arc::clone(&supervisor);
        self.install_facilities([
            CellNodeFacility::owned(NODE_DURABILITY_PROVIDER_COMPONENT, provider, || async {
                Ok(())
            })?,
            CellNodeFacility::owned(
                NODE_DURABILITY_SUPERVISOR_COMPONENT,
                Arc::clone(&supervisor),
                move || {
                    let drained = Arc::clone(&drained);
                    async move { drained.drain().await }
                },
            )?,
        ])?;
        // The task group owns supervision and health; the facility retains the
        // actual join so cancelling this watcher cannot cancel accepted effects.
        let result = task_group.spawn_boxed(async move { supervisor.join().await });
        if result.is_err() {
            self.remove_facility(NODE_DURABILITY_SUPERVISOR_COMPONENT)?;
            self.remove_facility(NODE_DURABILITY_PROVIDER_COMPONENT)?;
        }
        result
    }

    /// Requests confirmed retirement of one exact epoch through the existing
    /// supervisor, bypassing normal frame thresholds. The embedding application
    /// authorizes this call and journals acceptance/results before finalization.
    /// Duplicate epoch requests share retained progress; dropping a handle does
    /// not cancel work. An automatic rotation already in flight is refused.
    pub fn request_node_log_rotation(
        &self,
        log_epoch: u64,
    ) -> cellule_runtime::Result<NodeLogRotationRequest> {
        let supervisor = self
            .owned_component::<DurabilitySupervisor>(NODE_DURABILITY_SUPERVISOR_COMPONENT)
            .ok_or(Error::Control(
                "CellNode durability supervisor is not installed",
            ))?;
        if !matches!(
            self.state(),
            NodeState::Ready | NodeState::ScalingDown | NodeState::Maintenance
        ) {
            return Err(Error::CellDraining);
        }
        supervisor
            .requests
            .request(&self.runtime, log_epoch, &supervisor.cancellation)
    }

    /// Looks up one retained epoch request, including interrupted work during
    /// drain. Missing local progress never proves role absence or completion.
    pub fn node_log_rotation_request(
        &self,
        log_epoch: u64,
    ) -> cellule_runtime::Result<Option<NodeLogRotationRequest>> {
        match self.owned_component::<DurabilitySupervisor>(NODE_DURABILITY_SUPERVISOR_COMPONENT) {
            Some(supervisor) => supervisor.requests.lookup(log_epoch),
            None => Ok(None),
        }
    }

    /// Owns read-snapshot refresh, eviction, and terminal close for this node.
    ///
    /// Install during startup after the task group. The product supplies an
    /// application-scoped store, signed directory, private local root, and LTX
    /// bounds; it authorizes owner hints and wires the returned peer resolver.
    pub fn install_read_replicas(
        &self,
        layout: cellule_runtime::ltx::CellStorageLayout,
        directory: cellule_runtime::node::NodeDirectory,
        root: PathBuf,
        limits: ReplicaLimits,
    ) -> cellule_runtime::Result<crate::read_replicas::ReadReplicaManager> {
        const COMPONENT: &str = "read-replicas";
        let tasks = self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?
            .clone()
            .ok_or(Error::Control(
                "CellNode read replicas require an installed task group",
            ))?;
        let manager = crate::read_replicas::ReadReplicaManager::new(
            self.runtime.clone(),
            self.application.registry(),
            layout,
            directory,
            self.session,
            root,
            limits,
        );
        if self
            .fleet_startup
            .lock()
            .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?
            .is_some()
        {
            self.require_owned_components(["fleet-reader-enrollment"])?;
            manager.require_enrollment();
        }
        let drained = manager.clone();
        self.install_owned_component_with_drain(COMPONENT, Arc::new(manager.clone()), move || {
            let drained = drained.clone();
            async move {
                drained
                    .shutdown()
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
            }
        })?;
        let supervised = manager.clone();
        let cancellation = tasks.cancellation_token();
        if let Err(error) = tasks.spawn(async move { supervised.run(cancellation).await }) {
            self.remove_facility(COMPONENT)?;
            return Err(error);
        }
        Ok(manager)
    }

    /// Supervises owner-side reader recruitment after installing read replicas.
    ///
    /// The product supplies application scope and an activation-authorized peer
    /// client. The host retains the cursor and cancels recruitment before drain.
    pub fn install_read_replica_recruitment(
        &self,
        identity: cellule_runtime::cell::application::ApplicationIdentity,
        peer: cellule_runtime::peer::ReplicaPeerClient,
    ) -> cellule_runtime::Result<crate::read_replicas::ReadReplicaRecruiter> {
        const COMPONENT: &str = "read-replica-recruitment";
        let readers = self
            .owned_component::<crate::read_replicas::ReadReplicaManager>("read-replicas")
            .ok_or(Error::Control(
                "read recruitment requires installed read replicas",
            ))?;
        let tasks = self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?
            .clone()
            .ok_or(Error::Control(
                "read recruitment requires installed task group",
            ))?;
        let recruiter =
            crate::read_replicas::ReadReplicaRecruiter::new((*readers).clone(), identity, peer)?;
        self.install_owned_component(COMPONENT, Arc::new(recruiter.clone()))?;
        let supervised = recruiter.clone();
        let cancellation = tasks.cancellation_token();
        if let Err(error) = tasks.spawn(async move { supervised.run(cancellation).await }) {
            self.remove_facility(COMPONENT)?;
            return Err(error);
        }
        Ok(recruiter)
    }

    pub(super) fn remove_facility(&self, name: &'static str) -> cellule_runtime::Result<()> {
        let mut facilities = self
            .facilities
            .lock()
            .map_err(|_| Error::Control("CellNode facility lock poisoned"))?;
        let Some(index) = facilities.iter().position(|facility| facility.name == name) else {
            return Err(Error::Control(
                "CellNode facility rollback target is missing",
            ));
        };
        facilities.remove(index);
        Ok(())
    }

    /// Attaches one provider-owned lifecycle component during node startup.
    pub fn install_facility(&self, facility: CellNodeFacility) -> cellule_runtime::Result<()> {
        self.install_facilities(std::iter::once(facility))
    }

    /// Atomically attaches a bounded batch of provider-owned facilities.
    ///
    /// All names and capacity are validated before any facility is retained, so
    /// a failed composition cannot leave the node with a partial owner set.
    /// Registration closes when readiness opens so the owner set cannot change
    /// underneath admitted requests.
    pub fn install_facilities(
        &self,
        facilities: impl IntoIterator<Item = CellNodeFacility>,
    ) -> cellule_runtime::Result<()> {
        let mut additions = Vec::new();
        for facility in facilities {
            if additions.len() >= MAX_NODE_FACILITIES {
                return Err(Error::Capacity("CellNode facility limit reached"));
            }
            additions.push(facility);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
        if *state != NodeState::Starting {
            return Err(Error::CellDraining);
        }
        let mut facilities = self
            .facilities
            .lock()
            .map_err(|_| Error::Control("CellNode facility lock poisoned"))?;
        if facilities.len().saturating_add(additions.len()) > MAX_NODE_FACILITIES {
            return Err(Error::Capacity("CellNode facility limit reached"));
        }
        let mut names = facilities
            .iter()
            .map(|facility| facility.name)
            .collect::<HashSet<_>>();
        if additions
            .iter()
            .any(|facility| !names.insert(facility.name))
        {
            return Err(Error::Control("CellNode facility name already installed"));
        }
        facilities.extend(additions);
        Ok(())
    }

    /// Retains one shared composition component under the node lifecycle.
    ///
    /// Components are deliberately type-erased only inside the host. Callers
    /// retrieve them by the same stable name and concrete type, while the
    /// node remains the sole owner of the production composition boundary.
    pub fn install_owned_component<T>(
        &self,
        name: &'static str,
        component: Arc<T>,
    ) -> cellule_runtime::Result<()>
    where
        T: Send + Sync + 'static,
    {
        self.install_owned_component_with_drain(name, component, || async { Ok(()) })
    }

    /// Retains one component and attaches its idempotent drain callback.
    pub fn install_owned_component_with_drain<T, F, Fut>(
        &self,
        name: &'static str,
        component: Arc<T>,
        drain: F,
    ) -> cellule_runtime::Result<()>
    where
        T: Send + Sync + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = FacilityResult> + Send + 'static,
    {
        self.install_facility(CellNodeFacility::owned(name, component, drain)?)
    }

    pub(crate) fn install_follower_store(
        &self,
        configuration: Option<(PathBuf, ReplicaLimits, DiskBudget)>,
    ) -> cellule_runtime::Result<()> {
        let Some((root, limits, disk)) = configuration else {
            return Ok(());
        };
        let store = FollowerStore::open(root, limits, disk)?
            .with_node_admission(self.runtime.node_admission());
        self.install_owned_component(FOLLOWER_STORE_COMPONENT, Arc::new(store))
    }

    /// Looks up one node-owned component for a product adapter.
    #[must_use]
    pub fn owned_component<T>(&self, name: &str) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.facilities
            .lock()
            .ok()?
            .iter()
            .find(|facility| facility.name == name)
            .and_then(CellNodeFacility::owner)
    }
}
