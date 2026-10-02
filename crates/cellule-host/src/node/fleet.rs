use super::*;
use crate::fleet::{
    FLEET_ACTION_COMPONENT, FleetActionCompletion, FleetActionExecutor, FleetActionJournal,
    FleetCellProvider, FleetEnrollmentJournal,
};
use cellule_runtime::fleet::operations::{FleetAction, FleetScope};
use cellule_runtime::identity::NodeId;

impl CellNode {
    /// Captures one original native page through the shared finite fleet lane.
    /// Authenticate the caller first. Both journal checks use the exact original
    /// head/registry and endpoint intent. A lost/canceled waiter retains the
    /// original native read and join; no acquisition or cleanup effect starts.
    /// Missing owners are explicit Unbound coverage, never proof of empty roles.
    pub async fn fleet_snapshot(
        &self,
        request: crate::fleet::FleetSnapshotRequest,
    ) -> Result<Arc<crate::fleet::FleetNodeSnapshot>, Arc<Error>> {
        if !self.is_management_ready() {
            return Err(Arc::new(Error::CellDraining));
        }
        let managed_startup = {
            let startup = self
                .fleet_startup
                .lock()
                .map_err(|_| Arc::new(Error::Control("CellNode fleet startup lock poisoned")))?;
            startup.as_ref().is_some_and(|s| {
                s.boot.is_some()
                    && s.intent.scope() == request.expected().head().scope()
                    && s.intent.node() == request.node()
                    && s.intent.session() == request.session()
            })
        };
        let owners = crate::fleet::snapshot::SnapshotOwners {
            state: Arc::clone(&self.state),
            managed_startup,
            readers: self
                .try_owned_component::<crate::read_replicas::ReadReplicaManager>("read-replicas")
                .map_err(Arc::new)?,
            followers: self
                .try_owned_component::<cellule_runtime::follower::FollowerStore>(
                    FOLLOWER_STORE_COMPONENT,
                )
                .map_err(Arc::new)?,
            supervisor: self
                .try_owned_component::<DurabilitySupervisor>(NODE_DURABILITY_SUPERVISOR_COMPONENT)
                .map_err(Arc::new)?,
            producer: self
                .try_owned_component::<crate::durability::enrollment::FleetFollowerEnrollment>(
                    NODE_DURABILITY_PROVIDER_COMPONENT,
                )
                .map_err(Arc::new)?,
        };
        let executor = self
            .try_owned_component::<FleetActionExecutor>(FLEET_ACTION_COMPONENT)
            .map_err(Arc::new)?
            .ok_or_else(|| Arc::new(Error::Control("fleet action executor is not installed")))?;
        executor.snapshot(request, owners).await
    }

    /// Installs journal-bound follower production in the existing durability
    /// supervisor. Applications supply read-only signed preparation, authenticated
    /// transports/authority and one shared atomic fleet journal. Install before
    /// startup; every selected member is Pending before its leader CAS.
    pub fn install_fleet_node_durability_provider<P: crate::FleetNodeDurabilityProvider>(
        &self,
        scope: FleetScope,
        node: NodeId,
        journal: Arc<dyn crate::fleet::FleetJournal>,
        provider: Arc<P>,
        configuration: NodeDurabilitySupervisorConfig,
    ) -> cellule_runtime::Result<()> {
        if configuration.application != scope.application {
            return Err(Error::Fenced);
        }
        if let Some(startup) = self
            .fleet_startup
            .lock()
            .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?
            .as_ref()
            && (startup.intent.scope() != scope || startup.intent.node() != node)
        {
            return Err(Error::Fenced);
        }
        let tasks = self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?
            .clone()
            .ok_or(Error::Control(
                "fleet follower enrollment requires an installed task group",
            ))?;
        let producer = Arc::new(crate::durability::enrollment::FleetFollowerEnrollment::new(
            scope,
            node,
            self.session,
            provider,
            journal,
            self.runtime.clone(),
            configuration.recruit_interval,
            tasks.cancellation_token(),
        )?);
        self.install_node_durability_provider(producer, configuration)
    }

    /// Captures current local follower enrollment, including unknown and paused
    /// steps. Durable retirement removes local progress; None proves no absence,
    /// completion, withdrawal or fleet finalization.
    pub fn follower_enrollment_completion(
        &self,
        log_epoch: u64,
    ) -> cellule_runtime::Result<Option<crate::FollowerEnrollmentCompletion>> {
        match self.owned_component::<crate::durability::enrollment::FleetFollowerEnrollment>(
            NODE_DURABILITY_PROVIDER_COMPONENT,
        ) {
            Some(producer) => producer.completion(log_epoch),
            None => Ok(None),
        }
    }

    /// Captures every retained follower epoch through bounded advisory pages.
    /// A busy protocol includes preparation before any request exists. None
    /// means no managed producer is installed and supplies no role coverage.
    /// Current directory authority, inbound lanes and replacement policy must
    /// also be observed before settlement or maintenance finalization.
    pub fn fleet_follower_enrollments_page(
        &self,
        cursor: Option<crate::FollowerEnrollmentInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> cellule_runtime::Result<Option<crate::FollowerEnrollmentInventoryPage>> {
        if !(1..=crate::durability::enrollment::MAX_FOLLOWER_ENROLLMENT_EPOCHS).contains(&limit)
            || now_ms < 0
        {
            return Err(Error::Node("invalid follower enrollment inventory bounds"));
        }
        self.try_owned_component::<crate::durability::enrollment::FleetFollowerEnrollment>(
            NODE_DURABILITY_PROVIDER_COMPONENT,
        )?
        .map(|producer| producer.page(cursor, limit, now_ms))
        .transpose()
    }
    /// Confirms startup against an atomic current-intent/established-boot read.
    /// The application first journals Pending, performs canonical directory
    /// enrollment and publishes checked evidence. Missing, pending or ambiguous
    /// evidence leaves all new roles and readiness closed. This read is safely
    /// cancellable. Start removes the hold only after all owned startup probes.
    pub async fn confirm_fleet_startup(
        &self,
        journal: &dyn FleetEnrollmentJournal,
        enrollment_key: cellule_runtime::Digest,
    ) -> cellule_runtime::Result<()> {
        let original = self
            .fleet_startup
            .lock()
            .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?
            .as_ref()
            .map(|startup| startup.intent.clone())
            .ok_or(Error::Control("CellNode fleet startup is not configured"))?;
        let observed = journal
            .load_boot(original.scope(), original.node(), enrollment_key)
            .await
            .map_err(|source| Error::Facility {
                name: "fleet-enrollment-journal",
                source,
            })?
            .ok_or(Error::Control("CellNode fleet boot enrollment is absent"))?;
        self.apply_fleet_boot(&original, enrollment_key, &observed, true, None)
    }

    /// Refreshes this running boot's retained intent through one atomic journal read.
    /// Call from the application's supervised membership/lease loop before renewal.
    /// The original Established boot, acceptance time and evidence must still match.
    /// Cordon/drain close the shared role gate without stopping existing Cell work,
    /// reopening admission, or starting another scheduler. A read error or deadline
    /// preserves the local state and must be retried or handled by the application;
    /// it grants no authority to renew the boot's lease. Cancellation drops only
    /// this read waiter, and a delayed reply cannot regress intent or reopen shutdown.
    pub async fn refresh_fleet_intent(
        &self,
        journal: &dyn FleetEnrollmentJournal,
        deadline: Instant,
    ) -> cellule_runtime::Result<cellule_runtime::fleet::operations::NodeIntent> {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let (original, key) = {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
            if !matches!(
                *state,
                NodeState::Ready | NodeState::ScalingDown | NodeState::Maintenance
            ) {
                return Err(Error::CellDraining);
            }
            let startup = self
                .fleet_startup
                .lock()
                .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?;
            let startup = startup
                .as_ref()
                .ok_or(Error::Control("CellNode fleet startup is not configured"))?;
            let boot = startup.boot.as_ref().ok_or(Error::Control(
                "CellNode fleet boot enrollment is unconfirmed",
            ))?;
            (
                startup.intent.clone(),
                boot.spec().key().map_err(crate::fleet::operation)?,
            )
        };
        let observed = tokio::time::timeout_at(
            deadline.into(),
            journal.load_boot(original.scope(), original.node(), key),
        )
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(|source| Error::Facility {
            name: "fleet-enrollment-journal",
            source,
        })?
        .ok_or(Error::Control("CellNode fleet boot enrollment is absent"))?;
        self.apply_fleet_boot(&original, key, &observed, false, Some(deadline))?;
        Ok(observed.intent().clone())
    }

    fn apply_fleet_boot(
        &self,
        original: &cellule_runtime::fleet::operations::NodeIntent,
        enrollment_key: cellule_runtime::Digest,
        observed: &crate::fleet::FleetBootObservation,
        startup_only: bool,
        deadline: Option<Instant>,
    ) -> cellule_runtime::Result<()> {
        let intent = observed.intent();
        let enrollment = observed.enrollment();
        if intent.scope() != original.scope()
            || intent.node() != original.node()
            || intent.session() != self.session
            || intent.revision() < original.revision()
            || enrollment.spec().key().map_err(crate::fleet::operation)? != enrollment_key
        {
            return Err(Error::Fenced);
        }
        // Lifecycle-before-startup matches start() and refresh admission. Shutdown cannot race an
        // asynchronous read into reopening the already-draining host.
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
        if (startup_only && *state != NodeState::Starting)
            || (!startup_only
                && !matches!(
                    *state,
                    NodeState::Ready | NodeState::ScalingDown | NodeState::Maintenance
                ))
        {
            return Err(Error::CellDraining);
        }
        let mut startup = self
            .fleet_startup
            .lock()
            .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?;
        let startup = startup
            .as_mut()
            .ok_or(Error::Control("CellNode fleet startup is not configured"))?;
        if intent.revision() < startup.intent.revision()
            || (intent.revision() == startup.intent.revision() && intent != &startup.intent)
            || startup.boot.as_ref().is_some_and(|boot| boot != enrollment)
        {
            return Err(Error::Fenced);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::Deadline);
        }
        match intent.mode() {
            cellule_runtime::node::NodeMode::Active => {}
            cellule_runtime::node::NodeMode::Cordoned => self.runtime.node_admission().cordon()?,
            cellule_runtime::node::NodeMode::Draining => {
                self.runtime.node_admission().begin_drain()?
            }
        }
        startup.intent = intent.clone();
        startup.boot = Some(enrollment.clone());
        Ok(())
    }
    /// Binds every reader activation and canonical removal to the durable fleet
    /// registry. Install after read replicas and before start. Applications own
    /// journal storage and authorization; the manager owns accepted finite work.
    pub fn install_fleet_reader_enrollment(
        &self,
        scope: FleetScope,
        node: NodeId,
        journal: Arc<dyn crate::fleet::FleetJournal>,
    ) -> cellule_runtime::Result<()> {
        if let Some(startup) = self
            .fleet_startup
            .lock()
            .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?
            .as_ref()
            && (startup.intent.scope() != scope || startup.intent.node() != node)
        {
            return Err(Error::Fenced);
        }
        let manager = self
            .owned_component::<crate::read_replicas::ReadReplicaManager>("read-replicas")
            .ok_or(Error::Control(
                "read replicas must be installed before reader enrollment",
            ))?;
        let binding = Arc::new(crate::read_replicas::enrollment::ReaderEnrollment::new(
            scope, node, journal,
        )?);
        self.install_owned_component_with_drain(
            "fleet-reader-enrollment",
            Arc::clone(&binding),
            || async { Ok(()) },
        )?;
        if let Err(error) = manager.bind_enrollment(binding) {
            self.remove_facility("fleet-reader-enrollment")?;
            return Err(error);
        }
        Ok(())
    }

    /// Captures read-only request-bound evidence through the owned fleet lane.
    /// Authenticate the caller first. The node checks current journal state and
    /// actual authority/actor readiness; cached effect replies are not consulted
    /// as proof of current serving. A dropped waiter leaves the finite job owned.
    pub async fn inspect_fleet_action(
        &self,
        request: cellule_runtime::fleet::operations::FleetInspectionRequest,
    ) -> Result<Arc<cellule_runtime::fleet::operations::FleetInspectionObservation>, Arc<Error>>
    {
        if !self.is_management_ready() {
            return Err(Arc::new(Error::CellDraining));
        }
        let executor = self
            .owned_component::<FleetActionExecutor>(FLEET_ACTION_COMPONENT)
            .ok_or_else(|| Arc::new(Error::Control("fleet action executor is not installed")))?;
        executor.observe(request).await
    }

    /// Installs the journal-bound fleet executor during startup.
    ///
    /// The application supplies a stable physical node and authenticated scope,
    /// and a journal that atomically checks authorization at first acceptance.
    /// The executor uses this node's runtime/session and joins finite accepted
    /// work before runtime shutdown. Install before opening readiness.
    pub fn install_fleet_actions(
        &self,
        scope: FleetScope,
        node: NodeId,
        journal: Arc<dyn FleetActionJournal>,
        cells: Arc<dyn FleetCellProvider>,
    ) -> cellule_runtime::Result<()> {
        if let Some(startup) = self
            .fleet_startup
            .lock()
            .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?
            .as_ref()
            && (startup.intent.scope() != scope || startup.intent.node() != node)
        {
            return Err(Error::Fenced);
        }
        let executor = Arc::new(FleetActionExecutor::new(
            self.runtime.clone(),
            scope,
            node,
            self.session,
            journal,
            cells,
            self.application.registry(),
        )?);
        let drained = Arc::clone(&executor);
        self.install_owned_component_with_drain(FLEET_ACTION_COMPONENT, executor, move || {
            let drained = Arc::clone(&drained);
            async move {
                drained
                    .drain()
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
            }
        })
    }

    /// Executes a journal-bound movement or cordon independently of its waiter.
    ///
    /// Applications authenticate the caller before invoking this local boundary.
    /// The executor supports settled movement, explicit busy maintenance
    /// release, receiver inspection, and cordon through the shared role gate.
    /// Role settlement and
    /// finalization require their host barriers and are refused. Count a
    /// result only when `committed` is true, then inspect current serving evidence.
    /// Raw Inspect actions are refused: use `inspect_fleet_action` so a durable
    /// historical acknowledgement cannot masquerade as a current observation.
    pub async fn apply_fleet_action(
        &self,
        action: FleetAction,
        now_ms: i64,
    ) -> Result<Arc<FleetActionCompletion>, Arc<Error>> {
        if !self.is_management_ready() {
            return Err(Arc::new(Error::CellDraining));
        }
        let executor = self
            .owned_component::<FleetActionExecutor>(FLEET_ACTION_COMPONENT)
            .ok_or_else(|| Arc::new(Error::Control("fleet action executor is not installed")))?;
        executor.apply(action, now_ms).await
    }
}
