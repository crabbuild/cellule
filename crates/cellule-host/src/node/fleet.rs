use super::*;
use crate::fleet::{
    FLEET_ACTION_COMPONENT, FleetActionCompletion, FleetActionExecutor, FleetActionJournal,
    FleetCellProvider, FleetEnrollmentJournal,
};
use cellule_runtime::fleet::operations::{FleetAction, FleetScope};
use cellule_runtime::identity::NodeId;

impl CellNode {
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
        // Lifecycle-before-startup matches start(). Shutdown cannot race an
        // asynchronous read into reopening the already-draining host.
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
        if *state != NodeState::Starting {
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
        {
            return Err(Error::Fenced);
        }
        match intent.mode() {
            cellule_runtime::node::NodeMode::Active => {}
            cellule_runtime::node::NodeMode::Cordoned => self.runtime.node_admission().cordon()?,
            cellule_runtime::node::NodeMode::Draining => {
                self.runtime.node_admission().begin_drain()?
            }
        }
        startup.intent = intent.clone();
        startup.confirmed = true;
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
