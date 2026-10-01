use super::*;
use crate::fleet::{
    FLEET_ACTION_COMPONENT, FleetActionCompletion, FleetActionExecutor, FleetActionJournal,
    FleetCellProvider,
};
use cellule_runtime::fleet::operations::{FleetAction, FleetScope};
use cellule_runtime::identity::NodeId;

impl CellNode {
    /// Captures read-only request-bound evidence through the owned fleet lane.
    /// Authenticate the caller first. The node checks current journal state and
    /// actual authority/actor readiness; cached effect replies are not consulted
    /// as proof of current serving. A dropped waiter leaves the finite job owned.
    pub async fn inspect_fleet_action(
        &self,
        request: cellule_runtime::fleet::operations::FleetInspectionRequest,
    ) -> Result<Arc<cellule_runtime::fleet::operations::FleetInspectionObservation>, Arc<Error>>
    {
        if !self.is_ready() {
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

    /// Executes a journal-bound movement independently of its transport waiter.
    ///
    /// Applications authenticate the caller before invoking this local boundary.
    /// The current executor supports settled movement and receiver inspection;
    /// maintenance action integration remains under implementation. Count a
    /// result only when `committed` is true, then inspect current serving evidence.
    /// Raw Inspect actions are refused: use `inspect_fleet_action` so a durable
    /// historical acknowledgement cannot masquerade as a current observation.
    pub async fn apply_fleet_action(
        &self,
        action: FleetAction,
        now_ms: i64,
    ) -> Result<Arc<FleetActionCompletion>, Arc<Error>> {
        if !self.is_ready() {
            return Err(Arc::new(Error::CellDraining));
        }
        let executor = self
            .owned_component::<FleetActionExecutor>(FLEET_ACTION_COMPONENT)
            .ok_or_else(|| Arc::new(Error::Control("fleet action executor is not installed")))?;
        executor.apply(action, now_ms).await
    }
}
