use std::sync::Arc;

use cellule_runtime::Error;
use cellule_runtime::fleet::operations::{
    DrainBlocker, FleetActionKind, FleetActionOutcome, FleetInspectionObservation,
    FleetInspectionRequest, MaintenanceAction, MovementAction, OperationError,
};
use cellule_runtime::node::NodeMode;

use super::actions::{FleetActionExecutor, journal_error, operation, wall_time_ms};

impl FleetActionExecutor {
    pub(super) async fn execute_inspection(
        &self,
        request: FleetInspectionRequest,
    ) -> Result<Arc<FleetInspectionObservation>, Arc<Error>> {
        let started = wall_time_ms().map_err(Arc::new)?;
        if started < request.action().issued_at_ms() || started >= request.deadline_ms() {
            return Err(Arc::new(operation(OperationError::Deadline)));
        }
        self.journal
            .authorize_inspection(&request, started)
            .await
            .map_err(journal_error)
            .map_err(Arc::new)?;
        let inspected = match request.action().kind() {
            FleetActionKind::Movement {
                action: MovementAction::Inspect,
                attempt,
            } => {
                // This path never reads a cached Inspect result and never
                // invokes acquisition or cleanup. Current serving comes from
                // actor + authority.
                let result = self.inspect(attempt).await.map_err(Arc::new)?;
                if let Some(error) = result.error {
                    return Err(Arc::new(error));
                }
                result.outcome
            }
            FleetActionKind::Maintenance {
                action: MaintenanceAction::Inspect,
                operation,
            } => {
                if operation.node() != self.node || operation.session() != self.session {
                    return Err(Arc::new(Error::Fenced));
                }
                // This is deliberately a local admission check only. It can
                // confirm local Cordon after a lost reply, but cannot claim role settlement,
                // drained facilities, Stopped, or session withdrawal.
                match self.runtime.node_admission().mode().map_err(Arc::new)? {
                    NodeMode::Draining => {
                        cellule_runtime::fleet::operations::FleetOutcome::Cordoned
                    }
                    NodeMode::Active | NodeMode::Cordoned => {
                        cellule_runtime::fleet::operations::FleetOutcome::Blocked(
                            DrainBlocker::IncompleteObservation,
                        )
                    }
                }
            }
            _ => {
                return Err(Arc::new(Error::Control(
                    "fleet inspection requires an Inspect action",
                )));
            }
        };
        let finished = wall_time_ms().map_err(Arc::new)?;
        if finished >= request.deadline_ms() {
            return Err(Arc::new(operation(OperationError::Deadline)));
        }
        let outcome = FleetActionOutcome {
            scope: request.action().scope(),
            action_key: request
                .action()
                .key()
                .map_err(operation)
                .map_err(Arc::new)?,
            node: self.node,
            session: self.session,
            observed_at_ms: finished,
            outcome: inspected,
        };
        FleetInspectionObservation::new(request, started, outcome)
            .map(Arc::new)
            .map_err(operation)
            .map_err(Arc::new)
    }
}
