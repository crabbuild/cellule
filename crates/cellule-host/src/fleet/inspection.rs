use std::sync::Arc;

use cellule_runtime::Error;
use cellule_runtime::fleet::operations::{
    FleetActionKind, FleetActionOutcome, FleetInspectionObservation, FleetInspectionRequest,
    MovementAction, OperationError,
};

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
        let FleetActionKind::Movement {
            action: MovementAction::Inspect,
            attempt,
        } = request.action().kind()
        else {
            return Err(Arc::new(Error::Control("fleet inspection is not movement")));
        };
        // This path never reads a cached Inspect result and never invokes
        // acquisition or cleanup. Current serving comes from actor + authority.
        let result = self.inspect(attempt).await.map_err(Arc::new)?;
        if let Some(error) = result.error {
            return Err(Arc::new(error));
        }
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
            outcome: result.outcome,
        };
        FleetInspectionObservation::new(request, started, outcome)
            .map(Arc::new)
            .map_err(operation)
            .map_err(Arc::new)
    }
}
