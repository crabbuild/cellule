//! Monotonic admission closure under an exact accepted physical-node intent.

use super::actions::{ActionResult, FleetActionExecutor};
use cellule_runtime::Error;
use cellule_runtime::fleet::operations::{
    AcceptedFleetAction, FleetActionKind, FleetOutcome, MaintenanceAction,
};
use cellule_runtime::node::NodeMode;

impl FleetActionExecutor {
    pub(super) async fn perform_action(
        &self,
        accepted: &AcceptedFleetAction,
        settlement: Option<std::sync::Arc<crate::fleet::FleetRoleSettlement>>,
    ) -> cellule_runtime::Result<ActionResult> {
        match accepted.action().kind() {
            FleetActionKind::Movement { .. } => self.perform_movement(accepted).await,
            FleetActionKind::Maintenance { .. } => {
                self.perform_maintenance(accepted, settlement.as_deref())
            }
        }
    }

    pub(super) fn perform_maintenance(
        &self,
        accepted: &AcceptedFleetAction,
        settlement: Option<&crate::fleet::FleetRoleSettlement>,
    ) -> cellule_runtime::Result<ActionResult> {
        let FleetActionKind::Maintenance { action, operation } = accepted.action().kind() else {
            return Err(Error::Control("unsupported fleet maintenance effect"));
        };
        if accepted.action().scope() != self.scope
            || operation.node() != self.node
            || operation.session() != self.session
        {
            return Err(Error::Fenced);
        }
        match action {
            MaintenanceAction::Cordon => {
                // The journal accepted the retained Draining intent. This closes the
                // same gate used by writers, readers and followers, without releasing
                // any existing responsibility or taking the node shutdown lane.
                let gate = self.runtime.node_admission();
                gate.begin_drain()?;
                if gate.mode()? != NodeMode::Draining {
                    return Err(Error::CellDraining);
                }
                Ok(ActionResult::checked(FleetOutcome::Cordoned))
            }
            MaintenanceAction::SettleRoles => {
                let proof = settlement.ok_or(Error::Control(
                    "role settlement requires fresh complete host evidence",
                ))?;
                proof.validate_for(accepted.action())?;
                Ok(ActionResult::checked(FleetOutcome::RolesSettledAt {
                    inventory: proof.inventory(),
                    head_revision: proof.head_revision(),
                    registry: proof.registry(),
                }))
            }
            _ => Err(Error::Control("unsupported fleet maintenance effect")),
        }
    }
}
