//! Real receiver takeover faults at the durable journal boundaries.
use super::*;
use crate::journal::{RecoveryWrite, RecoveryWriteBoundary};
use cellule_host::fleet::{FleetActionCompletion, FleetAdapterFuture, FleetReconcileReport};
use cellule_runtime::control::Control;
use std::sync::Mutex;

mod fixture;
mod inherited;
mod tests;

struct CapturedTransport {
    fleet: Arc<adapters::LocalFleet>,
    activation: Mutex<Option<Arc<FleetActionCompletion>>>,
}
impl FleetTransport for CapturedTransport {
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            let result = self.fleet.dispatch(action, deadline).await?;
            if action.receiver_route().is_some()
                && matches!(
                    action.kind(),
                    FleetActionKind::Movement {
                        action: MovementAction::Activate,
                        ..
                    }
                )
            {
                *self.activation.lock().unwrap() = Some(result.clone());
            }
            Ok(result)
        })
    }
    fn inspect<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        self.fleet.inspect(request, deadline)
    }
}

fn assert_original_error(completion: &FleetActionCompletion) {
    let error = completion.execution_error.as_ref().unwrap();
    let mut source: &dyn std::error::Error = error;
    loop {
        if let Some(io) = source.downcast_ref::<std::io::Error>() {
            assert_eq!(
                io.to_string(),
                "injected receiver recovery write reply failure"
            );
            break;
        }
        source = source.source().expect("original journal error was lost");
    }
}
