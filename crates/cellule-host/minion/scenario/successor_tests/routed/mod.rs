//! Closed-receiver continuation through the real host and SQLite journal.
use super::*;
use cellule_host::fleet::{
    FleetActionAcceptance, FleetAdapterFuture, FleetFailedBootProcessEvidence,
    FleetFailedBootProcessRequest, FleetFailedBootProcesses, FleetFailedBootRetirement,
    FleetObservation, FleetObserver, FleetRoster, FleetTransport,
};
use cellule_runtime::control::{ControlState, Transition};
mod fixture;
mod tests;

/// Test-only provider: this in-process lifecycle fixture treats a joined
/// CellNode shutdown as its retained original-process evidence. It does not
/// qualify process supervision or replace an application's process provider.
#[derive(Clone)]
struct StoppedNodeProcesses {
    node: Arc<CellNode>,
}

impl FleetFailedBootProcesses for StoppedNodeProcesses {
    fn confirm_stopped<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence> {
        Box::pin(async move {
            let endpoint = request.boot().spec().target;
            let stats = self.node.stats();
            if endpoint.node != node_id(1)
                || endpoint.session != session(1)
                || self.node.state() != NodeState::Stopped
                || stats.active_cells() != 0
                || stats.worker_jobs() != 0
                || stats.retained_bytes() != 0
                || stats.resident_bytes() != 0
                || stats.file_descriptors() != 0
                || stats.local_disk_reserved_bytes() != 0
            {
                return Err(invalid("receiver process has not joined shutdown"));
            }
            let mut hash = blake3::Hasher::new();
            hash.update(b"cellule.example-test-stopped-node.v1\0");
            hash.update(request.digest().as_bytes());
            Ok(FleetFailedBootProcessEvidence::new(
                request,
                Digest::from_bytes(*hash.finalize().as_bytes()),
            )?)
        })
    }
}

struct ClosedBootObserver {
    fleet: Arc<adapters::LocalFleet>,
    request: FleetFailedBootProcessRequest,
    processes: StoppedNodeProcesses,
}

impl FleetObserver for ClosedBootObserver {
    fn observe<'a>(
        &'a self,
        roster: &'a FleetRoster,
        _: i64,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(observation::observe_with_failed_boot_closure(
            &self.fleet,
            roster,
            deadline,
            &self.request,
            &self.processes,
            session(0),
        ))
    }
}

struct LoseRoutedActivationReply {
    inner: Arc<adapters::LocalFleet>,
    lost: std::sync::atomic::AtomicBool,
}

impl FleetTransport for LoseRoutedActivationReply {
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<cellule_host::fleet::FleetActionCompletion>> {
        Box::pin(async move {
            let completion = self.inner.dispatch(action, deadline).await?;
            let routed_activation = action.receiver_route().is_some()
                && matches!(
                    action.kind(),
                    FleetActionKind::Movement {
                        action: MovementAction::Activate,
                        ..
                    }
                );
            if routed_activation && !self.lost.swap(true, std::sync::atomic::Ordering::AcqRel) {
                if !completion.committed
                    || !matches!(&completion.outcome.outcome, FleetOutcome::Activated(_))
                {
                    return Err(invalid(
                        "routed activation did not commit before reply loss",
                    ));
                }
                return Err(invalid("injected loss after committed routed activation"));
            }
            Ok(completion)
        })
    }

    fn inspect<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<cellule_runtime::fleet::operations::FleetInspectionObservation>>
    {
        self.inner.inspect(request, deadline)
    }
}
