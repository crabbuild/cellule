//! Fixed in-process failure adapters shared by the command and fault tests.
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetFailedBootProcessEvidence, FleetFailedBootProcessRequest,
    FleetFailedBootProcesses, FleetFailedBootRetirement, FleetObservation, FleetObserver,
    FleetRoster, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    EnrollmentStatus, FleetAction, FleetActionKind, FleetInspectionRequest, FleetOutcome,
    MovementAction,
};

/// Reference in-process provider: joined CellNode shutdown supplies retained
/// lifetime evidence for this finite constructor. It does not qualify OS
/// process supervision or replace an application's external-work provider.
#[derive(Clone)]
pub(in crate::scenario) struct StoppedNodeProcesses {
    pub(in crate::scenario) node: Arc<CellNode>,
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
                || stats.primitive_jobs() != 0
                || stats.hydration_jobs() != 0
                || stats.io_slots() != 0
                || stats.blocking_jobs() != 0
                || stats.recovery_jobs() != 0
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

pub(in crate::scenario) async fn close_receiver(
    fleet: Arc<LocalFleet>,
) -> JournalResult<Arc<ClosedBootObserver>> {
    let node = fleet
        .nodes
        .get(1)
        .ok_or_else(|| invalid("receiver-loss node absent"))?;
    let boot = fleet
        .boots
        .get(1)
        .ok_or_else(|| invalid("receiver-loss boot absent"))?;
    // This finite constructor omits normal host withdrawal on the failed
    // receiver. Join its runtime, then publish the independently checked typed
    // closure under its original enrollment; directory absence is insufficient.
    boot.guard
        .as_ref()
        .ok_or_else(|| invalid("receiver-loss guard absent"))?
        .fence();
    node.shutdown().await?;
    if node.state() != NodeState::Stopped {
        return Err(invalid("receiver-loss shutdown did not stop"));
    }
    let now = clock()?;
    let observed = boot
        .directory
        .load_if_live(session(1), now)
        .await?
        .ok_or_else(|| invalid("receiver-loss original advertisement absent"))?;
    boot.directory.withdraw_after_drain(&observed, now).await?;
    let snapshot = fleet.journal.load_snapshot(scope()).await?;
    let roster = FleetRoster::collect(
        fleet.journal.as_ref(),
        &snapshot,
        Instant::now() + Duration::from_secs(5),
    )
    .await?;
    let original = roster
        .enrollments()
        .iter()
        .find(|row| {
            row.spec().target.node == node_id(1)
                && row.spec().target.session == session(1)
                && row.status() == EnrollmentStatus::Established
        })
        .ok_or_else(|| invalid("receiver-loss original enrollment absent"))?;
    let processes = StoppedNodeProcesses { node: node.clone() };
    let retirement = FleetFailedBootRetirement::capture(
        fleet.journal.as_ref(),
        &boot.directory,
        &roster,
        original,
        session(0),
        Instant::now() + Duration::from_secs(5),
        clock,
    )
    .await?;
    let request = retirement.request().clone();
    retirement
        .publish(
            fleet.journal.as_ref(),
            &boot.directory,
            &processes,
            session(0),
            Instant::now() + Duration::from_secs(5),
            clock,
        )
        .await?
        .confirmed()?;
    Ok(Arc::new(ClosedBootObserver {
        fleet,
        request,
        processes,
    }))
}

pub(in crate::scenario) struct ClosedBootObserver {
    pub(in crate::scenario) fleet: Arc<LocalFleet>,
    pub(in crate::scenario) request: FleetFailedBootProcessRequest,
    pub(in crate::scenario) processes: StoppedNodeProcesses,
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

pub(in crate::scenario) struct LoseRoutedActivationReply {
    pub(in crate::scenario) inner: Arc<LocalFleet>,
    pub(in crate::scenario) lost: std::sync::atomic::AtomicBool,
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
