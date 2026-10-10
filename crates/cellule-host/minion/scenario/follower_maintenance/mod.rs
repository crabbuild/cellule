//! Live-follower maintenance through the existing supervisor and public driver.
use super::*;
use cellule_host::{
    NodeLogRotationPhase,
    fleet::{
        FleetFollowerEvacuationJournal, FleetFollowerEvacuationPublication,
        FleetFollowerEvacuationVerifier, FleetRoster,
    },
};
use cellule_runtime::{
    fleet::operations::{
        EnrollmentRole, EnrollmentStatus, FollowerReplacementPolicy, MaintenancePhase,
    },
    follower::FollowerStore,
    node::NodeDirectory,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

mod provider;
mod readers;
mod service;
mod setup;
#[cfg(test)]
mod tests;

struct Inputs {
    records: Arc<HashMap<CellId, Record>>,
    acknowledged: Acknowledged,
    readers: Option<readers::Readers>,
    coverage_hold: provider::CoverageHold,
}

pub(super) enum Roles {
    Followers,
    ReadersAndFollowers,
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

pub(super) async fn run(
    root: &tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    profile: FleetProfile,
    roles: Roles,
) -> JournalResult<ScenarioSummary> {
    // These role-enabled futures contain native peer activation and complete
    // fleet capture. Their synchronous boxed constructors keep construction
    // temporaries out of the parent poll frame as well as its retained state.
    let inputs = setup::initialize(root, &journal, nodes, boots, roles).await?;
    let version = journal.load_snapshot(scope()).await?.registry();
    let page = journal.enrollments_page(version, None, 128).await?;
    let original = page
        .entries()
        .iter()
        .find(|row| {
            row.spec().target.node == node_id(1)
                && matches!(row.spec().role, EnrollmentRole::Follower { log_epoch: 1 })
        })
        .ok_or_else(|| invalid("original managed follower is missing"))?
        .clone();
    if original.status() != EnrollmentStatus::Established {
        return Err(invalid("original follower is not established"));
    }
    let fleet = Arc::new(adapters::LocalFleet {
        nodes: nodes.clone(),
        boots: boots.clone(),
        records: inputs.records.clone(),
        journal: journal.clone(),
        reader_verifier: inputs
            .readers
            .as_ref()
            .map(|readers| readers.verifier.clone()),
        capture_sequence: AtomicU64::new(0),
        lose_release_replies: false,
        lost_release_replies: AtomicUsize::new(0),
        drop_closed_finalize_replies: AtomicUsize::new(0),
        expired_receiver_cleanups: AtomicUsize::new(0),
    });
    let claimant = SessionId::from_bytes([206; 16]);
    let now = clock()?;
    let snapshot = journal.load_snapshot(scope()).await?;
    let claimed = journal
        .claim_controller(scope(), snapshot.head().revision(), claimant, now)
        .await?;
    let epoch = claimed
        .head()
        .controller()
        .ok_or_else(|| invalid("follower maintenance controller is absent"))?
        .epoch;
    let operation = MaintenanceOperation::new(
        OperationId::from_bytes([85; 16])?,
        Digest::from_bytes([86; 32]),
        node_id(1),
        session(1),
        2,
        now,
        now.checked_add(60_000)
            .ok_or_else(|| invalid("follower maintenance deadline overflow"))?,
    )?;
    journal
        .compare_exchange(
            &claimed,
            epoch,
            now,
            &JournalTransition::BeginMaintenance(operation.clone()),
        )
        .await?;
    let driver = FleetReconciler::new(
        scope(),
        claimant,
        profile,
        journal.clone(),
        fleet.clone(),
        fleet.clone(),
    )?;
    let wall_deadline = Instant::now() + Duration::from_secs(60);
    let mut blockers = Vec::new();
    // The driver owns cordon and evacuation transitions. No fixture writes a
    // ready-to-close row or an invented native role proof.
    loop {
        let report = Box::pin(driver.reconcile_once(clock, deadline())).await?;
        checked_report(&report)?;
        for blocker in report.blockers {
            if !blockers.contains(&blocker) {
                blockers.push(blocker);
            }
        }
        let phase = report
            .snapshot
            .head()
            .maintenance()
            .ok_or_else(|| invalid("follower maintenance operation disappeared"))?
            .phase();
        if phase == MaintenancePhase::Evacuating {
            break;
        }
        if Instant::now() >= wall_deadline {
            return Err(invalid("follower maintenance did not enter evacuation"));
        }
    }
    boots[1]
        .refresh_capacity(1, journal.as_ref(), deadline())
        .await?;
    if nodes[1].state() == NodeState::Stopped || !nodes[1].is_management_ready() {
        return Err(invalid("follower source stopped before replacement"));
    }
    // The missing replacement remains an explicit policy blocker and cannot
    // retire an uncovered original follower or authorize native Finalize.
    let blocked = Box::pin(driver.reconcile_once(clock, deadline())).await?;
    checked_report(&blocked)?;
    if blocked
        .snapshot
        .head()
        .maintenance()
        .map(MaintenanceOperation::phase)
        != Some(MaintenancePhase::Evacuating)
        || journal
            .load_enrollment(scope(), original.spec().key()?)
            .await?
            .as_ref()
            != Some(&original)
    {
        return Err(invalid("follower maintenance bypassed replacement policy"));
    }
    for blocker in blocked.blockers {
        if !blockers.contains(&blocker) {
            blockers.push(blocker);
        }
    }
    if let Some(readers) = &inputs.readers {
        readers.check_original(&journal, true).await?;
    }
    let store = nodes[1]
        .try_owned_component::<FollowerStore>(cellule_host::FOLLOWER_STORE_COMPONENT)?
        .ok_or_else(|| invalid("original follower store is missing"))?;
    if store.retained_bytes() == 0 || nodes[1].stats().active_cells() != 0 {
        return Err(invalid(
            "follower maintenance fixture lacks a foreign retained tail",
        ));
    }
    let donor = boots[1]
        .directory
        .load_if_live(session(1), clock()?)
        .await?
        .ok_or_else(|| invalid("original follower advertisement is missing"))?;
    if donor.advertisement().capacity().follower_retained_bytes != store.retained_bytes() {
        return Err(invalid(
            "follower advertisement does not report its original retained bytes",
        ));
    }
    // Enable the spare only after observing that the complete foreign obligation
    // blocks finalization despite the donor having zero local writers.
    boots[3]
        .refresh_capacity(3, journal.as_ref(), deadline())
        .await?;
    let selected = boots[0]
        .directory
        .select_log_members(session(0), 1, clock()?, 4)
        .await?;
    if selected != [node_id(2), node_id(3)] {
        return Err(invalid(
            "follower replacement selection includes the donor or lacks redundancy",
        ));
    }
    // This canonical actor barrier covers every acknowledged frame before the
    // existing supervisor fences each old member and closes the original epoch.
    inputs.coverage_hold.release();
    inputs.acknowledged.source.drain().await?;
    let request = nodes[0].request_node_log_rotation(1)?;
    let completion = tokio::time::timeout_at(deadline(), async {
        loop {
            let observed = request.observe()?;
            if observed.phase() == NodeLogRotationPhase::Completed {
                return observed
                    .completion()
                    .cloned()
                    .ok_or_else(|| invalid("follower rotation completion is missing"));
            }
            if observed.phase() == NodeLogRotationPhase::Interrupted {
                return match observed.latest_failure() {
                    Some(source) => Err(Box::new(RotationFailure(source.clone())) as JournalError),
                    None => Err(invalid("follower rotation was interrupted")),
                };
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    if completion.retirement().barrier().covered_through() == 0
        || completion.replacement_epoch() != 2
    {
        return Err(invalid(
            "follower rotation lacks original tail coverage or the newer epoch",
        ));
    }
    let renewed = service::resume(root, nodes, boots, &inputs).await?;
    let current = journal.load_snapshot(scope()).await?;
    let policy = FollowerReplacementPolicy::new(scope(), 1, 2)?;
    journal
        .set_follower_replacement_policy(&current, policy, clock()?)
        .await?;
    let current = journal.load_snapshot(scope()).await?;
    let operation = current
        .head()
        .maintenance()
        .ok_or_else(|| invalid("follower maintenance operation disappeared"))?;
    let capture = nodes[0]
        .follower_evacuation(&original, operation, 2, deadline())
        .await?;
    if !Arc::ptr_eq(capture.rotation(), &completion)
        || capture.retired_members().len() != 2
        || capture
            .retired_members()
            .iter()
            .any(|row| row.status() != EnrollmentStatus::Retired)
        || capture.replacements().len() != 2
        || capture
            .authority()
            .log()
            .is_none_or(|log| log.epoch() != 2 || log.members() != [node_id(2), node_id(3)])
    {
        return Err(invalid(
            "follower evacuation lacks the exact original and replacement ensembles",
        ));
    }
    let verifier = FleetFollowerEvacuationVerifier::new(
        boots[0].directory.clone(),
        Arc::new(adapters::LocalSnapshots::new(nodes.clone())),
    );
    let publication = FleetFollowerEvacuationPublication::publish(
        &capture,
        policy,
        journal.as_ref(),
        &verifier,
        deadline(),
        clock,
    )
    .await?;
    let record = publication.record()?;
    let reader_capture = if let Some(readers) = &inputs.readers {
        // A follower proof alone cannot close a donor that also owns a reader.
        let blocked = Box::pin(driver.reconcile_once(clock, deadline())).await?;
        checked_report(&blocked)?;
        readers
            .require_blocked(&blocked, &journal, &nodes[1])
            .await?;
        for blocker in blocked.blockers {
            if !blockers.contains(&blocker) {
                blockers.push(blocker);
            }
        }
        readers.prepare_replacement(&journal, boots).await?;
        let blocked = Box::pin(driver.reconcile_once(clock, deadline())).await?;
        checked_report(&blocked)?;
        readers
            .require_blocked(&blocked, &journal, &nodes[1])
            .await?;
        Some(readers.complete_replacement(&journal, boots).await?)
    } else {
        None
    };
    let completed = loop {
        let report = Box::pin(driver.reconcile_once(clock, deadline())).await?;
        checked_report(&report)?;
        for blocker in report.blockers {
            if !blockers.contains(&blocker) {
                blockers.push(blocker);
            }
        }
        if report.snapshot.head().maintenance().is_some_and(|current| {
            current.id() == operation.id() && current.phase() == MaintenancePhase::Completed
        }) {
            break report.snapshot;
        }
        if Instant::now() >= wall_deadline {
            return Err(invalid(
                "follower maintenance did not complete before its deadline",
            ));
        }
    };
    let evidence = completed
        .head()
        .maintenance()
        .and_then(MaintenanceOperation::drain_evidence)
        .ok_or_else(|| invalid("follower maintenance completion evidence is missing"))?;
    if !evidence.relocated
        || !evidence.readers_settled
        || !evidence.followers_settled
        || evidence.remaining_cells != 0
        || evidence.unresolved_attempts != 0
        || !completed.head().attempts().is_empty()
        || nodes[1].state() != NodeState::Stopped
        || !boots[1].directory.is_withdrawn(session(1)).await?
    {
        return Err(invalid(
            "follower maintenance completed without native shutdown and withdrawal",
        ));
    }
    for retired in capture.retired_members() {
        if journal
            .load_enrollment(scope(), retired.spec().key()?)
            .await?
            .as_ref()
            != Some(retired)
        {
            return Err(invalid("original follower retirement history changed"));
        }
    }
    if record.retired() != capture.retired_members() {
        return Err(invalid(
            "published follower policy lost original retirement history",
        ));
    }
    service::readback(root, &inputs, &renewed).await?;
    if let Some((readers, capture)) = inputs.readers.as_ref().zip(reader_capture.as_ref()) {
        readers.readback(&journal, capture).await?;
    }
    let roster = FleetRoster::collect(journal.as_ref(), &completed, deadline()).await?;
    let final_counts = observation::complete_counts(&fleet, &roster, deadline())
        .await?
        .ok_or_else(|| invalid("follower maintenance final observation is incomplete"))?;
    if final_counts != [1, 0, 0, 0] {
        return Err(invalid("follower maintenance changed writer ownership"));
    }
    Ok(ScenarioSummary {
        released: 0,
        activated: 0,
        retired: 0,
        receipt_checks: if inputs.readers.is_some() { 8 } else { 5 },
        max_inflight: 0,
        max_restore_bytes: 0,
        joined_nodes: 0,
        boot_retirements: 0,
        receiver_nodes: 2,
        lost_release_replies: 0,
        controller_epoch: completed
            .head()
            .controller()
            .ok_or_else(|| invalid("controller is absent"))?
            .epoch,
        expired_receiver_cleanups: 0,
        blockers,
        final_counts,
        maintenance_completed: true,
        maintenance_boot_withdrawn: true,
        receiver_process_closures: 0,
        lost_activation_replies: 0,
        routed_activation_replays: 0,
    })
}
fn checked_report(report: &cellule_host::fleet::FleetReconcileReport) -> JournalResult<()> {
    if let Some(failure) = report.failures.first() {
        return Err(Box::new(failure.error.clone()));
    }
    if let Some(failure) = &report.maintenance_failure {
        return Err(Box::new(failure.clone()));
    }
    Ok(())
}

// Preserve the supervisor's shared source error when this finite command exits.
#[derive(Debug)]
struct RotationFailure(Arc<dyn std::error::Error + Send + Sync>);
impl std::fmt::Display for RotationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
impl std::error::Error for RotationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
