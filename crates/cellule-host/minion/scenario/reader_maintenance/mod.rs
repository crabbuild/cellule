//! Reader-only maintenance using managed boots and the public reconciler.
use super::*;
use cellule_host::{
    fleet::{FleetReaderEvacuationPublication, FleetReaderEvacuationVerifier, FleetRoster},
    read_replicas::ReadReplicaManager,
};
use cellule_runtime::{
    client::CellDescription,
    fleet::operations::{EnrollmentStatus, MaintenancePhase},
    node::NodeDirectory,
    peer::{PeerPrincipal, PeerReplicaResolver, PeerSigner, ReplicaPeerClient},
    read_policy::ReadPolicyStore,
};
use ed25519_dalek::SigningKey;
use std::sync::atomic::{AtomicU64, AtomicUsize};

mod setup;
#[cfg(test)]
mod tests;

struct Inputs {
    records: Arc<HashMap<CellId, Record>>,
    managers: Vec<ReadReplicaManager>,
    target: CellTarget,
    description: CellDescription,
    peer: ReplicaPeerClient,
    verifier: FleetReaderEvacuationVerifier,
    acknowledged: Acknowledged,
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(8)
}

pub(super) async fn run(
    root: &tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    profile: FleetProfile,
) -> JournalResult<ScenarioSummary> {
    let inputs = setup::initialize(root, &journal, nodes, boots).await?;
    let original_reader = inputs.managers[1].resolve(inputs.target.clone()).await?;
    let completion = inputs.managers[1]
        .enrollment_completion(inputs.target.cell_id())
        .await?
        .ok_or_else(|| invalid("reader enrollment completion is missing"))?;
    let original = journal
        .load_enrollment(scope(), completion.spec.key()?)
        .await?
        .ok_or_else(|| invalid("original managed reader is missing"))?;
    if original.status() != EnrollmentStatus::Established {
        return Err(invalid("original reader is not established"));
    }
    let fleet = Arc::new(adapters::LocalFleet {
        nodes: nodes.clone(),
        boots: boots.clone(),
        records: inputs.records.clone(),
        journal: journal.clone(),
        reader_verifier: Some(inputs.verifier.clone()),
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
        .ok_or_else(|| invalid("reader maintenance controller is absent"))?
        .epoch;
    let operation = MaintenanceOperation::new(
        OperationId::from_bytes([83; 16])?,
        Digest::from_bytes([84; 32]),
        node_id(1),
        session(1),
        2,
        now,
        now.checked_add(60_000)
            .ok_or_else(|| invalid("reader maintenance deadline overflow"))?,
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
        let report = driver.reconcile_once(clock, deadline()).await?;
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
            .ok_or_else(|| invalid("reader maintenance operation disappeared"))?
            .phase();
        if phase == MaintenancePhase::Evacuating {
            break;
        }
        if Instant::now() >= wall_deadline {
            return Err(invalid("reader maintenance did not enter evacuation"));
        }
    }
    boots[1]
        .refresh_capacity(1, journal.as_ref(), deadline())
        .await?;
    if nodes[1].state() == NodeState::Stopped || !nodes[1].is_management_ready() {
        return Err(invalid("reader source stopped before replacement"));
    }
    // The missing replacement remains an explicit policy blocker and cannot
    // erase the original ready reader or authorize native Finalize.
    let blocked = driver.reconcile_once(clock, deadline()).await?;
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
        return Err(invalid("reader maintenance bypassed replacement policy"));
    }
    for blocker in blocked.blockers {
        if !blockers.contains(&blocker) {
            blockers.push(blocker);
        }
    }
    if original_reader
        .query::<application::ReadValue>(None, 0)
        .await?
        .output
        != 29
    {
        return Err(invalid(
            "original reader lost acknowledged state before evacuation",
        ));
    }
    let spare = boots[2]
        .refresh_capacity(2, journal.as_ref(), deadline())
        .await?;
    // Reader discovery deliberately uses its bounded membership cache. Wait
    // for canonical selection to observe the signed cordon before opening a
    // replacement; a fresh heartbeat does not invalidate that read window.
    tokio::time::timeout_at(deadline(), async {
        loop {
            let selected = boots[0]
                .directory
                .select_readers(
                    inputs.target.cell_id(),
                    session(0),
                    inputs.description.code,
                    1,
                    clock()?,
                    128,
                )
                .await?;
            if selected.len() == 1 && selected[0].session() == session(2) {
                return Ok::<_, JournalError>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    inputs
        .peer
        .activate(
            &inputs.target,
            &boots[0].directory,
            spare,
            inputs.description,
        )
        .await?;
    let current = journal.load_snapshot(scope()).await?;
    let operation = current
        .head()
        .maintenance()
        .ok_or_else(|| invalid("reader maintenance operation disappeared"))?;
    let capture = inputs.managers[1]
        .evacuate(&original, operation, &inputs.peer, deadline())
        .await?;
    let publication = FleetReaderEvacuationPublication::publish(
        &capture,
        journal.as_ref(),
        &inputs.verifier,
        deadline(),
        clock,
    )
    .await?;
    let record = publication.record()?;
    let completed = loop {
        let report = driver.reconcile_once(clock, deadline()).await?;
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
                "reader maintenance did not complete before its deadline",
            ));
        }
    };
    let evidence = completed
        .head()
        .maintenance()
        .and_then(MaintenanceOperation::drain_evidence)
        .ok_or_else(|| invalid("reader maintenance completion evidence is missing"))?;
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
            "reader maintenance completed without native shutdown and withdrawal",
        ));
    }
    let retired = journal
        .load_enrollment(scope(), original.spec().key()?)
        .await?
        .ok_or_else(|| invalid("original reader history is missing"))?;
    if retired != *record.retired()
        || retired.status() != EnrollmentStatus::Retired
        || original_reader
            .query::<application::ReadValue>(None, 0)
            .await
            .is_ok()
    {
        return Err(invalid("original reader was not joined and retired"));
    }
    let replacement = inputs.managers[2].resolve(inputs.target.clone()).await?;
    if replacement
        .query::<application::ReadValue>(Some(capture.minimum()), 0)
        .await?
        .output
        != 29
    {
        return Err(invalid("replacement reader lost acknowledged state"));
    }
    // The original writer is unchanged; verify the durable request record too,
    // rather than equating successful reader queries with mutation durability.
    let acknowledged = &inputs.acknowledged;
    if acknowledged
        .source
        .resolve(acknowledged.identity, acknowledged.digest, clock()?, 64)
        .await?
        != Resolution::Committed(acknowledged.outcome.clone())
    {
        return Err(invalid(
            "reader evacuation changed the original mutation receipt",
        ));
    }
    let roster = FleetRoster::collect(journal.as_ref(), &completed, deadline()).await?;
    let final_counts = observation::complete_counts(&fleet, &roster, deadline())
        .await?
        .ok_or_else(|| invalid("reader maintenance final observation is incomplete"))?;
    if final_counts != [1, 0, 0] {
        return Err(invalid("reader maintenance changed writer ownership"));
    }
    Ok(ScenarioSummary {
        released: 0,
        activated: 0,
        retired: 0,
        receipt_checks: 2,
        max_inflight: 0,
        max_restore_bytes: 0,
        joined_nodes: 0,
        boot_retirements: 0,
        receiver_nodes: 1,
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
