//! Planned maintenance through the canonical driver, with optional busy SQL admission.
use super::*;

#[cfg(test)]
mod tests;
mod traffic;

pub(super) async fn run(
    root: &tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    profile: FleetProfile,
    busy: bool,
) -> JournalResult<ScenarioSummary> {
    let (records, acknowledged) = initialize(root, &journal, nodes, boots, 300_000).await?;
    let mut traffic = if busy {
        Some(traffic::Traffic::start(&acknowledged)?)
    } else {
        None
    };
    let result = async {
        if let Some(traffic) = &mut traffic {
            traffic.started().await?;
        }
        Box::pin(run_nodes(
            journal,
            nodes,
            boots,
            profile,
            records.clone(),
            &acknowledged,
        ))
        .await
    }
    .await;
    // Stop new offering on every exit and join all accepted client calls before
    // the shared node cleanup can close the journal or temporary databases.
    let joined = match traffic {
        Some(traffic) => Some(traffic.finish().await),
        None => None,
    };
    let mut summary = match result {
        Ok(summary) => summary,
        Err(error) => {
            if let Some(Err(client_error)) = joined {
                eprintln!("additional joined maintenance client failure: {client_error:?}");
            }
            return Err(error);
        }
    };
    if let Some(joined) = joined {
        summary.receipt_checks += joined?.verify(nodes, &records).await?;
    }
    Ok(summary)
}

async fn run_nodes(
    journal: Arc<SqliteJournal>,
    nodes: &[Arc<CellNode>],
    boots: &[startup::BootOwner],
    profile: FleetProfile,
    records: Arc<HashMap<CellId, Record>>,
    acknowledged: &HashMap<CellId, Acknowledged>,
) -> JournalResult<ScenarioSummary> {
    use cellule_host::fleet::FleetRoster;
    use cellule_runtime::fleet::operations::{EnrollmentStatus, MaintenancePhase};
    let fleet = Arc::new(adapters::LocalFleet {
        nodes: nodes.to_vec(),
        journal: journal.clone(),
        boots: boots.to_vec(),
        records: records.clone(),
        reader_verifier: None,
        capture_sequence: std::sync::atomic::AtomicU64::new(0),
        lose_release_replies: false,
        lost_release_replies: std::sync::atomic::AtomicUsize::new(0),
        drop_closed_finalize_replies: std::sync::atomic::AtomicUsize::new(0),
        expired_receiver_cleanups: std::sync::atomic::AtomicUsize::new(0),
    });
    let claimant = SessionId::from_bytes([206; 16]);
    let now_ms = clock()?;
    let initial = journal.load_snapshot(scope()).await?;
    let claimed = journal
        .claim_controller(scope(), initial.head().revision(), claimant, now_ms)
        .await?;
    let operation = MaintenanceOperation::new(
        OperationId::from_bytes([222; 16])?,
        Digest::from_bytes([223; 32]),
        node_id(0),
        session(0),
        2,
        now_ms,
        now_ms
            .checked_add(180_000)
            .ok_or_else(|| invalid("maintenance deadline overflow"))?,
    )?;
    let epoch = claimed
        .head()
        .controller()
        .ok_or_else(|| invalid("maintenance controller lease absent"))?
        .epoch;
    journal
        .compare_exchange(
            &claimed,
            epoch,
            now_ms,
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
    let mut summary = ScenarioSummary {
        released: 0,
        activated: 0,
        retired: 0,
        receipt_checks: 0,
        max_inflight: 0,
        max_restore_bytes: 0,
        joined_nodes: 0,
        boot_retirements: 0,
        receiver_nodes: 0,
        lost_release_replies: 0,
        controller_epoch: 0,
        expired_receiver_cleanups: 0,
        blockers: Vec::new(),
        final_counts: vec![0; 3],
        maintenance_completed: false,
        maintenance_boot_withdrawn: false,
        receiver_process_closures: 0,
        lost_activation_replies: 0,
        routed_activation_replays: 0,
    };
    let wall_deadline = Instant::now() + Duration::from_secs(180);
    let mut specs = HashMap::new();
    let mut completed_snapshot = None;
    while Instant::now() < wall_deadline {
        let report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(8))
            .await?;
        if let Some(failure) = report.failures.first() {
            return Err(Box::new(Arc::clone(&failure.error)) as JournalError);
        }
        if let Some(failure) = &report.maintenance_failure {
            return Err(Box::new(Arc::clone(failure)) as JournalError);
        }
        for attempt in report.snapshot.head().attempts() {
            let spec = attempt.spec();
            if let Some(original) = specs.insert(spec.target.cell_id(), spec.clone())
                && original != *spec
            {
                return Err(invalid("maintenance moved a Cell more than once"));
            }
        }
        summary.released += report.released;
        summary.activated += report.activated;
        summary.retired += report.retired;
        summary.max_inflight = summary
            .max_inflight
            .max(report.snapshot.head().attempts().len());
        summary.max_restore_bytes = summary
            .max_restore_bytes
            .max(report.snapshot.head().reserved_restore_bytes());
        summary.controller_epoch = report
            .snapshot
            .head()
            .controller()
            .ok_or_else(|| invalid("maintenance controller lease absent"))?
            .epoch;
        for blocker in report.blockers {
            if !summary.blockers.contains(&blocker) {
                summary.blockers.push(blocker);
            }
        }
        if report.snapshot.head().maintenance().is_some_and(|current| {
            current.id() == operation.id() && current.phase() == MaintenancePhase::Completed
        }) {
            completed_snapshot = Some(report.snapshot);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let snapshot = match completed_snapshot {
        Some(snapshot) => snapshot,
        None => {
            let retained = journal.load_snapshot(scope()).await?;
            return Err(std::io::Error::other(format!(
                "maintenance did not complete before its deadline: operation={:?} phase={:?} attempts={:?} blockers={:?}",
                operation.id(),
                retained.head().maintenance().map(MaintenanceOperation::phase),
                retained.head().attempts(),
                summary.blockers,
            )).into());
        }
    };
    let current = snapshot
        .head()
        .maintenance()
        .ok_or_else(|| invalid("completed maintenance operation is absent"))?;
    let evidence = current
        .drain_evidence()
        .ok_or_else(|| invalid("completed maintenance evidence is absent"))?;
    if current.id() != operation.id()
        || current.phase() != MaintenancePhase::Completed
        || !snapshot.head().attempts().is_empty()
        || evidence.remaining_cells != 0
        || evidence.unresolved_attempts != 0
        || !evidence.relocated
        || !evidence.readers_settled
        || !evidence.followers_settled
        || nodes[0].state() != NodeState::Stopped
        || nodes[0].stats().active_cells() != 0
    {
        return Err(invalid(
            "maintenance completed without the full drain barrier",
        ));
    }
    let boot = journal
        .load_enrollment(scope(), boots[0].spec.key()?)
        .await?
        .ok_or_else(|| invalid("maintenance boot enrollment is absent"))?;
    if boot.status() != EnrollmentStatus::Retired
        || !boots[0].directory.is_withdrawn(session(0)).await?
    {
        return Err(invalid("maintenance stopped without exact boot withdrawal"));
    }
    summary.maintenance_completed = true;
    summary.maintenance_boot_withdrawn = true;
    summary.lost_release_replies = fleet
        .lost_release_replies
        .load(std::sync::atomic::Ordering::SeqCst);
    summary.expired_receiver_cleanups = fleet
        .expired_receiver_cleanups
        .load(std::sync::atomic::Ordering::SeqCst);
    if specs.len() != CELL_COUNT
        || summary.released != CELL_COUNT
        || summary.activated != CELL_COUNT
        || summary.retired != CELL_COUNT
    {
        return Err(std::io::Error::other(format!(
            "maintenance did not relocate every Cell: summary={summary:?} specs={}",
            specs.len()
        ))
        .into());
    }
    let mut destinations = std::collections::HashSet::new();
    for spec in specs.values() {
        destinations.insert(spec.destination);
        verify_movement(&fleet, &records, acknowledged, spec).await?;
        summary.receipt_checks += 1;
    }
    summary.receiver_nodes = destinations.len();
    if summary.receiver_nodes != 2 {
        return Err(invalid("maintenance did not use both eligible receivers"));
    }
    let roster = FleetRoster::collect(
        journal.as_ref(),
        &snapshot,
        Instant::now() + Duration::from_secs(8),
    )
    .await?;
    summary.final_counts =
        observation::complete_counts(&fleet, &roster, Instant::now() + Duration::from_secs(8))
            .await?
            .ok_or_else(|| invalid("post-maintenance observation is incomplete"))?;
    if summary.final_counts[0] != 0 || summary.final_counts.iter().sum::<usize>() != CELL_COUNT {
        return Err(std::io::Error::other(format!(
            "maintenance left Cells on the stopped node or lost inventory: counts={:?}",
            summary.final_counts
        ))
        .into());
    }
    Ok(summary)
}
