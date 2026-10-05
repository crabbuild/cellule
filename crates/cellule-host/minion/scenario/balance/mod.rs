//! Caller-driven, real-time count convergence; no residence or lease clock shim.

use super::*;
use cellule_host::fleet::FleetRoster;

pub(super) async fn run(
    root: &tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    profile: FleetProfile,
) -> JournalResult<ScenarioSummary> {
    // This scenario spans real residence and several batches. Its newly issued
    // command receipts remain resolvable for five minutes; other profiles keep
    // their original one-minute receipts and timing bounds.
    let (records, acknowledged) = initialize(root, &journal, nodes, boots, 300_000).await?;
    let fleet = Arc::new(adapters::LocalFleet {
        nodes: nodes.clone(),
        boots: boots.clone(),
        records: records.clone(),
        reader_verifier: None,
        journal: journal.clone(),
        capture_sequence: std::sync::atomic::AtomicU64::new(0),
        lose_release_replies: false,
        lost_release_replies: std::sync::atomic::AtomicUsize::new(0),
        drop_closed_finalize_replies: std::sync::atomic::AtomicUsize::new(0),
        expired_receiver_cleanups: std::sync::atomic::AtomicUsize::new(0),
    });
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([206; 16]),
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
    let mut specs = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if Instant::now() >= deadline {
            return Err(std::io::Error::other(format!(
                "count convergence deadline: summary={summary:?} attempts={:?}",
                journal.load_snapshot(scope()).await?.head().attempts()
            ))
            .into());
        }
        let report = driver
            .reconcile_once(clock, deadline.min(Instant::now() + Duration::from_secs(5)))
            .await?;
        if let Some(failure) = report.failures.first() {
            return Err(Box::new(Arc::clone(&failure.error)) as JournalError);
        }
        for attempt in report.snapshot.head().attempts() {
            let spec = attempt.spec();
            if let Some(original) = specs.insert(spec.target.cell_id(), spec.clone())
                && original != *spec
            {
                return Err(invalid(
                    "count balancing repeated a Cell during its cooldown",
                ));
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
            .ok_or_else(|| invalid("count controller absent"))?
            .epoch;
        for blocker in report.blockers {
            if !summary.blockers.contains(&blocker) {
                summary.blockers.push(blocker);
            }
        }
        if summary.retired == 8 && report.snapshot.head().attempts().is_empty() {
            // Keep scheduling enabled while checking equilibrium. Stopping
            // first would make a no-oscillation assertion vacuous.
            let mut complete_samples = 0;
            while complete_samples < 2 {
                if Instant::now() >= deadline {
                    return Err(invalid(
                        "count equilibrium lacked two complete fresh samples",
                    ));
                }
                let stable = driver
                    .reconcile_once(clock, deadline.min(Instant::now() + Duration::from_secs(5)))
                    .await?;
                if stable.allocated != 0
                    || !stable.failures.is_empty()
                    || !stable.snapshot.head().attempts().is_empty()
                {
                    return Err(std::io::Error::other(format!(
                        "count equilibrium did not remain settled with scheduling enabled: {stable:?}"
                    )).into());
                }
                // Exact authority renewal or an actor transition can invalidate
                // an otherwise settled interval. The driver correctly refuses
                // count planning for it. Require two *consecutive* complete,
                // post-batch samples within the original convergence deadline;
                // any new allocation or native failure still fails immediately.
                if stable.blockers.iter().any(|blocker| {
                    matches!(
                        blocker,
                        cellule_runtime::fleet::operations::DrainBlocker::IncompleteObservation
                            | cellule_runtime::fleet::operations::DrainBlocker::StaleObservation
                    )
                }) {
                    complete_samples = 0;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                } else {
                    complete_samples += 1;
                }
            }
            let settled = journal.load_snapshot(scope()).await?;
            journal.set_scheduling(settled.registry(), false).await?;
            loop {
                if Instant::now() >= deadline {
                    return Err(invalid("final count coverage did not become complete"));
                }
                let snapshot = journal.load_snapshot(scope()).await?;
                let roster = FleetRoster::collect(journal.as_ref(), &snapshot, deadline).await?;
                if let Some(counts) =
                    observation::complete_counts(&fleet, &roster, deadline).await?
                {
                    summary.final_counts = counts;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            break;
        }
        // Each pass drives canonical signed boot renewal. Waiting for actual
        // residence never suspends the leases for a minute or invents samples.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if summary.final_counts != [4, 4, 4]
        || summary.released != 8
        || summary.activated != 8
        || summary.retired != 8
        || specs.len() != 8
        || summary.max_inflight > profile.max_inflight
        || summary.max_restore_bytes > profile.max_restore_bytes
    {
        return Err(
            std::io::Error::other(format!("count convergence differs: {summary:?}")).into(),
        );
    }
    for spec in specs.values() {
        verify_movement(&fleet, &records, &acknowledged, spec).await?;
        summary.receipt_checks += 1;
    }
    summary.receiver_nodes = specs
        .values()
        .map(|spec| spec.destination)
        .collect::<std::collections::HashSet<_>>()
        .len();
    Ok(summary)
}

#[cfg(test)]
mod tests;
