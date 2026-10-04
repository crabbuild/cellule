//! Adopt one exact release and retained routed result across a real lease expiry.
use super::*;
use adapters::{ClosedBootObserver, LoseRoutedActivationReply};
use cellule_host::fleet::FleetReconcileReport;

pub(super) fn continue_released<'a>(
    root: &'a tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    fleet: Arc<LocalFleet>,
    observer: Arc<ClosedBootObserver>,
    state: ReleasedScenario,
    profile: FleetProfile,
) -> FleetAdapterFuture<'a, ScenarioSummary> {
    Box::pin(continue_inner(
        root, journal, fleet, observer, state, profile,
    ))
}

async fn continue_inner(
    root: &tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    fleet: Arc<LocalFleet>,
    observer: Arc<ClosedBootObserver>,
    state: ReleasedScenario,
    profile: FleetProfile,
) -> JournalResult<ScenarioSummary> {
    let released = state
        .attempt
        .released()
        .ok_or_else(|| invalid("receiver-loss release evidence absent"))?;
    let loss = Arc::new(LoseRoutedActivationReply {
        inner: fleet.clone(),
        lost: std::sync::atomic::AtomicBool::new(false),
    });
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([206; 16]),
        profile,
        journal.clone(),
        observer.clone(),
        loss.clone(),
    )?;
    let mut summary = ScenarioSummary {
        released: 1,
        activated: 0,
        retired: 0,
        receipt_checks: 0,
        max_inflight: 1,
        max_restore_bytes: state.attempt.spec().cost.disk_bytes,
        joined_nodes: 0,
        boot_retirements: 0,
        receiver_nodes: 1,
        lost_release_replies: 0,
        controller_epoch: 1,
        expired_receiver_cleanups: 0,
        blockers: Vec::new(),
        final_counts: [0; 3],
        maintenance_completed: false,
        maintenance_boot_withdrawn: false,
        receiver_process_closures: 1,
        lost_activation_replies: 0,
        routed_activation_replays: 0,
    };
    let mut report = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await?;
    accumulate(&mut summary, &report, profile)?;
    for _ in 0..8 {
        if loss.lost.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
        report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await?;
        accumulate(&mut summary, &report, profile)?;
    }
    if !loss.lost.load(std::sync::atomic::Ordering::Acquire) {
        return Err(failure::endpoints(
            "receiver-loss replacement did not reach committed reply loss",
            8,
            report.failures,
        ));
    }
    let accepted = routed_acceptance(&journal, &state.attempt, MovementAction::Activate).await?;
    let replay = fleet
        .dispatch(accepted.action(), Instant::now() + Duration::from_secs(5))
        .await?;
    let FleetOutcome::Activated(ref activation) = replay.outcome.outcome else {
        return Err(invalid("receiver-loss replay did not confirm activation"));
    };
    if !replay.committed
        || fleet.nodes[2].stats().active_cells() != 1
        || activation.position.root != released.root
        || activation.position.epoch != released.epoch + 1
    {
        return Err(invalid(
            "receiver-loss replay changed root, epoch or actor count",
        ));
    }
    summary.lost_activation_replies = 1;
    summary.routed_activation_replays = 1;
    let expires = report
        .snapshot
        .head()
        .controller()
        .ok_or_else(|| invalid("receiver-loss controller lease absent"))?
        .expires_at_ms;
    let wait = u64::try_from(expires.saturating_sub(clock()?).max(0))?
        .checked_add(50)
        .ok_or_else(|| invalid("receiver-loss lease wait overflow"))?;
    tokio::time::sleep(Duration::from_millis(wait)).await;
    let reopened = Arc::new(
        SqliteJournal::open(
            root.path().join("fleet-journal.sqlite"),
            scope(),
            profile,
            clock()?,
        )
        .await?,
    );
    // Own this independently reconstructed client through every resume exit.
    // The outer scenario still owns and joins all native nodes and the original
    // journal before its private directory can be dropped.
    let result = async {
        let summary = Box::pin(resume(
            &reopened, &fleet, observer, &state, profile, report, summary,
        ))
        .await?;
        let before = reopened.load_snapshot(scope()).await?;
        let error = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(1))
            .await
            .err()
            .ok_or_else(|| invalid("receiver-loss old controller renewed successor lease"))?;
        if !matches!(
            &error,
            cellule_runtime::Error::Facility { name: "fleet-journal", source }
                if matches!(source.downcast_ref::<OperationError>(), Some(OperationError::Fenced))
        ) {
            return Err(error.into());
        }
        if reopened.load_snapshot(scope()).await? != before {
            return Err(invalid(
                "receiver-loss old controller changed successor journal",
            ));
        }
        Ok::<_, JournalError>(summary)
    }
    .await;
    let close = reopened.close().await;
    match result {
        Ok(summary) => {
            close?;
            Ok(summary)
        }
        Err(error) => {
            if let Err(cleanup) = close {
                eprintln!("additional receiver-loss journal cleanup failure: {cleanup:?}");
            }
            Err(error)
        }
    }
}

async fn resume(
    journal: &Arc<SqliteJournal>,
    fleet: &Arc<LocalFleet>,
    observer: Arc<ClosedBootObserver>,
    state: &ReleasedScenario,
    profile: FleetProfile,
    mut report: FleetReconcileReport,
    mut summary: ScenarioSummary,
) -> JournalResult<ScenarioSummary> {
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([207; 16]),
        profile,
        journal.clone(),
        observer,
        fleet.clone(),
    )?;
    let mut serving = None;
    for _ in 0..12 {
        if report.snapshot.head().attempts().is_empty() {
            break;
        }
        report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await?;
        accumulate(&mut summary, &report, profile)?;
        if let Some(attempt) = report.snapshot.head().attempts().first() {
            if attempt.spec() != state.attempt.spec()
                || attempt.released() != state.attempt.released()
            {
                return Err(invalid(
                    "receiver-loss reconstruction changed original release",
                ));
            }
            serving = serving.or_else(|| attempt.activated().cloned());
        }
    }
    if !report.snapshot.head().attempts().is_empty()
        || report.snapshot.head().reserved_restore_bytes() != 0
        || summary.released != 1
        || summary.activated != 1
        || summary.retired != 1
        || summary.controller_epoch != 2
    {
        return Err(failure::endpoints(
            "receiver-loss retained work did not settle",
            12,
            report.failures,
        ));
    }
    let serving = serving.ok_or_else(|| invalid("receiver-loss successor evidence absent"))?;
    let released = state
        .attempt
        .released()
        .ok_or_else(|| invalid("receiver-loss release disappeared"))?;
    if (serving.node, serving.session) != (node_id(2), session(2))
        || serving.position.root != released.root
        || serving.position.epoch != released.epoch + 1
    {
        return Err(invalid("receiver-loss successor differs from release"));
    }
    for effect in [MovementAction::Activate, MovementAction::Cancel] {
        routed_acceptance(journal, &state.attempt, effect).await?;
    }
    for (cell, record) in state.records.iter() {
        let original = state
            .acknowledged
            .get(cell)
            .ok_or_else(|| invalid("receiver-loss acknowledged Cell missing"))?;
        let observed = record
            .authority
            .load(*cell)
            .await?
            .ok_or_else(|| invalid("receiver-loss authority absent"))?;
        let owner = observed
            .value()
            .owner
            .as_ref()
            .ok_or_else(|| invalid("receiver-loss current owner absent"))?;
        let index = (0..3)
            .find(|index| session(*index) == owner.session)
            .ok_or_else(|| invalid("receiver-loss current owner unbound"))?;
        let handle = fleet.nodes[index]
            .runtime()
            .local_handle(record.catalog.clone(), &observed)
            .await?
            .ok_or_else(|| invalid("receiver-loss native actor absent"))?;
        if handle
            .resolve(original.identity, original.digest, clock()?, 64)
            .await?
            != Resolution::Committed(original.outcome.clone())
        {
            return Err(invalid("receiver-loss command receipt changed"));
        }
        let bytes = handle
            .query(64, 64, |connection| {
                let value: i64 =
                    connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await?;
        if bytes != original.value.to_be_bytes() {
            return Err(invalid("receiver-loss acknowledged state changed"));
        }
        summary.receipt_checks += 1;
    }
    let original = state
        .acknowledged
        .get(&state.attempt.spec().target.cell_id())
        .ok_or_else(|| invalid("receiver-loss original handle absent"))?;
    if original
        .source
        .query(64, 64, |_| Ok(Vec::new()))
        .await
        .is_ok()
    {
        return Err(invalid("receiver-loss released source still serves"));
    }
    let roster = cellule_host::fleet::FleetRoster::collect(
        journal.as_ref(),
        &report.snapshot,
        Instant::now() + Duration::from_secs(5),
    )
    .await?;
    summary.final_counts =
        observation::complete_counts(fleet, &roster, Instant::now() + Duration::from_secs(5))
            .await?
            .ok_or_else(|| invalid("receiver-loss final count coverage incomplete"))?;
    if summary.final_counts != [CELL_COUNT - 1, 0, 1] || summary.receipt_checks != CELL_COUNT {
        return Err(invalid("receiver-loss final placement differs"));
    }
    Ok(summary)
}

async fn routed_acceptance(
    journal: &SqliteJournal,
    attempt: &MoveAttempt,
    effect: MovementAction,
) -> JournalResult<AcceptedFleetAction> {
    let accepted = journal
        .load_movement_actions(scope(), attempt, effect)
        .await?
        .into_iter()
        .find_map(|value| match value {
            FleetActionAcceptance::New(accepted)
            | FleetActionAcceptance::Existing { accepted, .. }
                if accepted.action().receiver_endpoint() == Some((node_id(2), session(2))) =>
            {
                Some(accepted)
            }
            _ => None,
        })
        .ok_or_else(|| invalid("receiver-loss routed action absent"))?;
    if accepted
        .action()
        .receiver_route()
        .and_then(|route| route.latest_handoff())
        .is_none_or(|hop| hop.previous() != (node_id(1), session(1)))
    {
        return Err(invalid("receiver-loss closed handoff differs"));
    }
    Ok(accepted)
}

fn accumulate(
    summary: &mut ScenarioSummary,
    report: &FleetReconcileReport,
    profile: FleetProfile,
) -> JournalResult<()> {
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
        .ok_or_else(|| invalid("receiver-loss report has no controller"))?
        .epoch;
    for blocker in &report.blockers {
        if !summary.blockers.contains(blocker) {
            summary.blockers.push(*blocker);
        }
    }
    if summary.max_inflight > profile.max_inflight
        || summary.max_restore_bytes > profile.max_restore_bytes
    {
        return Err(invalid("receiver-loss exceeded shared movement budget"));
    }
    Ok(())
}
