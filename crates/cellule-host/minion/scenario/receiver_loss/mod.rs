//! Finite receiver loss and durable controller reconstruction.
use super::adapters::LocalFleet;
use super::*;
use cellule_host::fleet::{FleetActionAcceptance, FleetActionJournal, FleetTransport};
use cellule_runtime::fleet::operations::*;

pub(super) mod adapters;
mod movement;
#[cfg(test)]
mod tests;

struct ReleasedScenario {
    records: Arc<HashMap<CellId, Record>>,
    acknowledged: HashMap<CellId, Acknowledged>,
    attempt: MoveAttempt,
}

pub(super) fn run<'a>(
    root: &'a tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    nodes: &'a mut Vec<Arc<CellNode>>,
    boots: &'a mut Vec<startup::BootOwner>,
    profile: FleetProfile,
) -> FleetAdapterFuture<'a, ScenarioSummary> {
    Box::pin(run_inner(root, journal, nodes, boots, profile))
}

async fn run_inner(
    root: &tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    profile: FleetProfile,
) -> JournalResult<ScenarioSummary> {
    let (records, acknowledged) =
        initialize_inner(root, &journal, nodes, boots, 60_000, Some(1), false).await?;
    let fleet = Arc::new(LocalFleet {
        nodes: nodes.clone(),
        journal: journal.clone(),
        boots: boots.clone(),
        records: records.clone(),
        reader_verifier: None,
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
    let first = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await?;
    if !first.snapshot.head().attempts().is_empty() {
        return Err(invalid(
            "receiver-loss startup unexpectedly planned movement",
        ));
    }
    let page = nodes[0].runtime().fleet_cells_page(None, 128).await?;
    let Some(CellInventoryEntry::Owned(row)) = page.entries().first() else {
        return Err(invalid("receiver-loss source writer absent"));
    };
    let spec = MoveAttemptSpec {
        id: AttemptId {
            operation: OperationId::from_bytes([220; 16])?,
            sequence: 1,
        },
        target: row.target.clone(),
        incarnation: row.incarnation,
        source_node: node_id(0),
        source: session(0),
        generation: row.generation,
        source_epoch: row
            .position
            .as_ref()
            .ok_or_else(|| invalid("receiver-loss source has no position"))?
            .epoch,
        destination_node: node_id(1),
        destination: session(1),
        cost: row
            .cost
            .ok_or_else(|| invalid("receiver-loss source cost absent"))?,
        snapshot_digest: Digest::from_bytes([221; 32]),
        deadline_ms: clock()? + 60_000,
    };
    drop(page);
    journal
        .compare_exchange(
            &first.snapshot,
            first
                .snapshot
                .head()
                .controller()
                .ok_or_else(|| invalid("receiver-loss controller absent"))?
                .epoch,
            clock()?,
            &JournalTransition::Allocate(spec.clone()),
        )
        .await?;
    let version = journal.load_snapshot(scope()).await?.registry();
    journal.set_scheduling(version, false).await?;
    let prepared = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await?;
    if !prepared.failures.is_empty() {
        return Err(failure::endpoints(
            "receiver-loss prepare",
            0,
            prepared.failures,
        ));
    }
    if prepared
        .snapshot
        .head()
        .attempts()
        .first()
        .is_none_or(|attempt| attempt.phase() != AttemptPhase::Reserved)
    {
        return Err(invalid("receiver-loss preparation did not reserve"));
    }
    let release = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await?;
    if !release.failures.is_empty() {
        return Err(failure::endpoints(
            "receiver-loss release",
            0,
            release.failures,
        ));
    }
    if release.released != 1 {
        return Err(invalid("receiver-loss source release was not proved"));
    }
    let released_attempt = release
        .snapshot
        .head()
        .attempts()
        .first()
        .ok_or_else(|| invalid("receiver-loss release attempt absent"))?
        .clone();
    if released_attempt.released().is_none()
        || nodes[1].stats().local_disk_reserved_bytes() != spec.cost.disk_bytes
    {
        return Err(invalid("receiver-loss release or original credit differs"));
    }
    let observer = adapters::close_receiver(fleet.clone()).await?;
    movement::continue_released(
        root,
        journal,
        fleet,
        observer,
        ReleasedScenario {
            records,
            acknowledged,
            attempt: released_attempt,
        },
        profile,
    )
    .await
}
