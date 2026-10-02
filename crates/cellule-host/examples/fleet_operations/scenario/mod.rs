//! Finite real-node movement fixture shared by the executable and its tests.
//! Pressure is measured from an owned disk admission reservation. This is an
//! admission-load scenario, not physical disk throughput or provider qualification.

mod adapters;
mod application;
mod balance;
#[cfg(test)]
mod follower_tests;
mod observation;
#[cfg(test)]
mod reader_tests;
mod startup;
#[cfg(test)]
mod successor_tests;
#[cfg(test)]
mod tests;

use super::journal::{JournalError, JournalResult, SqliteJournal};
use cellule_host::fleet::{FleetEnrollmentJournal, FleetJournal, FleetReconciler};
use cellule_host::{CellNode, CellNodeBuilder, NodeState};
use cellule_runtime::{
    cell::{
        actor::{CellHandle, CellInventoryEntry},
        catalog::{CatalogEntry, CatalogProof, CatalogRole, CellCatalog},
        executor::{HandlerOutcome, MutationIdentity, Resolution, StoredOutcome},
        worker::SqlWorkerPool,
    },
    control::{Owner, authority::CellAuthority},
    fleet::operations::{FleetProfile, FleetScope, NodeIntent},
    identity::{
        ApplicationId, CellId, CellTarget, Digest, IncarnationId, NodeId, RequestId, SessionId,
        TenantId,
    },
    ltx::{CellReplica, CellStorageLayout, DiskBudget, Host, Limits},
    node::{NodePressure, lease::NodeLeaseGuard},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path as ObjectPath};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const CELL_COUNT: usize = 12;
fn scope() -> FleetScope {
    FleetScope {
        fleet: Digest::from_bytes([200; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    }
}
fn node_id(n: usize) -> NodeId {
    NodeId::from_bytes([n as u8 + 1; 16])
}
fn session(n: usize) -> SessionId {
    SessionId::from_bytes([n as u8 + 11; 16])
}
fn owner(n: usize) -> Owner {
    Owner {
        session: session(n),
        endpoint: format!("https://node-{n}.example:8789"),
    }
}
fn clock() -> cellule_runtime::Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| cellule_runtime::Error::Facility {
            name: "example-clock",
            source: Box::new(source),
        })?;
    i64::try_from(duration.as_millis()).map_err(|source| cellule_runtime::Error::Facility {
        name: "example-clock",
        source: Box::new(source),
    })
}
fn invalid(message: &'static str) -> JournalError {
    std::io::Error::other(message).into()
}

struct Record {
    target: CellTarget,
    incarnation: IncarnationId,
    catalog: CatalogProof,
    replica: CellReplica,
    authority: CellAuthority,
}
struct Acknowledged {
    identity: MutationIdentity,
    digest: Digest,
    outcome: StoredOutcome,
    value: i64,
    source: CellHandle,
}

struct SettlementContext<'a> {
    records: &'a HashMap<CellId, Record>,
    acknowledged: &'a HashMap<CellId, Acknowledged>,
    profile: FleetProfile,
    previous: Option<&'a FleetReconciler>,
}

#[derive(Debug)]
pub(super) struct ScenarioSummary {
    pub released: usize,
    pub activated: usize,
    pub retired: usize,
    pub receipt_checks: usize,
    pub max_inflight: usize,
    pub max_restore_bytes: u64,
    pub joined_nodes: usize,
    pub boot_retirements: usize,
    pub receiver_nodes: usize,
    pub lost_release_replies: usize,
    pub controller_epoch: u64,
    pub expired_receiver_cleanups: usize,
    pub blockers: Vec<cellule_runtime::fleet::operations::DrainBlocker>,
    pub final_counts: [usize; 3],
}

/// Owns the private directory until all runtime and journal jobs are joined.
pub(super) async fn overload() -> JournalResult<ScenarioSummary> {
    execute(false, false).await
}

pub(super) async fn controller_restart() -> JournalResult<ScenarioSummary> {
    execute(true, false).await
}

pub(super) async fn count_balance() -> JournalResult<ScenarioSummary> {
    execute(false, true).await
}

async fn execute(restart: bool, count_balance: bool) -> JournalResult<ScenarioSummary> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("fleet-journal.sqlite");
    let profile = if restart {
        FleetProfile {
            controller_lease_ms: 3_000,
            reconcile_interval_ms: 500,
            ..FleetProfile::default()
        }
    } else {
        FleetProfile::default()
    };
    let journal = Arc::new(SqliteJournal::open(path.clone(), scope(), profile, clock()?).await?);
    let mut nodes = Vec::new();
    let mut boots = Vec::new();
    let result = if count_balance {
        balance::run(&root, journal.clone(), &mut nodes, &mut boots, profile).await
    } else {
        run(
            &root,
            path,
            journal.clone(),
            &mut nodes,
            &mut boots,
            profile,
            restart,
        )
        .await
    };
    let mut cleanup_error = None;
    for node in &nodes {
        if let Err(error) = node.shutdown().await
            && cleanup_error.is_none()
        {
            cleanup_error = Some(Box::new(error) as JournalError);
        }
    }
    for boot in &boots {
        if let Err(error) = boot.withdraw(&journal).await
            && cleanup_error.is_none()
        {
            cleanup_error = Some(error);
        }
    }
    if result.is_ok() && cleanup_error.is_none() {
        let checked = async {
            let version = journal.load_snapshot(scope()).await?.registry();
            let page = journal.enrollments_page(version, None, 128).await?;
            if boots.len() != nodes.len()
                || page.next().is_some()
                || page.entries().len() != boots.len()
                || page.entries().iter().any(|entry| {
                    entry.status() != cellule_runtime::fleet::operations::EnrollmentStatus::Retired
                })
            {
                return Err(invalid("example boot registry still has obligations"));
            }
            Ok::<_, JournalError>(())
        }
        .await;
        if let Err(error) = checked {
            cleanup_error = Some(error);
        }
    }
    if let Err(error) = journal.close().await
        && cleanup_error.is_none()
    {
        cleanup_error = Some(error);
    }
    // Check all nodes even if an earlier shutdown failed; cleanup must never
    // return before joining an independent sibling's accepted work.
    for node in &nodes {
        let stats = node.stats();
        if (node.state() != NodeState::Stopped
            || stats.active_cells() != 0
            || stats.resident_bytes() != 0
            || stats.retained_bytes() != 0
            || stats.worker_jobs() != 0
            || stats.primitive_jobs() != 0
            || stats.hydration_jobs() != 0
            || stats.file_descriptors() != 0
            || stats.local_disk_reserved_bytes() != 0
            || stats.io_slots() != 0
            || stats.blocking_jobs() != 0
            || stats.recovery_jobs() != 0)
            && cleanup_error.is_none()
        {
            cleanup_error = Some(invalid("example shutdown left runtime resources"));
        }
    }
    let mut summary = match result {
        Ok(summary) => summary,
        Err(error) => {
            if let Some(cleanup) = cleanup_error {
                eprintln!("additional example cleanup failure: {cleanup:?}");
            }
            return Err(error);
        }
    };
    if let Some(error) = cleanup_error {
        return Err(error);
    }
    summary.joined_nodes = nodes.len();
    summary.boot_retirements = boots.len();
    Ok(summary)
}

/// The private reference profile provisions only catalog-backed SQL writers.
/// Register every boot before readiness; retain partial owners for exit cleanup.
async fn initialize(
    root: &tempfile::TempDir,
    journal: &Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    receipt_lifetime_ms: i64,
) -> JournalResult<(Arc<HashMap<CellId, Record>>, HashMap<CellId, Acknowledged>)> {
    let application = application::compile()?;
    let code = *application
        .registry()
        .module_digests()
        .first()
        .ok_or_else(|| invalid("example module absent"))?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("fleet-example-cells"),
        [3; 16],
    );
    let authority = CellAuthority::new(layout.clone());
    let limits = Limits {
        max_database_bytes: 64 << 20,
        max_capture_bytes: 16 << 20,
        ..Limits::default()
    };
    let mut records = HashMap::new();
    for n in 0..CELL_COUNT {
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            scope().application,
            application::NAMESPACE,
            &[n as u8],
        )?;
        let incarnation = IncarnationId::from_bytes([n as u8 + 101; 16]);
        let catalog = CellCatalog::new(layout.clone(), target.tenant())
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
            .await?;
        authority
            .create_initial(&catalog, incarnation, owner(0))
            .await?;
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            limits,
        )?;
        records.insert(
            target.cell_id(),
            Record {
                target,
                incarnation,
                catalog,
                replica,
                authority: authority.clone(),
            },
        );
    }
    let records = Arc::new(records);
    let directory = cellule_runtime::node::NodeDirectory::new(
        layout.clone(),
        scope().fleet,
        Digest::from_bytes([31; 32]),
        application.registry().release_digest(),
    );
    for index in 0..3 {
        let intent = journal
            .register_initial_intent(&NodeIntent::initial(
                scope(),
                node_id(index),
                session(index),
            )?)
            .await?;
        let node = Arc::new(
            CellNodeBuilder::new(application.clone())
                .with_runtime(
                    SqlWorkerPool::new(2, 32)?.with_native_memory_limit(128 << 20)?,
                    64 << 20,
                )
                .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
                .with_session(session(index))
                .with_fleet_startup_intent(intent.clone())
                .build()?,
        );
        // Register the runtime owner immediately so any later setup failure
        // still joins it before dropping private paths or the journal.
        nodes.push(node.clone());
        node.install_task_group(CancellationToken::new(), CancellationToken::new())?;
        node.install_fleet_actions(
            scope(),
            node_id(index),
            journal.clone(),
            Arc::new(adapters::Cells {
                records: records.clone(),
                local: index,
                root: root.path().into(),
            }),
        )?;
        let ad = startup::advertisement(index, &node, &intent).await?;
        let expires = ad.expires_at_ms();
        let spec = startup::spec(&intent)?;
        let boot_index = boots.len();
        boots.push(startup::BootOwner {
            node: node.clone(),
            directory: directory.clone(),
            spec: spec.clone(),
            advertisement: ad.clone(),
            guard: None,
        });
        let boot = startup::enroll(journal, &directory, &spec, ad, clock()?).await?;
        let guard = NodeLeaseGuard::new(clock()?, expires)?;
        node.install_node_lease_for_startup(guard.clone())?;
        boots[boot_index].guard = Some(guard);
        node.confirm_fleet_startup(journal.as_ref(), boot.spec().key()?)
            .await?;
        let observed = directory
            .load(session(index), clock()?)
            .await?
            .ok_or_else(|| invalid("example original boot is absent"))?;
        node.install_fleet_boot_withdrawal(directory.clone(), observed, boot, journal.clone())?;
        node.start()?;
    }
    let mut acknowledged = HashMap::new();
    // A reproducible, adversarial order makes the lowest Cell identities the
    // oldest local eviction candidates. The fleet must use actual actor demand
    // rather than succeed by chance on HashMap iteration order.
    let mut initial_cells = records.iter().collect::<Vec<_>>();
    initial_cells.sort_by_key(|(cell, _)| *cell.as_bytes());
    for (cell, record) in initial_cells {
        let initial = record
            .authority
            .load(*cell)
            .await?
            .ok_or_else(|| invalid("example initial authority absent"))?;
        let handle = nodes[0]
            .runtime()
            .bootstrap(
                record.catalog.clone(),
                record.replica.clone(),
                record.authority.clone(),
                initial,
                root.path().join(format!("source-{cell:?}.sqlite")),
                |tx| {
                    tx.execute_batch(
                        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (0)",
                    )?;
                    Ok(())
                },
            )
            .await?;
        let now = clock()?;
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes(*record.incarnation.as_bytes()),
            issued_at_ms: now,
            expires_at_ms: now
                .checked_add(receipt_lifetime_ms)
                .ok_or_else(|| invalid("example receipt lifetime overflow"))?,
        };
        let digest = Digest::from_bytes([record.incarnation.as_bytes()[0]; 32]);
        let value = i64::from(record.incarnation.as_bytes()[0]);
        let outcome = handle
            .execute(identity, digest, now, 64, 64, move |tx| {
                tx.execute("UPDATE counter SET value = ?1", [value])?;
                Ok(HandlerOutcome::Success(value.to_be_bytes().to_vec()))
            })
            .await?;
        acknowledged.insert(
            *cell,
            Acknowledged {
                identity,
                digest,
                outcome,
                value,
                source: handle,
            },
        );
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let page = nodes[0].runtime().fleet_cells_page(None,128).await?;
            if page.entries().len() == CELL_COUNT && page.entries().iter().all(|entry| matches!(entry, CellInventoryEntry::Owned(row) if row.cost.is_some() && row.stable_observations == 2 && row.blockers.is_empty())) { return Ok::<_, JournalError>(()); }
            drop(page); tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    let version = journal.load_snapshot(scope()).await?.registry();
    let version = journal.bootstrap_registry(version).await?;
    journal.set_scheduling(version, true).await?;
    Ok((records, acknowledged))
}

async fn run(
    root: &tempfile::TempDir,
    path: PathBuf,
    journal: Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
    profile: FleetProfile,
    restart: bool,
) -> JournalResult<ScenarioSummary> {
    let (records, acknowledged) = initialize(root, &journal, nodes, boots, 60_000).await?;
    let fleet = Arc::new(adapters::LocalFleet {
        nodes: nodes.clone(),
        journal: journal.clone(),
        boots: boots.clone(),
        records: records.clone(),
        capture_sequence: std::sync::atomic::AtomicU64::new(0),
        lose_release_replies: restart,
        lost_release_replies: std::sync::atomic::AtomicUsize::new(0),
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
    // Hold a real admission token; the actor's own 250-ms ledger samples and
    // 1000-ms dwell classify this load. No sample time or tier is fabricated.
    let pressure = nodes[0]
        .runtime()
        .local_disk_budget()
        .try_reserve(7 << 30)?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0]
                .runtime()
                .operational_sample()?
                .is_some_and(|sample| sample.pressure == NodePressure::Shedding)
            {
                return Ok::<_, JournalError>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    let first = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await?;
    drop(pressure);
    if first.allocated != 2 {
        return Err(std::io::Error::other(format!(
            "measured overload did not allocate the bounded two-move batch: report={first:?} current_source_sample={:?}",
            nodes[0].runtime().operational_sample()?
        )).into());
    }
    let specs = first
        .snapshot
        .head()
        .attempts()
        .iter()
        .map(|attempt| attempt.spec().clone())
        .collect::<Vec<_>>();
    journal
        .set_scheduling(first.snapshot.registry(), false)
        .await?;
    let prepared = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await?;
    if prepared.dispatched != 2 || !prepared.failures.is_empty() {
        return Err(invalid("real receivers did not prepare"));
    }
    if restart {
        let lost = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await?;
        if lost.dispatched != 2
            || lost.failures.len() != 2
            || lost.released != 0
            || fleet
                .lost_release_replies
                .load(std::sync::atomic::Ordering::SeqCst)
                != 2
            || lost.snapshot.head().attempts().len() != 2
        {
            return Err(std::io::Error::other(format!(
                "reply loss did not retain both unconfirmed releases: {lost:?}; lost replies={}",
                fleet
                    .lost_release_replies
                    .load(std::sync::atomic::Ordering::SeqCst)
            ))
            .into());
        }
        let expires = lost
            .snapshot
            .head()
            .controller()
            .ok_or_else(|| invalid("controller lease absent"))?
            .expires_at_ms;
        // Wait on real time. Neither the node lease nor reservation evidence is
        // restamped; the successor must settle expired prepared credit.
        tokio::time::timeout(Duration::from_secs(5), async {
            while clock()? < expires {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, JournalError>(())
        })
        .await??;
    }
    // Reopen an independent controller client over the same durable file while
    // node effects retain their original journal client and accepted envelopes.
    let reopened = Arc::new(SqliteJournal::open(path, scope(), profile, clock()?).await?);
    let mut blockers = first.blockers;
    for blocker in prepared.blockers {
        if !blockers.contains(&blocker) {
            blockers.push(blocker);
        }
    }
    let result = settle(
        reopened.clone(),
        fleet,
        specs,
        blockers,
        SettlementContext {
            records: &records,
            acknowledged: &acknowledged,
            profile,
            previous: restart.then_some(&driver),
        },
    )
    .await;
    let closed = reopened.close().await;
    let summary = result?;
    closed?;
    Ok(summary)
}

async fn settle(
    journal: Arc<SqliteJournal>,
    fleet: Arc<adapters::LocalFleet>,
    specs: Vec<cellule_runtime::fleet::operations::MoveAttemptSpec>,
    blockers: Vec<cellule_runtime::fleet::operations::DrainBlocker>,
    context: SettlementContext<'_>,
) -> JournalResult<ScenarioSummary> {
    let SettlementContext {
        records,
        acknowledged,
        profile,
        previous,
    } = context;
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([if previous.is_some() { 207 } else { 206 }; 16]),
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
        max_inflight: specs.len(),
        max_restore_bytes: specs.iter().map(|spec| spec.cost.disk_bytes).sum(),
        joined_nodes: 0,
        boot_retirements: 0,
        receiver_nodes: specs
            .iter()
            .map(|spec| spec.destination)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        lost_release_replies: 0,
        controller_epoch: 0,
        expired_receiver_cleanups: 0,
        blockers,
        final_counts: [0; 3],
    };
    let mut passes = Vec::new();
    for pass in 0..12 {
        let report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await?;
        if !report.failures.is_empty() {
            return Err(invalid("real movement endpoint failed"));
        }
        summary.controller_epoch = report
            .snapshot
            .head()
            .controller()
            .ok_or_else(|| invalid("controller lease absent"))?
            .epoch;
        if pass == 0
            && let Some(previous) = previous
        {
            let before = journal.load_snapshot(scope()).await?;
            let error = previous
                .reconcile_once(clock, Instant::now() + Duration::from_secs(1))
                .await
                .err()
                .ok_or_else(|| invalid("old controller renewed successor lease"))?;
            if !matches!(&error, cellule_runtime::Error::Facility { name: "fleet-journal", source } if matches!(source.downcast_ref::<cellule_runtime::fleet::operations::OperationError>(), Some(cellule_runtime::fleet::operations::OperationError::Fenced)))
            {
                return Err(error.into());
            }
            if summary.controller_epoch != 2 || journal.load_snapshot(scope()).await? != before {
                return Err(invalid("old controller was not fenced after replacement"));
            }
        }
        passes.push(format!("pass={pass} dispatched={} inspected={} released={} activated={} retired={} cancelled={} attempts={:?}", report.dispatched, report.inspected, report.released, report.activated, report.retired, report.cancelled, report.snapshot.head().attempts()));
        for blocker in report.blockers {
            if !summary.blockers.contains(&blocker) {
                summary.blockers.push(blocker);
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
        if report.snapshot.head().attempts().is_empty() {
            break;
        }
    }
    summary.lost_release_replies = fleet
        .lost_release_replies
        .load(std::sync::atomic::Ordering::SeqCst);
    summary.expired_receiver_cleanups = fleet
        .expired_receiver_cleanups
        .load(std::sync::atomic::Ordering::SeqCst);
    if previous.is_some()
        && (summary.lost_release_replies != 2 || summary.expired_receiver_cleanups != 2)
    {
        return Err(invalid(
            "controller restart did not settle both original reservations",
        ));
    }
    if summary.receiver_nodes != 2
        || summary.released != 2
        || summary.activated != 2
        || summary.retired != 2
        || !journal
            .load_snapshot(scope())
            .await?
            .head()
            .attempts()
            .is_empty()
    {
        let retained = journal.load_snapshot(scope()).await?;
        return Err(std::io::Error::other(format!("real movement did not settle both attempts: summary={summary:?} retained={:?} passes={passes:?}", retained.head().attempts())).into());
    }
    for spec in &specs {
        verify_movement(&fleet, records, acknowledged, spec).await?;
        summary.receipt_checks += 1;
    }
    Ok(summary)
}

async fn verify_movement(
    fleet: &adapters::LocalFleet,
    records: &HashMap<CellId, Record>,
    acknowledged: &HashMap<CellId, Acknowledged>,
    spec: &cellule_runtime::fleet::operations::MoveAttemptSpec,
) -> JournalResult<()> {
    let record = records
        .get(&spec.target.cell_id())
        .ok_or_else(|| invalid("missing readback record"))?;
    let receipt = acknowledged
        .get(&spec.target.cell_id())
        .ok_or_else(|| invalid("missing original acknowledgment"))?;
    let node = fleet
        .nodes
        .iter()
        .enumerate()
        .find(|(index, _)| session(*index) == spec.destination)
        .map(|(_, node)| node)
        .ok_or_else(|| invalid("missing receiver"))?;
    let current = record
        .authority
        .load(spec.target.cell_id())
        .await?
        .ok_or_else(|| invalid("receiver authority absent"))?;
    // Restored serving actors may still be hydrating. This lookup reads the
    // existing actor against authority; it cannot create another writer.
    let handle = node
        .runtime()
        .local_handle(record.catalog.clone(), &current)
        .await?
        .ok_or_else(|| invalid("receiver has no serving actor"))?;
    if handle
        .resolve(receipt.identity, receipt.digest, clock()?, 64)
        .await?
        != Resolution::Committed(receipt.outcome.clone())
    {
        return Err(invalid("original command receipt did not survive movement"));
    }
    let value = handle
        .query(64, 64, |db| {
            Ok(db
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .to_be_bytes()
                .to_vec())
        })
        .await?;
    if value != receipt.value.to_be_bytes() {
        return Err(invalid("restored Cell readback differs"));
    }
    let current = record
        .authority
        .load(spec.target.cell_id())
        .await?
        .ok_or_else(|| invalid("receiver authority absent"))?;
    if current
        .value()
        .owner
        .as_ref()
        .is_none_or(|owner| owner.session != spec.destination)
        || current.value().epoch <= spec.source_epoch
    {
        return Err(invalid("receiver lacks successor authority"));
    }
    if !matches!(
        receipt.source.query(64, 64, |_| Ok(Vec::new())).await,
        Err(cellule_runtime::Error::Fenced
            | cellule_runtime::Error::CellDraining
            | cellule_runtime::Error::CellNotActive)
    ) {
        return Err(invalid("old source handle still served after movement"));
    }
    Ok(())
}
