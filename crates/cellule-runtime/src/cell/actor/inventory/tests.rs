use super::*;
use crate::cell::worker::WorkerCellInventory;
use crate::fleet::operations::MAX_PAGE_BYTES;
use crate::primitives::maintenance::{PersistedWorkInventory, TransferWorkInventory};

fn retained(ledger: &ResourceLedger) -> ResourceReservation {
    ledger
        .try_reserve(ResourceCost::zero().with_retained_bytes(MAX_PAGE_BYTES as usize))
        .unwrap()
}

fn ledger() -> ResourceLedger {
    ResourceLedger::new(ResourceCost::zero().with_retained_bytes(2 * MAX_PAGE_BYTES as usize))
}

fn session() -> SessionId {
    SessionId::from_bytes([7; 16])
}

fn sample(at_ms: i64) -> WorkerCellInventory {
    WorkerCellInventory {
        persisted_work: PersistedWorkInventory::default(),
        transfer_work: TransferWorkInventory::default(),
        maintenance_work:
            crate::primitives::maintenance_readiness::MaintenanceWorkInventory::default(),
        database_bytes: 4096,
        commit_sequence: 1,
        observed_at_ms: at_ms,
    }
}

#[tokio::test]
async fn stale_actor_probe_cannot_clear_newer_mutation_markers_or_replace_newer_demand() {
    use crate::cell::catalog::{CatalogEntry, CellCatalog};
    use crate::control::Owner;
    use crate::identity::{ApplicationId, IncarnationId, NamespaceId, TenantId};
    use cellule_ltx::{CellReplica, CellStorageLayout};
    use object_store::{memory::InMemory, path::Path};

    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        NamespaceId::from_bytes([6; 16]),
        b"inventory-revision",
    )
    .unwrap();
    let layout = CellStorageLayout::new(
        cellule_store::Store::new(Arc::new(InMemory::new())),
        Path::from("root"),
        [3; 16],
    );
    let catalog = CellCatalog::new(layout.clone(), target.tenant())
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Application,
                Digest::from_bytes([5; 32]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([2; 16]);
    let control = authority
        .create_initial(
            &catalog,
            incarnation,
            Owner {
                session: session(),
                endpoint: "https://node.internal:8789".into(),
            },
        )
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let publisher = CellPublisher::new(
        CellReplica::new(
            layout,
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            cellule_ltx::Limits::default(),
        )
        .unwrap(),
        authority,
        control,
        root.path().to_owned(),
    );
    let connection = cellule_ltx::rusqlite::Connection::open_in_memory().unwrap();
    let now = std::time::Instant::now();
    let active = ActiveCell {
        generation: 1,
        admission: crate::cell::actor::admission::new_cell_admission(
            publisher.control().value().owner_fence(),
        ),
        incarnation,
        code: Digest::from_bytes([5; 32]),
        schema: 1,
        role: CatalogRole::Application,
        catalog,
        interrupt: Arc::new(connection.get_interrupt_handle()),
        durability_submitter: publisher.durability_submitter(),
        publisher: Some(publisher),
        publications: VecDeque::new(),
        publication_bytes: 0,
        unpublished_node_logs: 0,
        queue: VecDeque::new(),
        coordination: CoordinationState::serving_with_residency(true, Residency::Resident),
        persisted_work: PersistedWorkInventory::unknown(),
        demand: CellDemandState::default(),
        resource_limits: cellule_ltx::Limits::default(),
        inventory_refreshing: false,
        inventory_revision: 2,
        drain: None,
        transfer: None,
        resident_since_ms: 1,
        last_used_ms: 1,
        last_work_at: now,
        compaction_retry_at: now,
        hydration_retry_at: now,
        next_due_ms: None,
        published_sequence: 0,
    };
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    let cell = target.cell_id();
    let mut cells = HashMap::from([(cell, active)]);
    let mut transitioning = HashSet::new();
    let mut tasks = JoinSet::new();
    let mut shutdown = ShutdownState::default();
    let node_lease = RuntimeNodeLease::ObjectOnly;
    let unpublished = AtomicU64::new(0);
    let (publications, _) = tokio::sync::broadcast::channel(1);
    let mut movement = MovementBudget::with_requested_limit(2, 32, 1_000).unwrap();
    let mut permits = HashMap::new();
    // Each case completes the real actor task adapter, rather than testing only
    // a sample cache. The old revision must not clear newer unknown-work state.
    for (revision, result, known) in [
        (
            1,
            Ok(WorkerCellInventory {
                commit_sequence: 0,
                ..sample(1)
            }),
            false,
        ),
        (2, Ok(sample(101)), false), // An unpublished sequence is not valid inventory.
        (
            2,
            Ok(WorkerCellInventory {
                commit_sequence: 0,
                ..sample(201)
            }),
            true,
        ),
        (1, Err(Error::Fenced), true), // Older failed probes cannot destroy newer rows.
        (2, Err(Error::Fenced), false),
    ] {
        let active = cells.get_mut(&cell).unwrap();
        assert_eq!(
            active.coordination.step(CoordinationInput::BeginInventory {
                queue_empty: true,
                publication_idle: true,
                inventory_unknown: true,
                refreshing: false,
                lease_live: true,
            }),
            CoordinationDecision::Started
        );
        let effect_id = active.begin_task(CoordinationEffect::Inventory);
        active.inventory_refreshing = true;
        crate::cell::actor::tasks::handle_task(
            TaskResult::InventoryRefreshed {
                cell,
                generation: 1,
                effect_id,
                inventory_revision: revision,
                result,
            },
            &pool,
            &mut cells,
            &mut transitioning,
            &mut tasks,
            &mut shutdown,
            &node_lease,
            &unpublished,
            &publications,
            &mut movement,
            &mut permits,
        );
        let active = &cells[&cell];
        assert!(!active.inventory_refreshing);
        assert_eq!(!active.persisted_work.is_unknown(), known);
        assert_eq!(active.demand.fresh_sample(202, 0).is_some(), known);
        assert_eq!(active.demand.stable_observations(), u8::from(known));
    }
    assert!(tasks.is_empty());
    pool.shutdown().await.unwrap();
}

#[test]
fn demand_stability_counts_worker_measurements_not_page_reads_or_republished_time() {
    let mut state = CellDemandState::default();
    state
        .record(sample(1), cellule_ltx::Limits::default(), 1)
        .unwrap();
    assert_eq!(state.stable_observations(), 1);
    for _ in 0..10 {
        assert_eq!(state.fresh_sample(100, 1), Some(sample(1)));
        assert_eq!(state.stable_observations(), 1);
    }
    state
        .record(sample(1), cellule_ltx::Limits::default(), 1)
        .unwrap();
    assert_eq!(state.stable_observations(), 1);
    assert!(!state.should_refresh(100, false));
    assert!(state.should_refresh(101, false));
    state
        .record(sample(101), cellule_ltx::Limits::default(), 1)
        .unwrap();
    assert_eq!(state.stable_observations(), 2);
    assert!(!state.should_refresh(15_100, false));
    assert!(state.should_refresh(15_101, false));
    assert!(state.fresh_sample(100, 1).is_none());
    assert!(state.fresh_sample(30_101, 1).is_none());
    assert!(state.fresh_sample(102, 2).is_none());
}

#[test]
fn changed_mutated_unknown_or_regressing_demand_cannot_reuse_a_stable_sample() {
    let mut state = CellDemandState::default();
    state
        .record(sample(1), cellule_ltx::Limits::default(), 1)
        .unwrap();
    state
        .record(sample(101), cellule_ltx::Limits::default(), 1)
        .unwrap();
    let changed = WorkerCellInventory {
        database_bytes: 8192,
        ..sample(201)
    };
    state
        .record(changed, cellule_ltx::Limits::default(), 1)
        .unwrap();
    assert_eq!(state.stable_observations(), 1);
    state.clear();
    assert!(state.fresh_sample(202, 1).is_none());
    assert_eq!(state.stable_observations(), 0);
    assert!(
        state
            .record(sample(100), cellule_ltx::Limits::default(), 1)
            .is_err()
    );
    assert!(state.fresh_sample(203, 1).is_none());
    assert!(matches!(
        state.record(sample(301), cellule_ltx::Limits::default(), 2),
        Err(Error::PendingPublication)
    ));
    let unknown = WorkerCellInventory {
        persisted_work: PersistedWorkInventory::unknown(),
        ..sample(401)
    };
    assert!(
        state
            .record(unknown, cellule_ltx::Limits::default(), 1)
            .is_err()
    );
    assert_eq!(state.stable_observations(), 0);
    assert!(!state.should_refresh(402, false));
    assert!(state.should_refresh(1401, false));
}

#[test]
fn conservative_cost_covers_database_growth_and_rejects_unknown_or_overflowing_bounds() {
    let limits = cellule_ltx::Limits::default();
    let cost = demand::transfer_cost(limits, 4096).unwrap();
    assert_eq!(
        cost,
        demand::transfer_cost(limits, limits.max_database_bytes).unwrap()
    );
    assert_eq!(
        cost.disk_bytes,
        2 * limits.max_database_bytes + limits.max_plan_bytes + (64 << 20)
    );
    assert!(cost.memory_bytes >= super::super::ACTIVE_CELL_NATIVE_BYTES);
    assert!(cost.file_descriptors >= crate::cell::worker::ACTIVE_CELL_FILE_DESCRIPTORS as u32);
    assert_eq!(cost.job_credits, 1);
    assert!(demand::transfer_cost(limits, 0).is_err());
    assert!(demand::transfer_cost(limits, limits.max_database_bytes + 1).is_err());
    assert!(
        demand::transfer_cost(
            cellule_ltx::Limits {
                max_database_bytes: u64::MAX,
                ..limits
            },
            4096
        )
        .is_err()
    );
    assert!(
        demand::transfer_cost(
            cellule_ltx::Limits {
                max_plan_bytes: 0,
                ..limits
            },
            4096
        )
        .is_err()
    );
}

#[test]
fn pages_include_every_transition_once_in_sorted_order_and_own_memory_admission() {
    let cells = HashMap::new();
    let transitioning = (1..=255_u8)
        .rev()
        .map(|id| CellId::from_bytes([id; 32]))
        .collect();
    let ledger = ledger();
    let first = collect_page(
        &cells,
        &transitioning,
        255,
        session(),
        None,
        128,
        retained(&ledger),
    )
    .unwrap();
    assert_eq!(first.owned_cells(), 0);
    assert_eq!(first.transitioning_cells(), 255);
    assert_eq!(first.entries().len(), 128);
    assert_eq!(first.entries()[0].cell(), CellId::from_bytes([1; 32]));
    let cursor = first.next().unwrap();
    assert_eq!(
        CellInventoryCursor::from_bytes(&cursor.to_bytes()).unwrap(),
        cursor
    );
    let second = collect_page(
        &cells,
        &transitioning,
        255,
        session(),
        Some(cursor),
        128,
        retained(&ledger),
    )
    .unwrap();
    assert_eq!(second.entries().len(), 127);
    assert_eq!(second.entries()[0].cell(), CellId::from_bytes([129; 32]));
    assert_eq!(second.entries()[126].cell(), CellId::from_bytes([255; 32]));
    assert_eq!(second.topology(), first.topology());
    assert!(second.next().is_none());
    assert_eq!(
        ledger.snapshot().unwrap().used.retained_bytes(),
        2 * MAX_PAGE_BYTES as usize
    );
    drop(first);
    assert_eq!(
        ledger.snapshot().unwrap().used.retained_bytes(),
        MAX_PAGE_BYTES as usize
    );
    drop(second);
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
}

#[test]
fn cursor_refuses_membership_generation_and_runtime_replacement_without_leaking_bytes() {
    let cells = HashMap::new();
    let mut transitioning =
        HashSet::from([CellId::from_bytes([1; 32]), CellId::from_bytes([2; 32])]);
    let ledger = ledger();
    let first = collect_page(
        &cells,
        &transitioning,
        2,
        session(),
        None,
        1,
        retained(&ledger),
    )
    .unwrap();
    let cursor = first.next().unwrap();
    drop(first);
    for (generation, session) in [(3, session()), (2, SessionId::from_bytes([8; 16]))] {
        assert!(
            collect_page(
                &cells,
                &transitioning,
                generation,
                session,
                Some(cursor),
                1,
                retained(&ledger)
            )
            .is_err()
        );
        assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
    }
    transitioning.remove(&CellId::from_bytes([2; 32]));
    assert!(
        collect_page(
            &cells,
            &transitioning,
            2,
            session(),
            Some(cursor),
            1,
            retained(&ledger)
        )
        .is_err()
    );
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
}

#[test]
fn invalid_limits_and_cursor_keys_fail_closed_empty_inventory_finishes() {
    let ledger = ledger();
    let cells = HashMap::new();
    let transitioning = HashSet::new();
    for limit in [0, MAX_PAGE_ENTRIES + 1, usize::MAX] {
        assert!(
            collect_page(
                &cells,
                &transitioning,
                0,
                session(),
                None,
                limit,
                retained(&ledger)
            )
            .is_err()
        );
        assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
    }
    let page = collect_page(
        &cells,
        &transitioning,
        0,
        session(),
        None,
        1,
        retained(&ledger),
    )
    .unwrap();
    assert!(page.entries().is_empty());
    assert!(page.next().is_none());
    let cursor = CellInventoryCursor {
        topology: page.topology(),
        after: CellId::from_bytes([1; 32]),
    };
    drop(page);
    assert!(
        collect_page(
            &cells,
            &transitioning,
            0,
            session(),
            Some(cursor),
            1,
            retained(&ledger)
        )
        .is_err()
    );
    assert_eq!(ledger.snapshot().unwrap().used, ResourceCost::zero());
    for width in [0, 63, 65, 1024] {
        assert!(CellInventoryCursor::from_bytes(&vec![0; width]).is_err());
    }
}
