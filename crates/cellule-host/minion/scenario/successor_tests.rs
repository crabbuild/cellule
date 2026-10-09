use super::*;
use cellule_host::fleet::FleetActionJournal;
use cellule_runtime::fleet::operations::*;
mod routed;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_adopts_ordinary_winner_and_joins_original_receiver_before_retirement() {
    let root = tempfile::tempdir().unwrap();
    let profile = FleetProfile::default();
    let journal = Arc::new(
        SqliteJournal::open(
            root.path().join("successor-journal.sqlite"),
            scope(),
            profile,
            clock().unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut nodes = Vec::new();
    let mut boots = Vec::new();
    let (records, acknowledged) = initialize(&root, &journal, &mut nodes, &mut boots, 60_000)
        .await
        .unwrap();
    let fleet = Arc::new(adapters::LocalFleet {
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
        fleet,
    )
    .unwrap();
    let first = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(first.snapshot.head().attempts().is_empty());
    let page = nodes[0]
        .runtime()
        .fleet_cells_page(None, 128)
        .await
        .unwrap();
    let CellInventoryEntry::Owned(row) = &page.entries()[0] else {
        panic!("fixture writer missing")
    };
    let spec = MoveAttemptSpec {
        id: AttemptId {
            operation: OperationId::from_bytes([210; 16]).unwrap(),
            sequence: 1,
        },
        target: row.target.clone(),
        incarnation: row.incarnation,
        source_node: node_id(0),
        source: session(0),
        generation: row.generation,
        source_epoch: row.position.as_ref().unwrap().epoch,
        destination_node: node_id(1),
        destination: session(1),
        cost: row.cost.unwrap(),
        snapshot_digest: Digest::from_bytes([211; 32]),
        deadline_ms: clock().unwrap() + 60_000,
    };
    journal
        .compare_exchange(
            &first.snapshot,
            first.snapshot.head().controller().unwrap().epoch,
            clock().unwrap(),
            &JournalTransition::Allocate(spec.clone()),
        )
        .await
        .unwrap();
    let version = journal.load_snapshot(scope()).await.unwrap().registry();
    journal.set_scheduling(version, false).await.unwrap();
    drop(page);
    let preparing = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        preparing.snapshot.head().attempts()[0].phase(),
        AttemptPhase::Reserved
    );
    let release = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(release.released, 1);
    let released = release.snapshot.head().attempts()[0]
        .released()
        .unwrap()
        .clone();
    let record = &records[&spec.target.cell_id()];
    let idle = record
        .authority
        .load(spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let successor = nodes[2]
        .runtime()
        .acquire_idle_restored(
            record.catalog.clone(),
            record.replica.clone(),
            record.authority.clone(),
            idle,
            root.path().join("ordinary-successor.sqlite"),
            owner(2),
        )
        .await
        .unwrap();
    let charged = nodes[1].stats().local_disk_reserved_bytes();
    assert_eq!(charged, spec.cost.disk_bytes);
    let refused = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        refused.snapshot.head().attempts()[0].phase(),
        AttemptPhase::Activating
    );
    assert!(!refused.snapshot.head().attempts()[0].receiver_resources_settled());
    let adopted = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(adopted.activated, 1);
    let attempt = &adopted.snapshot.head().attempts()[0];
    let evidence = attempt.activated().unwrap();
    assert_eq!((evidence.node, evidence.session), (node_id(2), session(2)));
    assert_eq!(evidence.position.root, released.root);
    assert_eq!(evidence.position.epoch, released.epoch + 1);
    assert!(!attempt.receiver_resources_settled());
    assert!(adopted.snapshot.head().retirement_page(&[spec.id]).is_err());
    assert_eq!(nodes[1].stats().local_disk_reserved_bytes(), charged);
    let cleaned = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(cleaned.snapshot.head().attempts()[0].receiver_resources_settled());
    assert_eq!(nodes[1].stats().local_disk_reserved_bytes(), 0);
    let retired = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(retired.retired, 1);
    assert!(retired.snapshot.head().attempts().is_empty());
    assert_eq!(retired.snapshot.head().reserved_restore_bytes(), 0);
    let original = &acknowledged[&spec.target.cell_id()];
    assert_eq!(
        successor
            .resolve(original.identity, original.digest, clock().unwrap(), 64)
            .await
            .unwrap(),
        Resolution::Committed(original.outcome.clone())
    );
    let bytes = successor
        .query(64, 64, |connection| {
            let value: i64 =
                connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(bytes, original.value.to_be_bytes());
    assert!(
        journal
            .load_movement_action(
                scope(),
                spec.id,
                MovementAction::Activate,
                node_id(2),
                session(2)
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        record
            .authority
            .load(spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .epoch,
        released.epoch + 1
    );
    for node in &nodes {
        node.shutdown().await.unwrap();
        assert_eq!(node.state(), NodeState::Stopped);
        let stats = node.stats();
        assert_eq!(stats.active_cells(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.resident_bytes(), 0);
        assert_eq!(stats.file_descriptors(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
    }
    for boot in &boots {
        boot.withdraw(&journal).await.unwrap();
    }
    journal.close().await.unwrap();
}
