use super::*;
use cellule_host::fleet::FleetEnrollmentJournal;
use cellule_runtime::fleet::operations::{EnrollmentEndpoint, EnrollmentSpec};
use cellule_runtime::node::{NodeCapacity, NodeFailureDomain};
use ed25519_dalek::SigningKey;

struct Fixture {
    _root: tempfile::TempDir,
    fleet: adapters::LocalFleet,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            SqliteJournal::open(
                root.path().join("observation.sqlite"),
                scope(),
                FleetProfile::default(),
                clock().unwrap(),
            )
            .await
            .unwrap(),
        );
        let mut nodes = Vec::new();
        let mut boots = Vec::new();
        let (records, _) = initialize(&root, &journal, &mut nodes, &mut boots, 60_000)
            .await
            .unwrap();
        let snapshot = journal.load_snapshot(scope()).await.unwrap();
        journal
            .claim_controller(
                scope(),
                snapshot.head().revision(),
                SessionId::from_bytes([206; 16]),
                clock().unwrap(),
            )
            .await
            .unwrap();
        Self {
            _root: root,
            fleet: adapters::LocalFleet {
                nodes,
                boots,
                records,
                journal,
                capture_sequence: std::sync::atomic::AtomicU64::new(0),
                lose_release_replies: false,
                lost_release_replies: std::sync::atomic::AtomicUsize::new(0),
                expired_receiver_cleanups: std::sync::atomic::AtomicUsize::new(0),
            },
        }
    }
    async fn roster(&self) -> FleetRoster {
        let snapshot = self.fleet.journal.load_snapshot(scope()).await.unwrap();
        FleetRoster::collect(
            self.fleet.journal.as_ref(),
            &snapshot,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap()
    }
    async fn capture(&self) -> Capture {
        collect(
            &self.fleet,
            &self.roster().await,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap()
    }
    async fn close(self) {
        self.close_checked(false).await;
    }
    async fn close_checked(self, source_was_fenced: bool) {
        for (index, node) in self.fleet.nodes.iter().enumerate() {
            let joined = node.shutdown().await;
            if let Err(original) = joined {
                assert!(
                    source_was_fenced && index == 0,
                    "unexpected drain failure: {original:?}"
                );
                let first = fenced_cause(&original).expect("original fenced drain source");
                assert_ne!(node.state(), NodeState::Stopped);
                let again = node.shutdown().await.unwrap_err();
                assert!(std::ptr::eq(
                    first,
                    fenced_cause(&again).expect("retained fenced source")
                ));
                assert!(
                    self.fleet.boots[index]
                        .withdraw(&self.fleet.journal)
                        .await
                        .is_err()
                );
                let record = self
                    .fleet
                    .journal
                    .load_enrollment(scope(), self.fleet.boots[index].spec.key().unwrap())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(record.status(), EnrollmentStatus::Established);
            } else {
                assert_eq!(node.state(), NodeState::Stopped);
                self.fleet.boots[index]
                    .withdraw(&self.fleet.journal)
                    .await
                    .unwrap();
            }
            let stats = node.stats();
            assert_eq!(stats.retained_bytes(), 0);
            assert_eq!(stats.resident_bytes(), 0);
            assert_eq!(stats.active_cells(), 0);
            assert_eq!(stats.worker_jobs(), 0);
            assert_eq!(stats.file_descriptors(), 0);
            assert_eq!(stats.local_disk_reserved_bytes(), 0);
        }
        self.fleet.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn complete_writer_profile_uses_original_pages_authority_and_canonical_heartbeats() {
    let fixture = Fixture::new().await;
    let before = fixture.fleet.journal.load_snapshot(scope()).await.unwrap();
    let capture = fixture.capture().await;
    assert!(capture.complete);
    assert_eq!(capture.cells.len(), CELL_COUNT);
    assert_eq!(capture.nodes.len(), 3);
    assert!(capture.started <= capture.finished);
    assert_eq!(
        fixture.fleet.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert_eq!(fixture.fleet.capture_sequence.load(Ordering::SeqCst), 24);
    for (index, ad) in capture.nodes.iter().enumerate() {
        let canonical = fixture.fleet.boots[index]
            .directory
            .load(session(index), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(canonical.advertisement(), ad);
        assert_eq!(ad.generation(), 2);
        assert_eq!(
            ad.release(),
            fixture.fleet.nodes[index]
                .application()
                .registry()
                .release_digest()
        );
        assert_eq!(
            ad.verifying_key().unwrap(),
            fixture.fleet.boots[index]
                .advertisement
                .verifying_key()
                .unwrap()
        );
        fixture.fleet.boots[index]
            .guard
            .as_ref()
            .unwrap()
            .check()
            .unwrap();
    }
    // A second pass advances actual native classifier sequences and canonical
    // heartbeat generations; original establishment evidence remains unchanged.
    let second = fixture.capture().await;
    assert!(second.complete);
    for (old, new) in capture.nodes.iter().zip(&second.nodes) {
        assert_eq!(new.generation(), 3);
        assert!(
            new.operational_sample().unwrap().sequence > old.operational_sample().unwrap().sequence
        );
    }
    assert_eq!(
        fixture.fleet.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    fixture.close().await;
}

#[tokio::test]
async fn unexpected_advertised_boot_prevents_complete_counts_even_without_live_roles() {
    let fixture = Fixture::new().await;
    let original = &fixture.fleet.boots[0].advertisement;
    let now = clock().unwrap();
    let extra = NodeAdvertisement::sign(
        node_id(3),
        session(3),
        owner(3).endpoint,
        scope().fleet,
        original.certificate(),
        original.image(),
        original.release(),
        &SigningKey::from_bytes(&[4; 32]),
        1,
        now,
        now + 30_000,
        original.module_digests().to_vec(),
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            log_protocol: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let observed = fixture.fleet.boots[0]
        .directory
        .create(extra, now)
        .await
        .unwrap();
    let capture = fixture.capture().await;
    assert!(!capture.complete);
    assert_eq!(capture.cells.len(), CELL_COUNT);
    fixture.fleet.boots[0]
        .directory
        .withdraw(&observed, clock().unwrap())
        .await
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn pending_role_and_changed_journal_barrier_cannot_become_empty_coverage() {
    let fixture = Fixture::new().await;
    let original = fixture.roster().await;
    let record = fixture.fleet.records.values().next().unwrap();
    let authority = record
        .authority
        .load(record.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let spec = EnrollmentSpec {
        scope: scope(),
        request: Digest::from_bytes([87; 32]),
        role: EnrollmentRole::Reader {
            target: record.target.clone(),
            position: PublishedPosition {
                incarnation: record.incarnation,
                epoch: authority.value().epoch,
                root: authority.value().root.clone().unwrap(),
            },
        },
        source: Some(EnrollmentEndpoint {
            node: node_id(0),
            session: session(0),
            intent_revision: 1,
        }),
        target: EnrollmentEndpoint {
            node: node_id(1),
            session: session(1),
            intent_revision: 1,
        },
    };
    fixture
        .fleet
        .journal
        .accept_enrollment(&spec, clock().unwrap())
        .await
        .unwrap();
    assert!(
        collect(
            &fixture.fleet,
            &original,
            Instant::now() + Duration::from_secs(5)
        )
        .await
        .is_err()
    );
    let capture = fixture.capture().await;
    assert!(!capture.complete);
    assert_eq!(capture.cells.len(), CELL_COUNT);
    fixture.close().await;
}

#[tokio::test]
async fn foreign_current_owner_removes_stale_actor_from_pressure_and_count_inputs() {
    let fixture = Fixture::new().await;
    let record = fixture.fleet.records.values().next().unwrap();
    let original = record
        .authority
        .load(record.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let changed = original.value().takeover(owner(1)).unwrap();
    record
        .authority
        .transition(
            &original,
            changed,
            cellule_runtime::control::Transition::Takeover,
        )
        .await
        .unwrap();
    let capture = fixture.capture().await;
    assert!(!capture.complete);
    assert_eq!(capture.cells.len(), CELL_COUNT - 1);
    assert!(
        capture
            .cells
            .iter()
            .all(|row| row.observation.target.cell_id() != record.target.cell_id())
    );
    fixture.close_checked(true).await;
}

#[tokio::test]
async fn fenced_guard_cannot_publish_capacity_or_recreate_a_retired_boot() {
    let fixture = Fixture::new().await;
    let boot = &fixture.fleet.boots[1];
    let original = boot
        .directory
        .load(session(1), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    let before = fixture.fleet.journal.load_snapshot(scope()).await.unwrap();
    boot.guard.as_ref().unwrap().fence();
    let error = boot
        .refresh_capacity(
            1,
            fixture.fleet.journal.as_ref(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<cellule_runtime::Error>(),
        Some(cellule_runtime::Error::Fenced)
    ));
    let unchanged = boot
        .directory
        .load(session(1), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.advertisement(), original.advertisement());
    assert_eq!(
        fixture.fleet.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    boot.node.shutdown().await.unwrap();
    boot.withdraw(&fixture.fleet.journal).await.unwrap();
    assert!(boot.directory.is_retired(session(1)).await.unwrap());
    assert!(
        boot.refresh_capacity(
            1,
            fixture.fleet.journal.as_ref(),
            Instant::now() + Duration::from_secs(3)
        )
        .await
        .is_err()
    );
    assert!(boot.directory.is_retired(session(1)).await.unwrap());
    fixture.close().await;
}

#[tokio::test]
async fn canonical_heartbeat_consumes_live_maintenance_intent_and_preserves_existing_writers() {
    use cellule_runtime::fleet::operations::{
        JournalTransition, MaintenanceOperation, OperationId,
    };
    use cellule_runtime::node::NodeMode;

    let fixture = Fixture::new().await;
    let boot = &fixture.fleet.boots[0];
    let key = boot.spec.key().unwrap();
    let original = fixture
        .fleet
        .journal
        .load_enrollment(scope(), key)
        .await
        .unwrap()
        .unwrap();
    let snapshot = fixture.fleet.journal.load_snapshot(scope()).await.unwrap();
    let now = clock().unwrap();
    let operation = MaintenanceOperation::new(
        OperationId::from_bytes([80; 16]).unwrap(),
        Digest::from_bytes([81; 32]),
        node_id(0),
        session(0),
        2,
        now,
        now + 60_000,
    )
    .unwrap();
    fixture
        .fleet
        .journal
        .compare_exchange(
            &snapshot,
            snapshot.head().controller().unwrap().epoch,
            now,
            &JournalTransition::BeginMaintenance(operation),
        )
        .await
        .unwrap();
    // No Cordon action is dispatched. The ordinary caller-driven heartbeat
    // consumes the retained intent before its canonical CAS and guard renewal.
    let refreshed = boot
        .refresh_capacity(
            0,
            fixture.fleet.journal.as_ref(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(
        refreshed.operational_sample().unwrap().mode,
        NodeMode::Draining
    );
    assert!(!refreshed.accepts_new_roles(clock().unwrap()));
    assert!(
        boot.node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    assert_eq!(boot.node.state(), NodeState::Ready);
    assert!(boot.node.is_ready() && boot.node.is_management_ready());
    assert_eq!(
        fixture
            .fleet
            .journal
            .load_enrollment(scope(), key)
            .await
            .unwrap(),
        Some(original)
    );
    let canonical = boot
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(canonical.advertisement(), &refreshed);
    boot.guard.as_ref().unwrap().check().unwrap();
    for record in fixture.fleet.records.values() {
        let current = record
            .authority
            .load(record.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let handle = boot
            .node
            .runtime()
            .local_handle(record.catalog.clone(), &current)
            .await
            .unwrap()
            .unwrap();
        handle.query(64, 64, |_| Ok(Vec::new())).await.unwrap();
    }
    assert_eq!(boot.node.stats().active_cells(), CELL_COUNT);
    fixture.close().await;
}

fn fenced_cause<'a>(
    mut error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a cellule_runtime::Error> {
    loop {
        if let Some(source) = error.downcast_ref::<cellule_runtime::Error>()
            && matches!(source, cellule_runtime::Error::Fenced)
        {
            return Some(source);
        }
        error = error.source()?;
    }
}

#[tokio::test]
async fn one_canonical_release_keeps_other_native_writers_available_for_pressure_relief() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    let original = fixture.capture().await;
    assert!(original.complete);
    let selected = original.cells.first().unwrap().observation.clone();
    let before = page(
        &fixture.fleet,
        &roster,
        0,
        FleetSnapshotSubject::Cells(None),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let FleetSnapshotNativePage::Cells(before_page) = before.page() else {
        panic!("missing original actor page")
    };
    let topology = before_page.topology();
    let released = fixture.fleet.nodes[0]
        .runtime()
        .release_idle_cell_at(
            selected.target.cell_id(),
            session(0),
            selected.generation,
            selected.incarnation,
            selected.position.as_ref().unwrap().epoch,
        )
        .await
        .unwrap();
    assert_eq!(released, selected.position.unwrap());
    drop(before);
    let after = page(
        &fixture.fleet,
        &roster,
        0,
        FleetSnapshotSubject::Cells(None),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    let FleetSnapshotNativePage::Cells(after_page) = after.page() else {
        panic!("missing repeated actor page")
    };
    assert_ne!(after_page.topology(), topology);
    assert_eq!(after_page.owned_cells(), CELL_COUNT - 1);
    let mut candidates = original.cells;
    retain_unchanged_writers(&mut candidates, 0, after_page.entries());
    assert_eq!(candidates.len(), CELL_COUNT - 1);
    assert!(
        candidates
            .iter()
            .all(|row| row.observation.target.cell_id() != selected.target.cell_id())
    );
    drop(after);
    fixture.close().await;
}
