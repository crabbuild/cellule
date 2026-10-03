//! Complete metadata capture over authenticated closed test catalogs. The child
//! proves a lifetime stand-in; these cases supply no OS-crashed CellNode, prefix
//! availability or external-job qualification.
use super::*;
use cellule_host::fleet::{
    FleetJournalSnapshot, FleetOriginalCatalogSet, FleetOriginalCatalogSource,
    FleetOriginalCatalogs, FleetOriginalWriterCapture, FleetOriginalWriterInventory,
    FleetOriginalWriterJournal,
};
use cellule_runtime::control::Control;
use cellule_runtime::control::{Transition, authority::CellAuthority};
use cellule_runtime::fleet::operations::{
    JournalTransition, MaintenanceOperation, OperationId, OriginalWriterInventoryRecord,
};
use cellule_runtime::identity::NamespaceId;

struct Catalogs {
    sources: Vec<FleetOriginalCatalogSource>,
    reads: AtomicUsize,
    change_on_second: bool,
    error_on_second: bool,
}
impl FleetOriginalCatalogs for Catalogs {
    fn catalogs<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
        operation: &'a MaintenanceOperation,
        _: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, FleetOriginalCatalogSet> {
        Box::pin(async move {
            let read = self.reads.fetch_add(1, Ordering::AcqRel);
            if read == 1 && self.error_on_second {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "original catalog authentication failed",
                )
                .into());
            }
            let witness = Digest::from_bytes(
                [if read == 1 && self.change_on_second {
                    51
                } else {
                    50
                }; 32],
            );
            Ok(FleetOriginalCatalogSet::new(
                request,
                operation.clone(),
                witness,
                self.sources.clone(),
            )?)
        })
    }
}
struct WriterFixture {
    base: Fixture,
    request: FleetFailedBootProcessRequest,
    catalogs: Catalogs,
    expected: Vec<Control>,
    layouts: Vec<CellStorageLayout>,
}
impl WriterFixture {
    async fn new() -> Self {
        Self::with_originals(33).await
    }
    async fn with_originals(originals_per_catalog: u64) -> Self {
        let base = Fixture::new().await;
        let request = FleetFailedBootProcessRequest::capture_fenced(
            base.journal.as_ref(),
            &base.directory,
            &base.roster().await,
            &base.failed_boot().await,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
        let mut sources = Vec::new();
        let mut layouts = Vec::new();
        let mut expected = Vec::new();
        for index in 0..2 {
            let layout = CellStorageLayout::new(
                Store::new(Arc::new(InMemory::new())),
                ObjectPath::from(format!("original-catalog-{index}")),
                [index + 3; 16],
            );
            let tenant = TenantId::from_bytes([index + 1; 16]);
            let catalog = CellCatalog::new(layout.clone(), tenant);
            let authority = CellAuthority::new(layout.clone());
            // Accepted original metadata commits before process joining. Each
            // original is later removed by the sole authority takeover path.
            for n in 0_u64..originals_per_catalog {
                let target = CellTarget::new(
                    tenant,
                    catalog.application(),
                    NamespaceId::from_bytes([9; 16]),
                    &n.to_be_bytes(),
                )
                .unwrap();
                let proof = catalog
                    .provision(
                        CatalogEntry::new(
                            &target,
                            CatalogRole::Sql,
                            Digest::from_bytes([8; 32]),
                            1,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let observed = authority
                    .create_initial(&proof, IncarnationId::from_bytes([10; 16]), owner(0))
                    .await
                    .unwrap();
                expected.push(observed.value().clone());
                authority
                    .transition(
                        &observed,
                        observed.value().takeover(owner(1)).unwrap(),
                        Transition::Takeover,
                    )
                    .await
                    .unwrap();
            }
            // An unused bootstrap entry is part of complete traversal.
            let target = CellTarget::new(
                tenant,
                catalog.application(),
                NamespaceId::from_bytes([9; 16]),
                b"unused",
            )
            .unwrap();
            catalog
                .provision(
                    CatalogEntry::new(&target, CatalogRole::Sql, Digest::from_bytes([8; 32]), 1)
                        .unwrap(),
                )
                .await
                .unwrap();
            layouts.push(layout.clone());
            sources.push(
                FleetOriginalCatalogSource::new(
                    Digest::from_bytes([index + 60; 32]),
                    tenant,
                    layout,
                )
                .unwrap(),
            );
        }
        let mut child = Process::start(base.process_path());
        child.stop_and_retain(&request);
        let now = CHECK;
        let before = base.journal.load_snapshot(scope()).await.unwrap();
        let snapshot = base
            .journal
            .claim_controller(scope(), before.head().revision(), session(1), now)
            .await
            .unwrap();
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
        base.journal
            .compare_exchange(
                &snapshot,
                snapshot.head().controller().unwrap().epoch,
                now,
                &JournalTransition::BeginMaintenance(operation),
            )
            .await
            .unwrap();
        Self {
            base,
            request,
            catalogs: Catalogs {
                sources,
                reads: AtomicUsize::new(0),
                change_on_second: false,
                error_on_second: false,
            },
            expected,
            layouts,
        }
    }
    async fn capture(&self) -> cellule_runtime::Result<FleetOriginalWriterCapture> {
        FleetOriginalWriterCapture::capture(
            self.base.journal.as_ref(),
            &self.base.directory,
            &Processes::new(self.base.process_path()),
            &self.catalogs,
            &self.request,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
    }
}
#[tokio::test]
async fn complete_original_writers_survive_takeover_atomic_publication_and_reconstruction() {
    let fixture = WriterFixture::new().await;
    let capture = fixture.capture().await.unwrap();
    assert_eq!(capture.record().owner_count(), 66);
    assert_eq!(capture.pages().len(), 2);
    assert_eq!(
        capture
            .record()
            .catalogs()
            .iter()
            .map(|scope| scope.cells)
            .sum::<u64>(),
        68
    );
    let original_interval = capture.record().basis().interval;
    let original_digest = capture.record().digest().unwrap();
    let stored = capture
        .publish(
            fixture.base.journal.as_ref(),
            &fixture.base.directory,
            &Processes::new(fixture.base.process_path()),
            &fixture.catalogs,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap();
    assert_eq!(stored, *capture.record());
    let snapshot = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        snapshot.registry().revision(),
        stored.basis().registry.revision() + 1
    );
    let replay = capture
        .publish(
            fixture.base.journal.as_ref(),
            &fixture.base.directory,
            &Processes::new(fixture.base.process_path()),
            &fixture.catalogs,
            session(1),
            deadline(),
            || Ok(CHECK + 60_000),
        )
        .await
        .unwrap();
    assert_eq!(replay.digest().unwrap(), original_digest);
    assert_eq!(
        fixture.base.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    fixture.base.journal.close().await.unwrap();
    let independent = fixture.base.reconstruct().await;
    let snapshot = independent.load_snapshot(scope()).await.unwrap();
    let recovered = FleetOriginalWriterInventory::load(
        &independent,
        &snapshot,
        stored.basis().operation.id(),
        fixture.request.digest(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(recovered.record(), &stored);
    assert_eq!(recovered.pages(), capture.pages());
    assert_eq!(recovered.record().basis().interval, original_interval);
    let mut actual = recovered
        .writers()
        .map(|row| row.control.clone())
        .collect::<Vec<_>>();
    let mut expected = fixture.expected;
    actual.sort_by_key(|row| *row.cell.as_bytes());
    expected.sort_by_key(|row| *row.cell.as_bytes());
    assert_eq!(actual, expected);
    independent.close().await.unwrap();
}
#[tokio::test]
async fn changed_or_unauthenticated_original_catalog_configuration_cannot_publish() {
    for source_error in [false, true] {
        let mut fixture = WriterFixture::new().await;
        fixture.catalogs.change_on_second = !source_error;
        fixture.catalogs.error_on_second = source_error;
        let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
        let error = fixture.capture().await.err().unwrap();
        if source_error {
            let Error::Facility { source, .. } = error else {
                panic!("provider cause required")
            };
            assert_eq!(
                source.downcast_ref::<std::io::Error>().unwrap().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        } else {
            assert!(matches!(
                error,
                Error::Control("original catalog configuration changed")
            ));
        }
        assert_eq!(
            fixture.base.journal.load_snapshot(scope()).await.unwrap(),
            before
        );
        assert!(
            fixture
                .base
                .journal
                .original_writers(
                    &before,
                    before.head().maintenance().unwrap().id(),
                    fixture.request.digest()
                )
                .await
                .unwrap()
                .is_none()
        );
        fixture.base.journal.close().await.unwrap();
    }
}
#[tokio::test]
async fn original_set_lost_commit_reply_is_adopted_without_recollecting_or_restamping() {
    let fixture = WriterFixture::new().await;
    let capture = fixture.capture().await.unwrap();
    let expected = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    fixture.base.journal.lose_next_commit_reply();
    assert!(
        fixture
            .base
            .journal
            .persist_original_writers(&expected, capture.record(), capture.pages(), CHECK)
            .await
            .is_err()
    );
    let snapshot = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    assert_ne!(snapshot.registry(), expected.registry());
    let retained = fixture
        .base
        .journal
        .original_writers(
            &snapshot,
            capture.record().basis().operation.id(),
            fixture.request.digest(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained, *capture.record());
    fixture
        .base
        .journal
        .persist_original_writers(&expected, capture.record(), capture.pages(), CHECK + 60_000)
        .await
        .unwrap();
    assert_eq!(
        fixture.base.journal.load_snapshot(scope()).await.unwrap(),
        snapshot
    );
    fixture.base.journal.close().await.unwrap();
}
#[tokio::test]
async fn changed_barrier_and_incomplete_pages_cannot_commit_an_original_set() {
    let fixture = WriterFixture::new().await;
    let capture = fixture.capture().await.unwrap();
    let expected = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    assert!(
        fixture
            .base
            .journal
            .persist_original_writers(&expected, capture.record(), &capture.pages()[..1], CHECK)
            .await
            .is_err()
    );
    assert_eq!(
        fixture.base.journal.load_snapshot(scope()).await.unwrap(),
        expected
    );
    fixture
        .base
        .journal
        .set_scheduling(expected.registry(), true)
        .await
        .unwrap();
    assert_ne!(
        fixture
            .base
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .registry(),
        expected.registry()
    );
    assert!(
        capture
            .publish(
                fixture.base.journal.as_ref(),
                &fixture.base.directory,
                &Processes::new(fixture.base.process_path()),
                &fixture.catalogs,
                session(1),
                deadline(),
                || Ok(CHECK)
            )
            .await
            .is_err()
    );
    let current = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    assert!(
        fixture
            .base
            .journal
            .original_writers(
                &current,
                capture.record().basis().operation.id(),
                fixture.request.digest()
            )
            .await
            .unwrap()
            .is_none()
    );
    fixture.base.journal.close().await.unwrap();
}

#[tokio::test]
async fn missing_original_owner_history_is_a_typed_blocker_not_an_empty_set() {
    let fixture = WriterFixture::new().await;
    let original = &fixture.expected[0];
    let layout = &fixture.layouts[0];
    layout
        .store()
        .delete(&layout.owner_observation_path(
            original.cell.as_bytes(),
            original.incarnation.as_bytes(),
            original.epoch,
        ))
        .await
        .unwrap();
    let error = fixture.capture().await.err().unwrap();
    assert!(
        matches!(error,Error::OwnerHistoryIncomplete { cell,incarnation,epoch } if cell==original.cell && incarnation==original.incarnation && epoch==original.epoch)
    );
    let snapshot = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    assert!(
        fixture
            .base
            .journal
            .original_writers(
                &snapshot,
                snapshot.head().maintenance().unwrap().id(),
                fixture.request.digest()
            )
            .await
            .unwrap()
            .is_none()
    );
    fixture.base.journal.close().await.unwrap();
}

#[tokio::test]
async fn catalog_and_process_changes_before_first_publication_refuse() {
    for catalog_change in [false, true] {
        let fixture = WriterFixture::new().await;
        let capture = fixture.capture().await.unwrap();
        let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
        if catalog_change {
            let catalog =
                CellCatalog::new(fixture.layouts[0].clone(), TenantId::from_bytes([1; 16]));
            let target = CellTarget::new(
                catalog.tenant(),
                catalog.application(),
                NamespaceId::from_bytes([9; 16]),
                b"later",
            )
            .unwrap();
            catalog
                .provision(
                    CatalogEntry::new(&target, CatalogRole::Sql, Digest::from_bytes([8; 32]), 1)
                        .unwrap(),
                )
                .await
                .unwrap();
        } else {
            let mut bytes = std::fs::read(fixture.base.process_path()).unwrap();
            bytes[32] ^= 1;
            std::fs::write(fixture.base.process_path(), bytes).unwrap();
        }
        assert!(
            capture
                .publish(
                    fixture.base.journal.as_ref(),
                    &fixture.base.directory,
                    &Processes::new(fixture.base.process_path()),
                    &fixture.catalogs,
                    session(1),
                    deadline(),
                    || Ok(CHECK),
                )
                .await
                .is_err()
        );
        assert_eq!(
            fixture.base.journal.load_snapshot(scope()).await.unwrap(),
            before
        );
        assert!(
            fixture
                .base
                .journal
                .original_writers(
                    &before,
                    capture.record().basis().operation.id(),
                    fixture.request.digest(),
                )
                .await
                .unwrap()
                .is_none()
        );
        fixture.base.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn independent_publications_choose_one_immutable_original_set() {
    for identical in [false, true] {
        let fixture = WriterFixture::new().await;
        let capture = fixture.capture().await.unwrap();
        let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
        let independent = fixture.base.reconstruct().await;
        let mut basis = capture.record().basis().clone();
        if !identical {
            basis.catalog_witness = Digest::from_bytes([91; 32]);
        }
        let (other, pages) = OriginalWriterInventoryRecord::new(
            basis,
            capture.record().catalogs().to_vec(),
            capture
                .pages()
                .iter()
                .flat_map(|page| page.entries().iter().cloned())
                .collect(),
        )
        .unwrap();
        let (a, b) = tokio::join!(
            fixture.base.journal.persist_original_writers(
                &before,
                capture.record(),
                capture.pages(),
                CHECK
            ),
            independent.persist_original_writers(&before, &other, &pages, CHECK),
        );
        assert_eq!(
            usize::from(a.is_ok()) + usize::from(b.is_ok()),
            if identical { 2 } else { 1 }
        );
        let current = independent.load_snapshot(scope()).await.unwrap();
        assert_eq!(
            current.registry().revision(),
            before.registry().revision() + 1
        );
        let retained = independent
            .original_writers(
                &current,
                capture.record().basis().operation.id(),
                fixture.request.digest(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained, a.or(b).unwrap());
        independent.close().await.unwrap();
        fixture.base.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn canceled_publication_waiter_preserves_the_accepted_original_commit() {
    let fixture = WriterFixture::new().await;
    let capture = fixture.capture().await.unwrap();
    let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    let (committed, resume) = fixture.base.journal.pause_next_original_writer_reply();
    let journal = Arc::clone(&fixture.base.journal);
    let expected = before.clone();
    let record = capture.record().clone();
    let pages = capture.pages().to_vec();
    let waiter = tokio::spawn(async move {
        journal
            .persist_original_writers(&expected, &record, &pages, CHECK)
            .await
    });
    committed.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    drop(resume);
    fixture.base.journal.close().await.unwrap();
    let independent = fixture.base.reconstruct().await;
    let current = independent.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        current.registry().revision(),
        before.registry().revision() + 1
    );
    let retained = FleetOriginalWriterInventory::load(
        &independent,
        &current,
        capture.record().basis().operation.id(),
        fixture.request.digest(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(retained.record(), capture.record());
    assert_eq!(retained.pages(), capture.pages());
    assert_eq!(retained.writers().count(), 66);
    independent.close().await.unwrap();
}

#[tokio::test]
async fn original_writer_reload_distinguishes_absence_and_rejects_stale_barriers() {
    let fixture = WriterFixture::new().await;
    let capture = fixture.capture().await.unwrap();
    let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    let operation = capture.record().basis().operation.id();
    assert!(
        FleetOriginalWriterInventory::load(
            fixture.base.journal.as_ref(),
            &before,
            operation,
            fixture.request.digest(),
            deadline(),
        )
        .await
        .unwrap()
        .is_none()
    );
    fixture
        .base
        .journal
        .persist_original_writers(&before, capture.record(), capture.pages(), CHECK)
        .await
        .unwrap();
    let current = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    let error = FleetOriginalWriterInventory::load(
        fixture.base.journal.as_ref(),
        &before,
        operation,
        fixture.request.digest(),
        deadline(),
    )
    .await
    .err()
    .unwrap();
    let Error::Facility { source, .. } = error else {
        panic!("original journal conflict required")
    };
    assert!(matches!(
        source.downcast_ref::<cellule_runtime::fleet::operations::OperationError>(),
        Some(cellule_runtime::fleet::operations::OperationError::Conflict)
    ));
    let loaded = FleetOriginalWriterInventory::load(
        fixture.base.journal.as_ref(),
        &current,
        operation,
        fixture.request.digest(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(loaded.record(), capture.record());
    assert_eq!(loaded.pages(), capture.pages());
    assert_eq!(
        fixture.base.journal.load_snapshot(scope()).await.unwrap(),
        current
    );
    fixture.base.journal.close().await.unwrap();
}

#[tokio::test]
async fn original_writer_reload_never_returns_a_partial_missing_or_corrupt_set() {
    for corrupt in [false, true] {
        let fixture = WriterFixture::new().await;
        let capture = fixture.capture().await.unwrap();
        let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
        fixture
            .base
            .journal
            .persist_original_writers(&before, capture.record(), capture.pages(), CHECK)
            .await
            .unwrap();
        fixture.base.journal.close().await.unwrap();
        // Damage the final page offline. The first page remains valid, so a
        // streamed or prematurely returned inventory would lose original owners.
        let db = rusqlite::Connection::open(&fixture.base.path).unwrap();
        let digest = capture.record().pages()[1];
        let changed = if corrupt {
            db.execute(
                "UPDATE original_writer_pages SET body=?1 WHERE key=?2",
                rusqlite::params![vec![0_u8], digest.as_bytes().as_slice()],
            )
            .unwrap()
        } else {
            db.execute(
                "DELETE FROM original_writer_pages WHERE key=?1",
                [digest.as_bytes().as_slice()],
            )
            .unwrap()
        };
        assert_eq!(changed, 1);
        drop(db);
        let independent = fixture.base.reconstruct().await;
        let current = independent.load_snapshot(scope()).await.unwrap();
        let error = FleetOriginalWriterInventory::load(
            &independent,
            &current,
            capture.record().basis().operation.id(),
            fixture.request.digest(),
            deadline(),
        )
        .await
        .err()
        .unwrap();
        if corrupt {
            let Error::Facility { source, .. } = error else {
                panic!("original page decoder cause required")
            };
            assert!(
                source
                    .downcast_ref::<cellule_runtime::fleet::operations::OperationError>()
                    .is_some()
            );
        } else {
            assert!(matches!(error, Error::Fenced));
        }
        assert_eq!(independent.load_snapshot(scope()).await.unwrap(), current);
        independent.close().await.unwrap();
    }
}

#[tokio::test]
async fn original_writer_reload_preserves_sql_source_errors() {
    let fixture = WriterFixture::new().await;
    let capture = fixture.capture().await.unwrap();
    let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .base
        .journal
        .persist_original_writers(&before, capture.record(), capture.pages(), CHECK)
        .await
        .unwrap();
    fixture.base.journal.close().await.unwrap();
    let independent = fixture.base.reconstruct().await;
    let current = independent.load_snapshot(scope()).await.unwrap();
    // Open creates the schema; remove it afterwards to force the actual reader
    // to return its source failure rather than a missing-page observation.
    let db = rusqlite::Connection::open(&fixture.base.path).unwrap();
    db.execute("DROP TABLE original_writer_pages", []).unwrap();
    drop(db);
    let error = FleetOriginalWriterInventory::load(
        &independent,
        &current,
        capture.record().basis().operation.id(),
        fixture.request.digest(),
        deadline(),
    )
    .await
    .err()
    .unwrap();
    let Error::Facility { source, .. } = error else {
        panic!("original SQL source cause required")
    };
    assert!(source.downcast_ref::<rusqlite::Error>().is_some());
    assert_eq!(independent.load_snapshot(scope()).await.unwrap(), current);
    independent.close().await.unwrap();
}

#[tokio::test]
async fn original_writer_reload_refuses_expired_deadlines_and_invalid_keys() {
    let fixture = WriterFixture::new().await;
    let current = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    let operation = current.head().maintenance().unwrap().id();
    let error = FleetOriginalWriterInventory::load(
        fixture.base.journal.as_ref(),
        &current,
        operation,
        fixture.request.digest(),
        Instant::now(),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(error, Error::Deadline));
    let error = FleetOriginalWriterInventory::load(
        fixture.base.journal.as_ref(),
        &current,
        operation,
        Digest::from_bytes([0; 32]),
        deadline(),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        Error::Control("invalid original writer inventory key")
    ));
    assert_eq!(
        fixture.base.journal.load_snapshot(scope()).await.unwrap(),
        current
    );
    fixture.base.journal.close().await.unwrap();
}

#[tokio::test]
async fn original_writer_reload_retains_an_explicit_complete_empty_capture() {
    // Both authenticated original catalogs contain only unused bootstrap entries;
    // no original owner has existed. This is a real complete scan, not omission
    // of populated catalogs or removal of entries from a retained manifest.
    let fixture = WriterFixture::with_originals(0).await;
    let capture = fixture.capture().await.unwrap();
    assert_eq!(capture.record().owner_count(), 0);
    assert!(capture.pages().is_empty());
    assert_eq!(capture.record().catalogs().len(), 2);
    assert_eq!(
        capture
            .record()
            .catalogs()
            .iter()
            .map(|row| row.cells)
            .sum::<u64>(),
        2
    );
    let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .base
        .journal
        .persist_original_writers(&before, capture.record(), capture.pages(), CHECK)
        .await
        .unwrap();
    fixture.base.journal.close().await.unwrap();
    let independent = fixture.base.reconstruct().await;
    let current = independent.load_snapshot(scope()).await.unwrap();
    let retained = FleetOriginalWriterInventory::load(
        &independent,
        &current,
        capture.record().basis().operation.id(),
        fixture.request.digest(),
        deadline(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(retained.record(), capture.record());
    assert!(retained.pages().is_empty());
    assert_eq!(retained.writers().count(), 0);
    independent.close().await.unwrap();
}
