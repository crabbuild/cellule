use super::super::tests::FaultStore;
use super::*;
use crate::publication::CellPublisher;
use cellule_ltx::{CaptureBatch, CellReplica, Db, Limits};
use cellule_store::Store;
use object_store::path::Path;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

struct NativePreparation {
    store: Arc<FaultStore>,
    authority: CellAuthority,
    publisher: CellPublisher,
    database: Db,
    cuts: CaptureBatch,
    _directory: tempfile::TempDir,
}
impl NativePreparation {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut database =
            Db::open(&directory.path().join("native.sqlite"), Limits::default()).unwrap();
        database
            .transaction(|transaction| {
                transaction.execute_batch(
                    "CREATE TABLE events(value INTEGER); INSERT INTO events VALUES(7)",
                )?;
                Ok(())
            })
            .unwrap();
        let cuts = database.capture_deferred().unwrap();
        let store = Arc::new(FaultStore::default());
        let authority = CellAuthority::new(CellStorageLayout::new(
            Store::new(store.clone()),
            Path::from("native-preparation"),
            [3; 16],
        ));
        let cell = CellId::from_bytes([1; 32]);
        let initial = crate::control::Control::initial(
            cell,
            IncarnationId::from_bytes([2; 16]),
            crate::control::Owner {
                session: crate::identity::SessionId::from_bytes([4; 16]),
                endpoint: "https://publisher.internal".into(),
            },
            crate::identity::Digest::from_bytes([5; 32]),
            1,
        )
        .unwrap();
        authority
            .layout
            .store()
            .create_strict(
                &authority.layout.control_path(cell.as_bytes()),
                Bytes::from(initial.encode().unwrap()),
            )
            .await
            .unwrap();
        let replica = CellReplica::new(
            authority.layout.clone(),
            [1; 32],
            [2; 16],
            Limits::default(),
        )
        .unwrap();
        let observed = authority.load(cell).await.unwrap().unwrap();
        let publisher = CellPublisher::new(
            replica,
            authority.clone(),
            observed,
            directory.path().to_owned(),
        );
        Self {
            store,
            authority,
            publisher,
            database,
            cuts,
            _directory: directory,
        }
    }
    async fn assert_unpublished(&self) {
        assert!(
            self.authority
                .load(CellId::from_bytes([1; 32]))
                .await
                .unwrap()
                .unwrap()
                .value()
                .ltx_root()
                .is_none()
        );
    }
}

#[tokio::test]
async fn lineage_and_native_uploads_overlap_but_ready_and_authority_wait_for_both() {
    // Both layouts must finish all native root objects while metadata is
    // paused, and neither may expose readiness or advance authority early.
    for (segments, native_root_objects) in [(1, 1), (33, 1), (33, 2)] {
        let mut fixture = NativePreparation::new().await;
        if native_root_objects == 2 {
            // Retain external descriptors by exceeding the decoded merge
            // bound; the other multi-cut case verifies the merged layout.
            fixture
                .database
                .transaction(|tx| {
                    tx.execute_batch(
                        "CREATE TABLE padding(v); INSERT INTO padding VALUES(zeroblob(300000))",
                    )
                })
                .unwrap();
            let next = fixture.database.capture_deferred().unwrap();
            fixture.cuts.position = next.position;
            fixture.cuts.segments.extend(next.segments);
        }
        for _ in 1..segments {
            fixture
                .database
                .transaction(|transaction| {
                    transaction.execute_batch("INSERT INTO events VALUES(7)")
                })
                .unwrap();
            let next = fixture.database.capture_deferred().unwrap();
            fixture.cuts.position = next.position;
            fixture.cuts.segments.extend(next.segments);
        }
        fixture.store.fault.store(3, Ordering::SeqCst);
        let prepared = {
            let preparation = fixture.publisher.prepare_initial(&fixture.cuts);
            tokio::pin!(preparation);
            tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut preparation => panic!("preparation escaped paused metadata: {}", result.is_ok()),
            _ = async {
                fixture.store.entered.notified().await;
                while fixture.store.root_writes.load(Ordering::SeqCst) < native_root_objects {
                    fixture.store.root_written.notified().await;
                }
            } => {}
        }
    }).await.unwrap();
            assert!(
                fixture
                    .authority
                    .load(CellId::from_bytes([1; 32]))
                    .await
                    .unwrap()
                    .unwrap()
                    .value()
                    .ltx_root()
                    .is_none()
            );
            fixture.store.resume.notify_one();
            preparation.await.unwrap()
        };
        assert_eq!(fixture.store.lineage_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.store.lineage_reads.load(Ordering::SeqCst), 0);
        fixture
            .publisher
            .publish_prepared(&prepared, None)
            .await
            .unwrap();
        assert_eq!(
            fixture.store.lineage_writes.load(Ordering::SeqCst),
            1,
            "publication must reuse its exact retained preparation"
        );
        assert_eq!(fixture.store.lineage_reads.load(Ordering::SeqCst), 0);
        let replica = CellReplica::new(
            fixture.authority.layout.clone(),
            [1; 32],
            [2; 16],
            Limits::default(),
        )
        .unwrap();
        fixture
            .authority
            .verify_root_prefix(prepared.root(), prepared.root(), &replica, 64)
            .await
            .unwrap();
        fixture.database.close().unwrap();
    }
}

#[tokio::test]
async fn retained_lineage_cannot_publish_before_native_uploads_finish() {
    let mut fixture = NativePreparation::new().await;
    fixture.store.root_fault.store(3, Ordering::SeqCst);
    let prepared = {
        let preparation = fixture.publisher.prepare_initial(&fixture.cuts);
        tokio::pin!(preparation);
        tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut preparation => panic!("preparation escaped paused native upload: {}", result.is_ok()),
            _ = async {
                fixture.store.entered.notified().await;
                while fixture.store.lineage_writes.load(Ordering::SeqCst) < 1 {
                    tokio::task::yield_now().await;
                }
            } => {}
        }
    }).await.unwrap();
        assert!(
            fixture
                .authority
                .load(CellId::from_bytes([1; 32]))
                .await
                .unwrap()
                .unwrap()
                .value()
                .ltx_root()
                .is_none()
        );
        fixture.store.resume.notify_one();
        preparation.await.unwrap()
    };
    fixture
        .publisher
        .publish_prepared(&prepared, None)
        .await
        .unwrap();
    assert_eq!(fixture.store.lineage_writes.load(Ordering::SeqCst), 1);
    fixture.database.close().unwrap();
}

#[tokio::test]
async fn native_preparation_preserves_original_metadata_errors_and_adopts_only_exact_lost_replies()
{
    for fault in [1, 2] {
        let mut fixture = NativePreparation::new().await;
        fixture.store.fault.store(fault, Ordering::SeqCst);
        let result = fixture.publisher.prepare_initial(&fixture.cuts).await;
        fixture.assert_unpublished().await;
        let prepared = if fault == 1 {
            assert!(matches!(
                result,
                Err(Error::Storage(StorageError::NotSupported { .. }))
            ));
            fixture
                .publisher
                .prepare_initial(&fixture.cuts)
                .await
                .unwrap()
        } else {
            result.unwrap()
        };
        fixture
            .publisher
            .publish_prepared(&prepared, None)
            .await
            .unwrap();
        assert!(
            fixture
                .authority
                .root_lineage(prepared.root())
                .await
                .unwrap()
                .is_some()
        );
        fixture.database.close().unwrap();
    }
}

#[tokio::test]
async fn failed_native_upload_cannot_select_root_even_with_retained_metadata() {
    let mut fixture = NativePreparation::new().await;
    fixture.store.root_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        fixture.publisher.prepare_initial(&fixture.cuts).await,
        Err(Error::Ltx(_))
    ));
    fixture.assert_unpublished().await;
    let prepared = fixture
        .publisher
        .prepare_initial(&fixture.cuts)
        .await
        .unwrap();
    fixture
        .publisher
        .publish_prepared(&prepared, None)
        .await
        .unwrap();
    fixture.database.close().unwrap();
}

#[tokio::test]
async fn native_compaction_records_final_predecessor_without_a_second_conflict_write() {
    let mut fixture = NativePreparation::new().await;
    let initial = fixture
        .publisher
        .prepare_initial(&fixture.cuts)
        .await
        .unwrap();
    fixture
        .publisher
        .publish_prepared(&initial, None)
        .await
        .unwrap();
    for sequence in 1..=40 {
        fixture
            .database
            .transaction(|transaction| {
                transaction.execute("INSERT INTO events VALUES (?1)", [sequence])?;
                Ok(())
            })
            .unwrap();
        let cuts = fixture.database.capture_deferred().unwrap();
        let original = fixture
            .authority
            .load(CellId::from_bytes([1; 32]))
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root()
            .unwrap();
        let prepared = fixture
            .publisher
            .prepare_batch(&cuts, sequence)
            .await
            .unwrap();
        assert_eq!(prepared.predecessor(), Some(original));
        fixture
            .publisher
            .publish_prepared(&prepared, None)
            .await
            .unwrap();
    }
    assert_eq!(
        fixture.store.lineage_reads.load(Ordering::SeqCst),
        0,
        "native compaction must retain its final authority predecessor before Ready escapes"
    );
    assert_eq!(
        fixture.store.lineage_writes.load(Ordering::SeqCst),
        41,
        "one exact lineage write per complete proposal"
    );
    let replica = CellReplica::new(
        fixture.authority.layout.clone(),
        [1; 32],
        [2; 16],
        Limits::default(),
    )
    .unwrap();
    let latest = fixture
        .authority
        .load(CellId::from_bytes([1; 32]))
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert!(replica.open_root(&latest).await.unwrap().segment_count() < 32);
    fixture
        .authority
        .verify_root_prefix(initial.root(), latest, &replica, 64)
        .await
        .unwrap();
    fixture.database.close().unwrap();
}
