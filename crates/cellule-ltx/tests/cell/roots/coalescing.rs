use super::*;
use cellule_store::test_support::CountingObjectStore;

#[tokio::test]
async fn small_captured_batch_publishes_one_delta_and_restores_every_original_cut() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = Db::open(&directory.path().join("writer.sqlite"), Limits::default()).unwrap();
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let store = Store::new(counted.clone());
    let cell = replica(store.clone(), [239; 32], [240; 16]);
    writer
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(1)")
        })
        .unwrap();
    let first = writer.capture().unwrap();
    let base = cell.prepare(None, &first, 1, 1).await.unwrap().root();
    let mut all = first.segments;
    let mut batch = CaptureBatch {
        segments: Vec::new(),
        position: first.position,
        timing: CaptureTiming::default(),
    };
    for sequence in 2..=17 {
        writer
            .transaction(|tx| {
                tx.execute("UPDATE counter SET v=?1", [sequence])?;
                Ok(())
            })
            .unwrap();
        let cuts = writer.capture().unwrap();
        all.extend(cuts.segments.clone());
        batch.position = cuts.position;
        batch.segments.extend(cuts.segments);
    }
    counted.reset();
    let prepared = cell.prepare(Some(&base), &batch, 17, 1).await.unwrap();
    assert_eq!(prepared.predecessor(), Some(base));
    assert_eq!(prepared.root().position, batch.position);
    assert_eq!(
        prepared.verified().segment_count(),
        2,
        "one published base and one merged delta"
    );
    assert_eq!(
        counted.put_requests(),
        4,
        "one body/index pair, final directory and root"
    );
    let retried = cell.prepare(Some(&base), &batch, 17, 1).await.unwrap();
    assert_eq!(retried.root(), prepared.root());
    let limited = CellReplica::new(
        CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]),
        [239; 32],
        [240; 16],
        Limits {
            max_segments: 16,
            ..Limits::default()
        },
    )
    .unwrap();
    counted.reset();
    assert!(matches!(
        limited.prepare(Some(&base), &batch, 17, 1).await,
        Err(cellule_ltx::LtxError::Limit(
            cellule_ltx::LimitKind::CellRootSegments
        ))
    ));
    assert_eq!(
        counted.put_requests(),
        0,
        "original chain admission precedes merging"
    );
    let mut gap = batch.clone();
    gap.segments.remove(1);
    assert!(matches!(
        cell.prepare(Some(&base), &gap, 17, 1).await,
        Err(cellule_ltx::LtxError::LTXCorrupted)
    ));
    assert_eq!(counted.put_requests(), 0);
    writer.close().unwrap();
    let expected = directory.path().join("expected.sqlite");
    restore_exact(
        &VerifiedPlan::new(&all, batch.position, Limits::default()).unwrap(),
        &expected,
    )
    .unwrap();
    let cold = replica(store, [239; 32], [240; 16]);
    cold.reachable_objects(&prepared.root()).await.unwrap();
    let restored = directory.path().join("restored.sqlite");
    cold.open_root(&prepared.root())
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(restored).unwrap(),
        std::fs::read(expected).unwrap()
    );
    let old = directory.path().join("old.sqlite");
    cold.open_root(&base)
        .await
        .unwrap()
        .restore(&old)
        .await
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(old).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT v FROM counter", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn captured_merge_truncates_regrows_and_bounds_decoded_pages() {
    for payload_bytes in [200_000, 2_000_000] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("writer.sqlite");
        let connection = cellule_ltx::rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("PRAGMA auto_vacuum=FULL; VACUUM")
            .unwrap();
        drop(connection);
        let mut writer = Db::open(&path, Limits::default()).unwrap();
        writer
            .transaction(|tx| {
                tx.execute_batch(
                    "CREATE TABLE payload(v); INSERT INTO payload VALUES(zeroblob(200000))",
                )
            })
            .unwrap();
        let store = Store::new(Arc::new(InMemory::new()));
        let cell = replica(store.clone(), [241; 32], [242; 16]);
        let first = writer.capture().unwrap();
        let base = cell.prepare(None, &first, 1, 1).await.unwrap().root();
        let mut all = first.segments;
        writer
            .transaction(|tx| tx.execute_batch("DELETE FROM payload"))
            .unwrap();
        let mut batch = writer.capture().unwrap();
        all.extend(batch.segments.clone());
        let minimum = batch.segments.last().unwrap().info().database_pages;
        writer
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO payload VALUES(?1)",
                    [vec![0xab; payload_bytes]],
                )?;
                Ok(())
            })
            .unwrap();
        let growth = writer.capture().unwrap();
        assert!(growth.segments.last().unwrap().info().database_pages > minimum);
        assert!(
            growth
                .segments
                .iter()
                .all(|cut| cut.info().size_bytes < 256 << 10),
            "compressed input fits small source bound"
        );
        all.extend(growth.segments.clone());
        batch.segments.extend(growth.segments);
        batch.position = growth.position;
        let prepared = cell.prepare(Some(&base), &batch, 3, 1).await.unwrap();
        assert_eq!(
            prepared.verified().segment_count(),
            if payload_bytes < 256 << 10 { 2 } else { 3 },
            "decoded page bound selects the original streaming path"
        );
        writer.close().unwrap();
        let expected = directory.path().join("expected.sqlite");
        restore_exact(
            &VerifiedPlan::new(&all, batch.position, Limits::default()).unwrap(),
            &expected,
        )
        .unwrap();
        let cold = replica(store, [241; 32], [242; 16]);
        cold.reachable_objects(&prepared.root()).await.unwrap();
        let restored = directory.path().join("restored.sqlite");
        cold.open_root(&prepared.root())
            .await
            .unwrap()
            .restore(&restored)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(restored).unwrap(),
            std::fs::read(expected).unwrap()
        );
    }
}

#[tokio::test]
async fn captured_merge_rejects_mutated_source_before_uploads() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = Db::open(&directory.path().join("writer.sqlite"), Limits::default()).unwrap();
    writer
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE counter(v); INSERT INTO counter VALUES(1)")
        })
        .unwrap();
    let mut batch = writer.capture().unwrap();
    writer
        .transaction(|tx| tx.execute_batch("UPDATE counter SET v=2"))
        .unwrap();
    let next = writer.capture().unwrap();
    batch.position = next.position;
    batch.segments.extend(next.segments);
    let path = batch.segments[0].path();
    let original = std::fs::read(path).unwrap();
    let mut corrupted = original.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    std::fs::write(path, corrupted).unwrap();
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let cell = replica(Store::new(counted.clone()), [243; 32], [244; 16]);
    assert!(matches!(
        cell.prepare(None, &batch, 2, 1).await,
        Err(cellule_ltx::LtxError::ChecksumMismatch)
    ));
    assert_eq!(counted.put_requests(), 0);
    std::fs::write(path, original).unwrap();
    cell.prepare(None, &batch, 2, 1).await.unwrap();
    writer.close().unwrap();
}
