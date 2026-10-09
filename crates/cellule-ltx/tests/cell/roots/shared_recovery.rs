use super::*;

fn bundle(cells: &[([u8; 32], [u8; 16], CaptureBatch)]) -> Bundle {
    Bundle::encode(
        cells
            .iter()
            .flat_map(|(cell, incarnation, cuts)| {
                cuts.segments.iter().map(|segment| {
                    BundleEntry::for_cell(
                        *cell,
                        *incarnation,
                        segment.info().clone(),
                        std::fs::read(segment.path()).unwrap(),
                    )
                })
            })
            .collect(),
        Limits::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn shared_recovery_preserves_exact_scopes_original_chains_and_cold_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let counted = Arc::new(cellule_store::test_support::CountingObjectStore::new(
        Arc::new(InMemory::new()),
    ));
    let store = Store::new(counted.clone());
    let mut replicas = Vec::new();
    let mut bases = Vec::new();
    let mut cells = Vec::new();
    for i in 0..2_u8 {
        let cell = [91 + i; 32];
        let incarnation = [93 + i; 16];
        let replica = replica(store.clone(), cell, incarnation);
        let mut db = Db::open(
            &directory.path().join(format!("source-{i}")),
            Limits::default(),
        )
        .unwrap();
        db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(1)"))
            .unwrap();
        bases.push(
            replica
                .prepare(None, &db.capture().unwrap(), 1, 1)
                .await
                .unwrap()
                .root(),
        );
        db.transaction(|tx| tx.execute("INSERT INTO t VALUES(2)", []))
            .unwrap();
        let mut cuts = db.capture().unwrap();
        db.transaction(|tx| tx.execute("INSERT INTO t VALUES(3)", []))
            .unwrap();
        let last = db.capture().unwrap();
        cuts.segments.extend(last.segments);
        cuts.position = last.position;
        cells.push((cell, incarnation, cuts));
        replicas.push(replica);
        db.close().unwrap();
    }
    let mut captures = Vec::new();
    for (i, replica) in replicas.iter().enumerate() {
        let overlay = RecoveryOverlay::new(bases[i], bundle(&cells), cells[i].2.position, 3);
        let (_, rows) = overlay.shared_input_upper_bound().unwrap();
        assert_eq!(rows, 2, "only this Cell's original rows are charged");
        captures.push(
            replica
                .shared_recovered_captures(&overlay)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    counted.reset();
    let appends = CellReplica::upload_shared(captures, directory.path())
        .await
        .unwrap();
    assert_eq!(counted.put_requests(), 1, "one shared immutable body");
    assert!(
        replicas[1]
            .prepare_shared(Some(&bases[1]), &appends[0], 3, 1)
            .await
            .is_err()
    );
    for (i, replica) in replicas.iter().enumerate() {
        let shared = replica
            .prepare_shared(Some(&bases[i]), &appends[i], 3, 1)
            .await
            .unwrap();
        let overlay = RecoveryOverlay::new(bases[i], bundle(&cells), cells[i].2.position, 3);
        let canonical = replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        assert_eq!(shared.predecessor(), Some(bases[i]));
        assert_eq!(shared.root().position, canonical.root().position);
        assert_eq!(shared.root().commit_sequence, 3);
        let cold = super::replica(store.clone(), cells[i].0, cells[i].1);
        let shared_path = directory.path().join(format!("shared-{i}"));
        let direct_path = directory.path().join(format!("direct-{i}"));
        cold.open_root(&shared.root())
            .await
            .unwrap()
            .restore(&shared_path)
            .await
            .unwrap();
        cold.open_root(&canonical.root())
            .await
            .unwrap()
            .restore(&direct_path)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(shared_path).unwrap(),
            std::fs::read(direct_path).unwrap()
        );

        let limited = CellReplica::new(
            CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]),
            cells[i].0,
            cells[i].1,
            Limits {
                max_segments: 2,
                ..Limits::default()
            },
        )
        .unwrap();
        counted.reset();
        assert!(
            limited
                .prepare_shared(Some(&bases[i]), &appends[i], 3, 1)
                .await
                .is_err(),
            "one base plus two original cuts exceeds admission despite coalescing"
        );
        assert_eq!(counted.put_requests(), 0);
    }
}

#[tokio::test]
async fn shared_recovery_rejects_overlay_scope_and_endpoint_before_upload() {
    let directory = tempfile::tempdir().unwrap();
    let counted = Arc::new(cellule_store::test_support::CountingObjectStore::new(
        Arc::new(InMemory::new()),
    ));
    let replica = replica(Store::new(counted.clone()), [97; 32], [98; 16]);
    let mut db = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(1)"))
        .unwrap();
    let base = replica
        .prepare(None, &db.capture().unwrap(), 1, 1)
        .await
        .unwrap()
        .root();
    db.transaction(|tx| tx.execute("INSERT INTO t VALUES(2)", []))
        .unwrap();
    let cells = vec![([97; 32], [98; 16], db.capture().unwrap())];
    for (predecessor, endpoint) in [
        (
            RootRef {
                cell: [99; 32],
                ..base
            },
            cells[0].2.position,
        ),
        (base, base.position),
    ] {
        counted.reset();
        let overlay = RecoveryOverlay::new(predecessor, bundle(&cells), endpoint, 2);
        assert!(replica.shared_recovered_captures(&overlay).await.is_err());
        assert_eq!(counted.put_requests(), 0);
    }
    db.close().unwrap();
}
