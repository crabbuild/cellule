use super::*;

#[tokio::test]
async fn singleton_cohort_uses_canonical_packs_and_identical_native_or_coalesced_roots() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    for count in 1..=2_u8 {
        let replica = replica(store.clone(), [100 + count; 32], [110 + count; 16]);
        let mut db = Db::open(
            &directory.path().join(format!("source-{count}")),
            Limits::default(),
        )
        .unwrap();
        db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
            .unwrap();
        let mut cuts = db.capture().unwrap();
        if count == 2 {
            db.transaction(|tx| tx.execute_batch("INSERT INTO t VALUES(11)"))
                .unwrap();
            let next = db.capture().unwrap();
            cuts.segments.extend(next.segments);
            cuts.position = next.position;
        }
        let inputs = vec![replica.shared_captures(&cuts).await.unwrap().unwrap()];
        let appends = CellReplica::upload_shared(inputs, directory.path())
            .await
            .unwrap();
        assert_eq!(replica.publication_cost().objects, 0);
        let cohort = replica
            .prepare_shared(None, &appends[0], u64::from(count), 1)
            .await
            .unwrap();
        let direct = replica
            .prepare(None, &cuts, u64::from(count), 1)
            .await
            .unwrap();
        assert_eq!(cohort.root(), direct.root());
        let objects = replica.reachable_objects(&cohort.root()).await.unwrap();
        assert!(
            objects
                .iter()
                .any(|object| object.kind == CellObjectKind::Packed)
        );
        assert!(
            !objects
                .iter()
                .any(|object| object.kind == CellObjectKind::SharedPacked)
        );
        let destination = directory.path().join(format!("cohort-{count}"));
        let canonical = directory.path().join(format!("canonical-{count}"));
        cohort.verified().restore(&destination).await.unwrap();
        direct.verified().restore(&canonical).await.unwrap();
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            std::fs::read(canonical).unwrap()
        );
        let db = rusqlite::Connection::open(destination).unwrap();
        let rows: u8 = db
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, count);
    }
    assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("shared-publication")
    }));
}

#[tokio::test]
async fn shared_publication_uploads_once_and_restores_each_exact_cell() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = replica(store.clone(), [71; 32], [72; 16]);
    let second = replica(store.clone(), [73; 32], [74; 16]);
    let mut dbs = Vec::new();
    let mut captures = Vec::new();
    for (i, replica) in [&first, &second].into_iter().enumerate() {
        let mut db = Db::open(
            &directory.path().join(format!("source-{i}.db")),
            Limits::default(),
        )
        .unwrap();
        db.transaction(|tx| {
            tx.execute_batch(&format!(
                "CREATE TABLE items(value INTEGER); INSERT INTO items VALUES({i});"
            ))
        })
        .unwrap();
        let cuts = db.capture().unwrap();
        captures.push(replica.shared_captures(&cuts).await.unwrap().unwrap());
        dbs.push(db);
    }
    let appends = CellReplica::upload_shared(captures, directory.path())
        .await
        .unwrap();
    assert_eq!(appends.len(), 2);
    assert!(
        second
            .prepare_shared(None, &appends[0], 0, 1)
            .await
            .is_err()
    );
    let mut shared_digest = None;
    let mut prepared_roots = Vec::new();
    for (i, (replica, append)) in [&first, &second].into_iter().zip(&appends).enumerate() {
        let prepared = replica.prepare_shared(None, append, 0, 1).await.unwrap();
        let objects = replica.reachable_objects(&prepared.root()).await.unwrap();
        let shared = objects
            .iter()
            .find(|object| object.kind == CellObjectKind::SharedPacked)
            .unwrap();
        assert_eq!(*shared_digest.get_or_insert(shared.digest), shared.digest);
        assert!(!objects.iter().any(|object| matches!(
            object.kind,
            CellObjectKind::Ltx | CellObjectKind::Index | CellObjectKind::Packed
        )));
        let destination = directory.path().join(format!("restored-{i}.db"));
        replica
            .open_root(&prepared.root())
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap();
        let restored = rusqlite::Connection::open(&destination).unwrap();
        let value: i64 = restored
            .query_row("SELECT value FROM items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, i as i64);
        prepared_roots.push((prepared.root(), destination));
    }
    assert_eq!(
        first.publication_cost().objects + second.publication_cost().objects,
        3
    );
    for (i, (replica, (root, destination))) in [&first, &second]
        .into_iter()
        .zip(prepared_roots)
        .enumerate()
    {
        let compacted = replica
            .prepare_compaction(&root, 0..1, 9, directory.path())
            .await
            .unwrap();
        assert_eq!(compacted.root().position, root.position);
        let compacted_destination = directory.path().join(format!("compacted-{i}.db"));
        compacted
            .verified()
            .restore(&compacted_destination)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            std::fs::read(compacted_destination).unwrap()
        );
    }
    assert_eq!(
        first.publication_cost().objects + second.publication_cost().objects,
        7
    );
    let shared = first.scope();
    let layout = CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]);
    let path = layout.incarnation_object_path(
        &shared.0,
        &shared.1,
        &shared_digest.unwrap(),
        CellObjectKind::SharedPacked,
    );
    assert!(path.as_ref().contains("/shared/objects/"));
    store.delete(&path).await.unwrap();
    let prepared = second
        .prepare_shared(None, &appends[1], 0, 1)
        .await
        .unwrap();
    assert!(second.reachable_objects(&prepared.root()).await.is_err());
}

#[tokio::test]
async fn shared_scope_is_verified_even_when_sibling_native_bytes_are_identical() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(InMemory::new());
    let store = Store::new(backend.clone());
    let layout = CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]);
    let first = replica(store.clone(), [81; 32], [82; 16]);
    let second = replica(store.clone(), [83; 32], [84; 16]);
    let mut db = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let appends = CellReplica::upload_shared(
        vec![
            first.shared_captures(&cuts).await.unwrap().unwrap(),
            second.shared_captures(&cuts).await.unwrap().unwrap(),
        ],
        directory.path(),
    )
    .await
    .unwrap();
    let roots = [
        first
            .prepare_shared(None, &appends[0], 1, 1)
            .await
            .unwrap()
            .root(),
        second
            .prepare_shared(None, &appends[1], 1, 1)
            .await
            .unwrap()
            .root(),
    ];
    let path = layout.incarnation_object_path(
        &roots[1].cell,
        &roots[1].incarnation,
        &roots[1].digest,
        CellObjectKind::Root,
    );
    let bytes = backend.get(&path).await.unwrap().bytes().await.unwrap();
    let mut wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    wire["cell"] = serde_json::json!("51".repeat(32));
    wire["incarnation"] = serde_json::json!("52".repeat(16));
    let bytes = Bytes::from(serde_json::to_vec(&wire).unwrap());
    let forged = RootRef {
        digest: *blake3::hash(&bytes).as_bytes(),
        ..roots[0]
    };
    backend
        .put(
            &layout.incarnation_object_path(
                &forged.cell,
                &forged.incarnation,
                &forged.digest,
                CellObjectKind::Root,
            ),
            bytes.into(),
        )
        .await
        .unwrap();
    assert!(first.reachable_objects(&forged).await.is_err());
    assert!(
        first
            .prepare_compaction(&forged, 0..1, 9, directory.path())
            .await
            .is_err()
    );
    let reopened = first.open_root(&forged).await.unwrap();
    assert!(reopened.paged().read_page(1).await.is_err());
    assert!(
        reopened
            .restore(&directory.path().join("forged-restore"))
            .await
            .is_err()
    );

    let object = first
        .reachable_objects(&roots[0])
        .await
        .unwrap()
        .into_iter()
        .find(|object| object.kind == CellObjectKind::SharedPacked)
        .unwrap();
    let path = layout.incarnation_object_path(
        &roots[0].cell,
        &roots[0].incarnation,
        &object.digest,
        object.kind,
    );
    let bytes = backend.get(&path).await.unwrap().bytes().await.unwrap();
    for offset in [0, 8, 12, 16, 48, 64, 72, 80, 88, 96, 128, bytes.len() - 1] {
        let mut corrupt = bytes.to_vec();
        corrupt[offset] ^= 1;
        backend
            .put(&path, Bytes::from(corrupt).into())
            .await
            .unwrap();
        for root in roots {
            assert!(
                replica(store.clone(), root.cell, root.incarnation)
                    .reachable_objects(&root)
                    .await
                    .is_err(),
                "corruption at {offset}"
            );
        }
    }
    backend
        .put(&path, bytes.slice(..bytes.len() - 1).into())
        .await
        .unwrap();
    assert!(first.reachable_objects(&roots[0]).await.is_err());
}

#[tokio::test]
async fn shared_upload_refuses_cross_store_and_duplicate_ranges() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = replica(store.clone(), [91; 32], [92; 16]);
    let second = CellReplica::new(
        CellStorageLayout::new(store, Path::from("another-prefix"), [3; 16]),
        [93; 32],
        [94; 16],
        Limits::default(),
    )
    .unwrap();
    let mut db = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let inputs = vec![
        first.shared_captures(&cuts).await.unwrap().unwrap(),
        second.shared_captures(&cuts).await.unwrap().unwrap(),
    ];
    assert!(
        CellReplica::upload_shared(inputs, directory.path())
            .await
            .is_err()
    );
    let inputs = vec![
        first.shared_captures(&cuts).await.unwrap().unwrap(),
        first.shared_captures(&cuts).await.unwrap().unwrap(),
    ];
    assert!(
        CellReplica::upload_shared(inputs, directory.path())
            .await
            .is_err()
    );
    assert_eq!(first.publication_cost().objects, 0);
    assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("shared-publication")
    }));
}
