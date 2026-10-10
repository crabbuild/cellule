use super::*;

#[tokio::test]
async fn one_use_origin_verification_keeps_the_complete_inventory_and_fresh_body_checks() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = Db::open(&temp.path().join("source"), Limits::default()).unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    let backend = Arc::new(InMemory::new());
    let store = Store::new(backend.clone());
    let layout = CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]);
    let replica = replica(store, [161; 32], [162; 16]);
    let root = replica.prepare(None, &cuts, 1, 1).await.unwrap().root();
    let expected = replica.reachable_objects_bounded(&root, 64).await.unwrap();
    let operation = replica
        .small_root_origin_verification(&root, 64)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.working_bytes(), 512 << 10);
    assert_eq!(operation.verify().await.unwrap(), expected);

    let operation = replica
        .small_root_origin_verification(&root, 64)
        .await
        .unwrap()
        .unwrap();
    let packed = expected
        .iter()
        .find(|o| o.kind == CellObjectKind::Packed)
        .unwrap();
    let path =
        layout.incarnation_object_path(&root.cell, &root.incarnation, &packed.digest, packed.kind);
    let bytes = backend.get(&path).await.unwrap().bytes().await.unwrap();
    let mut corrupt = bytes.to_vec();
    corrupt[64] ^= 1;
    backend
        .put(&path, Bytes::from(corrupt).into())
        .await
        .unwrap();
    assert!(matches!(
        operation.verify().await,
        Err(cellule_ltx::LtxError::ChecksumMismatch)
    ));
    backend.put(&path, bytes.into()).await.unwrap();
    let operation = replica
        .small_root_origin_verification(&root, 64)
        .await
        .unwrap()
        .unwrap();
    backend.delete(&path).await.unwrap();
    assert!(
        operation.verify().await.is_err(),
        "prior inventory does not establish fresh body presence"
    );
    assert!(matches!(
        replica.small_root_origin_verification(&root, 0).await,
        Err(cellule_ltx::LtxError::Limit(
            cellule_ltx::LimitKind::RootInventoryObjects
        ))
    ));
}

#[tokio::test]
async fn large_origin_graph_defers_to_the_canonical_streaming_inventory_and_exact_restore() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = Db::open(&temp.path().join("source"), Limits::default()).unwrap();
    let mut payload = vec![0_u8; 512 << 10];
    blake3::Hasher::new().finalize_xof().fill(&mut payload);
    db.transaction(|tx| {
        tx.execute_batch("CREATE TABLE t(v BLOB)")?;
        tx.execute("INSERT INTO t VALUES(?1)", [&payload])?;
        Ok(())
    })
    .unwrap();
    let cuts = db.capture().unwrap();
    let replica = replica(Store::new(Arc::new(InMemory::new())), [163; 32], [164; 16]);
    let root = replica.prepare(None, &cuts, 1, 1).await.unwrap().root();
    assert!(
        replica
            .small_root_origin_verification(&root, 64)
            .await
            .unwrap()
            .is_none()
    );
    let objects = replica.reachable_objects_bounded(&root, 64).await.unwrap();
    assert!(
        objects
            .iter()
            .any(|object| object.kind == CellObjectKind::Ltx)
    );
    let destination = temp.path().join("cold");
    replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&destination)
        .await
        .unwrap();
    let conn = cellule_ltx::rusqlite::Connection::open(destination).unwrap();
    let restored: Vec<u8> = conn
        .query_row("SELECT v FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(restored, payload);
}
