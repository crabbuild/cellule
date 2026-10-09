//! Packed origin bytes, extents and inline directory corruption fail closed.
use super::*;

#[tokio::test]
async fn packed_root_vector_authenticates_exact_native_bytes_and_every_metadata_extent() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    writer
        .transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = writer.capture().unwrap();
    let native = std::fs::read(cuts.segments[0].path()).unwrap();
    let backend = Arc::new(InMemory::new());
    let store = Store::new(backend.clone());
    let layout = CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]);
    let cell = [241; 32];
    let incarnation = [242; 16];
    let replica = replica(store, cell, incarnation);
    let prepared = replica.prepare(None, &cuts, 1, 1).await.unwrap();
    let root = prepared.root();
    let root_path =
        layout.incarnation_object_path(&cell, &incarnation, &root.digest, CellObjectKind::Root);
    let root_bytes = backend
        .get(&root_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&root_bytes).unwrap();
    assert_eq!(wire["version"], 2);
    assert_eq!(wire["directory_height"], 0);
    assert!(wire["directory_inline"].as_str().unwrap().len() <= 4096);
    assert_eq!(wire["segments"][0]["packed"], true);
    assert_eq!(wire["segments"][0]["offset"], "64");
    let objects = replica.reachable_objects(&root).await.unwrap();
    assert_eq!(objects.len(), 2);
    let packed = objects
        .iter()
        .find(|object| object.kind == CellObjectKind::Packed)
        .unwrap();
    let path = layout.incarnation_object_path(&cell, &incarnation, &packed.digest, packed.kind);
    let bytes = backend.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&bytes[..8], b"CRBPACK1");
    assert_eq!(&bytes[8..16], &(native.len() as u64).to_be_bytes());
    assert_eq!(&bytes[24..32], &[0; 8]);
    assert_eq!(&bytes[32..64], &cuts.segments[0].info().blake3);
    assert_eq!(&bytes[64..64 + native.len()], native.as_slice());
    assert_eq!(*blake3::hash(&bytes).as_bytes(), packed.digest);
    writer.close().unwrap();
    let expected = directory.path().join("expected");
    restore_exact(
        &VerifiedPlan::new(&cuts.segments, cuts.position, Limits::default()).unwrap(),
        &expected,
    )
    .unwrap();

    // Header, native body and index all belong to the selected packed object.
    for position in [0, 8, 16, 24, 32, 64, bytes.len() - 1] {
        let mut corrupt = bytes.to_vec();
        corrupt[position] ^= 1;
        backend
            .put(&path, Bytes::from(corrupt).into())
            .await
            .unwrap();
        assert!(
            replica.reachable_objects(&root).await.is_err(),
            "byte {position}"
        );
        assert!(
            replica
                .prepare_compaction(&root, 0..1, 9, directory.path())
                .await
                .is_err(),
            "byte {position}"
        );
    }
    backend
        .put(&path, bytes.slice(..bytes.len() - 1).into())
        .await
        .unwrap();
    assert!(replica.reachable_objects(&root).await.is_err());
    backend.delete(&path).await.unwrap();
    assert!(replica.reachable_objects(&root).await.is_err());
    backend.put(&path, bytes.into()).await.unwrap();
    let restored = directory.path().join("restored");
    prepared.verified().restore(&restored).await.unwrap();
    assert_eq!(
        std::fs::read(&restored).unwrap(),
        std::fs::read(expected).unwrap()
    );
}

#[tokio::test]
async fn packed_root_rejects_cross_scope_and_noncanonical_overlapping_extents() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = Db::open(&directory.path().join("source"), Limits::default()).unwrap();
    writer
        .transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(7)"))
        .unwrap();
    let cuts = writer.capture().unwrap();
    let backend = Arc::new(InMemory::new());
    let store = Store::new(backend.clone());
    let layout = CellStorageLayout::new(store.clone(), Path::from("runtime"), [3; 16]);
    let cell = [243; 32];
    let incarnation = [244; 16];
    let replica = replica(store.clone(), cell, incarnation);
    let root = replica.prepare(None, &cuts, 1, 1).await.unwrap().root();
    let path =
        layout.incarnation_object_path(&cell, &incarnation, &root.digest, CellObjectKind::Root);
    let bytes = backend.get(&path).await.unwrap().bytes().await.unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (field, value) in [
        ("offset", serde_json::json!("0")),
        ("offset", serde_json::json!("65")),
        ("index_length", serde_json::json!("18446744073709551615")),
        ("length", serde_json::json!("18446744073709551615")),
    ] {
        let mut wire = original.clone();
        wire["segments"][0][field] = value;
        let bytes = serde_json::to_vec(&wire).unwrap();
        let bad = RootRef {
            digest: *blake3::hash(&bytes).as_bytes(),
            ..root
        };
        let path =
            layout.incarnation_object_path(&cell, &incarnation, &bad.digest, CellObjectKind::Root);
        backend.put(&path, Bytes::from(bytes).into()).await.unwrap();
        assert!(replica.open_root(&bad).await.is_err(), "{field}");
    }
    let foreign = super::replica(store, [245; 32], incarnation);
    assert!(foreign.open_root(&root).await.is_err());
    let mut wire = original;
    wire["directory_inline"] = serde_json::json!("00".repeat(2049));
    let bytes = serde_json::to_vec(&wire).unwrap();
    let bad = RootRef {
        digest: *blake3::hash(&bytes).as_bytes(),
        ..root
    };
    let path =
        layout.incarnation_object_path(&cell, &incarnation, &bad.digest, CellObjectKind::Root);
    backend.put(&path, Bytes::from(bytes).into()).await.unwrap();
    assert!(replica.open_root(&bad).await.is_err());
    writer.close().unwrap();
}
