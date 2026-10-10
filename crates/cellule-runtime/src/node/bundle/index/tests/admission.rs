use super::*;

#[tokio::test]
async fn aggregate_history_budget_is_checked_before_the_first_history_read() {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let layout = cellule_ltx::CellStorageLayout::new(
        cellule_store::Store::new(counted.clone()),
        Path::from("history-admission"),
        [9; 16],
    );
    let session = SessionId::from_bytes([1; 16]);
    let mut bindings = Vec::new();
    let mut histories = BTreeMap::new();
    let mut cells = BTreeSet::new();
    for number in 0_u64..100_000 {
        let mut cell = [0; 32];
        cell[..8].copy_from_slice(&number.to_le_bytes());
        if shard(&[9; 16], &cell) != 0 {
            continue;
        }
        let mut binding = history_binding(cell, 1);
        let extent = Locator {
            object: Some(Digest::from_bytes([8; 32])),
            offset: HEADER_BYTES as u64,
            bytes: history::MAX_HISTORY_BYTES,
            frame_digest: Digest::from_bytes([9; 32]),
        };
        binding.locators = vec![extent.clone()];
        histories.insert(
            history::pin(&binding).unwrap(),
            history::History {
                extent,
                count: MAX_LOCATORS,
                native_bytes: MAX_SUFFIX_BYTES,
                loaded: None,
            },
        );
        cells.insert(([9; 16], cell));
        bindings.push(binding);
        if bindings.len() == 129 {
            break;
        }
    }
    assert_eq!(bindings.len(), 129);
    bindings
        .sort_unstable_by_key(|binding| *binding.control.bundle_binding.unwrap().digest.as_bytes());
    let catalog = Catalog {
        session,
        epoch: 2,
        predecessor: None,
        selected_through: 1,
        bindings,
        index: None,
    };
    let body = history::encode_leaf(&catalog, &histories).unwrap();
    let mut shards = vec![None; SHARDS];
    shards[0] = Some(Shard {
        extent: Locator {
            object: None,
            offset: HEADER_BYTES as u64,
            bytes: body.len() as u64,
            frame_digest: Digest::from_bytes(*blake3::hash(&body).as_bytes()),
        },
        bindings: 129,
    });
    let root = Root {
        detached: true,
        session,
        epoch: 2,
        predecessor: None,
        selected_through: 1,
        object_bytes: HEADER_BYTES as u64 + body.len() as u64,
        shards,
        frames: Vec::new(),
    };
    let header = codec::encode(&root).unwrap();
    let head = NodeBundleHead {
        epoch: 2,
        digest: Digest::from_bytes(*blake3::hash(&header).as_bytes()),
        selected_through: 1,
    };
    let mut object = header.to_vec();
    object.extend_from_slice(&body);
    layout
        .store()
        .put_exact(
            &layout.node_coverage_bundle_path(session.as_bytes(), 2, head.digest.as_bytes()),
            Bytes::from(object),
        )
        .await
        .unwrap();
    counted.reset();
    assert!(matches!(
        load_cells(&layout, session, head, &cells, None, None).await,
        Err(Error::Capacity("bundle selected history bytes"))
    ));
    assert_eq!(
        counted.requests().len(),
        2,
        "header and selected shard only; missing history objects must not be fetched"
    );
}

#[tokio::test]
async fn selected_shard_budget_is_checked_before_loading_any_leaf() {
    let counted = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
    let layout = cellule_ltx::CellStorageLayout::new(
        cellule_store::Store::new(counted.clone()),
        Path::from("index-admission"),
        [9; 16],
    );
    let session = SessionId::from_bytes([1; 16]);
    let mut shards = vec![None; SHARDS];
    // Both extents are individually bounded. Neither missing leaf should be
    // requested when their selected aggregate already exceeds the budget.
    for id in [0, 1] {
        shards[id] = Some(Shard {
            extent: Locator {
                object: Some(Digest::from_bytes([id as u8 + 2; 32])),
                offset: HEADER_BYTES as u64,
                bytes: 3 << 20,
                frame_digest: Digest::from_bytes([id as u8 + 4; 32]),
            },
            bindings: 1,
        });
    }
    let root = Root {
        detached: false,
        session,
        epoch: 2,
        predecessor: None,
        selected_through: 0,
        object_bytes: HEADER_BYTES as u64,
        shards,
        frames: Vec::new(),
    };
    let header = codec::encode(&root).unwrap();
    let head = NodeBundleHead {
        epoch: 2,
        digest: Digest::from_bytes(*blake3::hash(&header).as_bytes()),
        selected_through: 0,
    };
    layout
        .store()
        .put_exact(
            &layout.node_coverage_bundle_path(session.as_bytes(), 2, head.digest.as_bytes()),
            header,
        )
        .await
        .unwrap();
    counted.reset();
    assert!(matches!(
        load(&layout, session, head, Some(&[0, 1].into_iter().collect())).await,
        Err(Error::Capacity("bundle selected shard bytes"))
    ));
    assert_eq!(
        counted.requests().len(),
        1,
        "only the authenticated header is fetched"
    );
    // A point lookup charges only its chosen shard. Other large shards are
    // not a reason to reject it; this request reaches the missing origin leaf.
    counted.reset();
    assert!(!matches!(
        load(&layout, session, head, Some(&[0].into_iter().collect())).await,
        Err(Error::Capacity("bundle selected shard bytes"))
    ));
    assert_eq!(counted.requests().len(), 2);
}
