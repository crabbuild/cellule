use super::*;
use cellule_store::test_support::CountingObjectStore;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;

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
