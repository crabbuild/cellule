use super::*;
use crate::fleet::resource::{ResourceCost, ResourceLedger};
use object_store::ObjectStoreExt;

fn scratch() -> BundlePreparation {
    BundlePreparation::reserve(&ResourceLedger::new(
        ResourceCost::zero().with_retained_bytes(PREPARATION_BYTES),
    ))
    .unwrap()
}

#[tokio::test]
async fn prepared_shards_remove_repeat_reads_without_changing_the_proposal() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    inventory(&mut f, &cell, 2_000).await;
    let mut scratch = scratch();
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[],
            Some(&mut scratch),
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    f.node = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    let (_, frames, assigned) = f.append(&mut cell, 3);
    f.count.reset();
    let cached = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[],
            Some(&mut scratch),
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let reads = f
        .count
        .requests()
        .into_iter()
        .filter(|read| {
            read.location.ends_with(".cnb")
                && read.kind == cellule_store::test_support::ObjectReadKind::Range
        })
        .count();
    assert_eq!(
        reads, 2,
        "only the fresh header and uncached exact history are needed"
    );
    f.count.reset();
    let fresh = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let fresh_reads = f
        .count
        .requests()
        .into_iter()
        .filter(|read| {
            read.location.ends_with(".cnb")
                && read.kind == cellule_store::test_support::ObjectReadKind::Range
        })
        .count();
    assert_eq!(fresh_reads, 3, "the uncached path also reads the shard");
    assert_eq!(cached.body, fresh.body);
    assert_eq!(cached.head, fresh.head);
    f.node = f
        .directory
        .select_node_bundle(&f.node, &cached, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    f.count.reset();
    let recovered = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(recovered.commit_sequence(), 3);
    assert!(
        f.count
            .requests()
            .iter()
            .filter(|read| read.location.ends_with(".cnb")
                && read.kind == cellule_store::test_support::ObjectReadKind::Range)
            .count()
            >= 5,
        "cold recovery must read header, shard, history, and both native extents"
    );
}

#[tokio::test]
async fn warm_preparation_cannot_hide_corrupt_native_dependencies_or_a_fence() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let mut scratch = scratch();
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[],
            Some(&mut scratch),
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    f.node = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let mut corrupt = prepared.body.to_vec();
    let offset = prepared
        .catalog
        .bindings
        .iter()
        .flat_map(|row| &row.locators)
        .find(|locator| locator.object.is_none())
        .unwrap()
        .offset as usize;
    corrupt[offset + 8] ^= 1;
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, prepared.head.digest.as_bytes());
    f.layout
        .store()
        .inner()
        .put(&path, Bytes::from(corrupt).into())
        .await
        .unwrap();
    // Preparation is only a proposal. Its valid cached metadata cannot prove
    // that the native dependency still exists with the authenticated bytes.
    let proposed = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[],
            Some(&mut scratch),
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &proposed, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .is_err()
    );
    f.layout
        .store()
        .inner()
        .put(&path, prepared.body.clone().into())
        .await
        .unwrap();
    f.lease.fence();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &proposed, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert_eq!(
        f.directory
            .load(SessionId::from_bytes([1; 16]), NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .bundle_head(),
        f.node.advertisement().bundle_head()
    );
}

#[tokio::test]
async fn warm_preparation_still_requires_an_authentic_fresh_header() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let mut scratch = scratch();
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[],
            Some(&mut scratch),
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    f.node = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, prepared.head.digest.as_bytes());
    let mut corrupt = prepared.body.to_vec();
    corrupt[24] ^= 1;
    f.layout
        .store()
        .inner()
        .put(&path, Bytes::from(corrupt).into())
        .await
        .unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 3);
    assert!(
        f.directory
            .prepare_node_bundle_with_checkpoints(
                &f.node,
                &frames,
                &[assigned],
                &[],
                Some(&mut scratch),
                Limits::default(),
                NOW,
            )
            .await
            .is_err()
    );
}
