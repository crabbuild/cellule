use super::*;

#[tokio::test]
async fn checkpoint_uses_the_exact_materialized_proof_without_scanning_siblings_or_old_frames() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    inventory(&mut f, &cell, 1_000).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (node, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let old = proofs.pop().unwrap();
    f.node = node;
    let mut publisher = f.publisher(&cell);
    let materialized = publisher.materialize_bundle(&old).await.unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (node, _) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    f.count.reset();
    let checkpoint = f
        .directory
        .checkpoint_bundle_cell(&f.node, &cell.authority, &old, Limits::default(), NOW)
        .await
        .unwrap();
    let reads: Vec<_> = f
        .count
        .requests()
        .into_iter()
        .filter(|read| read.location.ends_with(".cnb"))
        .collect();
    assert_eq!(
        reads.len(),
        2,
        "checkpoint should read only its authenticated header and chosen shard"
    );
    assert_eq!(f.count.put_requests(), 2);
    f.node = checkpoint;
    // A lost caller reply may retry the same exact checkpoint against a newer
    // node observation. The already installed base leaves the hot suffix intact.
    f.count.reset();
    let repeated = f
        .directory
        .checkpoint_bundle_cell(&f.node, &cell.authority, &old, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(
        repeated.advertisement().bundle_head(),
        f.node.advertisement().bundle_head()
    );
    assert_eq!(f.count.put_requests(), 0);
    let current = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    let suffix = f
        .directory
        .load_bundle_coverage(&cell.authority, &current, Limits::default())
        .await
        .unwrap();
    assert_eq!(suffix.base().unwrap(), materialized);
    assert_eq!(suffix.commit_sequence(), 3);
    assert_eq!(suffix.locator_count(), frames.len());
}

#[tokio::test]
async fn a_stale_proof_cannot_checkpoint_a_newer_materialized_endpoint() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (node, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let old = proofs.pop().unwrap();
    f.node = node;
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (node, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let latest = proofs.pop().unwrap();
    f.node = node;
    f.publisher(&cell)
        .materialize_bundle(&latest)
        .await
        .unwrap();
    f.count.reset();
    assert!(matches!(
        f.directory
            .checkpoint_bundle_cell(&f.node, &cell.authority, &old, Limits::default(), NOW)
            .await,
        Err(Error::PendingPublication)
    ));
    assert_eq!(
        f.count.put_requests(),
        0,
        "a stale endpoint must not change the catalog"
    );
    f.directory
        .checkpoint_bundle_cell(&f.node, &cell.authority, &latest, Limits::default(), NOW)
        .await
        .unwrap();
}
