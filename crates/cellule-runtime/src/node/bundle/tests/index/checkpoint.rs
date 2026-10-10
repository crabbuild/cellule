use super::*;

#[tokio::test]
async fn checkpoint_continuation_preserves_receipts_from_an_intermediate_base() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    macro_rules! select {
        ($sequence:literal) => {{
            let (_, frames, assigned) = f.append(&mut cell, $sequence);
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
            f.node = node;
            proofs.pop().unwrap()
        }};
    }
    let first = select!(2);
    let old_receipt = select!(3);
    let mut publisher = f.publisher(&cell);
    let root = publisher.materialize_bundle(&first).await.unwrap();
    let checkpoint = first.materialized_prefix(root, None).unwrap();
    f.node = f
        .directory
        .checkpoint_bundle_cell(&f.node, &cell.authority, &first, Limits::default(), NOW)
        .await
        .unwrap();
    let intermediate_receipt = select!(4);
    intermediate_receipt
        .continues_selected_prefix(&old_receipt, Some(&checkpoint))
        .unwrap();
    // The materializer still owns an older receipt, while new captures have
    // already been selected against the first checkpoint's base.
    let next_root = publisher.materialize_bundle(&old_receipt).await.unwrap();
    assert!(
        old_receipt
            .materialized_prefix(root, Some(&checkpoint))
            .is_err()
    );
    let next_checkpoint = old_receipt
        .materialized_prefix(next_root, Some(&checkpoint))
        .unwrap();
    f.node = f
        .directory
        .checkpoint_bundle_cell(
            &f.node,
            &cell.authority,
            &old_receipt,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let latest = select!(5);
    latest
        .continues_selected_prefix(&intermediate_receipt, Some(&next_checkpoint))
        .unwrap();
    let third_root = publisher
        .materialize_bundle(&intermediate_receipt)
        .await
        .unwrap();
    let third_checkpoint = intermediate_receipt
        .materialized_prefix(third_root, Some(&next_checkpoint))
        .unwrap();
    f.node = f
        .directory
        .checkpoint_bundle_cell(
            &f.node,
            &cell.authority,
            &intermediate_receipt,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let newest = select!(6);
    newest
        .continues_selected_prefix(&latest, Some(&third_checkpoint))
        .unwrap();
    assert!(
        newest
            .continues_selected_prefix(&latest, Some(&next_checkpoint))
            .is_err()
    );
    assert!(
        third_checkpoint.retained_bytes() <= MaterializedBundlePrefix::maximum_retained_bytes()
    );
}

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
    let (node, mut extended_proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let extended = extended_proofs.pop().unwrap();
    extended.continues_selected_prefix(&old, None).unwrap();
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
        3,
        "checkpoint reads its authenticated header, chosen shard and exact history; no native frames"
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
    let mut suffix = f
        .directory
        .load_bundle_coverage(&cell.authority, &current, Limits::default())
        .await
        .unwrap();
    assert_eq!(suffix.base().unwrap(), materialized);
    assert_eq!(suffix.commit_sequence(), 3);
    assert_eq!(suffix.locator_count(), frames.len());
    let confirmed = old.materialized_prefix(materialized, None).unwrap();
    assert!(suffix.continues_selected_prefix(&extended, None).is_err());
    suffix
        .continues_selected_prefix(&extended, Some(&confirmed))
        .unwrap();
    let mut foreign_root = materialized;
    foreign_root.digest[0] ^= 1;
    let foreign = old.materialized_prefix(foreign_root, None).unwrap();
    assert!(
        suffix
            .continues_selected_prefix(&extended, Some(&foreign))
            .is_err()
    );
    let retained = suffix.binding.locators.clone();
    suffix.binding.locators.clear();
    assert!(
        suffix
            .continues_selected_prefix(&extended, Some(&confirmed))
            .is_err(),
        "the confirmed root cannot erase its later selected suffix"
    );
    suffix.binding.locators = retained.clone();
    suffix.binding.locators[0].frame_digest = Digest::from_bytes([7; 32]);
    assert!(
        suffix
            .continues_selected_prefix(&extended, Some(&confirmed))
            .is_err(),
        "retained debt must be byte-identical"
    );
    suffix.binding.locators = retained;
    let mut changed_prefix = extended;
    changed_prefix.binding.locators[0].frame_digest = Digest::from_bytes([6; 32]);
    assert!(
        suffix
            .continues_selected_prefix(&changed_prefix, Some(&confirmed))
            .is_err(),
        "the materialized original prefix must match its witness"
    );
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
