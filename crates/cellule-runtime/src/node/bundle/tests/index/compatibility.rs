use super::*;

#[tokio::test]
async fn inline_indexed_suffix_migrates_to_detached_history_with_the_same_pin() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    f.node = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    let original = f.node.advertisement().bundle_head().unwrap();
    let mut catalog = load_catalog(&f.layout, head_session(&f), original)
        .await
        .unwrap();
    catalog.index = None;
    catalog.predecessor = Some(original.digest());
    let (body, digest) = catalog_index::encode_inline(&mut catalog, &[]).unwrap();
    assert_eq!(&body[..8], b"\0\0\0\x04CNB2");
    let legacy = PreparedNodeBundle {
        original: Some(original),
        head: NodeBundleHead {
            epoch: EPOCH,
            digest,
            selected_through: original.selected_through(),
        },
        body,
        catalog,
    };
    f.layout
        .store()
        .put_exact(
            &f.layout
                .node_coverage_bundle_path(&[1; 16], EPOCH, digest.as_bytes()),
            legacy.body.clone(),
        )
        .await
        .unwrap();
    f.node = f
        .directory
        .select_catalog(&f.node, &legacy, NOW)
        .await
        .unwrap();
    let old_pin = cell.control.value().bundle_binding.unwrap();
    assert_eq!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .unwrap()
            .commit_sequence(),
        2
    );
    let (_, frames, assignment) = f.append(&mut cell, 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    assert_eq!(&proposal.body[..8], b"\0\0\0\x04CNB3");
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs[0].binding(), old_pin);
    assert_eq!(proofs[0].locator_count(), 2);
    assert_eq!(proofs[0].commit_sequence(), 3);
}

#[tokio::test]
async fn legacy_selected_catalog_is_read_and_migrated_without_changing_the_cell_pin() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let original = f.node.advertisement().bundle_head().unwrap();
    let mut catalog = load_catalog(&f.layout, head_session(&f), original)
        .await
        .unwrap();
    catalog.index = None;
    catalog.predecessor = Some(original.digest());
    let body = codec::encode(&mut catalog, &[]).unwrap();
    let legacy = PreparedNodeBundle {
        original: Some(original),
        head: NodeBundleHead {
            epoch: EPOCH,
            digest: Digest::from_bytes(*blake3::hash(&body).as_bytes()),
            selected_through: 0,
        },
        body,
        catalog,
    };
    f.layout
        .store()
        .put_exact(
            &f.layout
                .node_coverage_bundle_path(&[1; 16], EPOCH, legacy.head.digest.as_bytes()),
            legacy.body.clone(),
        )
        .await
        .unwrap();
    f.node = f
        .directory
        .select_catalog(&f.node, &legacy, NOW)
        .await
        .unwrap();
    let old_pin = cell.control.value().bundle_binding.unwrap();
    let loaded = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(loaded.binding(), old_pin);
    assert_eq!(loaded.commit_sequence(), 1);
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    assert_eq!(&proposal.body[..8], b"\0\0\0\x04CNB3");
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs[0].binding(), old_pin);
    assert_eq!(proofs[0].commit_sequence(), 2);
}

#[tokio::test]
async fn indexed_shard_corruption_and_missing_reused_shards_fail_closed() {
    let mut f = Fixture::new().await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    assert_ne!(
        catalog_index::shard(
            a.authority.layout().application_id(),
            a.control.value().cell.as_bytes()
        ),
        catalog_index::shard(
            b.authority.layout().application_id(),
            b.control.value().cell.as_bytes()
        )
    );
    let (_, frames, assigned) = f.append(&mut a, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (selected, _) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = selected;
    let old_path =
        f.layout
            .node_coverage_bundle_path(&[1; 16], EPOCH, proposal.head.digest.as_bytes());
    let (_, frames, assigned) = f.append(&mut b, 2);
    let sibling = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (selected, _) = f
        .directory
        .select_node_bundle(&f.node, &sibling, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = selected;
    f.count.reset();
    assert_eq!(
        f.directory
            .load_bundle_coverage(&a.authority, &a.control, Limits::default())
            .await
            .unwrap()
            .commit_sequence(),
        2
    );
    let reads: Vec<_> = f
        .count
        .requests()
        .into_iter()
        .filter(|read| read.location.ends_with(".cnb"))
        .collect();
    assert_eq!(
        reads.len(),
        4,
        "a reused shard/history does not require the old root header"
    );
    assert_eq!(
        reads
            .iter()
            .filter(|read| read.location == old_path.as_ref())
            .count(),
        3
    );
    // The head/header stays unchanged; the independently authenticated reused
    // shard must still detect a corrupt provider payload in the old object.
    let mut corrupted = proposal.body.to_vec();
    corrupted[catalog_index::HEADER_BYTES + 8] ^= 1;
    f.layout
        .store()
        .put_overwrite(&old_path, Bytes::from(corrupted))
        .await
        .unwrap();
    assert!(
        f.directory
            .load_bundle_coverage(&a.authority, &a.control, Limits::default())
            .await
            .is_err()
    );
    // Unrelated Cell data remains readable; complete maintenance inventory must
    // refuse the missing/corrupt dependency rather than omitting this Cell.
    assert_eq!(
        f.directory
            .load_bundle_coverage(&b.authority, &b.control, Limits::default())
            .await
            .unwrap()
            .commit_sequence(),
        2
    );
    assert!(
        load_catalog(
            &f.layout,
            head_session(&f),
            f.node.advertisement().bundle_head().unwrap()
        )
        .await
        .is_err()
    );
    f.layout.store().delete(&old_path).await.unwrap();
    assert!(
        f.directory
            .load_bundle_coverage(&a.authority, &a.control, Limits::default())
            .await
            .is_err()
    );
}
