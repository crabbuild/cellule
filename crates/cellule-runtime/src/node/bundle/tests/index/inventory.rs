use super::*;

#[tokio::test]
async fn streamed_maintenance_inventory_verifies_the_last_shard_before_accepting_closure() {
    let mut f = Fixture::new().await;
    let cell = f.cell(4).await;
    inventory(&mut f, &cell, 1_000).await;
    let head = f.node.advertisement().bundle_head().unwrap();
    let mut catalog = load_catalog(&f.layout, head_session(&f), head)
        .await
        .unwrap();
    // Synthetic terminal rows exercise catalog integrity only. No Cell release,
    // root ownership, process closure or collection is granted by this fixture.
    for binding in &mut catalog.bindings {
        binding.phase = BindingPhase::Closed;
        binding.terminal = Some((
            binding.selected_sequence,
            binding.selected_commit,
            binding.selected_position,
        ));
    }
    let proposal = f
        .directory
        .upload_catalog(Some(head), catalog, &[])
        .await
        .unwrap();
    f.node = f
        .directory
        .select_catalog(&f.node, &proposal, NOW)
        .await
        .unwrap();
    f.count.reset();
    store::ensure_session_drained(&f.layout, head_session(&f), Some(proposal.head))
        .await
        .unwrap();
    let reads: Vec<_> = f
        .count
        .requests()
        .into_iter()
        .filter(|read| read.location.ends_with(".cnb"))
        .collect();
    assert!(reads.len() > 240, "closure checks every nonempty shard");
    assert!(
        reads
            .iter()
            .all(|read| read.kind == cellule_store::test_support::ObjectReadKind::Range)
    );
    let mut corrupted = proposal.body.to_vec();
    *corrupted.last_mut().unwrap() ^= 1;
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, proposal.head.digest.as_bytes());
    f.layout
        .store()
        .put_overwrite(&path, Bytes::from(corrupted))
        .await
        .unwrap();
    assert!(
        store::ensure_session_drained(&f.layout, head_session(&f), Some(proposal.head))
            .await
            .is_err()
    );
}
