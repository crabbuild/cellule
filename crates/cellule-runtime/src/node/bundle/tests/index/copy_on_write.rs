use super::*;

#[tokio::test]
async fn updating_one_of_a_thousand_cells_does_not_rewrite_the_complete_inventory() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    inventory(&mut f, &cell, 1_000).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    f.count.reset();
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let frame_bytes: usize = frames.iter().map(|frame| frame.encoded().len()).sum();
    assert!(
        prepared.body.len() <= frame_bytes + (64 << 10),
        "one changed Cell rewrote {} metadata bytes",
        prepared.body.len() - frame_bytes
    );
    eprintln!(
        "catalog update: cells=1000 metadata_bytes={} native_bytes={frame_bytes}",
        prepared.body.len() - frame_bytes
    );
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs.len(), 1);
    assert_eq!(f.count.put_requests(), 2, "indexing adds no extra PUT");
    f.node = selected;
    f.count.reset();
    let recovered = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(recovered.commit_sequence(), 2);
    let catalog_reads: Vec<_> = f
        .count
        .requests()
        .into_iter()
        .filter(|read| read.location.ends_with(".cnb"))
        .collect();
    assert!(
        catalog_reads
            .iter()
            .all(|read| read.kind == cellule_store::test_support::ObjectReadKind::Range),
        "one-Cell lookup downloaded complete bundle objects: {catalog_reads:?}"
    );
    assert_eq!(
        catalog_reads.len(),
        4,
        "one header, one shard, one history and one exact native frame"
    );
}
