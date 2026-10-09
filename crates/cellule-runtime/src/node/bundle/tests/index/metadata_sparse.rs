use super::*;

#[tokio::test]
async fn sparse_binding_shards_share_bounded_fresh_reads_and_exact_cold_recovery() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..68 {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    // Only these 64 original writers have available roots. The other rows
    // populate intervening shards and grant no reconstruction capability.
    inventory(&mut f, &cells[0], 1_937).await;
    let head = f.node.advertisement().bundle_head().unwrap();
    let path = f.layout.node_coverage_bundle_path(
        head_session(&f).as_bytes(),
        head.epoch,
        head.digest.as_bytes(),
    );
    let mut frames = Vec::new();
    let mut assigned = Vec::new();
    for cell in &mut cells {
        let (_, capture, range) = f.append(cell, 2);
        frames.extend(capture);
        assigned.push(range);
    }
    let now = f.node.advertisement().issued_at_ms();
    f.count.reset();
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assigned, now)
        .await
        .unwrap();
    let reads = f
        .count
        .requests()
        .iter()
        .filter(|request| request.location == path.as_ref())
        .count();
    eprintln!("sparse 2,000-binding preparation: cells=64 reads={reads}");
    assert!(
        reads <= 5,
        "shared sparse shards should use at most four bounded windows plus the fresh header; got {reads}"
    );
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    assert_eq!(proofs.len(), cells.len());
    for proof in &proofs {
        assert_eq!(proof.commit_sequence(), 2);
        let cell = cells
            .iter()
            .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
            .unwrap();
        let overlay = proof
            .recovery_overlay(&f.layout, Limits::default())
            .await
            .unwrap();
        let recovered = cell
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let destination = f.scratch.path().join(format!(
            "sparse-metadata-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&recovered.root())
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(destination).unwrap();
        let outcomes = db
            .prepare("SELECT request, result FROM outcomes ORDER BY request")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                ("request-2".into(), "result-2".into()),
                ("seed".into(), "original".into())
            ]
        );
    }
    // This call must reopen origin; neither an earlier preparation nor the
    // selected proof can replace missing original metadata in a later call.
    f.count.block_body_reads_for(&path);
    let puts = f.count.put_requests();
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &frames, &assigned, now)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), puts);
}
