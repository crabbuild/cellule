use super::*;

#[tokio::test]
async fn preparation_reads_one_catalog_window_and_one_history_window_for_a_shared_cohort() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..68 {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    inventory(&mut f, &cells[0], 1_937).await;
    let mut frames = Vec::new();
    let mut assigned = Vec::new();
    for cell in &mut cells {
        let (_, capture, range) = f.append(cell, 2);
        frames.extend(capture);
        assigned.push(range);
    }
    let now = f.node.advertisement().issued_at_ms();
    let first = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assigned, now)
        .await
        .unwrap();
    f.node = f
        .directory
        .select_node_bundle(&f.node, &first, &f.lease, Limits::default(), now)
        .await
        .unwrap()
        .0;
    let path = f.layout.node_coverage_bundle_path(
        head_session(&f).as_bytes(),
        first.head.epoch,
        first.head.digest.as_bytes(),
    );
    frames.clear();
    assigned.clear();
    for cell in &mut cells {
        let (_, capture, range) = f.append(cell, 3);
        frames.extend(capture);
        assigned.push(range);
    }
    f.count.reset();
    let next = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assigned, now)
        .await
        .unwrap();
    let requests = f
        .count
        .requests()
        .into_iter()
        .filter(|request| request.location == path.as_ref())
        .collect::<Vec<_>>();
    eprintln!(
        "shared metadata: cells={} reads={}",
        cells.len(),
        requests.len()
    );
    assert_eq!(
        requests.len(),
        3,
        "one fresh header, contiguous catalog window and contiguous history window"
    );
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &next, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    assert_eq!(proofs.len(), cells.len());
    for proof in &proofs {
        assert_eq!(proof.commit_sequence(), 3);
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
        let cold = f.scratch.path().join(format!(
            "metadata-window-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&recovered.root())
            .await
            .unwrap()
            .restore(&cold)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(cold).unwrap();
        let outcomes: Vec<(String, String)> = db
            .prepare("SELECT request, result FROM outcomes ORDER BY request")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                ("request-2".into(), "result-2".into()),
                ("request-3".into(), "result-3".into()),
                ("seed".into(), "original".into())
            ]
        );
    }
}
