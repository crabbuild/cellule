use super::*;

#[tokio::test]
async fn zero_command_bootstrap_enrolls_and_materializes_the_first_command() {
    let mut f = Fixture::new().await;
    let mut cell = f.unbound_cell_at_commit(4, [9; 16], 0).await;
    let initial = cell.control.value().ltx_root().unwrap();
    assert_eq!(initial.commit_sequence, 0);
    let (node, control) = f
        .directory
        .bind_bundle_cell(&f.node, &cell.authority, &cell.control, NOW)
        .await
        .unwrap();
    f.node = node;
    cell.control = control;
    let (_, frames, assigned) = f.append(&mut cell, 1);
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
    let proof = proofs.pop().unwrap();
    assert_eq!(proof.base().unwrap(), initial);
    assert_eq!(proof.commit_sequence(), 1);
    let root = f.publisher(&cell).materialize_bundle(&proof).await.unwrap();
    assert_eq!(root.commit_sequence, 1);
    assert_eq!(root.position, proof.position());
    f.directory
        .checkpoint_bundle_cell(&node, &cell.authority, &proof, Limits::default(), NOW)
        .await
        .unwrap();
    let file = f.scratch.path().join("bootstrap-restored.sqlite");
    cell.replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&file)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(file).unwrap();
    let result: String = db
        .query_row(
            "SELECT result FROM outcomes WHERE request='request-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(result, "result-1");
}

#[tokio::test]
async fn quiet_zero_command_bootstrap_can_drain_without_inventing_an_issued_command() {
    let mut f = Fixture::new().await;
    let mut cell = f.unbound_cell_at_commit(4, [9; 16], 0).await;
    let (node, control) = f
        .directory
        .bind_bundle_cell(&f.node, &cell.authority, &cell.control, NOW)
        .await
        .unwrap();
    cell.control = control;
    let pin = cell.control.value().bundle_binding.unwrap();
    let issued = f
        .gate
        .close_cell_issuance(
            Fixture::scope(&cell),
            cell.control.value().ltx_root().unwrap(),
        )
        .unwrap();
    assert_eq!(issued.commit_sequence(), 0);
    assert_eq!(issued.last_node_sequence(), 0);
    let closing = f
        .directory
        .begin_bundle_close(&node, pin, issued, NOW)
        .await
        .unwrap();
    let closed = f
        .directory
        .finish_bundle_close(&closing, pin, issued, NOW)
        .await
        .unwrap();
    f.directory.withdraw(&closed, NOW).await.unwrap();
}
