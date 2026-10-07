use super::*;

#[tokio::test]
async fn asynchronous_prefix_checkpoint_keeps_a_hot_selected_suffix() {
    verify_hot_materialization(true).await;
}

#[tokio::test]
async fn materializer_can_continue_from_a_newer_root_before_catalog_checkpoint() {
    verify_hot_materialization(false).await;
}

async fn verify_hot_materialization(checkpoint_first: bool) {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (node, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let old = proofs.pop().unwrap();
    f.node = node;
    // The writer advances selection while the older materialization is pending.
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (latest, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let mut publisher = f.publisher(&cell);
    let prefix = publisher.materialize_bundle(&old).await.unwrap();
    assert_eq!(prefix.commit_sequence, 2);
    let current = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    let between = f
        .directory
        .load_bundle_coverage(&cell.authority, &current, Limits::default())
        .await
        .unwrap();
    assert_eq!(between.commit_sequence(), 3);
    let (node, proof) = if checkpoint_first {
        let checkpoint = f
            .directory
            .checkpoint_bundle_cell(&latest, &cell.authority, &old, Limits::default(), NOW)
            .await
            .unwrap();
        let current = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        let proof = f
            .directory
            .load_bundle_coverage(&cell.authority, &current, Limits::default())
            .await
            .unwrap();
        assert_eq!(proof.base().unwrap(), prefix);
        assert_eq!(proof.commit_sequence(), 3);
        assert_eq!(proof.locator_count(), frames.len());
        (checkpoint, proof)
    } else {
        // Origin lookup works between the root CAS and the catalog checkpoint.
        assert_eq!(between.binding(), proofs.pop().unwrap().binding());
        (latest, between)
    };
    let final_root = publisher.materialize_bundle(&proof).await.unwrap();
    assert_eq!(final_root.commit_sequence, 3);
    assert_eq!(
        publisher.materialize_bundle(&proof).await.unwrap(),
        final_root
    );
    let checkpoint = f
        .directory
        .checkpoint_bundle_cell(&node, &cell.authority, &proof, Limits::default(), NOW)
        .await
        .unwrap();
    let current = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    let final_proof = f
        .directory
        .load_bundle_coverage(&cell.authority, &current, Limits::default())
        .await
        .unwrap();
    assert_eq!(final_proof.locator_count(), 0);
    assert_eq!(final_proof.base().unwrap(), final_root);
    assert_eq!(
        checkpoint
            .advertisement()
            .bundle_head()
            .unwrap()
            .selected_through(),
        assigned.ticket().last_sequence()
    );
}

#[tokio::test]
async fn checkpoint_releases_locators_and_the_next_range_continues_exactly() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (selected, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert!(matches!(
        f.directory
            .checkpoint_bundle_cell(
                &selected,
                &cell.authority,
                &proofs[0],
                Limits::default(),
                NOW
            )
            .await,
        Err(Error::PendingPublication)
    ));
    let proof = proofs.pop().unwrap();
    let mut publisher = f.publisher(&cell);
    let materialized = publisher.materialize_bundle(&proof).await.unwrap();
    cell.control = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    f.node = f
        .directory
        .checkpoint_bundle_cell(&selected, &cell.authority, &proof, Limits::default(), NOW)
        .await
        .unwrap();
    let checkpoint = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(checkpoint.base().unwrap(), materialized);
    assert_eq!(checkpoint.locator_count(), 0);
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs[0].base().unwrap(), materialized);
    assert_eq!(proofs[0].commit_sequence(), 3);
    assert_eq!(proofs[0].locator_count(), frames.len());
}

#[tokio::test]
async fn quiet_binding_must_close_and_checkpoint_before_session_withdrawal() {
    let mut f = Fixture::new().await;
    let cell = f.cell(4).await;
    assert!(matches!(
        f.directory.withdraw(&f.node, NOW).await,
        Err(Error::PendingPublication)
    ));
    let pin = cell.control.value().bundle_binding.unwrap();
    let issued = f
        .gate
        .close_cell_issuance(
            Fixture::scope(&cell),
            cell.control.value().ltx_root().unwrap(),
        )
        .unwrap();
    assert_eq!(issued.last_node_sequence(), 0);
    let closing = f
        .directory
        .begin_bundle_close(&f.node, pin, issued, NOW)
        .await
        .unwrap();
    let closed = f
        .directory
        .finish_bundle_close(&closing, pin, issued, NOW)
        .await
        .unwrap();
    let proof = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    let checkpoint = f
        .directory
        .checkpoint_bundle_cell(&closed, &cell.authority, &proof, Limits::default(), NOW)
        .await
        .unwrap();
    f.directory.withdraw(&checkpoint, NOW).await.unwrap();
    assert!(f.directory.is_withdrawn(pin.session).await.unwrap());
}

#[tokio::test]
async fn selected_suffix_survives_original_database_loss_and_node_fencing() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (cuts, frames, assigned) = f.append(&mut cell, 2);
    let position = cuts.position;
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (selected, _) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    drop(cell.db);
    std::fs::remove_dir_all(f.scratch.path()).unwrap();
    let old = NOW + 30_000 + crate::node::STALE_ADVERTISEMENT_RETENTION_MS;
    // The node-record scanner must see only records, never coverage data objects.
    assert_eq!(f.directory.collect_stale(old, 1).await.unwrap(), 1);
    assert!(
        f.directory
            .is_retired(SessionId::from_bytes([1; 16]))
            .await
            .unwrap()
    );
    assert!(
        !f.directory
            .is_withdrawn(SessionId::from_bytes([1; 16]))
            .await
            .unwrap()
    );
    let record = f
        .directory
        .layout
        .store()
        .get_with_etag_bounded(&f.layout.node_path(&[1; 16]), crate::node::MAX_NODE_BYTES)
        .await
        .unwrap()
        .0;
    let crate::node::directory::NodeRecord::Tombstone(record) =
        crate::node::directory::NodeRecord::decode_canonical(&record).unwrap()
    else {
        panic!("original boot must be fenced");
    };
    assert_eq!(record.bundle, selected.advertisement().bundle_head());
    let reopened = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    let overlay = reopened
        .recovery_overlay(&f.layout, Limits::default())
        .await
        .unwrap();
    let prepared = cell
        .replica
        .prepare_recovered_overlay(&overlay, 1)
        .await
        .unwrap();
    assert_eq!(prepared.root().position, position);
    let recovery = tempfile::tempdir().unwrap();
    let file = recovery.path().join("restored.sqlite");
    cell.replica
        .open_root(&prepared.root())
        .await
        .unwrap()
        .restore(&file)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(file).unwrap();
    let result: String = db
        .query_row(
            "SELECT result FROM outcomes WHERE request='request-2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(result, "result-2");
}

#[tokio::test]
async fn materializer_failure_preserves_selected_proof_and_lagging_root() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, prepared.head.digest.as_bytes());
    f.count.block_body_reads_for(&path);
    let mut publisher = f.publisher(&cell);
    assert!(publisher.materialize_bundle(&proofs[0]).await.is_err());
    assert_eq!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .root,
        cell.control.value().root
    );
    assert_eq!(
        f.directory
            .load(SessionId::from_bytes([1; 16]), NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .bundle_head(),
        selected.advertisement().bundle_head()
    );
    f.count.unblock_body_reads_for(&path);
    assert_eq!(
        publisher
            .materialize_bundle(&proofs[0])
            .await
            .unwrap()
            .commit_sequence,
        2
    );
}

#[tokio::test]
async fn untracked_legacy_issuance_cannot_close_a_cell() {
    let mut f = Fixture::new().await;
    let cell = f.cell(4).await;
    f.gate.issue(1).unwrap();
    assert!(matches!(
        f.gate.close_cell_issuance(
            Fixture::scope(&cell),
            cell.control.value().ltx_root().unwrap()
        ),
        Err(Error::Fenced)
    ));
}
