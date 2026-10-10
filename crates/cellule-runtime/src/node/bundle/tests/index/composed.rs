use super::*;

#[tokio::test]
async fn ready_checkpoint_and_new_capture_share_one_catalog_upload_and_cas() {
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
    f.node = node;
    let original = proofs.pop().unwrap();
    let root = f
        .publisher(&cell)
        .materialize_bundle(&original)
        .await
        .unwrap();
    let (cuts, frames, assigned) = f.append(&mut cell, 3);
    f.count.reset();
    let proposal = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[(&cell.authority, &original)],
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (node, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    assert_eq!(
        f.count.put_requests(),
        2,
        "one catalog upload and one node authority CAS for both obligations"
    );
    let selected = proofs.pop().unwrap();
    assert_eq!(selected.base().unwrap(), root);
    assert_eq!(selected.commit_sequence(), 3);
    assert_eq!(selected.locator_count(), frames.len());
    let prefix = original.materialized_prefix(root, None).unwrap();
    selected
        .continues_selected_prefix(&original, Some(&prefix))
        .unwrap();
    let expected = cell
        .replica
        .prepare(Some(&root), &cuts, 3, 1)
        .await
        .unwrap();
    let current = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    let cold = f
        .directory
        .load_bundle_coverage(&cell.authority, &current, Limits::default())
        .await
        .unwrap();
    let overlay = cold
        .recovery_overlay(&f.layout, Limits::default())
        .await
        .unwrap();
    let recovered = cell
        .replica
        .prepare_recovered_overlay(&overlay, 1)
        .await
        .unwrap();
    let actual_path = f.scratch.path().join("combined-actual.sqlite");
    let expected_path = f.scratch.path().join("combined-expected.sqlite");
    cell.replica
        .open_root(&recovered.root())
        .await
        .unwrap()
        .restore(&actual_path)
        .await
        .unwrap();
    cell.replica
        .open_root(&expected.root())
        .await
        .unwrap()
        .restore(&expected_path)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(actual_path).unwrap(),
        std::fs::read(expected_path).unwrap()
    );
}

#[tokio::test]
async fn combined_cohort_preserves_independent_cells_and_a_prior_hot_suffix() {
    let mut f = Fixture::new().await;
    let mut cells = Vec::new();
    for byte in 4..12 {
        cells.push(f.cell(byte).await);
    }
    let mut first_frames = Vec::new();
    let mut first_assignments = Vec::new();
    for cell in &mut cells {
        let (_, frames, assigned) = f.append(cell, 2);
        first_frames.extend(frames);
        first_assignments.push(assigned);
    }
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &first_frames, &first_assignments, NOW)
        .await
        .unwrap();
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    for cell in &cells {
        let proof = proofs
            .iter()
            .find(|p| p.binding() == cell.control.value().bundle_binding.unwrap())
            .unwrap();
        f.publisher(cell).materialize_bundle(proof).await.unwrap();
    }
    let (_, hot_frames, hot_assigned) = f.append(&mut cells[0], 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &hot_frames, &[hot_assigned], NOW)
        .await
        .unwrap();
    let (node, _) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    let mut frames = Vec::new();
    let mut assignments = Vec::new();
    for (index, cell) in cells.iter_mut().enumerate() {
        let (_, capture, assigned) = f.append(cell, if index == 0 { 4 } else { 3 });
        frames.extend(capture);
        assignments.push(assigned);
    }
    let checkpoints = cells
        .iter()
        .map(|cell| {
            let proof = proofs
                .iter()
                .find(|p| p.binding() == cell.control.value().bundle_binding.unwrap())
                .unwrap();
            (&cell.authority, proof)
        })
        .collect::<Vec<_>>();
    f.count.reset();
    let proposal = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &assignments,
            &checkpoints,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (node, selected) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(f.count.put_requests(), 2);
    f.node = node;
    for (index, cell) in cells.iter().enumerate() {
        let current = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        let cold = f
            .directory
            .load_bundle_coverage(&cell.authority, &current, Limits::default())
            .await
            .unwrap();
        let live = selected
            .iter()
            .find(|p| p.binding() == cold.binding())
            .unwrap();
        assert_eq!(cold.commit_sequence(), if index == 0 { 4 } else { 3 });
        assert_eq!(cold.locator_count(), if index == 0 { 2 } else { 1 });
        assert_eq!(live.commit_sequence(), cold.commit_sequence());
        let overlay = cold
            .recovery_overlay(&f.layout, Limits::default())
            .await
            .unwrap();
        let recovered = cell
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let path = f.scratch.path().join(format!("composed-{index}.sqlite"));
        cell.replica
            .open_root(&recovered.root())
            .await
            .unwrap()
            .restore(&path)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(path).unwrap();
        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM outcomes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, if index == 0 { 4 } else { 3 });
        let result: String = db
            .query_row(
                "SELECT result FROM outcomes WHERE request='request-2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(result, "result-2");
    }
}

#[tokio::test]
async fn stale_checkpoint_and_combined_overflow_upload_nothing() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let mut proofs = Vec::new();
    for commit in 2..=3 {
        let (_, frames, assigned) = f.append(&mut cell, commit);
        let proposal = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .unwrap();
        let (node, mut selected) = f
            .directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .unwrap();
        f.node = node;
        proofs.push(selected.pop().unwrap());
    }
    f.publisher(&cell)
        .materialize_bundle(&proofs[1])
        .await
        .unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 4);
    f.count.reset();
    assert!(matches!(
        f.directory
            .prepare_node_bundle_with_checkpoints(
                &f.node,
                &frames,
                &[assigned],
                &[(&cell.authority, &proofs[0])],
                Limits::default(),
                NOW
            )
            .await,
        Err(Error::PendingPublication)
    ));
    assert_eq!(f.count.put_requests(), 0);
    let checkpoints = vec![(&cell.authority, &proofs[1]); MAX_FRAMES];
    assert!(matches!(
        f.directory
            .prepare_node_bundle_with_checkpoints(
                &f.node,
                &frames,
                &[assigned],
                &checkpoints,
                Limits::default(),
                NOW
            )
            .await,
        Err(Error::Capacity(_))
    ));
    assert_eq!(f.count.put_requests(), 0);
}

#[tokio::test]
async fn combined_upload_cannot_select_after_origin_loss_or_original_lease_fencing() {
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
    f.node = node;
    let proof = proofs.pop().unwrap();
    f.publisher(&cell).materialize_bundle(&proof).await.unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let proposal = f
        .directory
        .prepare_node_bundle_with_checkpoints(
            &f.node,
            &frames,
            &[assigned],
            &[(&cell.authority, &proof)],
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let path = f.layout.node_coverage_bundle_path(
        f.node.advertisement.session.as_bytes(),
        proposal.head.epoch,
        proposal.head.digest.as_bytes(),
    );
    f.count.reset();
    f.count.block_body_reads_for(&path);
    assert!(
        f.directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), 0);
    f.lease.fence();
    assert!(matches!(
        f.directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(f.count.put_requests(), 0);
    assert_eq!(
        f.directory
            .load(f.node.advertisement.session, NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement
            .bundle,
        f.node.advertisement.bundle
    );
}
