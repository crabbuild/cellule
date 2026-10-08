use super::*;

#[tokio::test]
async fn selection_reads_one_fresh_cohort_object_instead_of_each_new_extent() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..(4 + MAX_FRAMES as u8) {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    let mut frames = Vec::new();
    let mut assignments = Vec::new();
    for cell in &mut cells {
        let (_, capture, assignment) = f.append(cell, 2);
        frames.extend(capture);
        assignments.push(assignment);
    }
    let now = f.node.advertisement().issued_at_ms();
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assignments, now)
        .await
        .unwrap();
    let path = f.layout.node_coverage_bundle_path(
        f.node.advertisement().session().as_bytes(),
        prepared.head.epoch,
        prepared.head.digest.as_bytes(),
    );
    f.count.reset();
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    let requests = f
        .count
        .requests()
        .into_iter()
        .filter(|request| request.location == path.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(proofs.len(), MAX_FRAMES);
    assert_eq!(node.advertisement().bundle_head(), Some(prepared.head));
    eprintln!(
        "fresh cohort object: cells={} reads={}",
        cells.len(),
        requests.len()
    );
    assert_eq!(
        requests.len(),
        1,
        "read the complete fresh object once per selection"
    );
    assert!(matches!(
        requests[0].kind,
        cellule_store::test_support::ObjectReadKind::Full
    ));
    for proof in &proofs {
        assert_eq!(proof.commit_sequence(), 2);
        assert_eq!(proof.locator_count(), 1);
    }
    f.node = node;
    // The read is scoped to the operation. Neither an earlier live selection
    // nor its prepared bytes can authorize a retry after origin disappears.
    f.count.block_body_reads_for(&path);
    let puts = f.count.put_requests();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), puts);
}

#[tokio::test]
async fn checkpoint_cohort_uses_two_puts_and_preserves_a_hot_cell_suffix() {
    let mut f = Fixture::new().await;
    let mut cells = Vec::new();
    for number in 4..(4 + MAX_FRAMES as u8) {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    let mut frames = Vec::new();
    let mut assigned = Vec::new();
    for cell in &mut cells {
        let (_, capture, range) = f.append(cell, 2);
        frames.extend(capture);
        assigned.push(range);
    }
    let proposal = f
        .directory
        .prepare_node_bundle(
            &f.node,
            &frames,
            &assigned,
            f.node.advertisement().issued_at_ms(),
        )
        .await
        .unwrap();
    let (node, proofs) = f
        .directory
        .select_node_bundle(
            &f.node,
            &proposal,
            &f.lease,
            Limits::default(),
            f.node.advertisement().issued_at_ms(),
        )
        .await
        .unwrap();
    f.node = node;
    assert_eq!(proofs.len(), MAX_FRAMES);
    f.count.reset();
    for cell in &cells {
        let proof = proofs
            .iter()
            .find(|proof| proof.binding() == cell.control.value().bundle_binding.unwrap())
            .unwrap();
        f.publisher(cell).materialize_bundle(proof).await.unwrap();
    }
    eprintln!(
        "materialization cohort: cells={} puts={}",
        cells.len(),
        f.count.put_requests()
    );
    assert_eq!(
        f.count.put_requests(),
        MAX_FRAMES * 4,
        "a singleton materialized suffix should use the canonical native pack"
    );
    let (_, frames, assigned) = f.append(&mut cells[0], 3);
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
    let checkpoints: Vec<_> = cells
        .iter()
        .map(|cell| {
            let proof = proofs
                .iter()
                .find(|proof| proof.binding() == cell.control.value().bundle_binding.unwrap())
                .unwrap();
            (&cell.authority, proof)
        })
        .collect();
    f.count.reset();
    f.node = f
        .directory
        .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(
        f.count.put_requests(),
        2,
        "64 root checkpoints share one index upload and one CAS"
    );
    for (number, cell) in cells.iter().enumerate() {
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
        assert_eq!(proof.base().unwrap().commit_sequence, 2);
        assert_eq!(proof.commit_sequence(), if number == 0 { 3 } else { 2 });
        assert_eq!(
            proof.locator_count(),
            if number == 0 { frames.len() } else { 0 }
        );
    }
}

#[tokio::test]
async fn checkpoint_cohort_refuses_duplicates_and_unmaterialized_participants_without_partial_cas()
{
    let mut f = Fixture::new().await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (_, mut frames, a_range) = f.append(&mut a, 2);
    let (_, other, b_range) = f.append(&mut b, 2);
    frames.extend(other);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[a_range, b_range], NOW)
        .await
        .unwrap();
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    let a_proof = proofs
        .iter()
        .find(|proof| proof.binding() == a.control.value().bundle_binding.unwrap())
        .unwrap();
    let b_proof = proofs
        .iter()
        .find(|proof| proof.binding() == b.control.value().bundle_binding.unwrap())
        .unwrap();
    f.publisher(&a).materialize_bundle(a_proof).await.unwrap();
    f.count.reset();
    assert!(matches!(
        f.directory
            .checkpoint_bundle_cells(
                &f.node,
                &[(&a.authority, a_proof), (&b.authority, b_proof)],
                Limits::default(),
                NOW
            )
            .await,
        Err(Error::PendingPublication)
    ));
    assert_eq!(
        f.count.put_requests(),
        0,
        "a failed cohort selects none of its roots"
    );
    assert!(matches!(
        f.directory
            .checkpoint_bundle_cells(
                &f.node,
                &[(&a.authority, a_proof), (&a.authority, a_proof)],
                Limits::default(),
                NOW
            )
            .await,
        Err(Error::Fenced)
    ));
    assert!(matches!(
        f.directory
            .checkpoint_bundle_cells(&f.node, &[], Limits::default(), NOW)
            .await,
        Err(Error::Capacity(_))
    ));
    assert_eq!(f.count.put_requests(), 0);
    let selected = f
        .directory
        .load_bundle_coverage(&a.authority, &a.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(
        selected.base().unwrap().commit_sequence,
        1,
        "the first root is still uncheckpointed"
    );
    assert!(selected.locator_count() > 0);
}
