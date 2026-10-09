use super::*;

#[tokio::test]
async fn selection_groups_fresh_historical_extents_across_sixty_four_cells() {
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
    let native_bytes = frames
        .iter()
        .map(|frame| frame.encoded().len())
        .sum::<usize>();
    let now = f.node.advertisement().issued_at_ms();
    let first = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assignments, now)
        .await
        .unwrap();
    let (node, _) = f
        .directory
        .select_node_bundle(&f.node, &first, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    f.node = node;
    let path = f.layout.node_coverage_bundle_path(
        f.node.advertisement().session().as_bytes(),
        first.head.epoch,
        first.head.digest.as_bytes(),
    );
    frames.clear();
    assignments.clear();
    for cell in &mut cells {
        let (_, capture, assignment) = f.append(cell, 3);
        frames.extend(capture);
        assignments.push(assignment);
    }
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assignments, now)
        .await
        .unwrap();
    f.count.reset();
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    let reads = f
        .count
        .requests()
        .into_iter()
        .filter(|request| request.location == path.as_ref())
        .collect::<Vec<_>>();
    eprintln!(
        "historical cohort: cells={} native_bytes={} reads={}",
        cells.len(),
        native_bytes,
        reads.len()
    );
    assert_eq!(
        reads.len(),
        1,
        "one fresh contiguous historical window, rather than a request per Cell"
    );
    assert_eq!(proofs.len(), MAX_FRAMES);
    for proof in &proofs {
        assert_eq!(proof.commit_sequence(), 3);
        assert_eq!(proof.locator_count(), 2);
        let restored = proof
            .recovery_overlay(&f.layout, Limits::default())
            .await
            .unwrap();
        assert_eq!(restored.final_commit_sequence(), 3);
        let cell = cells
            .iter()
            .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
            .unwrap();
        let recovered = cell
            .replica
            .prepare_recovered_overlay(&restored, 1)
            .await
            .unwrap();
        let cold = f.scratch.path().join(format!(
            "cohort-cold-{}.sqlite",
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
    f.node = node;
    let (body, _) = f
        .layout
        .store()
        .get_with_etag_bounded(&path, MAX_BUNDLE_BYTES)
        .await
        .unwrap();
    let mut corrupt = body.to_vec();
    corrupt[proofs[0].binding.locators[0].offset as usize + 8] ^= 1;
    f.layout
        .store()
        .put_overwrite(&path, Bytes::from(corrupt))
        .await
        .unwrap();
    let puts = f.count.put_requests();
    assert!(matches!(
        f.directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
            .await,
        Err(Error::Node("bundle frame digest differs"))
    ));
    assert_eq!(
        f.count.put_requests(),
        puts,
        "corrupt historical frames select no new authority"
    );
    f.layout.store().put_overwrite(&path, body).await.unwrap();
    // A prior selected proof and a proposal still cannot replace a fresh read
    // of its historical dependency in a later operation.
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
async fn distinct_historical_reads_overlap_and_cancel_without_selecting_authority() {
    use std::sync::atomic::Ordering;
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = [f.cell(4).await, f.cell(5).await];
    // Each original Cell frame lives in a different immutable object.
    for cell in &mut cells {
        let (_, frames, assigned) = f.append(cell, 2);
        let proposal = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .unwrap();
        f.node = f
            .directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .unwrap()
            .0;
    }
    let mut frames = Vec::new();
    let mut assigned = Vec::new();
    for cell in &mut cells {
        let (_, capture, range) = f.append(cell, 3);
        frames.extend(capture);
        assigned.push(range);
    }
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assigned, NOW)
        .await
        .unwrap();
    faults.mode.store(11, Ordering::SeqCst);
    f.count.reset();
    {
        let selection =
            f.directory
                .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW);
        tokio::pin!(selection);
        let overlap = async {
            loop {
                let started = faults.node_started.notified();
                if faults.range_started.load(Ordering::SeqCst) == 2 {
                    break;
                }
                started.await;
            }
        };
        tokio::select! {
            _ = &mut selection => panic!("held historical reads must not select"),
            result = tokio::time::timeout(std::time::Duration::from_secs(5), overlap) => result.unwrap(),
        }
        // Drop the pending original selection while both fresh reads wait.
    }
    assert_eq!(
        f.count.put_requests(),
        0,
        "cancellation selects no new authority"
    );
    faults.mode.store(0, Ordering::SeqCst);
    faults.node_resume.notify_waiters();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(
        proofs.len(),
        2,
        "a subsequent operation owns fresh scratch admission"
    );
    assert!(proofs.iter().all(|proof| proof.commit_sequence() == 3));
}
