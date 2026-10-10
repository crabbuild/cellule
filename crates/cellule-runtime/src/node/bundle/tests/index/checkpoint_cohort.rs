//! Independent root checks share bounded scratch before one checkpoint CAS.
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn checkpoint_authority_reads_overlap_before_any_prefix_changes() {
    held_checkpoints(16).await;
}

#[tokio::test]
async fn checkpoint_root_reads_overlap_within_original_working_credit() {
    held_checkpoints(12).await;
}

#[tokio::test]
async fn checkpoint_body_reads_overlap_and_cancellation_selects_no_prefix() {
    held_checkpoints(13).await;
}

async fn held_checkpoints(mode: u8) {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let mut cells = Vec::new();
    for number in 4..14 {
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
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    f.node = node;
    for proof in &proofs {
        let cell = cells
            .iter()
            .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
            .unwrap();
        f.publisher(cell).materialize_bundle(proof).await.unwrap();
    }
    let checkpoints: Vec<_> = proofs
        .iter()
        .map(|proof| {
            let cell = cells
                .iter()
                .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
                .unwrap();
            (&cell.authority, proof)
        })
        .collect();
    f.count.reset();
    faults.mode.store(mode, Ordering::SeqCst);
    let entered = if mode == 16 {
        &faults.control_started
    } else {
        &faults.base_started
    };
    let overlap = {
        let checkpoint =
            f.directory
                .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), now);
        tokio::pin!(checkpoint);
        let ready = async {
            loop {
                let started = faults.node_started.notified();
                if entered.load(Ordering::SeqCst) >= 8 {
                    break;
                }
                started.await;
            }
        };
        tokio::select! {
            _ = &mut checkpoint => panic!("held checkpoint dependencies must prevent CAS"),
            result = tokio::time::timeout(std::time::Duration::from_secs(2), ready) => result.is_ok(),
        }
    };
    let started = entered.load(Ordering::SeqCst);
    assert_eq!(
        f.count.put_requests(),
        0,
        "cancelled checks select no prefix"
    );
    assert!(
        started <= 8,
        "checkpoint exceeds the original scratch cohort"
    );
    faults.mode.store(0, Ordering::SeqCst);
    faults.node_resume.notify_waiters();

    // A successful materializer or a prior cancelled check supplies no cached
    // availability. One unavailable new root rejects the entire checkpoint.
    let root = checkpoints[0]
        .0
        .load(checkpoints[0].1.binding.control.cell)
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    let path = f.layout.incarnation_object_path(
        &root.cell,
        &root.incarnation,
        &root.digest,
        cellule_ltx::CellObjectKind::Root,
    );
    f.count.block_body_reads_for(&path);
    assert!(
        f.directory
            .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), now)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), 0);
    f.count.unblock_body_reads_for(&path);
    f.count.reset();
    let checkpoint = f
        .directory
        .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), now)
        .await
        .unwrap();
    assert_eq!(f.count.put_requests(), 2, "one upload and one complete CAS");
    assert_eq!(
        f.count
            .requests()
            .iter()
            .filter(|request| request.location.ends_with(".root"))
            .count(),
        cells.len(),
        "each checkpoint starts one fresh root verification"
    );
    f.node = checkpoint;
    for cell in &cells {
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
        assert_eq!(proof.commit_sequence(), 2);
        assert_eq!(proof.locator_count(), 0);
        let destination = f.scratch.path().join(format!(
            "checkpoint-{}.sqlite",
            current.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&proof.base().unwrap())
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap();
        let restored = rusqlite::Connection::open(destination).unwrap();
        let outcomes: Vec<(String, String)> = restored
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
                ("seed".into(), "original".into())
            ]
        );
    }
    assert!(
        overlap,
        "independent checkpoint reads remain serialized: {started}"
    );
    assert_eq!(started, 8);
}
