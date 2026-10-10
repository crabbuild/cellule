use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn fresh_base_root_reads_overlap_without_exceeding_the_original_admission() {
    held_bases(12).await;
}

#[tokio::test]
async fn fresh_base_body_reads_overlap_without_selecting_before_verification() {
    held_bases(13).await;
}

async fn held_bases(mode: u8) {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..14 {
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
    let now = f.node.advertisement().issued_at_ms();
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assigned, now)
        .await
        .unwrap();
    f.count.reset();
    faults.mode.store(mode, Ordering::SeqCst);
    {
        let selection =
            f.directory
                .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), now);
        tokio::pin!(selection);
        let overlap = async {
            loop {
                let started = faults.node_started.notified();
                if faults.base_started.load(Ordering::SeqCst) >= 8 {
                    break;
                }
                started.await;
            }
        };
        tokio::select! {
            _ = &mut selection => panic!("held base dependencies must prevent selection"),
            result = tokio::time::timeout(std::time::Duration::from_secs(5), overlap) => result.unwrap(),
        }
        assert_eq!(
            faults.base_started.load(Ordering::SeqCst),
            8,
            "the next base cohort cannot exceed the admitted eight operations"
        );
    }
    assert_eq!(
        f.count.put_requests(),
        0,
        "cancelled bases select no authority"
    );
    faults.mode.store(0, Ordering::SeqCst);
    faults.node_resume.notify_waiters();
    f.count.reset();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    assert_eq!(proofs.len(), cells.len());
    let roots = f
        .count
        .requests()
        .into_iter()
        .filter(|request| request.location.ends_with(".root"))
        .count();
    assert_eq!(roots, cells.len(), "one fresh root body per base operation");
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
            "base-cohort-{}.sqlite",
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
                ("seed".into(), "original".into())
            ]
        );
    }
    // Prior successful verification does not establish current availability.
    let base = cells[0].control.value().ltx_root().unwrap();
    let path = f.layout.incarnation_object_path(
        &base.cell,
        &base.incarnation,
        &base.digest,
        cellule_ltx::CellObjectKind::Root,
    );
    f.count.block_body_reads_for(&path);
    let puts = f.count.put_requests();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), now)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), puts);
}
