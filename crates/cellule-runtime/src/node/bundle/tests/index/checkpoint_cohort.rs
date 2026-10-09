use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn checkpoint_fresh_roots_overlap_within_original_working_admission() {
    held_checkpoint_bases(12).await;
}

#[tokio::test]
async fn checkpoint_fresh_bodies_join_before_upload_and_exact_cold_recovery() {
    held_checkpoint_bases(13).await;
}

async fn held_checkpoint_bases(mode: u8) {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
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
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    f.node = node;
    for proof in &proofs {
        let cell = cells
            .iter()
            .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
            .unwrap();
        f.publisher(cell).materialize_bundle(proof).await.unwrap();
        f.heartbeat().await;
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
    let now = f.node.advertisement().issued_at_ms();
    {
        let checkpoint =
            f.directory
                .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), now);
        tokio::pin!(checkpoint);
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
            _ = &mut checkpoint => panic!("held checkpoint bases must prevent upload and CAS"),
            result = tokio::time::timeout(std::time::Duration::from_secs(5), overlap) => {
                println!("checkpoint original dependencies started before release={}", faults.base_started.load(Ordering::SeqCst));
                result.unwrap();
            },
        }
        assert_eq!(
            faults.base_started.load(Ordering::SeqCst),
            8,
            "checkpoint verification must overlap only the admitted first eight bases"
        );
    }
    assert_eq!(f.count.put_requests(), 0);
    faults.mode.store(0, Ordering::SeqCst);
    faults.node_resume.notify_waiters();
    let original = cells[0]
        .authority
        .load(cells[0].control.value().cell)
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    let missing = f.layout.incarnation_object_path(
        &original.cell,
        &original.incarnation,
        &original.digest,
        cellule_ltx::CellObjectKind::Root,
    );
    f.count.block_body_reads_for(&missing);
    f.count.reset();
    assert!(
        f.directory
            .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), now)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), 0);
    f.count.unblock_body_reads_for(&missing);
    f.count.reset();
    f.node = f
        .directory
        .checkpoint_bundle_cells(&f.node, &checkpoints, Limits::default(), now)
        .await
        .unwrap();
    assert_eq!(
        f.count
            .requests()
            .iter()
            .filter(|request| request.location.ends_with(".root"))
            .count(),
        cells.len(),
        "each eligible checkpoint must freshly authenticate its original root once"
    );
    assert_eq!(f.count.put_requests(), 2);
    for (number, cell) in cells.iter().enumerate() {
        let current = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        let selected = f
            .directory
            .load_bundle_coverage(&cell.authority, &current, Limits::default())
            .await
            .unwrap();
        assert_eq!(selected.locator_count(), 0);
        assert_eq!(selected.commit_sequence(), 2);
        let root = selected.base().unwrap();
        let cold = CellReplica::new(
            f.layout.clone(),
            root.cell,
            root.incarnation,
            Limits::default(),
        )
        .unwrap();
        let destination = f
            .scratch
            .path()
            .join(format!("checkpoint-cohort-{number}.sqlite"));
        cold.open_root(&root)
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
}
