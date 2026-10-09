use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn preparation_overlaps_catalog_shards_under_the_original_metadata_bound() {
    held_metadata(14).await;
}

#[tokio::test]
async fn preparation_overlaps_requested_histories_without_granting_cancelled_coverage() {
    held_metadata(15).await;
}

async fn held_metadata(mode: u8) {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..20 {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    let shards = cells
        .iter()
        .map(|cell| catalog_index::shard(&[9; 16], cell.control.value().cell.as_bytes()))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(shards.len() > 8);
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
    let catalog = load_catalog(&f.layout, head_session(&f), first.head)
        .await
        .unwrap();
    for cell in &cells {
        let pin = cell.control.value().bundle_binding.unwrap().digest;
        let history = catalog_index::history_extent(&catalog, pin).unwrap();
        faults.held_metadata.lock().unwrap().push((
            f.layout
                .node_coverage_bundle_path(
                    head_session(&f).as_bytes(),
                    first.head.epoch,
                    history.object.unwrap().as_bytes(),
                )
                .to_string(),
            history.offset,
        ));
    }
    frames.clear();
    assigned.clear();
    for cell in &mut cells {
        let (_, capture, range) = f.append(cell, 3);
        frames.extend(capture);
        assigned.push(range);
    }
    f.count.reset();
    faults.mode.store(mode, Ordering::SeqCst);
    {
        let preparation = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &assigned, now);
        tokio::pin!(preparation);
        let overlap = async {
            loop {
                let started = faults.node_started.notified();
                if faults.range_started.load(Ordering::SeqCst) >= 8 {
                    break;
                }
                started.await;
            }
        };
        tokio::select! {
            _ = &mut preparation => panic!("held metadata must prevent upload and selection"),
            result = tokio::time::timeout(std::time::Duration::from_secs(5), overlap) => result.unwrap(),
        }
        assert_eq!(
            faults.range_started.load(Ordering::SeqCst),
            8,
            "no ninth metadata read may enter a held cohort"
        );
    }
    assert_eq!(
        f.count.put_requests(),
        0,
        "cancelled metadata grants no upload or authority"
    );
    faults.mode.store(0, Ordering::SeqCst);
    faults.node_resume.notify_waiters();
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assigned, now)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), now)
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
            "metadata-{}.sqlite",
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
    let dependency = Path::from(faults.held_metadata.lock().unwrap()[0].0.clone());
    f.count.block_body_reads_for(&dependency);
    let puts = f.count.put_requests();
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &frames, &assigned, now)
            .await
            .is_err()
    );
    assert_eq!(
        f.count.put_requests(),
        puts,
        "earlier verification cannot replace missing metadata"
    );
}
