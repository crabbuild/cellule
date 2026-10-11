//! A checkpoint-bearing predecessor can overlap a native successor safely.
use super::actor::{Authority, transport};
use super::composed::HeldSelection;
use super::pipeline::ManagedStore;
use super::receipt_pressure::submission;
use super::*;
use crate::cell::worker::SqlWorkerPool;
use crate::node::durability::NodeDurability;
use crate::node::log_shipper::NodeLogShipper;
use futures_util::poll;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn checkpoint_predecessor_overlaps_successor_and_cold_restores_both_exact_histories() {
    checkpoint_case(128 << 20, Outcome::Complete).await;
}

#[tokio::test]
async fn checkpoint_predecessor_overlaps_with_original_64_mib_budget() {
    checkpoint_case(64 << 20, Outcome::Complete).await;
}

#[tokio::test]
async fn both_cohorts_compose_checkpoints_and_credit_their_own_ordered_selections() {
    checkpoint_case(64 << 20, Outcome::SuccessorCheckpoint).await;
}

#[tokio::test]
async fn failed_checkpoint_predecessor_joins_accepted_successor_and_preserves_upload_source() {
    checkpoint_case(64 << 20, Outcome::FailedUpload).await;
}

#[tokio::test]
async fn fenced_checkpoint_predecessor_joins_uploads_after_shutdown_waiter_cancellation() {
    checkpoint_case(64 << 20, Outcome::Fenced).await;
}

#[tokio::test]
async fn checkpoint_base_missing_after_upload_prevents_both_selections_and_joins_successor() {
    checkpoint_case(64 << 20, Outcome::MissingRoot).await;
}

#[derive(Clone, Copy)]
enum Outcome {
    Complete,
    SuccessorCheckpoint,
    FailedUpload,
    Fenced,
    MissingRoot,
}

async fn checkpoint_case(retained: usize, outcome: Outcome) {
    let store = Arc::new(ManagedStore::default());
    let mut f = Fixture::with_store(store.clone()).await;
    super::coverage::enroll(&mut f).await;
    let mut cells = [f.cell(4).await, f.cell(5).await];
    let original = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let authority = Arc::new(HeldSelection {
        original: original.clone(),
        calls: AtomicUsize::new(0),
        combined: AtomicUsize::new(0),
        largest_combined: AtomicUsize::new(0),
        hold_after: 2,
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let peers = transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        original,
        peers,
        f.lease.clone(),
    ));
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(retained).unwrap();
    durability
        .attach_selection_resources(pool.resource_ledger())
        .unwrap();
    durability
        .start_bundle_publication(authority.clone())
        .unwrap();

    let seed = durability
        .submit_capture(submission(&mut cells[0], 2))
        .await
        .unwrap();
    let seed_selected = seed.selection.as_ref().unwrap().selected().await.unwrap();
    let root = f
        .publisher(&cells[0])
        .materialize_bundle(&seed_selected.proof)
        .await
        .unwrap();
    // Joining the original catalog lock proves the seed round has returned;
    // a ready receipt alone can precede the round's final cleanup.
    drop(authority.original.observed.lock().await);
    store.armed.store(true, Ordering::Release);
    let filler = durability
        .submit_capture(submission(&mut cells[1], 2))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
        .await
        .unwrap();
    // Fill the prior round's successor before queueing the checkpoint. Without
    // this barrier the checkpoint can become that successor, and this test
    // would measure the intentional two-cohort bound instead of its guard.
    let prior_successor = durability
        .submit_capture(submission(&mut cells[1], 3))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), store.second_done.notified())
        .await
        .unwrap();
    store.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(3), authority.entered.notified())
        .await
        .unwrap();
    let successor_checkpoint = matches!(outcome, Outcome::SuccessorCheckpoint);
    let second_base = if successor_checkpoint {
        let selected = filler.selection.as_ref().unwrap().selected().await.unwrap();
        let root = f
            .publisher(&cells[1])
            .materialize_bundle(&selected.proof)
            .await
            .unwrap();
        Some((root, selected))
    } else {
        None
    };
    let first = durability
        .submit_capture(submission(&mut cells[0], 3))
        .await
        .unwrap();
    let mut checkpoint = Box::pin(tokio::task::unconstrained(
        durability.checkpoint_materialized(cells[0].authority.clone(), root, seed_selected.clone()),
    ));
    assert!(poll!(checkpoint.as_mut()).is_pending());

    // Queue the original callback and native work behind the prior selection;
    // the held PUT therefore belongs to their combined catalog proposal.
    // Both prior PUTs joined before resetting this task-local fault counter.
    store.puts.store(0, Ordering::Release);
    authority.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
        .await
        .unwrap();
    let fault = matches!(
        outcome,
        Outcome::FailedUpload | Outcome::Fenced | Outcome::MissingRoot
    );
    store
        .hold_second
        .store(fault || successor_checkpoint, Ordering::Release);
    store
        .fail_first
        .store(matches!(outcome, Outcome::FailedUpload), Ordering::Release);
    let mut second_checkpoint = second_base.as_ref().map(|(root, selected)| {
        Box::pin(tokio::task::unconstrained(
            durability.checkpoint_materialized(cells[1].authority.clone(), *root, selected.clone()),
        ))
    });
    if let Some(callback) = &mut second_checkpoint {
        assert!(poll!(callback.as_mut()).is_pending());
    }
    let second = durability
        .submit_capture(submission(&mut cells[1], 4))
        .await
        .unwrap();
    let successor_entered = if fault || successor_checkpoint {
        &store.second_entered
    } else {
        &store.second_done
    };
    let overlap = tokio::time::timeout(Duration::from_secs(2), successor_entered.notified())
        .await
        .is_ok();
    assert!(!first.selection.as_ref().unwrap().is_ready());
    assert!(!second.selection.as_ref().unwrap().is_ready());
    assert!(
        poll!(checkpoint.as_mut()).is_pending(),
        "upload supplies no checkpoint credit"
    );
    assert_eq!(
        durability.progress().unwrap().tiered_through,
        prior_successor.assignment.ticket().last_sequence()
    );
    if fault {
        let prior_head = f
            .directory
            .load(SessionId::from_bytes([1; 16]), NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .bundle_head();
        if matches!(outcome, Outcome::Fenced) {
            f.lease.fence();
        }
        let root_path = f.layout.incarnation_object_path(
            &root.cell,
            &root.incarnation,
            &root.digest,
            cellule_ltx::CellObjectKind::Root,
        );
        if matches!(outcome, Outcome::MissingRoot) {
            f.count.block_body_reads_for(&root_path);
        }
        let mut joined = Box::pin(durability.shutdown());
        assert!(poll!(joined.as_mut()).is_pending());
        store.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(3), f.lease.wait_fenced())
            .await
            .unwrap();
        let checkpoint_error = tokio::time::timeout(Duration::from_secs(3), checkpoint.as_mut())
            .await
            .unwrap()
            .unwrap_err();
        if matches!(outcome, Outcome::FailedUpload) {
            assert!(format!("{checkpoint_error:?}").contains("first bundle PUT failed"));
        }
        assert!(
            poll!(joined.as_mut()).is_pending(),
            "accepted successor PUT was abandoned"
        );
        assert_eq!(
            durability.progress().unwrap().tiered_through,
            prior_successor.assignment.ticket().last_sequence()
        );
        assert_eq!(
            f.directory
                .load(SessionId::from_bytes([1; 16]), NOW)
                .await
                .unwrap()
                .unwrap()
                .advertisement()
                .bundle_head(),
            prior_head
        );
        drop(joined);
        store.second_resume.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(3), durability.shutdown())
            .await
            .unwrap()
            .unwrap_err();
        if matches!(outcome, Outcome::FailedUpload) {
            assert!(format!("{error:?}").contains("first bundle PUT failed"));
        }
        if matches!(outcome, Outcome::MissingRoot) {
            f.count.unblock_body_reads_for(&root_path);
        }
        drop(checkpoint);
        drop(seed_selected);
        drop(seed);
        drop(filler);
        drop(prior_successor);
        drop(first);
        drop(second);
        assert_eq!(
            pool.resource_ledger()
                .snapshot()
                .unwrap()
                .used
                .retained_bytes(),
            0
        );
        pool.shutdown().await.unwrap();
        assert!(overlap, "fault did not exercise both accepted uploads");
        return;
    }
    store.resume.notify_one();
    let (checkpoint_result, first_selected, second_selected) = if successor_checkpoint {
        let (checkpoint_result, first_selected) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    checkpoint.as_mut(),
                    first.selection.as_ref().unwrap().selected()
                )
            })
            .await
            .unwrap();
        checkpoint_result.unwrap();
        assert!(poll!(second_checkpoint.as_mut().unwrap().as_mut()).is_pending());
        assert!(!second.selection.as_ref().unwrap().is_ready());
        assert_eq!(
            durability.progress().unwrap().tiered_through,
            first.assignment.ticket().last_sequence()
        );
        store.second_resume.notify_one();
        let (checkpoint_result, second_selected) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    second_checkpoint.as_mut().unwrap(),
                    second.selection.as_ref().unwrap().selected()
                )
            })
            .await
            .unwrap();
        (checkpoint_result, first_selected, second_selected)
    } else {
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                checkpoint.as_mut(),
                first.selection.as_ref().unwrap().selected(),
                second.selection.as_ref().unwrap().selected()
            )
        })
        .await
        .unwrap()
    };
    checkpoint_result.unwrap();
    let selected = [first_selected.unwrap(), second_selected.unwrap()];
    assert_eq!(
        authority.combined.load(Ordering::Acquire),
        if successor_checkpoint { 2 } else { 1 }
    );
    assert_eq!(selected[0].proof.base().unwrap(), root);
    if let Some((root, _)) = &second_base {
        assert_eq!(selected[1].proof.base().unwrap(), *root);
    }

    for (index, (cell, selected)) in cells.iter_mut().zip(&selected).enumerate() {
        let overlay = selected
            .proof
            .recovery_overlay(&f.layout, Limits::default())
            .await
            .unwrap();
        let restored = cell
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let destination = f
            .scratch
            .path()
            .join(format!("checkpoint-pipeline-{index}.sqlite"));
        cell.replica
            .open_root(&restored.root())
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap();
        let database = rusqlite::Connection::open(destination).unwrap();
        let outcomes: Vec<(String, String)> = database
            .prepare("SELECT request, result FROM outcomes ORDER BY request")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let mut expected = vec![
            ("request-2".into(), "result-2".into()),
            ("request-3".into(), "result-3".into()),
        ];
        if index == 1 {
            expected.push(("request-4".into(), "result-4".into()));
        }
        expected.push(("seed".into(), "original".into()));
        assert_eq!(outcomes, expected);
        cell.control = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        let root = f
            .publisher(cell)
            .materialize_bundle(&selected.proof)
            .await
            .unwrap();
        durability
            .checkpoint_materialized(cell.authority.clone(), root, selected.clone())
            .await
            .unwrap();
        cell.control = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        durability
            .close_bundle_cell(&cell.authority, &cell.control)
            .await
            .unwrap();
    }
    durability.shutdown().await.unwrap();
    drop(selected);
    drop(seed_selected);
    drop(seed);
    drop(filler);
    drop(prior_successor);
    drop(first);
    drop(second);
    drop(checkpoint);
    drop(second_checkpoint);
    drop(second_base);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    pool.shutdown().await.unwrap();
    assert!(
        overlap,
        "checkpoint-bearing predecessor serialized immutable successor work"
    );
}
