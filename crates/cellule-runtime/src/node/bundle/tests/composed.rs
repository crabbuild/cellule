//! Original producer callbacks join a combined checkpoint/native selection.
use super::actor::{Authority, transport};
use super::receipt_pressure::submission;
use super::*;
use crate::cell::worker::SqlWorkerPool;
use crate::node::durability::{
    BundleCheckpoint, NodeBundleAuthority, NodeBundlePublicationAuthority, NodeDurability,
};
use crate::node::log_shipper::{AssignedCapture, NodeLogShipper};
use futures_util::{future::BoxFuture, poll};
use std::sync::atomic::{AtomicUsize, Ordering};

struct HeldSelection {
    original: Arc<Authority>,
    calls: AtomicUsize,
    combined: AtomicUsize,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl NodeBundleAuthority for HeldSelection {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        self.original.bind(authority, observed)
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        self.original.close(authority, observed, issued)
    }
}
impl NodeBundlePublicationAuthority for HeldSelection {
    fn select<'a>(
        &'a self,
        captures: &'a [AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async move {
            let result = self.original.select(captures, checkpoints, lease).await?;
            self.combined.fetch_add(checkpoints.len(), Ordering::SeqCst);
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            Ok(result)
        })
    }
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>> {
        self.original.checkpoint(checkpoints)
    }
}

#[tokio::test]
async fn producer_combines_ready_callback_with_queued_native_work_and_joins_complete_drain() {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let original = Arc::new(Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let peers = transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        original.clone(),
        peers,
        f.lease.clone(),
    ));
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(32 << 20).unwrap();
    durability
        .attach_selection_resources(pool.resource_ledger())
        .unwrap();
    let held = Arc::new(HeldSelection {
        original,
        calls: AtomicUsize::new(0),
        combined: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    durability.start_bundle_publication(held.clone()).unwrap();
    let first = durability
        .submit_capture(submission(&mut cell, 2))
        .await
        .unwrap();
    let first_selected = first.selection.as_ref().unwrap().selected().await.unwrap();
    let root = f
        .publisher(&cell)
        .materialize_bundle(&first_selected.proof)
        .await
        .unwrap();
    let second = durability
        .submit_capture(submission(&mut cell, 3))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), held.entered.notified())
        .await
        .unwrap();
    let third = durability
        .submit_capture(submission(&mut cell, 4))
        .await
        .unwrap();
    let checkpoint = tokio::task::unconstrained(durability.checkpoint_materialized(
        cell.authority.clone(),
        root,
        first_selected.clone(),
    ));
    tokio::pin!(checkpoint);
    // Remove only this test poll's cooperative budget: the available bounded
    // send cannot yield before accepting the notification. Pending then means
    // the original callback is queued and awaiting the held producer's completion.
    assert!(poll!(checkpoint.as_mut()).is_pending());
    held.resume.notify_one();
    let (checkpoint, second_selected, third_selected) =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                checkpoint,
                second.selection.as_ref().unwrap().selected(),
                third.selection.as_ref().unwrap().selected()
            )
        })
        .await
        .unwrap();
    checkpoint.unwrap();
    second_selected.unwrap();
    let last = third_selected.unwrap();
    assert_eq!(held.combined.load(Ordering::SeqCst), 1);
    assert_eq!(last.proof.base().unwrap(), root);
    assert_eq!(last.proof.commit_sequence(), 4);
    assert_eq!(last.proof.locator_count(), 2);
    cell.control = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    let final_root = f
        .publisher(&cell)
        .materialize_bundle(&last.proof)
        .await
        .unwrap();
    durability
        .checkpoint_materialized(cell.authority.clone(), final_root, last.clone())
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
    durability.shutdown().await.unwrap();
    drop(first);
    drop(second);
    drop(third);
    drop(first_selected);
    drop(last);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    pool.shutdown().await.unwrap();
}
