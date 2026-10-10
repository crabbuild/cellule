use super::actor::{Authority, transport};
use super::*;
use crate::cell::worker::SqlWorkerPool;
use crate::fleet::resource::{ResourceCost, ResourceLedger, ResourceReservation};
use crate::node::durability::{
    BundleCheckpoint, NodeBundleAuthority, NodeBundlePublicationAuthority, NodeDurability,
};
use crate::node::log_shipper::{AssignedCapture, NodeLogShipper, NodeLogSubmission};
use futures_util::future::BoxFuture;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct HeldReceiptCredit {
    original: Arc<Authority>,
    ledger: ResourceLedger,
    held: Mutex<Option<ResourceReservation>>,
    selects: AtomicUsize,
    checkpoints: AtomicUsize,
    selected: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl NodeBundleAuthority for HeldReceiptCredit {
    fn bind<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        NodeBundleAuthority::bind(self.original.as_ref(), authority, observed)
    }
    fn close<'a>(
        &'a self,
        authority: &'a CellAuthority,
        observed: &'a VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        NodeBundleAuthority::close(self.original.as_ref(), authority, observed, issued)
    }
}
impl NodeBundlePublicationAuthority for HeldReceiptCredit {
    fn select<'a>(
        &'a self,
        captures: &'a [AssignedCapture],
        checkpoints: &'a [BundleCheckpoint],
        prefixes: &'a [&'a BundleCoverageProof],
        preparation: Option<&'a mut crate::node::bundle::BundlePreparation>,
        lease: &'a NodeLeaseGuard,
    ) -> BoxFuture<'a, Result<Vec<BundleCoverageProof>>> {
        Box::pin(async move {
            let proofs = self
                .original
                .select(captures, checkpoints, prefixes, preparation, lease)
                .await?;
            if self.selects.fetch_add(1, Ordering::SeqCst) == 1 {
                let budget = self.ledger.snapshot()?;
                let remaining = budget.limit.retained_bytes() - budget.used.retained_bytes();
                // Reproduce an older root admission retaining the remaining
                // credit until its canonical checkpoint callback joins.
                *self.held.lock().unwrap() = Some(
                    self.ledger
                        .try_reserve(ResourceCost::zero().with_retained_bytes(remaining))?,
                );
                self.selected.notify_one();
                self.resume.notified().await;
            }
            Ok(proofs)
        })
    }
    fn checkpoint<'a>(&'a self, checkpoints: &'a [BundleCheckpoint]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.original.checkpoint(checkpoints).await?;
            self.checkpoints.fetch_add(1, Ordering::SeqCst);
            drop(self.held.lock().unwrap().take());
            Ok(())
        })
    }
}

pub(super) fn submission(cell: &mut Cell, commit: u64) -> NodeLogSubmission {
    cell.db
        .transaction(|tx| {
            tx.execute(
                "INSERT INTO outcomes VALUES(?1,?2)",
                [format!("request-{commit}"), format!("result-{commit}")],
            )
        })
        .unwrap();
    let cuts = cell.db.capture().unwrap();
    NodeLogSubmission::new(
        ApplicationId::from_bytes([9; 16]),
        cell.control.value().cell,
        cell.control.value().incarnation,
        cell.control.value().epoch,
        commit,
        &cuts,
    )
    .unwrap()
}

#[tokio::test]
async fn producer_waits_for_receipt_credit_and_services_the_checkpoint_that_releases_it() {
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
    let authority = Arc::new(HeldReceiptCredit {
        original,
        ledger: pool.resource_ledger(),
        held: Mutex::new(None),
        selects: AtomicUsize::new(0),
        checkpoints: AtomicUsize::new(0),
        selected: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    durability
        .start_bundle_publication(authority.clone())
        .unwrap();
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
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        authority.selected.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        durability.progress().unwrap().tiered_through,
        first.assignment.ticket().last_sequence()
    );
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        32 << 20
    );
    authority.resume.notify_one();
    let checkpoint =
        durability.checkpoint_materialized(cell.authority.clone(), root, first_selected.clone());
    let receipt = second.selection.as_ref().unwrap().selected();
    let (checkpoint, receipt) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(checkpoint, receipt)
    })
    .await
    .unwrap();
    checkpoint.unwrap();
    let receipt = receipt.unwrap();
    f.lease.check().unwrap();
    assert_eq!(authority.checkpoints.load(Ordering::SeqCst), 1);
    assert_eq!(
        authority.selects.load(Ordering::SeqCst),
        2,
        "credit wait must not repeat the durable selection"
    );
    assert_eq!(receipt.proof.commit_sequence(), 3);
    assert_eq!(
        durability.progress().unwrap().tiered_through,
        second.assignment.ticket().last_sequence()
    );
    let expected = 20 * 1024 * 1024
        + crate::node::bundle::PREPARATION_BYTES
        + first_selected.proof.retained_metadata_bytes().unwrap()
        + receipt.proof.retained_metadata_bytes().unwrap();
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        expected
    );
    cell.control = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    let root = f
        .publisher(&cell)
        .materialize_bundle(&receipt.proof)
        .await
        .unwrap();
    durability
        .checkpoint_materialized(cell.authority.clone(), root, receipt.clone())
        .await
        .unwrap();
    let cold = f.scratch.path().join("receipt-pressure-cold.sqlite");
    cell.replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&cold)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(cold).unwrap();
    let outcomes: Vec<(String, String)> = db
        .prepare("SELECT request,result FROM outcomes ORDER BY request")
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
    drop(first_selected);
    drop(receipt);
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
