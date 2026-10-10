//! The serving producer overlaps immutable work, credits in order and joins it.
use super::actor::{Authority, transport};
use super::receipt_pressure::submission;
use super::*;
use crate::cell::worker::SqlWorkerPool;
use crate::node::durability::NodeDurability;
use crate::node::log_shipper::{NodeLogShipper, SubmittedCapture};
use futures_util::poll;
use std::time::Duration;

use futures_util::stream::BoxStream;
use object_store::ObjectStore;
use std::sync::atomic::{AtomicBool, Ordering};
#[derive(Debug, Default)]
struct ManagedStore {
    inner: InMemory,
    armed: AtomicBool,
    puts: std::sync::atomic::AtomicUsize,
    hold_second: AtomicBool,
    second_entered: tokio::sync::Notify,
    second_done: tokio::sync::Notify,
    second_resume: tokio::sync::Notify,
    fail_first: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl std::fmt::Display for ManagedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagedStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ManagedStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let number = if path.as_ref().ends_with(".cnb") && self.armed.load(Ordering::Acquire) {
            Some(self.puts.fetch_add(1, Ordering::AcqRel))
        } else {
            None
        };
        if number == Some(0) {
            self.entered.notify_one();
            self.resume.notified().await;
            if self.fail_first.load(Ordering::Acquire) {
                // The canonical store preserves this permanent error's source.
                // PermissionDenied intentionally maps to a typed path-only
                // authorization error before publication sees it.
                return Err(object_store::Error::NotSupported {
                    source: Box::new(std::io::Error::other("first bundle PUT failed")),
                });
            }
        }
        if number == Some(1) {
            self.second_entered.notify_one();
            if self.hold_second.load(Ordering::Acquire) {
                self.second_resume.notified().await;
            }
        }
        let result = self.inner.put_opts(path, payload, options).await;
        if number == Some(1) {
            self.second_done.notify_one();
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct Case {
    f: Fixture,
    cells: [Cell; 2],
    store: Arc<ManagedStore>,
    durability: Arc<NodeDurability>,
    pool: SqlWorkerPool,
}
impl Case {
    async fn new(retained: usize, hold_second: bool, fail_first: bool) -> Self {
        let store = Arc::new(ManagedStore::default());
        let mut f = Fixture::with_store(store.clone()).await;
        super::coverage::enroll(&mut f).await;
        let cells = [f.cell(4).await, f.cell(5).await];
        let authority = Arc::new(Authority {
            directory: f.directory.clone(),
            observed: tokio::sync::Mutex::new(f.node.clone()),
        });
        let peers = transport(&f, true);
        let shipper =
            NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
        let durability = Arc::new(NodeDurability::new(
            f.gate.clone(),
            shipper,
            authority.clone(),
            peers,
            f.lease.clone(),
        ));
        let pool = SqlWorkerPool::new(1, 1).unwrap();
        pool.configure_retained_capacity(retained).unwrap();
        durability
            .attach_selection_resources(pool.resource_ledger())
            .unwrap();
        store.hold_second.store(hold_second, Ordering::Release);
        store.fail_first.store(fail_first, Ordering::Release);
        store.armed.store(true, Ordering::Release);
        durability.start_bundle_publication(authority).unwrap();
        Self {
            f,
            cells,
            store,
            durability,
            pool,
        }
    }
    async fn first(&mut self) -> SubmittedCapture {
        let pending = self
            .durability
            .submit_capture(submission(&mut self.cells[0], 2))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.store.entered.notified())
            .await
            .unwrap();
        pending
    }
    async fn second(&mut self) -> SubmittedCapture {
        self.durability
            .submit_capture(submission(&mut self.cells[1], 2))
            .await
            .unwrap()
    }
    async fn finish(mut self, pending: [SubmittedCapture; 2]) {
        let mut receipts = Vec::new();
        for pending in &pending {
            receipts.push(
                pending
                    .selection
                    .as_ref()
                    .unwrap()
                    .selected()
                    .await
                    .unwrap(),
            );
        }
        for (cell, selected) in self.cells.iter_mut().zip(&receipts) {
            assert!(
                selected.proof.retained_metadata_bytes().unwrap()
                    <= self.f.directory.bundle_receipt_memory_bound(1).unwrap()
            );
            let overlay = selected
                .proof
                .recovery_overlay(&self.f.layout, Limits::default())
                .await
                .unwrap();
            let restored = cell
                .replica
                .prepare_recovered_overlay(&overlay, 1)
                .await
                .unwrap();
            let cold = self.f.scratch.path().join(format!(
                "managed-pipeline-{}.sqlite",
                cell.control.value().cell.as_bytes()[0]
            ));
            cell.replica
                .open_root(&restored.root())
                .await
                .unwrap()
                .restore(&cold)
                .await
                .unwrap();
            let db = rusqlite::Connection::open(cold).unwrap();
            let result: String = db
                .query_row(
                    "SELECT result FROM outcomes WHERE request='request-2'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(result, "result-2");
            let root = self
                .f
                .publisher(cell)
                .materialize_bundle(&selected.proof)
                .await
                .unwrap();
            self.durability
                .checkpoint_materialized(cell.authority.clone(), root, selected.clone())
                .await
                .unwrap();
            cell.control = cell
                .authority
                .load(cell.control.value().cell)
                .await
                .unwrap()
                .unwrap();
            self.durability
                .close_bundle_cell(&cell.authority, &cell.control)
                .await
                .unwrap();
        }
        self.durability.shutdown().await.unwrap();
        drop(receipts);
        drop(pending);
        assert_eq!(
            self.pool
                .resource_ledger()
                .snapshot()
                .unwrap()
                .used
                .retained_bytes(),
            0
        );
        self.pool.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn managed_successor_upload_finishes_before_held_predecessor_without_early_credit() {
    let mut case = Case::new(128 << 20, false, false).await;
    let first = case.first().await;
    let second = case.second().await;
    tokio::time::timeout(Duration::from_secs(2), case.store.second_done.notified())
        .await
        .expect("the managed successor must upload while the first PUT is held");
    assert_eq!(case.durability.progress().unwrap().tiered_through, 0);
    assert!(!first.selection.as_ref().unwrap().is_ready());
    assert!(!second.selection.as_ref().unwrap().is_ready());
    case.store.resume.notify_one();
    case.finish([first, second]).await;
}

#[tokio::test]
async fn first_cohort_confirms_while_successor_upload_remains_held() {
    let mut case = Case::new(128 << 20, true, false).await;
    let first = case.first().await;
    let second = case.second().await;
    tokio::time::timeout(Duration::from_secs(2), case.store.second_entered.notified())
        .await
        .unwrap();
    case.store.resume.notify_one();
    let selected = tokio::time::timeout(
        Duration::from_secs(2),
        first.selection.as_ref().unwrap().selected(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(case.durability.progress().unwrap().tiered_through, 1);
    assert!(!second.selection.as_ref().unwrap().is_ready());
    assert_eq!(selected.proof.commit_sequence(), 2);
    drop(selected);
    case.store.second_resume.notify_one();
    case.finish([first, second]).await;
}

#[tokio::test]
async fn tight_original_budget_uses_one_cohort_and_returns_all_credit() {
    let mut case = Case::new(32 << 20, false, false).await;
    let first = case.first().await;
    let second = case.second().await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert_eq!(case.store.puts.load(Ordering::Acquire), 1);
    assert!(
        case.pool
            .resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes()
            < 32 << 20
    );
    case.store.resume.notify_one();
    case.finish([first, second]).await;
}

#[tokio::test]
async fn fenced_join_waits_for_both_accepted_uploads_after_caller_cancellation() {
    failed_join(false).await;
}
#[tokio::test]
async fn first_upload_failure_joins_later_upload_and_preserves_original_cause() {
    failed_join(true).await;
}
async fn failed_join(fail_first: bool) {
    let mut case = Case::new(128 << 20, true, fail_first).await;
    let first = case.first().await;
    let second = case.second().await;
    tokio::time::timeout(Duration::from_secs(2), case.store.second_entered.notified())
        .await
        .unwrap();
    if !fail_first {
        case.f.lease.fence();
    }
    let mut joined = Box::pin(case.durability.shutdown());
    assert!(poll!(joined.as_mut()).is_pending());
    case.store.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(2), case.f.lease.wait_fenced())
        .await
        .unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        poll!(joined.as_mut()).is_pending(),
        "the second accepted PUT still owns its lifetime"
    );
    assert_eq!(case.durability.progress().unwrap().tiered_through, 0);
    drop(joined);
    case.store.second_resume.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(3), case.durability.shutdown())
        .await
        .unwrap()
        .unwrap_err();
    if fail_first {
        assert!(format!("{error:?}").contains("first bundle PUT failed"));
    }
    drop(first);
    drop(second);
    assert_eq!(
        case.pool
            .resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    case.pool.shutdown().await.unwrap();
}
