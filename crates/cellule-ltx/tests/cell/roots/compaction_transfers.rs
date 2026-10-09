//! Slow source transfers must not stall unrelated compaction inputs.

use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    collections::HashSet,
    fmt,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Duration,
};

#[derive(Debug)]
struct TransferGateStore {
    inner: InMemory,
    kind: Mutex<Option<&'static str>>,
    first: AtomicBool,
    paths: Mutex<HashSet<Path>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    entered: tokio::sync::Notify,
    fifth: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl TransferGateStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            kind: Mutex::new(None),
            first: AtomicBool::new(true),
            paths: Mutex::new(HashSet::new()),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            fifth: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }
}

impl fmt::Display for TransferGateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TransferGateStore")
    }
}

struct ActiveTransfer<'a>(&'a AtomicUsize);
impl Drop for ActiveTransfer<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[async_trait::async_trait]
impl ObjectStore for TransferGateStore {
    async fn put_opts(
        &self,
        path: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, bytes, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let kind = *self.kind.lock().unwrap();
        let selected = !opts.head
            && (opts.range.is_some() || kind == Some("pack"))
            && kind.is_some()
            && path.extension() == kind;
        let _active = selected.then(|| {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak.fetch_max(active, Ordering::AcqRel);
            ActiveTransfer(&self.active)
        });
        if selected {
            let fifth = {
                let mut paths = self.paths.lock().unwrap();
                paths.insert(path.clone()) && paths.len() == 5
            };
            if fifth {
                self.fifth.notify_one();
            }
            if self.first.swap(false, Ordering::AcqRel) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
        }
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[tokio::test]
async fn compaction_body_transfers_refill_before_first_source_finishes() {
    verify_transfer_refill("ltx").await;
}

#[tokio::test]
async fn compaction_index_transfers_refill_before_first_source_finishes() {
    verify_transfer_refill("index").await;
}

#[tokio::test]
async fn packed_compaction_transfers_refill_before_first_source_finishes() {
    verify_transfer_refill("pack").await;
}

async fn verify_transfer_refill(kind: &'static str) {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(TransferGateStore::new());
    let cell = replica(Store::new(backend.clone()), [233; 32], [234; 16]);
    let mut writer = Db::open(&directory.path().join("writer.sqlite"), Limits::default()).unwrap();
    let padding = if kind == "pack" { 96 } else { 300_000 };
    writer
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(0); CREATE TABLE padding(v)")?;
            tx.execute("INSERT INTO padding VALUES(randomblob(?1))", [padding])?;
            Ok(())
        })
        .unwrap();
    let mut root = cell
        .prepare(None, &writer.capture().unwrap(), 1, 1)
        .await
        .unwrap()
        .root();
    for sequence in 2..=5 {
        writer
            .transaction(|tx| {
                tx.execute_batch("UPDATE counter SET value=value+1")?;
                tx.execute("UPDATE padding SET v=randomblob(?1)", [padding])?;
                Ok(())
            })
            .unwrap();
        root = cell
            .prepare(Some(&root), &writer.capture().unwrap(), sequence, 1)
            .await
            .unwrap()
            .root();
    }
    assert_eq!(cell.open_root(&root).await.unwrap().segment_count(), 5);
    *backend.kind.lock().unwrap() = Some(kind);
    let compactor = cell.clone();
    let scratch = directory.path().to_owned();
    let task =
        tokio::spawn(async move { compactor.prepare_compaction(&root, 0..5, 1, &scratch).await });
    tokio::time::timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    // The first transfer stays pending. The other three slots must refill,
    // without changing the four-transfer limit or descriptor lineage order.
    let refilled = tokio::time::timeout(Duration::from_secs(2), backend.fifth.notified()).await;
    backend.resume.notify_one();
    let compacted = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    *backend.kind.lock().unwrap() = None;
    assert_eq!(backend.active.load(Ordering::Acquire), 0);
    assert!(backend.peak.load(Ordering::Acquire) <= 4);
    assert_eq!(compacted.root().position, root.position);
    assert_eq!(compacted.root().commit_sequence, root.commit_sequence);
    let repeated = cell
        .prepare_compaction(&root, 0..5, 1, directory.path())
        .await
        .unwrap();
    assert_eq!(
        repeated.root(),
        compacted.root(),
        "completion order cannot change immutable bytes"
    );
    let restored = directory.path().join("restored.sqlite");
    compacted.verified().restore(&restored).await.unwrap();
    let db = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        4
    );
    drop(db);
    writer.close().unwrap();
    assert!(
        refilled.is_ok(),
        "a stalled {kind} source held completed transfer slots"
    );
}
