use super::*;
use futures_util::stream::BoxStream;
use object_store::ObjectStore;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct HeldFrameStore {
    inner: InMemory,
    armed: AtomicBool,
    reads: AtomicUsize,
    fail_second: AtomicBool,
    failed_path: std::sync::Mutex<Option<String>>,
    paths: std::sync::Mutex<std::collections::BTreeSet<String>>,
    first: Notify,
    second: Notify,
    third: Notify,
    resume: Notify,
    resume_third: Notify,
}
impl std::fmt::Display for HeldFrameStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldFrameStore")
    }
}
#[async_trait::async_trait]
impl ObjectStore for HeldFrameStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, payload, options).await
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
        if self.armed.load(Ordering::Acquire)
            && options.range.is_some()
            && path.as_ref().ends_with(".nodebundle")
        {
            self.paths.lock().unwrap().insert(path.to_string());
            if self.fail_second.load(Ordering::Acquire)
                && self.failed_path.lock().unwrap().as_deref() == Some(path.as_ref())
            {
                return Err(object_store::Error::Generic {
                    store: "HeldFrameStore",
                    source: std::io::Error::other("deliberate historical read failure").into(),
                });
            }
            match self.reads.fetch_add(1, Ordering::AcqRel) {
                0 => {
                    self.first.notify_one();
                    self.resume.notified().await;
                }
                1 => {
                    self.second.notify_one();
                    if self.fail_second.load(Ordering::Acquire) {
                        *self.failed_path.lock().unwrap() = Some(path.to_string());
                        return Err(object_store::Error::Generic {
                            store: "HeldFrameStore",
                            source: std::io::Error::other("deliberate historical read failure")
                                .into(),
                        });
                    }
                }
                2 if self.fail_second.load(Ordering::Acquire) => {
                    self.third.notify_one();
                    self.resume_third.notified().await;
                }
                _ => {}
            }
        }
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

#[tokio::test]
async fn failed_historical_window_joins_accepted_reads_and_publishes_no_root() {
    let store = Arc::new(HeldFrameStore::default());
    let mut f = Fixture::with_store(store.clone()).await;
    let mut cell = f.cell(4).await;
    let mut proof = None;
    for commit in 2..=11 {
        let (_, frames, assigned) = f.append(&mut cell, commit);
        let prepared = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .unwrap();
        let (node, mut proofs) = f
            .directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .unwrap();
        f.node = node;
        proof = proofs.pop();
    }
    let before = cell.control.value().encode().unwrap();
    let proof = proof.unwrap();
    let mut publisher = f.publisher(&cell);
    store.fail_second.store(true, Ordering::Release);
    store.armed.store(true, Ordering::Release);
    let materialized = publisher.materialize_bundle(&proof);
    tokio::pin!(materialized);
    tokio::select! {
        _ = store.third.notified() => {},
        _ = &mut materialized => panic!("accepted historical reads must remain joined"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => panic!("historical window was not dispatched"),
    }
    store.resume.notify_one();
    tokio::select! {
        _ = &mut materialized => panic!("failure escaped while an accepted read remained held"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
    }
    // Transport retries of the same missing frame are not another dispatched
    // locator. The accepted window and its one earlier refill remain bounded.
    let dispatched = store.paths.lock().unwrap().len();
    assert!(dispatched <= verification::READ_CONCURRENCY + 1);
    store.resume_third.notify_one();
    let error = materialized.await.unwrap_err();
    assert_eq!(store.paths.lock().unwrap().len(), dispatched);
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut found = false;
    while let Some(error) = source {
        found |= error
            .to_string()
            .contains("deliberate historical read failure");
        source = error.source();
    }
    assert!(found, "original read failure was replaced: {error:?}");
    assert_eq!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .encode()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn materialization_overlaps_historical_reads_before_exact_cold_restore() {
    let store = Arc::new(HeldFrameStore::default());
    let mut f = Fixture::with_store(store.clone()).await;
    let mut cell = f.cell(4).await;
    let mut proof = None;
    for commit in [2, 3] {
        let (_, frames, assigned) = f.append(&mut cell, commit);
        let prepared = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .unwrap();
        let (node, mut proofs) = f
            .directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .unwrap();
        f.node = node;
        proof = proofs.pop();
    }
    let proof = proof.unwrap();
    assert_eq!(proof.locator_count(), 2);
    let mut publisher = f.publisher(&cell);
    store.armed.store(true, Ordering::Release);
    let materialized = publisher.materialize_bundle(&proof);
    tokio::pin!(materialized);
    tokio::select! {
        _ = store.first.notified() => {},
        _ = &mut materialized => panic!("first historical read must remain held"),
    }
    let overlapped = tokio::select! {
        _ = store.second.notified() => true,
        _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => false,
        _ = &mut materialized => panic!("root cannot publish before its first frame"),
    };
    assert!(
        overlapped,
        "historical reads remained serial under the original suffix admission"
    );
    store.resume.notify_one();
    let root = materialized.await.unwrap();
    assert_eq!(root.commit_sequence, 3);
    let restored = f.scratch.path().join("overlapped-restore.sqlite");
    cell.replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(restored).unwrap();
    let mut stmt = db
        .prepare("SELECT request, result FROM outcomes ORDER BY request")
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("request-2".into(), "result-2".into()),
            ("request-3".into(), "result-3".into()),
            ("seed".into(), "original".into())
        ]
    );
}
