use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Default)]
struct ReplyFault {
    inner: InMemory,
    mode: AtomicU8,
}
impl std::fmt::Display for ReplyFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bundle-reply-fault")
    }
}
fn denied() -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(std::io::Error::other("injected bundle reply failure")),
    }
}
#[async_trait::async_trait]
impl ObjectStore for ReplyFault {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mode = self.mode.load(Ordering::SeqCst);
        let eligible = (mode == 1 && path.as_ref().ends_with(".cnb"))
            || (mode == 2
                && path.as_ref().contains("/nodes/")
                && matches!(opts.mode, object_store::PutMode::Update(_)));
        let lose = eligible
            && self
                .mode
                .compare_exchange(mode, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        let result = self.inner.put_opts(path, body, opts).await?;
        if lose {
            return Err(denied());
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
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
async fn lost_immutable_reply_grants_no_proof_and_retry_reuses_exact_bytes() {
    let faults = Arc::new(ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    faults.mode.store(1, Ordering::SeqCst);
    // Even a successful origin PUT with a lost reply cannot select a range.
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .is_err()
    );
    assert_eq!(
        f.directory
            .load(SessionId::from_bytes([1; 16]), NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .bundle_head(),
        f.node.advertisement().bundle_head()
    );
    let retry = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &retry, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs[0].commit_sequence(), 2);
}

#[tokio::test]
async fn lost_node_cas_reply_reconciles_only_the_exact_selected_head() {
    let faults = Arc::new(ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    faults.mode.store(2, Ordering::SeqCst);
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(selected.advertisement().bundle_head(), Some(prepared.head));
    assert_eq!(proofs[0].commit_sequence(), 2);
}
