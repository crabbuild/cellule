//! Real Blob provider work retained through the existing host drain owner.
use super::*;
use cellule_runtime::primitives::blob::BlobArtifactStore;
use cellule_store::Store;
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::{collections::BTreeSet, fmt, sync::atomic::AtomicUsize};
use tokio::sync::Notify;

#[derive(Default, Debug)]
struct Gate {
    entered: AtomicUsize,
    released: AtomicBool,
    changed: Notify,
    resume: Notify,
}
impl Gate {
    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.entered.load(Ordering::Acquire) != 0 {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.resume.notify_waiters();
    }
}
#[derive(Debug)]
struct Provider {
    inner: InMemory,
    gate: Arc<Gate>,
}
impl fmt::Display for Provider {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("host-blob-drain")
    }
}
#[async_trait::async_trait]
impl ObjectStore for Provider {
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
        let gate = self.gate.clone();
        Box::pin(self.inner.list(prefix).then(move |item| {
            let gate = gate.clone();
            async move {
                gate.entered.fetch_add(1, Ordering::AcqRel);
                gate.changed.notify_waiters();
                loop {
                    let resume = gate.resume.notified();
                    tokio::pin!(resume);
                    resume.as_mut().enable();
                    if gate.released.load(Ordering::Acquire) {
                        return item;
                    }
                    resume.await;
                }
            }
        }))
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
fn node() -> CellNode {
    CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 << 20)
        .with_replica_host(ReplicaHost::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(SessionId::from_bytes([252; 16]))
        .with_required_owned_components([BLOB_ARTIFACT_STORE_COMPONENT])
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_blob_sweep_survives_caller_and_shutdown_waiter_cancellation() {
    let node = Arc::new(node());
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let provider = Arc::new(Provider {
        inner: InMemory::new(),
        gate: Arc::new(Gate::default()),
    });
    let path = cellule_store::global_content_path(
        cellule_store::GLOBAL_PREFIX,
        "blob-parts",
        &"ab".repeat(32),
    );
    provider
        .inner
        .put(&path, b"orphan".to_vec().into())
        .await
        .unwrap();
    let artifacts = node
        .install_blob_artifact_store(BlobArtifactStore::new(Store::new(provider.clone())))
        .unwrap();
    assert!(node.install_blob_artifact_store(artifacts.clone()).is_err());
    node.install_node_lease(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    let unrelated = BlobArtifactStore::new(Store::new(Arc::new(InMemory::new())));
    assert!(node.install_blob_artifact_store(unrelated.clone()).is_err());
    let references = Arc::new(BTreeSet::new());
    let weak = Arc::downgrade(&references);
    let caller = {
        let store = artifacts.clone();
        tokio::spawn(async move { store.sweep_unreferenced(references, i64::MAX).await })
    };
    provider.gate.wait().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let shutdown = {
        let node = node.clone();
        tokio::spawn(async move { node.shutdown().await })
    };
    let closing = tokio::time::timeout(Duration::from_secs(5), async {
        while !artifacts
            .lifecycle_observation()
            .unwrap()
            .admission_closed()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before = artifacts.lifecycle_observation().unwrap();
    let stopped_early = node.state() == NodeState::Stopped;
    let retained_references = weak.upgrade().is_some();
    shutdown.abort();
    let cancelled = shutdown.await.is_err_and(|error| error.is_cancelled());
    // Always release the real provider and join native drain before assertions.
    provider.gate.release();
    let drained = node.shutdown().await;
    assert!(closing.is_ok() && cancelled && !stopped_early && retained_references);
    assert_eq!(before.accepted_jobs(), 1);
    drained.unwrap();
    assert_eq!(node.state(), NodeState::Stopped);
    let joined = artifacts.close_and_join().await.unwrap();
    assert!(joined.locally_joined() && joined.first_failure().is_none());
    assert!(
        !unrelated
            .lifecycle_observation()
            .unwrap()
            .admission_closed()
    );
    assert!(weak.upgrade().is_none());
    assert_eq!(provider.gate.entered.load(Ordering::Acquire), 1);
    assert!(matches!(
        provider.inner.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(
        node.try_owned_component::<BlobArtifactStore>(BLOB_ARTIFACT_STORE_COMPONENT)
            .unwrap()
            .is_none()
    );
    assert_eq!(node.stats().active_cells(), 0);
    assert_eq!(node.stats().worker_jobs(), 0);
    assert_eq!(node.stats().retained_bytes(), 0);
    assert_eq!(node.stats().local_disk_reserved_bytes(), 0);
}

#[tokio::test]
async fn blob_installation_requires_owned_tasks_and_an_open_original_store() {
    let node = node();
    let artifacts = BlobArtifactStore::new(Store::new(Arc::new(InMemory::new())));
    assert!(node.install_blob_artifact_store(artifacts.clone()).is_err());
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    artifacts.close();
    assert!(matches!(
        node.install_blob_artifact_store(artifacts),
        Err(Error::CellDraining)
    ));
    assert!(
        node.try_owned_component::<BlobArtifactStore>(BLOB_ARTIFACT_STORE_COMPONENT)
            .unwrap()
            .is_none()
    );
    // Complete the required owner set before normal startup/cleanup.
    let installed = node
        .install_blob_artifact_store(BlobArtifactStore::new(Store::new(
            Arc::new(InMemory::new()),
        )))
        .unwrap();
    node.shutdown().await.unwrap();
    assert!(installed.close_and_join().await.unwrap().locally_joined());
}
