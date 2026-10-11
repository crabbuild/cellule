//! Capture must keep its exact ensemble across a legitimate boot heartbeat.
use super::*;
use futures_util::stream::BoxStream;
use object_store::{ObjectStore, path::Path};

#[derive(Debug, Default)]
struct HeldNodeRead {
    inner: InMemory,
    path: Mutex<Option<Path>>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl std::fmt::Display for HeldNodeRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldNodeRead")
    }
}
#[async_trait::async_trait]
impl ObjectStore for HeldNodeRead {
    async fn put_opts(
        &self,
        path: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn get_opts(
        &self,
        path: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let result = self.inner.get_opts(path, options).await;
        let held = {
            let mut target = self.path.lock().unwrap();
            if target.as_ref() == Some(path) {
                target.take();
                true
            } else {
                false
            }
        };
        if held {
            // The provider has captured the old exact record. Publish the real
            // heartbeat before delivering it, then let capture reobserve it.
            self.entered.notify_one();
            self.resume.notified().await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_preserves_replacement_across_captured_heartbeat() {
    captured_change(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_follower_evacuation_rejects_closed_replacement_after_captured_read() {
    captured_change(true).await;
}

async fn captured_change(close: bool) {
    let store = Arc::new(HeldNodeRead::default());
    let fixture = ManagedFixture::with_store(4, 2, store.clone()).await;
    let (original, operation) = evacuation::maintenance(&fixture).await;
    evacuation::spare(&fixture).await;
    evacuation::rotated(&fixture).await;
    *store.path.lock().unwrap() = Some(fixture.native.layout.node_path(session(0).as_bytes()));
    let (result, ()) = tokio::join!(
        evacuation::evidence(&fixture, &original, &operation),
        async {
            tokio::time::timeout(Duration::from_secs(3), store.entered.notified())
                .await
                .unwrap();
            if close {
                fixture
                    .native
                    .node
                    .runtime()
                    .node_durability()
                    .unwrap()
                    .1
                    .shutdown()
                    .await
                    .unwrap();
            } else {
                fixture.boots[0]
                    .refresh_capacity(
                        0,
                        fixture.native.journal.as_ref(),
                        Instant::now() + Duration::from_secs(3),
                    )
                    .await
                    .unwrap();
            }
            store.resume.notify_one();
        }
    );
    let message = result.as_ref().err().map(|error| format!("{error:?}"));
    let valid = if close {
        matches!(result, Err(Error::Fenced))
    } else {
        result.as_ref().is_ok_and(|capture| {
            capture
                .authority()
                .log()
                .is_some_and(|log| log.epoch() == 2 && log.members() == [node_id(2), node_id(3)])
        })
    };
    drop(result);
    fixture.finish().await;
    assert!(
        valid,
        "unexpected captured authority result: close={close} {message:?}"
    );
}
