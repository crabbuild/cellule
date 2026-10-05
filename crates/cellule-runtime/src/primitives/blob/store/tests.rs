//! Real provider entrypoints under caller loss, closure, failure and capacity.
use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    fmt,
    sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[derive(Default, Debug)]
struct Gate {
    kind: AtomicU8,
    entered: AtomicUsize,
    released: AtomicBool,
    changed: Notify,
    resume: Notify,
    fail: AtomicBool,
    panic: AtomicBool,
}
impl Gate {
    async fn wait(&self, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.entered.load(Ordering::Acquire) >= count {
                    break;
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
    async fn before(&self, kind: u8) {
        if self.kind.load(Ordering::Acquire) != kind {
            return;
        }
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
        loop {
            let resumed = self.resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            resumed.await;
        }
    }
}
#[derive(Debug)]
struct ProviderFailure(Arc<u8>);
impl fmt::Display for ProviderFailure {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("original Blob provider failure")
    }
}
impl std::error::Error for ProviderFailure {}
#[derive(Debug)]
struct Provider {
    inner: Arc<object_store::memory::InMemory>,
    gate: Arc<Gate>,
    failure: Arc<u8>,
    blocking: Option<Arc<BlockingJob>>,
}
#[derive(Default, Debug)]
struct BlockingJob {
    entered: AtomicBool,
    finished: AtomicBool,
    released: std::sync::Mutex<bool>,
    resume: std::sync::Condvar,
}
impl BlockingJob {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.resume.notify_all();
    }
}
impl fmt::Display for Provider {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("blob-native-test-provider")
    }
}
#[async_trait::async_trait]
impl ObjectStore for Provider {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.gate.before(1).await;
        if let Some(blocking) = &self.blocking {
            let blocking = blocking.clone();
            let inner = self.inner.clone();
            let path = location.clone();
            return tokio::task::spawn_blocking(move || {
                use futures_util::FutureExt;
                blocking.entered.store(true, Ordering::Release);
                let mut released = blocking.released.lock().unwrap();
                while !*released {
                    released = blocking.resume.wait(released).unwrap();
                }
                drop(released);
                // This uncontended InMemory put is immediately ready. Actual
                // publication occurs on the original blocking worker, which
                // survives teardown of its waiting Tokio runtime.
                let result = inner
                    .put_opts(&path, payload, options)
                    .now_or_never()
                    .unwrap();
                blocking.finished.store(true, Ordering::Release);
                result
            })
            .await
            .unwrap();
        }
        assert!(
            !self.gate.panic.load(Ordering::Acquire),
            "original Blob provider panic"
        );
        if self.gate.fail.load(Ordering::Acquire) {
            // Store preserves NotSupported's source and does not retry it.
            // Auth classification intentionally maps PermissionDenied to a
            // domain variant without a source before Blob receives the error.
            return Err(object_store::Error::NotSupported {
                source: Box::new(ProviderFailure(self.failure.clone())),
            });
        }
        self.inner.put_opts(location, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gate.before(2).await;
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let gate = self.gate.clone();
        let inner = self.inner.clone();
        Box::pin(locations.then(move |location| {
            let gate = gate.clone();
            let inner = inner.clone();
            async move {
                let location = location?;
                gate.before(3).await;
                inner.delete(&location).await?;
                Ok(location)
            }
        }))
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
fn fixture() -> (BlobArtifactStore, Arc<Provider>) {
    let provider = Arc::new(Provider {
        inner: Arc::new(object_store::memory::InMemory::new()),
        gate: Arc::new(Gate::default()),
        failure: Arc::new(37),
        blocking: None,
    });
    (
        BlobArtifactStore::new(Store::new(provider.clone())),
        provider,
    )
}
async fn join(store: &BlobArtifactStore) -> BlobArtifactLifecycleObservation {
    tokio::time::timeout(std::time::Duration::from_secs(5), store.close_and_join())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_original_put_read_and_gc_remain_owned_through_repeated_close() {
    for kind in [1, 2, 3] {
        let (store, provider) = fixture();
        let payload = b"retained immutable Blob bytes";
        let digest = part_digest(payload);
        if kind != 1 {
            provider
                .inner
                .put(
                    &store.part_path(&digest),
                    Bytes::from_static(payload).into(),
                )
                .await
                .unwrap();
        }
        let refs = Arc::new(BTreeSet::new());
        let retained_refs = Arc::downgrade(&refs);
        provider.gate.kind.store(kind, Ordering::Release);
        let caller = {
            let store = store.clone();
            tokio::spawn(async move {
                match kind {
                    1 => {
                        store.put_part(digest, payload).await?;
                        Ok(Vec::new())
                    }
                    2 => store.read_part(digest, payload.len() as u32).await,
                    _ => {
                        let result = store.sweep_unreferenced(refs, i64::MAX).await?;
                        Ok(result.deleted().to_be_bytes().to_vec())
                    }
                }
            })
        };
        provider.gate.wait(1).await;
        assert_eq!(store.lifecycle_observation().unwrap().accepted_jobs(), 1);
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let closing = {
            let store = store.clone();
            tokio::spawn(async move { store.close_and_join().await })
        };
        tokio::task::yield_now().await;
        store.close();
        let closed = store.lifecycle_observation().unwrap();
        assert!(closed.admission_closed() && !closed.locally_joined());
        assert_eq!(closed.accepted_jobs(), 1);
        assert!(matches!(
            store.put_part(digest, payload).await,
            Err(Error::CellDraining)
        ));
        assert!(matches!(
            store.read_part(digest, payload.len() as u32).await,
            Err(Error::CellDraining)
        ));
        assert!(!closing.is_finished());
        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        if kind == 3 {
            assert!(retained_refs.upgrade().is_some());
        }
        provider.gate.release();
        let closed = join(&store).await;
        assert!(closed.locally_joined());
        assert!(closed.first_failure().is_none());
        assert!(join(&store.clone()).await.locally_joined());
        assert!(retained_refs.upgrade().is_none());
        let native = provider.inner.get(&store.part_path(&digest)).await;
        if kind == 3 {
            assert!(matches!(native, Err(object_store::Error::NotFound { .. })));
        } else {
            assert_eq!(native.unwrap().bytes().await.unwrap(), payload.as_slice());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_and_panic_keep_original_native_sources_after_caller_loss() {
    for (panic, cancel) in [(false, false), (false, true), (true, true)] {
        let (store, provider) = fixture();
        provider.gate.kind.store(1, Ordering::Release);
        provider.gate.fail.store(!panic, Ordering::Release);
        provider.gate.panic.store(panic, Ordering::Release);
        let caller = {
            let store = store.clone();
            tokio::spawn(async move { store.put_part(part_digest(b"fail"), b"fail").await })
        };
        provider.gate.wait(1).await;
        let caller = if cancel {
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            None
        } else {
            Some(caller)
        };
        provider.gate.release();
        let observed = join(&store).await;
        assert!(observed.locally_joined());
        let original = observed.first_failure().unwrap();
        if let Some(caller) = caller {
            let Error::Shared(shared) = caller.await.unwrap().unwrap_err() else {
                panic!("missing retained original error")
            };
            assert!(Arc::ptr_eq(&shared, original));
        }
        let mut cause: &(dyn std::error::Error + 'static) = original.as_ref();
        loop {
            if let Some(failure) = cause.downcast_ref::<ProviderFailure>() {
                assert!(!panic);
                assert!(Arc::ptr_eq(&failure.0, &provider.failure));
                break;
            }
            if let Some(failure) = cause.downcast_ref::<tokio::task::JoinError>() {
                assert!(panic && failure.is_panic());
                break;
            }
            cause = cause.source().expect("native source was replaced");
        }
        assert!(Arc::ptr_eq(
            join(&store).await.first_failure().unwrap(),
            original
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_capacity_is_retained_after_waiter_cancellation_until_actual_join() {
    let (store, provider) = fixture();
    provider.gate.kind.store(1, Ordering::Release);
    let mut callers = Vec::new();
    for index in 0..64_u8 {
        let store = store.clone();
        callers.push(tokio::spawn(async move {
            let bytes = [index; 4];
            store.put_part(part_digest(&bytes), &bytes).await
        }));
    }
    provider.gate.wait(64).await;
    assert_eq!(store.lifecycle_observation().unwrap().accepted_jobs(), 64);
    assert!(matches!(
        store.put_part(part_digest(b"spill"), b"spill").await,
        Err(Error::Capacity("Blob artifact jobs"))
    ));
    for caller in &callers {
        caller.abort();
    }
    for caller in callers {
        assert!(caller.await.unwrap_err().is_cancelled());
    }
    assert_eq!(store.lifecycle_observation().unwrap().accepted_jobs(), 64);
    store.close();
    provider.gate.release();
    assert!(join(&store).await.locally_joined());
    assert_eq!(
        provider.inner.list(None).collect::<Vec<_>>().await.len(),
        64
    );
    assert!(matches!(
        store.put_part(part_digest(b"spill"), b"spill").await,
        Err(Error::CellDraining)
    ));
}

#[test]
fn forced_runtime_loss_cannot_turn_an_unjoined_provider_job_into_local_closure() {
    let blocking = Arc::new(BlockingJob::default());
    let provider = Arc::new(Provider {
        inner: Arc::new(object_store::memory::InMemory::new()),
        gate: Arc::new(Gate::default()),
        failure: Arc::new(38),
        blocking: Some(blocking.clone()),
    });
    let store = BlobArtifactStore::new(Store::new(provider.clone()));
    let former = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let caller = {
        let store = store.clone();
        former.spawn(async move {
            store
                .put_part(part_digest(b"unfinished"), b"unfinished")
                .await
        })
    };
    former.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !blocking.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
    former.shutdown_background();
    store.close();
    let current = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let supervisors_stopped = current.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while store.lifecycle_observation().unwrap().accepted_jobs() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
    });
    let observed = store.lifecycle_observation().unwrap();
    let early_close = current.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), store.close_and_join()).await
    });
    let finished_early = blocking.finished.load(Ordering::Acquire);
    // Always let the original native worker finish before any failure assertion.
    blocking.release();
    let native_finished = current.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !blocking.finished.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
    });
    drop(caller);
    assert!(supervisors_stopped.is_ok() && native_finished.is_ok() && !finished_early);
    assert!(observed.admission_closed());
    assert_eq!(observed.accepted_jobs(), 0);
    assert_eq!(observed.unjoined_jobs(), 1);
    assert!(!observed.locally_joined());
    assert!(matches!(
        early_close,
        Ok(Err(Error::Control(
            "Blob artifact original native join is unproven"
        )))
    ));
    assert!(matches!(
        observed.first_failure().unwrap().as_ref(),
        Error::Control("Blob artifact supervisor ended before original native join")
    ));
    current.block_on(async {
        let raw = provider
            .inner
            .get(&store.part_path(&part_digest(b"unfinished")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(raw.as_ref(), b"unfinished");
        // Provider completion after abandonment cannot reconstruct the original
        // join or erase its retained unknown result/ownership obligation.
        assert!(store.close_and_join().await.is_err());
        assert!(!store.lifecycle_observation().unwrap().locally_joined());
    });
}

#[tokio::test]
async fn malformed_inputs_never_start_native_work_or_consume_admission() {
    let (store, provider) = fixture();
    assert!(store.put_part([0; 32], b"mismatch").await.is_err());
    let oversized = vec![0; MAX_BLOB_PART_BYTES + 1];
    assert!(
        store
            .put_part(part_digest(&oversized), &oversized)
            .await
            .is_err()
    );
    assert!(
        store
            .read_part([0; 32], (MAX_BLOB_PART_BYTES + 1) as u32)
            .await
            .is_err()
    );
    assert!(
        store
            .sweep_unreferenced(Arc::new(BTreeSet::new()), -1)
            .await
            .is_err()
    );
    assert_eq!(store.lifecycle_observation().unwrap().accepted_jobs(), 0);
    assert!(
        store
            .lifecycle_observation()
            .unwrap()
            .first_failure()
            .is_none()
    );
    assert_eq!(provider.inner.list(None).collect::<Vec<_>>().await.len(), 0);
    assert!(join(&store).await.locally_joined());
}
