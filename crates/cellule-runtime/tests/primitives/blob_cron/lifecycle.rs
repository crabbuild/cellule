//! Original public Blob operations across metadata, provider I/O and caller loss.
use super::*;
use cellule_runtime::BlobArtifactStore;
use cellule_runtime::client::InvocationError;
use futures_util::stream::BoxStream;
use object_store::ObjectStore;
use std::{
    fmt,
    sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default, Debug)]
struct Gate {
    kind: AtomicU8,
    entered: AtomicUsize,
    released: AtomicBool,
    changed: Notify,
    resume: Notify,
}
impl Gate {
    async fn wait(&self, count: usize) -> bool {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.entered.load(Ordering::Acquire) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
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
            let resume = self.resume.notified();
            tokio::pin!(resume);
            resume.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            resume.await;
        }
    }
}
#[derive(Debug)]
struct Provider {
    inner: InMemory,
    gate: Gate,
}
impl fmt::Display for Provider {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("public-blob-lifecycle")
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
        self.gate.before(1).await;
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
        self.gate.before(2).await;
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
struct Fixture {
    runtime: CellRuntime,
    handle: cellule_runtime::cell::actor::CellHandle,
    client: CellClient,
    target: CellTarget,
    blobs: BlobNamespace<TestBlob>,
    artifacts: BlobArtifactStore,
    provider: Arc<Provider>,
    _directory: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let registry = registry();
        let tenant = TenantId::from_bytes([100; 16]);
        let application = ApplicationId::from_bytes([101; 16]);
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("blob-local-lifecycle"),
            *application.as_bytes(),
        );
        let target =
            CellTarget::new(tenant, application, BLOB_NAMESPACE, &0_u32.to_be_bytes()).unwrap();
        let incarnation = IncarnationId::from_bytes([102; 16]);
        let proof = CellCatalog::new(layout.clone(), tenant)
            .provision(
                CatalogEntry::new(
                    &target,
                    CatalogRole::Blob,
                    registry.module_code(BLOB_MODULE).unwrap(),
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(layout.clone());
        let session = SessionId::from_bytes([103; 16]);
        let control = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session,
                    endpoint: "https://blob-lifecycle.internal:8081".into(),
                },
            )
            .await
            .unwrap();
        let runtime = super::maintenance::node_runtime(session);
        let directory = tempfile::TempDir::new().unwrap();
        let handle = runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    layout,
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    Limits::default(),
                )
                .unwrap(),
                authority,
                control,
                directory.path().join("blob.sqlite"),
                cellule_runtime::primitives::blob::install_blob_schema,
            )
            .await
            .unwrap();
        let provider = Arc::new(Provider {
            inner: InMemory::new(),
            gate: Gate::default(),
        });
        let artifacts = BlobArtifactStore::new(Store::new(provider.clone()));
        let client =
            CellClient::local(registry, handle.clone()).with_blob_artifact_store(artifacts.clone());
        let blobs = BlobNamespace::new(client.clone(), tenant, application).unwrap();
        Self {
            runtime,
            handle,
            client,
            target,
            blobs,
            artifacts,
            provider,
            _directory: directory,
        }
    }
    async fn mutate(
        &self,
        id: u8,
        mutation: BlobMutation,
    ) -> cellule_runtime::Committed<BlobMutationOutcome> {
        let now = now_ms();
        self.blobs
            .mutate(mutation_identity_window(id, now, now + 60_000), mutation)
            .await
            .unwrap()
    }
    async fn begin(&self) {
        self.mutate(
            104,
            BlobMutation::Begin {
                key: b"key".to_vec(),
                upload_id: [105; 16],
                condition: BlobCondition::Any,
                content_type: None,
                metadata: vec![],
                expires_at_ms: now_ms() + 120_000,
            },
        )
        .await;
    }
    async fn finish(&self) {
        self.handle.drain().await.unwrap();
        self.runtime.shutdown().await.unwrap();
        assert_eq!(self.runtime.stats().active_cells(), 0);
        assert_eq!(self.runtime.stats().worker_jobs(), 0);
        assert_eq!(self.runtime.stats().retained_bytes(), 0);
        assert_eq!(self.runtime.stats().local_disk_reserved_bytes(), 0);
    }
}
async fn joined(
    store: &BlobArtifactStore,
) -> cellule_runtime::primitives::blob::BlobArtifactLifecycleObservation {
    tokio::time::timeout(Duration::from_secs(5), store.close_and_join())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_range_lifetime_covers_metadata_and_all_parts_after_close_and_waiter_loss() {
    for cancel in [false, true] {
        let fixture = Fixture::new().await;
        fixture.begin().await;
        for (part_number, payload, id) in [(1, b"abc".to_vec(), 106), (2, b"def".to_vec(), 107)] {
            fixture
                .mutate(
                    id,
                    BlobMutation::PutPart {
                        key: b"key".to_vec(),
                        upload_id: [105; 16],
                        part_number,
                        payload,
                    },
                )
                .await;
        }
        let committed = fixture
            .mutate(
                108,
                BlobMutation::Complete {
                    key: b"key".to_vec(),
                    upload_id: [105; 16],
                    part_count: 2,
                },
            )
            .await;
        let started = Arc::new(Notify::new());
        let (release, receive) = std::sync::mpsc::channel();
        let blocked_metadata = {
            let handle = fixture.handle.clone();
            let started = started.clone();
            tokio::spawn(async move {
                handle
                    .query(1, 1, move |_| {
                        started.notify_one();
                        receive.recv().unwrap();
                        Ok(vec![1])
                    })
                    .await
            })
        };
        started.notified().await;
        fixture.provider.gate.kind.store(2, Ordering::Release);
        let caller = {
            let blobs = fixture.blobs.clone();
            tokio::spawn(async move {
                blobs
                    .query(
                        BlobQuery::Read {
                            key: b"key".to_vec(),
                            offset: 1,
                            limit: 4,
                        },
                        Some(committed.receipt),
                    )
                    .await
            })
        };
        // Observe admission before the held real SQL query's existing deadline.
        let accepted = tokio::time::timeout(Duration::from_millis(500), async {
            while fixture
                .artifacts
                .lifecycle_observation()
                .unwrap()
                .accepted_jobs()
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let reads_before_metadata = fixture.provider.gate.entered.load(Ordering::Acquire);
        fixture.artifacts.close();
        let caller = if cancel {
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            None
        } else {
            Some(caller)
        };
        release.send(()).unwrap();
        blocked_metadata.await.unwrap().unwrap();
        let provider_entered = fixture.provider.gate.wait(1).await;
        let close = {
            let store = fixture.artifacts.clone();
            tokio::spawn(async move { store.close_and_join().await })
        };
        close.abort();
        let close_cancelled = close.await.is_err_and(|error| error.is_cancelled());
        let pending = fixture
            .artifacts
            .lifecycle_observation()
            .unwrap()
            .accepted_jobs();
        fixture.provider.gate.release();
        let observed = joined(&fixture.artifacts).await;
        let output = match caller {
            Some(caller) => Some(caller.await),
            None => None,
        };
        fixture.finish().await;
        // Resume/join the original worker and all provider I/O before failure
        // assertions, including when ownership admission regresses.
        assert!(accepted.is_ok() && provider_entered && close_cancelled);
        assert_eq!(reads_before_metadata, 0);
        assert_eq!(pending, 1);
        assert!(observed.locally_joined() && observed.first_failure().is_none());
        assert_eq!(fixture.provider.gate.entered.load(Ordering::Acquire), 2);
        if let Some(output) = output {
            assert!(
                matches!(output.unwrap().unwrap().output, BlobQueryResult::Read(Some(read)) if read.bytes == b"bcde")
            );
        }
        assert!(matches!(
            fixture
                .blobs
                .query(
                    BlobQuery::Head {
                        key: b"key".to_vec()
                    },
                    None
                )
                .await,
            Err(InvocationError::NotStarted(
                cellule_runtime::Error::CellDraining
            ))
        ));
        assert!(matches!(
            fixture.blobs.list_shard(0, vec![], None, 1, None).await,
            Err(InvocationError::NotStarted(
                cellule_runtime::Error::CellDraining
            ))
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_part_mutation_joins_manifest_publication_after_cancelled_upload_waiter() {
    let fixture = Fixture::new().await;
    fixture.begin().await;
    let payload = b"accepted-part".to_vec();
    let mut digest = blake3::Hasher::new();
    digest.update(b"crab.blob-part.v1\0");
    digest.update(&payload);
    let now = now_ms();
    let identity = mutation_identity_window(109, now, now + 60_000);
    let prepared = fixture
        .client
        .prepare_command::<cellule_runtime::primitives::blob::BlobCommand<TestBlob>>(
            &fixture.target,
            identity,
            BlobMutation::PutPartRef {
                key: b"key".to_vec(),
                upload_id: [105; 16],
                part_number: 1,
                digest: *digest.finalize().as_bytes(),
                size: payload.len() as u32,
            },
        )
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    fixture.provider.gate.kind.store(1, Ordering::Release);
    let caller = {
        let blobs = fixture.blobs.clone();
        tokio::spawn(async move {
            blobs
                .mutate(
                    identity,
                    BlobMutation::PutPart {
                        key: b"key".to_vec(),
                        upload_id: [105; 16],
                        part_number: 1,
                        payload,
                    },
                )
                .await
        })
    };
    let provider_entered = fixture.provider.gate.wait(1).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    fixture.artifacts.close();
    let pending = fixture
        .artifacts
        .lifecycle_observation()
        .unwrap()
        .accepted_jobs();
    fixture.provider.gate.release();
    assert!(joined(&fixture.artifacts).await.first_failure().is_none());
    assert!(provider_entered);
    assert_eq!(pending, 1);
    // Observe before any duplicate dispatch: the retained original operation
    // must have published its manifest after its public caller disappeared.
    assert!(matches!(
        fixture.client.resolve(&evidence).await.unwrap(),
        cellule_runtime::Resolution::Committed(_)
    ));
    let committed = prepared.execute().await.unwrap();
    assert!(matches!(
        committed.output,
        BlobMutationOutcome::PartStored { .. }
    ));
    assert!(matches!(
        fixture.client.resolve(&evidence).await.unwrap(),
        cellule_runtime::Resolution::Committed(_)
    ));
    assert_eq!(committed.receipt.commit_sequence, 2);
    assert_eq!(fixture.provider.gate.entered.load(Ordering::Acquire), 1);
    assert!(matches!(
        fixture
            .blobs
            .prepare_mutation(
                identity,
                BlobMutation::Abort {
                    key: b"key".to_vec(),
                    upload_id: [105; 16]
                }
            )
            .await,
        Err(InvocationError::NotStarted(
            cellule_runtime::Error::CellDraining
        ))
    ));
    fixture.finish().await;
}
