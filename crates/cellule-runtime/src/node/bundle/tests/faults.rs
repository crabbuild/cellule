use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub(super) struct ReplyFault {
    inner: InMemory,
    pub(super) mode: AtomicU8,
    pub(super) node_updates: AtomicUsize,
    pub(super) coverage_puts: AtomicUsize,
    pub(super) range_started: AtomicUsize,
    pub(super) base_started: AtomicUsize,
    pub(super) held_metadata: std::sync::Mutex<Vec<(String, u64)>>,
    pub(super) pin_started: tokio::sync::Notify,
    pub(super) pin_resume: tokio::sync::Notify,
    pub(super) node_started: tokio::sync::Notify,
    pub(super) node_resume: tokio::sync::Notify,
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
        if mode == 9
            && path.as_ref().ends_with("/control.json")
            && matches!(opts.mode, object_store::PutMode::Update(_))
            && self
                .mode
                .compare_exchange(9, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.pin_started.notify_one();
            self.pin_resume.notified().await;
        }
        if path.as_ref().ends_with("/control.json")
            && matches!(opts.mode, object_store::PutMode::Update(_))
        {
            if mode == 6 {
                self.mode.store(7, Ordering::SeqCst);
            } else if mode == 7 {
                return Err(denied());
            }
        }
        let node_update = path.as_ref().contains("/nodes/")
            && matches!(opts.mode, object_store::PutMode::Update(_));
        if node_update {
            self.node_updates.fetch_add(1, Ordering::SeqCst);
        }
        if node_update
            && mode == 10
            && self
                .mode
                .compare_exchange(10, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.node_started.notify_one();
            self.node_resume.notified().await;
        }
        if path.as_ref().ends_with(".cnb") {
            self.coverage_puts.fetch_add(1, Ordering::SeqCst);
        }
        if node_update && mode == 4 {
            self.mode
                .compare_exchange(4, 5, Ordering::SeqCst, Ordering::SeqCst)
                .unwrap();
        } else if node_update && mode == 5 {
            return Err(denied());
        }
        if mode == 3
            && path.as_ref().ends_with("/control.json")
            && matches!(opts.mode, object_store::PutMode::Update(_))
            && self
                .mode
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(denied());
        }
        let eligible = (mode == 1 && path.as_ref().ends_with(".cnb"))
            || (mode == 8
                && path.as_ref().ends_with("/control.json")
                && matches!(opts.mode, object_store::PutMode::Update(_)))
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
        let mode = self.mode.load(Ordering::SeqCst);
        let metadata = match &opts.range {
            Some(object_store::GetRange::Bounded(range)) if path.as_ref().ends_with(".cnb") => {
                (mode == 14 && range.start != 0)
                    || (mode == 15
                        && self
                            .held_metadata
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|(object, offset)| {
                                object == path.as_ref() && *offset == range.start
                            }))
            }
            _ => false,
        };
        if metadata {
            self.range_started.fetch_add(1, Ordering::SeqCst);
            self.node_started.notify_one();
            self.node_resume.notified().await;
        }
        if (mode == 12 && path.as_ref().ends_with(".root"))
            || (mode == 13 && path.as_ref().ends_with(".pack"))
            || (mode == 16
                && path.as_ref().ends_with(".root")
                && self
                    .held_metadata
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(object, _)| object == path.as_ref()))
        {
            self.base_started.fetch_add(1, Ordering::SeqCst);
            self.node_started.notify_one();
            self.node_resume.notified().await;
        }
        if self.mode.load(Ordering::SeqCst) == 11
            && path.as_ref().ends_with(".cnb")
            && opts.range.is_some()
        {
            self.range_started.fetch_add(1, Ordering::SeqCst);
            self.node_started.notify_one();
            self.node_resume.notified().await;
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
async fn interrupted_pin_cas_retains_a_provisional_inventory_obligation() {
    let faults = Arc::new(ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let cell = f.unbound_cell_for_application(4, [9; 16]).await;
    faults.mode.store(3, Ordering::SeqCst);
    assert!(
        f.directory
            .bind_bundle_cell(&f.node, &cell.authority, &cell.control, NOW)
            .await
            .is_err()
    );
    let reserved = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    let catalog = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        reserved.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.bindings.len(), 1);
    assert_eq!(catalog.bindings[0].phase, BindingPhase::Provisional);
    assert!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .bundle_binding
            .is_none()
    );
    assert!(matches!(
        f.directory.withdraw(&reserved, NOW).await,
        Err(Error::PendingPublication)
    ));
    let (opened, pinned) = f
        .directory
        .bind_bundle_cell(&reserved, &cell.authority, &cell.control, NOW)
        .await
        .unwrap();
    assert_eq!(
        pinned.value().bundle_binding,
        catalog.bindings[0].control.bundle_binding
    );
    let catalog = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        opened.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.bindings.len(), 1);
    assert_eq!(catalog.bindings[0].phase, BindingPhase::Open);
}

#[tokio::test]
async fn lower_level_pin_cas_cannot_bypass_catalog_reservation() {
    let mut f = Fixture::new().await;
    let cell = f.unbound_cell_for_application(4, [9; 16]).await;
    let mut next = cell.control.value().clone();
    next.revision += 1;
    next.progress += 1;
    next.bundle_binding = Some(BundleBindingRef {
        session: SessionId::from_bytes([1; 16]),
        epoch: EPOCH,
        digest: Digest::from_bytes([55; 32]),
    });
    assert!(
        cell.authority
            .transition(&cell.control, next, Transition::BindBundle)
            .await
            .is_err()
    );
    assert!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .bundle_binding
            .is_none()
    );
}

#[tokio::test]
async fn interrupted_activation_retains_the_already_selected_cell_pin() {
    let faults = Arc::new(ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let cell = f.unbound_cell_for_application(4, [9; 16]).await;
    faults.mode.store(4, Ordering::SeqCst);
    let error = f
        .directory
        .bind_bundle_cell(&f.node, &cell.authority, &cell.control, NOW)
        .await
        .err()
        .unwrap();
    let Error::Storage(cellule_store::StorageError::NotSupported {
        source: object_store::Error::NotSupported { source },
    }) = error
    else {
        panic!("original provider error must survive unchanged-head reconciliation: {error:?}");
    };
    assert_eq!(source.to_string(), "injected bundle reply failure");
    let pinned = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    assert!(pinned.value().bundle_binding.is_some());
    let reserved = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.directory.withdraw(&reserved, NOW).await,
        Err(Error::PendingPublication)
    ));
    assert!(matches!(
        f.directory
            .load_bundle_coverage(&cell.authority, &pinned, Limits::default())
            .await,
        Err(Error::PendingPublication)
    ));
    faults.mode.store(0, Ordering::SeqCst);
    let (opened, same_pin) = f
        .directory
        .bind_bundle_cell(&reserved, &cell.authority, &pinned, NOW)
        .await
        .unwrap();
    assert_eq!(same_pin.value(), pinned.value());
    let catalog = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        opened.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.bindings.len(), 1);
    assert_eq!(catalog.bindings[0].phase, BindingPhase::Open);
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
    super::coverage::enroll(&mut f).await;
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
    assert_eq!(selected.advertisement().log().unwrap().tiered_through(), 1);
    let updates = faults.node_updates.load(Ordering::SeqCst);
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs).unwrap(),
        1
    );
    assert_eq!(faults.node_updates.load(Ordering::SeqCst), updates);
}

#[tokio::test]
async fn lease_loss_during_combined_cas_cannot_release_original_captures() {
    let faults = Arc::new(ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let directory = f.directory.clone();
    let node = f.node.clone();
    let lease = f.lease.clone();
    faults.mode.store(10, Ordering::SeqCst);
    let selecting = tokio::spawn(async move {
        directory
            .select_node_bundle(&node, &prepared, &lease, Limits::default(), NOW)
            .await
    });
    faults.node_started.notified().await;
    f.lease.fence();
    faults.node_resume.notify_one();
    assert!(matches!(selecting.await.unwrap(), Err(Error::Fenced)));
    let selected = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.advertisement().log().unwrap().tiered_through(), 1);
    assert_eq!(
        selected
            .advertisement()
            .bundle_head()
            .unwrap()
            .selected_through(),
        1
    );
    assert_eq!(f.gate.tiered_through(), 0);
    let cold = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(cold.commit_sequence(), 2);
    assert!(matches!(
        confirm_selected_coverage(&f.gate, &f.lease, &[cold]),
        Err(Error::Fenced)
    ));
    assert_eq!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root()
            .unwrap()
            .commit_sequence,
        1
    );
}

#[tokio::test]
async fn cancelled_combined_cas_keeps_assignments_for_exact_retry() {
    let faults = Arc::new(ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = Arc::new(
        f.directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .unwrap(),
    );
    let proposal = Arc::clone(&prepared);
    let directory = f.directory.clone();
    let node = f.node.clone();
    let lease = f.lease.clone();
    faults.mode.store(10, Ordering::SeqCst);
    let selecting = tokio::spawn(async move {
        directory
            .select_node_bundle(&node, &proposal, &lease, Limits::default(), NOW)
            .await
    });
    faults.node_started.notified().await;
    selecting.abort();
    assert!(selecting.await.err().unwrap().is_cancelled());
    let unchanged = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unchanged.advertisement().bundle_head(),
        f.node.advertisement().bundle_head()
    );
    assert_eq!(unchanged.advertisement().log().unwrap().tiered_through(), 0);
    assert_eq!(f.gate.progress().unwrap().issued_through, 1);
    assert_eq!(f.gate.tiered_through(), 0);
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs).unwrap(),
        1
    );
    assert_eq!(
        f.gate.prove(assigned.ticket()).await.unwrap().source(),
        DurabilitySource::Bundle
    );
}
