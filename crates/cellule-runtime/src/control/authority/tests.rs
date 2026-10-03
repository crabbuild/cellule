use super::*;
use crate::control::{ControlState, RootRef};
use crate::identity::{Digest, SessionId};
use cellule_store::Store;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(super) struct FaultStore {
    inner: InMemory,
    pub(super) fault: AtomicUsize,
    pub(super) entered: Notify,
    pub(super) resume: Notify,
    history_writes: AtomicUsize,
    control_writes: AtomicUsize,
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owner-history-fault-store")
    }
}

fn denied(_path: &Path) -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "original owner history failure",
        )),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        body: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let history = path.as_ref().contains("/owner-history/");
        let control = path.as_ref().ends_with("/control.json");
        if history {
            self.history_writes.fetch_add(1, Ordering::SeqCst);
        }
        if control {
            self.control_writes.fetch_add(1, Ordering::SeqCst);
        }
        let fault = if history || path.as_ref().contains("/acquisitions/") {
            self.fault
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |fault| {
                    (1..=3).contains(&fault).then_some(0)
                })
                .unwrap_or(0)
        } else {
            0
        };
        if fault == 1 {
            return Err(denied(path));
        }
        if fault == 3 {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.put_opts(path, body, opts).await?;
        if fault == 2 {
            return Err(denied(path));
        }
        if control
            && self
                .fault
                .compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(denied(path));
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
        if path.as_ref().contains("/acquisitions/")
            && self
                .fault
                .compare_exchange(6, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(denied(path));
        }
        if path.as_ref().contains("/owner-history/")
            && self
                .fault
                .compare_exchange(5, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.entered.notify_one();
            self.resume.notified().await;
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

struct Fixture {
    store: Arc<FaultStore>,
    authority: CellAuthority,
    original: VersionedControl,
}

impl Fixture {
    async fn new(published: bool) -> Self {
        let store = Arc::new(FaultStore::default());
        let authority = CellAuthority::new(CellStorageLayout::new(
            Store::new(store.clone()),
            Path::from("owner-test"),
            [7; 16],
        ));
        let mut control = Control::initial(
            CellId::from_bytes([1; 32]),
            IncarnationId::from_bytes([2; 16]),
            owner(3),
            Digest::from_bytes([4; 32]),
            1,
        )
        .unwrap();
        if published {
            control.root = Some(RootRef {
                digest: Digest::from_bytes([8; 32]),
                txid: 7,
                checksum: cellule_ltx::types::CHECKSUM_FLAG | 7,
                commit_sequence: 7,
            });
            control.state = ControlState::Serving;
        }
        authority
            .layout
            .store()
            .create_strict(
                &authority.layout.control_path(control.cell.as_bytes()),
                Bytes::from(control.encode().unwrap()),
            )
            .await
            .unwrap();
        let original = authority.load(control.cell).await.unwrap().unwrap();
        Self {
            store,
            authority,
            original,
        }
    }
    fn cell(&self) -> CellId {
        self.original.value.cell
    }
    fn history_path(&self, epoch: u64) -> Path {
        self.authority.layout.owner_observation_path(
            self.cell().as_bytes(),
            self.original.value.incarnation.as_bytes(),
            epoch,
        )
    }
}

fn owner(byte: u8) -> Owner {
    Owner {
        session: SessionId::from_bytes([byte; 16]),
        endpoint: format!("https://node-{byte}.test"),
    }
}

#[tokio::test]
async fn bounded_canonical_history_rejects_conflicting_or_malformed_observations() {
    let f = Fixture::new(false).await;
    assert!(matches!(
        f.authority.owner_history(f.cell(), 0).await,
        Err(Error::Capacity(_))
    ));
    for body in [
        Bytes::from_static(b"{}"),
        Bytes::from(vec![b' '; 8193]),
        Bytes::from(format!(
            "{}\n",
            String::from_utf8(f.original.value.encode().unwrap()).unwrap()
        )),
    ] {
        f.authority
            .layout
            .store()
            .create_strict(&f.history_path(1), body)
            .await
            .unwrap();
        assert!(
            f.authority
                .owner_observation(f.cell(), f.original.value.incarnation, 1)
                .await
                .is_err()
        );
        f.authority
            .layout
            .store()
            .delete(&f.history_path(1))
            .await
            .unwrap();
    }
    let mut conflicting = f.original.value.clone();
    conflicting.owner = Some(owner(9));
    f.authority
        .layout
        .store()
        .create_strict(
            &f.history_path(1),
            Bytes::from(conflicting.encode().unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(
        f.authority
            .transition(
                &f.original,
                f.original.value.takeover(owner(5)).unwrap(),
                Transition::Takeover
            )
            .await,
        Err(Error::Control(_))
    ));
    assert_eq!(
        f.authority.load(f.cell()).await.unwrap().unwrap().value,
        f.original.value
    );
    assert_eq!(f.store.control_writes.load(Ordering::SeqCst), 1);
}

fn tombstone(control: &Control) -> Control {
    let mut next = control.clone();
    next.state = ControlState::Tombstoned;
    next.owner = None;
    next.epoch += 1;
    next.revision += 1;
    next.progress += 1;
    next
}

#[tokio::test]
async fn rootless_and_recovering_takeovers_retain_all_original_epochs() {
    let f = Fixture::new(false).await;
    let first = f
        .authority
        .transition(
            &f.original,
            f.original.value.takeover(owner(5)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    let second = f
        .authority
        .transition(
            &first,
            first.value.takeover(owner(6)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    let independent = CellAuthority::new(CellStorageLayout::new(
        Store::new(f.store.clone()),
        Path::from("owner-test"),
        [7; 16],
    ));
    let history = independent.owner_history(f.cell(), 3).await.unwrap();
    assert_eq!(history.current(), second.value());
    assert_eq!(
        history.owners(),
        &[
            f.original.value.clone(),
            first.value.clone(),
            second.value.clone()
        ]
    );
    assert!(
        history
            .owners()
            .iter()
            .all(|control| control.root.is_none())
    );
    assert_eq!(f.store.history_writes.load(Ordering::SeqCst), 2);
    assert!(matches!(
        independent.owner_history(f.cell(), 2).await,
        Err(Error::Capacity(_))
    ));
}

#[tokio::test]
async fn release_idle_acquisition_and_tombstone_keep_object_covered_history() {
    let f = Fixture::new(true).await;
    let idle = f
        .authority
        .transition(
            &f.original,
            f.original.value.release().unwrap(),
            Transition::Release,
        )
        .await
        .unwrap();
    let history = f.authority.owner_history(f.cell(), 1).await.unwrap();
    assert_eq!(history.owners(), std::slice::from_ref(&f.original.value));
    assert_eq!(history.current(), idle.value());
    let next = f
        .authority
        .transition(
            &idle,
            idle.value.takeover(owner(5)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    let retired = f
        .authority
        .transition(&next, tombstone(&next.value), Transition::Tombstone)
        .await
        .unwrap();
    let history = f.authority.owner_history(f.cell(), 2).await.unwrap();
    assert_eq!(
        history.owners(),
        &[f.original.value.clone(), next.value.clone()]
    );
    assert_eq!(history.current(), retired.value());
    assert_eq!(f.store.history_writes.load(Ordering::SeqCst), 2);
    assert!(
        history
            .owners()
            .iter()
            .all(|control| control.root == f.original.value.root)
    );
}

#[tokio::test]
async fn idle_tombstone_does_not_invent_an_owner_epoch() {
    let f = Fixture::new(true).await;
    let idle = f
        .authority
        .transition(
            &f.original,
            f.original.value.release().unwrap(),
            Transition::Release,
        )
        .await
        .unwrap();
    f.authority
        .transition(&idle, tombstone(&idle.value), Transition::Tombstone)
        .await
        .unwrap();
    assert_eq!(
        f.authority
            .owner_history(f.cell(), 1)
            .await
            .unwrap()
            .owners(),
        std::slice::from_ref(&f.original.value)
    );
}

#[tokio::test]
async fn same_owner_publication_and_renewal_do_not_write_history() {
    let f = Fixture::new(false).await;
    let mut published = f.original.value.clone();
    published.state = ControlState::Serving;
    published.root = Some(RootRef {
        digest: Digest::from_bytes([8; 32]),
        txid: 1,
        checksum: cellule_ltx::types::CHECKSUM_FLAG | 1,
        commit_sequence: 1,
    });
    published.revision += 1;
    published.progress += 1;
    let published = f
        .authority
        .transition(&f.original, published, Transition::Publish)
        .await
        .unwrap();
    let renewed = f
        .authority
        .transition(
            &published,
            published.value.renew().unwrap(),
            Transition::Renew,
        )
        .await
        .unwrap();
    assert_eq!(f.store.history_writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        f.authority
            .owner_history(f.cell(), 1)
            .await
            .unwrap()
            .owners(),
        &[renewed.value]
    );
}

#[tokio::test]
async fn history_failure_preserves_source_and_prevents_owner_departure() {
    let f = Fixture::new(false).await;
    f.store.fault.store(1, Ordering::SeqCst);
    let error = f
        .authority
        .transition(
            &f.original,
            f.original.value.takeover(owner(5)).unwrap(),
            Transition::Takeover,
        )
        .await
        .err()
        .unwrap();
    let Error::Storage(source) = error else {
        panic!("original storage failure expected");
    };
    let mut cause = std::error::Error::source(&source);
    let mut original = None;
    while let Some(error) = cause {
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            original = Some(error);
        }
        cause = error.source();
    }
    let original = original.unwrap();
    assert_eq!(original.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(original.to_string(), "original owner history failure");
    assert_eq!(
        f.authority.load(f.cell()).await.unwrap().unwrap().value,
        f.original.value
    );
    assert_eq!(f.store.control_writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lost_history_reply_is_confirmed_before_the_authority_cas() {
    let f = Fixture::new(false).await;
    f.store.fault.store(2, Ordering::SeqCst);
    let moved = f
        .authority
        .transition(
            &f.original,
            f.original.value.takeover(owner(5)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    let history = f.authority.owner_history(f.cell(), 2).await.unwrap();
    assert_eq!(history.owners(), &[f.original.value.clone(), moved.value]);
    assert_eq!(f.store.history_writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_delayed_original_proposal_cannot_replace_a_newer_observation() {
    let f = Fixture::new(false).await;
    f.store.fault.store(3, Ordering::SeqCst);
    let original = f.original.clone();
    let authority = f.authority.clone();
    let delayed = tokio::spawn(async move {
        authority
            .transition(
                &original,
                original.value.takeover(owner(5)).unwrap(),
                Transition::Takeover,
            )
            .await
    });
    f.store.entered.notified().await;
    let renewed = f
        .authority
        .transition(
            &f.original,
            f.original.value.renew().unwrap(),
            Transition::Renew,
        )
        .await
        .unwrap();
    let moved = f
        .authority
        .transition(
            &renewed,
            renewed.value.takeover(owner(6)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    f.store.resume.notify_one();
    assert!(matches!(
        delayed.await.unwrap(),
        Err(Error::Storage(StorageError::StateConflict { .. }))
    ));
    let history = f.authority.owner_history(f.cell(), 2).await.unwrap();
    assert_eq!(history.owners(), &[renewed.value.clone(), moved.value]);
    assert_eq!(
        f.authority
            .owner_observation(f.cell(), renewed.value.incarnation, 1)
            .await
            .unwrap()
            .unwrap(),
        renewed.value
    );
}

#[tokio::test]
async fn cancelled_history_waiter_cannot_remove_original_owner() {
    let f = Fixture::new(false).await;
    f.store.fault.store(3, Ordering::SeqCst);
    let original = f.original.clone();
    let authority = f.authority.clone();
    let pending = tokio::spawn(async move {
        authority
            .transition(
                &original,
                original.value.takeover(owner(5)).unwrap(),
                Transition::Takeover,
            )
            .await
    });
    f.store.entered.notified().await;
    pending.abort();
    let Err(cancelled) = pending.await else {
        panic!("waiter must be cancelled");
    };
    assert!(cancelled.is_cancelled());
    assert_eq!(
        f.authority.load(f.cell()).await.unwrap().unwrap().value,
        f.original.value
    );
    let moved = f
        .authority
        .transition(
            &f.original,
            f.original.value.takeover(owner(6)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    assert_eq!(
        f.authority
            .owner_history(f.cell(), 2)
            .await
            .unwrap()
            .owners(),
        &[f.original.value.clone(), moved.value]
    );
}

#[tokio::test]
async fn retained_proposal_supplies_no_departure_proof() {
    let f = Fixture::new(false).await;
    f.authority.retain_owner(&f.original.value).await.unwrap();
    let history = f.authority.owner_history(f.cell(), 1).await.unwrap();
    assert_eq!(history.current(), f.original.value());
    assert_eq!(history.owners(), std::slice::from_ref(&f.original.value));
    assert!(
        f.authority
            .owner_observation(f.cell(), f.original.value.incarnation, 1)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn missing_legacy_history_and_invalid_rows_cannot_exclude_original_owners() {
    let f = Fixture::new(false).await;
    let moved = f
        .authority
        .transition(
            &f.original,
            f.original.value.takeover(owner(5)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    f.authority
        .layout
        .store()
        .delete(&f.history_path(1))
        .await
        .unwrap();
    assert!(matches!(
        f.authority.owner_history(f.cell(), 2).await,
        Err(Error::OwnerHistoryIncomplete { epoch: 1, .. })
    ));
    for variant in 0..4 {
        let mut foreign = f.original.value.clone();
        match variant {
            0 => foreign.cell = CellId::from_bytes([9; 32]),
            1 => foreign.incarnation = IncarnationId::from_bytes([9; 16]),
            2 => foreign.epoch = 2,
            _ => {
                foreign.state = ControlState::Tombstoned;
                foreign.owner = None;
            }
        }
        f.authority
            .layout
            .store()
            .create_strict(&f.history_path(1), Bytes::from(foreign.encode().unwrap()))
            .await
            .unwrap();
        assert!(matches!(
            f.authority.owner_history(f.cell(), 2).await,
            Err(Error::Control(_))
        ));
        f.authority
            .layout
            .store()
            .delete(&f.history_path(1))
            .await
            .unwrap();
    }
    assert_eq!(
        f.authority.load(f.cell()).await.unwrap().unwrap().value,
        moved.value
    );
    assert!(
        f.authority
            .owner_observation(f.cell(), f.original.value.incarnation, 0)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn lost_authority_reply_does_not_lose_original_history() {
    let f = Fixture::new(true).await;
    f.store.fault.store(4, Ordering::SeqCst);
    let idle = f.original.value.release().unwrap();
    assert!(matches!(
        f.authority
            .transition(&f.original, idle.clone(), Transition::Release)
            .await,
        Err(Error::Storage(_))
    ));
    let history = f.authority.owner_history(f.cell(), 1).await.unwrap();
    assert_eq!(history.current(), &idle);
    assert_eq!(history.owners(), std::slice::from_ref(&f.original.value));
    assert_eq!(f.store.history_writes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        f.authority
            .transition(&f.original, idle, Transition::Release)
            .await,
        Err(Error::Storage(StorageError::StateConflict { .. }))
    ));
    assert_eq!(f.store.history_writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn history_read_refuses_authority_progress_during_collection() {
    let f = Fixture::new(false).await;
    let moved = f
        .authority
        .transition(
            &f.original,
            f.original.value.takeover(owner(5)).unwrap(),
            Transition::Takeover,
        )
        .await
        .unwrap();
    f.store.fault.store(5, Ordering::SeqCst);
    let authority = f.authority.clone();
    let cell = f.cell();
    let pending = tokio::spawn(async move { authority.owner_history(cell, 2).await });
    f.store.entered.notified().await;
    let renewed = f
        .authority
        .transition(&moved, moved.value.renew().unwrap(), Transition::Renew)
        .await
        .unwrap();
    f.store.resume.notify_one();
    assert!(matches!(pending.await.unwrap(), Err(Error::Fenced)));
    assert_eq!(
        f.authority
            .owner_history(f.cell(), 2)
            .await
            .unwrap()
            .current(),
        renewed.value()
    );
}
