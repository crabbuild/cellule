//! Exercise the real preparation/upload/selection path while its PUT is paused.
use super::*;
use bytes::Bytes;
use cellule_ltx::{CellReplica, CellStorageLayout, Db, Limits};
use cellule_runtime::control::authority::{CellAuthority, VersionedControl};
use cellule_runtime::control::{Control, ControlState, Owner, RootRef};
use cellule_runtime::identity::{CellId, IncarnationId};
use cellule_runtime::node::log::DurabilityGate;
use cellule_runtime::node::log_shipper::{
    AssignedCapture, NodeLogShipper, NodeLogSubmission, NodePublicationFeed,
};
use cellule_runtime::node::log_transport::{
    AppendRequest, LocalFollowerTransport, NodeLogTransport, RetireRequest, SealRequest,
    TailRequest,
};
use cellule_store::Store;
use futures_util::stream::BoxStream;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
struct PausedBundleStore {
    inner: InMemory,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl std::fmt::Display for PausedBundleStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausedBundleStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PausedBundleStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if path.as_ref().ends_with(".cnb") && self.armed.swap(false, Ordering::AcqRel) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
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
    authority: Arc<Authority>,
    store: Arc<PausedBundleStore>,
    layout: CellStorageLayout,
    scratch: tempfile::TempDir,
    captures: Vec<AssignedCapture>,
    feed: NodePublicationFeed,
    shipper: NodeLogShipper,
    cell: CellAuthority,
    control: VersionedControl,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_followers(false).await
    }

    async fn with_followers(real: bool) -> Self {
        let tls_root = PathBuf::from(std::env::var_os("CELLULE_TEST_FLEET_TLS").unwrap());
        let tls = |index| {
            Arc::new(
                LoadedPeerTls::load(
                    &tls_root.join(format!("node-{index}.crt")),
                    &tls_root.join(format!("node-{index}.key")),
                    &tls_root.join("ca.crt"),
                    "localhost",
                )
                .unwrap(),
            )
        };
        let owner_tls = tls(0);
        let code = Digest::from_bytes([7; 32]);
        let store = Arc::new(PausedBundleStore::default());
        let layout = CellStorageLayout::new(
            Store::new(store.clone()),
            Path::from("publication-test"),
            [9; 16],
        );
        let directory = NodeDirectory::new(layout.clone(), owner_tls.fleet(), code, code);
        let session = SessionId::from_bytes([11; 16]);
        let enrollment = Enrollment::start(
            directory.clone(),
            owner_tls,
            0,
            session,
            "https://localhost:8081".into(),
            code,
            None,
        )
        .await
        .unwrap();
        enrollment.stop.send(true).unwrap();
        enrollment.heartbeat.await.unwrap().unwrap();
        let authority = enrollment.authority;
        for index in 1..=2 {
            let peer_tls = tls(index);
            let now = clock().unwrap();
            let mut available = capacity(None);
            available.follower_free_bytes = 1 << 30;
            let peer = NodeAdvertisement::sign(
                node(index),
                SessionId::from_bytes([11 + index; 16]),
                format!("https://localhost:{}", 8081 + u16::from(index)),
                peer_tls.fleet(),
                peer_tls.certificate(),
                code,
                code,
                peer_tls.signing_key(),
                1,
                now,
                now + 30_000,
                vec![code],
                vec![1],
                NodeFailureDomain::default(),
                available,
            )
            .unwrap();
            directory.create(peer, now).await.unwrap();
        }
        authority.recruit().await.unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let (mut db, cell, control) = Self::unbound(&layout, &scratch, session, 4).await;
        let control = authority.bind(&cell, &control).await.unwrap();
        db.transaction(|tx| tx.execute_batch("INSERT INTO values_ VALUES (2)"))
            .unwrap();
        let cuts = db.capture().unwrap();
        let gate = DurabilityGate::new(session, node(0), 1, [node(1), node(2)]).unwrap();
        let transport: Arc<dyn NodeLogTransport> = if real {
            Arc::new(TestFollowers([1, 2].map(|index| {
                let store = cellule_runtime::follower::FollowerStore::open(
                    scratch.path().join(format!("follower-{index}")),
                    Limits::default(),
                    cellule_ltx::DiskBudget::new(32 << 20),
                )
                .unwrap();
                LocalFollowerTransport::new(node(index), store)
            })))
        } else {
            Arc::new(UnavailableFollowers)
        };
        let shipper = NodeLogShipper::new(gate, transport, Limits::default()).unwrap();
        let mut feed = shipper.take_publication_feed().unwrap();
        shipper
            .submit(
                NodeLogSubmission::new(
                    ApplicationId::from_bytes([9; 16]),
                    control.value().cell,
                    control.value().incarnation,
                    control.value().epoch,
                    2,
                    &cuts,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let capture = feed.recv().await.unwrap();
        Self {
            authority,
            store,
            layout,
            scratch,
            captures: vec![capture],
            feed,
            shipper,
            cell,
            control,
        }
    }

    async fn unbound(
        layout: &CellStorageLayout,
        scratch: &tempfile::TempDir,
        session: SessionId,
        byte: u8,
    ) -> (Db, CellAuthority, VersionedControl) {
        let cell = CellId::from_bytes([byte; 32]);
        let incarnation = IncarnationId::from_bytes([byte; 16]);
        let mut db = Db::open(
            &scratch.path().join(format!("{byte}.sqlite")),
            Limits::default(),
        )
        .unwrap();
        db.transaction(|tx| {
            tx.execute_batch("CREATE TABLE values_(n INTEGER); INSERT INTO values_ VALUES (1)")
        })
        .unwrap();
        let cuts = db.capture().unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *cell.as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let prepared = replica.prepare(None, &cuts, 1, 1).await.unwrap();
        let mut control = Control::initial(
            cell,
            incarnation,
            Owner {
                session,
                endpoint: "https://localhost:8081".into(),
            },
            Digest::from_bytes([7; 32]),
            1,
        )
        .unwrap();
        control.state = ControlState::Serving;
        control.root = Some(RootRef::from_ltx(cell, incarnation, prepared.root()).unwrap());
        layout
            .store()
            .create_strict(
                &layout.control_path(cell.as_bytes()),
                Bytes::from(control.encode().unwrap()),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(layout.clone());
        let observed = authority.load(cell).await.unwrap().unwrap();
        (db, authority, observed)
    }
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn two_cohort_round_keeps_renewal_live_and_selects_only_in_order() {
    let mut f = Fixture::with_followers(true).await;
    let session = SessionId::from_bytes([11; 16]);
    let (mut db, second, unbound) = Fixture::unbound(&f.layout, &f.scratch, session, 5).await;
    let pinned = f.authority.bind(&second, &unbound).await.unwrap();
    db.transaction(|tx| tx.execute_batch("INSERT INTO values_ VALUES (2)"))
        .unwrap();
    let cuts = db.capture().unwrap();
    f.shipper
        .submit(
            NodeLogSubmission::new(
                ApplicationId::from_bytes([9; 16]),
                pinned.value().cell,
                pinned.value().incarnation,
                pinned.value().epoch,
                2,
                &cuts,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let second_capture = [f.feed.recv().await.unwrap()];
    let (_, third, unbound_third) = Fixture::unbound(&f.layout, &f.scratch, session, 6).await;
    let round = f.authority.begin_round().await.unwrap();
    let original_expiry = f
        .authority
        .state
        .lock()
        .await
        .observed
        .advertisement()
        .expires_at_ms();
    let first = round.stage(None, &f.captures, &[], None).await.unwrap();
    f.store.armed.store(true, Ordering::Release);
    let mut first_upload = Box::pin(round.upload(Arc::clone(&first)));
    assert!(futures_util::poll!(first_upload.as_mut()).is_pending());
    assert!(futures_util::poll!(Box::pin(f.store.entered.notified())).is_ready());
    // The example uses wall time for signed advertisements. Actual elapsed
    // time is required here; paused Tokio time cannot expire that snapshot.
    for seconds in [11, 11, 9] {
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        f.authority.refresh().await.unwrap();
    }
    assert!(clock().unwrap() > original_expiry);
    f.authority.lease.check().unwrap();
    let successor = round
        .stage(Some(&first), &second_capture, &[], None)
        .await
        .unwrap();
    let successor = round.upload(successor).await.unwrap();
    assert!(
        round
            .select(&successor, &[], &f.authority.lease)
            .await
            .is_err(),
        "a completed successor PUT cannot select ahead of the predecessor"
    );
    assert_eq!(
        f.authority
            .state
            .lock()
            .await
            .observed
            .advertisement()
            .log()
            .unwrap()
            .tiered_through(),
        0
    );
    let bind = f.authority.bind(&third, &unbound_third);
    tokio::pin!(bind);
    assert!(futures_util::poll!(bind.as_mut()).is_pending());
    f.store.resume.notify_one();
    let uploaded = first_upload.await.unwrap();
    let first_proofs = round
        .select(&uploaded, &[], &f.authority.lease)
        .await
        .unwrap();
    assert_eq!(first_proofs[0].commit_sequence(), 2);
    // First selection can win while the managed successor is still assembling.
    // Re-observing that exact selected predecessor must yield the same proposal.
    let after_selection = round
        .stage(Some(&first), &second_capture, &[], None)
        .await
        .unwrap();
    assert_eq!(after_selection.head(), successor.head());
    drop(after_selection);
    let second_proofs = round
        .select(&successor, &[], &f.authority.lease)
        .await
        .unwrap();
    assert_eq!(second_proofs[0].commit_sequence(), 2);
    assert_eq!(
        f.authority
            .state
            .lock()
            .await
            .observed
            .advertisement()
            .log()
            .unwrap()
            .tiered_through(),
        2
    );
    drop(round);
    bind.await.unwrap();
    for (cell, control) in [(&f.cell, &f.control), (&second, &pinned)] {
        assert_eq!(
            f.authority
                .directory
                .load_bundle_coverage(cell, control, Limits::default())
                .await
                .unwrap()
                .commit_sequence(),
            2
        );
    }
    f.authority.lease.fence();
    f.shipper.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn paused_bundle_upload_allows_renewal_but_serializes_new_bindings() {
    let f = Fixture::new().await;
    let (_, sibling, unbound) =
        Fixture::unbound(&f.layout, &f.scratch, SessionId::from_bytes([11; 16]), 5).await;
    tokio::time::pause();
    f.store.armed.store(true, Ordering::Release);
    let selection = f
        .authority
        .select(&f.captures, &[], &[], None, &f.authority.lease);
    tokio::pin!(selection);
    assert!(futures_util::poll!(selection.as_mut()).is_pending());
    assert!(futures_util::poll!(Box::pin(f.store.entered.notified())).is_ready());
    tokio::time::advance(Duration::from_secs(11)).await;
    let refresh = f.authority.refresh();
    tokio::pin!(refresh);
    assert!(
        matches!(
            futures_util::poll!(refresh.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ),
        "renewal must complete while the immutable bundle PUT is paused"
    );
    let bind = f.authority.bind(&sibling, &unbound);
    tokio::pin!(bind);
    assert!(
        futures_util::poll!(bind.as_mut()).is_pending(),
        "binding cannot replace the prepared catalog"
    );
    f.store.resume.notify_one();
    let proofs = selection.await.unwrap();
    assert_eq!(proofs.len(), 1);
    assert_eq!(proofs[0].commit_sequence(), 2);
    let pinned = bind.await.unwrap();
    let state = f.authority.state.lock().await;
    assert_eq!(state.observed.advertisement().progress(), 2);
    assert_eq!(
        state
            .observed
            .advertisement()
            .log()
            .unwrap()
            .tiered_through(),
        1
    );
    assert_eq!(
        f.authority
            .directory
            .load_bundle_coverage(&f.cell, &f.control, Limits::default())
            .await
            .unwrap()
            .commit_sequence(),
        2
    );
    assert!(
        f.authority
            .directory
            .load_bundle_coverage(&sibling, &pinned, Limits::default())
            .await
            .is_ok()
    );
    f.authority.lease.fence();
    f.shipper.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn fenced_owner_cannot_select_a_late_bundle_upload() {
    let f = Fixture::new().await;
    let before = f
        .authority
        .state
        .lock()
        .await
        .observed
        .advertisement()
        .bundle_head();
    f.store.armed.store(true, Ordering::Release);
    let selection = f
        .authority
        .select(&f.captures, &[], &[], None, &f.authority.lease);
    tokio::pin!(selection);
    assert!(futures_util::poll!(selection.as_mut()).is_pending());
    assert!(futures_util::poll!(Box::pin(f.store.entered.notified())).is_ready());
    f.authority.lease.fence();
    f.store.resume.notify_one();
    assert!(matches!(selection.await, Err(Error::Fenced)));
    let current = f
        .authority
        .directory
        .load_if_live(SessionId::from_bytes([11; 16]), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.advertisement().bundle_head(), before);
    assert_eq!(current.advertisement().log().unwrap().tiered_through(), 0);
    f.shipper.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn changed_catalog_is_not_overwritten_after_upload() {
    let f = Fixture::new().await;
    let (_, sibling, unbound) =
        Fixture::unbound(&f.layout, &f.scratch, SessionId::from_bytes([11; 16]), 5).await;
    let before = f.authority.state.lock().await.observed.clone();
    f.store.armed.store(true, Ordering::Release);
    let selection = f
        .authority
        .select(&f.captures, &[], &[], None, &f.authority.lease);
    tokio::pin!(selection);
    assert!(futures_util::poll!(selection.as_mut()).is_pending());
    assert!(futures_util::poll!(Box::pin(f.store.entered.notified())).is_ready());
    // A canonical writer outside this process is not governed by our mutex.
    // The actual predecessor CAS must still reject its replacement.
    let (changed, pinned) = f
        .authority
        .directory
        .bind_bundle_cell(&before, &sibling, &unbound, clock().unwrap())
        .await
        .unwrap();
    f.store.resume.notify_one();
    assert!(matches!(selection.await, Err(Error::Fenced)));
    let current = f
        .authority
        .directory
        .load_if_live(SessionId::from_bytes([11; 16]), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.advertisement().bundle_head(),
        changed.advertisement().bundle_head()
    );
    assert_eq!(current.advertisement().log().unwrap().tiered_through(), 0);
    assert!(
        f.authority
            .directory
            .load_bundle_coverage(&sibling, &pinned, Limits::default())
            .await
            .is_ok()
    );
    f.authority.lease.fence();
    f.shipper.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires generated CELLULE_TEST_FLEET_TLS fixture"]
async fn cancelled_preparation_releases_ordering_without_crediting_the_capture() {
    let f = Fixture::new().await;
    let (_, sibling, unbound) =
        Fixture::unbound(&f.layout, &f.scratch, SessionId::from_bytes([11; 16]), 5).await;
    f.store.armed.store(true, Ordering::Release);
    let mut selection = f
        .authority
        .select(&f.captures, &[], &[], None, &f.authority.lease);
    assert!(futures_util::poll!(selection.as_mut()).is_pending());
    assert!(futures_util::poll!(Box::pin(f.store.entered.notified())).is_ready());
    drop(selection);
    let pinned = f.authority.bind(&sibling, &unbound).await.unwrap();
    assert_eq!(
        f.authority
            .state
            .lock()
            .await
            .observed
            .advertisement()
            .log()
            .unwrap()
            .tiered_through(),
        0
    );
    // The original captured assignment and its admission are still held by the
    // caller. Retry that same obligation against the new predecessor catalog.
    let proofs = f
        .authority
        .select(&f.captures, &[], &[], None, &f.authority.lease)
        .await
        .unwrap();
    assert_eq!(proofs[0].commit_sequence(), 2);
    assert!(
        f.authority
            .directory
            .load_bundle_coverage(&sibling, &pinned, Limits::default())
            .await
            .is_ok()
    );
    f.authority.lease.fence();
    f.shipper.shutdown().await.unwrap();
}

// The two-cohort fixture retains actual file-backed follower acknowledgements.
struct TestFollowers([LocalFollowerTransport; 2]);
impl TestFollowers {
    fn member(&self, member: NodeId) -> &LocalFollowerTransport {
        &self.0[usize::from(member == node(2))]
    }
}
impl NodeLogTransport for TestFollowers {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<cellule_runtime::follower::FollowerReceipt>> {
        self.member(member).append(member, request)
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, Result<cellule_runtime::follower::FollowerReceipt>> {
        self.member(member).seal(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, Result<cellule_runtime::follower::FollowerReceipt>> {
        self.member(member).retire(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        self.member(member).tail(member, request)
    }
}

// These single-cohort fixtures certify only bucket publication, never follower durability.
struct UnavailableFollowers;
impl NodeLogTransport for UnavailableFollowers {
    fn append<'a>(
        &'a self,
        _: NodeId,
        _: AppendRequest,
    ) -> BoxFuture<'a, Result<cellule_runtime::follower::FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("test follower unavailable")) })
    }
    fn seal<'a>(
        &'a self,
        _: NodeId,
        _: SealRequest,
    ) -> BoxFuture<'a, Result<cellule_runtime::follower::FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("test follower unavailable")) })
    }
    fn retire<'a>(
        &'a self,
        _: NodeId,
        _: RetireRequest,
    ) -> BoxFuture<'a, Result<cellule_runtime::follower::FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("test follower unavailable")) })
    }
    fn tail<'a>(&'a self, _: NodeId, _: TailRequest) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        Box::pin(async { Err(Error::Node("test follower unavailable")) })
    }
}
