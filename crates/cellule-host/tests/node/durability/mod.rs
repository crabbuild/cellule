//! Requested rotation, native follower fences and retained supervisor joins.
mod observation;
use super::fleet_actions::clock;
use super::*;
use bytes::Bytes;
use cellule_runtime::cell::catalog::{CatalogEntry, CellCatalog};
use cellule_runtime::cell::executor::{HandlerOutcome, MutationIdentity, StoredOutcome};
use cellule_runtime::control::{Owner, authority::CellAuthority};
use cellule_runtime::follower::FollowerReceipt;
use cellule_runtime::identity::{
    CellTarget, IncarnationId, NamespaceId, NodeId, RequestId, TenantId,
};
use cellule_runtime::ltx::{CellReplica, CellStorageLayout};
use cellule_runtime::node::durability::NodeLogAuthority;
use cellule_runtime::node::log_transport::{
    AppendRequest, LocalFollowerTransport, NodeLogTransport, RetireRequest, SealRequest,
    TailRequest,
};
use cellule_store::Store;
use futures_util::future::BoxFuture;
use object_store::{memory::InMemory, path::Path};
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct Authority {
    closes: Mutex<Vec<u64>>,
    coverage: Mutex<Vec<(u64, u64)>>,
    lose_cleanup_reply: AtomicBool,
    close_attempts: Mutex<Vec<u64>>,
}
impl NodeLogAuthority for Authority {
    fn activate<'a>(&'a self, _epoch: u64) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn advance_coverage<'a>(
        &'a self,
        epoch: u64,
        through: u64,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            self.coverage.lock().unwrap().push((epoch, through));
            Ok(())
        })
    }
    fn close<'a>(
        &'a self,
        retirement: &'a cellule_runtime::node::log::NodeLogRetirementObservation,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        let barrier = retirement.barrier();
        Box::pin(async move {
            let epoch = barrier.log_epoch();
            self.close_attempts.lock().unwrap().push(epoch);
            {
                let mut closes = self.closes.lock().unwrap();
                if !closes.contains(&epoch) {
                    closes.push(epoch);
                }
            }
            if epoch == 2 && self.lose_cleanup_reply.load(Ordering::Acquire) {
                return Err(Error::Facility {
                    name: "test-authority-close",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "accepted cleanup close reply lost",
                    )),
                });
            }
            Ok(())
        })
    }
}

struct Transport {
    locals: Vec<(NodeId, LocalFollowerTransport)>,
    requests: Mutex<Vec<(NodeId, RetireRequest)>>,
    frames: Mutex<Vec<Bytes>>,
    lose_retire: AtomicBool,
    pause_retire: AtomicBool,
    entered: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
}
impl Transport {
    fn local(&self, member: NodeId) -> &LocalFollowerTransport {
        &self.locals.iter().find(|(id, _)| *id == member).unwrap().1
    }
}
impl NodeLogTransport for Transport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.frames.lock().unwrap().extend(request.frames.clone());
        self.local(member).append(member, request)
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.local(member).seal(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<Vec<Bytes>>> {
        self.local(member).tail(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push((member, request));
            let receipt = self.local(member).retire(member, request).await?;
            if member == self.locals[0].0 {
                if self.pause_retire.swap(false, Ordering::AcqRel) {
                    self.entered.add_permits(1);
                    self.resume.acquire().await.unwrap().forget();
                }
                if self.lose_retire.load(Ordering::Acquire) {
                    return Err(Error::Facility {
                        name: "test-retirement",
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "native retire response lost",
                        )),
                    });
                }
            }
            Ok(receipt)
        })
    }
}

struct Provider {
    session: SessionId,
    lease: NodeLeaseGuard,
    transport: Arc<Transport>,
    authority: Arc<Authority>,
    next_epoch: AtomicU64,
    recruited: Mutex<Vec<(u64, Vec<NodeId>)>>,
    events: Mutex<Vec<NodeDurabilityRotation>>,
    fail_recruit: AtomicBool,
    pause_recruit: AtomicBool,
    entered: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
    abandoned: Arc<AtomicBool>,
    in_flight: AtomicBool,
    wrong_boot: AtomicBool,
    wrong_node: AtomicBool,
    non_advancing: AtomicBool,
}
impl NodeDurabilityProvider for Provider {
    fn recruit(
        self: Arc<Self>,
        limits: ReplicaLimits,
        _bytes: u64,
        _live: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        Box::pin(async move {
            if self.pause_recruit.swap(false, Ordering::AcqRel) {
                self.in_flight.store(true, Ordering::Release);
                struct AcceptedRecruit {
                    abandoned: Arc<AtomicBool>,
                    finished: bool,
                }
                impl Drop for AcceptedRecruit {
                    fn drop(&mut self) {
                        if !self.finished {
                            self.abandoned.store(true, Ordering::Release);
                        }
                    }
                }
                let mut accepted = AcceptedRecruit {
                    abandoned: self.abandoned.clone(),
                    finished: false,
                };
                self.entered.add_permits(1);
                self.resume.acquire().await.unwrap().forget();
                accepted.finished = true;
                self.in_flight.store(false, Ordering::Release);
            }
            if self.fail_recruit.swap(false, Ordering::AcqRel) {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "replacement recruitment unavailable",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            let epoch = self.next_epoch.fetch_add(1, Ordering::AcqRel);
            let session = if self.wrong_boot.swap(false, Ordering::AcqRel) {
                SessionId::from_bytes([238; 16])
            } else {
                self.session
            };
            let node = if self.wrong_node.swap(false, Ordering::AcqRel) {
                NodeId::from_bytes([237; 16])
            } else {
                NodeId::from_bytes([240; 16])
            };
            let configured_epoch = if self.non_advancing.swap(false, Ordering::AcqRel) {
                1
            } else {
                epoch
            };
            let members = if epoch == 1 {
                vec![self.transport.locals[0].0, self.transport.locals[1].0]
            } else {
                vec![self.transport.locals[1].0, self.transport.locals[2].0]
            };
            self.recruited
                .lock()
                .unwrap()
                .push((epoch, members.clone()));
            Ok(Some(NodeDurabilityConfig::new(
                session,
                node,
                configured_epoch,
                members,
                self.transport.clone(),
                self.authority.clone(),
                self.lease.clone(),
                limits,
                Default::default(),
            )?))
        })
    }
    fn rotation_event(&self, event: NodeDurabilityRotation) {
        self.events.lock().unwrap().push(event);
    }
}

struct Fixture {
    node: Arc<CellNode>,
    provider: Arc<Provider>,
    directories: Vec<tempfile::TempDir>,
    source: tempfile::TempDir,
    target: CellTarget,
    replica: CellReplica,
    authority: CellAuthority,
    handle: cellule_runtime::cell::actor::CellHandle,
    withdrawn: Arc<AtomicBool>,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_threshold(u64::MAX).await
    }
    async fn with_threshold(max_frames: u64) -> Self {
        let session = SessionId::from_bytes([239; 16]);
        let node = Arc::new(
            CellNodeBuilder::new(application())
                .with_runtime(
                    SqlWorkerPool::new(1, 8)
                        .unwrap()
                        .with_native_memory_limit(128 << 20)
                        .unwrap(),
                    64 << 20,
                )
                .with_replica_host(
                    ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
                )
                .with_session(session)
                .build()
                .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let tasks = node
            .install_task_group(CancellationToken::new(), shutdown.clone())
            .unwrap();
        let now = clock();
        let lease = NodeLeaseGuard::new(now, now + 60_000).unwrap();
        node.install_node_lease_for_startup(lease.clone()).unwrap();
        let members = [
            NodeId::from_bytes([241; 16]),
            NodeId::from_bytes([242; 16]),
            NodeId::from_bytes([243; 16]),
        ];
        let directories: Vec<_> = members
            .iter()
            .map(|_| tempfile::tempdir().unwrap())
            .collect();
        let locals = members
            .iter()
            .zip(&directories)
            .map(|(member, directory)| {
                let store = FollowerStore::open(
                    directory.path().to_owned(),
                    ReplicaLimits::default(),
                    DiskBudget::new(1 << 30),
                )
                .unwrap();
                (*member, LocalFollowerTransport::new(*member, store))
            })
            .collect();
        let transport = Arc::new(Transport {
            locals,
            requests: Mutex::new(Vec::new()),
            frames: Mutex::new(Vec::new()),
            lose_retire: AtomicBool::new(false),
            pause_retire: AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            resume: tokio::sync::Semaphore::new(0),
        });
        let provider = Arc::new(Provider {
            session,
            lease,
            transport,
            authority: Arc::new(Authority::default()),
            next_epoch: AtomicU64::new(1),
            recruited: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            fail_recruit: AtomicBool::new(false),
            pause_recruit: AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            resume: tokio::sync::Semaphore::new(0),
            abandoned: Arc::new(AtomicBool::new(false)),
            in_flight: AtomicBool::new(false),
            wrong_boot: AtomicBool::new(false),
            wrong_node: AtomicBool::new(false),
            non_advancing: AtomicBool::new(false),
        });
        let withdrawn = Arc::new(AtomicBool::new(false));
        let observed_withdrawal = withdrawn.clone();
        let observed_provider = provider.clone();
        tasks
            .spawn_lease_maintenance(async move {
                shutdown.cancelled().await;
                observed_withdrawal.store(true, Ordering::Release);
                if observed_provider.in_flight.load(Ordering::Acquire) {
                    return Err(Error::Control(
                        "session withdrew during accepted recruitment",
                    ));
                }
                Ok(())
            })
            .unwrap();
        let limits = ReplicaLimits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..ReplicaLimits::default()
        };
        node.install_node_durability_provider(
            provider.clone(),
            NodeDurabilitySupervisorConfig::new(
                ApplicationId::from_bytes([3; 16]),
                limits,
                1,
                3,
                Duration::from_millis(10),
                Duration::from_millis(5),
                max_frames,
            )
            .unwrap(),
        )
        .unwrap();
        until(|| node.runtime().node_durability().is_some()).await;
        node.start().unwrap();
        let source = tempfile::tempdir().unwrap();
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            ApplicationId::from_bytes([3; 16]),
            NamespaceId::from_bytes([2; 16]),
            b"requested-rotation",
        )
        .unwrap();
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("requested-rotation"),
            [3; 16],
        );
        let incarnation = IncarnationId::from_bytes([244; 16]);
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            limits,
        )
        .unwrap();
        let catalog = CellCatalog::new(layout.clone(), target.tenant());
        let code = node.application().registry().module_digests()[0];
        let proof = catalog
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
            .await
            .unwrap();
        let authority = CellAuthority::new(layout);
        let initial = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session,
                    endpoint: "https://rotation.internal:8789".into(),
                },
            )
            .await
            .unwrap();
        let handle = node
            .runtime()
            .bootstrap(
                proof,
                replica.clone(),
                authority.clone(),
                initial,
                source.path().join("cell.sqlite"),
                |transaction| {
                    transaction.execute_batch(
                        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (16)",
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        Self {
            node,
            provider,
            directories,
            source,
            target,
            replica,
            authority,
            handle,
            withdrawn,
        }
    }
    async fn command(&self, id: u8, expected: u8) -> StoredOutcome {
        let now = clock();
        let result = self
            .handle
            .execute(
                MutationIdentity {
                    request_id: RequestId::from_bytes([id; 16]),
                    issued_at_ms: now,
                    expires_at_ms: now + 60_000,
                },
                Digest::from_bytes([id; 32]),
                now,
                64,
                64,
                move |transaction| {
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(vec![expected]))
                },
            )
            .await
            .unwrap();
        assert!(matches!(&result, StoredOutcome::Success { result, .. } if result == &[expected]));
        result
    }
    async fn readback(&self, expected: u8) {
        let control = self
            .authority
            .load(self.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let root = control.value().ltx_root().unwrap();
        let destination = self.source.path().join("restored.sqlite");
        let verified = self.replica.open_root(&root).await.unwrap();
        assert_eq!(verified.restore(&destination).await.unwrap(), root.position);
        let database = rusqlite::Connection::open(destination).unwrap();
        assert_eq!(
            database
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, u8>(0))
                .unwrap(),
            expected
        );
        assert_eq!(
            database
                .query_row(
                    "SELECT result FROM sys_requests WHERE request_id = ?1",
                    [RequestId::from_bytes([245; 16]).as_bytes().as_slice()],
                    |row| row.get::<_, Vec<u8>>(0)
                )
                .unwrap(),
            vec![17]
        );
    }
}

async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
async fn entered(signal: &tokio::sync::Semaphore) {
    tokio::time::timeout(Duration::from_secs(5), signal.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
fn error_contains(mut error: &(dyn std::error::Error + 'static), text: &str) -> bool {
    loop {
        if error.to_string().contains(text) {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requested_rotation_keeps_strict_scope_through_retries_and_continued_writes() {
    let test = Fixture::new().await;
    let first = test.command(245, 17).await;
    test.provider
        .transport
        .lose_retire
        .store(true, Ordering::Release);
    assert!(matches!(
        test.node.request_node_log_rotation(999),
        Err(Error::Control("node-log rotation request has stale scope"))
    ));
    let request = test.node.request_node_log_rotation(1).unwrap();
    until(|| request.observe().unwrap().first_failure().is_some()).await;
    until(|| test.provider.transport.requests.lock().unwrap().len() >= 4).await;
    assert!(test.provider.authority.closes.lock().unwrap().is_empty());
    assert_eq!(
        request.observe().unwrap().phase(),
        NodeLogRotationPhase::Retiring
    );
    assert!(request.observe().unwrap().completion().is_none());
    assert_eq!(
        test.node.request_node_log_rotation(1).unwrap().log_epoch(),
        1
    );
    let second = test.command(246, 18).await;
    assert_eq!(second.commit_sequence(), first.commit_sequence() + 1);
    test.provider.fail_recruit.store(true, Ordering::Release);
    test.provider
        .transport
        .lose_retire
        .store(false, Ordering::Release);
    until(|| request.observe().unwrap().completion().is_some()).await;
    let observation = request.observe().unwrap();
    assert!(error_contains(
        observation.first_failure().unwrap().as_ref(),
        "native retire response lost"
    ));
    assert!(error_contains(
        observation.latest_failure().unwrap().as_ref(),
        "replacement recruitment unavailable"
    ));
    let completion = observation.completion().unwrap();
    assert_eq!(completion.retirement().barrier().log_epoch(), 1);
    assert_eq!(completion.retirement().barrier().covered_through(), 1);
    assert_eq!(completion.replacement_epoch(), 2);
    assert!(Arc::ptr_eq(
        completion,
        test.node
            .request_node_log_rotation(1)
            .unwrap()
            .observe()
            .unwrap()
            .completion()
            .unwrap()
    ));
    assert_eq!(
        test.provider.recruited.lock().unwrap()[1].1,
        vec![NodeId::from_bytes([242; 16]), NodeId::from_bytes([243; 16])]
    );
    test.command(247, 19).await;
    test.handle.drain().await.unwrap();
    for directory in &test.directories[..2] {
        let store = FollowerStore::open(
            directory.path().to_owned(),
            ReplicaLimits::default(),
            DiskBudget::new(1 << 30),
        )
        .unwrap();
        let page = store.fleet_lanes_page(None, 128, clock()).await.unwrap();
        let old = page
            .entries()
            .iter()
            .find(|entry| entry.epoch == 1)
            .unwrap();
        assert_eq!(
            old.state,
            cellule_runtime::follower::FollowerLaneState::Retired
        );
        assert_eq!(old.retired_through, Some(1));
        let frame = test.provider.transport.frames.lock().unwrap()[0].clone();
        assert!(
            store
                .append(test.provider.session, 1, vec![frame], 0)
                .await
                .is_err()
        );
    }
    test.readback(19).await;
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_request_handle_does_not_cancel_rotation() {
    let test = Fixture::new().await;
    test.command(245, 17).await;
    test.handle.drain().await.unwrap();
    test.provider
        .transport
        .pause_retire
        .store(true, Ordering::Release);
    let request = test.node.request_node_log_rotation(1).unwrap();
    entered(&test.provider.transport.entered).await;
    drop(request);
    let recovered = test.node.node_log_rotation_request(1).unwrap().unwrap();
    assert!(recovered.observe().unwrap().completion().is_none());
    test.provider.transport.resume.add_permits(1);
    until(|| recovered.observe().unwrap().completion().is_some()).await;
    assert_eq!(test.provider.recruited.lock().unwrap().len(), 2);
    assert_eq!(*test.provider.authority.closes.lock().unwrap(), vec![1]);
    test.readback(17).await;
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_deadline_retains_accepted_recruitment_until_native_cleanup_joins() {
    let test = Fixture::new().await;
    test.command(245, 17).await;
    test.handle.drain().await.unwrap();
    test.provider.pause_recruit.store(true, Ordering::Release);
    let request = test.node.request_node_log_rotation(1).unwrap();
    entered(&test.provider.entered).await;
    assert_eq!(
        request.observe().unwrap().phase(),
        NodeLogRotationPhase::Recruiting
    );
    let result = test
        .node
        .shutdown_until(Instant::now() + Duration::from_millis(20))
        .await;
    assert!(result.is_err());
    assert_eq!(test.node.state(), NodeState::Draining);
    assert!(!test.provider.abandoned.load(Ordering::Acquire));
    assert!(!test.withdrawn.load(Ordering::Acquire));
    assert!(request.observe().unwrap().completion().is_none());
    assert_eq!(test.provider.recruited.lock().unwrap().len(), 1);
    until(|| test.node.runtime().is_shutting_down()).await;
    assert!(matches!(
        test.node.runtime().try_reserve_node_bytes(1),
        Err(Error::RuntimeClosed)
    ));
    let retained = test.node.stats().retained_bytes();
    let supervisor = test
        .node
        .fleet_durability_supervisor(clock())
        .unwrap()
        .unwrap();
    assert_eq!(supervisor.state, NodeDurabilitySupervisorState::Running);
    assert!(supervisor.cancellation_requested);
    let inventory = supervisor.rotations.unwrap();
    assert!(!inventory.stopped);
    assert_eq!(inventory.running_epoch, Some(1));
    let pending = inventory.pending.unwrap();
    assert_eq!(pending.epoch, 1);
    assert_eq!(pending.progress.phase(), NodeLogRotationPhase::Recruiting);
    assert!(Arc::ptr_eq(
        pending.progress.retirement().unwrap(),
        request.observe().unwrap().retirement().unwrap()
    ));
    assert_eq!(test.node.stats().retained_bytes(), retained);
    test.provider.resume.add_permits(1);
    test.node.shutdown().await.unwrap();
    assert!(!test.provider.abandoned.load(Ordering::Acquire));
    assert_eq!(test.node.state(), NodeState::Stopped);
    assert!(test.withdrawn.load(Ordering::Acquire));
    assert_eq!(*test.provider.authority.closes.lock().unwrap(), vec![1, 2]);
    assert_eq!(test.node.stats().retained_bytes(), 0);
    assert!(request.observe().is_err());
    assert!(test.node.node_log_rotation_request(1).unwrap().is_none());
    test.readback(17).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_host_drain_waiter_retains_native_retirement_and_original_request() {
    let test = Fixture::new().await;
    test.command(245, 17).await;
    test.handle.drain().await.unwrap();
    test.provider
        .transport
        .pause_retire
        .store(true, Ordering::Release);
    let request = test.node.request_node_log_rotation(1).unwrap();
    entered(&test.provider.transport.entered).await;
    let node = test.node.clone();
    let waiter = tokio::spawn(async move { node.shutdown().await });
    until(|| test.node.state() == NodeState::Draining).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        request.observe().unwrap().phase(),
        NodeLogRotationPhase::Retiring
    );
    assert!(!test.withdrawn.load(Ordering::Acquire));
    assert!(request.observe().unwrap().completion().is_none());
    test.provider.transport.resume.add_permits(1);
    until(|| request.observe().unwrap().phase() == NodeLogRotationPhase::Interrupted).await;
    let interrupted = test
        .node
        .node_log_rotation_request(1)
        .unwrap()
        .unwrap()
        .observe()
        .unwrap();
    assert!(interrupted.retirement().is_some());
    assert!(interrupted.completion().is_none());
    assert_eq!(test.provider.recruited.lock().unwrap().len(), 1);
    test.node.shutdown().await.unwrap();
    assert_eq!(*test.provider.authority.closes.lock().unwrap(), vec![1]);
    assert!(test.withdrawn.load(Ordering::Acquire));
    assert_eq!(test.node.stats().retained_bytes(), 0);
    test.readback(17).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_rotation_in_flight_refuses_retroactive_strict_request() {
    let test = Fixture::with_threshold(1).await;
    test.provider
        .transport
        .pause_retire
        .store(true, Ordering::Release);
    test.command(245, 17).await;
    entered(&test.provider.transport.entered).await;
    let result = test.node.request_node_log_rotation(1);
    let retained = test.node.node_log_rotation_request(1).unwrap();
    test.provider.transport.resume.add_permits(1);
    until(|| {
        test.node
            .runtime()
            .node_durability()
            .unwrap()
            .1
            .log_epoch()
            .unwrap()
            == 2
    })
    .await;
    assert!(matches!(
        result,
        Err(Error::Control(
            "node-log automatic rotation already running"
        ))
    ));
    assert!(retained.is_none());
    test.handle.drain().await.unwrap();
    test.readback(17).await;
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_history_is_bounded_and_weak_handles_do_not_hold_runtime_resources() {
    let test = Fixture::new().await;
    test.command(245, 17).await;
    test.handle.drain().await.unwrap();
    let first = test.node.request_node_log_rotation(1).unwrap();
    until(|| first.observe().unwrap().completion().is_some()).await;
    let completed_bytes = test.node.stats().retained_bytes();
    test.provider.pause_recruit.store(true, Ordering::Release);
    let second = test.node.request_node_log_rotation(2).unwrap();
    entered(&test.provider.entered).await;
    assert_eq!(
        test.node.stats().retained_bytes(),
        completed_bytes + 4 * 1024
    );
    assert!(matches!(
        test.node.request_node_log_rotation(3),
        Err(Error::Capacity("node-log rotation request already pending"))
    ));
    assert!(first.observe().unwrap().completion().is_some());
    test.provider.resume.add_permits(1);
    until(|| second.observe().unwrap().completion().is_some()).await;
    assert!(first.observe().is_err());
    assert!(test.node.node_log_rotation_request(1).unwrap().is_none());
    assert_eq!(test.node.stats().retained_bytes(), completed_bytes);
    test.node.shutdown().await.unwrap();
    assert!(second.observe().is_err());
    assert_eq!(test.node.stats().retained_bytes(), 0);
    test.readback(17).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_replacement_boot_node_or_epoch_never_builds_or_closes_foreign_scope() {
    for bad in 0..3 {
        let test = Fixture::new().await;
        test.command(245, 17).await;
        test.handle.drain().await.unwrap();
        match bad {
            0 => test.provider.wrong_boot.store(true, Ordering::Release),
            1 => test.provider.wrong_node.store(true, Ordering::Release),
            _ => test.provider.non_advancing.store(true, Ordering::Release),
        }
        let request = test.node.request_node_log_rotation(1).unwrap();
        until(|| request.observe().unwrap().completion().is_some()).await;
        let observed = request.observe().unwrap();
        assert!(error_contains(
            observed.first_failure().unwrap().as_ref(),
            "node-log replacement identity or epoch differs"
        ));
        assert_eq!(observed.completion().unwrap().replacement_epoch(), 3);
        assert_eq!(
            test.node
                .runtime()
                .node_durability()
                .unwrap()
                .1
                .identity()
                .unwrap(),
            (test.provider.session, NodeId::from_bytes([240; 16]), 3)
        );
        assert_eq!(*test.provider.authority.closes.lock().unwrap(), vec![1]);
        assert!(
            test.provider
                .transport
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(
                    |(_, request)| request.leader_session == test.provider.session
                        && request.log_epoch == 1
                )
        );
        test.readback(17).await;
        test.node.shutdown().await.unwrap();
        assert_eq!(*test.provider.authority.closes.lock().unwrap(), vec![1, 3]);
        assert_eq!(test.node.stats().retained_bytes(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_cleanup_close_replies_keep_the_same_generation_owned_across_deadlines() {
    let test = Fixture::new().await;
    test.command(245, 17).await;
    test.handle.drain().await.unwrap();
    test.provider.pause_recruit.store(true, Ordering::Release);
    test.provider
        .authority
        .lose_cleanup_reply
        .store(true, Ordering::Release);
    let request = test.node.request_node_log_rotation(1).unwrap();
    entered(&test.provider.entered).await;
    assert!(
        test.node
            .shutdown_until(Instant::now() + Duration::from_millis(20))
            .await
            .is_err()
    );
    test.provider.resume.add_permits(1);
    until(|| request.observe().unwrap().first_failure().is_some()).await;
    until(|| {
        test.provider
            .authority
            .close_attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|epoch| **epoch == 2)
            .count()
            >= 2
    })
    .await;
    let observed = request.observe().unwrap();
    assert_eq!(observed.phase(), NodeLogRotationPhase::Recruiting);
    assert!(observed.completion().is_none());
    assert!(error_contains(
        observed.first_failure().unwrap().as_ref(),
        "accepted cleanup close reply lost"
    ));
    assert!(
        test.node
            .shutdown_until(Instant::now() + Duration::from_millis(20))
            .await
            .is_err()
    );
    assert_eq!(test.node.state(), NodeState::Draining);
    assert!(!test.withdrawn.load(Ordering::Acquire));
    assert_eq!(test.provider.recruited.lock().unwrap().len(), 2);
    test.provider
        .authority
        .lose_cleanup_reply
        .store(false, Ordering::Release);
    test.node.shutdown().await.unwrap();
    assert_eq!(*test.provider.authority.closes.lock().unwrap(), vec![1, 2]);
    assert!(test.withdrawn.load(Ordering::Acquire));
    assert_eq!(test.node.stats().retained_bytes(), 0);
    assert!(error_contains(
        observed.first_failure().unwrap().as_ref(),
        "accepted cleanup close reply lost"
    ));
    test.readback(17).await;
}
