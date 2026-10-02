//! Maintenance evidence from actual native follower fences and Cell publication.
use super::*;
use cellule_runtime::node::log_transport::LocalFollowerTransport;
use cellule_runtime::node::{NodeDirectory, VersionedNodeAdvertisement};
use futures_util::future::BoxFuture;

struct RetirementTransport {
    locals: Vec<(NodeId, LocalFollowerTransport)>,
    requests: Mutex<Vec<(NodeId, RetireRequest)>>,
    frames: Mutex<Vec<Bytes>>,
    lose_reply: AtomicBool,
    contradict_reply: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    directory: Option<NodeDirectory>,
}

impl RetirementTransport {
    fn local(&self, member: NodeId) -> &LocalFollowerTransport {
        &self.locals.iter().find(|(id, _)| *id == member).unwrap().1
    }
}

impl NodeLogTransport for RetirementTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.frames.lock().unwrap().extend(request.frames.clone());
        Box::pin(async move {
            if let Some(directory) = &self.directory {
                directory
                    .authorize_log_append(
                        request.leader_session,
                        member,
                        request.log_epoch,
                        request.covered_through,
                        now_ms(),
                    )
                    .await?;
            }
            self.local(member).append(member, request).await
        })
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
            if let Some(directory) = &self.directory {
                directory
                    .authorize_log_retire(
                        request.leader_session,
                        member,
                        request.log_epoch,
                        request.covered_through,
                        now_ms(),
                    )
                    .await?;
            }
            let mut receipt = self.local(member).retire(member, request).await?;
            if member == self.locals[0].0 {
                if self.lose_reply.swap(false, Ordering::AcqRel) {
                    return Err(cellule_runtime::Error::Node("lost native retirement reply"));
                }
                if self.contradict_reply.swap(false, Ordering::AcqRel) {
                    receipt.durable_through += 1;
                }
            } else if self.pause.swap(false, Ordering::AcqRel) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            Ok(receipt)
        })
    }
}

// The fixture owns the exact successful close observation. A retry reconciles
// that observation rather than treating an arbitrary absent log as its result.
struct SignedRetirementAuthority {
    directory: NodeDirectory,
    observed: tokio::sync::Mutex<VersionedNodeAdvertisement>,
    closed_barrier: Mutex<Option<NodeLogRotationBarrier>>,
    attempts: AtomicUsize,
    lose_reply: AtomicBool,
    pause_reply: AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl NodeLogAuthority for SignedRetirementAuthority {
    fn activate<'a>(&'a self, epoch: u64) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            if observed
                .advertisement()
                .log()
                .is_none_or(|log| log.epoch() != epoch)
            {
                return Err(cellule_runtime::Error::Fenced);
            }
            *observed = self.directory.activate_log(&observed, now_ms()).await?;
            Ok(())
        })
    }
    fn advance_coverage<'a>(
        &'a self,
        epoch: u64,
        through: u64,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            if observed
                .advertisement()
                .log()
                .is_none_or(|log| log.epoch() != epoch)
            {
                return Err(cellule_runtime::Error::Fenced);
            }
            *observed = self
                .directory
                .advance_log_coverage(&observed, through, now_ms())
                .await?;
            Ok(())
        })
    }
    fn close<'a>(
        &'a self,
        retirement: &'a cellule_runtime::node::log::NodeLogRetirementObservation,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        let barrier = retirement.barrier();
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::AcqRel);
            let mut observed = self.observed.lock().await;
            if let Some(closed) = self.closed_barrier.lock().unwrap().as_ref() {
                assert_eq!(closed, barrier);
                assert!(observed.advertisement().log().is_none());
                return Ok(());
            }
            *observed = self
                .directory
                .close_log(&observed, barrier, now_ms())
                .await?;
            *self.closed_barrier.lock().unwrap() = Some(barrier.clone());
            // Both faults happen after the canonical CAS, not before dispatch.
            if self.pause_reply.swap(false, Ordering::AcqRel) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            if self.lose_reply.swap(false, Ordering::AcqRel) {
                return Err(cellule_runtime::Error::Facility {
                    name: "signed-close-reply",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "canonical close accepted but reply lost",
                    )),
                });
            }
            Ok(())
        })
    }
}

struct RetirementFixture {
    fixture: Fixture,
    runtime: CellRuntime,
    handle: cellule_runtime::cell::actor::CellHandle,
    durability: Arc<NodeDurability>,
    authority: Arc<TestNodeAuthority>,
    transport: Arc<RetirementTransport>,
    directories: Vec<tempfile::TempDir>,
}

impl RetirementFixture {
    async fn new(key: &[u8], store: Store) -> Self {
        Self::new_protocol(key, store, false).await.0
    }

    async fn new_protocol(
        key: &[u8],
        store: Store,
        authenticated: bool,
    ) -> (Self, Option<Arc<SignedRetirementAuthority>>) {
        let fixture = fixture_with_limits_and_store(key, Limits::default(), store);
        let session = SessionId::from_bytes([191; 16]);
        let leader = NodeId::from_bytes([192; 16]);
        let members = [NodeId::from_bytes([193; 16]), NodeId::from_bytes([194; 16])];
        let signed_authority = if authenticated {
            let fleet = Digest::from_bytes([181; 32]);
            let image = Digest::from_bytes([182; 32]);
            let release = Digest::from_bytes([183; 32]);
            let directory = NodeDirectory::new(fixture.layout.clone(), fleet, image, release);
            let key = ed25519_dalek::SigningKey::from_bytes(&[184; 32]);
            let now = now_ms();
            let signed = |node, session| {
                cellule_runtime::node::NodeAdvertisement::sign(
                    node,
                    session,
                    "https://retirement.internal:8789".into(),
                    fleet,
                    Digest::from_bytes([185; 32]),
                    image,
                    release,
                    &key,
                    1,
                    now,
                    now + 20_000,
                    vec![Digest::from_bytes([186; 32])],
                    vec![1],
                    cellule_runtime::node::NodeFailureDomain::default(),
                    cellule_runtime::node::NodeCapacity {
                        free_memory_bytes: 1,
                        free_disk_bytes: 1,
                        follower_free_bytes: 1,
                        follower_retained_bytes: 0,
                        job_credits: 1,
                        log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
                    },
                )
                .unwrap()
            };
            let created = directory
                .create(signed(leader, session), now)
                .await
                .unwrap();
            for (index, member) in members.iter().enumerate() {
                directory
                    .create(
                        signed(*member, SessionId::from_bytes([197 + index as u8; 16])),
                        now,
                    )
                    .await
                    .unwrap();
            }
            let enrolled = directory.recruit_log(&created, 7, 1, 3, now).await.unwrap();
            assert_eq!(enrolled.advertisement().log().unwrap().members(), members);
            Some(Arc::new(SignedRetirementAuthority {
                directory,
                observed: tokio::sync::Mutex::new(enrolled),
                closed_barrier: Mutex::new(None),
                attempts: AtomicUsize::new(0),
                lose_reply: AtomicBool::new(false),
                pause_reply: AtomicBool::new(false),
                entered: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
            }))
        } else {
            None
        };
        let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
            SqlWorkerPool::new(1, 1).unwrap(),
            2 * 1024 * 1024,
            session,
            ReplicaHost::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
        )
        .unwrap();
        let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
        runtime.install_node_lease(lease.clone()).unwrap();
        let directories: Vec<_> = members
            .iter()
            .map(|_| tempfile::tempdir().unwrap())
            .collect();
        let locals = members
            .iter()
            .zip(&directories)
            .map(|(member, directory)| {
                let store = cellule_runtime::FollowerStore::open(
                    directory.path().to_owned(),
                    Limits::default(),
                    DiskBudget::new(1 << 30),
                )
                .unwrap();
                (*member, LocalFollowerTransport::new(*member, store))
            })
            .collect();
        let transport = Arc::new(RetirementTransport {
            locals,
            requests: Mutex::new(Vec::new()),
            frames: Mutex::new(Vec::new()),
            lose_reply: AtomicBool::new(false),
            contradict_reply: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            directory: signed_authority
                .as_ref()
                .map(|authority| authority.directory.clone()),
        });
        let gate = DurabilityGate::new(session, leader, 7, members).unwrap();
        let node_transport: Arc<dyn NodeLogTransport> = transport.clone();
        let shipper =
            NodeLogShipper::new(gate.clone(), node_transport.clone(), Limits::default()).unwrap();
        let authority = Arc::new(TestNodeAuthority::default());
        let log_authority: Arc<dyn NodeLogAuthority> = match &signed_authority {
            Some(authority) => authority.clone(),
            None => authority.clone(),
        };
        let durability = Arc::new(NodeDurability::new(
            gate,
            shipper,
            log_authority,
            node_transport,
            lease,
        ));
        runtime
            .install_node_durability(fixture.target.application(), durability.clone())
            .unwrap();
        let handle = bootstrap_on(&runtime, &fixture, session).await;
        (
            Self {
                fixture,
                runtime,
                handle,
                durability,
                authority,
                transport,
                directories,
            },
            signed_authority,
        )
    }

    async fn command(&self) {
        let outcome = self
            .handle
            .execute(
                mutation_identity_window(195, 10, 10_000),
                Digest::from_bytes([196; 32]),
                20,
                1_024,
                1_024,
                |transaction| {
                    transaction.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(b"durable maintenance".to_vec()))
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome, StoredOutcome::Success { result, commit_sequence: 1 } if result == b"durable maintenance")
        );
    }

    async fn assert_native_fences(&self) {
        let frame = self.transport.frames.lock().unwrap()[0].clone();
        for directory in &self.directories {
            // Reopening independently proves the append fence survived fsync.
            let store = cellule_runtime::FollowerStore::open(
                directory.path().to_owned(),
                Limits::default(),
                DiskBudget::new(1 << 30),
            )
            .unwrap();
            let page = store.fleet_lanes_page(None, 128, now_ms()).await.unwrap();
            assert_eq!(page.entries().len(), 1);
            assert_eq!(
                page.entries()[0].state,
                cellule_runtime::follower::FollowerLaneState::Retired
            );
            assert_eq!(page.entries()[0].retired_through, Some(1));
            assert!(
                store
                    .append(SessionId::from_bytes([191; 16]), 7, vec![frame.clone()], 0)
                    .await
                    .is_err()
            );
        }
        let control = CellAuthority::new(self.fixture.layout.clone())
            .load(self.fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let root = control.value().ltx_root().unwrap();
        assert_eq!(root.commit_sequence, 1);
        let restored = self
            .fixture
            ._directory
            .path()
            .join("maintenance-restored.sqlite");
        let verified = self.fixture.replica.open_root(&root).await.unwrap();
        assert_eq!(verified.restore(&restored).await.unwrap(), root.position);
        let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let request = mutation_identity_window(195, 10, 10_000);
        assert_eq!(
            connection
                .query_row(
                    "SELECT result FROM sys_requests WHERE request_id = ?1",
                    [request.request_id.as_bytes().as_slice()],
                    |row| row.get::<_, Vec<u8>>(0)
                )
                .unwrap(),
            b"durable maintenance"
        );
    }
}

fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_joins_every_member_and_retries_original_scope_after_lost_reply() {
    let test = RetirementFixture::new(b"maintenance-retire-lost", store()).await;
    test.command().await;
    test.handle.drain().await.unwrap();
    test.transport.lose_reply.store(true, Ordering::Release);
    test.transport.pause.store(true, Ordering::Release);
    let durability = test.durability.clone();
    let task = tokio::spawn(async move { durability.shutdown_for_maintenance().await });
    test.transport.entered.acquire().await.unwrap().forget();
    let pending = !task.is_finished();
    let closed = test.authority.closes.lock().unwrap().clone();
    test.transport.release.add_permits(1);
    let error = task.await.unwrap().unwrap_err();
    assert!(pending);
    assert!(closed.is_empty());
    assert!(std::error::Error::source(&error).is_some());
    let observation = test.durability.retirement_observation().unwrap().unwrap();
    assert_eq!(observation.members().len(), 2);
    assert!(
        matches!(observation.members()[0].result(), Err(error) if matches!(error.as_ref(), cellule_runtime::Error::Node("lost native retirement reply")))
    );
    assert!(observation.members()[1].result().is_ok());
    assert!(observation.confirmed().is_err());
    assert!(test.authority.closes.lock().unwrap().is_empty());
    let proof = test.durability.shutdown_for_maintenance().await.unwrap();
    assert_eq!(proof.barrier(), observation.barrier());
    assert_eq!(proof.barrier().log_epoch(), 7);
    assert_eq!(proof.barrier().covered_through(), 1);
    assert_eq!(proof.barrier().members().len(), 2);
    assert!(Arc::ptr_eq(
        &proof,
        &test.durability.shutdown_for_maintenance().await.unwrap()
    ));
    assert_eq!(*test.authority.closes.lock().unwrap(), vec![7]);
    let requests = test.transport.requests.lock().unwrap().clone();
    assert_eq!(&requests[..2], &requests[2..]);
    test.assert_native_fences().await;
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_best_effort_close_cannot_be_upgraded_to_maintenance_proof() {
    let test = RetirementFixture::new(b"maintenance-retire-ordinary", store()).await;
    test.command().await;
    test.handle.drain().await.unwrap();
    test.transport.lose_reply.store(true, Ordering::Release);
    test.durability.shutdown().await.unwrap();
    assert_eq!(*test.authority.closes.lock().unwrap(), vec![7]);
    assert!(
        test.durability
            .retirement_observation()
            .unwrap()
            .unwrap()
            .confirmed()
            .is_err()
    );
    assert!(matches!(
        test.durability.shutdown_for_maintenance().await,
        Err(cellule_runtime::Error::Node(
            "node-log member retirement remains unconfirmed"
        ))
    ));
    assert_eq!(test.transport.requests.lock().unwrap().len(), 2);
    test.assert_native_fences().await;
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn contradictory_success_blocks_both_close_paths() {
    let test = RetirementFixture::new(b"maintenance-retire-contradict", store()).await;
    test.command().await;
    test.handle.drain().await.unwrap();
    for strict in [false, true] {
        test.transport
            .contradict_reply
            .store(true, Ordering::Release);
        let result = if strict {
            test.durability.shutdown_for_maintenance().await.map(|_| ())
        } else {
            test.durability.shutdown().await
        };
        assert!(matches!(
            result,
            Err(cellule_runtime::Error::Node(
                "follower retire receipt differs"
            ))
        ));
        assert!(test.authority.closes.lock().unwrap().is_empty());
    }
    test.durability.shutdown_for_maintenance().await.unwrap();
    test.assert_native_fences().await;
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_maintenance_waiter_leaves_no_proof_or_authority_close() {
    let test = RetirementFixture::new(b"maintenance-retire-cancel", store()).await;
    test.command().await;
    test.handle.drain().await.unwrap();
    test.transport.pause.store(true, Ordering::Release);
    let durability = test.durability.clone();
    let task = tokio::spawn(async move { durability.shutdown_for_maintenance().await });
    test.transport.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(test.authority.closes.lock().unwrap().is_empty());
    assert!(test.durability.retirement_observation().unwrap().is_none());
    test.durability.shutdown_for_maintenance().await.unwrap();
    test.assert_native_fences().await;
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn uncovered_cell_publication_blocks_member_retirement() {
    let pausing = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let test =
        RetirementFixture::new(b"maintenance-retire-coverage", Store::new(pausing.clone())).await;
    pausing.arm_next_update();
    test.command().await;
    pausing.wait_until_blocked().await;
    let result = test.durability.shutdown_for_maintenance().await;
    let requests = test.transport.requests.lock().unwrap().len();
    let closes = test.authority.closes.lock().unwrap().len();
    pausing.release();
    test.handle.drain().await.unwrap();
    assert!(matches!(
        result,
        Err(cellule_runtime::Error::PendingPublication)
    ));
    assert_eq!(requests, 0);
    assert_eq!(closes, 0);
    test.durability.shutdown_for_maintenance().await.unwrap();
    test.assert_native_fences().await;
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn confirmed_fences_survive_lost_signed_authority_close_reply() {
    let (test, authority) =
        RetirementFixture::new_protocol(b"signed-close-lost", store(), true).await;
    let authority = authority.unwrap();
    test.command().await;
    test.handle.drain().await.unwrap();
    authority.lose_reply.store(true, Ordering::Release);
    let error = test
        .durability
        .shutdown_for_maintenance()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        cellule_runtime::Error::Facility {
            name: "signed-close-reply",
            ..
        }
    ));
    assert!(std::error::Error::source(&error).is_some());
    let observation = test.durability.retirement_observation().unwrap().unwrap();
    let confirmed = observation.confirmed().unwrap();
    assert_eq!(test.transport.requests.lock().unwrap().len(), 2);
    // A fresh request now fails the real signed-directory authorization path.
    assert!(
        authority
            .directory
            .authorize_log_retire(
                SessionId::from_bytes([191; 16]),
                NodeId::from_bytes([193; 16]),
                7,
                1,
                now_ms()
            )
            .await
            .is_err()
    );
    let proof = test.durability.shutdown_for_maintenance().await.unwrap();
    assert_eq!(proof.as_ref(), &confirmed);
    assert!(Arc::ptr_eq(
        &observation,
        &test.durability.retirement_observation().unwrap().unwrap()
    ));
    assert_eq!(test.transport.requests.lock().unwrap().len(), 2);
    assert_eq!(authority.attempts.load(Ordering::Acquire), 2);
    test.durability.shutdown().await.unwrap();
    assert!(Arc::ptr_eq(
        &proof,
        &test.durability.shutdown_for_maintenance().await.unwrap()
    ));
    assert_eq!(authority.attempts.load(Ordering::Acquire), 2);
    test.assert_native_fences().await;
    test.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_signed_close_waiter_reuses_confirmed_fences_for_both_paths() {
    for strict_retry in [false, true] {
        let (test, authority) = RetirementFixture::new_protocol(
            if strict_retry {
                b"signed-close-cancel-strict"
            } else {
                b"signed-close-cancel-ordinary"
            },
            store(),
            true,
        )
        .await;
        let authority = authority.unwrap();
        test.command().await;
        test.handle.drain().await.unwrap();
        authority.pause_reply.store(true, Ordering::Release);
        let durability = test.durability.clone();
        let task = tokio::spawn(async move { durability.shutdown_for_maintenance().await });
        authority.entered.acquire().await.unwrap().forget();
        let original = test.durability.retirement_observation().unwrap().unwrap();
        assert!(original.confirmed().is_ok());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            authority
                .directory
                .authorize_log_retire(
                    SessionId::from_bytes([191; 16]),
                    NodeId::from_bytes([194; 16]),
                    7,
                    1,
                    now_ms()
                )
                .await
                .is_err()
        );
        if strict_retry {
            test.durability.shutdown_for_maintenance().await.unwrap();
        } else {
            test.durability.shutdown().await.unwrap();
        }
        let proof = test.durability.shutdown_for_maintenance().await.unwrap();
        assert_eq!(proof.as_ref(), &original.confirmed().unwrap());
        assert!(Arc::ptr_eq(
            &original,
            &test.durability.retirement_observation().unwrap().unwrap()
        ));
        assert_eq!(test.transport.requests.lock().unwrap().len(), 2);
        assert_eq!(authority.attempts.load(Ordering::Acquire), 2);
        test.assert_native_fences().await;
        test.runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_enrollment_and_refusal_replies_reconcile_only_the_original_retained_attempt() {
    for refusing in [false, true] {
        let pausing = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
        let (test, authority) = RetirementFixture::new_protocol(
            if refusing {
                b"enrollment-refusal-lost"
            } else {
                b"enrollment-commit-lost"
            },
            Store::new(pausing.clone()),
            true,
        )
        .await;
        let authority = authority.unwrap();
        test.command().await;
        test.handle.drain().await.unwrap();
        test.durability.shutdown_for_maintenance().await.unwrap();
        let source = authority.observed.lock().await.clone();
        let directory = authority.directory.clone();
        let prepared = directory
            .prepare_log_enrollment(&source, 8, 1, 3, now_ms())
            .await
            .unwrap()
            .unwrap();
        let attempt = directory
            .prepare_log_enrollment_attempt(&prepared, now_ms())
            .await
            .unwrap();
        pausing.lost_update_response.store(true, Ordering::Release);
        pausing.arm_gets();
        let job = {
            let directory = directory.clone();
            let attempt = attempt.clone();
            tokio::spawn(async move {
                if refusing {
                    directory
                        .fence_log_enrollment(&attempt, now_ms())
                        .await
                        .map(|_| ())
                } else {
                    directory
                        .commit_log_enrollment(&attempt, now_ms())
                        .await
                        .map(|_| ())
                }
            })
        };
        // The backend accepted the CAS, lost its reply and is now paused in the
        // canonical reconciliation read. No new selection occurs after abort.
        pausing.wait_until_get_blocked().await;
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        pausing.release_gets();
        if refusing {
            assert!(
                directory
                    .commit_log_enrollment(&attempt, now_ms())
                    .await
                    .is_err()
            );
            let proof = directory
                .fence_log_enrollment(&attempt, now_ms())
                .await
                .unwrap();
            assert_eq!(proof.prepared().followers(), prepared.followers());
            assert!(proof.refusal().advertisement().log().is_none());
            assert!(
                directory
                    .inspect_log_enrollment(&attempt, now_ms())
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert!(
                directory
                    .fence_log_enrollment(&attempt, now_ms())
                    .await
                    .is_err()
            );
            let proof = directory
                .inspect_log_enrollment(&attempt, now_ms())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(proof.prepared().followers(), prepared.followers());
            assert_eq!(
                proof.enrollment().advertisement().log(),
                Some(prepared.log())
            );
            let gate = DurabilityGate::new(
                source.advertisement().session(),
                source.advertisement().node(),
                8,
                prepared.log().members().iter().copied(),
            )
            .unwrap();
            directory
                .close_log(
                    proof.enrollment(),
                    &gate.begin_rotation().unwrap(),
                    now_ms(),
                )
                .await
                .unwrap();
            assert!(
                directory
                    .inspect_log_enrollment(&attempt, now_ms())
                    .await
                    .unwrap()
                    .is_none()
            );
            // Enrollment followed by closure is not a refusal of that enrollment.
            assert!(
                directory
                    .fence_log_enrollment(&attempt, now_ms())
                    .await
                    .is_err()
            );
        }
        test.assert_native_fences().await;
        test.runtime.shutdown().await.unwrap();
    }
}
