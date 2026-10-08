use super::{CellRuntimeStats, bounded_u32};

#[tokio::test(flavor = "multi_thread")]
async fn pressured_fleet_roots_batch_without_hiding_reads_or_delaying_drain() {
    use super::*;
    use crate::cell::catalog::CellCatalog;
    use crate::cell::executor::{HandlerOutcome, MutationIdentity};
    use crate::control::Owner;
    use crate::identity::{IncarnationId, NamespaceId, NodeId, RequestId, TenantId};
    use crate::node::log::DurabilityGate;
    use crate::node::log_shipper::NodeLogShipper;
    use crate::node::log_transport::LocalFollowerTransport;

    let session = SessionId::from_bytes([87; 16]);
    let incarnation = IncarnationId::from_bytes([88; 16]);
    let member = NodeId::from_bytes([89; 16]);
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        NamespaceId::from_bytes([6; 16]),
        b"pressured-fleet-roots",
    )
    .unwrap();
    let layout = cellule_ltx::CellStorageLayout::new(
        cellule_store::Store::new(Arc::new(object_store::memory::InMemory::new())),
        object_store::path::Path::from("pressured-roots"),
        *target.application().as_bytes(),
    );
    let limits = cellule_ltx::Limits::default();
    let replica = cellule_ltx::CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        limits,
    )
    .unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let follower = crate::FollowerStore::open(
        directory.path().join("follower"),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport = Arc::new(LocalFollowerTransport::new(member, follower));
    let gate = DurabilityGate::new(session, NodeId::from_bytes([90; 16]), 1, [member]).unwrap();
    let shipper = NodeLogShipper::new(gate.clone(), transport.clone(), limits).unwrap();
    let node_lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        8 << 20,
        session,
        cellule_ltx::Host::default(),
    )
    .unwrap();
    runtime.install_node_lease(node_lease.clone()).unwrap();
    runtime
        .install_node_durability(
            target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(BatchNodeAuthority),
                transport,
                node_lease,
            )),
        )
        .unwrap();
    let catalog = CellCatalog::new(layout.clone(), target.tenant());
    let code = Digest::from_bytes([91; 32]);
    let proof = catalog
        .provision(CatalogEntry::new(&target, CatalogRole::Application, code, 1).unwrap())
        .await
        .unwrap();
    let authority = CellAuthority::new(layout);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://node.internal".into(),
            },
        )
        .await
        .unwrap();
    let handle = runtime
        .bootstrap(
            proof,
            replica.clone(),
            authority.clone(),
            observed,
            directory.path().join("cell.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (0)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let identity = |id| MutationIdentity {
        request_id: RequestId::from_bytes([id; 16]),
        issued_at_ms: 10,
        expires_at_ms: 60_000,
    };
    let mut publications = runtime.subscribe_publications();
    let mut warmup_slots = Vec::new();
    for _ in 0..cellule_ltx::SHARED_PUBLICATION_ROWS {
        warmup_slots.push(runtime.inner.shared_publication.admit().await.unwrap());
    }
    let first = handle
        .execute(identity(1), code, 20, 1_024, 1_024, |tx| {
            tx.execute("UPDATE counter SET value = value + 1", [])?;
            Ok(HandlerOutcome::Success(vec![1]))
        })
        .await
        .unwrap();
    assert_eq!(first.commit_sequence(), 1);
    assert!(
        runtime
            .node_durability()
            .unwrap()
            .1
            .progress()
            .unwrap()
            .fleet_active
    );
    drop(warmup_slots);
    tokio::time::timeout(std::time::Duration::from_secs(5), publications.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let control = authority.load(target.cell_id()).await.unwrap().unwrap();
            if control.value().ltx_root().unwrap().commit_sequence == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    // Model the bounded node lane's complete occupancy without retaining any
    // synthetic captures. These are its real original preparation permits.
    let mut occupied = Vec::new();
    for _ in 0..cellule_ltx::SHARED_PUBLICATION_ROWS {
        occupied.push(runtime.inner.shared_publication.admit().await.unwrap());
    }
    let mut acknowledged = Vec::new();
    for id in 2_u8..=12 {
        let outcome = handle
            .execute(identity(id), code, 20, 1_024, 1_024, move |tx| {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(vec![id]))
            })
            .await
            .unwrap();
        acknowledged.push(outcome);
    }
    drop(occupied);
    // Once pressure clears, an already-proven cohort still owns its original
    // bounded age. It does not immediately turn into another tiny root.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let current = authority.load(target.cell_id()).await.unwrap().unwrap();
    assert_eq!(current.value().ltx_root().unwrap().commit_sequence, 1);
    assert_eq!(
        handle
            .query(1_024, 1_024, |db| {
                let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_le_bytes().to_vec())
            })
            .await
            .unwrap(),
        12_i64.to_le_bytes()
    );
    for (id, expected) in (2_u8..=12).zip(acknowledged) {
        assert_eq!(
            handle
                .execute(identity(id), code, 20, 1_024, 1_024, |_| {
                    panic!("exact retry must not execute SQL")
                })
                .await
                .unwrap(),
            expected
        );
    }
    // Drain must wake the original delayed task rather than wait its five-
    // second age. It still selects and releases the complete captured prefix.
    tokio::time::timeout(std::time::Duration::from_secs(2), handle.drain())
        .await
        .unwrap()
        .unwrap();
    runtime.shutdown().await.unwrap();
    let root = authority
        .load(target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert_eq!(root.commit_sequence, 12);
    let restored = directory.path().join("cold.sqlite");
    replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let db = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        12
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM sys_requests", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        12
    );
    for id in 1_u8..=12 {
        let actual = db
            .query_row(
                "SELECT operation_digest, result, commit_sequence FROM sys_requests WHERE request_id = ?1",
                [RequestId::from_bytes([id; 16]).as_bytes().as_slice()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, i64>(2)?)),
            )
            .unwrap();
        assert_eq!(actual, (code.as_bytes().to_vec(), vec![id], i64::from(id)));
    }
    let stats = runtime.stats();
    assert_eq!(stats.active_cells(), 0);
    assert_eq!(stats.retained_bytes(), 0);
}

struct BatchNodeAuthority;

impl crate::node::durability::NodeLogAuthority for BatchNodeAuthority {
    fn activate<'a>(
        &'a self,
        _epoch: u64,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn advance_coverage<'a>(
        &'a self,
        _epoch: u64,
        _through: u64,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(
        &'a self,
        _retirement: &'a crate::node::log::NodeLogRetirementObservation,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn empty_runtime_miss_does_not_wait_for_the_dispatcher() {
    use super::*;
    use futures_util::FutureExt;

    let pool = SqlWorkerPool::new(1, 1).unwrap();
    let runtime = CellRuntime::new(pool, 1 << 20, SessionId::from_bytes([62; 16])).unwrap();
    // On this current-thread executor the actor has not been polled. An empty
    // runtime must forward without first waking that actor for a negative lookup.
    assert!(matches!(
        runtime
            .has_local_owner(CellId::from_bytes([62; 32]))
            .now_or_never(),
        Some(Ok(false))
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_runtime_miss_preserves_lease_and_closed_dispatcher_errors() {
    use super::*;

    let fenced = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        1 << 20,
        SessionId::from_bytes([64; 16]),
        cellule_ltx::Host::default(),
    )
    .unwrap();
    let cell = CellId::from_bytes([64; 32]);
    assert!(matches!(
        fenced.has_local_owner(cell).await,
        Err(Error::Fenced)
    ));
    fenced.shutdown().await.unwrap();

    let mut runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 1).unwrap(),
        1 << 20,
        SessionId::from_bytes([65; 16]),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);
    let original = std::mem::replace(
        &mut Arc::get_mut(&mut runtime.inner).unwrap().sender,
        sender,
    );
    assert!(matches!(
        runtime.has_local_owner(cell).await,
        Err(Error::RuntimeClosed)
    ));
    Arc::get_mut(&mut runtime.inner).unwrap().sender = original;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn activation_reservation_prevents_the_empty_runtime_shortcut() {
    use super::*;
    use futures_util::FutureExt;

    let pool = SqlWorkerPool::new(1, 1).unwrap();
    let mut runtime =
        CellRuntime::new(pool.clone(), 1 << 20, SessionId::from_bytes([63; 16])).unwrap();
    let cell = CellId::from_bytes([63; 32]);
    let (sender, mut receiver) = mpsc::channel(1);
    let original = std::mem::replace(
        &mut Arc::get_mut(&mut runtime.inner).unwrap().sender,
        sender,
    );
    let reservation = pool.reserve_activation().unwrap();
    {
        let lookup = runtime.has_local_owner(cell);
        tokio::pin!(lookup);
        assert!(lookup.as_mut().now_or_never().is_none());
        match receiver.try_recv().unwrap() {
            Message::Lookup {
                cell: requested,
                require_resident,
                reply,
            } => {
                assert_eq!(requested, cell);
                assert!(!require_resident);
                assert!(reply.send(None).is_ok());
            }
            _ => panic!("activation in progress must consult actor admission"),
        }
        assert!(!lookup.await.unwrap());
    }
    drop(reservation);
    assert!(matches!(
        runtime.has_local_owner(cell).now_or_never(),
        Some(Ok(false))
    ));
    Arc::get_mut(&mut runtime.inner).unwrap().sender = original;
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        runtime.has_local_owner(cell).await,
        Err(Error::RuntimeClosed)
    ));
}

#[tokio::test]
async fn shutdown_reports_deactivation_failure_completed_before_it_started() {
    assert!(matches!(
        shutdown_after_release(Err(super::Error::Fenced), None).await,
        Err(super::Error::Fenced)
    ));
}

#[tokio::test]
async fn shutdown_preserves_release_failure_when_its_caller_was_cancelled() {
    let source = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "release transport failed");
    let (reply, response) = tokio::sync::oneshot::channel();
    drop(response);
    let error = shutdown_after_release(Err(super::Error::FollowerIo(source)), Some(reply))
        .await
        .unwrap_err();
    match error {
        super::Error::FollowerIo(source) => {
            assert_eq!(source.kind(), std::io::ErrorKind::BrokenPipe);
            assert_eq!(source.to_string(), "release transport failed");
        }
        other => panic!("original release source was lost: {other:?}"),
    }
}

#[tokio::test]
async fn shutdown_does_not_repeat_a_release_failure_observed_by_its_caller() {
    let (reply, response) = tokio::sync::oneshot::channel();
    shutdown_after_release(Err(super::Error::Fenced), Some(reply))
        .await
        .unwrap();
    assert!(matches!(response.await.unwrap(), Err(super::Error::Fenced)));
}

async fn shutdown_after_release(
    result: crate::Result<()>,
    reply: Option<tokio::sync::oneshot::Sender<crate::Result<()>>>,
) -> crate::Result<()> {
    use super::*;

    let pool = SqlWorkerPool::new(1, 1).unwrap();
    let mut cells = HashMap::new();
    let cell = CellId::from_bytes([61; 32]);
    let mut transitioning = HashSet::from([cell]);
    let mut tasks = JoinSet::new();
    let mut shutdown = ShutdownState::default();
    let node_lease = RuntimeNodeLease::ObjectOnly;
    let unpublished = AtomicU64::new(0);
    let (publications, _) = broadcast::channel(1);
    let mut movement = MovementBudget::with_requested_limit(2, 32, 1_000).unwrap();
    let mut permits = HashMap::new();
    super::tasks::handle_task(
        TaskResult::Deactivated {
            cell,
            generation: 1,
            reply: reply.map(DrainReply::Unit),
            shutdown_drain: false,
            result,
            released: None,
        },
        &pool,
        &mut cells,
        &mut transitioning,
        &mut tasks,
        &mut shutdown,
        &node_lease,
        &unpublished,
        &publications,
        &mut movement,
        &mut permits,
    );
    assert!(transitioning.is_empty());
    let (_sender, mut receiver) = mpsc::channel(1);
    let (reply, response) = oneshot::channel();
    let mut pressure = PressureClassifier::new(800, 600, 1_000).unwrap();
    let mut generation = 1;
    handle_message(
        Message::Shutdown { reply },
        &mut receiver,
        &pool,
        &mut cells,
        &mut transitioning,
        &mut tasks,
        &mut shutdown,
        &node_lease,
        &crate::fleet::telemetry::CellTelemetryHandle::default(),
        &mut pressure,
        &NodeAdmission::default(),
        &mut movement,
        &mut permits,
        &mut generation,
    );
    start_shutdown_drain(
        &pool,
        &mut cells,
        &mut transitioning,
        &mut tasks,
        &mut shutdown,
        &node_lease,
    );
    super::admission::finish_shutdown(&mut shutdown);
    let result = response.await.unwrap();
    pool.shutdown().await.unwrap();
    result
}

#[test]
fn placement_projection_saturates_large_node_counters() {
    let stats = CellRuntimeStats {
        active_cells: usize::MAX,
        active_cell_capacity: usize::MAX,
        resident_bytes: 0,
        resident_capacity_bytes: 0,
        file_descriptors: 0,
        file_descriptor_capacity: 0,
        retained_bytes: 0,
        retained_capacity_bytes: 0,
        worker_jobs: usize::MAX,
        worker_job_capacity: usize::MAX,
        primitive_jobs: usize::MAX,
        primitive_job_capacity: usize::MAX,
        hydration_jobs: usize::MAX,
        hydration_job_capacity: usize::MAX,
        io_slots: 0,
        io_slot_capacity: 0,
        blocking_jobs: 0,
        blocking_job_capacity: 0,
        recovery_jobs: 0,
        recovery_job_capacity: 0,
        dirty_jobs: 0,
        dirty_job_capacity: 0,
        scratch_units: 0,
        scratch_unit_capacity: 0,
        local_disk_reserved_bytes: 0,
        local_disk_capacity_bytes: 0,
        unpublished_node_log_bytes: 0,
    };

    assert_eq!(bounded_u32(usize::MAX), u32::MAX);
    assert_eq!(stats.placement_active_cells(), u32::MAX);
    assert_eq!(stats.placement_active_cell_capacity(), u32::MAX);
    assert_eq!(stats.placement_running_jobs(), u32::MAX);
    assert_eq!(stats.placement_job_capacity(), u32::MAX);
}

#[tokio::test]
async fn publication_pressure_preserves_fenced_and_draining_admission_errors() {
    use super::*;
    use crate::identity::{ApplicationId, IncarnationId, NamespaceId, TenantId};

    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 1).unwrap(),
        1 << 20,
        SessionId::from_bytes([66; 16]),
    )
    .unwrap();
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        NamespaceId::from_bytes([6; 16]),
        b"admission-errors",
    )
    .unwrap();
    let incarnation = IncarnationId::from_bytes([2; 16]);
    let code = Digest::from_bytes([5; 32]);
    let handle = CellHandle {
        cell: target.cell_id(),
        incarnation,
        code,
        schema: 1,
        catalog: CatalogProof::local(
            CatalogEntry::new(&target, CatalogRole::Application, code, 1).unwrap(),
            &target,
        ),
        inner: runtime.inner.clone(),
        admission: admission::new_cell_admission(crate::control::OwnerFence {
            incarnation,
            epoch: 1,
        }),
    };
    let retained = runtime
        .inner
        .resources
        .try_reserve(ResourceCost::zero().with_retained_bytes(3 * (1 << 20) / 4))
        .unwrap();
    runtime
        .inner
        .unpublished_node_log_bytes
        .store(1, Ordering::Release);
    assert!(matches!(
        handle.reserve_work(1, 1),
        Err(Error::Capacity("publication backlog"))
    ));
    handle.admission.fenced.store(true, Ordering::Release);
    assert!(matches!(handle.reserve_work(1, 1), Err(Error::Fenced)));
    handle.admission.fenced.store(false, Ordering::Release);
    handle.admission.draining.store(true, Ordering::Release);
    assert!(matches!(
        handle.reserve_work(1, 1),
        Err(Error::CellDraining)
    ));
    runtime
        .inner
        .unpublished_node_log_bytes
        .store(0, Ordering::Release);
    drop(retained);
    drop(handle);
    runtime.shutdown().await.unwrap();
}
