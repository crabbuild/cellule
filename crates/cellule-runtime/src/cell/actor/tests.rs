use super::{CellRuntimeStats, bounded_u32};

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
            reply,
            shutdown_drain: false,
            result,
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
