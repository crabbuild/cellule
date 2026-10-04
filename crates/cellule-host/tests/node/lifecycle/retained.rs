//! Original host drain ownership across lost management waiters.
use super::*;

struct DrainProbe {
    completed: Arc<AtomicBool>,
    abandoned: Arc<AtomicBool>,
}
impl Drop for DrainProbe {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::Acquire) {
            self.abandoned.store(true, Ordering::Release);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_facility_drain_continues_after_its_only_waiter_is_cancelled() {
    let node = Arc::new(
        CellNodeBuilder::new(application())
            .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
            .with_replica_host(ReplicaHost::default())
            .with_session(SessionId::from_bytes([35; 16]))
            .build()
            .unwrap(),
    );
    let lease_shutdown = CancellationToken::new();
    let tasks = node
        .install_task_group(CancellationToken::new(), lease_shutdown.clone())
        .unwrap();
    let withdrawn = Arc::new(AtomicBool::new(false));
    let lease_withdrawn = Arc::clone(&withdrawn);
    tasks
        .spawn_lease_maintenance(async move {
            lease_shutdown.cancelled().await;
            lease_withdrawn.store(true, Ordering::Release);
            Ok::<(), Error>(())
        })
        .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let completed = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let entered = Arc::clone(&started);
    let gate = Arc::clone(&resume);
    let finished = Arc::clone(&completed);
    let cancelled = Arc::clone(&abandoned);
    let invoked = Arc::clone(&calls);
    node.install_facility(
        CellNodeFacility::new("accepted-drain", move || {
            let entered = Arc::clone(&entered);
            let gate = Arc::clone(&gate);
            let finished = Arc::clone(&finished);
            let cancelled = Arc::clone(&cancelled);
            let invoked = Arc::clone(&invoked);
            async move {
                let _probe = DrainProbe {
                    completed: Arc::clone(&finished),
                    abandoned: cancelled,
                };
                invoked.fetch_add(1, Ordering::AcqRel);
                entered.notify_one();
                gate.acquire().await.unwrap().forget();
                finished.store(true, Ordering::Release);
                Ok(())
            }
        })
        .unwrap(),
    )
    .unwrap();
    node.install_node_lease_for_startup(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    node.start().unwrap();
    assert!(node.is_ready());

    let caller = Arc::clone(&node);
    let waiter = tokio::spawn(async move { caller.shutdown().await });
    started.notified().await;
    assert_eq!(node.state(), NodeState::Draining);
    let running = node.drain_observation().unwrap().unwrap();
    assert_eq!(running.serial, 1);
    assert_eq!(running.phase, NodeDrainPhase::Running);
    assert!(running.result.is_none());
    assert!(!withdrawn.load(Ordering::Acquire));
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let was_abandoned = abandoned.load(Ordering::Acquire);
    // The extra permit lets the old implementation retry during test cleanup.
    // The fixed implementation must finish its first callback without that retry.
    resume.add_permits(2);
    let autonomous_stop = tokio::time::timeout(Duration::from_secs(1), async {
        while node.state() != NodeState::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    node.shutdown().await.unwrap();

    assert!(
        !was_abandoned,
        "caller cancellation dropped accepted facility work"
    );
    assert!(
        autonomous_stop,
        "accepted host drain did not continue independently"
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert!(completed.load(Ordering::Acquire));
    assert!(!abandoned.load(Ordering::Acquire));
    assert!(withdrawn.load(Ordering::Acquire));
    assert_eq!(node.stats().active_cells(), 0);
    assert_eq!(node.stats().retained_bytes(), 0);
    assert_eq!(node.stats().worker_jobs(), 0);
    assert_eq!(node.stats().file_descriptors(), 0);
    let joined = node.drain_observation().unwrap().unwrap();
    assert_eq!(joined.serial, 1);
    assert_eq!(joined.phase, NodeDrainPhase::Joined);
    assert!(joined.result.unwrap().is_ok());
    assert!(joined.first_failure.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_shutdown_keeps_scale_down_queued_on_the_original_lane() {
    let node = starting_node(36);
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let started = Arc::clone(&entered);
    let resume = Arc::clone(&gate);
    let invoked = Arc::clone(&calls);
    node.install_facility(
        CellNodeFacility::new("closing-gate", move || {
            let started = Arc::clone(&started);
            let resume = Arc::clone(&resume);
            let invoked = Arc::clone(&invoked);
            async move {
                invoked.fetch_add(1, Ordering::AcqRel);
                started.notify_one();
                resume.acquire().await.unwrap().forget();
                Ok(())
            }
        })
        .unwrap(),
    )
    .unwrap();
    node.start().unwrap();
    let caller = Arc::clone(&node);
    let waiter = tokio::spawn(async move { caller.shutdown().await });
    entered.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());

    let mut scale_down =
        Box::pin(node.drain_for_scale_down(Instant::now() + Duration::from_millis(20)));
    assert!(futures_util::poll!(&mut scale_down).is_pending());
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(futures_util::poll!(&mut scale_down).is_pending());
    assert_eq!(calls.load(Ordering::Acquire), 1);
    let original = node.drain_observation().unwrap().unwrap();
    assert_eq!(original.serial, 1);
    assert_eq!(original.phase, NodeDrainPhase::Running);
    assert!(original.result.is_none());
    gate.add_permits(1);
    let settled = tokio::time::timeout(Duration::from_secs(1), scale_down)
        .await
        .unwrap()
        .unwrap();
    assert!(settled.ready_to_stop());
    assert_eq!(node.state(), NodeState::Stopped);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    let joined = node.drain_observation().unwrap().unwrap();
    assert_eq!(joined.serial, 1);
    assert_eq!(joined.phase, NodeDrainPhase::Joined);
    assert!(joined.result.unwrap().is_ok());
    assert_eq!(node.stats().active_cells(), 0);
    assert_eq!(node.stats().retained_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_deadline_retry_retains_original_failure_history_after_joined_stop() {
    let node = starting_node(37);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::clone(&gate);
    node.install_facility(
        CellNodeFacility::new("retryable-drain", move || {
            let resume = Arc::clone(&resume);
            async move {
                resume.acquire().await.unwrap().forget();
                Ok(())
            }
        })
        .unwrap(),
    )
    .unwrap();
    node.start().unwrap();
    let failed = node
        .shutdown_until(Instant::now() + Duration::from_millis(10))
        .await
        .unwrap_err();
    let first = node.drain_observation().unwrap().unwrap();
    assert_eq!(first.serial, 1);
    assert_eq!(first.phase, NodeDrainPhase::Joined);
    let original = first.result.unwrap().unwrap_err();
    assert!(Arc::ptr_eq(
        &original,
        first.first_failure.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &original,
        first.latest_failure.as_ref().unwrap()
    ));
    assert!(std::ptr::eq(
        error_source::<std::io::Error>(&failed),
        error_source::<std::io::Error>(original.as_ref()),
    ));
    assert_eq!(
        error_source::<std::io::Error>(original.as_ref()).kind(),
        std::io::ErrorKind::TimedOut,
    );
    assert_eq!(node.state(), NodeState::Draining);
    gate.add_permits(1);
    node.shutdown().await.unwrap();
    let completed = node.drain_observation().unwrap().unwrap();
    assert_eq!(completed.serial, 2);
    assert_eq!(completed.phase, NodeDrainPhase::Joined);
    assert!(completed.result.unwrap().is_ok());
    assert!(Arc::ptr_eq(
        &original,
        completed.first_failure.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &original,
        completed.latest_failure.as_ref().unwrap()
    ));
    assert_eq!(node.state(), NodeState::Stopped);
    assert_eq!(node.stats().retained_bytes(), 0);
    node.shutdown().await.unwrap();
    assert_eq!(node.drain_observation().unwrap().unwrap().serial, 2);
}

fn starting_node(session: u8) -> Arc<CellNode> {
    let node = Arc::new(
        CellNodeBuilder::new(application())
            .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
            .with_replica_host(ReplicaHost::default())
            .with_session(SessionId::from_bytes([session; 16]))
            .build()
            .unwrap(),
    );
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_node_lease_for_startup(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    assert!(node.drain_observation().unwrap().is_none());
    node
}

fn error_source<'a, E: std::error::Error + 'static>(
    error: &'a (dyn std::error::Error + 'static),
) -> &'a E {
    let mut source = error;
    loop {
        if let Some(error) = source.downcast_ref::<E>() {
            return error;
        }
        source = source.source().expect("original error must be retained");
    }
}
