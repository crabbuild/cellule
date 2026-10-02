//! Public supervision captures use the original native owner and request bank.
use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct IdleProvider {
    calls: AtomicUsize,
}
impl NodeDurabilityProvider for IdleProvider {
    fn recruit(
        self: Arc<Self>,
        _limits: ReplicaLimits,
        _bytes: u64,
        _live: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(None) })
    }
}

fn configuration() -> NodeDurabilitySupervisorConfig {
    NodeDurabilitySupervisorConfig::new(
        ApplicationId::from_bytes([3; 16]),
        ReplicaLimits::default(),
        1,
        2,
        Duration::from_millis(10),
        Duration::from_secs(3600),
        u64::MAX,
    )
    .unwrap()
}

#[tokio::test]
async fn metadata_admission_precedes_provider_start_and_releases_after_join() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 4).unwrap(), 16 << 10)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([91; 16]))
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let provider = Arc::new(IdleProvider::default());
    let budget = node
        .runtime()
        .try_reserve_node_metadata_bytes(node.stats().retained_capacity_bytes())
        .unwrap();
    assert!(matches!(
        node.install_node_durability_provider(provider.clone(), configuration()),
        Err(Error::Capacity(_))
    ));
    assert_eq!(provider.calls.load(Ordering::Relaxed), 0);
    assert!(node.fleet_durability_supervisor(clock()).unwrap().is_none());
    assert!(
        node.owned_component::<IdleProvider>(NODE_DURABILITY_PROVIDER_COMPONENT)
            .is_none()
    );
    drop(budget);
    assert!(matches!(
        node.runtime().try_reserve_node_bytes(1),
        Err(Error::Fenced)
    ));
    node.install_node_durability_provider(provider.clone(), configuration())
        .unwrap();
    until(|| provider.calls.load(Ordering::Relaxed) > 0).await;
    assert_eq!(node.stats().retained_bytes(), 4 * 1024);
    let before = node.stats().retained_bytes();
    for _ in 0..20 {
        let observed = node.fleet_durability_supervisor(clock()).unwrap().unwrap();
        assert_eq!(observed.application, ApplicationId::from_bytes([3; 16]));
        assert_eq!(observed.session, SessionId::from_bytes([91; 16]));
        assert_eq!(observed.state, NodeDurabilitySupervisorState::Running);
        let inventory = observed.rotations.unwrap();
        assert!(!inventory.stopped);
        assert!(inventory.pending.is_none() && inventory.completed.is_none());
    }
    assert_eq!(node.stats().retained_bytes(), before);
    assert!(matches!(
        node.runtime().try_reserve_node_bytes(1),
        Err(Error::Fenced)
    ));
    node.shutdown().await.unwrap();
    assert_eq!(node.stats().retained_bytes(), 0);
    assert!(node.fleet_durability_supervisor(clock()).unwrap().is_none());
}

#[tokio::test]
async fn unbound_invalid_and_wrong_typed_supervisor_capture_are_distinct() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 << 20)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([92; 16]))
        .build()
        .unwrap();
    assert!(node.fleet_durability_supervisor(clock()).unwrap().is_none());
    assert!(matches!(
        node.fleet_durability_supervisor(-1),
        Err(Error::Node(_))
    ));
    node.install_owned_component_with_drain(
        "node-durability-supervisor",
        Arc::new(7_u8),
        || async { Ok(()) },
    )
    .unwrap();
    assert!(matches!(
        node.fleet_durability_supervisor(clock()),
        Err(Error::Control(_))
    ));
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_member_and_original_completed_rotation_remain_visible_without_new_admission() {
    let test = Fixture::new().await;
    test.command(245, 17).await;
    test.provider
        .transport
        .lose_retire
        .store(true, Ordering::Release);
    let request = test.node.request_node_log_rotation(1).unwrap();
    until(|| request.observe().unwrap().first_failure().is_some()).await;
    let original = request.observe().unwrap().first_failure().unwrap().clone();
    let stats = test.node.stats();
    let budget = test
        .node
        .runtime()
        .try_reserve_node_bytes(stats.retained_capacity_bytes() - stats.retained_bytes())
        .unwrap();
    let retained = test.node.stats().retained_bytes();
    for _ in 0..20 {
        let observed = test
            .node
            .fleet_durability_supervisor(clock())
            .unwrap()
            .unwrap();
        assert_eq!(observed.state, NodeDurabilitySupervisorState::Running);
        let inventory = observed.rotations.unwrap();
        assert_eq!(inventory.running_epoch, Some(1));
        let pending = inventory.pending.unwrap();
        assert_eq!(pending.epoch, 1);
        assert_eq!(pending.progress.phase(), NodeLogRotationPhase::Retiring);
        assert!(Arc::ptr_eq(
            pending.progress.first_failure().unwrap(),
            &original
        ));
        assert!(pending.progress.completion().is_none());
    }
    assert_eq!(test.node.stats().retained_bytes(), retained);
    drop(budget);
    test.provider
        .transport
        .lose_retire
        .store(false, Ordering::Release);
    until(|| request.observe().unwrap().completion().is_some()).await;
    let completed = request.observe().unwrap().completion().unwrap().clone();
    let observed = test
        .node
        .fleet_durability_supervisor(clock())
        .unwrap()
        .unwrap();
    assert_eq!(observed.state, NodeDurabilitySupervisorState::Running);
    let inventory = observed.rotations.unwrap();
    assert!(inventory.pending.is_none());
    let latest = inventory.completed.unwrap();
    assert_eq!(latest.epoch, 1);
    assert!(Arc::ptr_eq(
        latest.progress.completion().unwrap(),
        &completed
    ));
    assert!(Arc::ptr_eq(
        latest.progress.first_failure().unwrap(),
        &original
    ));
    assert_eq!(completed.replacement_epoch(), 2);
    test.node.shutdown().await.unwrap();
    assert_eq!(test.node.stats().retained_bytes(), 0);
    test.readback(17).await;
}
