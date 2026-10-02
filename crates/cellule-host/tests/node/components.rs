//! Owned components and provider facilities: registration, readiness, drain.

use super::*;

struct UnmanagedDurability;
impl NodeDurabilityProvider for UnmanagedDurability {
    fn recruit(
        self: Arc<Self>,
        _limits: ReplicaLimits,
        _bytes: u64,
        _live: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        Box::pin(async { panic!("rejected fleet provider started recruitment") })
    }
}

#[tokio::test]
async fn configured_fleet_rejects_an_unmanaged_follower_provider_before_starting_it() {
    use cellule_runtime::fleet::operations::{FleetScope, NodeIntent};
    use cellule_runtime::identity::NodeId;
    let session = SessionId::from_bytes([248; 16]);
    let scope = FleetScope {
        fleet: Digest::from_bytes([249; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    };
    let node = CellNodeBuilder::new(application())
        .with_session(session)
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 << 20)
        .with_replica_host(ReplicaHost::default())
        .with_fleet_startup_intent(
            NodeIntent::initial(scope, NodeId::from_bytes([248; 16]), session).unwrap(),
        )
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_node_lease_for_startup(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    let error = node
        .install_node_durability_provider(
            Arc::new(UnmanagedDurability),
            NodeDurabilitySupervisorConfig::new(
                scope.application,
                ReplicaLimits::default(),
                1,
                3,
                Duration::from_millis(10),
                Duration::from_secs(60),
                100,
            )
            .unwrap(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Control("configured fleet durability requires managed follower enrollment")
    ));
    assert!(
        node.owned_component::<UnmanagedDurability>(NODE_DURABILITY_PROVIDER_COMPONENT)
            .is_none()
    );
    assert!(node.runtime().node_durability().is_none());
    assert_eq!(node.state(), NodeState::Starting);
    assert!(node.runtime().node_admission().startup_held().unwrap());
    node.shutdown().await.unwrap();
    assert_eq!(node.stats().retained_bytes(), 0);
}

#[tokio::test]
async fn reader_reconciliation_before_lease_installation_keeps_its_owner_healthy() {
    use cellule_runtime::ltx::CellStorageLayout;
    use cellule_runtime::node::NodeDirectory;
    use cellule_store::Store;
    use object_store::{memory::InMemory, path::Path};

    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 << 20)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([246; 16]))
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("reader-prelease"),
        [3; 16],
    );
    let directory = NodeDirectory::new(
        layout.clone(),
        Digest::from_bytes([2; 32]),
        Digest::from_bytes([4; 32]),
        Digest::from_bytes([5; 32]),
    );
    let manager = node
        .install_read_replicas(
            layout,
            directory,
            root.path().into(),
            ReplicaLimits::default(),
        )
        .unwrap();
    // Drive the same public loop through its first immediate tick before the
    // lease exists. This fixes the ordering that raced host startup in CI.
    let cancellation = CancellationToken::new();
    let mut running = Box::pin(manager.run(cancellation.clone()));
    let early = tokio::select! {
        result = &mut running => Some(result),
        () = tokio::time::sleep(Duration::from_millis(20)) => None,
    };
    cancellation.cancel();
    let outcome = match early {
        Some(result) => result,
        None => running.await,
    };
    let start = node.install_node_lease(NodeLeaseGuard::new(0, 60_000).unwrap());
    let shutdown = node.shutdown().await;
    assert!(
        outcome.is_ok(),
        "pre-lease reconciliation returned {outcome:?}"
    );
    start.unwrap();
    shutdown.unwrap();
    assert_eq!(node.state(), NodeState::Stopped);
    assert_eq!(node.stats().retained_bytes(), 0);
    assert_eq!(node.stats().worker_jobs(), 0);
    assert_eq!(node.stats().file_descriptors(), 0);
}

#[tokio::test]
async fn node_retains_typed_components_without_duplicate_names() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([26; 16]))
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let component = Arc::new(7_u64);
    let weak = Arc::downgrade(&component);
    node.install_facility(CellNodeFacility::new("unowned", || async { Ok(()) }).unwrap())
        .unwrap();
    assert!(
        node.install_owned_component("unowned", Arc::new(6_u64))
            .is_err()
    );
    node.install_owned_component("fixture-component", Arc::clone(&component))
        .unwrap();
    drop(component);
    assert_eq!(
        node.owned_component::<u64>("fixture-component").as_deref(),
        Some(&7)
    );
    assert_eq!(
        node.try_owned_component::<u64>("fixture-component")
            .unwrap()
            .as_deref(),
        Some(&7)
    );
    assert!(
        node.try_owned_component::<String>("fixture-component")
            .is_err()
    );
    assert!(node.try_owned_component::<u64>("unowned").is_err());
    assert!(
        node.try_owned_component::<u64>("missing")
            .unwrap()
            .is_none()
    );
    assert!(
        node.install_owned_component("fixture-component", Arc::new(8_u64))
            .is_err()
    );
    node.shutdown().await.unwrap();
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn facility_batch_installation_is_atomic_on_name_conflict() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([30; 16]))
        .build()
        .unwrap();
    node.install_owned_component("existing", Arc::new(1_u64))
        .unwrap();
    let first = CellNodeFacility::owned("first", Arc::new(2_u64), || async { Ok(()) }).unwrap();
    let duplicate =
        CellNodeFacility::owned("existing", Arc::new(3_u64), || async { Ok(()) }).unwrap();

    assert!(node.install_facilities([first, duplicate]).is_err());
    assert!(node.owned_component::<u64>("first").is_none());
    assert_eq!(node.owned_component::<u64>("existing").as_deref(), Some(&1));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn readiness_requires_declared_owned_components() {
    let node = CellNodeBuilder::new(application())
        .with_required_owned_components(["catalog", "router"])
        .unwrap()
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([28; 16]))
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_node_lease_for_startup(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    node.install_owned_component("catalog", Arc::new(1_u64))
        .unwrap();

    assert!(matches!(node.start(), Err(Error::Control(_))));
    node.install_owned_component("router", Arc::new(2_u64))
        .unwrap();
    node.start().unwrap();
    assert!(node.is_ready());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn facility_registration_is_frozen_after_readiness() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([34; 16]))
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_node_lease(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap();
    assert!(node.is_ready());

    assert!(matches!(
        node.install_facility(CellNodeFacility::new("late", || async { Ok(()) }).unwrap()),
        Err(Error::CellDraining)
    ));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn required_component_declaration_rejects_duplicates_and_late_changes() {
    assert!(
        CellNodeBuilder::new(application())
            .with_required_owned_components(["catalog", "catalog"])
            .is_err()
    );
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([29; 16]))
        .build()
        .unwrap();
    assert!(
        node.require_owned_components(["catalog", "catalog"])
            .is_err()
    );
    node.require_owned_components(["catalog"]).unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_node_lease(NodeLeaseGuard::new(0, 60_000).unwrap())
        .unwrap_err();
    assert!(node.require_owned_components(["router"]).is_ok());
    node.shutdown().await.unwrap();
    assert!(node.require_owned_components(["late"]).is_err());
}

#[tokio::test]
async fn facilities_drain_in_reverse_registration_order() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([16; 16]))
        .build()
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    for name in ["storage", "transport", "scheduler"] {
        let events = Arc::clone(&events);
        node.install_facility(
            CellNodeFacility::new(name, move || {
                let events = Arc::clone(&events);
                async move {
                    events
                        .lock()
                        .map_err(|_| {
                            Box::new(std::io::Error::other("event lock poisoned"))
                                as Box<dyn std::error::Error + Send + Sync>
                        })?
                        .push(name);
                    Ok(())
                }
            })
            .unwrap(),
        )
        .unwrap();
    }

    node.shutdown().await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec!["scheduler", "transport", "storage"]
    );
}

#[tokio::test]
async fn facility_failure_is_reported_after_all_facilities_attempt_and_runtime_drains() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([17; 16]))
        .build()
        .unwrap();
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_clone = Arc::clone(&completed);
    node.install_facility(
        CellNodeFacility::new("healthy", move || {
            let completed = Arc::clone(&completed_clone);
            async move {
                completed.store(true, Ordering::Release);
                Ok(())
            }
        })
        .unwrap(),
    )
    .unwrap();
    node.install_facility(
        CellNodeFacility::new("broken", || async {
            Err(Box::new(std::io::Error::other("drain failed"))
                as Box<dyn std::error::Error + Send + Sync>)
        })
        .unwrap(),
    )
    .unwrap();

    let error = node.shutdown().await.unwrap_err();
    assert!(matches!(error, Error::Facility { name: "broken", .. }));
    assert!(completed.load(Ordering::Acquire));
    assert!(node.is_shutting_down());
    assert_eq!(node.state(), NodeState::Draining);
}

#[tokio::test]
async fn facility_registration_is_bounded_and_rejected_after_drain() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 1).unwrap(), 16 * 1024 * 1024)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([18; 16]))
        .build()
        .unwrap();
    assert!(CellNodeFacility::new("", || async { Ok(()) }).is_err());
    for index in 0..MAX_NODE_FACILITIES {
        let name = Box::leak(format!("facility-{index}").into_boxed_str());
        node.install_facility(CellNodeFacility::new(name, || async { Ok(()) }).unwrap())
            .unwrap();
    }
    assert!(matches!(
        node.install_facility(CellNodeFacility::new("overflow", || async { Ok(()) }).unwrap()),
        Err(Error::Capacity(_))
    ));
    node.shutdown().await.unwrap();
    assert!(matches!(
        node.install_facility(CellNodeFacility::new("late", || async { Ok(()) }).unwrap()),
        Err(Error::CellDraining)
    ));
}
