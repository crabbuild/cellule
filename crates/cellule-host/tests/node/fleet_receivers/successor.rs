use super::*;
use cellule_runtime::identity::NodeId;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_successor_inspection_does_not_acquire_or_settle_preferred_credit() {
    inspect_successor(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preferred_node_new_session_requires_its_own_read_only_successor_request() {
    inspect_successor(true).await;
}

async fn inspect_successor(preferred_physical_node: bool) {
    let movement = Movement::new(128 << 20).await;
    let released = movement.release().await;
    let node_id = if preferred_physical_node {
        movement.spec.destination_node
    } else {
        NodeId::from_bytes([203; 16])
    };
    let session = SessionId::from_bytes([205; 16]);
    let mut inputs = movement.inputs.clone();
    inputs.destination = inputs.destination.with_file_name("actual-successor.sqlite");
    inputs.owner = Owner {
        session,
        endpoint: "https://actual-successor.internal:8789".into(),
    };
    let node = CellNodeBuilder::new(application())
        .with_runtime(
            SqlWorkerPool::new(1, 8)
                .unwrap()
                .with_native_memory_limit(128 << 20)
                .unwrap(),
            64 << 20,
        )
        .with_replica_host(ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
        .with_session(session)
        .build()
        .unwrap();
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    node.install_fleet_actions(
        scope(),
        node_id,
        movement.source.journal.clone(),
        Arc::new(Cells(inputs.clone(), Arc::new(Mutex::new(None)))),
    )
    .unwrap();
    let lease_observed_at = clock();
    node.install_node_lease(
        NodeLeaseGuard::new(lease_observed_at, lease_observed_at + 60_000).unwrap(),
    )
    .unwrap();
    let make_request = |nonce| {
        FleetInspectionRequest::new(
            movement.action(MovementAction::Inspect),
            movement.source.journal.registry(),
            Digest::from_bytes([nonce; 32]),
            node_id,
            session,
            clock() + 10_000,
        )
        .unwrap()
    };
    // A known release permits a read, but Idle authority and no actor cannot
    // manufacture serving. Inspection leaves the pinned authority unchanged.
    let idle = inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(node.inspect_fleet_action(make_request(231)).await.is_err());
    assert_eq!(
        inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        idle.value()
    );
    let handle = node
        .runtime()
        .acquire_idle_restored(
            inputs.catalog.clone(),
            inputs.replica.clone(),
            inputs.authority.clone(),
            idle,
            inputs.destination.clone(),
            inputs.owner.clone(),
        )
        .await
        .unwrap();
    assert_eq!(counter(&handle).await, 42);
    let current = inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let accepts = movement.source.journal.accepted_count();
    let charged = movement.receiver.stats().local_disk_reserved_bytes();
    assert!(charged > 0);
    let request = make_request(232);
    let observed = node.inspect_fleet_action(request.clone()).await.unwrap();
    observed.validate_for(&request, clock(), 10_000).unwrap();
    let FleetOutcome::Activated(evidence) = &observed.outcome().outcome else {
        panic!("not actual serving: {observed:?}")
    };
    assert_eq!((evidence.node, evidence.session), (node_id, session));
    assert_eq!(evidence.position.root, released.root);
    assert_eq!(evidence.position.epoch, released.epoch + 1);
    assert_eq!(
        inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        current.value()
    );
    assert_eq!(movement.source.journal.accepted_count(), accepts);
    assert_eq!(
        movement.receiver.stats().local_disk_reserved_bytes(),
        charged
    );
    assert_eq!(
        movement
            .receiver
            .runtime()
            .prepared_receiver(movement.spec.id)
            .unwrap()
            .unwrap()
            .state()
            .unwrap(),
        ReceiverState::Prepared
    );
    assert!(
        node.apply_fleet_action(movement.action(MovementAction::Activate), clock())
            .await
            .is_err()
    );
    assert_eq!(movement.source.journal.accepted_count(), accepts);
    movement.event(AttemptEvent::Activated(evidence.clone()));
    let attempt = movement.source.journal.current_attempt();
    assert_eq!(attempt.next_action(), MovementAction::Cancel);
    assert!(!attempt.receiver_resources_settled());
    let cleanup = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(cleanup.committed && cleanup.execution_error.is_none());
    assert!(matches!(
        cleanup.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    movement.event(AttemptEvent::ReceiverCleaned);
    assert_eq!(
        movement.source.journal.current_attempt().next_action(),
        MovementAction::Retire
    );
    let fresh = make_request(233);
    assert!(matches!(
        node.inspect_fleet_action(fresh)
            .await
            .unwrap()
            .outcome()
            .outcome,
        FleetOutcome::Activated(_)
    ));
    handle.drain().await.unwrap();
    assert!(node.inspect_fleet_action(make_request(234)).await.is_err());
    node.shutdown().await.unwrap();
    assert_eq!(node.stats().active_cells(), 0);
    assert_eq!(node.stats().worker_jobs(), 0);
    assert_eq!(node.stats().retained_bytes(), 0);
    assert_eq!(node.stats().resident_bytes(), 0);
    assert_eq!(node.stats().file_descriptors(), 0);
    assert_eq!(node.stats().local_disk_reserved_bytes(), 0);
    movement.shutdown().await;
}
