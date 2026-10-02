//! Journal-bound receiver admission, takeover, evidence and cancellation.

use super::fleet_actions::{Fixture, clock, fixture, scope};
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetActionJournal, FleetAdapterFuture, FleetCellInputs,
    FleetCellProvider, FleetRecoveryInputs,
};
use cellule_runtime::cell::actor::ReceiverState;
use cellule_runtime::cell::executor::{HandlerOutcome, MutationIdentity, Resolution};
use cellule_runtime::control::{ControlState, Owner};
use cellule_runtime::fleet::operations::*;
use cellule_runtime::identity::RequestId;
use cellule_runtime::ltx::CellReplica;

struct Cells(FleetCellInputs, Arc<Mutex<Option<FleetRecoveryInputs>>>);

impl FleetCellProvider for Cells {
    fn cell_inputs<'a>(
        &'a self,
        _spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetCellInputs> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
    fn recovery_inputs<'a>(
        &'a self,
        _spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetRecoveryInputs> {
        Box::pin(async {
            self.1.lock().unwrap().clone().ok_or_else(|| {
                Box::new(std::io::Error::other(
                    "canonical recovery proof unavailable",
                )) as Box<dyn std::error::Error + Send + Sync>
            })
        })
    }
}

struct Movement {
    source: Fixture,
    receiver: Arc<CellNode>,
    inputs: FleetCellInputs,
    spec: MoveAttemptSpec,
    recovery_inputs: Arc<Mutex<Option<FleetRecoveryInputs>>>,
}

impl Movement {
    async fn new(memory: usize) -> Self {
        Self::with_deadline(memory, 60_000).await
    }

    async fn with_deadline(memory: usize, duration_ms: i64) -> Self {
        let source = fixture().await;
        let FleetActionKind::Movement { attempt, .. } = source.action.kind() else {
            panic!()
        };
        let mut spec = attempt.spec().clone();
        spec.deadline_ms = clock() + duration_ms;
        source.journal.reset_preparing(spec.clone());
        let inputs = FleetCellInputs {
            catalog: source.handle.catalog().clone(),
            replica: CellReplica::new(
                source.authority.layout().clone(),
                *spec.target.cell_id().as_bytes(),
                *spec.incarnation.as_bytes(),
                ReplicaLimits {
                    max_database_bytes: 64 << 20,
                    max_capture_bytes: 16 << 20,
                    ..ReplicaLimits::default()
                },
            )
            .unwrap(),
            authority: source.authority.clone(),
            destination: source._root.path().join("fleet-receiver.sqlite"),
            owner: Owner {
                session: spec.destination,
                endpoint: "https://fleet-receiver.internal:8789".into(),
            },
        };
        let receiver = Arc::new(
            CellNodeBuilder::new(application())
                .with_runtime(
                    SqlWorkerPool::new(1, 8)
                        .unwrap()
                        .with_native_memory_limit(memory)
                        .unwrap(),
                    64 << 20,
                )
                .with_replica_host(
                    ReplicaHost::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
                )
                .with_session(spec.destination)
                .build()
                .unwrap(),
        );
        receiver
            .install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        let recovery_inputs = Arc::new(Mutex::new(None));
        receiver
            .install_fleet_actions(
                scope(),
                spec.destination_node,
                source.journal.clone(),
                Arc::new(Cells(inputs.clone(), recovery_inputs.clone())),
            )
            .unwrap();
        receiver
            .install_node_lease(NodeLeaseGuard::new(clock(), clock() + 60_000).unwrap())
            .unwrap();
        Self {
            source,
            receiver,
            inputs,
            spec,
            recovery_inputs,
        }
    }

    fn event(&self, event: AttemptEvent) {
        self.source.journal.transition(JournalTransition::Attempt {
            id: self.spec.id,
            event,
        });
    }

    fn action(&self, effect: MovementAction) -> FleetAction {
        self.source.journal.action(self.spec.id, effect)
    }

    fn inspection(&self, nonce: u8) -> FleetInspectionRequest {
        FleetInspectionRequest::new(
            self.action(MovementAction::Inspect),
            self.source.journal.registry(),
            Digest::from_bytes([nonce; 32]),
            self.spec.destination_node,
            self.spec.destination,
            clock() + 10_000,
        )
        .unwrap()
    }

    async fn inspect(&self, nonce: u8) -> Arc<FleetInspectionObservation> {
        let request = self.inspection(nonce);
        let observation = self
            .receiver
            .inspect_fleet_action(request.clone())
            .await
            .unwrap();
        observation.validate_for(&request, clock(), 10_000).unwrap();
        observation
    }

    async fn prepare(&self) -> Arc<FleetActionCompletion> {
        apply(&self.receiver, self.action(MovementAction::Prepare)).await
    }

    async fn release(&self) -> PublishedPosition {
        let reserved = self.prepare().await;
        let FleetOutcome::Reserved(reservation) = reserved.outcome.outcome else {
            panic!("not reserved: {:?}", reserved)
        };
        assert!(reserved.committed && reserved.execution_error.is_none());
        self.event(AttemptEvent::Reserved(reservation));
        self.event(AttemptEvent::BeginRelease);
        let released = apply(&self.source.node, self.action(MovementAction::Release)).await;
        let FleetOutcome::Released(position) = &released.outcome.outcome else {
            panic!("not released: {:?}", released)
        };
        assert!(released.committed && released.execution_error.is_none());
        self.event(AttemptEvent::Released(position.clone()));
        self.event(AttemptEvent::BeginActivate);
        position.clone()
    }

    async fn start_recovery(&self, idle: bool) {
        let prepared = self.prepare().await;
        let FleetOutcome::Reserved(reservation) = prepared.outcome.outcome else {
            panic!("not prepared")
        };
        self.event(AttemptEvent::Reserved(reservation));
        self.event(AttemptEvent::BeginRelease);
        if idle {
            // The canonical source release happened, but its action result was
            // never observed. Do not invent Released from the later Idle root.
            self.source.handle.drain().await.unwrap();
        }
        self.source.lease.fence();
        assert!(matches!(
            self.source.handle.query(1, 1, |_| Ok(Vec::new())).await,
            Err(Error::Fenced) | Err(Error::CellDraining)
        ));
        let now = clock();
        let image = Digest::from_bytes([221; 32]);
        let release = Digest::from_bytes([222; 32]);
        let directory = cellule_runtime::node::NodeDirectory::new(
            self.inputs.authority.layout().clone(),
            scope().fleet,
            image,
            release,
        );
        let key = ed25519_dalek::SigningKey::from_bytes(&[223; 32]);
        let signed = |node, session, issued, expires| {
            cellule_runtime::node::NodeAdvertisement::sign(
                node,
                session,
                "https://recovery.internal:8789".into(),
                scope().fleet,
                Digest::from_bytes([224; 32]),
                image,
                release,
                &key,
                1,
                issued,
                expires,
                vec![self.source.node.application().registry().module_digests()[0]],
                vec![1],
                cellule_runtime::node::NodeFailureDomain::default(),
                cellule_runtime::node::NodeCapacity {
                    free_memory_bytes: 128 << 20,
                    free_disk_bytes: 8 << 30,
                    follower_free_bytes: 8 << 30,
                    follower_retained_bytes: 0,
                    job_credits: 1,
                    log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
                },
            )
            .unwrap()
        };
        directory
            .create(
                signed(
                    self.spec.source_node,
                    self.spec.source,
                    now - 10_000,
                    now - 1,
                ),
                now - 10_000,
            )
            .await
            .unwrap();
        directory
            .create(
                signed(
                    self.spec.destination_node,
                    self.spec.destination,
                    now,
                    now + 10_000,
                ),
                now,
            )
            .await
            .unwrap();
        let takeover = directory
            .claim_expired_for_takeover(self.spec.source, self.spec.destination, now)
            .await
            .unwrap();
        *self.recovery_inputs.lock().unwrap() = Some(FleetRecoveryInputs {
            takeover,
            manifests: cellule_runtime::recovery::manifest::RecoveryManifestStore::new(
                self.inputs.authority.layout().clone(),
                self.inputs.replica.limits(),
            ),
        });
        self.event(AttemptEvent::OutcomeUnknown);
        self.event(AttemptEvent::BeginRecover);
    }

    async fn shutdown(&self) {
        self.receiver.shutdown().await.unwrap();
        self.source.node.shutdown().await.unwrap();
        for node in [&self.receiver, &self.source.node] {
            assert_eq!(node.state(), NodeState::Stopped);
            let stats = node.stats();
            assert_eq!(stats.active_cells(), 0);
            assert_eq!(stats.worker_jobs(), 0);
            assert_eq!(stats.retained_bytes(), 0);
            assert_eq!(stats.resident_bytes(), 0);
            assert_eq!(stats.file_descriptors(), 0);
            assert_eq!(stats.local_disk_reserved_bytes(), 0);
        }
    }
}

async fn apply(node: &CellNode, action: FleetAction) -> Arc<FleetActionCompletion> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match node.apply_fleet_action(action.clone(), clock()).await {
                Ok(result) => return result,
                Err(error) if matches!(error.as_ref(), Error::FleetOperation(error) if matches!(error.as_ref(), OperationError::Busy)) => tokio::task::yield_now().await,
                Err(error) => panic!("action failed: {error:?}"),
            }
        }
    }).await.unwrap()
}

async fn counter(handle: &cellule_runtime::cell::actor::CellHandle) -> i64 {
    let bytes = handle
        .query(64, 64, |connection| {
            Ok(connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .to_be_bytes()
                .to_vec())
        })
        .await
        .unwrap();
    i64::from_be_bytes(bytes.try_into().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_eviction_before_fleet_release_proves_refusal_and_joins_receiver_credit() {
    let movement = Movement::new(128 << 20).await;
    let prepared = movement.prepare().await;
    let FleetOutcome::Reserved(reservation) = prepared.outcome.outcome else {
        panic!("receiver did not reserve: {prepared:?}")
    };
    assert!(prepared.committed && prepared.execution_error.is_none());
    movement.event(AttemptEvent::Reserved(reservation));
    assert_eq!(
        movement.receiver.stats().local_disk_reserved_bytes(),
        movement.spec.cost.disk_bytes
    );

    // Emergency local shedding uses this same canonical eviction path. The
    // exact fleet action has not been accepted while that independent job runs.
    assert_eq!(
        movement.source.node.runtime().evict_idle(1).await.unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = movement
                .inputs
                .authority
                .load(movement.spec.target.cell_id())
                .await
                .unwrap()
                .unwrap();
            if current.value().state == ControlState::Idle
                && movement.source.node.stats().active_cells() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    movement.event(AttemptEvent::BeginRelease);
    let action = movement.action(MovementAction::Release);
    let refused = apply(&movement.source.node, action.clone()).await;
    assert!(refused.committed && refused.journal_error.is_none());
    assert_eq!(
        refused.outcome.outcome,
        FleetOutcome::Rejected(DrainBlocker::IncompleteObservation)
    );
    assert!(matches!(
        refused.execution_error.as_deref(),
        Some(Error::CellNotActive)
    ));
    let replay = apply(&movement.source.node, action).await;
    assert_eq!(replay.outcome, refused.outcome);
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        idle.value()
    );

    movement.event(AttemptEvent::ReleaseRefused(
        DrainBlocker::IncompleteObservation,
    ));
    movement.event(AttemptEvent::BeginCancel);
    let cancelled = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(cancelled.committed && cancelled.execution_error.is_none());
    assert_eq!(cancelled.outcome.outcome, FleetOutcome::ReceiverCleaned);
    assert_eq!(movement.receiver.stats().local_disk_reserved_bytes(), 0);
    movement.event(AttemptEvent::Cancelled);
    assert!(
        movement
            .source
            .journal
            .current_attempt()
            .released()
            .is_none()
    );
    assert!(
        movement
            .source
            .journal
            .current_attempt()
            .activated()
            .is_none()
    );
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_release_without_result_stays_unknown_after_local_eviction() {
    let movement = Movement::new(128 << 20).await;
    let prepared = movement.prepare().await;
    let FleetOutcome::Reserved(reservation) = prepared.outcome.outcome else {
        panic!("receiver did not reserve: {prepared:?}")
    };
    movement.event(AttemptEvent::Reserved(reservation));
    movement.event(AttemptEvent::BeginRelease);
    let action = movement.action(MovementAction::Release);
    assert!(matches!(
        movement
            .source
            .journal
            .accept_action(
                &action,
                movement.spec.source_node,
                movement.spec.source,
                clock()
            )
            .await
            .unwrap(),
        cellule_host::fleet::FleetActionAcceptance::New(_)
    ));

    // Journal acceptance with no original result cannot establish whether its
    // source effect ran. An independently released Idle root grants no proof.
    assert_eq!(
        movement.source.node.runtime().evict_idle(1).await.unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = movement
                .inputs
                .authority
                .load(movement.spec.target.cell_id())
                .await
                .unwrap()
                .unwrap();
            if current.value().state == ControlState::Idle
                && movement.source.node.stats().active_cells() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let result = apply(&movement.source.node, action).await;
    assert!(result.committed && result.execution_error.is_some());
    assert_eq!(result.outcome.outcome, FleetOutcome::Unknown);
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        idle.value()
    );
    assert_eq!(
        movement.receiver.stats().local_disk_reserved_bytes(),
        movement.spec.cost.disk_bytes
    );
    assert!(
        movement
            .source
            .journal
            .current_attempt()
            .released()
            .is_none()
    );
    assert_eq!(
        movement.source.journal.current_attempt().phase(),
        AttemptPhase::Releasing
    );
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_inspection_bypasses_historical_cache_and_rechecks_stopped_actor() {
    let movement = Movement::new(128 << 20).await;
    let preparation = movement.prepare().await;
    // Simulate a retained older-format Inspect response. It must stay readable
    // as history, but the host cannot dispatch it as a fresh observation.
    let old_action = movement.action(MovementAction::Inspect);
    let accepted = match movement
        .source
        .journal
        .accept_action(
            &old_action,
            movement.spec.destination_node,
            movement.spec.destination,
            clock(),
        )
        .await
        .unwrap()
    {
        cellule_host::fleet::FleetActionAcceptance::New(accepted) => accepted,
        _ => panic!("not original Inspect record"),
    };
    let cached = FleetActionOutcome {
        scope: scope(),
        action_key: old_action.key().unwrap(),
        node: movement.spec.destination_node,
        session: movement.spec.destination,
        observed_at_ms: clock(),
        outcome: preparation.outcome.outcome.clone(),
    };
    movement
        .source
        .journal
        .publish_action_result(&accepted, &cached)
        .await
        .unwrap();
    assert!(matches!(cached.outcome, FleetOutcome::Reserved(_)));
    movement.release().await;
    let activated = apply(
        &movement.receiver,
        movement.action(MovementAction::Activate),
    )
    .await;
    let FleetOutcome::Activated(original) = &activated.outcome.outcome else {
        panic!("{activated:?}")
    };
    movement.event(AttemptEvent::Activated(original.clone()));
    let request = movement.inspection(230);
    let first = movement
        .receiver
        .inspect_fleet_action(request.clone())
        .await
        .unwrap();
    first.validate_for(&request, clock(), 10_000).unwrap();
    let FleetOutcome::Activated(serving) = &first.outcome().outcome else {
        panic!("{first:?}")
    };
    assert_eq!(serving.position, original.position);
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &current)
        .await
        .unwrap()
        .unwrap();
    let now = clock();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([231; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([231; 32]),
            now,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value=101", [])?;
                Ok(HandlerOutcome::Success(vec![101]))
            },
        )
        .await
        .unwrap();
    let next = movement.inspection(232);
    let second = movement
        .receiver
        .inspect_fleet_action(next.clone())
        .await
        .unwrap();
    second.validate_for(&next, clock(), 10_000).unwrap();
    assert!(first.validate_for(&next, clock(), 10_000).is_err());
    let FleetOutcome::Activated(newer) = &second.outcome().outcome else {
        panic!("{second:?}")
    };
    assert!(newer.position.root.commit_sequence > serving.position.root.commit_sequence);
    assert_eq!(counter(&handle).await, 101);
    let retained = movement
        .source
        .journal
        .load_movement_action(
            scope(),
            movement.spec.id,
            MovementAction::Inspect,
            movement.spec.destination_node,
            movement.spec.destination,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(retained, cellule_host::fleet::FleetActionAcceptance::Existing { result: Some(result), .. } if *result == cached)
    );
    assert!(
        movement
            .receiver
            .apply_fleet_action(movement.action(MovementAction::Inspect), clock())
            .await
            .is_err()
    );
    handle.drain().await.unwrap();
    let stopped = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.value().state, ControlState::Idle);
    let error = movement
        .receiver
        .inspect_fleet_action(movement.inspection(233))
        .await
        .unwrap_err();
    assert!(matches!(
        error.as_ref(),
        Error::Fenced | Error::CellDraining
    ));
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        stopped.value()
    );
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_recovery_inspection_never_retries_acquisition_with_only_retained_input() {
    let movement = Movement::new(128 << 20).await;
    movement.start_recovery(false).await;
    movement
        .source
        .journal
        .lose_basis_reply
        .store(true, Ordering::SeqCst);
    let recover = movement.action(MovementAction::Recover);
    let failed = apply(&movement.receiver, recover.clone()).await;
    assert!(matches!(failed.outcome.outcome, FleetOutcome::Unknown));
    assert!(failed.execution_error.is_some());
    let before = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let writes = movement.source.journal.basis_writes.load(Ordering::SeqCst);
    let request = movement.inspection(234);
    let observed = movement
        .receiver
        .inspect_fleet_action(request.clone())
        .await
        .unwrap();
    observed.validate_for(&request, clock(), 10_000).unwrap();
    assert!(matches!(observed.outcome().outcome, FleetOutcome::Unknown));
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        before.value()
    );
    assert_eq!(
        movement.source.journal.basis_writes.load(Ordering::SeqCst),
        writes
    );
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    // Explicit replay of the accepted effect remains the only resumption path.
    let resumed = apply(&movement.receiver, recover).await;
    assert!(matches!(
        resumed.outcome.outcome,
        FleetOutcome::Recovered(_)
    ));
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_inspection_waiters_share_action_bound_and_are_joined_by_shutdown() {
    let movement = Movement::new(128 << 20).await;
    let journal = &movement.source.journal;
    journal.block_inspections.store(true, Ordering::SeqCst);
    let node = movement.receiver.clone();
    let a = movement.inspection(235);
    let first = tokio::spawn(async move { node.inspect_fleet_action(a).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        journal.inspection_entered.notified(),
    )
    .await
    .unwrap();
    let node = movement.receiver.clone();
    let b = movement.inspection(236);
    let second = tokio::spawn(async move { node.inspect_fleet_action(b).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        journal.inspection_entered.notified(),
    )
    .await
    .unwrap();
    let third = movement
        .receiver
        .inspect_fleet_action(movement.inspection(237))
        .await
        .unwrap_err();
    assert!(matches!(
        third.as_ref(),
        Error::Capacity("fleet action receipt bound")
    ));
    first.abort();
    second.abort();
    assert!(movement.receiver.stats().retained_bytes() >= 6 * MAX_RECORD_BYTES as usize);
    let node = movement.receiver.clone();
    let shutdown = tokio::spawn(async move { node.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    journal.block_inspections.store(false, Ordering::SeqCst);
    journal.inspection_resume.add_permits(2);
    shutdown.await.unwrap().unwrap();
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_movement_preserves_receipt_and_inspection_after_local_receipt_retirement() {
    let movement = Movement::new(128 << 20).await;
    let now = clock();
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([211; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    let request_digest = Digest::from_bytes([211; 32]);
    let acknowledged = movement
        .source
        .handle
        .execute(identity, request_digest, now, 64, 64, |transaction| {
            transaction.execute("UPDATE counter SET value = 77", [])?;
            Ok(HandlerOutcome::Success(vec![77]))
        })
        .await
        .unwrap();
    let released = movement.release().await;
    assert!(released.root.commit_sequence >= acknowledged.commit_sequence());
    assert_eq!(movement.source.node.stats().active_cells(), 0);
    let activation = movement.action(MovementAction::Activate);
    let (a, b) = tokio::join!(
        apply(&movement.receiver, activation.clone()),
        apply(&movement.receiver, activation.clone())
    );
    assert!(a.committed && b.committed && a.execution_error.is_none());
    assert_eq!(a.outcome, b.outcome);
    let FleetOutcome::Activated(evidence) = &a.outcome.outcome else {
        panic!("not activated: {a:?}")
    };
    assert_eq!(evidence.session, movement.spec.destination);
    assert_eq!(evidence.position.root, released.root);
    let basis = movement
        .source
        .journal
        .basis(activation.key().unwrap())
        .unwrap();
    assert_eq!(basis.position().unwrap(), released);
    basis.validate_result(&a.outcome).unwrap();
    assert_eq!(movement.source.journal.accepted_count(), 3);
    let serving = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &serving)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, 77);
    assert_eq!(
        handle
            .resolve(identity, request_digest, clock(), 64)
            .await
            .unwrap(),
        Resolution::Committed(acknowledged)
    );
    movement.event(AttemptEvent::Activated(evidence.clone()));
    let cleanup = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(cleanup.committed && cleanup.execution_error.is_none());
    assert!(matches!(
        cleanup.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    movement.event(AttemptEvent::ReceiverCleaned);
    let inspection = movement.inspect(250).await;
    assert!(matches!(
        inspection.outcome().outcome,
        FleetOutcome::Activated(_)
    ));
    assert!(
        movement
            .receiver
            .runtime()
            .prepared_receiver(movement.spec.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    assert!(movement.receiver.stats().local_disk_reserved_bytes() < movement.spec.cost.disk_bytes);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_refusal_keeps_source_serving_without_partial_receiver_credit() {
    let movement = Movement::new(64 << 10).await;
    let before = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let result = movement.prepare().await;
    assert!(result.committed && result.execution_error.is_some());
    assert!(matches!(
        result.outcome.outcome,
        FleetOutcome::Rejected(DrainBlocker::ReceiverCapacity)
    ));
    assert!(
        movement
            .receiver
            .runtime()
            .prepared_receiver(movement.spec.id)
            .unwrap()
            .is_none()
    );
    let stats = movement.receiver.stats();
    assert_eq!(stats.active_cells(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value(), before.value());
    assert_eq!(counter(&movement.source.handle).await, 42);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_prepare_and_cancel_return_original_evidence_without_double_reserving() {
    let movement = Movement::new(128 << 20).await;
    let action = movement.action(MovementAction::Prepare);
    let (a, b) = tokio::join!(
        apply(&movement.receiver, action.clone()),
        apply(&movement.receiver, action)
    );
    assert!(a.committed && b.committed);
    assert_eq!(a.outcome, b.outcome);
    assert_eq!(movement.source.journal.accepted_count(), 1);
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    assert_eq!(movement.receiver.stats().worker_jobs(), 1);
    assert_eq!(
        movement.receiver.stats().local_disk_reserved_bytes(),
        movement.spec.cost.disk_bytes
    );
    movement.event(AttemptEvent::BeginCancel);
    let cancel = movement.action(MovementAction::Cancel);
    let first = apply(&movement.receiver, cancel.clone()).await;
    assert!(first.committed && matches!(first.outcome.outcome, FleetOutcome::ReceiverCleaned));
    let duplicate = apply(&movement.receiver, cancel).await;
    assert_eq!(first.outcome, duplicate.outcome);
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    assert_eq!(movement.receiver.stats().local_disk_reserved_bytes(), 0);
    movement.event(AttemptEvent::Cancelled);
    let inspected = movement.inspect(250).await;
    assert!(matches!(
        inspected.outcome().outcome,
        FleetOutcome::ReceiverCleaned
    ));
    assert_eq!(counter(&movement.source.handle).await, 42);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_basis_reply_prevents_takeover_until_the_original_basis_is_confirmed() {
    let movement = Movement::new(128 << 20).await;
    let released = movement.release().await;
    movement
        .source
        .journal
        .lose_basis_reply
        .store(true, Ordering::SeqCst);
    let action = movement.action(MovementAction::Activate);
    let first = apply(&movement.receiver, action.clone()).await;
    assert!(first.committed && first.execution_error.is_some());
    assert!(matches!(first.outcome.outcome, FleetOutcome::Unknown));
    let retained = movement
        .source
        .journal
        .basis(action.key().unwrap())
        .unwrap();
    assert_eq!(retained.position().unwrap(), released);
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle.value(), retained.control());
    assert_eq!(idle.value().state, ControlState::Idle);
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
    let activated = apply(&movement.receiver, action.clone()).await;
    assert!(activated.committed && activated.execution_error.is_none());
    assert!(matches!(
        activated.outcome.outcome,
        FleetOutcome::Activated(_)
    ));
    assert_eq!(
        movement
            .source
            .journal
            .basis(action.key().unwrap())
            .unwrap(),
        retained
    );
    assert_eq!(
        movement.source.journal.basis_writes.load(Ordering::SeqCst),
        1
    );
    assert_eq!(movement.source.journal.accepted_count(), 3);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_waiter_cannot_cancel_a_basis_write_or_the_owned_activation() {
    let movement = Movement::new(128 << 20).await;
    movement.release().await;
    movement
        .source
        .journal
        .block_basis
        .store(true, Ordering::SeqCst);
    let node = movement.receiver.clone();
    let action = movement.action(MovementAction::Activate);
    let issued = action.clone();
    let waiter = tokio::spawn(async move { node.apply_fleet_action(issued, clock()).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        movement.source.journal.basis_entered.notified(),
    )
    .await
    .unwrap();
    assert!(
        movement
            .source
            .journal
            .basis(action.key().unwrap())
            .is_none()
    );
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle.value().state, ControlState::Idle);
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
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
    movement.source.journal.basis_resume.add_permits(1);
    let result = apply(&movement.receiver, action).await;
    assert!(result.committed && result.execution_error.is_none());
    assert!(matches!(result.outcome.outcome, FleetOutcome::Activated(_)));
    assert_eq!(
        movement.source.journal.basis_writes.load(Ordering::SeqCst),
        1
    );
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_unused_credit_is_cleaned_after_release_then_canonical_cold_activation_resumes() {
    let movement = Movement::with_deadline(128 << 20, 1_000).await;
    let released = movement.release().await;
    while clock() < movement.spec.deadline_ms {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        movement.receiver.stats().local_disk_reserved_bytes(),
        movement.spec.cost.disk_bytes
    );
    movement.event(AttemptEvent::BeginCancel);
    let cleanup = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(cleanup.committed && cleanup.execution_error.is_none());
    assert!(matches!(
        cleanup.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    assert_eq!(movement.receiver.stats().local_disk_reserved_bytes(), 0);
    movement.event(AttemptEvent::ReceiverCleaned);
    movement.event(AttemptEvent::BeginActivate);
    let activated = apply(
        &movement.receiver,
        movement.action(MovementAction::Activate),
    )
    .await;
    assert!(
        activated.committed && activated.execution_error.is_none(),
        "{activated:?}"
    );
    let FleetOutcome::Activated(evidence) = &activated.outcome.outcome else {
        panic!("not activated: {activated:?}")
    };
    assert_eq!(evidence.position.root, released.root);
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    let serving = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &serving)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, 42);
    movement.event(AttemptEvent::Activated(evidence.clone()));
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_cleanup_reply_after_release_retains_the_original_resource_evidence() {
    let movement = Movement::new(128 << 20).await;
    let released = movement.release().await;
    movement.event(AttemptEvent::BeginCancel);
    movement.source.journal.lose_next_result_reply();
    let action = movement.action(MovementAction::Cancel);
    let first = apply(&movement.receiver, action.clone()).await;
    assert!(!first.committed && first.journal_error.is_some());
    assert!(matches!(
        first.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    let confirmed = apply(&movement.receiver, action).await;
    assert!(confirmed.committed && confirmed.execution_error.is_none());
    assert_eq!(first.outcome, confirmed.outcome);
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle.value().state, ControlState::Idle);
    assert_eq!(idle.value().root.as_ref(), Some(&released.root));
    movement.event(AttemptEvent::ReceiverCleaned);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_result_retry_preserves_basis_and_original_root_after_successor_publication() {
    let movement = Movement::new(128 << 20).await;
    let released = movement.release().await;
    movement.source.journal.lose_next_result_reply();
    let action = movement.action(MovementAction::Activate);
    let first = apply(&movement.receiver, action.clone()).await;
    assert!(!first.committed && first.journal_error.is_some());
    let basis = movement
        .source
        .journal
        .basis(action.key().unwrap())
        .unwrap();
    let serving = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &serving)
        .await
        .unwrap()
        .unwrap();
    let now = clock();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([212; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([212; 32]),
            now,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = 99", [])?;
                Ok(HandlerOutcome::Success(vec![99]))
            },
        )
        .await
        .unwrap();
    let confirmed = apply(&movement.receiver, action.clone()).await;
    assert!(confirmed.committed && confirmed.execution_error.is_none());
    assert_eq!(confirmed.outcome, first.outcome);
    assert_eq!(
        movement
            .source
            .journal
            .basis(action.key().unwrap())
            .unwrap(),
        basis
    );
    assert_eq!(basis.position().unwrap(), released);
    let FleetOutcome::Activated(evidence) = &confirmed.outcome.outcome else {
        panic!()
    };
    movement.event(AttemptEvent::Activated(evidence.clone()));
    let inspected = movement.inspect(250).await;
    let FleetOutcome::Activated(current) = &inspected.outcome().outcome else {
        panic!("not serving: {inspected:?}")
    };
    assert!(current.position.root.commit_sequence > evidence.position.root.commit_sequence);
    assert_eq!(counter(&handle).await, 99);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_acquisition_on_the_preferred_session_joins_after_unused_credit_cleanup() {
    let movement = Movement::new(128 << 20).await;
    let released = movement.release().await;
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let runtime = movement.receiver.runtime();
    let inputs = movement.inputs.clone();
    let cold = tokio::spawn(async move {
        runtime
            .acquire_idle_restored(
                inputs.catalog,
                inputs.replica,
                inputs.authority,
                idle,
                inputs
                    .destination
                    .with_file_name("ordinary-receiver.sqlite"),
                inputs.owner,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = movement
                .inputs
                .authority
                .load(movement.spec.target.cell_id())
                .await
                .unwrap()
                .unwrap();
            if current
                .value()
                .owner
                .as_ref()
                .is_some_and(|owner| owner.session == movement.spec.destination)
            {
                assert_eq!(current.value().root.as_ref(), Some(&released.root));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Ordinary takeover owns authority while its affine SQL open waits for the
    // unused prepared token. Cleanup frees that token, not the new writer.
    assert!(!cold.is_finished());
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
    movement.event(AttemptEvent::BeginCancel);
    let cleanup = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(cleanup.committed && cleanup.execution_error.is_none());
    assert!(matches!(
        cleanup.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    movement.event(AttemptEvent::ReceiverCleaned);
    let handle = tokio::time::timeout(Duration::from_secs(5), cold)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, 42);
    movement.event(AttemptEvent::BeginActivate);
    let action = movement.action(MovementAction::Activate);
    let result = apply(&movement.receiver, action.clone()).await;
    assert!(result.committed && result.execution_error.is_none());
    let FleetOutcome::Activated(evidence) = &result.outcome.outcome else {
        panic!("not serving: {result:?}")
    };
    assert_eq!(evidence.position.epoch, released.epoch + 1);
    // This action observed the ordinary winner; it performed no acquisition CAS.
    assert!(
        movement
            .source
            .journal
            .basis(action.key().unwrap())
            .is_none()
    );
    movement.event(AttemptEvent::Activated(evidence.clone()));
    let inspected = movement.inspect(250).await;
    assert!(matches!(
        inspected.outcome().outcome,
        FleetOutcome::Activated(_)
    ));
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_task_failure_retains_its_original_error_across_repeated_shutdown() {
    let movement = Movement::new(128 << 20).await;
    movement.release().await;
    movement
        .source
        .journal
        .panic_basis
        .store(true, Ordering::SeqCst);
    let failed = movement
        .receiver
        .apply_fleet_action(movement.action(MovementAction::Activate), clock())
        .await
        .unwrap_err();
    let original = original_task_failure(failed.as_ref());
    assert!(original.is_panic());
    assert!(
        original
            .to_string()
            .contains("injected acquisition-basis panic")
    );
    let first = movement.receiver.shutdown().await.unwrap_err();
    let second = movement.receiver.shutdown().await.unwrap_err();
    let first_source = original_task_failure(&first);
    let second_source = original_task_failure(&second);
    assert_eq!(original.id(), first_source.id());
    assert_eq!(first_source.id(), second_source.id());
    assert_eq!(original.to_string(), first_source.to_string());
    assert_eq!(first_source.to_string(), second_source.to_string());
    assert_ne!(movement.receiver.state(), NodeState::Stopped);
    let idle = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle.value().state, ControlState::Idle);
    // The host still joined canonical runtime cleanup after the facility failed.
    // Retained fatal evidence prevents a later drain from claiming success.
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    assert_eq!(movement.receiver.stats().retained_bytes(), 0);
    assert_eq!(movement.receiver.stats().file_descriptors(), 0);
    assert_eq!(movement.receiver.stats().local_disk_reserved_bytes(), 0);
    assert!(movement.receiver.shutdown().await.is_err());
    assert_ne!(movement.receiver.state(), NodeState::Stopped);
    movement.source.node.shutdown().await.unwrap();
}

fn original_task_failure<'a>(
    mut error: &'a (dyn std::error::Error + 'static),
) -> &'a tokio::task::JoinError {
    loop {
        if let Some(error) = error.downcast_ref::<tokio::task::JoinError>() {
            return error;
        }
        error = error
            .source()
            .expect("original task source must be preserved");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_action_drain_joins_a_sibling_inspection_before_returning_original_failure() {
    let movement = Movement::new(128 << 20).await;
    movement.release().await;
    let journal = &movement.source.journal;
    journal.block_basis.store(true, Ordering::SeqCst);
    let node = movement.receiver.clone();
    let action = movement.action(MovementAction::Activate);
    let activation = tokio::spawn(async move { node.apply_fleet_action(action, clock()).await });
    tokio::time::timeout(Duration::from_secs(5), journal.basis_entered.notified())
        .await
        .unwrap();
    journal.block_inspections.store(true, Ordering::SeqCst);
    let node = movement.receiver.clone();
    let request = movement.inspection(238);
    let inspection = tokio::spawn(async move { node.inspect_fleet_action(request).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        journal.inspection_entered.notified(),
    )
    .await
    .unwrap();
    // Inject the failure at the same before-claim boundary after both finite
    // jobs are accepted. The sibling remains deliberately paused in its adapter.
    journal.panic_basis.store(true, Ordering::SeqCst);
    journal.basis_resume.add_permits(1);
    let failed = activation.await.unwrap().unwrap_err();
    let original = original_task_failure(failed.as_ref()).id();
    inspection.abort();
    let node = movement.receiver.clone();
    let mut shutdown = tokio::spawn(async move { node.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut shutdown)
            .await
            .is_err()
    );
    assert!(!shutdown.is_finished());
    journal.block_inspections.store(false, Ordering::SeqCst);
    journal.inspection_resume.add_permits(1);
    let failure = shutdown.await.unwrap().unwrap_err();
    assert_eq!(original_task_failure(&failure).id(), original);
    let repeated = movement.receiver.shutdown().await.unwrap_err();
    assert_eq!(original_task_failure(&repeated).id(), original);
    assert_ne!(movement.receiver.state(), NodeState::Stopped);
    let stats = movement.receiver.stats();
    assert_eq!(stats.active_cells(), 0);
    assert_eq!(stats.worker_jobs(), 0);
    assert_eq!(stats.retained_bytes(), 0);
    assert_eq!(stats.resident_bytes(), 0);
    assert_eq!(stats.file_descriptors(), 0);
    assert_eq!(stats.local_disk_reserved_bytes(), 0);
    movement.source.node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_recovery_preserves_receipt_and_immutable_basis_after_root_advances() {
    let movement = Movement::new(128 << 20).await;
    let now = clock();
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([231; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    let digest = Digest::from_bytes([231; 32]);
    let receipt = movement
        .source
        .handle
        .execute(identity, digest, now, 64, 64, |tx| {
            tx.execute("UPDATE counter SET value = 77", [])?;
            Ok(HandlerOutcome::Success(vec![77]))
        })
        .await
        .unwrap();
    movement.start_recovery(false).await;
    let source_control = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_control.value().state, ControlState::Serving);
    movement.source.journal.lose_next_result_reply();
    let action = movement.action(MovementAction::Recover);
    let recovered = apply(&movement.receiver, action.clone()).await;
    assert!(!recovered.committed && recovered.execution_error.is_none());
    let FleetOutcome::Recovered(e) = &recovered.outcome.outcome else {
        panic!("not recovered: {recovered:?}")
    };
    assert_eq!(e.recovery.basis().control(), source_control.value());
    assert_eq!(
        e.recovery.position().unwrap().root,
        source_control.value().root.clone().unwrap()
    );
    let original = movement
        .source
        .journal
        .recovery_evidence(action.key().unwrap())
        .unwrap();
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &current)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, 77);
    assert_eq!(
        handle.resolve(identity, digest, clock(), 64).await.unwrap(),
        Resolution::Committed(receipt)
    );
    let now = clock();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([232; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([232; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value = 99", [])?;
                Ok(HandlerOutcome::Success(vec![99]))
            },
        )
        .await
        .unwrap();
    let republished = apply(&movement.receiver, action).await;
    assert!(republished.committed);
    assert_eq!(republished.outcome, recovered.outcome);
    assert_eq!(
        movement
            .source
            .journal
            .recovery_evidence(republished.outcome.action_key)
            .unwrap(),
        original
    );
    movement.event(AttemptEvent::Recovered(e.clone()));
    let history = movement.source.journal.current_attempt();
    assert_eq!(history.phase(), AttemptPhase::Recovered);
    assert!(history.released().is_none() && history.activated().is_none());
    let cleaned = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(cleaned.committed && cleaned.execution_error.is_none());
    assert!(matches!(
        cleaned.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    movement.event(AttemptEvent::ReceiverCleaned);
    let inspected = movement.inspect(250).await;
    let FleetOutcome::Recovered(fresh) = &inspected.outcome().outcome else {
        panic!("not recovered: {inspected:?}")
    };
    assert_eq!(fresh.recovery, original);
    assert!(
        fresh.serving.position.root.commit_sequence
            > fresh.recovery.position().unwrap().root.commit_sequence
    );
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unobserved_release_cas_recovers_idle_input_without_fabricating_clean_release() {
    let movement = Movement::new(128 << 20).await;
    movement.start_recovery(true).await;
    let result = apply(&movement.receiver, movement.action(MovementAction::Recover)).await;
    assert!(result.committed && result.execution_error.is_none());
    let FleetOutcome::Recovered(evidence) = &result.outcome.outcome else {
        panic!("not recovered: {result:?}")
    };
    assert_eq!(
        evidence.recovery.basis().control().state,
        ControlState::Idle
    );
    assert_eq!(
        evidence.recovery.basis().control().epoch,
        movement.spec.source_epoch
    );
    movement.event(AttemptEvent::Recovered(evidence.clone()));
    assert!(
        movement
            .source
            .journal
            .current_attempt()
            .released()
            .is_none()
    );
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_recovery_basis_reply_retains_original_input_and_prevents_cas_until_confirmation() {
    let movement = Movement::new(128 << 20).await;
    movement.start_recovery(false).await;
    movement
        .source
        .journal
        .lose_basis_reply
        .store(true, Ordering::SeqCst);
    let action = movement.action(MovementAction::Recover);
    let unknown = apply(&movement.receiver, action.clone()).await;
    assert!(unknown.committed && unknown.execution_error.is_some());
    assert!(matches!(unknown.outcome.outcome, FleetOutcome::Unknown));
    let original = movement
        .source
        .journal
        .recovery_basis(action.key().unwrap())
        .unwrap();
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value(),
        original.control()
    );
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    let recovered = apply(&movement.receiver, action.clone()).await;
    assert!(recovered.committed && recovered.execution_error.is_none());
    assert!(matches!(
        recovered.outcome.outcome,
        FleetOutcome::Recovered(_)
    ));
    assert_eq!(
        movement
            .source
            .journal
            .recovery_basis(action.key().unwrap())
            .unwrap(),
        original
    );
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_recovery_evidence_reply_cannot_admit_actor_or_claim_completion() {
    let movement = Movement::new(128 << 20).await;
    movement.start_recovery(false).await;
    movement
        .source
        .journal
        .lose_recovery_evidence_reply
        .store(true, Ordering::SeqCst);
    let action = movement.action(MovementAction::Recover);
    let failed = apply(&movement.receiver, action.clone()).await;
    assert!(failed.committed && failed.execution_error.is_some());
    assert!(matches!(failed.outcome.outcome, FleetOutcome::Unknown));
    let evidence = movement
        .source
        .journal
        .recovery_evidence(action.key().unwrap())
        .unwrap();
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value().state, ControlState::Idle);
    assert_eq!(
        current.value().root.as_ref(),
        evidence.restored().root.as_ref()
    );
    assert_eq!(current.value().epoch, evidence.restored().epoch);
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    let repeated = apply(&movement.receiver, action.clone()).await;
    assert!(matches!(repeated.outcome.outcome, FleetOutcome::Unknown));
    assert!(repeated.execution_error.is_some());
    assert_eq!(
        movement.source.journal.current_attempt().phase(),
        AttemptPhase::Recovering
    );
    assert_eq!(
        movement
            .source
            .journal
            .current_attempt()
            .spec()
            .cost
            .disk_bytes,
        movement.spec.cost.disk_bytes
    );
    // Ordinary canonical recovery can acquire the safe Idle root after rollback.
    // Fleet inspection then proves current serving against the retained original
    // recovery result, without fabricating a second clean source release.
    let ordinary = movement
        .receiver
        .runtime()
        .acquire_idle_restored(
            movement.inputs.catalog.clone(),
            movement.inputs.replica.clone(),
            movement.inputs.authority.clone(),
            current,
            movement.inputs.destination.clone(),
            movement.inputs.owner.clone(),
        )
        .await
        .unwrap();
    assert_eq!(counter(&ordinary).await, 42);
    let finished = apply(&movement.receiver, action).await;
    assert!(finished.committed && finished.execution_error.is_none());
    let FleetOutcome::Recovered(finished_evidence) = &finished.outcome.outcome else {
        panic!("not recovered: {finished:?}")
    };
    assert_eq!(finished_evidence.recovery, evidence);
    assert!(finished_evidence.serving.position.epoch > evidence.restored().epoch);
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_recovery_waiter_leaves_owned_basis_recording_and_takeover_running() {
    let movement = Movement::new(128 << 20).await;
    movement.start_recovery(false).await;
    movement
        .source
        .journal
        .block_basis
        .store(true, Ordering::SeqCst);
    let node = movement.receiver.clone();
    let action = movement.action(MovementAction::Recover);
    let dispatched = action.clone();
    let waiter = tokio::spawn(async move { node.apply_fleet_action(dispatched, clock()).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        movement.source.journal.basis_entered.notified(),
    )
    .await
    .unwrap();
    assert!(
        movement
            .source
            .journal
            .recovery_basis(action.key().unwrap())
            .is_none()
    );
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .epoch,
        movement.spec.source_epoch
    );
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    movement.source.journal.basis_resume.add_permits(1);
    let result = apply(&movement.receiver, action).await;
    assert!(result.committed && result.execution_error.is_none());
    assert!(matches!(result.outcome.outcome, FleetOutcome::Recovered(_)));
    movement.shutdown().await;
}
