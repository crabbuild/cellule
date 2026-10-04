//! Exact Activity completion, lease expiry, timers and external waits across a move.

use super::*;
use cellule_runtime::cell::actor::{CellHandle, CellInventoryEntry, MaintenanceCellRelease};
use cellule_runtime::control::ControlState;
use cellule_runtime::primitives::workflow::{
    ActivityClaim, ActivityCompletion, ActivityCompletionOutcome, ActivityLeaseOutcome,
    WorkflowActivityClaimRequest, WorkflowActivityExtendRequest, WorkflowActivityValidateRequest,
};

struct Fixture {
    registry: Arc<cellule_runtime::Registry>,
    target: CellTarget,
    incarnation: IncarnationId,
    session: SessionId,
    directory: tempfile::TempDir,
    replica: CellReplica,
    authority: CellAuthority,
    runtime: CellRuntime,
    handle: CellHandle,
}

async fn fixture() -> Fixture {
    let registry = registry();
    let target = CellTarget::new(
        TenantId::from_bytes([61; 16]),
        ApplicationId::from_bytes([62; 16]),
        WORKFLOW_NAMESPACE,
        &0_u32.to_be_bytes(),
    )
    .unwrap();
    let incarnation = IncarnationId::from_bytes([63; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("workflow-maintenance"),
        [62; 16],
    );
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let proof = CellCatalog::new(layout.clone(), target.tenant())
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Workflow,
                registry.module_code(WORKFLOW_MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout);
    let session = SessionId::from_bytes([64; 16]);
    let control = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://source.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 10).unwrap(), 16 << 20, session).unwrap();
    let handle = runtime
        .bootstrap(
            proof,
            replica.clone(),
            authority.clone(),
            control,
            directory.path().join("source.sqlite"),
            install_workflow_schema,
        )
        .await
        .unwrap();
    Fixture {
        registry,
        target,
        incarnation,
        session,
        directory,
        replica,
        authority,
        runtime,
        handle,
    }
}

fn completion(claim: &ActivityClaim) -> ActivityCompletion {
    ActivityCompletion {
        run_id: claim.run_id,
        activity_id: claim.activity_id,
        attempt: claim.attempt,
        lease_token: claim.token,
        completion_token: [80; 16],
        result: b"settled".to_vec(),
        failed: false,
        retryable: false,
    }
}

async fn maintenance_workflow_movement(expire: bool) {
    let fixture = fixture().await;
    let client = CellClient::local(fixture.registry.clone(), fixture.handle.clone());
    let workflows = WorkflowNamespace::<TestWorkflow>::new(
        client.clone(),
        fixture.target.tenant(),
        fixture.target.application(),
    )
    .unwrap();
    workflows
        .start(
            mutation_identity(65),
            b"leased".to_vec(),
            b"activity".to_vec(),
        )
        .await
        .unwrap();
    let claimed = client
        .command::<WorkflowActivityClaimCommand<TestWorkflow>>(
            &fixture.target,
            mutation_identity(66),
            WorkflowActivityClaimRequest {
                limit: 1,
                lease_ms: if expire { 5_000 } else { 30_000 },
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.output.len(), 1);
    let mut claim = claimed.output[0].clone();
    workflows
        .start(
            mutation_identity(67),
            b"unclaimed".to_vec(),
            b"activity-blocking".to_vec(),
        )
        .await
        .unwrap();
    let waiting = workflows
        .start(
            mutation_identity(68),
            b"external-wait".to_vec(),
            b"waiting".to_vec(),
        )
        .await
        .unwrap();
    let WorkflowOutcome::Applied {
        run_id: waiting_run,
        ..
    } = waiting.output
    else {
        panic!("wait not applied")
    };
    let timer = workflows
        .start(mutation_identity(69), b"timer".to_vec(), b"timer".to_vec())
        .await
        .unwrap();
    let page = fixture.runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("missing owner")
    };
    let generation = owner.generation;
    let epoch = owner.position.as_ref().unwrap().epoch;
    drop(page);
    let mut moving = {
        let runtime = fixture.runtime.clone();
        let cell = fixture.target.cell_id();
        let session = fixture.session;
        let incarnation = fixture.incarnation;
        tokio::spawn(async move {
            runtime
                .release_maintenance_cell_at(
                    cell,
                    session,
                    generation,
                    incarnation,
                    epoch,
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let page = fixture.runtime.fleet_cells_page(None, 128).await.unwrap();
            if let CellInventoryEntry::Owned(owner) = &page.entries()[0]
                && owner.quiescing
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut moving)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .state,
        ControlState::Serving
    );
    assert!(
        client
            .query::<WorkflowActivityValidateQuery<TestWorkflow>>(
                &fixture.target,
                Some(claimed.receipt),
                WorkflowActivityValidateRequest {
                    claimed: vec![claim.clone()]
                }
            )
            .await
            .unwrap()
            .output
    );
    assert!(matches!(
        client
            .command::<WorkflowActivityClaimCommand<TestWorkflow>>(
                &fixture.target,
                mutation_identity(70),
                WorkflowActivityClaimRequest {
                    limit: 1,
                    lease_ms: 5_000
                }
            )
            .await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    assert!(matches!(
        fixture
            .registry
            .run_maintenance_once(
                client.clone(),
                fixture.target.clone(),
                mutation_identity(71),
                MaintenanceTickRequest {
                    expected_commit_sequence: timer.receipt.commit_sequence
                }
            )
            .await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    assert!(matches!(
        workflows.state(b"external-wait".to_vec(), None).await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    let original_completion = completion(&claim);
    if !expire {
        let extended = client
            .command::<WorkflowActivityExtendCommand<TestWorkflow>>(
                &fixture.target,
                mutation_identity(72),
                WorkflowActivityExtendRequest {
                    claim: claim.clone(),
                    extension_ms: 30_000,
                },
            )
            .await
            .unwrap();
        let ActivityLeaseOutcome::Extended { lease_until_ms } = extended.output else {
            panic!("extension lost")
        };
        assert!(lease_until_ms > claim.lease_until_ms);
        claim.lease_until_ms = lease_until_ms;
        assert!(
            client
                .query::<WorkflowActivityValidateQuery<TestWorkflow>>(
                    &fixture.target,
                    Some(extended.receipt),
                    WorkflowActivityValidateRequest {
                        claimed: vec![claim.clone()]
                    }
                )
                .await
                .unwrap()
                .output
        );
        assert!(!moving.is_finished());
        let completed = client
            .command::<WorkflowActivityCompleteCommand<TestWorkflow>>(
                &fixture.target,
                mutation_identity(73),
                original_completion.clone(),
            )
            .await
            .unwrap();
        assert!(matches!(
            completed.output,
            ActivityCompletionOutcome::Applied(_)
        ));
    }
    let MaintenanceCellRelease::Released(position) = moving.await.unwrap().unwrap() else {
        panic!("activity move refused")
    };
    let idle = fixture
        .authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle.value().state, ControlState::Idle);
    assert_eq!(idle.value().root.as_ref(), Some(&position.root));
    let session = SessionId::from_bytes([81; 16]);
    let destination =
        CellRuntime::new(SqlWorkerPool::new(1, 10).unwrap(), 16 << 20, session).unwrap();
    let restored = destination
        .acquire_idle_restored(
            fixture.handle.catalog().clone(),
            fixture.replica.clone(),
            fixture.authority.clone(),
            idle,
            fixture.directory.path().join("receiver.sqlite"),
            Owner {
                session,
                endpoint: "https://receiver.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let client = CellClient::local(fixture.registry.clone(), restored.clone());
    let workflows = WorkflowNamespace::<TestWorkflow>::new(
        client.clone(),
        fixture.target.tenant(),
        fixture.target.application(),
    )
    .unwrap();
    let late = client
        .command::<WorkflowActivityCompleteCommand<TestWorkflow>>(
            &fixture.target,
            mutation_identity(74),
            original_completion.clone(),
        )
        .await;
    // The receiver uses the registered native driver to resume the unclaimed
    // activity; no raw row mutation or alternate scheduler performs the work.
    let pool = BlockingActivityPool::new(1).unwrap();
    assert!(matches!(
        fixture
            .registry
            .run_activity_once(
                client.clone(),
                &fixture.target,
                5_000,
                pool.try_reserve().unwrap()
            )
            .await
            .unwrap(),
        ActivityRunOutcome::Completed { .. }
    ));
    assert_eq!(
        workflows
            .state(b"unclaimed".to_vec(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        WorkflowStatus::Completed
    );
    if expire {
        assert!(
            matches!(late, Err(InvocationError::Rejected(outcome)) if outcome.output == ActivityCompletionOutcome::LeaseLost)
        );
        let reclaimed = client
            .command::<WorkflowActivityClaimCommand<TestWorkflow>>(
                &fixture.target,
                mutation_identity(75),
                WorkflowActivityClaimRequest {
                    limit: 1,
                    lease_ms: 30_000,
                },
            )
            .await
            .unwrap();
        assert_eq!(reclaimed.output.len(), 1);
        let retry = &reclaimed.output[0];
        assert_eq!(retry.activity_id, claim.activity_id);
        assert_eq!(retry.attempt, 2);
        assert_ne!(retry.token, claim.token);
        assert_eq!(retry.definition_digest, claim.definition_digest);
        assert!(
            matches!(client.command::<WorkflowActivityCompleteCommand<TestWorkflow>>(&fixture.target, mutation_identity(76), original_completion).await,
            Err(InvocationError::Rejected(outcome)) if outcome.output == ActivityCompletionOutcome::LeaseLost)
        );
        assert!(matches!(
            client
                .command::<WorkflowActivityCompleteCommand<TestWorkflow>>(
                    &fixture.target,
                    mutation_identity(77),
                    completion(retry)
                )
                .await
                .unwrap()
                .output,
            ActivityCompletionOutcome::Applied(_)
        ));
    } else {
        assert!(
            matches!(late.unwrap().output, ActivityCompletionOutcome::Duplicate { result } if result == b"settled")
        );
    }
    assert_eq!(
        workflows
            .state(b"leased".to_vec(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        WorkflowStatus::Completed
    );
    let state = workflows
        .state(b"external-wait".to_vec(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(state.run_id, waiting_run);
    assert_eq!(state.state, b"waiting");
    assert_eq!(state.status, WorkflowStatus::Running);
    let signal = WorkflowSignal {
        workflow_id: b"external-wait".to_vec(),
        run_id: waiting_run,
        signal_id: [82; 16],
        event: b"finish".to_vec(),
    };
    workflows
        .signal(mutation_identity(78), signal.clone())
        .await
        .unwrap();
    assert!(matches!(
        workflows
            .signal(mutation_identity(79), signal)
            .await
            .unwrap()
            .output,
        WorkflowOutcome::Duplicate { .. }
    ));
    assert_eq!(
        workflows
            .state(b"external-wait".to_vec(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        WorkflowStatus::Completed
    );
    assert_eq!(
        workflows
            .state(b"timer".to_vec(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state,
        b"timer-waiting"
    );
    let sequence = fixture
        .authority
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .root
        .as_ref()
        .unwrap()
        .commit_sequence;
    let tick = fixture
        .registry
        .run_maintenance_once(
            client,
            fixture.target.clone(),
            mutation_identity(80),
            MaintenanceTickRequest {
                expected_commit_sequence: sequence,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        tick.output,
        MaintenanceTickOutcome::Applied { processed: 1 }
    );
    assert_eq!(
        workflows
            .state(b"timer".to_vec(), Some(tick.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        WorkflowStatus::Completed
    );
    pool.shutdown().await.unwrap();
    restored.drain().await.unwrap();
    destination.shutdown().await.unwrap();
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(destination.stats().retained_bytes(), 0);
    assert_eq!(fixture.runtime.stats().retained_bytes(), 0);
}

#[tokio::test]
async fn maintenance_preserves_activity_heartbeat_completion_workflow_wait_and_timer() {
    maintenance_workflow_movement(false).await;
}

#[tokio::test]
async fn maintenance_preserves_expired_activity_for_reclaim_and_fences_late_completion() {
    maintenance_workflow_movement(true).await;
}
