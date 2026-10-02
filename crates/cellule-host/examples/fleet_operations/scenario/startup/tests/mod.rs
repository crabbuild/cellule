use super::*;
use cellule_host::fleet::FleetJournal;
use cellule_runtime::Error;
use cellule_runtime::fleet::operations::*;
use cellule_runtime::node::NodeMode;

struct Fixture {
    root: tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    node: Arc<CellNode>,
    directory: NodeDirectory,
    layout: CellStorageLayout,
    intent: NodeIntent,
    ad: NodeAdvertisement,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let now = clock().unwrap();
    let journal = Arc::new(
        SqliteJournal::open(
            root.path().join("startup.sqlite"),
            scope(),
            FleetProfile::default(),
            now,
        )
        .await
        .unwrap(),
    );
    let intent = journal
        .register_initial_intent(&NodeIntent::initial(scope(), node_id(0), session(0)).unwrap())
        .await
        .unwrap();
    let node = build(&intent, session(0));
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("boot"),
        [3; 16],
    );
    let directory = NodeDirectory::new(
        layout.clone(),
        scope().fleet,
        Digest::from_bytes([31; 32]),
        node.application().registry().release_digest(),
    );
    let ad = advertisement(0, &node, &intent).await.unwrap();
    Fixture {
        root,
        journal,
        node,
        directory,
        layout,
        intent,
        ad,
    }
}

fn build(intent: &NodeIntent, boot: SessionId) -> Arc<CellNode> {
    let node = Arc::new(
        CellNodeBuilder::new(super::super::application::compile().unwrap())
            .with_runtime(SqlWorkerPool::new(1, 10).unwrap(), 16 << 20)
            .with_replica_host(Host::default())
            .with_session(boot)
            .with_fleet_startup_intent(intent.clone())
            .build()
            .unwrap(),
    );
    node.install_task_group(CancellationToken::new(), CancellationToken::new())
        .unwrap();
    let now = clock().unwrap();
    node.install_node_lease_for_startup(NodeLeaseGuard::new(now, now + 60_000).unwrap())
        .unwrap();
    node
}

async fn close(fixture: Fixture) {
    fixture.node.shutdown().await.unwrap();
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert_eq!(fixture.node.stats().retained_bytes(), 0);
    assert_eq!(fixture.node.stats().active_cells(), 0);
    fixture.journal.close().await.unwrap();
}

async fn maintenance(fixture: &Fixture) -> cellule_host::fleet::FleetJournalSnapshot {
    let now = clock().unwrap();
    let old = fixture.journal.load_snapshot(scope()).await.unwrap();
    let controller = fixture
        .journal
        .claim_controller(
            scope(),
            old.head().revision(),
            SessionId::from_bytes([206; 16]),
            now,
        )
        .await
        .unwrap();
    fixture
        .journal
        .compare_exchange(
            &controller,
            controller.head().controller().unwrap().epoch,
            now,
            &JournalTransition::BeginMaintenance(
                MaintenanceOperation::new(
                    OperationId::from_bytes([80; 16]).unwrap(),
                    Digest::from_bytes([81; 32]),
                    node_id(0),
                    session(0),
                    2,
                    now,
                    now + 60_000,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn fleet_boot_stays_closed_for_missing_pending_or_foreign_role_records() {
    let fixture = fixture().await;
    let spec = spec(&fixture.intent).unwrap();
    assert!(matches!(
        fixture.node.runtime().node_admission().check_new_role(),
        Err(Error::CellDraining)
    ));
    assert!(fixture.node.start().is_err());
    assert!(!fixture.node.is_ready() && !fixture.node.is_management_ready());
    assert!(
        fixture
            .node
            .confirm_fleet_startup(fixture.journal.as_ref(), spec.key().unwrap())
            .await
            .is_err()
    );
    fixture
        .journal
        .accept_enrollment(&spec, clock().unwrap())
        .await
        .unwrap();
    assert!(
        fixture
            .node
            .confirm_fleet_startup(fixture.journal.as_ref(), spec.key().unwrap())
            .await
            .is_err()
    );
    assert!(fixture.node.start().is_err());
    let source = fixture
        .journal
        .register_initial_intent(&NodeIntent::initial(scope(), node_id(1), session(1)).unwrap())
        .await
        .unwrap();
    let role = EnrollmentSpec {
        request: Digest::from_bytes([90; 32]),
        role: EnrollmentRole::Follower { log_epoch: 1 },
        source: Some(EnrollmentEndpoint {
            node: source.node(),
            session: source.session(),
            intent_revision: source.revision(),
        }),
        ..spec.clone()
    };
    let FleetEnrollmentAcceptance::New(record) = fixture
        .journal
        .accept_enrollment(&role, clock().unwrap())
        .await
        .unwrap()
    else {
        panic!("role duplicate")
    };
    fixture
        .journal
        .publish_enrollment_result(
            &record,
            EnrollmentEvent::Established(Digest::from_bytes([91; 32])),
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .node
            .confirm_fleet_startup(fixture.journal.as_ref(), role.key().unwrap())
            .await
            .is_err()
    );
    assert!(fixture.node.start().is_err());
    assert_eq!(fixture.node.state(), NodeState::Starting);
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    close(fixture).await;
}

#[tokio::test]
async fn canonical_boot_enrollment_and_lost_result_reply_open_only_the_original_boot() {
    let fixture = fixture().await;
    let now = clock().unwrap();
    let spec = spec(&fixture.intent).unwrap();
    let FleetEnrollmentAcceptance::New(record) =
        fixture.journal.accept_enrollment(&spec, now).await.unwrap()
    else {
        panic!("boot duplicate")
    };
    let observed = fixture
        .directory
        .create(fixture.ad.clone(), now)
        .await
        .unwrap();
    fixture.journal.lose_next_commit_reply();
    assert!(
        fixture
            .journal
            .publish_enrollment_result(
                &record,
                EnrollmentEvent::Established(evidence(&spec, observed.advertisement()).unwrap()),
                now
            )
            .await
            .is_err()
    );
    let independent = SqliteJournal::open(
        fixture.root.path().join("startup.sqlite"),
        scope(),
        FleetProfile::default(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    let checked = independent
        .load_boot(scope(), node_id(0), spec.key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checked.enrollment().accepted_at_ms(), now);
    let contradictory = NodeIntent::maintenance(
        scope(),
        &MaintenanceOperation::new(
            OperationId::from_bytes([80; 16]).unwrap(),
            Digest::from_bytes([81; 32]),
            node_id(0),
            session(0),
            checked.intent().revision(),
            now,
            now + 60_000,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        cellule_host::fleet::FleetBootObservation::new(contradictory, checked.enrollment().clone()),
        Err(OperationError::Conflict)
    ));
    let replay = enroll(
        &independent,
        &fixture.directory,
        &spec,
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(&replay, checked.enrollment());
    fixture
        .node
        .confirm_fleet_startup(&independent, spec.key().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        fixture.node.runtime().node_admission().check_new_role(),
        Err(Error::CellDraining)
    ));
    fixture
        .node
        .require_owned_components(["startup-probe"])
        .unwrap();
    assert!(fixture.node.start().is_err());
    assert_eq!(fixture.node.state(), NodeState::Starting);
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    fixture
        .node
        .install_owned_component("startup-probe", Arc::new(()))
        .unwrap();
    fixture.node.start().unwrap();
    assert_eq!(fixture.node.state(), NodeState::Ready);
    assert!(fixture.node.is_ready() && fixture.node.is_management_ready());
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_ok()
    );
    independent.close().await.unwrap();
    close(fixture).await;
}

#[tokio::test]
async fn lost_acceptance_reply_retains_pending_boot_without_repeating_advertisement() {
    let fixture = fixture().await;
    let spec = spec(&fixture.intent).unwrap();
    fixture.journal.lose_next_commit_reply();
    assert!(
        enroll(
            &fixture.journal,
            &fixture.directory,
            &spec,
            fixture.ad.clone(),
            clock().unwrap()
        )
        .await
        .is_err()
    );
    let original = fixture
        .journal
        .load_enrollment(scope(), spec.key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.status(), EnrollmentStatus::Pending);
    assert!(
        fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        enroll(
            &fixture.journal,
            &fixture.directory,
            &spec,
            fixture.ad.clone(),
            clock().unwrap()
        )
        .await
        .is_err()
    );
    assert_eq!(
        fixture
            .journal
            .load_enrollment(scope(), spec.key().unwrap())
            .await
            .unwrap()
            .unwrap(),
        original
    );
    assert!(
        fixture
            .node
            .confirm_fleet_startup(fixture.journal.as_ref(), spec.key().unwrap())
            .await
            .is_err()
    );
    assert!(fixture.node.start().is_err());
    close(fixture).await;
}

#[tokio::test]
async fn cordon_racing_accepted_boot_and_draining_reboot_keep_management_without_serving() {
    let fixture = fixture().await;
    let now = clock().unwrap();
    let boot_spec = spec(&fixture.intent).unwrap();
    let FleetEnrollmentAcceptance::New(record) = fixture
        .journal
        .accept_enrollment(&boot_spec, now)
        .await
        .unwrap()
    else {
        panic!("boot duplicate")
    };
    let observed = fixture
        .directory
        .create(fixture.ad.clone(), now)
        .await
        .unwrap();
    let requested = maintenance(&fixture).await;
    fixture
        .journal
        .publish_enrollment_result(
            &record,
            EnrollmentEvent::Established(evidence(&boot_spec, observed.advertisement()).unwrap()),
            clock().unwrap(),
        )
        .await
        .unwrap();
    fixture
        .node
        .confirm_fleet_startup(fixture.journal.as_ref(), boot_spec.key().unwrap())
        .await
        .unwrap();
    fixture.node.start().unwrap();
    assert_eq!(fixture.node.state(), NodeState::Maintenance);
    assert!(!fixture.node.is_ready() && fixture.node.is_management_ready());
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    fixture.node.start().unwrap();
    assert!(!fixture.node.is_ready());
    let predecessor = fixture
        .journal
        .load_boot(scope(), node_id(0), boot_spec.key().unwrap())
        .await
        .unwrap()
        .unwrap()
        .intent()
        .clone();
    fixture.node.shutdown().await.unwrap();
    fixture
        .directory
        .withdraw_after_drain(&observed, clock().unwrap())
        .await
        .unwrap();
    assert!(fixture.directory.is_retired(session(0)).await.unwrap());
    fixture
        .journal
        .publish_enrollment_result(
            &record,
            EnrollmentEvent::Retired(Digest::from_bytes([92; 32])),
            clock().unwrap(),
        )
        .await
        .unwrap();
    let new_session = SessionId::from_bytes([99; 16]);
    let reboot = build(&predecessor, new_session);
    let now = clock().unwrap();
    assert!(
        fixture
            .journal
            .compare_exchange(
                &requested,
                requested.head().controller().unwrap().epoch,
                now,
                &JournalTransition::Maintenance(MaintenanceEvent::SessionReplaced(new_session))
            )
            .await
            .is_err()
    );
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    let replaced = fixture
        .journal
        .compare_exchange(
            &current,
            current.head().controller().unwrap().epoch,
            now,
            &JournalTransition::Maintenance(MaintenanceEvent::SessionReplaced(new_session)),
        )
        .await
        .unwrap();
    let intents = fixture
        .journal
        .intents_page(replaced.registry(), None, 128)
        .await
        .unwrap();
    let intent = &intents.entries()[0];
    let ad = advertisement(0, &reboot, intent).await.unwrap();
    let record = enroll(
        &fixture.journal,
        &fixture.directory,
        &spec(intent).unwrap(),
        ad,
        now,
    )
    .await
    .unwrap();
    reboot
        .install_fleet_actions(
            scope(),
            node_id(0),
            fixture.journal.clone(),
            Arc::new(super::super::adapters::Cells {
                records: Arc::new(HashMap::new()),
                local: 0,
                root: fixture.root.path().into(),
            }),
        )
        .unwrap();
    reboot
        .confirm_fleet_startup(fixture.journal.as_ref(), record.spec().key().unwrap())
        .await
        .unwrap();
    reboot.start().unwrap();
    assert_eq!(reboot.state(), NodeState::Maintenance);
    assert!(!reboot.is_ready() && reboot.is_management_ready());
    assert!(matches!(
        reboot.runtime().node_admission().check_new_role(),
        Err(Error::CellDraining)
    ));
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    let action = current
        .head()
        .maintenance_action(MaintenanceAction::Cordon, clock().unwrap())
        .unwrap();
    let result = reboot
        .apply_fleet_action(action, clock().unwrap())
        .await
        .unwrap();
    assert!(result.committed && result.execution_error.is_none());
    assert_eq!(result.outcome.outcome, FleetOutcome::Cordoned);
    reboot.shutdown().await.unwrap();
    assert_eq!(reboot.stats().retained_bytes(), 0);
    close(fixture).await;
}

#[tokio::test]
async fn boot_cleanup_adopts_canonical_withdrawal_and_lost_retirement_reply() {
    for lost_retirement in [false, true] {
        let fixture = fixture().await;
        let boot_spec = spec(&fixture.intent).unwrap();
        let record = enroll(
            &fixture.journal,
            &fixture.directory,
            &boot_spec,
            fixture.ad.clone(),
            clock().unwrap(),
        )
        .await
        .unwrap();
        let boot = BootOwner {
            node: fixture.node.clone(),
            directory: fixture.directory.clone(),
            spec: boot_spec.clone(),
            advertisement: fixture.ad.clone(),
            guard: None,
        };
        assert!(boot.withdraw(&fixture.journal).await.is_err());
        assert_eq!(
            fixture
                .journal
                .load_enrollment(scope(), boot_spec.key().unwrap())
                .await
                .unwrap(),
            Some(record.clone())
        );
        fixture.node.shutdown().await.unwrap();
        let observed = fixture
            .directory
            .load(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        fixture
            .directory
            .withdraw_after_drain(&observed, clock().unwrap())
            .await
            .unwrap();
        assert!(fixture.directory.is_retired(session(0)).await.unwrap());
        // Simulate the caller losing the canonical withdrawal reply. The boot
        // owner must observe the tombstone instead of requiring a live ad again.
        if lost_retirement {
            let mut hash = blake3::Hasher::new();
            hash.update(b"cellule.example-fleet-boot-retirement.v1\0");
            hash.update(evidence(&boot_spec, &fixture.ad).unwrap().as_bytes());
            fixture.journal.lose_next_commit_reply();
            assert!(
                fixture
                    .journal
                    .publish_enrollment_result(
                        &record,
                        EnrollmentEvent::Retired(Digest::from_bytes(*hash.finalize().as_bytes())),
                        clock().unwrap(),
                    )
                    .await
                    .is_err()
            );
        }
        boot.withdraw(&fixture.journal).await.unwrap();
        let retired = fixture
            .journal
            .load_enrollment(scope(), boot_spec.key().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retired.status(), EnrollmentStatus::Retired);
        let different = BootOwner {
            spec: EnrollmentSpec {
                role: EnrollmentRole::Node {
                    mode: NodeMode::Draining,
                },
                ..boot_spec.clone()
            },
            node: fixture.node.clone(),
            directory: fixture.directory.clone(),
            advertisement: fixture.ad.clone(),
            guard: None,
        };
        let error = different.withdraw(&fixture.journal).await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<OperationError>(),
            Some(OperationError::Conflict)
        ));
        boot.withdraw(&fixture.journal).await.unwrap();
        assert_eq!(
            fixture
                .journal
                .load_enrollment(scope(), boot_spec.key().unwrap())
                .await
                .unwrap(),
            Some(retired)
        );
        close(fixture).await;
    }
}

#[tokio::test]
async fn delayed_boot_confirmation_cannot_replace_newer_intent_or_reopen_shutdown() {
    let fixture = fixture().await;
    let boot_spec = spec(&fixture.intent).unwrap();
    let key = boot_spec.key().unwrap();
    enroll(
        &fixture.journal,
        &fixture.directory,
        &boot_spec,
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    let (captured, resume) = fixture.journal.pause_next_boot_reply();
    let node = fixture.node.clone();
    let journal = fixture.journal.clone();
    let old = tokio::spawn(async move { node.confirm_fleet_startup(journal.as_ref(), key).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    maintenance(&fixture).await;
    fixture
        .node
        .confirm_fleet_startup(fixture.journal.as_ref(), key)
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(matches!(old.await.unwrap(), Err(Error::Fenced)));
    assert_eq!(fixture.node.state(), NodeState::Starting);
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );

    let (captured, resume) = fixture.journal.pause_next_boot_reply();
    let node = fixture.node.clone();
    let journal = fixture.journal.clone();
    let delayed =
        tokio::spawn(async move { node.confirm_fleet_startup(journal.as_ref(), key).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    fixture.node.shutdown().await.unwrap();
    resume.send(()).unwrap();
    assert!(matches!(delayed.await.unwrap(), Err(Error::CellDraining)));
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert!(!fixture.node.is_ready() && !fixture.node.is_management_ready());
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    close(fixture).await;
}

#[tokio::test]
async fn foreign_active_boot_and_unavailable_startup_journal_fail_closed() {
    let intent = NodeIntent::initial(scope(), node_id(0), session(1)).unwrap();
    assert!(matches!(
        CellNodeBuilder::new(super::super::application::compile().unwrap())
            .with_runtime(SqlWorkerPool::new(1, 10).unwrap(), 16 << 20)
            .with_replica_host(Host::default())
            .with_session(session(0))
            .with_fleet_startup_intent(intent)
            .build(),
        Err(Error::Fenced)
    ));
    let fixture = fixture().await;
    fixture.journal.close().await.unwrap();
    let error = fixture
        .node
        .confirm_fleet_startup(
            fixture.journal.as_ref(),
            spec(&fixture.intent).unwrap().key().unwrap(),
        )
        .await
        .unwrap_err();
    let Error::Facility { name, source } = error else {
        panic!("startup backend error lost")
    };
    assert_eq!(name, "fleet-enrollment-journal");
    assert!(matches!(
        source.downcast_ref::<Error>(),
        Some(Error::RuntimeClosed)
    ));
    assert!(fixture.node.start().is_err());
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    close(fixture).await;
}

#[tokio::test]
async fn fleet_reader_manager_requires_its_journal_binding_before_start() {
    let fixture = fixture().await;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("startup-readers"),
        [3; 16],
    );
    let manager = fixture
        .node
        .install_read_replicas(
            layout,
            fixture.directory.clone(),
            fixture.root.path().join("readers"),
            Limits::default(),
        )
        .unwrap();
    let boot_spec = spec(&fixture.intent).unwrap();
    enroll(
        &fixture.journal,
        &fixture.directory,
        &boot_spec,
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    fixture
        .node
        .confirm_fleet_startup(fixture.journal.as_ref(), boot_spec.key().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        fixture.node.start(),
        Err(Error::Control("CellNode required component is missing"))
    ));
    assert_eq!(fixture.node.state(), NodeState::Starting);
    assert!(matches!(
        fixture.node.runtime().node_admission().check_new_role(),
        Err(Error::CellDraining)
    ));
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        scope().application,
        super::super::application::NAMESPACE,
        &[1],
    )
    .unwrap();
    assert!(matches!(
        manager.activate(target, session(1)).await,
        Err(Error::Control("fleet reader enrollment is not installed"))
    ));
    fixture
        .node
        .install_fleet_reader_enrollment(scope(), node_id(0), fixture.journal.clone())
        .unwrap();
    fixture.node.start().unwrap();
    assert!(fixture.node.is_ready());
    close(fixture).await;
}

#[tokio::test]
async fn missing_reader_enrollment_does_not_prevent_joined_startup_shutdown() {
    let fixture = fixture().await;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("startup-readers"),
        [3; 16],
    );
    fixture
        .node
        .install_read_replicas(
            layout,
            fixture.directory.clone(),
            fixture.root.path().join("readers"),
            Limits::default(),
        )
        .unwrap();
    close(fixture).await;
}

async fn start_enrolled_boot(fixture: &Fixture) -> EnrollmentRecord {
    let original = enroll(
        &fixture.journal,
        &fixture.directory,
        &spec(&fixture.intent).unwrap(),
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    fixture
        .node
        .confirm_fleet_startup(fixture.journal.as_ref(), original.spec().key().unwrap())
        .await
        .unwrap();
    fixture.node.start().unwrap();
    original
}

#[tokio::test]
async fn live_intent_refresh_closes_new_roles_without_cordon_rpc_or_boot_restamping() {
    let fixture = fixture().await;
    let original = start_enrolled_boot(&fixture).await;
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_ok()
    );
    let snapshot = maintenance(&fixture).await;
    let current = fixture
        .node
        .refresh_fleet_intent(
            fixture.journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
        .unwrap();
    assert_eq!(current, snapshot.head().node_intent().unwrap().unwrap());
    assert_eq!(current.mode(), NodeMode::Draining);
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    // Cordon preserves routing to existing owners and management. Terminal
    // shutdown has not begun and no role settlement is claimed by this read.
    assert_eq!(fixture.node.state(), NodeState::Ready);
    assert!(fixture.node.is_ready() && fixture.node.is_management_ready());
    let observed = fixture
        .journal
        .load_boot(scope(), node_id(0), original.spec().key().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.enrollment(), &original);
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        fixture
            .node
            .refresh_fleet_intent(
                fixture.journal.as_ref(),
                (Instant::now() + Duration::from_secs(3)).into_std(),
            )
            .await
            .unwrap(),
        current
    );
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    close(fixture).await;
}

#[tokio::test]
async fn delayed_live_intent_reply_cannot_regress_a_newer_checked_intent() {
    let fixture = fixture().await;
    start_enrolled_boot(&fixture).await;
    let (captured, resume) = fixture.journal.pause_next_boot_reply();
    let node = fixture.node.clone();
    let journal = fixture.journal.clone();
    let old = tokio::spawn(async move {
        node.refresh_fleet_intent(
            journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    maintenance(&fixture).await;
    let current = fixture
        .node
        .refresh_fleet_intent(
            fixture.journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(matches!(old.await.unwrap(), Err(Error::Fenced)));
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    assert_eq!(
        fixture
            .node
            .refresh_fleet_intent(
                fixture.journal.as_ref(),
                (Instant::now() + Duration::from_secs(3)).into_std(),
            )
            .await
            .unwrap(),
        current
    );
    close(fixture).await;
}

#[tokio::test]
async fn delayed_live_intent_reply_cannot_reopen_joined_shutdown() {
    let fixture = fixture().await;
    start_enrolled_boot(&fixture).await;
    let (captured, resume) = fixture.journal.pause_next_boot_reply();
    let node = fixture.node.clone();
    let journal = fixture.journal.clone();
    let old = tokio::spawn(async move {
        node.refresh_fleet_intent(
            journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    fixture.node.shutdown().await.unwrap();
    resume.send(()).unwrap();
    assert!(matches!(old.await.unwrap(), Err(Error::CellDraining)));
    assert_eq!(fixture.node.state(), NodeState::Stopped);
    assert!(!fixture.node.is_ready() && !fixture.node.is_management_ready());
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    close(fixture).await;
}

#[tokio::test]
async fn live_intent_deadline_and_journal_error_preserve_original_state_and_source() {
    let fixture = fixture().await;
    let original = start_enrolled_boot(&fixture).await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert!(matches!(
        fixture
            .node
            .refresh_fleet_intent(fixture.journal.as_ref(), Instant::now().into_std(),)
            .await,
        Err(Error::Deadline)
    ));
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    let (captured, resume) = fixture.journal.pause_next_boot_reply();
    let node = fixture.node.clone();
    let journal = fixture.journal.clone();
    let expired = tokio::spawn(async move {
        node.refresh_fleet_intent(
            journal.as_ref(),
            (Instant::now() + Duration::from_secs(1)).into_std(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    maintenance(&fixture).await;
    assert!(matches!(expired.await.unwrap(), Err(Error::Deadline)));
    assert!(resume.send(()).is_err());
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Active
    );
    assert_eq!(
        fixture
            .node
            .refresh_fleet_intent(
                fixture.journal.as_ref(),
                (Instant::now() + Duration::from_secs(3)).into_std(),
            )
            .await
            .unwrap()
            .mode(),
        NodeMode::Draining
    );
    assert_eq!(
        fixture
            .journal
            .load_enrollment(scope(), original.spec().key().unwrap())
            .await
            .unwrap(),
        Some(original)
    );
    fixture.journal.close().await.unwrap();
    let error = fixture
        .node
        .refresh_fleet_intent(
            fixture.journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
        .unwrap_err();
    let Error::Facility { name, source } = error else {
        panic!("live intent backend source lost")
    };
    assert_eq!(name, "fleet-enrollment-journal");
    assert!(matches!(
        source.downcast_ref::<Error>(),
        Some(Error::RuntimeClosed)
    ));
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Draining
    );
    close(fixture).await;
}

#[tokio::test]
async fn cancelled_live_intent_read_and_active_reply_cannot_clear_local_cordon() {
    let fixture = fixture().await;
    start_enrolled_boot(&fixture).await;
    let (captured, resume) = fixture.journal.pause_next_boot_reply();
    let node = fixture.node.clone();
    let journal = fixture.journal.clone();
    let cancelled = tokio::spawn(async move {
        node.refresh_fleet_intent(
            journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(resume.send(()).is_err());
    fixture.node.runtime().node_admission().cordon().unwrap();
    let observed = fixture
        .node
        .refresh_fleet_intent(
            fixture.journal.as_ref(),
            (Instant::now() + Duration::from_secs(3)).into_std(),
        )
        .await
        .unwrap();
    assert_eq!(observed.mode(), NodeMode::Active);
    assert_eq!(
        fixture.node.runtime().node_admission().mode().unwrap(),
        NodeMode::Cordoned
    );
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    close(fixture).await;
}

#[tokio::test]
async fn confirmed_boot_cannot_be_replaced_by_another_established_request() {
    let fixture = fixture().await;
    let original_spec = spec(&fixture.intent).unwrap();
    let original = enroll(
        &fixture.journal,
        &fixture.directory,
        &original_spec,
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    fixture
        .node
        .confirm_fleet_startup(fixture.journal.as_ref(), original_spec.key().unwrap())
        .await
        .unwrap();
    let other_spec = EnrollmentSpec {
        request: Digest::from_bytes([89; 32]),
        ..original_spec.clone()
    };
    let other = enroll(
        &fixture.journal,
        &fixture.directory,
        &other_spec,
        fixture.ad.clone(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    assert_ne!(other, original);
    assert!(matches!(
        fixture
            .node
            .confirm_fleet_startup(fixture.journal.as_ref(), other_spec.key().unwrap(),)
            .await,
        Err(Error::Fenced)
    ));
    assert!(
        fixture
            .node
            .runtime()
            .node_admission()
            .check_new_role()
            .is_err()
    );
    fixture.node.start().unwrap();
    assert_eq!(
        fixture
            .node
            .refresh_fleet_intent(
                fixture.journal.as_ref(),
                (Instant::now() + Duration::from_secs(3)).into_std(),
            )
            .await
            .unwrap(),
        fixture.intent
    );
    assert_eq!(
        fixture
            .journal
            .load_enrollment(scope(), original_spec.key().unwrap())
            .await
            .unwrap(),
        Some(original)
    );
    close(fixture).await;
}

mod withdrawal;
