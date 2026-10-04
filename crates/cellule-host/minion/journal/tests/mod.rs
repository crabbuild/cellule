use super::*;

mod maintenance_enrollments;
use cellule_runtime::control::{Control, ControlState, Owner, RootRef};
use cellule_runtime::identity::{ApplicationId, CellTarget, IncarnationId, NamespaceId, TenantId};

fn scope() -> FleetScope {
    FleetScope {
        fleet: Digest::from_bytes([200; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    }
}
fn endpoint(n: u8) -> EnrollmentEndpoint {
    EnrollmentEndpoint {
        node: NodeId::from_bytes([n; 16]),
        session: SessionId::from_bytes([n + 10; 16]),
        intent_revision: 1,
    }
}
fn intent(n: u8) -> NodeIntent {
    NodeIntent::initial(scope(), endpoint(n).node, endpoint(n).session).unwrap()
}
fn spec(n: u64) -> MoveAttemptSpec {
    MoveAttemptSpec {
        id: AttemptId {
            operation: OperationId::from_bytes([1; 16]).unwrap(),
            sequence: n,
        },
        target: CellTarget::new(
            TenantId::from_bytes([4; 16]),
            scope().application,
            NamespaceId::from_bytes([5; 16]),
            &n.to_be_bytes(),
        )
        .unwrap(),
        incarnation: IncarnationId::from_bytes([6; 16]),
        source_node: endpoint(1).node,
        source: endpoint(1).session,
        generation: 7,
        source_epoch: 4,
        destination_node: endpoint(2).node,
        destination: endpoint(2).session,
        cost: TransferCost {
            memory_bytes: 65536,
            disk_bytes: 4096,
            file_descriptors: 8,
            job_credits: 1,
        },
        snapshot_digest: Digest::from_bytes([7; 32]),
        deadline_ms: 20_000,
    }
}
fn position() -> PublishedPosition {
    PublishedPosition {
        incarnation: spec(1).incarnation,
        epoch: 4,
        root: RootRef {
            digest: Digest::from_bytes([8; 32]),
            txid: 9,
            checksum: u64::MAX,
            commit_sequence: 9,
        },
    }
}
fn control(idle: bool) -> Control {
    let mut control = Control::initial(
        spec(1).target.cell_id(),
        spec(1).incarnation,
        Owner {
            session: spec(1).source,
            endpoint: "https://source.internal:8789".into(),
        },
        Digest::from_bytes([9; 32]),
        1,
    )
    .unwrap();
    control.epoch = position().epoch;
    control.revision = 9;
    control.progress = 9;
    control.root = Some(position().root);
    control.state = if idle {
        ControlState::Idle
    } else {
        ControlState::Serving
    };
    if idle {
        control.owner = None;
    }
    control
}
fn request(node: u8, id: u8) -> MaintenanceOperation {
    MaintenanceOperation::new(
        OperationId::from_bytes([id; 16]).unwrap(),
        Digest::from_bytes([id + 30; 32]),
        endpoint(node).node,
        endpoint(node).session,
        2,
        0,
        20_000,
    )
    .unwrap()
}
fn enrollment(id: u8, target: u8) -> EnrollmentSpec {
    EnrollmentSpec {
        scope: scope(),
        request: Digest::from_bytes([id; 32]),
        role: EnrollmentRole::Follower { log_epoch: 7 },
        source: Some(endpoint(3)),
        target: endpoint(target),
    }
}

// Journal reducer evidence only: these tests exercise transaction ordering,
// not the host observer's authority to certify a drained process.
fn drain_proof(node: u8) -> DrainEvidence {
    DrainEvidence {
        node: endpoint(node).node,
        session: endpoint(node).session,
        remaining_cells: 0,
        unresolved_attempts: 0,
        relocated: true,
        readers_settled: true,
        followers_settled: true,
        facilities_closed: true,
        stopped: true,
        withdrawn: true,
    }
}

#[tokio::test]
async fn closing_source_fences_new_roles_and_preserves_original_completion_after_reconstruction() {
    for role in [
        EnrollmentRole::Follower { log_epoch: 7 },
        EnrollmentRole::Reader {
            target: spec(1).target,
            position: position(),
        },
    ] {
        let fixture = Fixture::new().await;
        fixture
            .transition(JournalTransition::BeginMaintenance(request(3, 3)))
            .await;
        fixture
            .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned))
            .await;
        fixture
            .transition(JournalTransition::Maintenance(
                MaintenanceEvent::BeginEvacuation,
            ))
            .await;
        let mut original = enrollment(60, 1);
        original.role = role;
        original.source.as_mut().unwrap().intent_revision = 2;
        let accepted = match fixture
            .journal
            .accept_enrollment(&original, 1)
            .await
            .unwrap()
        {
            FleetEnrollmentAcceptance::New(record) => record,
            _ => panic!("first acceptance expected"),
        };
        fixture
            .transition(JournalTransition::Maintenance(
                MaintenanceEvent::ReadyToClose(drain_proof(3)),
            ))
            .await;
        fixture.journal.close().await.unwrap();
        let restarted = fixture.client().await;
        let before = restarted.load_snapshot(scope()).await.unwrap();
        let fresh = EnrollmentSpec {
            request: Digest::from_bytes([61; 32]),
            ..original.clone()
        };
        assert!(restarted.accept_enrollment(&fresh, 2).await.is_err());
        assert_eq!(restarted.load_snapshot(scope()).await.unwrap(), before);
        assert_eq!(
            restarted
                .load_enrollment(scope(), fresh.key().unwrap())
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            restarted.accept_enrollment(&original, 2).await.unwrap(),
            FleetEnrollmentAcceptance::Existing(record) if record == accepted
        ));
        let established = restarted
            .publish_enrollment_result(
                &accepted,
                EnrollmentEvent::Established(Digest::from_bytes([62; 32])),
                3,
            )
            .await
            .unwrap();
        let retired = restarted
            .publish_enrollment_result(
                &established,
                EnrollmentEvent::Retired(Digest::from_bytes([63; 32])),
                4,
            )
            .await
            .unwrap();
        assert_eq!(retired.accepted_at_ms(), accepted.accepted_at_ms());
        assert!(!retired.unresolved());
        restarted.close().await.unwrap();
        let resumed = fixture.client().await;
        let current = resumed.load_snapshot(scope()).await.unwrap();
        let completed = resumed
            .compare_exchange(
                &current,
                1,
                4,
                &JournalTransition::Maintenance(MaintenanceEvent::Stopped(drain_proof(3))),
            )
            .await
            .unwrap();
        // The current head may now belong to a different node. First acceptance
        // must check the source's retained operation, not the head's operation.
        let later = resumed
            .compare_exchange(
                &completed,
                1,
                4,
                &JournalTransition::BeginMaintenance(request(2, 4)),
            )
            .await
            .unwrap();
        assert!(resumed.accept_enrollment(&fresh, 5).await.is_err());
        assert_eq!(resumed.load_snapshot(scope()).await.unwrap(), later);
        assert!(matches!(
            resumed.accept_enrollment(&original, 5).await.unwrap(),
            FleetEnrollmentAcceptance::Existing(record) if record == retired
        ));
        resumed.close().await.unwrap();
    }
}

#[tokio::test]
async fn delayed_source_acceptance_checks_closing_in_its_original_transaction() {
    let fixture = Fixture::new().await;
    fixture
        .transition(JournalTransition::BeginMaintenance(request(3, 3)))
        .await;
    fixture
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned))
        .await;
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::BeginEvacuation,
        ))
        .await;
    let independent = fixture.client().await;
    let mut spec = enrollment(64, 1);
    spec.source.as_mut().unwrap().intent_revision = 2;
    let (paused, resume) = independent.pause_before_enrollment_acceptance();
    let original = spec.clone();
    let client = independent.clone();
    let task = tokio::spawn(async move { client.accept_enrollment(&original, 1).await });
    paused.await.unwrap();
    let closed = fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::ReadyToClose(drain_proof(3)),
        ))
        .await;
    resume.send(()).unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        closed
    );
    assert_eq!(
        independent
            .load_enrollment(scope(), spec.key().unwrap())
            .await
            .unwrap(),
        None
    );
    independent.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn source_acceptance_and_closing_have_one_committed_registry_barrier() {
    for reader in [false, true] {
        let fixture = Fixture::new().await;
        fixture
            .transition(JournalTransition::BeginMaintenance(request(3, 3)))
            .await;
        fixture
            .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned))
            .await;
        let expected = fixture
            .transition(JournalTransition::Maintenance(
                MaintenanceEvent::BeginEvacuation,
            ))
            .await;
        let independent = fixture.client().await;
        let mut enrollment_spec = enrollment(65, 1);
        enrollment_spec.source.as_mut().unwrap().intent_revision = 2;
        if reader {
            enrollment_spec.role = EnrollmentRole::Reader {
                target: spec(1).target,
                position: position(),
            };
        }
        let transition =
            JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(drain_proof(3)));
        let (accepted, closing) = tokio::join!(
            independent.accept_enrollment(&enrollment_spec, 1),
            fixture
                .journal
                .compare_exchange(&expected, 1, 1, &transition),
        );
        assert_ne!(accepted.is_ok(), closing.is_ok());
        let current = fixture.journal.load_snapshot(scope()).await.unwrap();
        match accepted {
            Ok(FleetEnrollmentAcceptance::New(original)) => {
                assert!(matches!(
                    closing.unwrap_err().downcast_ref::<OperationError>(),
                    Some(OperationError::Conflict)
                ));
                assert_eq!(current.head(), expected.head());
                assert_eq!(
                    current.registry().revision(),
                    expected.registry().revision() + 1
                );
                assert_eq!(
                    independent
                        .load_enrollment(scope(), enrollment_spec.key().unwrap())
                        .await
                        .unwrap(),
                    Some(original)
                );
            }
            Err(error) => {
                assert!(matches!(
                    error.downcast_ref::<OperationError>(),
                    Some(OperationError::Conflict)
                ));
                assert_eq!(current, closing.unwrap());
                assert_eq!(current.registry(), expected.registry());
                assert_eq!(
                    independent
                        .load_enrollment(scope(), enrollment_spec.key().unwrap())
                        .await
                        .unwrap(),
                    None
                );
            }
            _ => panic!("first acceptance expected"),
        }
        independent.close().await.unwrap();
        fixture.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn source_operation_lookup_preserves_missing_and_codec_errors_without_new_rows() {
    for missing in [true, false] {
        let fixture = Fixture::new().await;
        fixture
            .transition(JournalTransition::BeginMaintenance(request(3, 3)))
            .await;
        let mut spec = enrollment(66, 1);
        spec.source.as_mut().unwrap().intent_revision = 2;
        let original = fixture.journal.accept_enrollment(&spec, 1).await.unwrap();
        let version = fixture
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .registry();
        fixture
            .journal
            .run(move |db| {
                if missing {
                    db.tx.execute("DELETE FROM operations", [])?;
                } else {
                    db.tx
                        .execute("UPDATE operations SET body=?1", [vec![0_u8; 1]])?;
                }
                Ok(())
            })
            .await
            .unwrap();
        let fresh = EnrollmentSpec {
            request: Digest::from_bytes([67; 32]),
            ..spec.clone()
        };
        let error = match fixture.journal.accept_enrollment(&fresh, 2).await {
            Err(error) => error,
            Ok(_) => panic!("unavailable source operation admitted new work"),
        };
        if missing {
            assert!(matches!(
                error.downcast_ref::<OperationError>(),
                Some(OperationError::NotFound)
            ));
        } else {
            assert!(matches!(
                error.downcast_ref::<OperationError>(),
                Some(OperationError::Codec(_))
            ));
            assert!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<cellule_runtime::codec::CodecError>()
                    .is_some()
            );
        }
        assert_eq!(
            fixture
                .journal
                .load_enrollment(scope(), fresh.key().unwrap())
                .await
                .unwrap(),
            None
        );
        fixture
            .journal
            .run(move |db| {
                // The deliberately damaged operation makes a full snapshot
                // unavailable. Inspect only the retained registry bytes here.
                let bytes =
                    db.tx
                        .query_row("SELECT registry FROM state WHERE singleton=1", [], |row| {
                            blob(row, 0, MAX_RECORD_BYTES)
                        })?;
                assert_eq!(RegistryVersion::from_bytes(&bytes)?, version);
                Ok(())
            })
            .await
            .unwrap();
        // Full replay precedes the unavailable current operation and preserves
        // the immutable acceptance, including its original timestamp.
        let replay = fixture.journal.accept_enrollment(&spec, 3).await.unwrap();
        let FleetEnrollmentAcceptance::New(original) = original else {
            panic!("new expected")
        };
        assert!(
            matches!(replay, FleetEnrollmentAcceptance::Existing(record) if record == original)
        );
        fixture.journal.close().await.unwrap();
    }
}
fn result(
    accepted: &AcceptedFleetAction,
    outcome: FleetOutcome,
    now_ms: i64,
) -> FleetActionOutcome {
    FleetActionOutcome {
        scope: scope(),
        action_key: accepted.action().key().unwrap(),
        node: accepted.node(),
        session: accepted.session(),
        observed_at_ms: now_ms,
        outcome,
    }
}
fn lose(journal: &SqliteJournal) {
    journal
        .inner
        .lose_commit_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
}

struct Fixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
    journal: SqliteJournal,
}

#[tokio::test]
async fn atomic_absence_resolution_rejects_accepted_release_and_preserves_original() {
    let fixture = Fixture::new().await;
    let delayed = fixture.releasing().await;
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    let resolved = fixture
        .journal
        .compare_exchange(
            &current,
            1,
            1,
            &JournalTransition::ResolveUnaccepted {
                id: spec(1).id,
                effect: MovementAction::Release,
            },
        )
        .await
        .unwrap();
    assert_eq!(resolved.head().revision(), current.head().revision() + 1);
    assert_eq!(resolved.head().reserved_restore_bytes(), 4096);
    assert!(
        fixture
            .journal
            .accept_action(&delayed, spec(1).source_node, spec(1).source, 1)
            .await
            .is_err()
    );
    let action = resolved
        .head()
        .movement_action(spec(1).id, MovementAction::Release, 1)
        .unwrap();
    let accepted = fixture
        .journal
        .accept_action(&action, spec(1).source_node, spec(1).source, 1)
        .await
        .unwrap();
    assert!(matches!(accepted, FleetActionAcceptance::New(_)));
    let error = fixture
        .journal
        .compare_exchange(
            &resolved,
            1,
            1,
            &JournalTransition::ResolveUnaccepted {
                id: spec(1).id,
                effect: MovementAction::Release,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<OperationError>(),
        Some(OperationError::Busy)
    ));
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        resolved
    );
    assert!(matches!(
        fixture
            .journal
            .load_movement_action(
                scope(),
                spec(1).id,
                MovementAction::Release,
                spec(1).source_node,
                spec(1).source
            )
            .await
            .unwrap(),
        Some(FleetActionAcceptance::Existing { result: None, .. })
    ));
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn independent_acceptance_and_absence_cas_have_one_linearized_winner() {
    let fixture = Fixture::new().await;
    let action = fixture.releasing().await;
    let other = fixture.client().await;
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    let transition = JournalTransition::ResolveUnaccepted {
        id: spec(1).id,
        effect: MovementAction::Release,
    };
    let (accepted, resolved) = tokio::join!(
        fixture
            .journal
            .accept_action(&action, spec(1).source_node, spec(1).source, 1),
        other.compare_exchange(&current, 1, 1, &transition)
    );
    match (accepted, resolved) {
        (Ok(FleetActionAcceptance::New(_)), Err(error)) => {
            assert!(matches!(
                error.downcast_ref::<OperationError>(),
                Some(OperationError::Busy)
            ));
            assert_eq!(
                fixture.journal.load_snapshot(scope()).await.unwrap(),
                current
            );
        }
        (Err(_), Ok(resolved)) => {
            assert_eq!(resolved.head().revision(), current.head().revision() + 1);
            assert!(
                fixture
                    .journal
                    .load_movement_action(
                        scope(),
                        spec(1).id,
                        MovementAction::Release,
                        spec(1).source_node,
                        spec(1).source
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        _ => panic!("acceptance/absence race did not linearize"),
    }
    assert_eq!(
        other
            .load_snapshot(scope())
            .await
            .unwrap()
            .head()
            .reserved_restore_bytes(),
        4096
    );
    other.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn lost_absence_cas_reply_survives_reconstruction_and_fences_delayed_envelope() {
    let fixture = Fixture::new().await;
    let delayed = fixture.releasing().await;
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .compare_exchange(
                &current,
                1,
                1,
                &JournalTransition::ResolveUnaccepted {
                    id: spec(1).id,
                    effect: MovementAction::Release
                }
            )
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
    let reopened = fixture.client().await;
    let resolved = reopened.load_snapshot(scope()).await.unwrap();
    assert_eq!(resolved.head().revision(), current.head().revision() + 1);
    assert_eq!(resolved.head().reserved_restore_bytes(), 4096);
    assert!(
        reopened
            .accept_action(&delayed, spec(1).source_node, spec(1).source, 1)
            .await
            .is_err()
    );
    let fresh = resolved
        .head()
        .movement_action(spec(1).id, MovementAction::Release, 1)
        .unwrap();
    assert_eq!(fresh.key().unwrap(), delayed.key().unwrap());
    assert!(matches!(
        reopened
            .accept_action(&fresh, spec(1).source_node, spec(1).source, 1)
            .await
            .unwrap(),
        FleetActionAcceptance::New(_)
    ));
    reopened.close().await.unwrap();
}
impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fleet.sqlite");
        let journal = SqliteJournal::open(path.clone(), scope(), FleetProfile::default(), 0)
            .await
            .unwrap();
        for n in 1..=3 {
            journal.register_initial_intent(&intent(n)).await.unwrap();
        }
        let registry = journal.load_snapshot(scope()).await.unwrap().registry();
        let registry = journal.bootstrap_registry(registry).await.unwrap();
        journal.set_scheduling(registry, true).await.unwrap();
        journal
            .claim_controller(scope(), 0, SessionId::from_bytes([206; 16]), 0)
            .await
            .unwrap();
        Self {
            _directory: directory,
            path,
            journal,
        }
    }
    async fn client(&self) -> SqliteJournal {
        SqliteJournal::open(self.path.clone(), scope(), FleetProfile::default(), 0)
            .await
            .unwrap()
    }
    async fn client_error(&self) -> JournalError {
        match SqliteJournal::open(self.path.clone(), scope(), FleetProfile::default(), 0).await {
            Err(error) => error,
            Ok(journal) => {
                journal.close().await.unwrap();
                panic!("corrupt journal unexpectedly reopened")
            }
        }
    }
    async fn transition(&self, transition: JournalTransition) -> FleetJournalSnapshot {
        let current = self.journal.load_snapshot(scope()).await.unwrap();
        self.journal
            .compare_exchange(
                &current,
                current.head().controller().unwrap().epoch,
                0,
                &transition,
            )
            .await
            .unwrap()
    }
    async fn event(&self, event: AttemptEvent) -> FleetJournalSnapshot {
        self.transition(JournalTransition::Attempt {
            id: spec(1).id,
            event,
        })
        .await
    }
    async fn preparing(&self) -> FleetAction {
        self.transition(JournalTransition::Allocate(spec(1))).await;
        self.event(AttemptEvent::BeginPrepare)
            .await
            .head()
            .movement_action(spec(1).id, MovementAction::Prepare, 0)
            .unwrap()
    }
    async fn releasing(&self) -> FleetAction {
        self.preparing().await;
        self.event(AttemptEvent::Reserved(ReceiverReservation {
            session: spec(1).destination,
            expires_at_ms: 20_000,
        }))
        .await;
        self.event(AttemptEvent::BeginRelease)
            .await
            .head()
            .movement_action(spec(1).id, MovementAction::Release, 0)
            .unwrap()
    }
    async fn activating(&self) -> AcceptedFleetAction {
        self.releasing().await;
        self.event(AttemptEvent::Released(position())).await;
        let head = self.event(AttemptEvent::BeginActivate).await;
        let action = head
            .head()
            .movement_action(spec(1).id, MovementAction::Activate, 0)
            .unwrap();
        match self
            .journal
            .accept_action(&action, spec(1).destination_node, spec(1).destination, 0)
            .await
            .unwrap()
        {
            FleetActionAcceptance::New(accepted) => accepted,
            _ => panic!("not first acceptance"),
        }
    }
}

#[tokio::test]
async fn reconstruction_preserves_scope_profile_header_and_stopped_policy() {
    let fixture = Fixture::new().await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let stopped = fixture
        .journal
        .set_scheduling(before.registry(), false)
        .await
        .unwrap();
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let loaded = restarted.load_snapshot(scope()).await.unwrap();
    assert_eq!(loaded.head(), before.head());
    assert_eq!(loaded.registry(), stopped);
    let foreign = FleetScope {
        fleet: Digest::from_bytes([199; 32]),
        ..scope()
    };
    assert!(
        SqliteJournal::open(fixture.path.clone(), foreign, FleetProfile::default(), 0)
            .await
            .is_err()
    );
    assert!(
        SqliteJournal::open(
            fixture.path.clone(),
            scope(),
            FleetProfile {
                max_inflight: 1,
                ..FleetProfile::default()
            },
            0
        )
        .await
        .is_err()
    );
    assert!(restarted.load_snapshot(foreign).await.is_err());
    restarted.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independently_opened_clients_linearize_competing_controller_and_allocation_cas() {
    let fixture = Fixture::new().await;
    let other = fixture.client().await;
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    let (a, b) = tokio::join!(
        fixture.journal.claim_controller(
            scope(),
            current.head().revision(),
            SessionId::from_bytes([207; 16]),
            30_000
        ),
        other.claim_controller(
            scope(),
            current.head().revision(),
            SessionId::from_bytes([208; 16]),
            30_000
        )
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(current.head().controller().unwrap().epoch, 2);
    let mut a_spec = spec(1);
    a_spec.deadline_ms = 50_000;
    let mut b_spec = a_spec.clone();
    b_spec.target = spec(2).target;
    let a = JournalTransition::Allocate(a_spec);
    let b = JournalTransition::Allocate(b_spec);
    let (a, b) = tokio::join!(
        fixture.journal.compare_exchange(&current, 2, 30_000, &a),
        other.compare_exchange(&current, 2, 30_000, &b)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let loaded = other.load_snapshot(scope()).await.unwrap();
    assert_eq!(loaded.head().attempts().len(), 1);
    assert_eq!(loaded.head().reserved_restore_bytes(), 4096);
    assert!(
        other
            .compare_exchange(
                &loaded,
                1,
                30_000,
                &JournalTransition::Attempt {
                    id: spec(1).id,
                    event: AttemptEvent::BeginPrepare
                }
            )
            .await
            .is_err()
    );
    other.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn expired_controller_and_lost_allocation_reply_preserve_both_permits_after_restart() {
    let fixture = Fixture::new().await;
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .compare_exchange(&current, 1, 0, &JournalTransition::Allocate(spec(1)))
            .await
            .is_err()
    );
    fixture.event(AttemptEvent::BeginPrepare).await;
    fixture.event(AttemptEvent::OutcomeUnknown).await;
    fixture
        .transition(JournalTransition::Allocate(spec(2)))
        .await;
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let current = restarted.load_snapshot(scope()).await.unwrap();
    let current = restarted
        .claim_controller(
            scope(),
            current.head().revision(),
            SessionId::from_bytes([209; 16]),
            30_000,
        )
        .await
        .unwrap();
    assert_eq!(current.head().attempts().len(), 2);
    assert_eq!(current.head().reserved_restore_bytes(), 8192);
    let mut third = spec(3);
    third.deadline_ms = 50_000;
    assert!(matches!(
        restarted
            .compare_exchange(&current, 2, 30_000, &JournalTransition::Allocate(third))
            .await
            .err()
            .unwrap()
            .downcast_ref::<OperationError>(),
        Some(OperationError::Budget)
    ));
    let stopped = restarted
        .set_scheduling(current.registry(), false)
        .await
        .unwrap();
    let current = restarted.load_snapshot(scope()).await.unwrap();
    assert_eq!(current.registry(), stopped);
    assert_eq!(current.head().reserved_restore_bytes(), 8192);
    assert!(matches!(
        restarted
            .compare_exchange(&current, 2, 30_000, &JournalTransition::Allocate(spec(3)))
            .await
            .err()
            .unwrap()
            .downcast_ref::<OperationError>(),
        Some(OperationError::Stopped)
    ));
    restarted.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_first_action_acceptance_is_one_effect_and_one_durable_original() {
    let fixture = Fixture::new().await;
    let other = fixture.client().await;
    let action = fixture.preparing().await;
    let (a, b) = tokio::join!(
        fixture
            .journal
            .accept_action(&action, spec(1).destination_node, spec(1).destination, 0),
        other.accept_action(&action, spec(1).destination_node, spec(1).destination, 0)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    let original = match (a, b) {
        (
            FleetActionAcceptance::New(a),
            FleetActionAcceptance::Existing {
                accepted: b,
                result: None,
            },
        )
        | (
            FleetActionAcceptance::Existing {
                accepted: a,
                result: None,
            },
            FleetActionAcceptance::New(b),
        ) => {
            assert_eq!(a, b);
            a
        }
        _ => panic!("not exactly one first acceptance"),
    };
    let reserved = result(
        &original,
        FleetOutcome::Reserved(ReceiverReservation {
            session: spec(1).destination,
            expires_at_ms: 20_000,
        }),
        1,
    );
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .publish_action_result(&original, &reserved)
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    match restarted
        .accept_action(
            &action,
            spec(1).destination_node,
            spec(1).destination,
            50_000,
        )
        .await
        .unwrap()
    {
        FleetActionAcceptance::Existing {
            accepted,
            result: Some(result),
        } => {
            assert_eq!(accepted, original);
            assert_eq!(*result, reserved);
        }
        _ => panic!("lost accepted result"),
    }
    let changed = result(
        &original,
        FleetOutcome::Blocked(DrainBlocker::ReceiverCapacity),
        2,
    );
    assert!(
        restarted
            .publish_action_result(&original, &changed)
            .await
            .is_err()
    );
    other.close().await.unwrap();
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn source_result_and_endpoint_specific_inspections_survive_controller_reconstruction() {
    let fixture = Fixture::new().await;
    let action = fixture.releasing().await;
    let original = match fixture
        .journal
        .accept_action(&action, spec(1).source_node, spec(1).source, 0)
        .await
        .unwrap()
    {
        FleetActionAcceptance::New(original) => original,
        _ => panic!("not new"),
    };
    let released = result(&original, FleetOutcome::Released(position()), 1);
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .publish_action_result(&original, &released)
            .await
            .is_err()
    );
    let current = fixture.event(AttemptEvent::OutcomeUnknown).await;
    let inspection = current
        .head()
        .movement_action(spec(1).id, MovementAction::Inspect, 0)
        .unwrap();
    assert!(matches!(
        fixture
            .journal
            .accept_action(&inspection, spec(1).source_node, spec(1).source, 0)
            .await
            .unwrap(),
        FleetActionAcceptance::New(_)
    ));
    assert!(matches!(
        fixture
            .journal
            .accept_action(
                &inspection,
                spec(1).destination_node,
                spec(1).destination,
                0
            )
            .await
            .unwrap(),
        FleetActionAcceptance::New(_)
    ));
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    match restarted
        .load_movement_action(
            scope(),
            spec(1).id,
            MovementAction::Release,
            spec(1).source_node,
            spec(1).source,
        )
        .await
        .unwrap()
        .unwrap()
    {
        FleetActionAcceptance::Existing {
            accepted,
            result: Some(result),
        } => {
            assert_eq!(accepted, original);
            assert_eq!(*result, released);
        }
        _ => panic!("lost source release"),
    }
    assert_eq!(
        restarted
            .load_snapshot(scope())
            .await
            .unwrap()
            .head()
            .reserved_restore_bytes(),
        4096
    );
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn acquisition_basis_keeps_original_input_and_time_after_lost_commit_reply() {
    let fixture = Fixture::new().await;
    let accepted = fixture.activating().await;
    let basis = AcquisitionBasis::new(accepted.clone(), control(true), 1).unwrap();
    let activated = ActivationEvidence {
        node: spec(1).destination_node,
        session: spec(1).destination,
        position: PublishedPosition {
            epoch: 5,
            ..position()
        },
    };
    let outcome = result(&accepted, FleetOutcome::Activated(activated), 3);
    assert!(
        fixture
            .journal
            .publish_action_result(&accepted, &outcome)
            .await
            .is_err()
    );
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .record_acquisition_basis(&basis)
            .await
            .is_err()
    );
    let retry = AcquisitionBasis::new(accepted.clone(), control(true), 2).unwrap();
    assert_eq!(
        fixture
            .journal
            .record_acquisition_basis(&retry)
            .await
            .unwrap(),
        basis
    );
    let mut changed = control(true);
    changed.revision += 1;
    assert!(
        fixture
            .journal
            .record_acquisition_basis(&AcquisitionBasis::new(accepted.clone(), changed, 2).unwrap())
            .await
            .is_err()
    );
    fixture
        .journal
        .publish_action_result(&accepted, &outcome)
        .await
        .unwrap();
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    assert_eq!(
        restarted.load_acquisition_basis(&accepted).await.unwrap(),
        Some(basis)
    );
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn recovery_records_reconstruct_exact_original_basis_position_and_result() {
    use cellule_runtime::node::{
        NODE_LOG_PROTOCOL_VERSION, NodeAdvertisement, NodeCapacity, NodeDirectory,
        NodeFailureDomain,
    };
    let fixture = Fixture::new().await;
    fixture.releasing().await;
    let head = fixture.event(AttemptEvent::BeginRecover).await;
    let action = head
        .head()
        .movement_action(spec(1).id, MovementAction::Recover, 0)
        .unwrap();
    let accepted = match fixture
        .journal
        .accept_action(&action, spec(1).destination_node, spec(1).destination, 0)
        .await
        .unwrap()
    {
        FleetActionAcceptance::New(a) => a,
        _ => panic!("not new"),
    };
    let layout = cellule_runtime::ltx::CellStorageLayout::new(
        cellule_store::Store::new(Arc::new(object_store::memory::InMemory::new())),
        object_store::path::Path::from("fleet-reference-recovery"),
        *scope().application.as_bytes(),
    );
    let image = Digest::from_bytes([90; 32]);
    let release = Digest::from_bytes([91; 32]);
    let key = ed25519_dalek::SigningKey::from_bytes(&[92; 32]);
    let directory = NodeDirectory::new(layout, scope().fleet, image, release);
    let signed = |n, issued, expires| {
        NodeAdvertisement::sign(
            endpoint(n).node,
            endpoint(n).session,
            "https://recovery.internal:8789".into(),
            scope().fleet,
            Digest::from_bytes([93; 32]),
            image,
            release,
            &key,
            1,
            issued,
            expires,
            vec![Digest::from_bytes([9; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 128 << 20,
                free_disk_bytes: 8 << 30,
                follower_free_bytes: 8 << 30,
                follower_retained_bytes: 0,
                job_credits: 1,
                log_protocol: NODE_LOG_PROTOCOL_VERSION,
            },
        )
        .unwrap()
    };
    directory.create(signed(1, 1, 9), 1).await.unwrap();
    directory.create(signed(2, 10, 10_010), 10).await.unwrap();
    let proof = directory
        .claim_expired_for_takeover(spec(1).source, spec(1).destination, 10)
        .await
        .unwrap();
    let basis = RecoveryBasis::new(&accepted, control(false), proof, 10).unwrap();
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .record_recovery_basis(&accepted, &basis)
            .await
            .is_err()
    );
    let retry = RecoveryBasis::new(&accepted, control(false), proof, 11).unwrap();
    assert_eq!(
        fixture
            .journal
            .record_recovery_basis(&accepted, &retry)
            .await
            .unwrap(),
        basis
    );
    let restored = basis
        .control()
        .takeover(Owner {
            session: spec(1).destination,
            endpoint: "https://receiver.internal:8789".into(),
        })
        .unwrap();
    let evidence = RecoveryEvidence::new(basis.clone(), restored.clone(), 11).unwrap();
    let recovered = RecoveredActivation {
        recovery: evidence.clone(),
        serving: ActivationEvidence {
            node: spec(1).destination_node,
            session: spec(1).destination,
            position: evidence.position().unwrap(),
        },
    };
    let outcome = result(&accepted, FleetOutcome::Recovered(Box::new(recovered)), 12);
    assert!(
        fixture
            .journal
            .publish_action_result(&accepted, &outcome)
            .await
            .is_err()
    );
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .record_recovery_evidence(&accepted, &evidence)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .journal
            .record_recovery_evidence(
                &accepted,
                &RecoveryEvidence::new(basis.clone(), restored, 12).unwrap()
            )
            .await
            .unwrap(),
        evidence
    );
    fixture
        .journal
        .publish_action_result(&accepted, &outcome)
        .await
        .unwrap();
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    assert_eq!(
        restarted.load_recovery_basis(&accepted).await.unwrap(),
        Some(basis)
    );
    assert_eq!(
        restarted.load_recovery_evidence(&accepted).await.unwrap(),
        Some(evidence)
    );
    match restarted
        .load_movement_action(
            scope(),
            spec(1).id,
            MovementAction::Recover,
            spec(1).destination_node,
            spec(1).destination,
        )
        .await
        .unwrap()
        .unwrap()
    {
        FleetActionAcceptance::Existing {
            result: Some(saved),
            ..
        } => assert_eq!(*saved, outcome),
        _ => panic!("not retained"),
    }
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn maintenance_cordon_enrollment_and_original_request_survive_lost_reply_and_reboot() {
    let fixture = Fixture::new().await;
    let spec = enrollment(40, 1);
    let admitted = match fixture.journal.accept_enrollment(&spec, 1).await.unwrap() {
        FleetEnrollmentAcceptance::New(r) => r,
        _ => panic!("not new"),
    };
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .compare_exchange(
                &current,
                1,
                0,
                &JournalTransition::BeginMaintenance(request(1, 1))
            )
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let current = restarted.load_snapshot(scope()).await.unwrap();
    let page = restarted
        .intents_page(current.registry(), None, 128)
        .await
        .unwrap();
    assert_eq!(page.entries()[0].mode(), NodeMode::Draining);
    assert_eq!(page.entries()[0].revision(), 2);
    assert!(restarted.register_initial_intent(&intent(1)).await.is_err());
    assert!(
        matches!(restarted.accept_enrollment(&spec,50_000).await.unwrap(),FleetEnrollmentAcceptance::Existing(original) if original==admitted)
    );
    let mut new = spec.clone();
    new.request = Digest::from_bytes([41; 32]);
    new.target.intent_revision = 2;
    assert!(restarted.accept_enrollment(&new, 2).await.is_err());
    let same = restarted
        .compare_exchange(
            &current,
            1,
            0,
            &JournalTransition::BeginMaintenance(request(1, 1)),
        )
        .await
        .unwrap();
    assert_eq!(same, current);
    assert!(
        restarted
            .compare_exchange(
                &current,
                99,
                0,
                &JournalTransition::BeginMaintenance(request(1, 1))
            )
            .await
            .is_err()
    );
    let deadline = restarted
        .compare_exchange(
            &current,
            1,
            1,
            &JournalTransition::Maintenance(MaintenanceEvent::ExtendDeadline(25_000)),
        )
        .await
        .unwrap();
    assert_eq!(deadline.head().maintenance().unwrap().intent_revision(), 3);
    let duplicate = restarted
        .compare_exchange(
            &deadline,
            1,
            1,
            &JournalTransition::BeginMaintenance(request(1, 1)),
        )
        .await
        .unwrap();
    assert_eq!(duplicate, deadline);
    let changed = MaintenanceOperation::new(
        request(1, 1).id(),
        Digest::from_bytes([99; 32]),
        endpoint(1).node,
        endpoint(1).session,
        2,
        0,
        20_000,
    )
    .unwrap();
    assert!(
        restarted
            .compare_exchange(
                &deadline,
                1,
                1,
                &JournalTransition::BeginMaintenance(changed)
            )
            .await
            .is_err()
    );
    let reboot = restarted
        .compare_exchange(
            &deadline,
            1,
            2,
            &JournalTransition::Maintenance(MaintenanceEvent::SessionReplaced(
                SessionId::from_bytes([99; 16]),
            )),
        )
        .await
        .unwrap();
    let page = restarted
        .intents_page(reboot.registry(), None, 128)
        .await
        .unwrap();
    assert_eq!(page.entries()[0].session(), SessionId::from_bytes([99; 16]));
    assert_eq!(page.entries()[0].revision(), 4);
    assert_eq!(page.entries()[0].mode(), NodeMode::Draining);
    let obligations = restarted
        .enrollments_page(reboot.registry(), None, 128)
        .await
        .unwrap();
    assert_eq!(obligations.entries(), &[admitted.clone()]);
    assert!(obligations.entries()[0].unresolved());
    let completed = restarted
        .publish_enrollment_result(
            &admitted,
            EnrollmentEvent::Established(Digest::from_bytes([42; 32])),
            3,
        )
        .await
        .unwrap();
    assert_eq!(completed.accepted_at_ms(), 1);
    restarted.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrollment_racing_cordon_is_admitted_before_it_or_definitely_refused() {
    let fixture = Fixture::new().await;
    let other = fixture.client().await;
    let expected = fixture.journal.load_snapshot(scope()).await.unwrap();
    let spec = enrollment(43, 1);
    let transition = JournalTransition::BeginMaintenance(request(1, 1));
    let (enrolled, cordoned) = tokio::join!(
        fixture.journal.accept_enrollment(&spec, 1),
        other.compare_exchange(&expected, 1, 0, &transition)
    );
    assert_ne!(enrolled.is_ok(), cordoned.is_ok());
    if let Ok(FleetEnrollmentAcceptance::New(record)) = enrolled {
        let current = other.load_snapshot(scope()).await.unwrap();
        other
            .compare_exchange(&current, 1, 0, &transition)
            .await
            .unwrap();
        assert_eq!(
            other
                .load_enrollment(scope(), spec.key().unwrap())
                .await
                .unwrap(),
            Some(record)
        );
    } else {
        assert!(
            fixture
                .journal
                .load_enrollment(scope(), spec.key().unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
    let mut late = spec;
    late.request = Digest::from_bytes([44; 32]);
    late.target.intent_revision = 2;
    assert!(fixture.journal.accept_enrollment(&late, 2).await.is_err());
    other.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn terminal_history_and_permit_retirement_commit_atomically_across_reconstruction() {
    let fixture = Fixture::new().await;
    let stale = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .transition(JournalTransition::Allocate(spec(1)))
        .await;
    fixture.event(AttemptEvent::BeginCancel).await;
    let current = fixture.event(AttemptEvent::Cancelled).await;
    let progress = current.head().retirement_page(&[spec(1).id]).unwrap();
    assert!(
        fixture
            .journal
            .compare_exchange(
                &stale,
                1,
                0,
                &JournalTransition::Retire {
                    progress: progress.clone()
                }
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .journal
            .load_progress(scope(), progress.digest().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .head()
            .reserved_restore_bytes(),
        4096
    );
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .compare_exchange(
                &current,
                1,
                0,
                &JournalTransition::Retire {
                    progress: progress.clone()
                }
            )
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let head = restarted.load_snapshot(scope()).await.unwrap();
    assert!(head.head().attempts().is_empty());
    assert_eq!(head.head().reserved_restore_bytes(), 0);
    assert_eq!(
        head.head().progress().unwrap().digest,
        progress.digest().unwrap()
    );
    assert_eq!(
        restarted
            .load_progress(scope(), progress.digest().unwrap())
            .await
            .unwrap(),
        Some(progress)
    );
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn later_operations_retain_old_cordons_and_return_to_service_does_not_rewrite_head() {
    let fixture = Fixture::new().await;
    fixture
        .transition(JournalTransition::BeginMaintenance(request(1, 1)))
        .await;
    fixture
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Cordoned))
        .await;
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::BeginEvacuation,
        ))
        .await;
    let proof = DrainEvidence {
        node: endpoint(1).node,
        session: endpoint(1).session,
        remaining_cells: 0,
        unresolved_attempts: 0,
        relocated: true,
        readers_settled: true,
        followers_settled: true,
        facilities_closed: true,
        stopped: true,
        withdrawn: true,
    };
    fixture
        .transition(JournalTransition::Maintenance(
            MaintenanceEvent::ReadyToClose(proof),
        ))
        .await;
    fixture
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Stopped(
            proof,
        )))
        .await;
    let completed = fixture
        .journal
        .load_operation(scope(), request(1, 1).id())
        .await
        .unwrap()
        .unwrap();
    let later = fixture
        .transition(JournalTransition::BeginMaintenance(request(2, 2)))
        .await;
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let page = restarted
        .intents_page(later.registry(), None, 128)
        .await
        .unwrap();
    assert_eq!(page.entries()[0].mode(), NodeMode::Draining);
    assert_eq!(page.entries()[1].mode(), NodeMode::Draining);
    assert_eq!(
        restarted
            .load_operation(scope(), request(1, 1).id())
            .await
            .unwrap(),
        Some(completed)
    );
    let returned = restarted
        .return_to_service(
            scope(),
            endpoint(1).node,
            request(1, 1).id(),
            SessionId::from_bytes([41; 16]),
            3,
        )
        .await
        .unwrap();
    assert_eq!(returned.mode(), NodeMode::Active);
    let again = restarted
        .return_to_service(
            scope(),
            endpoint(1).node,
            request(1, 1).id(),
            SessionId::from_bytes([41; 16]),
            3,
        )
        .await
        .unwrap();
    assert_eq!(again, returned);
    let current = restarted.load_snapshot(scope()).await.unwrap();
    assert_eq!(current.head(), later.head());
    let mut move_spec = spec(1);
    move_spec.source_node = endpoint(3).node;
    move_spec.source = endpoint(3).session;
    move_spec.destination_node = endpoint(1).node;
    move_spec.destination = returned.session();
    restarted
        .compare_exchange(&current, 1, 0, &JournalTransition::Allocate(move_spec))
        .await
        .unwrap();
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn bounded_scans_reject_changed_versions_and_keep_pending_failed_boots() {
    let fixture = Fixture::new().await;
    let a = match fixture
        .journal
        .accept_enrollment(&enrollment(45, 1), 1)
        .await
        .unwrap()
    {
        FleetEnrollmentAcceptance::New(r) => r,
        _ => panic!("not new"),
    };
    let b = match fixture
        .journal
        .accept_enrollment(&enrollment(46, 2), 1)
        .await
        .unwrap()
    {
        FleetEnrollmentAcceptance::New(r) => r,
        _ => panic!("not new"),
    };
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let first = fixture
        .journal
        .intents_page(snapshot.registry(), None, 1)
        .await
        .unwrap();
    assert_eq!(first.entries(), &[intent(1)]);
    assert_eq!(first.next(), Some(endpoint(1).node));
    let second = fixture
        .journal
        .intents_page(snapshot.registry(), first.next(), 1)
        .await
        .unwrap();
    assert_eq!(second.entries(), &[intent(2)]);
    let enrolled = fixture
        .journal
        .enrollments_page(snapshot.registry(), None, 1)
        .await
        .unwrap();
    assert_eq!(enrolled.entries().len(), 1);
    assert!(enrolled.next().is_some());
    let last = fixture
        .journal
        .enrollments_page(snapshot.registry(), enrolled.next(), 1)
        .await
        .unwrap();
    assert!(last.next().is_none());
    assert_eq!(last.entries().len(), 1);
    let new = fixture
        .journal
        .rebind_active_intent(&intent(3), SessionId::from_bytes([47; 16]), 2)
        .await
        .unwrap();
    assert_ne!(new.session(), endpoint(3).session);
    assert!(
        fixture
            .journal
            .intents_page(snapshot.registry(), second.next(), 1)
            .await
            .is_err()
    );
    assert!(
        fixture
            .journal
            .enrollments_page(snapshot.registry(), None, 1)
            .await
            .is_err()
    );
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    let rows = fixture
        .journal
        .enrollments_page(current.registry(), None, 128)
        .await
        .unwrap();
    assert!(rows.entries().contains(&a));
    assert!(rows.entries().contains(&b));
    assert!(rows.entries().iter().all(|r| r.unresolved()));
    for limit in [0, MAX_PAGE_ENTRIES + 1] {
        assert!(
            fixture
                .journal
                .intents_page(current.registry(), None, limit)
                .await
                .is_err()
        );
        assert!(
            fixture
                .journal
                .enrollments_page(current.registry(), None, limit)
                .await
                .is_err()
        );
    }
    assert!(
        fixture
            .journal
            .intents_page(current.registry(), Some(NodeId::from_bytes([99; 16])), 1)
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_transaction_waiter_cannot_cancel_commit_and_close_joins_owned_job() {
    let fixture = Fixture::new().await;
    let journal = fixture.journal.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let writer = tokio::spawn(async move {
        journal
            .run(move |db| {
                db.write_intent(&intent(4))?;
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    entered_rx.await.unwrap();
    writer.abort();
    let closer = fixture.journal.clone();
    let closing = tokio::spawn(async move { closer.close().await });
    tokio::task::yield_now().await;
    assert!(!closing.is_finished());
    assert_eq!(fixture.journal.inner.activity.lock().unwrap().pending, 1);
    resume_tx.send(()).unwrap();
    closing.await.unwrap().unwrap();
    assert_eq!(
        fixture.journal.inner.slots.available_permits(),
        MAX_PENDING_JOBS
    );
    assert!(fixture.journal.inner.connection.lock().unwrap().is_none());
    assert!(matches!(
        fixture
            .journal
            .load_snapshot(scope())
            .await
            .err()
            .unwrap()
            .downcast_ref::<cellule_runtime::Error>(),
        Some(cellule_runtime::Error::RuntimeClosed)
    ));
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let snapshot = restarted.load_snapshot(scope()).await.unwrap();
    assert!(
        restarted
            .intents_page(snapshot.registry(), None, 128)
            .await
            .unwrap()
            .entries()
            .contains(&intent(4))
    );
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn lost_enrollment_replies_preserve_pending_and_immutable_settlement_after_restart() {
    let fixture = Fixture::new().await;
    let spec = enrollment(48, 1);
    lose(&fixture.journal);
    assert!(fixture.journal.accept_enrollment(&spec, 1).await.is_err());
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let original = match restarted.accept_enrollment(&spec, 30_000).await.unwrap() {
        FleetEnrollmentAcceptance::Existing(record) => record,
        _ => panic!("lost reply must not create another enrollment"),
    };
    assert_eq!(original.status(), EnrollmentStatus::Pending);
    assert_eq!(original.accepted_at_ms(), 1);
    assert_eq!(original.updated_at_ms(), 1);
    let mut changed = spec.clone();
    changed.role = EnrollmentRole::Follower { log_epoch: 8 };
    assert!(restarted.accept_enrollment(&changed, 30_000).await.is_err());
    let event = EnrollmentEvent::Refused(Digest::from_bytes([49; 32]));
    lose(&restarted);
    assert!(
        restarted
            .publish_enrollment_result(&original, event, 30_000)
            .await
            .is_err()
    );
    let settled = restarted
        .publish_enrollment_result(&original, event, 30_001)
        .await
        .unwrap();
    assert_eq!(settled.status(), EnrollmentStatus::Refused);
    assert_eq!(settled.updated_at_ms(), 30_000);
    assert!(!settled.unresolved());
    assert!(
        restarted
            .publish_enrollment_result(
                &original,
                EnrollmentEvent::Established(Digest::from_bytes([50; 32])),
                30_002
            )
            .await
            .is_err()
    );
    restarted.close().await.unwrap();
    let reconstructed = fixture.client().await;
    assert_eq!(
        reconstructed
            .load_enrollment(scope(), spec.key().unwrap())
            .await
            .unwrap(),
        Some(settled)
    );
    reconstructed.close().await.unwrap();
}

#[tokio::test]
async fn unexecuted_refusal_fences_delayed_acceptance_and_survives_client_restart() {
    let fixture = Fixture::new().await;
    let spec = enrollment(88, 1);
    let proof = Digest::from_bytes([89; 32]);
    let before = fixture
        .journal
        .load_snapshot(scope())
        .await
        .unwrap()
        .registry();
    lose(&fixture.journal);
    assert!(
        fixture
            .journal
            .refuse_unexecuted_enrollment(&spec, proof, 10)
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    let original = restarted
        .refuse_unexecuted_enrollment(&spec, proof, 20)
        .await
        .unwrap();
    assert_eq!(original.status(), EnrollmentStatus::Refused);
    assert_eq!(original.accepted_at_ms(), 10);
    let after = restarted.load_snapshot(scope()).await.unwrap().registry();
    assert_eq!(after.revision(), before.revision() + 1);
    match restarted.accept_enrollment(&spec, 30).await.unwrap() {
        FleetEnrollmentAcceptance::Existing(observed) => assert_eq!(observed, original),
        FleetEnrollmentAcceptance::New(_) => panic!("delayed acceptance must replay exclusion"),
    }
    let mut changed = spec.clone();
    changed.role = EnrollmentRole::Follower { log_epoch: 8 };
    assert!(
        restarted
            .refuse_unexecuted_enrollment(&changed, proof, 40)
            .await
            .is_err()
    );
    assert!(
        restarted
            .refuse_unexecuted_enrollment(&spec, Digest::from_bytes([90; 32]), 40)
            .await
            .is_err()
    );
    assert_eq!(
        restarted.load_snapshot(scope()).await.unwrap().registry(),
        after
    );
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_acceptance_and_unexecuted_refusal_share_one_transaction_domain() {
    let fixture = Fixture::new().await;
    let other = fixture.client().await;
    let spec = enrollment(91, 1);
    let proof = Digest::from_bytes([92; 32]);
    let (accepted, refused) = tokio::join!(
        fixture.journal.accept_enrollment(&spec, 10),
        other.refuse_unexecuted_enrollment(&spec, proof, 11)
    );
    accepted.unwrap();
    let refused = refused.unwrap();
    assert_eq!(refused.status(), EnrollmentStatus::Refused);
    assert_eq!(
        fixture
            .journal
            .load_enrollment(scope(), spec.key().unwrap())
            .await
            .unwrap(),
        Some(refused)
    );
    let live = enrollment(93, 2);
    let original = match fixture.journal.accept_enrollment(&live, 20).await.unwrap() {
        FleetEnrollmentAcceptance::New(record) => record,
        _ => panic!("new request"),
    };
    fixture
        .journal
        .publish_enrollment_result(&original, EnrollmentEvent::Established(proof), 21)
        .await
        .unwrap();
    assert!(
        other
            .refuse_unexecuted_enrollment(&live, proof, 22)
            .await
            .is_err()
    );
    other.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn failed_transaction_rolls_back_head_intent_and_registry_together() {
    let fixture = Fixture::new().await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let captured = before.clone();
    let error = fixture
        .journal
        .run(move |db| -> JournalResult<()> {
            db.write_intent(&intent(4))?;
            let next = captured.head().transition(
                db.profile,
                captured.head().revision(),
                1,
                0,
                JournalTransition::Allocate(spec(1)),
            )?;
            db.set_head(&next)?;
            Err(std::io::Error::other("injected failure before commit").into())
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().to_string(),
        "injected failure before commit"
    );
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert_eq!(
        fixture
            .journal
            .intents_page(before.registry(), None, 128)
            .await
            .unwrap()
            .entries(),
        &[intent(1), intent(2), intent(3)]
    );
    fixture.journal.close().await.unwrap();
    let restarted = fixture.client().await;
    assert_eq!(restarted.load_snapshot(scope()).await.unwrap(), before);
    restarted.close().await.unwrap();
}

#[tokio::test]
async fn malformed_head_and_missing_committed_references_fail_reconstruction() {
    let fixture = Fixture::new().await;
    let saved = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .run(|db| {
            db.tx.execute("UPDATE state SET head=?1", [vec![0u8]])?;
            Ok(())
        })
        .await
        .unwrap();
    let error = fixture.journal.load_snapshot(scope()).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<OperationError>(),
        Some(OperationError::Codec(_))
    ));
    assert!(std::error::Error::source(error.as_ref()).is_some());
    fixture.journal.close().await.unwrap();
    let error = fixture.client_error().await;
    assert!(matches!(
        error.downcast_ref::<OperationError>(),
        Some(OperationError::Codec(_))
    ));

    // Repair only this deliberately corrupted fixture through SQLite, then
    // remove a referenced operation. The adapter must reject either root.
    let connection = Connection::open(&fixture.path).unwrap();
    connection
        .execute(
            "UPDATE state SET head=?1",
            [saved.head().to_bytes().unwrap()],
        )
        .unwrap();
    connection.close().unwrap();
    let restarted = fixture.client().await;
    let current = restarted.load_snapshot(scope()).await.unwrap();
    restarted
        .compare_exchange(
            &current,
            1,
            0,
            &JournalTransition::BeginMaintenance(request(1, 52)),
        )
        .await
        .unwrap();
    restarted
        .run(|db| {
            db.tx.execute("DELETE FROM operations", [])?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(restarted.load_snapshot(scope()).await.is_err());
    restarted.close().await.unwrap();
    assert!(matches!(
        fixture
            .client_error()
            .await
            .downcast_ref::<OperationError>(),
        Some(OperationError::Invalid(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn job_admission_is_bounded_and_close_joins_every_accepted_transaction() {
    let fixture = Fixture::new().await;
    let journal = fixture.journal.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let first = tokio::spawn(async move {
        journal
            .run(move |_| {
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    entered_rx.await.unwrap();
    let mut accepted = Vec::new();
    for _ in 1..MAX_PENDING_JOBS {
        let journal = fixture.journal.clone();
        accepted.push(tokio::spawn(async move { journal.run(|_| Ok(())).await }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture.journal.inner.activity.lock().unwrap().pending == MAX_PENDING_JOBS {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let error = fixture.journal.run(|_| Ok(())).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<cellule_runtime::Error>(),
        Some(cellule_runtime::Error::Capacity("reference journal jobs"))
    ));
    let journal = fixture.journal.clone();
    let closing = tokio::spawn(async move { journal.close().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !fixture.journal.inner.activity.lock().unwrap().accepting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!closing.is_finished());
    resume_tx.send(()).unwrap();
    first.await.unwrap().unwrap();
    for job in accepted {
        job.await.unwrap().unwrap();
    }
    closing.await.unwrap().unwrap();
    assert_eq!(fixture.journal.inner.activity.lock().unwrap().pending, 0);
    assert_eq!(
        fixture.journal.inner.slots.available_permits(),
        MAX_PENDING_JOBS
    );
    assert!(fixture.journal.inner.connection.lock().unwrap().is_none());
}

#[tokio::test]
async fn fresh_inspection_authorization_checks_current_head_and_boot_without_writes() {
    let fixture = Fixture::new().await;
    fixture.preparing().await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let action = before
        .head()
        .movement_action(spec(1).id, MovementAction::Inspect, 0)
        .unwrap();
    let request = FleetInspectionRequest::new(
        action,
        before.registry(),
        Digest::from_bytes([60; 32]),
        spec(1).destination_node,
        spec(1).destination,
        5_000,
    )
    .unwrap();
    fixture
        .journal
        .authorize_inspection(&request, 1)
        .await
        .unwrap();
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert!(
        fixture
            .journal
            .load_movement_action(
                scope(),
                spec(1).id,
                MovementAction::Inspect,
                spec(1).destination_node,
                spec(1).destination
            )
            .await
            .unwrap()
            .is_none()
    );
    // A current head is insufficient if the physical endpoint has a newer boot.
    fixture
        .journal
        .rebind_active_intent(&intent(2), SessionId::from_bytes([61; 16]), 2)
        .await
        .unwrap();
    assert!(
        fixture
            .journal
            .authorize_inspection(&request, 2)
            .await
            .is_err()
    );
    let changed = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(changed.head(), before.head());
    let old_boot = FleetInspectionRequest::new(
        request.action().clone(),
        changed.registry(),
        Digest::from_bytes([62; 32]),
        request.node(),
        request.session(),
        request.deadline_ms(),
    )
    .unwrap();
    assert!(
        fixture
            .journal
            .authorize_inspection(&old_boot, 2)
            .await
            .is_err()
    );
    fixture
        .journal
        .claim_controller(
            scope(),
            before.head().revision(),
            before.head().controller().unwrap().claimant,
            2,
        )
        .await
        .unwrap();
    assert!(
        fixture
            .journal
            .authorize_inspection(&request, 2)
            .await
            .is_err()
    );
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn native_snapshot_authorization_rechecks_full_barrier_and_exact_boot_without_writes() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    let before = fixture.journal.load_snapshot(scope()).await.unwrap();
    let request = FleetSnapshotRequest::new(
        before.clone(),
        Digest::from_bytes([63; 32]),
        endpoint(2).node,
        endpoint(2).session,
        FleetSnapshotSubject::Cells(None),
        128,
        0,
        5_000,
    )
    .unwrap();
    client.authorize_snapshot(&request, 1).await.unwrap();
    assert_eq!(
        fixture.journal.load_snapshot(scope()).await.unwrap(),
        before
    );
    assert!(client.authorize_snapshot(&request, 5_000).await.is_err());
    fixture
        .journal
        .rebind_active_intent(&intent(2), SessionId::from_bytes([64; 16]), 2)
        .await
        .unwrap();
    assert!(client.authorize_snapshot(&request, 2).await.is_err());
    let changed = client.load_snapshot(scope()).await.unwrap();
    assert_eq!(changed.head(), before.head());
    let old_boot = FleetSnapshotRequest::new(
        changed.clone(),
        Digest::from_bytes([65; 32]),
        request.node(),
        request.session(),
        FleetSnapshotSubject::Host,
        1,
        0,
        5_000,
    )
    .unwrap();
    assert!(client.authorize_snapshot(&old_boot, 2).await.is_err());
    let fresh = FleetSnapshotRequest::new(
        changed,
        Digest::from_bytes([66; 32]),
        endpoint(2).node,
        SessionId::from_bytes([64; 16]),
        FleetSnapshotSubject::Host,
        1,
        0,
        5_000,
    )
    .unwrap();
    client.authorize_snapshot(&fresh, 2).await.unwrap();
    fixture
        .journal
        .claim_controller(
            scope(),
            before.head().revision(),
            before.head().controller().unwrap().claimant,
            2,
        )
        .await
        .unwrap();
    assert!(client.authorize_snapshot(&fresh, 2).await.is_err());
    client.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn complete_roster_traverses_multiple_pages_and_retains_every_status() {
    let fixture = Fixture::new().await;
    for n in 4..=132 {
        fixture
            .journal
            .register_initial_intent(&intent(n))
            .await
            .unwrap();
    }
    let mut records = Vec::new();
    for n in 1..=130 {
        let FleetEnrollmentAcceptance::New(mut record) = fixture
            .journal
            .accept_enrollment(&enrollment(n, 1), 1)
            .await
            .unwrap()
        else {
            panic!("duplicate fixture enrollment")
        };
        match n % 4 {
            0 => {
                record = fixture
                    .journal
                    .publish_enrollment_result(
                        &record,
                        EnrollmentEvent::Refused(Digest::from_bytes([210; 32])),
                        2,
                    )
                    .await
                    .unwrap()
            }
            1 => {}
            _ => {
                record = fixture
                    .journal
                    .publish_enrollment_result(
                        &record,
                        EnrollmentEvent::Established(Digest::from_bytes([211; 32])),
                        2,
                    )
                    .await
                    .unwrap();
                if n % 4 == 3 {
                    record = fixture
                        .journal
                        .publish_enrollment_result(
                            &record,
                            EnrollmentEvent::Retired(Digest::from_bytes([212; 32])),
                            3,
                        )
                        .await
                        .unwrap();
                }
            }
        }
        records.push(record);
    }
    records.sort_by_key(|record| *record.spec().key().unwrap().as_bytes());
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let roster = FleetRoster::collect(&fixture.journal, &snapshot, deadline)
        .await
        .unwrap();
    assert_eq!(roster.snapshot(), &snapshot);
    assert_eq!(roster.intents().len(), 132);
    assert_eq!(roster.enrollments(), records);
    assert_eq!(
        roster.required_boots(),
        vec![
            FleetRosterBoot {
                node: endpoint(1).node,
                session: endpoint(1).session
            },
            FleetRosterBoot {
                node: endpoint(3).node,
                session: endpoint(3).session
            }
        ]
    );
    roster.confirm(&fixture.journal, deadline).await.unwrap();
    let digest = roster.digest().unwrap();
    fixture.journal.close().await.unwrap();
    let reopened = fixture.client().await;
    let again = FleetRoster::collect(&reopened, &snapshot, deadline)
        .await
        .unwrap();
    assert_eq!(again.enrollments(), records);
    assert_eq!(again.digest().unwrap(), digest);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn roster_barrier_rejects_independent_enrollment_commit_and_original_reply_loss() {
    let fixture = Fixture::new().await;
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let roster = FleetRoster::collect(&fixture.journal, &snapshot, deadline)
        .await
        .unwrap();
    let client = fixture.client().await;
    lose(&client);
    let request = enrollment(233, 1);
    assert!(client.accept_enrollment(&request, 1).await.is_err());
    let current = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(current.head(), snapshot.head());
    assert_ne!(current.registry(), snapshot.registry());
    assert!(roster.confirm(&fixture.journal, deadline).await.is_err());
    assert!(
        FleetRoster::collect(&fixture.journal, &snapshot, deadline)
            .await
            .is_err()
    );
    let fresh = FleetRoster::collect(&fixture.journal, &current, deadline)
        .await
        .unwrap();
    assert_eq!(fresh.enrollments().len(), 1);
    assert_eq!(fresh.enrollments()[0].spec(), &request);
    assert_eq!(fresh.enrollments()[0].status(), EnrollmentStatus::Pending);
    assert_eq!(fresh.required_boots().len(), 2);
    client.close().await.unwrap();
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn roster_barrier_rechecks_controller_head_even_with_unchanged_registry() {
    let fixture = Fixture::new().await;
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let roster = FleetRoster::collect(&fixture.journal, &snapshot, deadline)
        .await
        .unwrap();
    let renewed = fixture
        .journal
        .claim_controller(
            scope(),
            snapshot.head().revision(),
            snapshot.head().controller().unwrap().claimant,
            1,
        )
        .await
        .unwrap();
    assert_eq!(renewed.registry(), snapshot.registry());
    assert_ne!(renewed.head(), snapshot.head());
    assert!(roster.confirm(&fixture.journal, deadline).await.is_err());
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn unbootstrapped_roster_and_expired_collection_cannot_claim_coverage() {
    let directory = tempfile::tempdir().unwrap();
    let journal = SqliteJournal::open(
        directory.path().join("fleet.sqlite"),
        scope(),
        FleetProfile::default(),
        0,
    )
    .await
    .unwrap();
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(
        &journal,
        &snapshot,
        tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    )
    .await
    .unwrap();
    assert!(roster.enrollments().is_empty());
    assert!(roster.snapshot().registry().bootstrap_revision().is_none());
    assert!(!roster.covers_advertisements(&[], 0).unwrap());
    journal.close().await.unwrap();
    assert!(matches!(
        FleetRoster::collect(&journal, &snapshot, tokio::time::Instant::now()).await,
        Err(cellule_runtime::Error::Node(
            "fleet roster collection deadline elapsed"
        ))
    ));
}
