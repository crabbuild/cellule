use crate::identity::{
    ApplicationId, CellTarget, Digest, IncarnationId, NamespaceId, NodeId, SessionId, TenantId,
};

use super::*;

mod absence;
mod accepted;
mod acquisition;
mod cleanup;
mod contracts;
mod inspection;
mod maintenance_release;
mod recovery;
mod registry;

fn operation_id(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16]).unwrap()
}

fn head() -> FleetHead {
    FleetHead::new(
        FleetScope {
            fleet: Digest::from_bytes([1; 32]),
            application: ApplicationId::from_bytes([2; 16]),
        },
        0,
    )
    .unwrap()
    .claim(
        FleetProfile::default(),
        0,
        SessionId::from_bytes([9; 16]),
        0,
    )
    .unwrap()
}

fn spec(sequence: u64) -> MoveAttemptSpec {
    MoveAttemptSpec {
        id: AttemptId {
            operation: operation_id(1),
            sequence,
        },
        target: CellTarget::new(
            TenantId::from_bytes([3; 16]),
            ApplicationId::from_bytes([2; 16]),
            NamespaceId::from_bytes([4; 16]),
            &sequence.to_be_bytes(),
        )
        .unwrap(),
        incarnation: IncarnationId::from_bytes([5; 16]),
        source_node: NodeId::from_bytes([1; 16]),
        source: SessionId::from_bytes([11; 16]),
        generation: 7,
        source_epoch: 4,
        destination_node: NodeId::from_bytes([2; 16]),
        destination: SessionId::from_bytes([12; 16]),
        cost: TransferCost {
            memory_bytes: 64 * 1024,
            disk_bytes: 4096,
            file_descriptors: 8,
            job_credits: 1,
        },
        snapshot_digest: Digest::from_bytes([6; 32]),
        deadline_ms: 10_000,
    }
}

fn transition(head: &FleetHead, event: JournalTransition) -> FleetHead {
    head.transition(
        FleetProfile::default(),
        head.revision(),
        head.controller().unwrap().epoch,
        head.last_observed_ms,
        event,
    )
    .unwrap()
}

fn retirement(head: &FleetHead, id: AttemptId) -> JournalTransition {
    let entry = head
        .attempts
        .iter()
        .find(|attempt| attempt.spec.id == id)
        .unwrap()
        .clone();
    JournalTransition::Retire {
        progress: ProgressPage {
            scope: head.scope(),
            operation: id.operation,
            sequence: head.progress().map_or(1, |progress| progress.sequence + 1),
            previous: head.progress().map(|progress| progress.digest),
            entries: vec![entry],
        },
    }
}

fn attempt(head: &FleetHead, id: AttemptId, event: AttemptEvent) -> FleetHead {
    transition(head, JournalTransition::Attempt { id, event })
}

fn release() -> PublishedPosition {
    PublishedPosition {
        incarnation: IncarnationId::from_bytes([5; 16]),
        epoch: 4,
        root: crate::control::RootRef {
            digest: Digest::from_bytes([8; 32]),
            txid: 9,
            checksum: cellule_ltx::types::CHECKSUM_FLAG | 10,
            commit_sequence: 9,
        },
    }
}

fn activation(node: u8, session: u8) -> ActivationEvidence {
    let mut position = release();
    position.epoch = 5;
    position.root.commit_sequence = 10;
    ActivationEvidence {
        node: NodeId::from_bytes([node; 16]),
        session: SessionId::from_bytes([session; 16]),
        position,
    }
}

fn reserved() -> (FleetHead, AttemptId) {
    let spec = spec(1);
    let id = spec.id;
    let head = transition(&head(), JournalTransition::Allocate(spec));
    let head = attempt(&head, id, AttemptEvent::BeginPrepare);
    let head = attempt(
        &head,
        id,
        AttemptEvent::Reserved(ReceiverReservation {
            session: SessionId::from_bytes([12; 16]),
            expires_at_ms: 20_000,
        }),
    );
    (head, id)
}

fn rejected(head: &FleetHead, id: AttemptId, event: AttemptEvent) {
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            head.controller().unwrap().epoch,
            head.last_observed_ms,
            JournalTransition::Attempt { id, event }
        )
        .is_err()
    );
}

#[test]
fn successor_session_node_root_and_cleanup_proofs_cannot_be_substituted() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    rejected(&head, id, AttemptEvent::Activated(activation(3, 12)));
    let mut changed_root = activation(2, 12);
    changed_root.position.root.commit_sequence = release().root.commit_sequence;
    changed_root.position.root.digest = Digest::from_bytes([99; 32]);
    rejected(&head, id, AttemptEvent::Activated(changed_root));
    let alternate = activation(3, 13);
    let head = attempt(&head, id, AttemptEvent::Activated(alternate.clone()));
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    assert_eq!(head.attempts()[0].next_action(), MovementAction::Inspect);
    let head = attempt(&head, id, AttemptEvent::ReceiverCleaned);
    let duplicate = attempt(&head, id, AttemptEvent::Activated(alternate));
    assert_eq!(duplicate, head);
    assert!(duplicate.attempts()[0].can_retire());
    let mut malformed = head.attempts()[0].clone();
    malformed.phase = AttemptPhase::Cancelled;
    malformed.released = None;
    malformed.activated = None;
    malformed.receiver_cleaned = false;
    assert!(malformed.to_bytes().is_err());
}

#[test]
fn terminal_maintenance_codec_retains_proof_and_rejects_fabricated_completion() {
    let mut operation = MaintenanceOperation::new(
        operation_id(1),
        Digest::from_bytes([2; 32]),
        NodeId::from_bytes([1; 16]),
        SessionId::from_bytes([11; 16]),
        1,
        0,
        10_000,
    )
    .unwrap();
    operation.apply(MaintenanceEvent::Cordoned, 0).unwrap();
    operation
        .apply(MaintenanceEvent::BeginEvacuation, 0)
        .unwrap();
    let mut evidence = DrainEvidence {
        node: operation.node(),
        session: operation.session(),
        remaining_cells: 0,
        unresolved_attempts: 0,
        relocated: true,
        readers_settled: true,
        followers_settled: true,
        facilities_closed: false,
        stopped: false,
        withdrawn: false,
    };
    operation
        .apply(MaintenanceEvent::ReadyToClose(evidence), 0)
        .unwrap();
    assert_eq!(
        MaintenanceOperation::from_bytes(&operation.to_bytes().unwrap()).unwrap(),
        operation
    );
    let mut corrupted = operation.clone();
    corrupted.phase = MaintenancePhase::Completed;
    assert!(corrupted.to_bytes().is_err());
    corrupted = operation.clone();
    corrupted.drain_evidence = None;
    assert!(corrupted.to_bytes().is_err());
    evidence.facilities_closed = true;
    evidence.stopped = true;
    evidence.withdrawn = true;
    operation
        .apply(MaintenanceEvent::Stopped(evidence), 1)
        .unwrap();
    assert_eq!(operation.drain_evidence(), Some(evidence));
    assert_eq!(
        MaintenanceOperation::from_bytes(&operation.to_bytes().unwrap()).unwrap(),
        operation
    );
    corrupted = operation;
    corrupted.drain_evidence.as_mut().unwrap().session = SessionId::from_bytes([22; 16]);
    assert!(corrupted.to_bytes().is_err());
}

#[test]
fn expiry_and_failover_retain_unknown_release_and_fence_the_old_controller() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    let restored = FleetHead::from_bytes(&head.to_bytes().unwrap()).unwrap();
    let successor = restored
        .claim(
            FleetProfile::default(),
            restored.revision(),
            SessionId::from_bytes([10; 16]),
            30_000,
        )
        .unwrap();
    assert_eq!(successor.controller().unwrap().epoch, 2);
    assert_eq!(successor.attempts(), head.attempts());
    assert_eq!(successor.reserved_restore_bytes(), 4096);
    assert_eq!(
        successor.attempts()[0].next_action(),
        MovementAction::Inspect
    );
    assert!(matches!(
        successor.transition(
            FleetProfile::default(),
            successor.revision(),
            1,
            30_000,
            retirement(&successor, id)
        ),
        Err(OperationError::Fenced)
    ));
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            30_000,
            retirement(&head, id)
        ),
        Err(OperationError::Fenced)
    ));
    rejected(&successor, id, AttemptEvent::BeginCancel);
    let observed = attempt(&successor, id, AttemptEvent::Released(release()));
    assert_eq!(
        observed.attempts()[0].next_action(),
        MovementAction::Activate
    );
    // Reconciliation finishes accepted work even though its original admission deadline expired.
    let activating = attempt(&observed, id, AttemptEvent::BeginActivate);
    let done = attempt(&activating, id, AttemptEvent::Activated(activation(2, 12)));
    let done = attempt(&done, id, AttemptEvent::ReceiverCleaned);
    let done = transition(&done, retirement(&done, id));
    assert!(done.attempts().is_empty());
}

#[test]
fn same_claimant_after_expiry_gets_a_new_fencing_epoch() {
    let head = head();
    let renewed = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([9; 16]),
            10_000,
        )
        .unwrap();
    assert_eq!(renewed.controller().unwrap().epoch, 1);
    let reacquired = renewed
        .claim(
            FleetProfile::default(),
            renewed.revision(),
            SessionId::from_bytes([9; 16]),
            40_000,
        )
        .unwrap();
    assert_eq!(reacquired.controller().unwrap().epoch, 2);
}

#[test]
fn competing_claim_and_cas_revision_do_not_allocate_work() {
    let head = head();
    assert!(matches!(
        head.claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([8; 16]),
            1
        ),
        Err(OperationError::Fenced)
    ));
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            0,
            1,
            1,
            JournalTransition::Allocate(spec(1))
        ),
        Err(OperationError::Conflict)
    ));
    assert!(
        head.claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([9; 16]),
            -1
        )
        .is_err()
    );
}

#[test]
fn out_of_order_and_wrong_scope_evidence_cannot_establish_readiness() {
    let (head, id) = reserved();
    rejected(&head, id, AttemptEvent::Activated(activation(2, 12)));
    rejected(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let mut wrong = release();
    wrong.epoch += 1;
    rejected(&head, id, AttemptEvent::Released(wrong));
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let duplicate = attempt(&head, id, AttemptEvent::Released(release()));
    assert_eq!(duplicate, head);
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let mut behind = activation(2, 12);
    behind.position.root.commit_sequence = 8;
    rejected(&head, id, AttemptEvent::Activated(behind));
    rejected(&head, id, AttemptEvent::Activated(activation(1, 11)));
    let mut stale_epoch = activation(2, 12);
    stale_epoch.position.epoch = 4;
    rejected(&head, id, AttemptEvent::Activated(stale_epoch));
}

#[test]
fn alternate_successor_requires_positive_cleanup_before_permit_retirement() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let head = attempt(&head, id, AttemptEvent::Activated(activation(3, 13)));
    assert_eq!(head.attempts()[0].next_action(), MovementAction::Cancel);
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            retirement(&head, id)
        )
        .is_err()
    );
    let head = attempt(&head, id, AttemptEvent::ReceiverCleaned);
    assert!(
        transition(&head, retirement(&head, id))
            .attempts()
            .is_empty()
    );
}

#[test]
fn unresolved_preparation_and_cancellation_still_consume_the_fleet_budget() {
    let head = transition(&head(), JournalTransition::Allocate(spec(1)));
    let head = attempt(&head, spec(1).id, AttemptEvent::BeginPrepare);
    let head = attempt(&head, spec(1).id, AttemptEvent::OutcomeUnknown);
    let head = transition(&head, JournalTransition::Allocate(spec(2)));
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::Allocate(spec(3))
        ),
        Err(OperationError::Budget)
    ));
    let head = attempt(&head, spec(1).id, AttemptEvent::BeginCancel);
    assert_eq!(head.reserved_restore_bytes(), 8192);
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            retirement(&head, spec(1).id)
        )
        .is_err()
    );
    let head = attempt(&head, spec(1).id, AttemptEvent::Cancelled);
    let head = transition(&head, retirement(&head, spec(1).id));
    let head = transition(&head, JournalTransition::Allocate(spec(3)));
    assert_eq!(head.attempts().len(), 2);
    assert_eq!(head.next_sequence(), 4);
}

#[test]
fn cost_and_duplicate_cell_are_checked_before_allocation() {
    let mut large = spec(1);
    large.cost.disk_bytes = MAX_RESTORE_BYTES;
    let head = transition(&head(), JournalTransition::Allocate(large));
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::Allocate(spec(2))
        ),
        Err(OperationError::Budget)
    ));
    let mut duplicate = spec(2);
    duplicate.target = spec(1).target;
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::Allocate(duplicate)
        ),
        Err(OperationError::Busy)
    ));
    let mut unknown = spec(2);
    unknown.cost.disk_bytes = 0;
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::Allocate(unknown)
        )
        .is_err()
    );
}

fn maintenance() -> MaintenanceOperation {
    MaintenanceOperation::new(
        operation_id(1),
        Digest::from_bytes([10; 32]),
        NodeId::from_bytes([1; 16]),
        SessionId::from_bytes([11; 16]),
        1,
        0,
        10_000,
    )
    .unwrap()
}

fn drain_evidence() -> DrainEvidence {
    DrainEvidence {
        node: NodeId::from_bytes([1; 16]),
        session: SessionId::from_bytes([11; 16]),
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

fn closing_evidence() -> DrainEvidence {
    DrainEvidence {
        facilities_closed: false,
        stopped: false,
        withdrawn: false,
        ..drain_evidence()
    }
}

#[test]
fn maintenance_relocation_foreign_tails_and_shutdown_all_gate_completion() {
    let head = transition(&head(), JournalTransition::BeginMaintenance(maintenance()));
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    for evidence in [
        DrainEvidence {
            followers_settled: false,
            ..drain_evidence()
        },
        DrainEvidence {
            relocated: false,
            ..drain_evidence()
        },
        DrainEvidence {
            remaining_cells: 1,
            ..drain_evidence()
        },
        DrainEvidence {
            unresolved_attempts: 1,
            ..drain_evidence()
        },
        DrainEvidence {
            readers_settled: false,
            ..drain_evidence()
        },
    ] {
        assert!(
            head.transition(
                FleetProfile::default(),
                head.revision(),
                1,
                0,
                JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(evidence))
            )
            .is_err()
        );
    }
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(closing_evidence())),
    );
    let missing_withdrawal = DrainEvidence {
        withdrawn: false,
        ..drain_evidence()
    };
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::Maintenance(MaintenanceEvent::Stopped(missing_withdrawal))
        )
        .is_err()
    );
    let done = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Stopped(drain_evidence())),
    );
    assert_eq!(
        done.maintenance().unwrap().phase(),
        MaintenancePhase::Completed
    );
    let duplicate = transition(
        &done,
        JournalTransition::Maintenance(MaintenanceEvent::Stopped(drain_evidence())),
    );
    assert_eq!(duplicate, done);
}

#[test]
fn deadline_and_reboot_preserve_physical_node_intent_without_false_success() {
    let head = transition(&head(), JournalTransition::BeginMaintenance(maintenance()));
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            10_000,
            JournalTransition::Allocate(spec(1))
        ),
        Err(OperationError::Deadline)
    ));
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Blocked(DrainBlocker::Deadline)),
    );
    assert_eq!(
        head.maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::ExtendDeadline(20_000)),
    );
    let rebooted = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::SessionReplaced(SessionId::from_bytes(
            [99; 16],
        ))),
    );
    let operation = rebooted.maintenance().unwrap();
    assert_eq!(operation.node(), NodeId::from_bytes([1; 16]));
    assert_eq!(operation.phase(), MaintenancePhase::Requested);
    assert_eq!(operation.intent_revision(), 3);
    assert!(
        rebooted
            .transition(
                FleetProfile::default(),
                rebooted.revision(),
                1,
                0,
                JournalTransition::Maintenance(MaintenanceEvent::Stopped(drain_evidence()))
            )
            .is_err()
    );
}

#[test]
fn maintenance_idempotency_and_unresolved_attempts_prevent_false_close() {
    let request = maintenance();
    let head = transition(
        &head(),
        JournalTransition::BeginMaintenance(request.clone()),
    );
    assert_eq!(
        transition(&head, JournalTransition::BeginMaintenance(request.clone())),
        head
    );
    let mut conflicting = request;
    conflicting.request_digest = Digest::from_bytes([99; 32]);
    assert!(matches!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::BeginMaintenance(conflicting)
        ),
        Err(OperationError::Conflict)
    ));
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    let head = transition(&head, JournalTransition::Allocate(spec(1)));
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            0,
            JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(closing_evidence()))
        )
        .is_err()
    );
}

#[test]
fn codec_rejects_truncation_trailing_bytes_wrong_record_type_and_oversize() {
    let (head, _) = reserved();
    let bytes = head.to_bytes().unwrap();
    assert_eq!(FleetHead::from_bytes(&bytes).unwrap(), head);
    for end in 0..bytes.len() {
        assert!(FleetHead::from_bytes(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(FleetHead::from_bytes(&trailing).is_err());
    assert!(FleetHead::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
    let attempt = &head.attempts()[0];
    let encoded_attempt = attempt.to_bytes().unwrap();
    assert_eq!(MoveAttempt::from_bytes(&encoded_attempt).unwrap(), *attempt);
    assert!(FleetHead::from_bytes(&encoded_attempt).is_err());
    let maintenance = maintenance();
    assert_eq!(
        MaintenanceOperation::from_bytes(&maintenance.to_bytes().unwrap()).unwrap(),
        maintenance
    );
}

#[test]
fn decoder_and_encoder_reject_unproven_or_overbudget_stored_states() {
    let (head, _) = reserved();
    let mut corrupted = head.clone();
    corrupted.attempts[0].phase = AttemptPhase::Activated;
    assert!(corrupted.to_bytes().is_err());
    let mut corrupted = head.clone();
    corrupted.attempts[0].spec.cost.disk_bytes = MAX_RESTORE_BYTES + 1;
    assert!(corrupted.to_bytes().is_err());
    let mut corrupted = head;
    corrupted.next_sequence = 1;
    assert!(corrupted.to_bytes().is_err());
}

proptest::proptest! {
    #[test]
    fn arbitrary_small_records_never_invent_a_valid_head(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048)) {
        if let Ok(head) = FleetHead::from_bytes(&bytes) {
            proptest::prop_assert_eq!(head.to_bytes().unwrap(), bytes);
            proptest::prop_assert!(head.attempts().len() <= MAX_ACTIVE_ATTEMPTS);
            proptest::prop_assert!(head.reserved_restore_bytes() <= MAX_RESTORE_BYTES);
        }
    }
}
