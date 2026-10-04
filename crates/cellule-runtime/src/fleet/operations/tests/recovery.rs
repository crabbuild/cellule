use super::*;
use crate::control::{Control, ControlState, Owner, RecoveryOverlayRef};

fn recovered(overlay: bool, idle: bool) -> (FleetHead, AcceptedFleetAction, RecoveredActivation) {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::BeginRecover);
    let spec = spec(1);
    let action = head
        .movement_action(id, MovementAction::Recover, 0)
        .unwrap();
    let accepted =
        AcceptedFleetAction::new(action, &head, spec.destination_node, spec.destination, 0)
            .unwrap();
    let mut control = Control::initial(
        spec.target.cell_id(),
        spec.incarnation,
        Owner {
            session: spec.source,
            endpoint: "https://source.internal:8789".into(),
        },
        Digest::from_bytes([35; 32]),
        1,
    )
    .unwrap();
    control.epoch = spec.source_epoch;
    control.revision = 9;
    control.progress = 9;
    control.state = if idle {
        ControlState::Idle
    } else {
        ControlState::Serving
    };
    control.root = Some(release().root);
    if idle {
        control.owner = None;
    }
    if overlay {
        control = control
            .attach_recovery(RecoveryOverlayRef {
                leader_session: spec.source,
                log_epoch: 1,
                manifest_digest: Digest::from_bytes([36; 32]),
                first_node_sequence: 1,
                last_node_sequence: 2,
                predecessor: control.root.clone().unwrap(),
                final_txid: 10,
                final_checksum: cellule_ltx::types::CHECKSUM_FLAG | 11,
                final_commit_sequence: 10,
            })
            .unwrap();
    }
    // Pure tests model a retained record's shape. Canonical capability creation
    // and actual takeover are exercised through public host/runtime scenarios.
    let basis = RecoveryBasis {
        spec: spec.clone(),
        scope: head.scope(),
        action_key: accepted.action().key().unwrap(),
        node: spec.destination_node,
        session: spec.destination,
        accepted_at_ms: 0,
        control,
        observed_at_ms: 0,
    };
    basis.validate_acceptance(&accepted).unwrap();
    let mut restored = basis
        .control()
        .takeover(Owner {
            session: spec.destination,
            endpoint: "https://successor.internal:8789".into(),
        })
        .unwrap();
    if let Some(overlay) = restored.recovery.take() {
        restored.revision += 1;
        restored.progress += 1;
        restored.root = Some(crate::control::RootRef {
            digest: Digest::from_bytes([37; 32]),
            txid: overlay.final_txid,
            checksum: overlay.final_checksum,
            commit_sequence: overlay.final_commit_sequence,
        });
    }
    let recovery = RecoveryEvidence::new(basis, restored, 0).unwrap();
    let serving = ActivationEvidence {
        node: spec.destination_node,
        session: spec.destination,
        position: recovery.position().unwrap(),
    };
    (head, accepted, RecoveredActivation { recovery, serving })
}

#[test]
fn recovered_history_is_distinct_and_charged_until_independent_credit_settlement() {
    let (head, accepted, evidence) = recovered(true, false);
    let id = evidence.recovery.basis().spec().id;
    let pending = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    assert_eq!(pending.reserved_restore_bytes(), spec(1).cost.disk_bytes);
    rejected(&pending, id, AttemptEvent::Cancelled);
    rejected(&pending, id, AttemptEvent::Released(release()));
    let done = attempt(
        &pending,
        id,
        AttemptEvent::Recovered(Box::new(evidence.clone())),
    );
    assert_eq!(done.attempts()[0].phase(), AttemptPhase::Recovered);
    assert!(done.attempts()[0].released().is_none() && done.attempts()[0].activated().is_none());
    assert_eq!(done.attempts()[0].next_action(), MovementAction::Cancel);
    assert!(done.retirement_page(&[id]).is_err());
    let duplicate = attempt(
        &done,
        id,
        AttemptEvent::Recovered(Box::new(evidence.clone())),
    );
    assert_eq!(duplicate, done);
    let done = attempt(&done, id, AttemptEvent::ReceiverCleaned);
    let page = done.retirement_page(&[id]).unwrap();
    assert_eq!(
        ProgressPage::from_bytes(&page.to_bytes().unwrap()).unwrap(),
        page
    );
    let retired = transition(
        &done,
        JournalTransition::Retire {
            progress: page.clone(),
        },
    );
    assert!(retired.attempts().is_empty());
    assert!(page.entries()[0].recovered().is_some());
    let result = FleetActionOutcome {
        scope: head.scope(),
        action_key: accepted.action().key().unwrap(),
        node: spec(1).destination_node,
        session: spec(1).destination,
        observed_at_ms: 0,
        outcome: FleetOutcome::Recovered(Box::new(evidence)),
    };
    accepted.validate_result(&result).unwrap();
    assert_eq!(
        FleetActionOutcome::from_bytes(&result.to_bytes().unwrap()).unwrap(),
        result
    );
}

#[test]
fn recovery_records_require_the_exact_canonical_takeover_and_overlay_position() {
    for overlay in [false, true] {
        let (_, _, evidence) = recovered(overlay, false);
        let basis = evidence.recovery.basis();
        for mutation in 0..8 {
            let mut restored = evidence.recovery.restored().clone();
            match mutation {
                0 => restored.epoch += 1,
                1 => restored.revision += 1,
                2 => restored.state = ControlState::Serving,
                3 => restored.owner.as_mut().unwrap().session = spec(1).source,
                4 => restored.root.as_mut().unwrap().txid += 1,
                5 => restored.root.as_mut().unwrap().commit_sequence += 1,
                6 => restored.root.as_mut().unwrap().checksum ^= 1,
                _ => restored.incarnation = IncarnationId::from_bytes([99; 16]),
            }
            assert!(RecoveryEvidence::new(basis.clone(), restored, 0).is_err());
        }
        let mut before = basis.clone();
        before.control.epoch += 1;
        assert!(before.to_bytes().is_err());
        let mut before = basis.clone();
        before.action_key = Digest::from_bytes([99; 32]);
        assert!(before.to_bytes().is_err());
        let mut before = basis.clone();
        before.control.owner.as_mut().unwrap().session = spec(1).destination;
        assert!(before.to_bytes().is_err());
    }
}

#[test]
fn idle_source_input_is_recovery_without_a_clean_release_claim() {
    let (head, _, evidence) = recovered(false, true);
    let id = spec(1).id;
    let done = attempt(&head, id, AttemptEvent::Recovered(Box::new(evidence)));
    assert!(done.attempts()[0].released().is_none());
    assert_eq!(
        done.attempts()[0]
            .recovered()
            .unwrap()
            .recovery
            .basis()
            .control()
            .state,
        ControlState::Idle
    );
    assert_eq!(
        FleetHead::from_bytes(&done.to_bytes().unwrap()).unwrap(),
        done
    );
}

#[test]
fn recovery_cannot_substitute_a_newer_root_or_foreign_fleet_for_retained_evidence() {
    let (head, accepted, original) = recovered(true, false);
    let mut changed = original.clone();
    changed.serving.position.root.digest = Digest::from_bytes([99; 32]);
    rejected(
        &head,
        spec(1).id,
        AttemptEvent::Recovered(Box::new(changed)),
    );
    let mut advanced = original.clone();
    advanced.serving.position.root.digest = Digest::from_bytes([99; 32]);
    advanced.serving.position.root.txid += 1;
    advanced.serving.position.root.commit_sequence += 1;
    let done = attempt(
        &head,
        spec(1).id,
        AttemptEvent::Recovered(Box::new(advanced)),
    );
    assert_eq!(
        done.attempts()[0].recovered().unwrap().recovery,
        original.recovery
    );
    let mut foreign = original;
    foreign.recovery.basis.scope.fleet = Digest::from_bytes([99; 32]);
    foreign.recovery.basis.action_key = super::super::actions::movement_key(
        foreign.recovery.basis.scope,
        MovementAction::Recover,
        &spec(1),
    );
    rejected(
        &head,
        spec(1).id,
        AttemptEvent::Recovered(Box::new(foreign.clone())),
    );
    assert!(
        foreign
            .recovery
            .basis
            .validate_acceptance(&accepted)
            .is_err()
    );
    let mut early = done.attempts()[0].clone();
    early.recovered.as_mut().unwrap().recovery.recorded_at_ms = 1;
    assert!(early.to_bytes().is_err());
}

#[test]
fn recovery_codecs_reject_truncation_extra_wrong_kind_version_and_oversize() {
    let (head, _, evidence) = recovered(true, false);
    let basis = evidence.recovery.basis();
    let basis_bytes = basis.to_bytes().unwrap();
    let evidence_bytes = evidence.recovery.to_bytes().unwrap();
    assert_eq!(RecoveryBasis::from_bytes(&basis_bytes).unwrap(), *basis);
    assert_eq!(
        RecoveryEvidence::from_bytes(&evidence_bytes).unwrap(),
        evidence.recovery
    );
    for end in 0..basis_bytes.len() {
        assert!(RecoveryBasis::from_bytes(&basis_bytes[..end]).is_err());
    }
    for end in 0..evidence_bytes.len() {
        assert!(RecoveryEvidence::from_bytes(&evidence_bytes[..end]).is_err());
    }
    let mut trailing = basis_bytes.clone();
    trailing.push(0);
    assert!(RecoveryBasis::from_bytes(&trailing).is_err());
    let mut trailing = evidence_bytes.clone();
    trailing.push(0);
    assert!(RecoveryEvidence::from_bytes(&trailing).is_err());
    let mut version = basis_bytes;
    version[4 + b"cellule.fleet-operation\0".len()] = 255;
    assert!(RecoveryBasis::from_bytes(&version).is_err());
    assert!(RecoveryBasis::from_bytes(&evidence_bytes).is_err());
    assert!(RecoveryEvidence::from_bytes(&head.to_bytes().unwrap()).is_err());
    assert!(RecoveryBasis::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
    assert!(RecoveryEvidence::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
    let action = head
        .movement_action(spec(1).id, MovementAction::Recover, 0)
        .unwrap();
    assert_eq!(
        FleetAction::from_bytes(&action.to_bytes().unwrap()).unwrap(),
        action
    );
}
