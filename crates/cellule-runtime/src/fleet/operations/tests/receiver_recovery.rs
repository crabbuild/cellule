use super::*;
use crate::control::{Control, ControlState, Owner};

fn evidence() -> ReceiverRecoveryEvidence {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let spec = spec(1);
    let registry = RegistryVersion::new(head.scope())
        .unwrap()
        .bootstrap(0)
        .unwrap();
    let node = NodeId::from_bytes([7; 16]);
    let session = SessionId::from_bytes([77; 16]);
    let route = ReceiverRoute::begin(
        head.scope(),
        &spec,
        node,
        session,
        Digest::from_bytes([31; 32]),
        registry,
    )
    .unwrap();
    let action = head
        .movement_action_with_receiver_route(id, MovementAction::Activate, route, 0)
        .unwrap();
    let accepted =
        AcceptedFleetAction::new_with_registry(action, &head, registry, node, session, 0).unwrap();
    // Unit records check shape. Real NodeDirectory capability construction,
    // persistence and observed takeover are covered by minion's public cases.
    let mut control = Control::initial(
        spec.target.cell_id(),
        spec.incarnation,
        Owner {
            session: spec.destination,
            endpoint: "https://receiver.internal".into(),
        },
        Digest::from_bytes([35; 32]),
        1,
    )
    .unwrap();
    control.epoch = release().epoch + 1;
    control.revision = 9;
    control.progress = 9;
    control.root = Some(release().root);
    control.state = ControlState::Serving;
    let basis = ReceiverRecoveryBasis {
        accepted,
        control,
        observed_at_ms: 0,
    };
    basis.validate().unwrap();
    let restored = basis
        .control
        .takeover(Owner {
            session,
            endpoint: "https://successor.internal".into(),
        })
        .unwrap();
    ReceiverRecoveryEvidence::new(basis, restored, 0).unwrap()
}

#[test]
fn receiver_recovery_requires_exact_closed_owner_and_clean_release() {
    let original = evidence();
    for mutation in 0..7 {
        let mut basis = original.basis.clone();
        match mutation {
            0 => basis.control.owner.as_mut().unwrap().session = spec(1).source,
            1 => basis.control.owner.as_mut().unwrap().session = basis.accepted.session(),
            2 => basis.control.epoch = release().epoch,
            3 => basis.control.incarnation = IncarnationId::from_bytes([99; 16]),
            4 => basis.control.cell = spec(2).target.cell_id(),
            5 => basis.control.state = ControlState::Idle,
            _ => basis.observed_at_ms = -1,
        }
        assert!(basis.to_bytes().is_err(), "mutation {mutation}");
    }
    for mutation in 0..6 {
        let mut restored = original.restored.clone();
        match mutation {
            0 => restored.epoch += 1,
            1 => restored.revision += 1,
            2 => restored.root.as_mut().unwrap().digest = Digest::from_bytes([99; 32]),
            3 => restored.owner.as_mut().unwrap().session = spec(1).destination,
            4 => restored.state = ControlState::Serving,
            _ => restored.progress += 1,
        }
        assert!(ReceiverRecoveryEvidence::new(original.basis.clone(), restored, 0).is_err());
    }
    let accepted = original.basis.accepted();
    let result = FleetActionOutcome {
        scope: accepted.action().scope(),
        action_key: accepted.action().key().unwrap(),
        node: accepted.node(),
        session: accepted.session(),
        observed_at_ms: 0,
        outcome: FleetOutcome::Activated(ActivationEvidence {
            node: accepted.node(),
            session: accepted.session(),
            position: PublishedPosition {
                incarnation: original.restored.incarnation,
                epoch: original.restored.epoch,
                root: original.restored.root.clone().unwrap(),
            },
        }),
    };
    original.validate_result(&result).unwrap();
    let mut changed = result;
    let FleetOutcome::Activated(ref mut serving) = changed.outcome else {
        unreachable!()
    };
    serving.position.root.digest = Digest::from_bytes([99; 32]);
    assert!(original.validate_result(&changed).is_err());
}

#[test]
fn receiver_recovery_records_reject_incomplete_wrong_version_kind_and_oversize() {
    let evidence = evidence();
    let basis = evidence.basis();
    let basis_bytes = basis.to_bytes().unwrap();
    let evidence_bytes = evidence.to_bytes().unwrap();
    assert_eq!(
        ReceiverRecoveryBasis::from_bytes(&basis_bytes).unwrap(),
        *basis
    );
    assert_eq!(
        ReceiverRecoveryEvidence::from_bytes(&evidence_bytes).unwrap(),
        evidence
    );
    for end in 0..basis_bytes.len() {
        assert!(ReceiverRecoveryBasis::from_bytes(&basis_bytes[..end]).is_err());
    }
    for end in 0..evidence_bytes.len() {
        assert!(ReceiverRecoveryEvidence::from_bytes(&evidence_bytes[..end]).is_err());
    }
    let mut trailing = basis_bytes.clone();
    trailing.push(0);
    assert!(ReceiverRecoveryBasis::from_bytes(&trailing).is_err());
    let mut trailing = evidence_bytes.clone();
    trailing.push(0);
    assert!(ReceiverRecoveryEvidence::from_bytes(&trailing).is_err());
    for bytes in [&basis_bytes, &evidence_bytes] {
        let mut version = bytes.clone();
        version[4 + b"cellule.fleet-operation\0".len()] = 255;
        assert!(ReceiverRecoveryBasis::from_bytes(&version).is_err());
        assert!(ReceiverRecoveryEvidence::from_bytes(&version).is_err());
    }
    assert!(ReceiverRecoveryBasis::from_bytes(&evidence_bytes).is_err());
    assert!(ReceiverRecoveryEvidence::from_bytes(&basis_bytes).is_err());
    assert!(RecoveryBasis::from_bytes(&basis_bytes).is_err());
    assert!(RecoveryEvidence::from_bytes(&evidence_bytes).is_err());
    assert!(ReceiverRecoveryBasis::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
    assert!(ReceiverRecoveryEvidence::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
}
