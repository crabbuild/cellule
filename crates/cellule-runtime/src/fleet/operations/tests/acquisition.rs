use super::*;
use crate::control::{Control, ControlState, Owner};

fn basis() -> AcquisitionBasis {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let action = head
        .movement_action(id, MovementAction::Activate, 0)
        .unwrap();
    let accepted = AcceptedFleetAction::new(
        action,
        &head,
        spec(1).destination_node,
        spec(1).destination,
        1,
    )
    .unwrap();
    let mut control = Control::initial(
        spec(1).target.cell_id(),
        spec(1).incarnation,
        Owner {
            session: spec(1).source,
            endpoint: "https://source.internal:8789".into(),
        },
        Digest::from_bytes([1; 32]),
        1,
    )
    .unwrap();
    control.epoch = release().epoch;
    control.revision = 9;
    control.progress = 9;
    control.state = ControlState::Idle;
    control.owner = None;
    control.root = Some(release().root);
    AcquisitionBasis::new(accepted, control, 2).unwrap()
}

#[test]
fn acquisition_basis_requires_exact_idle_scope_and_release_position() {
    let original = basis();
    for change in 0..6 {
        let mut control = original.control().clone();
        match change {
            0 => control.state = ControlState::Serving,
            1 => control.incarnation = IncarnationId::from_bytes([99; 16]),
            2 => control.epoch -= 1,
            3 => control.root.as_mut().unwrap().digest = Digest::from_bytes([99; 32]),
            4 => control.root = None,
            _ => control.cell = spec(2).target.cell_id(),
        }
        assert!(AcquisitionBasis::new(original.accepted().clone(), control, 2).is_err());
    }
    assert!(
        AcquisitionBasis::new(original.accepted().clone(), original.control().clone(), 0).is_err()
    );
    let (_, accepted_release) = super::accepted::accepted_release();
    assert!(AcquisitionBasis::new(accepted_release, original.control().clone(), 2).is_err());
}

#[test]
fn intervening_idle_epoch_retains_its_exact_input_without_rewriting_source_release() {
    let original = basis();
    let mut control = original.control().clone();
    control.epoch += 1;
    let root = control.root.as_mut().unwrap();
    root.commit_sequence += 1;
    root.txid += 1;
    root.digest = Digest::from_bytes([99; 32]);
    let newer = AcquisitionBasis::new(original.accepted().clone(), control, 3).unwrap();
    assert_ne!(newer.position().unwrap(), release());
    assert_eq!(original.position().unwrap(), release());
    assert_eq!(
        AcquisitionBasis::from_bytes(&newer.to_bytes().unwrap()).unwrap(),
        newer
    );
}

#[test]
fn acquisition_basis_requires_an_activation_result_after_the_retained_input() {
    let original = basis();
    let result = FleetActionOutcome {
        scope: original.accepted().action().scope(),
        action_key: original.accepted().action().key().unwrap(),
        node: spec(1).destination_node,
        session: spec(1).destination,
        observed_at_ms: 3,
        outcome: FleetOutcome::Activated(activation(2, 12)),
    };
    original.validate_result(&result).unwrap();
    let mut changed = result.clone();
    changed.observed_at_ms = 1;
    assert!(original.validate_result(&changed).is_err());
    changed = result;
    let FleetOutcome::Activated(evidence) = &mut changed.outcome else {
        panic!()
    };
    evidence.position.epoch = original.control().epoch;
    assert!(original.validate_result(&changed).is_err());
}

#[test]
fn acquisition_basis_codec_rejects_partial_extra_wrong_kind_and_oversized_records() {
    let original = basis();
    let bytes = original.to_bytes().unwrap();
    assert_eq!(AcquisitionBasis::from_bytes(&bytes).unwrap(), original);
    for end in 0..bytes.len() {
        assert!(AcquisitionBasis::from_bytes(&bytes[..end]).is_err());
    }
    let mut changed = bytes;
    changed.push(0);
    assert!(AcquisitionBasis::from_bytes(&changed).is_err());
    assert!(AcquisitionBasis::from_bytes(&original.accepted().to_bytes().unwrap()).is_err());
    assert!(AcquisitionBasis::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
}
