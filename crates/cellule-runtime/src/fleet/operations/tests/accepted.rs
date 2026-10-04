use super::*;

pub(super) fn accepted_release() -> (FleetHead, AcceptedFleetAction) {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let action = head
        .movement_action(id, MovementAction::Release, 0)
        .unwrap();
    let accepted =
        AcceptedFleetAction::new(action, &head, spec(1).source_node, spec(1).source, 1).unwrap();
    (head, accepted)
}

#[test]
fn acceptance_checks_current_head_endpoint_deadline_and_scope() {
    let (head, accepted) = accepted_release();
    let action = accepted.action().clone();
    for (node, session) in [
        (spec(1).destination_node, spec(1).destination),
        (spec(1).source_node, SessionId::from_bytes([99; 16])),
        (NodeId::from_bytes([99; 16]), spec(1).source),
    ] {
        assert!(AcceptedFleetAction::new(action.clone(), &head, node, session, 1).is_err());
    }
    assert!(
        AcceptedFleetAction::new(
            action.clone(),
            &head,
            accepted.node(),
            accepted.session(),
            10_000
        )
        .is_err()
    );
    let mut altered = action.clone();
    altered.scope.application = ApplicationId::from_bytes([99; 16]);
    assert!(
        AcceptedFleetAction::new(altered, &head, accepted.node(), accepted.session(), 1).is_err()
    );
    let newer = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            action.controller(),
            1,
        )
        .unwrap();
    assert!(
        AcceptedFleetAction::new(action, &newer, accepted.node(), accepted.session(), 1).is_err()
    );
}

#[test]
fn stable_key_cannot_substitute_cost_epoch_nodes_or_snapshot() {
    let (_, accepted) = accepted_release();
    for field in 0..5 {
        let mut altered = accepted.action().clone();
        let FleetActionKind::Movement { attempt, .. } = &mut altered.kind else {
            panic!()
        };
        match field {
            0 => attempt.spec.cost.disk_bytes += 1,
            1 => attempt.spec.source_epoch += 1,
            2 => attempt.spec.source_node = NodeId::from_bytes([99; 16]),
            3 => attempt.spec.snapshot_digest = Digest::from_bytes([99; 32]),
            _ => attempt.spec.deadline_ms += 1,
        }
        assert_eq!(altered.key().unwrap(), accepted.action().key().unwrap());
        assert!(matches!(
            accepted.validate_replay(&altered, accepted.node(), accepted.session()),
            Err(OperationError::Conflict)
        ));
    }
}

#[test]
fn existing_acceptance_survives_changed_controller_but_never_creates_new_work() {
    let (head, accepted) = accepted_release();
    let mut replay = accepted.action().clone();
    replay.controller = SessionId::from_bytes([88; 16]);
    replay.controller_epoch += 1;
    replay.journal_revision += 1;
    replay.issued_at_ms = 2;
    accepted
        .validate_replay(&replay, accepted.node(), accepted.session())
        .unwrap();
    assert!(replay.authorize_against(&head, 2).is_err());
    let expired = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            replay.controller,
            40_000,
        )
        .unwrap();
    assert!(
        AcceptedFleetAction::new(
            accepted.action().clone(),
            &expired,
            accepted.node(),
            accepted.session(),
            40_000
        )
        .is_err()
    );
}

#[test]
fn result_must_bind_original_endpoint_and_acceptance_time() {
    let (_, accepted) = accepted_release();
    let result = FleetActionOutcome {
        scope: accepted.action().scope(),
        action_key: accepted.action().key().unwrap(),
        node: accepted.node(),
        session: accepted.session(),
        observed_at_ms: 2,
        outcome: FleetOutcome::Released(release()),
    };
    accepted.validate_result(&result).unwrap();
    let mut changed = result.clone();
    changed.observed_at_ms = 0;
    assert!(accepted.validate_result(&changed).is_err());
    changed = result.clone();
    changed.session = spec(1).destination;
    assert!(accepted.validate_result(&changed).is_err());
    changed = result;
    let FleetOutcome::Released(position) = &mut changed.outcome else {
        panic!()
    };
    position.epoch += 1;
    assert!(accepted.validate_result(&changed).is_err());
}

#[test]
fn bounded_acceptance_codec_rejects_partial_trailing_version_and_oversized_records() {
    let (_, accepted) = accepted_release();
    let bytes = accepted.to_bytes().unwrap();
    assert_eq!(AcceptedFleetAction::from_bytes(&bytes).unwrap(), accepted);
    for end in 0..bytes.len() {
        assert!(AcceptedFleetAction::from_bytes(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(AcceptedFleetAction::from_bytes(&trailing).is_err());
    let mut wrong_version = bytes.clone();
    wrong_version[4 + b"cellule.fleet-operation\0".len()] = 255;
    assert!(AcceptedFleetAction::from_bytes(&wrong_version).is_err());
    assert!(AcceptedFleetAction::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
    let mut wrong_time = accepted.clone();
    wrong_time.accepted_at_ms = -1;
    assert!(wrong_time.to_bytes().is_err());
    wrong_time.accepted_at_ms = spec(1).deadline_ms;
    assert!(wrong_time.to_bytes().is_err());
    let mut wrong_endpoint = accepted;
    wrong_endpoint.session = spec(1).destination;
    assert!(wrong_endpoint.to_bytes().is_err());
}
