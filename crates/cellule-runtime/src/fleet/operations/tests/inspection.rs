use super::*;

fn successor_request(node: u8, session: u8) -> (FleetHead, FleetInspectionRequest) {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let request = FleetInspectionRequest::new(
        head.movement_action(id, MovementAction::Inspect, 0)
            .unwrap(),
        RegistryVersion::new(head.scope()).unwrap(),
        Digest::from_bytes([71; 32]),
        NodeId::from_bytes([node; 16]),
        SessionId::from_bytes([session; 16]),
        5_000,
    )
    .unwrap();
    (head, request)
}

#[test]
fn released_successor_inspection_is_read_only_and_preserves_receiver_credit() {
    for (node, session) in [(3, 13), (2, 13)] {
        let (head, request) = successor_request(node, session);
        request
            .authorize_against(&head, request.registry(), 1)
            .unwrap();
        request
            .validate_endpoint(request.node(), request.session())
            .unwrap();
        assert!(
            request
                .validate_endpoint(NodeId::from_bytes([99; 16]), request.session())
                .is_err()
        );
        assert!(
            request
                .validate_endpoint(request.node(), SessionId::from_bytes([99; 16]))
                .is_err()
        );
        assert_eq!(
            FleetInspectionRequest::from_bytes(&request.to_bytes().unwrap()).unwrap(),
            request
        );
        let observed = FleetInspectionObservation::new(
            request.clone(),
            1,
            FleetActionOutcome {
                scope: head.scope(),
                action_key: request.action().key().unwrap(),
                node: request.node(),
                session: request.session(),
                observed_at_ms: 3,
                outcome: FleetOutcome::Activated(activation(node, session)),
            },
        )
        .unwrap();
        observed.validate_for(&request, 4, 10).unwrap();
        assert_eq!(
            FleetInspectionObservation::from_bytes(&observed.to_bytes().unwrap()).unwrap(),
            observed
        );
        let id = head.attempts()[0].spec().id;
        let activating = attempt(&head, id, AttemptEvent::BeginActivate);
        let effect = activating
            .movement_action(id, MovementAction::Activate, 1)
            .unwrap();
        assert!(
            effect
                .validate_endpoint(request.node(), request.session())
                .is_err()
        );
        assert!(
            AcceptedFleetAction::new(effect, &activating, request.node(), request.session(), 1)
                .is_err()
        );
        assert!(
            AcceptedFleetAction::new(
                request.action().clone(),
                &head,
                request.node(),
                request.session(),
                1
            )
            .is_err()
        );
        let activated = attempt(
            &activating,
            id,
            AttemptEvent::Activated(activation(node, session)),
        );
        assert!(!activated.attempts()[0].receiver_resources_settled());
        assert_eq!(
            activated.reserved_restore_bytes(),
            head.reserved_restore_bytes()
        );
        assert!(activated.retirement_page(&[id]).is_err());
    }
}

#[test]
fn successor_inspection_rejects_source_aliases_and_unreleased_attempts() {
    let (head, request) = successor_request(3, 13);
    for (node, session) in [(1, 13), (3, 11), (3, 12), (0, 13), (3, 0)] {
        assert!(
            FleetInspectionRequest::new(
                request.action().clone(),
                request.registry(),
                request.nonce(),
                NodeId::from_bytes([node; 16]),
                SessionId::from_bytes([session; 16]),
                5_000
            )
            .is_err()
        );
    }
    let (reserved, id) = reserved();
    for state in [
        reserved.clone(),
        attempt(&reserved, id, AttemptEvent::BeginRelease),
    ] {
        assert!(
            FleetInspectionRequest::new(
                state
                    .movement_action(id, MovementAction::Inspect, 0)
                    .unwrap(),
                request.registry(),
                request.nonce(),
                request.node(),
                request.session(),
                5_000
            )
            .is_err()
        );
    }
    let renewed = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([99; 16]),
            30_000,
        )
        .unwrap();
    assert!(
        request
            .authorize_against(&renewed, request.registry(), 30_000)
            .is_err()
    );
}

#[test]
fn successor_inspection_cannot_supply_cleanup_or_a_position_before_release() {
    let (_, request) = successor_request(3, 13);
    let mut behind = activation(3, 13);
    behind.position.root.commit_sequence = release().root.commit_sequence - 1;
    let mut different_root = activation(3, 13);
    different_root.position.root.commit_sequence = release().root.commit_sequence;
    different_root.position.root.digest = Digest::from_bytes([99; 32]);
    for outcome in [
        FleetOutcome::Activated(behind),
        FleetOutcome::Activated(different_root),
        FleetOutcome::ReceiverCleaned,
        FleetOutcome::Reserved(ReceiverReservation {
            session: request.session(),
            expires_at_ms: 10_000,
        }),
    ] {
        assert!(
            FleetInspectionObservation::new(
                request.clone(),
                1,
                FleetActionOutcome {
                    scope: request.action().scope(),
                    action_key: request.action().key().unwrap(),
                    node: request.node(),
                    session: request.session(),
                    observed_at_ms: 3,
                    outcome,
                }
            )
            .is_err()
        );
    }
    for outcome in [
        FleetOutcome::Unknown,
        FleetOutcome::Blocked(DrainBlocker::OutcomeUnknown),
    ] {
        let observed = FleetInspectionObservation::new(
            request.clone(),
            1,
            FleetActionOutcome {
                scope: request.action().scope(),
                action_key: request.action().key().unwrap(),
                node: request.node(),
                session: request.session(),
                observed_at_ms: 3,
                outcome: outcome.clone(),
            },
        );
        assert_eq!(observed.is_ok(), matches!(outcome, FleetOutcome::Unknown));
    }
}

fn request() -> (FleetHead, FleetInspectionRequest) {
    let (head, id) = reserved();
    let request = FleetInspectionRequest::new(
        head.movement_action(id, MovementAction::Inspect, 0)
            .unwrap(),
        RegistryVersion::new(head.scope()).unwrap(),
        Digest::from_bytes([70; 32]),
        spec(1).destination_node,
        spec(1).destination,
        5_000,
    )
    .unwrap();
    (head, request)
}

fn observation(request: &FleetInspectionRequest) -> FleetInspectionObservation {
    FleetInspectionObservation::new(
        request.clone(),
        1,
        FleetActionOutcome {
            scope: request.action().scope(),
            action_key: request.action().key().unwrap(),
            node: request.node(),
            session: request.session(),
            observed_at_ms: 3,
            outcome: FleetOutcome::Activated(activation(2, 12)),
        },
    )
    .unwrap()
}

#[test]
fn inspection_requires_current_head_and_never_authorizes_an_effect() {
    let (head, request) = request();
    request
        .authorize_against(&head, request.registry(), 1)
        .unwrap();
    assert!(matches!(
        request.authorize_against(&head, request.registry(), 5_000),
        Err(OperationError::Deadline)
    ));
    let renewed = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            head.controller().unwrap().claimant,
            2,
        )
        .unwrap();
    assert!(
        request
            .authorize_against(&renewed, request.registry(), 2)
            .is_err()
    );
    assert!(
        request
            .authorize_against(&head, request.registry(), 30_000)
            .is_err()
    );
    let changed_registry = request
        .registry()
        .advance(request.registry().revision())
        .unwrap();
    assert!(
        request
            .authorize_against(&head, changed_registry, 2)
            .is_err()
    );
    let release = attempt(&head, spec(1).id, AttemptEvent::BeginRelease)
        .movement_action(spec(1).id, MovementAction::Release, 0)
        .unwrap();
    assert!(
        FleetInspectionRequest::new(
            release,
            request.registry(),
            request.nonce(),
            spec(1).source_node,
            spec(1).source,
            5_000
        )
        .is_err()
    );
    for (node, session) in [
        (request.node(), SessionId::from_bytes([99; 16])),
        (NodeId::from_bytes([99; 16]), request.session()),
    ] {
        assert!(
            FleetInspectionRequest::new(
                request.action().clone(),
                request.registry(),
                request.nonce(),
                node,
                session,
                5_000
            )
            .is_err()
        );
    }
    assert!(
        FleetInspectionRequest::new(
            request.action().clone(),
            request.registry(),
            Digest::from_bytes([0; 32]),
            request.node(),
            request.session(),
            5_000
        )
        .is_err()
    );
    assert!(
        FleetInspectionRequest::new(
            request.action().clone(),
            request.registry(),
            request.nonce(),
            request.node(),
            request.session(),
            0
        )
        .is_err()
    );
}

#[test]
fn inspection_binds_nonce_full_payload_endpoint_and_original_capture_interval() {
    let (_, request) = request();
    let observed = observation(&request);
    observed.validate_for(&request, 5, 4).unwrap();
    assert!(observed.validate_for(&request, 5, 3).is_err());
    assert!(observed.validate_for(&request, 2, 10).is_err());
    assert!(observed.validate_for(&request, 5, 0).is_err());
    assert!(observed.validate_for(&request, 5_000, 10_000).is_err());
    for field in 0..6 {
        let mut changed = request.clone();
        match field {
            0 => changed.nonce = Digest::from_bytes([71; 32]),
            1 => changed.deadline_ms += 1,
            2 => changed.action.journal_revision += 1,
            3 => changed.action.issued_at_ms += 1,
            4 => {
                changed.registry = changed
                    .registry
                    .advance(changed.registry.revision())
                    .unwrap()
            }
            _ => {
                let FleetActionKind::Movement { attempt, .. } = &mut changed.action.kind else {
                    panic!()
                };
                attempt.spec.cost.disk_bytes += 1;
            }
        }
        assert_eq!(
            request.action().key().unwrap(),
            changed.action().key().unwrap()
        );
        assert_ne!(request.key().unwrap(), changed.key().unwrap());
        assert!(matches!(
            observed.validate_for(&changed, 5, 10),
            Err(OperationError::Conflict)
        ));
    }
    let source = FleetInspectionRequest::new(
        request.action().clone(),
        request.registry(),
        request.nonce(),
        spec(1).source_node,
        spec(1).source,
        5_000,
    )
    .unwrap();
    assert_ne!(request.key().unwrap(), source.key().unwrap());
    assert!(observed.validate_for(&source, 5, 10).is_err());
    assert_eq!(observed.capture_started_at_ms(), 1);
    assert_eq!(observed.capture_finished_at_ms(), 3);
}

#[test]
fn inspection_codec_rejects_malformed_nested_records_and_does_not_restamp_evidence() {
    let (_, request) = request();
    let observed = observation(&request);
    let bytes = request.to_bytes().unwrap();
    let encoded = observed.to_bytes().unwrap();
    assert_eq!(FleetInspectionRequest::from_bytes(&bytes).unwrap(), request);
    let restored = FleetInspectionObservation::from_bytes(&encoded).unwrap();
    assert_eq!(restored, observed);
    assert!(restored.validate_for(&request, 100, 50).is_err());
    for end in 0..bytes.len() {
        assert!(FleetInspectionRequest::from_bytes(&bytes[..end]).is_err());
    }
    for end in 0..encoded.len() {
        assert!(FleetInspectionObservation::from_bytes(&encoded[..end]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(FleetInspectionObservation::from_bytes(&trailing).is_err());
    let mut wrong = encoded;
    wrong[4 + b"cellule.fleet-operation\0".len()] = 255;
    assert!(FleetInspectionObservation::from_bytes(&wrong).is_err());
    let oversized = vec![0; MAX_RECORD_BYTES as usize + 1];
    assert!(FleetInspectionRequest::from_bytes(&oversized).is_err());
    assert!(FleetInspectionObservation::from_bytes(&oversized).is_err());
    assert!(matches!(
        FleetInspectionObservation::from_bytes(&[0]),
        Err(OperationError::Codec(_))
    ));
}

#[test]
fn invalid_observation_origin_and_capture_times_fail_before_encoding() {
    let (_, request) = request();
    let observed = observation(&request);
    for field in 0..6 {
        let mut changed = observed.clone();
        match field {
            0 => changed.capture_started_at_ms = -1,
            1 => changed.capture_started_at_ms = 4,
            2 => changed.outcome.observed_at_ms = request.deadline_ms(),
            3 => changed.outcome.session = spec(1).source,
            4 => changed.outcome.node = spec(1).source_node,
            _ => changed.outcome.action_key = Digest::from_bytes([99; 32]),
        }
        assert!(changed.to_bytes().is_err());
    }
}
