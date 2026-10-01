use super::*;

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
