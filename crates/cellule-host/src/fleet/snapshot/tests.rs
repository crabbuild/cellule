use super::*;
use cellule_runtime::fleet::operations::{
    FleetHead, FleetProfile, FleetScope, NodeIntent, OperationError, RegistryVersion,
};

fn barrier() -> (FleetJournalSnapshot, NodeIntent) {
    let scope = FleetScope {
        fleet: Digest::from_bytes([1; 32]),
        application: cellule_runtime::identity::ApplicationId::from_bytes([2; 16]),
    };
    let head = FleetHead::new(scope, 100)
        .unwrap()
        .claim(
            FleetProfile::default(),
            0,
            SessionId::from_bytes([3; 16]),
            100,
        )
        .unwrap();
    (
        FleetJournalSnapshot::new(head, RegistryVersion::new(scope).unwrap()).unwrap(),
        NodeIntent::initial(
            scope,
            NodeId::from_bytes([4; 16]),
            SessionId::from_bytes([5; 16]),
        )
        .unwrap(),
    )
}
fn request(subject: FleetSnapshotSubject) -> FleetSnapshotRequest {
    let (snapshot, intent) = barrier();
    FleetSnapshotRequest::new(
        snapshot,
        Digest::from_bytes([6; 32]),
        intent.node(),
        intent.session(),
        subject,
        2,
        100,
        1_000,
    )
    .unwrap()
}

#[test]
fn every_request_input_binds_the_original_key_and_authorization() {
    let original = request(FleetSnapshotSubject::Cells(None));
    let key = original.key().unwrap();
    let (snapshot, intent) = barrier();
    original.authorize_against(&snapshot, &intent, 100).unwrap();
    let subjects = [
        FleetSnapshotSubject::Host,
        FleetSnapshotSubject::Readers(None),
        FleetSnapshotSubject::ReaderEnrollments(None),
        FleetSnapshotSubject::FollowerLanes(None),
        FleetSnapshotSubject::FollowerEnrollments(None),
        FleetSnapshotSubject::DurabilitySupervisor,
    ];
    for subject in subjects {
        assert_ne!(key, request(subject).key().unwrap());
    }
    for (nonce, node, session, limit, issued, deadline) in [
        (
            Digest::from_bytes([7; 32]),
            original.node(),
            original.session(),
            2,
            100,
            1_000,
        ),
        (
            original.nonce(),
            NodeId::from_bytes([7; 16]),
            original.session(),
            2,
            100,
            1_000,
        ),
        (
            original.nonce(),
            original.node(),
            SessionId::from_bytes([7; 16]),
            2,
            100,
            1_000,
        ),
        (
            original.nonce(),
            original.node(),
            original.session(),
            1,
            100,
            1_000,
        ),
        (
            original.nonce(),
            original.node(),
            original.session(),
            2,
            101,
            1_000,
        ),
        (
            original.nonce(),
            original.node(),
            original.session(),
            2,
            100,
            1_001,
        ),
    ] {
        let changed = FleetSnapshotRequest::new(
            snapshot.clone(),
            nonce,
            node,
            session,
            original.subject().clone(),
            limit,
            issued,
            deadline,
        )
        .unwrap();
        assert_ne!(key, changed.key().unwrap());
    }
    let changed = FleetJournalSnapshot::new(
        snapshot.head().clone(),
        snapshot
            .registry()
            .advance(snapshot.registry().revision())
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        original.authorize_against(&changed, &intent, 100),
        Err(OperationError::Conflict)
    ));
    let changed_request = FleetSnapshotRequest::new(
        changed,
        original.nonce(),
        original.node(),
        original.session(),
        original.subject().clone(),
        2,
        100,
        1_000,
    )
    .unwrap();
    assert_ne!(key, changed_request.key().unwrap());
    let replaced = NodeIntent::initial(
        intent.scope(),
        intent.node(),
        SessionId::from_bytes([8; 16]),
    )
    .unwrap();
    assert!(matches!(
        original.authorize_against(&snapshot, &replaced, 100),
        Err(OperationError::Conflict)
    ));
    assert!(matches!(
        original.authorize_against(&snapshot, &intent, 99),
        Err(OperationError::Deadline)
    ));
    assert!(matches!(
        original.authorize_against(&snapshot, &intent, 1_000),
        Err(OperationError::Deadline)
    ));
}

#[test]
fn native_continuations_bounds_and_controller_deadline_are_checked() {
    let original = request(FleetSnapshotSubject::Cells(None));
    let cursor = cellule_runtime::cell::actor::CellInventoryCursor::from_bytes(&[9; 64]).unwrap();
    let continued = request(FleetSnapshotSubject::Cells(Some(cursor)));
    assert_ne!(original.key().unwrap(), continued.key().unwrap());
    for (subject, limit, nonce, issued, deadline) in [
        (
            FleetSnapshotSubject::Cells(None),
            0,
            original.nonce(),
            100,
            1_000,
        ),
        (
            FleetSnapshotSubject::Cells(None),
            129,
            original.nonce(),
            100,
            1_000,
        ),
        (
            FleetSnapshotSubject::FollowerEnrollments(None),
            33,
            original.nonce(),
            100,
            1_000,
        ),
        (
            FleetSnapshotSubject::Host,
            1,
            Digest::from_bytes([0; 32]),
            100,
            1_000,
        ),
        (FleetSnapshotSubject::Host, 1, original.nonce(), -1, 1_000),
        (FleetSnapshotSubject::Host, 1, original.nonce(), 100, 100),
        (FleetSnapshotSubject::Host, 1, original.nonce(), 100, 30_101),
    ] {
        assert!(
            FleetSnapshotRequest::new(
                original.expected().clone(),
                nonce,
                original.node(),
                original.session(),
                subject,
                limit,
                issued,
                deadline
            )
            .is_err()
        );
    }
    let lease = original.expected().head().controller().unwrap();
    assert!(
        FleetSnapshotRequest::new(
            original.expected().clone(),
            original.nonce(),
            original.node(),
            original.session(),
            FleetSnapshotSubject::Host,
            1,
            lease.expires_at_ms - 1,
            lease.expires_at_ms + 1
        )
        .is_err()
    );
}

#[test]
fn response_replay_cannot_restamp_its_original_interval_or_claim_unbound_absence() {
    let original = request(FleetSnapshotSubject::Readers(None));
    let response = FleetNodeSnapshot {
        request: original.clone(),
        started_at_ms: 101,
        finished_at_ms: 102,
        state_before: NodeState::Ready,
        state_after: NodeState::Draining,
        mode: NodeMode::Draining,
        bindings: FleetSnapshotBindings {
            managed_startup: false,
            readers: false,
            follower_store: false,
            durability_supervisor: false,
            follower_producer: false,
        },
        node_log: None,
        action_work: super::super::FleetActionWorkSnapshot::for_request(&original, 101),
        page: FleetSnapshotNativePage::Unbound,
    };
    response.validate(&original, 102).unwrap();
    assert!(matches!(response.page(), FleetSnapshotNativePage::Unbound));
    assert_eq!(response.state_before(), NodeState::Ready);
    assert_eq!(response.state_after(), NodeState::Draining);
    assert!(response.validate(&original, 101).is_err());
    assert!(response.validate(&original, 1_000).is_err());
    assert!(
        response
            .validate(&request(FleetSnapshotSubject::Host), 102)
            .is_err()
    );
}
