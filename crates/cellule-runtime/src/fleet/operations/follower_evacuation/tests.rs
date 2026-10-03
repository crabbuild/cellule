use super::*;
use crate::identity::{ApplicationId, NodeId, SessionId};

fn record() -> FollowerEvacuationRecord {
    let scope = FleetScope {
        fleet: Digest::from_bytes([100; 32]),
        application: ApplicationId::from_bytes([9; 16]),
    };
    let source = NodeIntent::initial(
        scope,
        NodeId::from_bytes([1; 16]),
        SessionId::from_bytes([11; 16]),
    )
    .unwrap();
    let member = |node, epoch| {
        let intent = NodeIntent::initial(
            scope,
            NodeId::from_bytes([node; 16]),
            SessionId::from_bytes([node + 10; 16]),
        )
        .unwrap();
        EnrollmentRecord::pending(
            EnrollmentSpec {
                scope,
                request: Digest::from_bytes([node + epoch as u8 * 10; 32]),
                source: Some(EnrollmentEndpoint {
                    node: source.node(),
                    session: source.session(),
                    intent_revision: 1,
                }),
                target: EnrollmentEndpoint {
                    node: intent.node(),
                    session: intent.session(),
                    intent_revision: 1,
                },
                role: EnrollmentRole::Follower { log_epoch: epoch },
            },
            Some(&source),
            &intent,
            10,
        )
        .unwrap()
        .establish(Digest::from_bytes([70; 32]), 20)
        .unwrap()
    };
    let original = member(2, 1);
    let retired = vec![
        original.retire(Digest::from_bytes([71; 32]), 30).unwrap(),
        member(3, 1)
            .retire(Digest::from_bytes([72; 32]), 30)
            .unwrap(),
    ];
    let mut operation = MaintenanceOperation::new(
        OperationId::from_bytes([80; 16]).unwrap(),
        Digest::from_bytes([81; 32]),
        NodeId::from_bytes([2; 16]),
        SessionId::from_bytes([12; 16]),
        2,
        0,
        60_000,
    )
    .unwrap();
    operation.apply(MaintenanceEvent::Cordoned, 0).unwrap();
    operation
        .apply(MaintenanceEvent::BeginEvacuation, 0)
        .unwrap();
    FollowerEvacuationRecord::new(
        operation,
        (
            Digest::from_bytes([82; 32]),
            RegistryVersion::new(scope).unwrap().bootstrap(0).unwrap(),
        ),
        FollowerReplacementPolicy::new(scope, 1, 2).unwrap(),
        (
            original.spec().key().unwrap(),
            Digest::from_bytes(*blake3::hash(&original.to_bytes().unwrap()).as_bytes()),
        ),
        retired,
        8,
        Digest::from_bytes([83; 32]),
        (2, Digest::from_bytes([84; 32])),
        vec![
            FollowerReplacementWitness {
                enrollment: member(3, 2),
                boot_identity: Digest::from_bytes([85; 32]),
            },
            FollowerReplacementWitness {
                enrollment: member(4, 2),
                boot_identity: Digest::from_bytes([86; 32]),
            },
        ],
        (40, 50),
    )
    .unwrap()
}

#[test]
fn follower_policy_record_roundtrips_complete_original_and_replacement_ensembles() {
    let record = record();
    assert_eq!(
        FollowerEvacuationRecord::from_bytes(&record.to_bytes().unwrap()).unwrap(),
        record
    );
    assert_eq!(
        FollowerReplacementPolicy::from_bytes(&record.policy().to_bytes().unwrap()).unwrap(),
        record.policy()
    );
    assert_eq!(record.retired().len(), 2);
    assert_eq!(record.replacements().len(), 2);
    assert_ne!(record.original_digest(), record.digest().unwrap());
}
#[test]
fn follower_policy_record_rejects_missing_duplicate_foreign_and_unretired_members() {
    let original = record();
    for mutation in 0..7 {
        let mut row = original.clone();
        match mutation {
            0 => {
                row.retired.remove(0);
            }
            1 => {
                row.retired[1] = row.retired[0].clone();
            }
            2 => {
                row.retired[0].status = EnrollmentStatus::Established;
                row.retired[0].settlement = None;
            }
            3 => {
                row.retired[0].spec.source.as_mut().unwrap().session =
                    SessionId::from_bytes([88; 16]);
            }
            4 => {
                row.replacements[1] = row.replacements[0].clone();
            }
            5 => {
                row.replacements[0].enrollment.spec.target.node = row.operation.node();
            }
            _ => {
                row.replacements[0].enrollment.spec.role =
                    EnrollmentRole::Follower { log_epoch: 1 };
            }
        };
        assert!(row.to_bytes().is_err(), "mutation {mutation}");
    }
}
#[test]
fn follower_policy_record_rejects_stale_revision_scope_epoch_and_capture_times() {
    let original = record();
    for mutation in 0..9 {
        let mut row = original.clone();
        match mutation {
            0 => row.policy.revision = 0,
            1 => row.policy.minimum_members = 0,
            2 => row.policy.minimum_members = 3,
            3 => row.registry = RegistryVersion::new(row.policy.scope).unwrap(),
            4 => row.policy.scope.fleet = Digest::from_bytes([89; 32]),
            5 => row.replacement_epoch = 1,
            6 => row.head_digest = Digest::from_bytes([0; 32]),
            7 => row.finished_at_ms = 30_041,
            _ => row.finished_at_ms = 39,
        }
        assert!(row.to_bytes().is_err(), "mutation {mutation}");
    }
}
#[test]
fn follower_policy_record_preserves_retirement_across_deadline_and_boot_adoption() {
    let mut row = record();
    let retired = row.retired.clone();
    row.operation
        .apply(
            MaintenanceEvent::SessionReplaced(SessionId::from_bytes([90; 16])),
            40,
        )
        .unwrap();
    row.operation.apply(MaintenanceEvent::Cordoned, 40).unwrap();
    row.operation
        .apply(MaintenanceEvent::BeginEvacuation, 40)
        .unwrap();
    row.operation
        .apply(MaintenanceEvent::ExtendDeadline(70_000), 40)
        .unwrap();
    row.operation
        .apply(
            MaintenanceEvent::ReadyToClose(DrainEvidence {
                node: row.operation.node(),
                session: row.operation.session(),
                remaining_cells: 0,
                unresolved_attempts: 0,
                relocated: true,
                readers_settled: true,
                followers_settled: true,
                facilities_closed: false,
                stopped: false,
                withdrawn: false,
            }),
            40,
        )
        .unwrap();
    assert_eq!(
        FollowerEvacuationRecord::from_bytes(&row.to_bytes().unwrap())
            .unwrap()
            .retired(),
        retired
    );
}
#[test]
fn follower_policy_codecs_reject_truncation_trailing_unknown_and_oversized_envelopes() {
    let record = record();
    let bytes = record.to_bytes().unwrap();
    for length in 0..bytes.len() {
        assert!(FollowerEvacuationRecord::from_bytes(&bytes[..length]).is_err());
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(FollowerEvacuationRecord::from_bytes(&extra).is_err());
    let policy = record.policy.to_bytes().unwrap();
    assert!(FollowerEvacuationRecord::from_bytes(&policy).is_err());
    assert!(FollowerReplacementPolicy::from_bytes(&bytes).is_err());
    assert!(FollowerEvacuationRecord::from_bytes(&vec![0; MAX_PAGE_BYTES as usize + 1]).is_err());
    assert!(
        FollowerReplacementPolicy::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err()
    );
}
