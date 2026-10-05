use super::*;
use cellule_runtime::fleet::operations::{
    FleetHead, FleetProfile, FleetScope, JournalTransition, MaintenanceEvent, OperationId,
};
use cellule_runtime::identity::{ApplicationId, Digest, NodeId, SessionId};

fn scope() -> FleetScope {
    FleetScope {
        fleet: Digest::from_bytes([1; 32]),
        application: ApplicationId::from_bytes([2; 16]),
    }
}

fn next(head: &FleetHead, transition: JournalTransition, now_ms: i64) -> FleetHead {
    head.transition(
        FleetProfile::default(),
        head.revision(),
        head.controller().unwrap().epoch,
        now_ms,
        transition,
    )
    .unwrap()
}

fn proof() -> (FleetRoleSettlement, FleetAction, FleetHead) {
    let scope = scope();
    let head = FleetHead::new(scope, 0)
        .unwrap()
        .claim(
            FleetProfile::default(),
            0,
            SessionId::from_bytes([3; 16]),
            0,
        )
        .unwrap();
    let operation = MaintenanceOperation::new(
        OperationId::from_bytes([4; 16]).unwrap(),
        Digest::from_bytes([5; 32]),
        NodeId::from_bytes([6; 16]),
        SessionId::from_bytes([7; 16]),
        1,
        0,
        10_000,
    )
    .unwrap();
    let head = next(&head, JournalTransition::BeginMaintenance(operation), 0);
    let head = next(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        0,
    );
    let head = next(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
        0,
    );
    let action = head
        .maintenance_action(MaintenanceAction::SettleRoles, 10)
        .unwrap();
    let registry = RegistryVersion::new(scope).unwrap().bootstrap(0).unwrap();
    let snapshot = crate::fleet::FleetJournalSnapshot::new(head.clone(), registry).unwrap();
    let operation = snapshot.head().maintenance().unwrap().clone();
    let settlement = FleetRoleSettlement {
        scope,
        action_key: action.key().unwrap(),
        operation,
        snapshot,
        inventory: Digest::from_bytes([8; 32]),
        failed_boot_closure: None,
        capture_interval: (0, 10),
    };
    (settlement, action, head)
}

#[test]
fn settlement_binds_exact_settle_action_and_snapshot_head() {
    let (settlement, action, _) = proof();
    settlement.validate_for(&action).unwrap();
    assert_eq!(settlement.failed_boot_closure(), None);
    assert_eq!(settlement.head_revision(), action.journal_revision());
}

#[test]
fn settlement_rejects_an_invalid_closed_boot_marker() {
    let (mut settlement, action, _) = proof();
    settlement.failed_boot_closure = Some(Digest::from_bytes([0; 32]));
    assert!(settlement.validate_for(&action).is_err());
}

#[test]
fn settlement_cannot_be_replayed_after_head_changes_with_same_registry() {
    let (settlement, action, head) = proof();
    let next_head = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([3; 16]),
            11,
        )
        .unwrap();
    let replay = next_head
        .maintenance_action(MaintenanceAction::SettleRoles, 11)
        .unwrap();
    assert_eq!(action.key().unwrap(), replay.key().unwrap());
    assert_ne!(action.journal_revision(), replay.journal_revision());
    assert!(settlement.validate_for(&replay).is_err());
}
