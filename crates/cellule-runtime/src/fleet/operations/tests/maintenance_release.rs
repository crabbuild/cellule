use super::*;

fn maintenance_reserved() -> (FleetHead, AttemptId) {
    let (head, id) = reserved();
    let head = transition(&head, JournalTransition::BeginMaintenance(maintenance()));
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    (head, id)
}

#[test]
fn busy_release_requires_exact_evacuating_maintenance_intent() {
    let (head, id) = reserved();
    rejected(&head, id, AttemptEvent::BeginMaintenanceRelease);
    let requested = transition(&head, JournalTransition::BeginMaintenance(maintenance()));
    rejected(&requested, id, AttemptEvent::BeginMaintenanceRelease);
    let (head, id) = maintenance_reserved();
    for field in 0..3 {
        let mut changed = head.clone();
        let operation = changed.maintenance.as_mut().unwrap();
        match field {
            0 => operation.id = operation_id(99),
            1 => operation.node = NodeId::from_bytes([99; 16]),
            _ => operation.session = SessionId::from_bytes([99; 16]),
        }
        rejected(&changed, id, AttemptEvent::BeginMaintenanceRelease);
    }
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            10_000,
            JournalTransition::Attempt {
                id,
                event: AttemptEvent::BeginMaintenanceRelease
            }
        )
        .is_err()
    );
    assert_eq!(head.attempts()[0].phase(), AttemptPhase::Reserved);
    assert_eq!(head.reserved_restore_bytes(), 4096);
}

#[test]
fn busy_release_codec_and_results_retain_distinct_dispatch_identity() {
    let (head, id) = maintenance_reserved();
    let busy = attempt(&head, id, AttemptEvent::BeginMaintenanceRelease);
    assert_eq!(
        busy.attempts()[0].phase(),
        AttemptPhase::MaintenanceReleasing
    );
    assert_eq!(AttemptPhase::MaintenanceReleasing as u8, 13);
    assert_eq!(MovementAction::ReleaseMaintenance as u8, 8);
    assert_eq!(
        FleetHead::from_bytes(&busy.to_bytes().unwrap()).unwrap(),
        busy
    );
    let action = busy
        .movement_action(id, MovementAction::ReleaseMaintenance, 0)
        .unwrap();
    assert!(
        busy.movement_action(id, MovementAction::Release, 0)
            .is_err()
    );
    let accepted =
        AcceptedFleetAction::new(action, &busy, spec(1).source_node, spec(1).source, 0).unwrap();
    assert_eq!(
        AcceptedFleetAction::from_bytes(&accepted.to_bytes().unwrap()).unwrap(),
        accepted
    );
    assert!(
        AcceptedFleetAction::new(
            accepted.action().clone(),
            &busy,
            spec(1).destination_node,
            spec(1).destination,
            0
        )
        .is_err()
    );
    let result = FleetActionOutcome {
        scope: busy.scope(),
        action_key: accepted.action().key().unwrap(),
        node: spec(1).source_node,
        session: spec(1).source,
        observed_at_ms: 1,
        outcome: FleetOutcome::Released(release()),
    };
    accepted.validate_result(&result).unwrap();
    assert_eq!(
        FleetActionOutcome::from_bytes(&result.to_bytes().unwrap()).unwrap(),
        result
    );
    let ordinary = attempt(&head, id, AttemptEvent::BeginRelease);
    let ordinary_action = ordinary
        .movement_action(id, MovementAction::Release, 0)
        .unwrap();
    assert_ne!(
        ordinary_action.key().unwrap(),
        accepted.action().key().unwrap()
    );
    let ordinary_accepted = AcceptedFleetAction::new(
        ordinary_action,
        &ordinary,
        spec(1).source_node,
        spec(1).source,
        0,
    )
    .unwrap();
    assert!(ordinary_accepted.validate_result(&result).is_err());
    assert!(
        accepted
            .validate_replay(
                ordinary_accepted.action(),
                spec(1).source_node,
                spec(1).source
            )
            .is_err()
    );
}

#[test]
fn busy_release_unknown_and_absence_keep_permits_and_exact_effect() {
    let (head, id) = maintenance_reserved();
    let head = attempt(&head, id, AttemptEvent::BeginMaintenanceRelease);
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    rejected(&head, id, AttemptEvent::BeginCancel);
    assert_eq!(head.attempts()[0].next_action(), MovementAction::Inspect);
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            1,
            1,
            JournalTransition::ResolveUnaccepted {
                id,
                effect: MovementAction::Release
            }
        )
        .is_err()
    );
    let resolved = head
        .transition(
            FleetProfile::default(),
            head.revision(),
            1,
            1,
            JournalTransition::ResolveUnaccepted {
                id,
                effect: MovementAction::ReleaseMaintenance,
            },
        )
        .unwrap();
    assert_eq!(
        resolved.attempts()[0].phase(),
        AttemptPhase::MaintenanceReleasing
    );
    assert_eq!(resolved.attempts()[0].blocker(), None);
    assert_eq!(resolved.reserved_restore_bytes(), 4096);
    let expired = resolved
        .transition(
            FleetProfile::default(),
            resolved.revision(),
            1,
            10_000,
            JournalTransition::ResolveUnaccepted {
                id,
                effect: MovementAction::ReleaseMaintenance,
            },
        )
        .unwrap();
    assert_eq!(expired.attempts()[0].phase(), AttemptPhase::Reserved);
    assert_eq!(
        expired.attempts()[0].blocker(),
        Some(DrainBlocker::Deadline)
    );
    assert!(expired.attempts()[0].released().is_none());
    assert_eq!(expired.reserved_restore_bytes(), 4096);
}

#[test]
fn controller_replacement_adopts_busy_release_without_changing_policy() {
    let (head, id) = maintenance_reserved();
    let head = attempt(&head, id, AttemptEvent::BeginMaintenanceRelease);
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    let restored = FleetHead::from_bytes(&head.to_bytes().unwrap()).unwrap();
    let successor = restored
        .claim(
            FleetProfile::default(),
            restored.revision(),
            SessionId::from_bytes([99; 16]),
            30_000,
        )
        .unwrap();
    assert_eq!(successor.attempts(), head.attempts());
    assert_eq!(successor.reserved_restore_bytes(), 4096);
    let released = attempt(&successor, id, AttemptEvent::Released(release()));
    assert_eq!(released.attempts()[0].phase(), AttemptPhase::Released);
    assert_eq!(released.attempts()[0].released(), Some(&release()));
    let recovering = attempt(&successor, id, AttemptEvent::BeginRecover);
    assert_eq!(recovering.attempts()[0].phase(), AttemptPhase::Recovering);
    assert_eq!(recovering.reserved_restore_bytes(), 4096);
}

#[test]
fn retained_ordinary_release_is_not_reinterpreted_by_new_maintenance() {
    let (head, id) = reserved();
    let ordinary = attempt(&head, id, AttemptEvent::BeginRelease);
    let action = ordinary
        .movement_action(id, MovementAction::Release, 0)
        .unwrap();
    let newer = transition(
        &ordinary,
        JournalTransition::BeginMaintenance(maintenance()),
    );
    assert_eq!(newer.attempts()[0].phase(), AttemptPhase::Releasing);
    assert!(
        newer
            .movement_action(id, MovementAction::ReleaseMaintenance, 0)
            .is_err()
    );
    let replay = newer
        .movement_action(id, MovementAction::Release, 0)
        .unwrap();
    assert_eq!(action.key().unwrap(), replay.key().unwrap());
    assert_eq!(action.kind(), replay.kind());
}
