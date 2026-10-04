use super::*;

fn resolve(head: &FleetHead, id: AttemptId, effect: MovementAction, now: i64) -> FleetHead {
    head.transition(
        FleetProfile::default(),
        head.revision(),
        head.controller().unwrap().epoch,
        now,
        JournalTransition::ResolveUnaccepted { id, effect },
    )
    .unwrap()
}

#[test]
fn atomic_absence_resolution_keeps_resources_and_fences_old_authorization() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let delayed = head
        .movement_action(id, MovementAction::Release, 0)
        .unwrap();
    let unmarked = resolve(&head, id, MovementAction::Release, 0);
    assert_eq!(unmarked.revision(), head.revision() + 1);
    assert_eq!(unmarked.attempts(), head.attempts());
    assert!(
        AcceptedFleetAction::new(
            delayed.clone(),
            &unmarked,
            spec(1).source_node,
            spec(1).source,
            0
        )
        .is_err()
    );
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    let resolved = resolve(&head, id, MovementAction::Release, 1);
    assert_eq!(resolved.revision(), head.revision() + 1);
    assert_eq!(resolved.attempts()[0].phase(), AttemptPhase::Releasing);
    assert_eq!(resolved.attempts()[0].blocker(), None);
    assert_eq!(
        resolved.reserved_restore_bytes(),
        head.reserved_restore_bytes()
    );
    assert_eq!(
        resolved.attempts()[0].reservation(),
        head.attempts()[0].reservation()
    );
    assert!(resolved.attempts()[0].released().is_none());
    assert!(
        AcceptedFleetAction::new(delayed, &resolved, spec(1).source_node, spec(1).source, 1)
            .is_err()
    );
    let fresh = resolved
        .movement_action(id, MovementAction::Release, 1)
        .unwrap();
    assert!(
        AcceptedFleetAction::new(fresh, &resolved, spec(1).source_node, spec(1).source, 1).is_ok()
    );
}

#[test]
fn expired_unaccepted_release_returns_to_cleanup_without_fabricating_release() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    let resolved = resolve(&head, id, MovementAction::Release, spec(1).deadline_ms);
    let row = &resolved.attempts()[0];
    assert_eq!(row.phase(), AttemptPhase::Reserved);
    assert_eq!(row.blocker(), Some(DrainBlocker::Deadline));
    assert!(row.released().is_none() && row.activated().is_none());
    assert!(!row.can_retire());
    assert_eq!(resolved.reserved_restore_bytes(), 4096);
    let cancelled = attempt(&resolved, id, AttemptEvent::BeginCancel);
    assert_eq!(cancelled.attempts()[0].phase(), AttemptPhase::Cancelling);
    assert_eq!(cancelled.reserved_restore_bytes(), 4096);
}

#[test]
fn absence_resolution_matches_dispatch_phase_and_does_not_extend_admission() {
    let spec = spec(1);
    let id = spec.id;
    let planned = transition(&head(), JournalTransition::Allocate(spec.clone()));
    assert!(
        planned
            .transition(
                FleetProfile::default(),
                planned.revision(),
                1,
                0,
                JournalTransition::ResolveUnaccepted {
                    id,
                    effect: MovementAction::Prepare
                }
            )
            .is_err()
    );
    let preparing = attempt(&planned, id, AttemptEvent::BeginPrepare);
    assert!(
        preparing
            .transition(
                FleetProfile::default(),
                preparing.revision(),
                1,
                0,
                JournalTransition::ResolveUnaccepted {
                    id,
                    effect: MovementAction::Release
                }
            )
            .is_err()
    );
    let expired = resolve(&preparing, id, MovementAction::Prepare, spec.deadline_ms);
    assert_eq!(expired.attempts()[0].phase(), AttemptPhase::Cancelling);
    assert_eq!(expired.attempts()[0].spec().deadline_ms, spec.deadline_ms);
    assert_eq!(expired.reserved_restore_bytes(), 4096);
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    let activated = resolve(&head, id, MovementAction::Activate, spec.deadline_ms);
    assert_eq!(activated.attempts()[0].phase(), AttemptPhase::Activating);
    assert_eq!(
        activated.attempts()[0].released(),
        head.attempts()[0].released()
    );
    assert!(activated.attempts()[0].activated().is_none());
    assert_eq!(activated.reserved_restore_bytes(), 4096);
}
