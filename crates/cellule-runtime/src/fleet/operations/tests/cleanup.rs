use super::*;

fn released() -> (FleetHead, AttemptId) {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    (attempt(&head, id, AttemptEvent::Released(release())), id)
}

#[test]
fn receiver_cleanup_after_release_retains_position_and_both_fleet_permits() {
    let (head, id) = released();
    let bytes = head.reserved_restore_bytes();
    let cleaning = attempt(&head, id, AttemptEvent::BeginCancel);
    assert_eq!(
        cleaning.attempts()[0].phase(),
        AttemptPhase::CleaningReceiver
    );
    assert_eq!(cleaning.attempts()[0].next_action(), MovementAction::Cancel);
    assert_eq!(cleaning.attempts()[0].released(), Some(&release()));
    assert_eq!(cleaning.reserved_restore_bytes(), bytes);
    assert_eq!(
        FleetHead::from_bytes(&cleaning.to_bytes().unwrap()).unwrap(),
        cleaning
    );
    let action = cleaning
        .movement_action(id, MovementAction::Cancel, 0)
        .unwrap();
    assert_eq!(
        FleetAction::from_bytes(&action.to_bytes().unwrap()).unwrap(),
        action
    );
    rejected(&cleaning, id, AttemptEvent::Cancelled);
    let cleaned = attempt(&cleaning, id, AttemptEvent::ReceiverCleaned);
    assert_eq!(cleaned.attempts()[0].phase(), AttemptPhase::Released);
    assert_eq!(
        cleaned.attempts()[0].next_action(),
        MovementAction::Activate
    );
    assert_eq!(cleaned.attempts()[0].released(), Some(&release()));
    assert_eq!(cleaned.reserved_restore_bytes(), bytes);
    assert!(cleaned.retirement_page(&[id]).is_err());
    let activated = attempt(&cleaned, id, AttemptEvent::BeginActivate);
    let activated = attempt(&activated, id, AttemptEvent::Activated(activation(2, 12)));
    assert_eq!(
        activated.attempts()[0].next_action(),
        MovementAction::Retire
    );
    assert!(
        transition(&activated, retirement(&activated, id))
            .attempts()
            .is_empty()
    );
}

#[test]
fn preferred_session_serving_is_not_evidence_that_prepared_credit_was_consumed() {
    let (head, id) = released();
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let head = attempt(&head, id, AttemptEvent::Activated(activation(2, 12)));
    assert_eq!(head.attempts()[0].next_action(), MovementAction::Cancel);
    assert!(!head.attempts()[0].can_retire());
    assert!(head.retirement_page(&[id]).is_err());
    let head = attempt(&head, id, AttemptEvent::ReceiverCleaned);
    assert!(head.attempts()[0].can_retire());
    assert_eq!(
        FleetHead::from_bytes(&head.to_bytes().unwrap()).unwrap(),
        head
    );
}

#[test]
fn ambiguous_cleanup_preserves_relocation_until_positive_resource_and_serving_evidence() {
    let (head, id) = released();
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let head = attempt(&head, id, AttemptEvent::BeginCancel);
    let head = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    assert_eq!(head.attempts()[0].next_action(), MovementAction::Inspect);
    assert!(head.movement_action(id, MovementAction::Cancel, 0).is_err());
    rejected(&head, id, AttemptEvent::Cancelled);
    let head = attempt(&head, id, AttemptEvent::ReceiverCleaned);
    assert_eq!(head.attempts()[0].phase(), AttemptPhase::Released);
    assert_eq!(head.attempts()[0].blocker(), None);
    assert!(!head.attempts()[0].can_retire());
}

#[test]
fn activation_finishing_during_cleanup_still_needs_independent_resource_settlement() {
    let (head, id) = released();
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let head = attempt(&head, id, AttemptEvent::BeginCancel);
    let head = attempt(&head, id, AttemptEvent::Activated(activation(2, 12)));
    assert!(!head.attempts()[0].can_retire());
    let head = attempt(&head, id, AttemptEvent::ReceiverCleaned);
    assert!(head.attempts()[0].can_retire());
}
