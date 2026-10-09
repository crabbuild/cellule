use super::*;
use crate::node::NodeMode;

fn cancelled(head: &FleetHead, sequence: u64) -> FleetHead {
    let spec = spec(sequence);
    let id = spec.id;
    let head = transition(head, JournalTransition::Allocate(spec));
    let head = attempt(&head, id, AttemptEvent::BeginCancel);
    attempt(&head, id, AttemptEvent::Cancelled)
}

#[test]
fn retirement_publishes_exact_history_atomically_and_extends_it_after_restart() {
    let head = cancelled(&head(), 1);
    let id = spec(1).id;
    let page = head.retirement_page(&[id]).unwrap();
    assert_eq!(page.sequence(), 1);
    assert_eq!(page.previous(), None);
    assert_eq!(page.entries()[0].completed_at_ms(), Some(0));
    assert_eq!(
        ProgressPage::from_bytes(&page.to_bytes().unwrap()).unwrap(),
        page
    );
    let digest = page.digest().unwrap();
    let old_revision = head.revision();
    let retired = transition(
        &head,
        JournalTransition::Retire {
            progress: page.clone(),
        },
    );
    assert!(retired.attempts().is_empty());
    assert_eq!(
        retired.progress(),
        Some(ProgressHead {
            digest,
            sequence: 1
        })
    );
    assert_eq!(retired.reserved_restore_bytes(), 0);
    assert_eq!(head.attempts().len(), 1);
    assert!(matches!(
        retired.transition(
            FleetProfile::default(),
            old_revision,
            1,
            0,
            JournalTransition::Retire { progress: page }
        ),
        Err(OperationError::Conflict)
    ));

    let restored = FleetHead::from_bytes(&retired.to_bytes().unwrap()).unwrap();
    let adopted = restored
        .claim(
            FleetProfile::default(),
            restored.revision(),
            SessionId::from_bytes([33; 16]),
            30_000,
        )
        .unwrap();
    let mut next_spec = spec(2);
    next_spec.deadline_ms = 50_000;
    let second = transition(&adopted, JournalTransition::Allocate(next_spec));
    let second = attempt(&second, spec(2).id, AttemptEvent::BeginCancel);
    let second = attempt(&second, spec(2).id, AttemptEvent::Cancelled);
    let page = second.retirement_page(&[spec(2).id]).unwrap();
    assert_eq!(page.previous(), Some(digest));
    assert_eq!(page.sequence(), 2);
    assert_eq!(page.entries()[0].completed_at_ms(), Some(30_000));
    let retired = transition(&second, JournalTransition::Retire { progress: page });
    assert_eq!(retired.progress().unwrap().sequence, 2);
}

#[test]
fn retirement_refuses_unknown_unfinished_changed_or_unreachable_history() {
    let allocated = transition(&head(), JournalTransition::Allocate(spec(1)));
    assert!(allocated.retirement_page(&[spec(1).id]).is_err());
    let head = cancelled(&head(), 1);
    assert!(head.retirement_page(&[]).is_err());
    assert!(head.retirement_page(&[spec(2).id]).is_err());
    assert!(head.retirement_page(&[spec(1).id, spec(1).id]).is_err());
    let page = head.retirement_page(&[spec(1).id]).unwrap();
    for invalid in [
        ProgressPage {
            sequence: 2,
            previous: Some(Digest::from_bytes([88; 32])),
            ..page.clone()
        },
        ProgressPage {
            scope: FleetScope {
                fleet: Digest::from_bytes([99; 32]),
                ..page.scope
            },
            ..page.clone()
        },
        ProgressPage {
            entries: vec![MoveAttempt {
                completed_at_ms: Some(1),
                ..page.entries[0].clone()
            }],
            ..page.clone()
        },
    ] {
        assert!(
            head.transition(
                FleetProfile::default(),
                head.revision(),
                1,
                1,
                JournalTransition::Retire { progress: invalid }
            )
            .is_err()
        );
    }
    assert_eq!(head.attempts().len(), 1);
    assert_eq!(head.reserved_restore_bytes(), 4096);
}

#[test]
fn progress_pages_bound_count_before_allocation_and_reject_noncanonical_envelopes() {
    let head = cancelled(&head(), 1);
    let page = head.retirement_page(&[spec(1).id]).unwrap();
    let bytes = page.to_bytes().unwrap();
    for end in 0..bytes.len() {
        assert!(ProgressPage::from_bytes(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(ProgressPage::from_bytes(&trailing).is_err());
    assert!(ProgressPage::from_bytes(&vec![0; MAX_PAGE_BYTES as usize + 1]).is_err());
    let mut future = bytes.clone();
    future[4 + b"cellule.fleet-operation\0".len()] = FORMAT_VERSION + 1;
    assert!(ProgressPage::from_bytes(&future).is_err());
    let mut count = bytes;
    // First page ends its fixed header with count, then canonical attempt bytes.
    let count_offset =
        4 + b"cellule.fleet-operation\0".len() + 2 + (4 + 32) + (4 + 16) + (4 + 16) + 8 + 1;
    count[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        ProgressPage::from_bytes(&count),
        Err(OperationError::Codec(crate::codec::CodecError::Limit))
    ));
}

#[test]
fn node_intent_retains_cordon_until_exact_completed_revision_and_new_boot() {
    let scope = head().scope();
    let initial =
        NodeIntent::initial(scope, maintenance().node(), maintenance().session()).unwrap();
    assert_eq!(initial.mode(), NodeMode::Active);
    assert_eq!(
        NodeIntent::from_bytes(&initial.to_bytes().unwrap()).unwrap(),
        initial
    );
    let mut operation = maintenance();
    let intent = NodeIntent::maintenance(scope, &operation).unwrap();
    assert_eq!(intent.mode(), NodeMode::Draining);
    assert_eq!(
        NodeIntent::from_bytes(&intent.to_bytes().unwrap()).unwrap(),
        intent
    );
    let new_boot = SessionId::from_bytes([77; 16]);
    assert!(intent.return_to_service(&operation, new_boot, 2).is_err());
    operation.apply(MaintenanceEvent::Cordoned, 0).unwrap();
    operation
        .apply(MaintenanceEvent::BeginEvacuation, 0)
        .unwrap();
    operation
        .apply(MaintenanceEvent::ReadyToClose(closing_evidence()), 0)
        .unwrap();
    operation
        .apply(MaintenanceEvent::Stopped(drain_evidence()), 0)
        .unwrap();
    assert!(intent.return_to_service(&operation, new_boot, 1).is_err());
    assert!(
        intent
            .return_to_service(&operation, intent.session(), 2)
            .is_err()
    );
    let active = intent.return_to_service(&operation, new_boot, 2).unwrap();
    assert_eq!(active.mode(), NodeMode::Active);
    assert_eq!(active.operation(), None);
    let stale = NodeIntent {
        revision: 2,
        ..intent
    };
    assert!(stale.return_to_service(&operation, new_boot, 3).is_err());
}

#[test]
fn action_requires_persisted_dispatch_current_revision_live_controller_and_deadline() {
    let head = transition(&head(), JournalTransition::Allocate(spec(1)));
    let id = spec(1).id;
    assert!(
        head.movement_action(id, MovementAction::Prepare, 0)
            .is_err()
    );
    let head = attempt(&head, id, AttemptEvent::BeginPrepare);
    let action = head
        .movement_action(id, MovementAction::Prepare, 0)
        .unwrap();
    action.authorize_against(&head, 1).unwrap();
    assert_eq!(
        FleetAction::from_bytes(&action.to_bytes().unwrap()).unwrap(),
        action
    );
    let advanced = attempt(&head, id, AttemptEvent::OutcomeUnknown);
    assert!(matches!(
        action.authorize_against(&advanced, 1),
        Err(OperationError::Conflict)
    ));
    assert!(
        advanced
            .movement_action(id, MovementAction::Prepare, 1)
            .is_err()
    );
    assert!(
        advanced
            .movement_action(id, MovementAction::Inspect, 1)
            .is_ok()
    );
    assert!(matches!(
        action.authorize_against(&head, 10_000),
        Err(OperationError::Deadline)
    ));
    assert!(matches!(
        action.authorize_against(&head, 30_000),
        Err(OperationError::Fenced)
    ));
    let mut forged = action.clone();
    forged.controller_epoch += 1;
    assert!(matches!(
        forged.authorize_against(&head, 1),
        Err(OperationError::Fenced)
    ));
    forged = action;
    forged.scope.fleet = Digest::from_bytes([55; 32]);
    assert!(matches!(
        forged.authorize_against(&head, 1),
        Err(OperationError::Conflict)
    ));
}

#[test]
fn action_key_survives_controller_adoption_but_not_another_generation() {
    let mut spec = spec(1);
    spec.deadline_ms = 80_000;
    let id = spec.id;
    let head = transition(&head(), JournalTransition::Allocate(spec));
    let head = attempt(&head, id, AttemptEvent::BeginPrepare);
    let first = head
        .movement_action(id, MovementAction::Prepare, 0)
        .unwrap();
    let adopted = head
        .claim(
            FleetProfile::default(),
            head.revision(),
            SessionId::from_bytes([33; 16]),
            30_000,
        )
        .unwrap();
    let second = adopted
        .movement_action(id, MovementAction::Prepare, 30_001)
        .unwrap();
    assert_eq!(first.key().unwrap(), second.key().unwrap());
    assert!(first.authorize_against(&adopted, 30_001).is_err());
    second.authorize_against(&adopted, 30_001).unwrap();
    let mut other = second.clone();
    if let FleetActionKind::Movement { attempt, .. } = &mut other.kind {
        attempt.spec.generation += 1;
    }
    assert_ne!(other.key().unwrap(), second.key().unwrap());
    assert!(other.authorize_against(&adopted, 30_001).is_err());
}

fn reply(action: &FleetAction, source: bool, outcome: FleetOutcome) -> FleetActionOutcome {
    FleetActionOutcome {
        scope: action.scope(),
        action_key: action.key().unwrap(),
        node: NodeId::from_bytes([if source { 1 } else { 2 }; 16]),
        session: SessionId::from_bytes([if source { 11 } else { 12 }; 16]),
        observed_at_ms: 1,
        outcome,
    }
}

#[test]
fn action_results_bind_effect_and_authenticated_endpoint_including_failures() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let action = head
        .movement_action(id, MovementAction::Release, 0)
        .unwrap();
    for outcome in [
        FleetOutcome::Unknown,
        FleetOutcome::Blocked(DrainBlocker::BusyExecution),
        FleetOutcome::Rejected(DrainBlocker::ReceiverCapacity),
    ] {
        reply(&action, true, outcome.clone())
            .validate_for(&action)
            .unwrap();
        assert!(
            reply(&action, false, outcome)
                .validate_for(&action)
                .is_err()
        );
    }
    let released = reply(&action, true, FleetOutcome::Released(release()));
    released.validate_for(&action).unwrap();
    assert_eq!(
        FleetActionOutcome::from_bytes(&released.to_bytes().unwrap()).unwrap(),
        released
    );
    assert!(
        reply(&action, false, FleetOutcome::Released(release()))
            .validate_for(&action)
            .is_err()
    );
    assert!(
        reply(&action, true, FleetOutcome::ReceiverCleaned)
            .validate_for(&action)
            .is_err()
    );
    assert!(
        reply(
            &action,
            true,
            FleetOutcome::Blocked(DrainBlocker::OutcomeUnknown)
        )
        .to_bytes()
        .is_err()
    );
    assert!(
        reply(
            &action,
            true,
            FleetOutcome::Rejected(DrainBlocker::OutcomeUnknown)
        )
        .to_bytes()
        .is_err()
    );
}

#[test]
fn activation_reply_cannot_impersonate_a_rebooted_receiver_or_the_source() {
    let (head, id) = reserved();
    let head = attempt(&head, id, AttemptEvent::BeginRelease);
    let head = attempt(&head, id, AttemptEvent::Released(release()));
    let head = attempt(&head, id, AttemptEvent::BeginActivate);
    let action = head
        .movement_action(id, MovementAction::Activate, 0)
        .unwrap();
    let actual = reply(&action, false, FleetOutcome::Activated(activation(2, 12)));
    actual.validate_for(&action).unwrap();
    for (node, session) in [(2, 99), (1, 11)] {
        let evidence = activation(node, session);
        let forged = FleetActionOutcome {
            node: evidence.node,
            session: evidence.session,
            outcome: FleetOutcome::Activated(evidence),
            ..actual.clone()
        };
        assert!(forged.validate_for(&action).is_err());
    }
    let evidence = activation(3, 13);
    FleetActionOutcome {
        node: evidence.node,
        session: evidence.session,
        outcome: FleetOutcome::Activated(evidence),
        ..actual
    }
    .validate_for(&action)
    .unwrap();
}

#[test]
fn maintenance_actions_and_results_require_exact_phase_and_terminal_proof() {
    let head = transition(&head(), JournalTransition::BeginMaintenance(maintenance()));
    assert!(
        head.maintenance_action(MaintenanceAction::Finalize, 0)
            .is_err()
    );
    let cordon = head
        .maintenance_action(MaintenanceAction::Cordon, 0)
        .unwrap();
    reply(&cordon, true, FleetOutcome::Cordoned)
        .validate_for(&cordon)
        .unwrap();
    assert!(
        reply(&cordon, true, FleetOutcome::Stopped(drain_evidence()))
            .validate_for(&cordon)
            .is_err()
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    let settle = head
        .maintenance_action(MaintenanceAction::SettleRoles, 0)
        .unwrap();
    assert!(
        reply(
            &settle,
            true,
            FleetOutcome::RolesSettled {
                inventory: Digest::from_bytes([0; 32])
            }
        )
        .to_bytes()
        .is_err()
    );
    let registry = RegistryVersion::new(settle.scope())
        .unwrap()
        .bootstrap(0)
        .unwrap();
    let settled = reply(
        &settle,
        true,
        FleetOutcome::RolesSettledAt {
            inventory: Digest::from_bytes([88; 32]),
            head_revision: settle.journal_revision(),
            registry,
        },
    );
    assert_eq!(
        FleetActionOutcome::from_bytes(&settled.to_bytes().unwrap()).unwrap(),
        settled
    );
    assert!(
        reply(
            &settle,
            true,
            FleetOutcome::RolesSettledAt {
                inventory: Digest::from_bytes([88; 32]),
                head_revision: settle.journal_revision(),
                registry: RegistryVersion::new(FleetScope {
                    fleet: Digest::from_bytes([17; 32]),
                    application: ApplicationId::from_bytes([18; 16]),
                })
                .unwrap()
                .bootstrap(0)
                .unwrap(),
            },
        )
        .to_bytes()
        .is_err()
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(closing_evidence())),
    );
    let finalize = head
        .maintenance_action(MaintenanceAction::Finalize, 0)
        .unwrap();
    let stopped = reply(&finalize, true, FleetOutcome::Stopped(drain_evidence()));
    stopped.validate_for(&finalize).unwrap();
    assert_eq!(
        FleetActionOutcome::from_bytes(&stopped.to_bytes().unwrap()).unwrap(),
        stopped
    );
    assert!(
        reply(
            &finalize,
            true,
            FleetOutcome::Stopped(DrainEvidence {
                withdrawn: false,
                ..drain_evidence()
            })
        )
        .to_bytes()
        .is_err()
    );
}

#[test]
fn ready_to_close_cannot_claim_host_shutdown_or_withdrawal_before_finalize() {
    let head = transition(&head(), JournalTransition::BeginMaintenance(maintenance()));
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let head = transition(
        &head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    let mut evidence = drain_evidence();
    evidence.facilities_closed = true;
    let epoch = head.controller().unwrap().epoch;
    assert!(
        head.transition(
            FleetProfile::default(),
            head.revision(),
            epoch,
            0,
            JournalTransition::Maintenance(MaintenanceEvent::ReadyToClose(evidence)),
        )
        .is_err()
    );
}

#[test]
fn action_and_intent_codecs_reject_truncation_extra_bytes_and_other_record_families() {
    let head = transition(&head(), JournalTransition::BeginMaintenance(maintenance()));
    let action = head
        .maintenance_action(MaintenanceAction::Cordon, 0)
        .unwrap();
    let intent = head.node_intent().unwrap().unwrap();
    let outcome = reply(&action, true, FleetOutcome::Cordoned);
    for bytes in [
        action.to_bytes().unwrap(),
        intent.to_bytes().unwrap(),
        outcome.to_bytes().unwrap(),
    ] {
        let kind = bytes[5 + b"cellule.fleet-operation\0".len()];
        for end in 0..bytes.len() {
            let slice = &bytes[..end];
            assert!(match kind {
                4 => NodeIntent::from_bytes(slice).is_err(),
                6 => FleetAction::from_bytes(slice).is_err(),
                7 => FleetActionOutcome::from_bytes(slice).is_err(),
                _ => unreachable!(),
            });
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(FleetAction::from_bytes(&trailing).is_err());
        assert!(NodeIntent::from_bytes(&trailing).is_err());
        assert!(FleetActionOutcome::from_bytes(&trailing).is_err());
    }
    assert!(FleetAction::from_bytes(&intent.to_bytes().unwrap()).is_err());
    assert!(NodeIntent::from_bytes(&action.to_bytes().unwrap()).is_err());
}

#[test]
fn receiver_continuation_is_registry_bound_endpoint_bound_and_versioned() {
    let original = spec(1);
    let head = transition(&head(), JournalTransition::Allocate(original.clone()));
    let head = attempt(&head, original.id, AttemptEvent::BeginPrepare);
    let head = attempt(
        &head,
        original.id,
        AttemptEvent::Reserved(ReceiverReservation {
            session: original.destination,
            expires_at_ms: 5_000,
        }),
    );
    let head = attempt(&head, original.id, AttemptEvent::BeginRelease);
    let head = attempt(&head, original.id, AttemptEvent::Released(release()));
    let head = attempt(&head, original.id, AttemptEvent::BeginActivate);
    let registry = RegistryVersion::new(head.scope())
        .unwrap()
        .bootstrap(0)
        .unwrap();
    let first = ReceiverRoute::begin(
        head.scope(),
        &original,
        NodeId::from_bytes([7; 16]),
        SessionId::from_bytes([77; 16]),
        Digest::from_bytes([31; 32]),
        registry,
    )
    .unwrap();
    let next_registry = registry.advance(registry.revision()).unwrap();
    let route = first
        .extend(
            head.scope(),
            &original,
            NodeId::from_bytes([8; 16]),
            SessionId::from_bytes([78; 16]),
            Digest::from_bytes([32; 32]),
            next_registry,
        )
        .unwrap();
    assert_eq!(
        route.target(&original),
        (NodeId::from_bytes([8; 16]), SessionId::from_bytes([78; 16]))
    );
    assert!(
        route
            .extend(
                head.scope(),
                &original,
                NodeId::from_bytes([9; 16]),
                SessionId::from_bytes([79; 16]),
                Digest::from_bytes([33; 32]),
                next_registry.advance(next_registry.revision()).unwrap(),
            )
            .is_err()
    );

    let action = head
        .movement_action_with_receiver_route(
            original.id,
            MovementAction::Activate,
            route.clone(),
            0,
        )
        .unwrap();
    assert!(action.authorize_against(&head, 1).is_err());
    action
        .authorize_against_registry(&head, next_registry, 1)
        .unwrap();
    assert!(
        action
            .authorize_against_registry(&head, registry, 1)
            .is_err()
    );
    let unknown_head = attempt(&head, original.id, AttemptEvent::OutcomeUnknown);
    let adopted = unknown_head
        .movement_action_with_receiver_route(original.id, MovementAction::Activate, route, 0)
        .unwrap();
    adopted
        .authorize_against_registry(&unknown_head, next_registry, 1)
        .unwrap();
    assert!(
        action
            .validate_endpoint(NodeId::from_bytes([2; 16]), original.destination)
            .is_err()
    );
    assert!(
        action
            .validate_endpoint(NodeId::from_bytes([8; 16]), SessionId::from_bytes([78; 16]))
            .is_ok()
    );
    assert_eq!(
        FleetAction::from_bytes(&action.to_bytes().unwrap()).unwrap(),
        action
    );

    let accepted = AcceptedFleetAction::new_with_registry(
        action.clone(),
        &head,
        next_registry,
        NodeId::from_bytes([8; 16]),
        SessionId::from_bytes([78; 16]),
        1,
    )
    .unwrap();
    let result = FleetActionOutcome {
        scope: head.scope(),
        action_key: action.key().unwrap(),
        node: NodeId::from_bytes([8; 16]),
        session: SessionId::from_bytes([78; 16]),
        observed_at_ms: 2,
        outcome: FleetOutcome::Activated(ActivationEvidence {
            node: NodeId::from_bytes([8; 16]),
            session: SessionId::from_bytes([78; 16]),
            position: PublishedPosition {
                incarnation: original.incarnation,
                epoch: original.source_epoch + 1,
                root: release().root,
            },
        }),
    };
    accepted.validate_result(&result).unwrap();
}
