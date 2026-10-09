use super::*;
use crate::codec::CodecError;
use crate::node::NodeMode;

fn endpoint(byte: u8) -> EnrollmentEndpoint {
    EnrollmentEndpoint {
        node: NodeId::from_bytes([byte; 16]),
        session: SessionId::from_bytes([byte + 10; 16]),
        intent_revision: 1,
    }
}

#[test]
fn unexecuted_refusal_is_a_terminal_exclusion_row_without_role_admission() {
    let spec = enrollment(EnrollmentRole::Follower { log_epoch: 7 });
    let proof = Digest::from_bytes([42; 32]);
    let record = EnrollmentRecord::unexecuted_refusal(spec.clone(), proof, 10).unwrap();
    assert_eq!(record.status(), EnrollmentStatus::Refused);
    assert!(!record.unresolved());
    record.validate_replay(&spec).unwrap();
    assert_eq!(
        EnrollmentRecord::from_bytes(&record.to_bytes().unwrap()).unwrap(),
        record
    );
    assert_eq!(record.refuse(proof, 20).unwrap(), record);
    assert!(record.establish(proof, 20).is_err());
    assert!(record.retire(proof, 20).is_err());
    assert!(
        EnrollmentRecord::unexecuted_refusal(spec.clone(), Digest::from_bytes([0; 32]), 10)
            .is_err()
    );
    assert!(EnrollmentRecord::unexecuted_refusal(spec, proof, -1).is_err());
}

fn intent(byte: u8) -> NodeIntent {
    NodeIntent::initial(head().scope(), endpoint(byte).node, endpoint(byte).session).unwrap()
}

fn enrollment(role: EnrollmentRole) -> EnrollmentSpec {
    EnrollmentSpec {
        scope: head().scope(),
        request: Digest::from_bytes([20; 32]),
        source: (!matches!(role, EnrollmentRole::Node { .. })).then_some(endpoint(1)),
        target: endpoint(2),
        role,
    }
}

fn pending(role: EnrollmentRole) -> EnrollmentRecord {
    let spec = enrollment(role);
    EnrollmentRecord::pending(
        spec.clone(),
        spec.source.map(|_| intent(1)).as_ref(),
        None,
        &intent(2),
        10,
    )
    .unwrap()
}

fn bootstrapped() -> RegistryVersion {
    RegistryVersion::new(head().scope())
        .unwrap()
        .advance(0)
        .unwrap()
        .bootstrap(1)
        .unwrap()
}

fn maintenance_for(node: u8, revision: u64) -> MaintenanceOperation {
    let mut operation = maintenance();
    operation.node = endpoint(node).node;
    operation.session = endpoint(node).session;
    operation.intent_revision = revision;
    operation
}

#[test]
fn retained_intent_revision_blocks_reboot_reopening_and_old_enrollment_checks() {
    let active = intent(2);
    let mut operation = maintenance_for(2, 2);
    let draining = active.advance_maintenance(&operation).unwrap();
    assert_eq!(draining.mode(), NodeMode::Draining);
    assert_eq!(draining.advance_maintenance(&operation).unwrap(), draining);
    assert!(
        draining
            .rebind_active(SessionId::from_bytes([99; 16]), 3)
            .is_err()
    );
    assert!(active.advance_maintenance(&maintenance_for(2, 1)).is_err());
    assert!(active.advance_maintenance(&maintenance_for(1, 2)).is_err());
    let mut foreign = operation.clone();
    foreign.id = operation_id(9);
    foreign.intent_revision = 3;
    assert!(draining.advance_maintenance(&foreign).is_err());

    operation
        .apply(
            MaintenanceEvent::SessionReplaced(SessionId::from_bytes([99; 16])),
            1,
        )
        .unwrap();
    assert_eq!(operation.intent_revision(), 3);
    let rebound = draining.advance_maintenance(&operation).unwrap();
    assert_eq!(rebound.node(), draining.node());
    assert_eq!(rebound.mode(), NodeMode::Draining);
    assert_eq!(rebound.session(), operation.session());
    assert_eq!(rebound.revision(), 3);
    assert_eq!(
        NodeIntent::from_bytes(&rebound.to_bytes().unwrap()).unwrap(),
        rebound
    );
    let mut old_revision = operation.clone();
    old_revision.intent_revision = 2;
    assert!(rebound.advance_maintenance(&old_revision).is_err());
    let mut overflow = operation;
    overflow.intent_revision = u64::MAX;
    assert!(
        overflow
            .apply(
                MaintenanceEvent::SessionReplaced(SessionId::from_bytes([98; 16])),
                2
            )
            .is_err()
    );
    let active_boot = active
        .rebind_active(SessionId::from_bytes([97; 16]), 2)
        .unwrap();
    assert_eq!(active_boot.mode(), NodeMode::Active);
    assert!(active.rebind_active(active.session(), 2).is_err());
    assert!(active.rebind_active(active_boot.session(), 1).is_err());
}

#[test]
fn pending_enrollment_checks_exact_committed_intents_and_allows_draining_donor() {
    let operation = maintenance_for(1, 2);
    let source = intent(1).advance_maintenance(&operation).unwrap();
    let mut spec = enrollment(EnrollmentRole::Follower { log_epoch: 7 });
    assert!(
        EnrollmentRecord::pending(
            spec.clone(),
            Some(&source),
            Some(&operation),
            &intent(2),
            10
        )
        .is_err()
    );
    spec.source.as_mut().unwrap().intent_revision = source.revision();
    let admitted = EnrollmentRecord::pending(
        spec.clone(),
        Some(&source),
        Some(&operation),
        &intent(2),
        10,
    )
    .unwrap();
    assert!(admitted.unresolved());
    assert_eq!(admitted.status(), EnrollmentStatus::Pending);
    let draining_target = intent(2)
        .advance_maintenance(&maintenance_for(2, 2))
        .unwrap();
    spec.target.intent_revision = draining_target.revision();
    assert!(
        EnrollmentRecord::pending(
            spec.clone(),
            Some(&source),
            Some(&operation),
            &draining_target,
            10
        )
        .is_err()
    );
    let cordoned_target = NodeIntent {
        mode: NodeMode::Cordoned,
        ..draining_target
    };
    assert!(
        EnrollmentRecord::pending(
            spec.clone(),
            Some(&source),
            Some(&operation),
            &cordoned_target,
            10
        )
        .is_err()
    );
    let mut boot = enrollment(EnrollmentRole::Node {
        mode: NodeMode::Cordoned,
    });
    boot.target.intent_revision = cordoned_target.revision();
    let enrolled_boot =
        EnrollmentRecord::pending(boot.clone(), None, None, &cordoned_target, 10).unwrap();
    assert!(enrolled_boot.unresolved());
    boot.role = EnrollmentRole::Node {
        mode: NodeMode::Active,
    };
    assert!(EnrollmentRecord::pending(boot, None, None, &cordoned_target, 10).is_err());
    assert!(EnrollmentRecord::pending(spec.clone(), None, None, &intent(2), 10).is_err());
    let foreign = NodeIntent {
        scope: FleetScope {
            fleet: Digest::from_bytes([99; 32]),
            ..source.scope()
        },
        ..source.clone()
    };
    assert!(
        EnrollmentRecord::pending(spec, Some(&foreign), Some(&operation), &intent(2), 10).is_err()
    );
    assert!(
        EnrollmentRecord::pending(
            enrollment(EnrollmentRole::Node {
                mode: NodeMode::Active
            }),
            Some(&intent(1)),
            None,
            &intent(2),
            10
        )
        .is_err()
    );
    assert!(
        EnrollmentRecord::pending(
            enrollment(EnrollmentRole::Node {
                mode: NodeMode::Active
            }),
            None,
            None,
            &intent(2),
            -1
        )
        .is_err()
    );
}

#[test]
fn source_maintenance_phase_fences_new_roles_without_invalidating_replay() {
    for role in [
        EnrollmentRole::Follower { log_epoch: 7 },
        EnrollmentRole::Reader {
            target: spec(1).target,
            position: release(),
        },
    ] {
        let mut operation = maintenance_for(1, 2);
        let source = intent(1).advance_maintenance(&operation).unwrap();
        let mut request = enrollment(role);
        request.source.as_mut().unwrap().intent_revision = source.revision();
        let admit = |operation: &MaintenanceOperation| {
            EnrollmentRecord::pending(
                request.clone(),
                Some(&source),
                Some(operation),
                &intent(2),
                10,
            )
        };
        let original = admit(&operation).unwrap();
        assert!(
            EnrollmentRecord::pending(request.clone(), Some(&source), None, &intent(2), 10)
                .is_err()
        );
        for change in 0..4 {
            let mut foreign = operation.clone();
            match change {
                0 => foreign.id = operation_id(9),
                1 => foreign.node = endpoint(9).node,
                2 => foreign.session = endpoint(9).session,
                _ => foreign.intent_revision += 1,
            }
            assert!(matches!(admit(&foreign), Err(OperationError::Conflict)));
        }
        operation.apply(MaintenanceEvent::Cordoned, 1).unwrap();
        assert!(admit(&operation).is_ok());
        operation
            .apply(MaintenanceEvent::BeginEvacuation, 2)
            .unwrap();
        assert!(admit(&operation).is_ok());
        operation
            .apply(MaintenanceEvent::ReadyToClose(closing_evidence()), 3)
            .unwrap();
        assert!(matches!(admit(&operation), Err(OperationError::Conflict)));
        assert_eq!(source.advance_maintenance(&operation).unwrap(), source);
        let established = original
            .establish(Digest::from_bytes([41; 32]), 11)
            .unwrap();
        established.validate_replay(&request).unwrap();
        operation
            .apply(MaintenanceEvent::Stopped(drain_evidence()), 4)
            .unwrap();
        assert!(matches!(admit(&operation), Err(OperationError::Conflict)));
        let retired = established
            .retire(Digest::from_bytes([42; 32]), 12)
            .unwrap();
        retired.validate_replay(&request).unwrap();
        assert_eq!(retired.accepted_at_ms(), original.accepted_at_ms());
        // Adoption invalidates the old envelope and returns the new boot to the
        // initial maintenance barrier; it does not reopen target admission.
        let mut adopted = maintenance_for(1, 2);
        adopted
            .apply(MaintenanceEvent::SessionReplaced(endpoint(9).session), 5)
            .unwrap();
        assert!(matches!(admit(&adopted), Err(OperationError::Conflict)));
        let rebound = source.advance_maintenance(&adopted).unwrap();
        request.source.as_mut().unwrap().session = rebound.session();
        request.source.as_mut().unwrap().intent_revision = rebound.revision();
        assert!(
            EnrollmentRecord::pending(request, Some(&rebound), Some(&adopted), &intent(2), 10)
                .is_ok()
        );
    }
}

#[test]
fn source_maintenance_input_is_absent_for_active_sources_and_node_enrollment() {
    let operation = maintenance_for(1, 2);
    assert!(matches!(
        EnrollmentRecord::pending(
            enrollment(EnrollmentRole::Follower { log_epoch: 7 }),
            Some(&intent(1)),
            Some(&operation),
            &intent(2),
            10,
        ),
        Err(OperationError::Conflict)
    ));
    assert!(
        EnrollmentRecord::pending(
            enrollment(EnrollmentRole::Node {
                mode: NodeMode::Active
            }),
            None,
            Some(&operation),
            &intent(2),
            10,
        )
        .is_err()
    );
}

#[test]
fn duplicate_enrollment_compares_full_inputs_and_preserves_original_progress_time() {
    let original = pending(EnrollmentRole::Follower { log_epoch: 7 });
    let complete = original
        .establish(Digest::from_bytes([31; 32]), 20)
        .unwrap();
    complete.validate_replay(original.spec()).unwrap();
    assert_eq!(
        complete
            .establish(Digest::from_bytes([31; 32]), 100)
            .unwrap(),
        complete
    );
    assert_eq!(complete.updated_at_ms(), 20);
    assert_eq!(complete.accepted_at_ms(), 10);
    assert!(
        complete
            .establish(Digest::from_bytes([32; 32]), 100)
            .is_err()
    );
    assert!(complete.refuse(Digest::from_bytes([33; 32]), 100).is_err());
    for change in 0..5 {
        let mut spec = original.spec().clone();
        match change {
            0 => spec.role = EnrollmentRole::Follower { log_epoch: 8 },
            1 => spec.source.as_mut().unwrap().intent_revision += 1,
            2 => spec.source.as_mut().unwrap().session = SessionId::from_bytes([90; 16]),
            3 => spec.target.node = NodeId::from_bytes([91; 16]),
            _ => spec.target.intent_revision += 1,
        }
        assert_eq!(spec.key().unwrap(), original.spec().key().unwrap());
        assert!(original.validate_replay(&spec).is_err());
    }
    let retired = complete.retire(Digest::from_bytes([34; 32]), 30).unwrap();
    assert_eq!(
        retired.established_evidence(),
        complete.established_evidence()
    );
    assert!(!retired.unresolved());
    assert_eq!(
        retired.retire(Digest::from_bytes([34; 32]), 500).unwrap(),
        retired
    );
    assert!(
        retired
            .establish(Digest::from_bytes([31; 32]), 500)
            .is_err()
    );
    assert!(retired.retire(Digest::from_bytes([35; 32]), 500).is_err());
}

#[test]
fn unknown_enrollment_requires_canonical_settlement_and_refusal_cannot_reopen() {
    let unknown = pending(EnrollmentRole::Node {
        mode: NodeMode::Active,
    });
    assert_eq!(
        unknown
            .apply(
                EnrollmentEvent::Established(Digest::from_bytes([43; 32])),
                20
            )
            .unwrap(),
        unknown.establish(Digest::from_bytes([43; 32]), 20).unwrap()
    );
    assert!(unknown.unresolved());
    assert!(
        unknown
            .retire(Digest::from_bytes([0; 32]), 500_000)
            .is_err()
    );
    assert_eq!(unknown.status(), EnrollmentStatus::Pending);
    let closed = unknown
        .retire(Digest::from_bytes([40; 32]), 500_000)
        .unwrap();
    assert_eq!(closed.status(), EnrollmentStatus::Retired);
    assert_eq!(closed.established_evidence(), None);
    assert_eq!(closed.accepted_at_ms(), 10);
    let refused = unknown.refuse(Digest::from_bytes([41; 32]), 15).unwrap();
    assert!(!refused.unresolved());
    assert_eq!(
        refused.refuse(Digest::from_bytes([41; 32]), 30).unwrap(),
        refused
    );
    assert!(refused.retire(Digest::from_bytes([42; 32]), 30).is_err());
    assert!(unknown.establish(Digest::from_bytes([43; 32]), 9).is_err());
    let mut fabricated = unknown.clone();
    fabricated.status = EnrollmentStatus::Established;
    assert!(fabricated.to_bytes().is_err());
    fabricated = closed;
    fabricated.settlement = None;
    assert!(fabricated.to_bytes().is_err());
}

#[test]
fn registry_barrier_is_explicit_stable_and_retained_after_restart() {
    let initial = RegistryVersion::new(head().scope()).unwrap();
    assert!(initial.confirm(initial).is_err());
    assert!(initial.advance(1).is_err());
    let complete = initial.bootstrap(0).unwrap();
    assert_eq!(complete.bootstrap_revision(), Some(1));
    complete.confirm(complete).unwrap();
    assert_eq!(complete.bootstrap(1).unwrap(), complete);
    let changed = complete.advance(1).unwrap();
    assert_eq!(changed.bootstrap_revision(), Some(1));
    assert!(complete.confirm(changed).is_err());
    assert!(changed.confirm(complete).is_err());
    let restored = RegistryVersion::from_bytes(&changed.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, changed);
    assert!(restored.advance(1).is_err());
    let future = RegistryVersion {
        bootstrap_revision: Some(3),
        ..restored
    };
    assert!(future.to_bytes().is_err());
    let overflow = RegistryVersion {
        revision: u64::MAX,
        ..restored
    };
    assert!(overflow.advance(u64::MAX).is_err());
    assert_eq!(overflow.bootstrap(u64::MAX).unwrap(), overflow);
}

#[test]
fn scheduling_stop_resume_is_revision_checked_and_preserves_coverage() {
    let empty = RegistryVersion::new(head().scope()).unwrap();
    assert!(!empty.scheduling_enabled());
    assert!(empty.set_scheduling(0, true).is_err());
    let covered = empty.bootstrap(0).unwrap();
    let running = covered.set_scheduling(covered.revision(), true).unwrap();
    assert!(running.scheduling_enabled());
    assert_eq!(
        running.set_scheduling(running.revision(), true).unwrap(),
        running
    );
    assert!(running.set_scheduling(covered.revision(), false).is_err());
    let stopped = running.set_scheduling(running.revision(), false).unwrap();
    assert!(!stopped.scheduling_enabled());
    assert_eq!(stopped.bootstrap_revision(), running.bootstrap_revision());
    assert_eq!(
        RegistryVersion::from_bytes(&stopped.to_bytes().unwrap()).unwrap(),
        stopped
    );
    let resumed = stopped.set_scheduling(stopped.revision(), true).unwrap();
    assert_eq!(resumed.bootstrap_revision(), covered.bootstrap_revision());
    assert_eq!(resumed.revision(), stopped.revision() + 1);
}

#[test]
fn allocation_checks_retained_cordons_and_stop_preserves_charged_unknown_attempts() {
    let running = bootstrapped().set_scheduling(2, true).unwrap();
    let current = head();
    running
        .authorize_allocation(&current, &spec(1), &intent(1), &intent(2))
        .unwrap();
    let cordon = intent(2)
        .advance_maintenance(&maintenance_for(2, 2))
        .unwrap();
    assert!(
        running
            .authorize_allocation(&current, &spec(1), &intent(1), &cordon)
            .is_err()
    );
    let changed_boot = intent(2)
        .rebind_active(SessionId::from_bytes([99; 16]), 2)
        .unwrap();
    assert!(
        running
            .authorize_allocation(&current, &spec(1), &intent(1), &changed_boot)
            .is_err()
    );
    let draining_source = intent(1)
        .advance_maintenance(&maintenance_for(1, 2))
        .unwrap();
    assert!(
        running
            .authorize_allocation(&current, &spec(1), &draining_source, &intent(2))
            .is_err()
    );
    let maintenance_head = transition(
        &current,
        JournalTransition::BeginMaintenance(maintenance_for(1, 2)),
    );
    assert!(
        running
            .authorize_allocation(&maintenance_head, &spec(1), &draining_source, &intent(2))
            .is_err()
    );
    let maintenance_head = transition(
        &maintenance_head,
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
    );
    let maintenance_head = transition(
        &maintenance_head,
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    );
    running
        .authorize_allocation(&maintenance_head, &spec(1), &draining_source, &intent(2))
        .unwrap();
    let allocated = transition(&maintenance_head, JournalTransition::Allocate(spec(1)));
    let preparing = attempt(&allocated, spec(1).id, AttemptEvent::BeginPrepare);
    let unknown = attempt(&preparing, spec(1).id, AttemptEvent::OutcomeUnknown);
    let stopped = running.set_scheduling(running.revision(), false).unwrap();
    assert!(matches!(
        stopped.authorize_allocation(&unknown, &spec(2), &draining_source, &intent(2)),
        Err(OperationError::Stopped)
    ));
    assert_eq!(unknown.attempts().len(), 1);
    assert_eq!(unknown.reserved_restore_bytes(), spec(1).cost.disk_bytes);
    let cancelling = attempt(&unknown, spec(1).id, AttemptEvent::BeginCancel);
    assert_eq!(
        cancelling.reserved_restore_bytes(),
        unknown.reserved_restore_bytes()
    );
    assert_eq!(
        cancelling.node_intent().unwrap().unwrap().mode(),
        NodeMode::Draining
    );
    let mut foreign_spec = spec(1);
    foreign_spec.id.operation = operation_id(99);
    assert!(
        running
            .authorize_allocation(
                &maintenance_head,
                &foreign_spec,
                &draining_source,
                &intent(2)
            )
            .is_err()
    );
}

#[test]
fn bounded_pages_retain_older_cordons_and_failed_session_obligations() {
    let version = bootstrapped();
    let old_cordon = intent(1)
        .advance_maintenance(&maintenance_for(1, 2))
        .unwrap();
    let page = IntentPage::new(
        version,
        None,
        vec![old_cordon.clone(), intent(2)],
        Some(endpoint(2).node),
    )
    .unwrap();
    assert_eq!(
        IntentPage::from_bytes(&page.to_bytes().unwrap()).unwrap(),
        page
    );
    assert_eq!(page.entries()[0], old_cordon);
    assert_eq!(page.next(), Some(endpoint(2).node));
    let last = IntentPage::new(version, page.next(), vec![intent(3)], None).unwrap();
    assert_eq!(
        IntentPage::from_bytes(&last.to_bytes().unwrap()).unwrap(),
        last
    );
    assert!(IntentPage::new(version, None, vec![intent(2), intent(1)], None).is_err());
    assert!(IntentPage::new(version, None, vec![intent(1), intent(1)], None).is_err());
    assert!(IntentPage::new(version, None, vec![intent(1)], Some(endpoint(2).node)).is_err());
    assert!(IntentPage::new(version, Some(endpoint(2).node), vec![intent(1)], None).is_err());
    assert!(IntentPage::new(version, page.next(), vec![], None).is_err());
    assert!(IntentPage::new(version, None, vec![intent(1); MAX_PAGE_ENTRIES + 1], None).is_err());
    let foreign = RegistryVersion::new(FleetScope {
        fleet: Digest::from_bytes([99; 32]),
        ..version.scope()
    })
    .unwrap();
    assert!(IntentPage::new(foreign, None, vec![intent(1)], None).is_err());
    let unknown = pending(EnrollmentRole::Follower { log_epoch: 7 });
    let retired = pending(EnrollmentRole::Node {
        mode: NodeMode::Active,
    })
    .retire(Digest::from_bytes([60; 32]), 100)
    .unwrap();
    let mut retired = retired;
    retired.spec.request = Digest::from_bytes([61; 32]);
    let mut records = vec![unknown, retired];
    records.sort_by_key(|record| *record.spec().key().unwrap().as_bytes());
    let last_key = records.last().unwrap().spec().key().unwrap();
    let page = EnrollmentPage::new(version, None, records.clone(), Some(last_key)).unwrap();
    assert_eq!(
        EnrollmentPage::from_bytes(&page.to_bytes().unwrap()).unwrap(),
        page
    );
    assert_eq!(
        page.entries()
            .iter()
            .filter(|record| record.unresolved())
            .count(),
        1
    );
    assert!(
        EnrollmentPage::new(
            version,
            None,
            vec![records[0].clone(), records[0].clone()],
            None
        )
        .is_err()
    );
    records.reverse();
    assert!(EnrollmentPage::new(version, None, records, None).is_err());
    assert!(EnrollmentPage::new(version, Some(last_key), vec![], None).is_err());
    assert!(EnrollmentPage::new(version, None, vec![], Some(last_key)).is_err());
    assert!(EnrollmentPage::new(foreign, None, page.entries().to_vec(), None).is_err());
    assert!(IntentPage::new(version, None, vec![], None).is_ok());
    assert!(EnrollmentPage::new(version, None, vec![], None).is_ok());
    let empty_version = RegistryVersion::new(version.scope()).unwrap();
    assert!(IntentPage::new(empty_version, None, vec![intent(1)], None).is_err());
    assert!(EnrollmentPage::new(empty_version, None, page.entries().to_vec(), None).is_err());
}

#[test]
fn enrollment_roles_and_all_progress_states_round_trip_with_exact_inputs() {
    for role in [
        EnrollmentRole::Node {
            mode: NodeMode::Active,
        },
        EnrollmentRole::Reader {
            target: spec(1).target,
            position: release(),
        },
        EnrollmentRole::Follower { log_epoch: 7 },
    ] {
        let record = pending(role);
        assert_eq!(
            EnrollmentSpec::from_bytes(&record.spec().to_bytes().unwrap()).unwrap(),
            *record.spec()
        );
        for state in [
            record.clone(),
            record.establish(Digest::from_bytes([70; 32]), 20).unwrap(),
            record.refuse(Digest::from_bytes([71; 32]), 20).unwrap(),
            record.retire(Digest::from_bytes([72; 32]), 20).unwrap(),
            record
                .establish(Digest::from_bytes([73; 32]), 20)
                .unwrap()
                .retire(Digest::from_bytes([74; 32]), 30)
                .unwrap(),
        ] {
            assert_eq!(
                EnrollmentRecord::from_bytes(&state.to_bytes().unwrap()).unwrap(),
                state
            );
        }
    }
    let mut invalid = enrollment(EnrollmentRole::Node {
        mode: NodeMode::Active,
    });
    invalid.source = Some(endpoint(1));
    assert!(invalid.key().is_err());
    invalid = enrollment(EnrollmentRole::Follower { log_epoch: 0 });
    assert!(invalid.key().is_err());
    invalid = enrollment(EnrollmentRole::Follower { log_epoch: 1 });
    invalid.source = None;
    assert!(invalid.key().is_err());
    invalid = enrollment(EnrollmentRole::Follower { log_epoch: 1 });
    invalid.source = Some(invalid.target);
    assert!(invalid.key().is_err());
    invalid = enrollment(EnrollmentRole::Reader {
        target: spec(1).target,
        position: release(),
    });
    invalid.scope.application = ApplicationId::from_bytes([99; 16]);
    assert!(invalid.key().is_err());
}

fn malformed(bytes: &[u8], decode: fn(&[u8]) -> Result<()>, limit: u32) {
    for end in 0..bytes.len() {
        assert!(decode(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    let mut future = bytes.to_vec();
    future[4 + b"cellule.fleet-operation\0".len()] = FORMAT_VERSION + 1;
    assert!(decode(&future).is_err());
    let mut unknown_kind = bytes.to_vec();
    unknown_kind[5 + b"cellule.fleet-operation\0".len()] = 255;
    assert!(decode(&unknown_kind).is_err());
    assert!(decode(&vec![0; limit as usize + 1]).is_err());
}

#[test]
fn registry_codecs_reject_partial_future_wrong_type_oversize_and_unbounded_counts() {
    let version = bootstrapped();
    malformed(
        &version.to_bytes().unwrap(),
        |b| RegistryVersion::from_bytes(b).map(|_| ()),
        MAX_RECORD_BYTES,
    );
    let record = pending(EnrollmentRole::Reader {
        target: spec(1).target,
        position: release(),
    });
    malformed(
        &record.to_bytes().unwrap(),
        |b| EnrollmentRecord::from_bytes(b).map(|_| ()),
        MAX_RECORD_BYTES,
    );
    let intents = IntentPage::new(version, None, vec![intent(1)], None)
        .unwrap()
        .to_bytes()
        .unwrap();
    malformed(
        &intents,
        |b| IntentPage::from_bytes(b).map(|_| ()),
        MAX_PAGE_BYTES,
    );
    let enrollments = EnrollmentPage::new(version, None, vec![record], None)
        .unwrap()
        .to_bytes()
        .unwrap();
    malformed(
        &enrollments,
        |b| EnrollmentPage::from_bytes(b).map(|_| ()),
        MAX_PAGE_BYTES,
    );
    malformed(
        &pending(EnrollmentRole::Node {
            mode: NodeMode::Active,
        })
        .spec()
        .to_bytes()
        .unwrap(),
        |b| EnrollmentSpec::from_bytes(b).map(|_| ()),
        MAX_RECORD_BYTES,
    );
    // Fixed envelope, scope, version, bootstrap marker/revision, policy, absent after.
    let count_offset =
        4 + b"cellule.fleet-operation\0".len() + 2 + 4 + 32 + 4 + 16 + 8 + 1 + 8 + 1 + 1;
    for mut page in [intents, enrollments] {
        page[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(
            matches!(
                IntentPage::from_bytes(&page),
                Err(OperationError::Codec(CodecError::Limit))
            ) || matches!(
                EnrollmentPage::from_bytes(&page),
                Err(OperationError::Codec(CodecError::Limit))
            )
        );
    }
    assert!(IntentPage::from_bytes(&version.to_bytes().unwrap()).is_err());
    assert!(EnrollmentRecord::from_bytes(&version.to_bytes().unwrap()).is_err());
    assert!(EnrollmentPage::from_bytes(&version.to_bytes().unwrap()).is_err());
    let boot = pending(EnrollmentRole::Node {
        mode: NodeMode::Active,
    });
    let role_offset = 4 + b"cellule.fleet-operation\0".len() + 2 + 4 + 32 + 4 + 16 + 4 + 32;
    let mut unknown_role = boot.spec().to_bytes().unwrap();
    unknown_role[role_offset] = 255;
    assert!(EnrollmentSpec::from_bytes(&unknown_role).is_err());
    let mut unknown_mode = boot.to_bytes().unwrap();
    unknown_mode[role_offset + 1] = 255;
    assert!(EnrollmentRecord::from_bytes(&unknown_mode).is_err());
    assert!(EnrollmentSpec::from_bytes(&boot.to_bytes().unwrap()).is_err());
}
