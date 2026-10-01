use super::*;

#[test]
fn final_maintenance_inventory_requires_closed_transfer_admission() {
    let mut state = CoordinationState::serving(true);
    state.step(CoordinationInput::BeginMaintenanceQuiescence);
    let input = CoordinationInput::BeginMaintenanceInventory {
        refreshing: false,
        finalizing: true,
        queue_empty: true,
        publisher_ready: true,
        lease_live: true,
    };
    assert_eq!(state.step(input), CoordinationDecision::Ignored);
    state.step(CoordinationInput::BeginTransferPreflight {
        queue_empty: true,
        publication_idle: true,
        lease_live: true,
    });
    assert_eq!(state.step(input), CoordinationDecision::Started);
}

#[test]
fn busy_maintenance_closes_foreground_and_preserves_exact_completion_schedule() {
    let mut state = CoordinationState::serving(true);
    assert_eq!(
        state.step(CoordinationInput::BeginWork {
            kind: AdmissionKind::Command,
            publisher_ready: true
        }),
        CoordinationDecision::Started
    );
    assert_eq!(
        state.step(CoordinationInput::BeginMaintenanceQuiescence),
        CoordinationDecision::Started
    );
    assert!(state.is_maintenance_quiescing());
    assert!(state.is_busy());
    for kind in [
        AdmissionKind::Command,
        AdmissionKind::Query,
        AdmissionKind::Migration,
    ] {
        assert_eq!(
            state.step(CoordinationInput::Admit {
                kind,
                admission_matches: true
            }),
            CoordinationDecision::Reject(RejectReason::Draining)
        );
    }
    for kind in [
        AdmissionKind::LeaseCommand,
        AdmissionKind::LeaseQuery,
        AdmissionKind::Resolve,
    ] {
        assert_eq!(
            state.step(CoordinationInput::Admit {
                kind,
                admission_matches: true
            }),
            CoordinationDecision::Admit
        );
    }
    state.step(CoordinationInput::AbortTransfer);
    assert!(state.is_maintenance_quiescing());
    state.step(CoordinationInput::FinishWork { fenced: false });
    assert_eq!(
        state.step(CoordinationInput::BeginWork {
            kind: AdmissionKind::LeaseCommand,
            publisher_ready: true
        }),
        CoordinationDecision::Started
    );
    state.step(CoordinationInput::FinishWork { fenced: false });
    state.step(CoordinationInput::Fence);
    assert_eq!(
        state.step(CoordinationInput::Admit {
            kind: AdmissionKind::LeaseCommand,
            admission_matches: true
        }),
        CoordinationDecision::Reject(RejectReason::Fenced)
    );
}

#[test]
fn terminal_transfer_and_shutdown_close_even_native_completion_admission() {
    for terminal in [
        CoordinationInput::BeginDrain,
        CoordinationInput::BeginShutdown,
    ] {
        let mut state = CoordinationState::serving(true);
        state.step(CoordinationInput::BeginMaintenanceQuiescence);
        state.step(terminal);
        assert_eq!(
            state.step(CoordinationInput::Admit {
                kind: AdmissionKind::LeaseCommand,
                admission_matches: true
            }),
            CoordinationDecision::Reject(RejectReason::Draining)
        );
    }
    let mut state = CoordinationState::serving(true);
    state.step(CoordinationInput::BeginMaintenanceQuiescence);
    state.step(CoordinationInput::BeginTransferPreflight {
        queue_empty: true,
        publication_idle: true,
        lease_live: true,
    });
    assert_eq!(
        state.step(CoordinationInput::Admit {
            kind: AdmissionKind::LeaseCommand,
            admission_matches: true
        }),
        CoordinationDecision::Reject(RejectReason::Draining)
    );
}

#[test]
fn maintenance_inventory_has_a_serialized_slot_even_during_accepted_work() {
    let mut state = CoordinationState::serving(true);
    state.step(CoordinationInput::BeginWork {
        kind: AdmissionKind::Command,
        publisher_ready: true,
    });
    let input = CoordinationInput::BeginMaintenanceInventory {
        refreshing: false,
        finalizing: false,
        queue_empty: false,
        publisher_ready: true,
        lease_live: true,
    };
    assert_eq!(
        state.step(input),
        CoordinationDecision::Reject(RejectReason::Draining)
    );
    state.step(CoordinationInput::BeginMaintenanceQuiescence);
    assert_eq!(state.step(input), CoordinationDecision::Started);
    let effect = state.begin_effect(CoordinationEffect::Inventory);
    assert!(!state.can_deactivate());
    state.step(CoordinationInput::CompleteEffect {
        effect_id: effect,
        effect: CoordinationEffect::Inventory,
    });
    state.step(CoordinationInput::BeginTransferPreflight {
        queue_empty: false,
        publication_idle: true,
        lease_live: true,
    });
    assert_eq!(
        state.step(CoordinationInput::BeginMaintenanceInventory {
            refreshing: false,
            finalizing: true,
            queue_empty: false,
            publisher_ready: true,
            lease_live: true
        }),
        CoordinationDecision::Ignored
    );
    state.step(CoordinationInput::FinishWork { fenced: false });
    assert_eq!(
        state.step(CoordinationInput::BeginMaintenanceInventory {
            finalizing: true,
            queue_empty: true,
            refreshing: false,
            publisher_ready: true,
            lease_live: true
        }),
        CoordinationDecision::Started
    );
}

#[test]
fn quiesced_source_keeps_canonical_renewal_before_final_release() {
    let mut state = CoordinationState::serving(true);
    state.step(CoordinationInput::BeginMaintenanceQuiescence);
    state.step(CoordinationInput::BeginTransferPreflight {
        queue_empty: true,
        publication_idle: true,
        lease_live: true,
    });
    assert_eq!(
        state.step(CoordinationInput::BeginRenewal {
            queue_empty: true,
            publication_idle: true,
            lease_live: true
        }),
        CoordinationDecision::Started
    );
    assert_eq!(
        state.step(CoordinationInput::ConfirmTransfer),
        CoordinationDecision::Ignored
    );
    state.step(CoordinationInput::FinishRenewal { fenced: false });
    assert_eq!(
        state.step(CoordinationInput::ConfirmTransfer),
        CoordinationDecision::ReadyToDeactivate
    );
    assert!(matches!(
        state.step(CoordinationInput::BeginRenewal {
            queue_empty: true,
            publication_idle: true,
            lease_live: true
        }),
        CoordinationDecision::Reject(RejectReason::Draining)
    ));
}
