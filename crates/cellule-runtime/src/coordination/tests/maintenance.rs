use super::*;

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
