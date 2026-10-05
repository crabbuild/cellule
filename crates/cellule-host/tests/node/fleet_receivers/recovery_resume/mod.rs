//! Accepted failed-source recovery must finish a proven safe rollback itself.
use super::*;
use bytes::Bytes;
use cellule_runtime::cell::executor::StoredOutcome;
use cellule_runtime::control::Control;

struct Acknowledged {
    identity: MutationIdentity,
    outcome: StoredOutcome,
}

mod continuation;
mod history;
mod origin;
mod suffix;

async fn failed(
    writes: usize,
    value: i64,
    overlay: bool,
) -> (Movement, FleetAction, Control, Acknowledged) {
    let movement = Movement::new(128 << 20).await;
    let now = clock();
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([247; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    let outcome = movement
        .source
        .handle
        .execute(
            identity,
            Digest::from_bytes([247; 32]),
            now,
            64,
            64,
            move |tx| {
                tx.execute("UPDATE counter SET value = ?1", [value])?;
                Ok(HandlerOutcome::Success(value.to_be_bytes().to_vec()))
            },
        )
        .await
        .unwrap();
    if overlay {
        super::suffix::start_suffix_recovery(&movement).await;
    } else {
        movement.start_recovery(false).await;
    }
    if writes == 0 {
        movement
            .source
            .journal
            .lose_recovery_evidence_reply
            .store(true, Ordering::SeqCst);
    }
    movement
        .source
        .journal
        .fail_recovery_evidence_writes
        .store(writes, Ordering::SeqCst);
    let action = movement.action(MovementAction::Recover);
    let failure = apply(&movement.receiver, action.clone()).await;
    assert!(failure.committed && matches!(failure.outcome.outcome, FleetOutcome::Unknown));
    let Error::Facility { source, .. } = failure.execution_error.as_ref().unwrap().as_ref() else {
        panic!("original journal error lost: {failure:?}");
    };
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().to_string(),
        if writes == 0 {
            "injected lost recovery evidence reply"
        } else {
            "injected recovery evidence write failure"
        }
    );
    assert_eq!(
        movement
            .source
            .journal
            .recovery_evidence(action.key().unwrap())
            .is_some(),
        writes == 0
    );
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value().state, ControlState::Idle);
    assert!(current.value().owner.is_none());
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    assert_eq!(movement.receiver.stats().worker_jobs(), 0);
    assert_eq!(movement.receiver.stats().file_descriptors(), 0);
    assert_eq!(movement.receiver.stats().local_disk_reserved_bytes(), 0);
    (
        movement,
        action,
        current.value().clone(),
        Acknowledged { identity, outcome },
    )
}

async fn finish(
    movement: Movement,
    action: FleetAction,
    idle: &Control,
    value: i64,
    ordinary: bool,
    receipt: Acknowledged,
) {
    let basis = movement
        .source
        .journal
        .recovery_basis(action.key().unwrap())
        .unwrap();
    let canonical = movement
        .inputs
        .authority
        .acquisition_record(idle.cell, idle.incarnation, idle.epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(canonical.input(), basis.control());
    let before = movement
        .inputs
        .authority
        .load(idle.cell)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before.value().state,
        if ordinary {
            ControlState::Serving
        } else {
            ControlState::Idle
        }
    );
    let completion = apply(&movement.receiver, action.clone()).await;
    assert!(
        completion.committed && completion.execution_error.is_none(),
        "{completion:?}"
    );
    let FleetOutcome::Recovered(result) = &completion.outcome.outcome else {
        panic!("{completion:?}");
    };
    assert_eq!(result.recovery.basis(), &basis);
    assert_eq!(result.recovery.restored(), canonical.materialized());
    assert_eq!(result.serving.position.epoch, idle.epoch + 1);
    let current = movement
        .inputs
        .authority
        .load(idle.cell)
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &current)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, value);
    assert_eq!(
        handle
            .resolve(receipt.identity, Digest::from_bytes([247; 32]), clock(), 64)
            .await
            .unwrap(),
        Resolution::Committed(receipt.outcome)
    );
    if ordinary {
        assert_eq!(before.value(), current.value());
    }
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    assert_eq!(
        movement.source.journal.basis_writes.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        movement.source.journal.current_attempt().phase(),
        AttemptPhase::Recovering
    );
    assert_eq!(
        movement.source.journal.current_attempt().spec().cost,
        movement.spec.cost
    );
    let repeated = apply(&movement.receiver, action).await;
    assert!(repeated.committed && repeated.execution_error.is_none());
    let FleetOutcome::Recovered(repeated) = &repeated.outcome.outcome else {
        panic!("{repeated:?}");
    };
    assert_eq!(repeated.recovery, result.recovery);
    assert_eq!(repeated.serving.position.epoch, idle.epoch + 1);
    assert_eq!(
        movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        current.value()
    );
    assert_eq!(current.value().epoch, canonical.materialized().epoch + 1);
    movement.shutdown().await;
}
