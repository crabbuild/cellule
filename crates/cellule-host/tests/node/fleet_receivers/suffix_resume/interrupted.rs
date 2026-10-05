use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_overlay_materialization_resumes_exact_claim_after_waiter_cancellation() {
    let movement = Movement::new(128 << 20).await;
    let now = clock();
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([246; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    let digest = Digest::from_bytes([246; 32]);
    let receipt = movement
        .source
        .handle
        .execute(identity, digest, now, 64, 64, |tx| {
            tx.execute("UPDATE counter SET value=value", [])?;
            Ok(HandlerOutcome::Success(vec![42]))
        })
        .await
        .unwrap();
    suffix::start_suffix_recovery(&movement).await;
    let original = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    let recovery = original.recovery.as_ref().unwrap();
    let layout = movement.inputs.authority.layout();
    let path = layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let (body, _) = layout.store().get_with_etag(&path).await.unwrap();
    layout.store().delete(&path).await.unwrap();
    for _ in 0..2 {
        let failure = apply(&movement.receiver, movement.action(MovementAction::Recover)).await;
        assert!(failure.committed && matches!(failure.outcome.outcome, FleetOutcome::Unknown));
        assert!(matches!(
            failure.execution_error.as_ref().unwrap().as_ref(),
            Error::Storage(cellule_store::StorageError::NotFound { .. })
        ));
        let claimed = movement
            .inputs
            .authority
            .load(original.cell)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.value().epoch, original.epoch + 1);
        assert_eq!(claimed.value().state, ControlState::Recovering);
        assert_eq!(
            claimed.value().owner.as_ref().unwrap().session,
            movement.spec.destination
        );
        assert_eq!(claimed.value().recovery, original.recovery);
        assert_eq!(claimed.value().root, original.root);
        assert_eq!(movement.receiver.stats().active_cells(), 0);
        assert_eq!(movement.receiver.stats().worker_jobs(), 0);
        assert_eq!(movement.receiver.stats().file_descriptors(), 0);
        assert!(
            movement
                .inputs
                .authority
                .acquisition_record(original.cell, original.incarnation, claimed.value().epoch)
                .await
                .unwrap()
                .is_none()
        );
    }
    let FleetActionAcceptance::Existing { accepted, .. } = movement
        .source
        .journal
        .load_movement_action(
            scope(),
            movement.spec.id,
            MovementAction::Recover,
            movement.spec.destination_node,
            movement.spec.destination,
        )
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("accepted recovery missing")
    };
    let basis = movement
        .source
        .journal
        .load_recovery_basis(&accepted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(basis.control(), &original);
    assert!(
        movement
            .source
            .journal
            .load_recovery_evidence(&accepted)
            .await
            .unwrap()
            .is_none()
    );
    layout.store().create_strict(&path, body).await.unwrap();
    movement
        .source
        .journal
        .block_basis
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let node = movement.receiver.clone();
    let action = movement.action(MovementAction::Recover);
    let waiter = tokio::spawn(async move { apply(&node, action).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        movement.source.journal.basis_entered.notified(),
    )
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    movement.source.journal.basis_resume.add_permits(1);
    let recovered = apply(&movement.receiver, movement.action(MovementAction::Recover)).await;
    assert!(
        recovered.committed && recovered.execution_error.is_none(),
        "{recovered:?}"
    );
    let FleetOutcome::Recovered(evidence) = &recovered.outcome.outcome else {
        panic!("not recovered: {recovered:?}")
    };
    assert_eq!(evidence.recovery.basis(), &basis);
    assert_eq!(evidence.recovery.restored().epoch, original.epoch + 1);
    assert!(evidence.recovery.restored().recovery.is_none());
    assert_eq!(evidence.serving.position.epoch, original.epoch + 1);
    movement.event(AttemptEvent::Recovered(evidence.clone()));
    let current = movement
        .inputs
        .authority
        .load(original.cell)
        .await
        .unwrap()
        .unwrap();
    let canonical = movement
        .inputs
        .authority
        .acquisition_record(original.cell, original.incarnation, current.value().epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(canonical.input(), &original);
    assert_eq!(canonical.materialized(), evidence.recovery.restored());
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &current)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        handle.resolve(identity, digest, clock(), 64).await.unwrap(),
        Resolution::Committed(receipt)
    );
    assert_eq!(counter(&handle).await, 43);
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    let inputs = movement.recovery_inputs.lock().unwrap().clone().unwrap();
    let manifest = inputs
        .manifests
        .load_manifest(
            recovery.leader_session,
            recovery.log_epoch,
            recovery.manifest_digest,
        )
        .await
        .unwrap();
    assert_eq!(manifest.cells()[0].cell_epoch, original.epoch);
    assert_eq!(manifest.cells()[0].recovery, *recovery);
    movement.inspect(247).await;
    layout.store().delete(&path).await.unwrap();
    layout
        .store()
        .create_strict(&path, Bytes::from_static(b"substituted historical suffix"))
        .await
        .unwrap();
    assert!(
        movement
            .receiver
            .inspect_fleet_action(movement.inspection(248))
            .await
            .is_err()
    );
    assert_eq!(counter(&handle).await, 43);
    movement.shutdown().await;
}
