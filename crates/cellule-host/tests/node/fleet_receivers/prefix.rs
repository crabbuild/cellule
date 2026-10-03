//! Fresh native inspection still requires preparation lineage and origin bytes.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_serving_refuses_missing_lineage_after_successor_publication() {
    inspect_missing_evidence(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_serving_refuses_missing_origin_root_despite_a_native_actor() {
    inspect_missing_evidence(true).await;
}

async fn inspect_missing_evidence(origin: bool) {
    let movement = Movement::new(128 << 20).await;
    movement.release().await;
    let activated = apply(
        &movement.receiver,
        movement.action(MovementAction::Activate),
    )
    .await;
    assert!(activated.committed && activated.execution_error.is_none());
    let FleetOutcome::Activated(evidence) = &activated.outcome.outcome else {
        panic!("not activated")
    };
    movement.event(AttemptEvent::Activated(evidence.clone()));
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
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
    let now = clock();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([213; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([213; 32]),
            now,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = 99", [])?;
                Ok(HandlerOutcome::Success(vec![99]))
            },
        )
        .await
        .unwrap();
    let advanced = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    movement.inspect(247).await;
    let layout = movement.inputs.authority.layout();
    let path = if origin {
        layout.incarnation_object_path(
            &advanced.cell,
            &advanced.incarnation,
            &advanced.digest,
            cellule_runtime::ltx::CellObjectKind::Root,
        )
    } else {
        layout.root_lineage_path(&advanced.cell, &advanced.incarnation, &advanced.digest)
    };
    let (original, _) = layout.store().get_with_etag(&path).await.unwrap();
    layout.store().delete(&path).await.unwrap();
    assert_eq!(counter(&handle).await, 99);
    let accepted = movement.source.journal.accepted_count();
    let error = movement
        .receiver
        .inspect_fleet_action(movement.inspection(248))
        .await
        .unwrap_err();
    if origin {
        assert!(matches!(error.as_ref(), Error::Ltx(_)));
    } else {
        assert!(
            matches!(error.as_ref(), Error::RootLineageIncomplete { root } if *root == advanced)
        );
    }
    assert_eq!(movement.source.journal.accepted_count(), accepted);
    assert_eq!(
        movement
            .inputs
            .authority
            .load(movement.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root(),
        Some(advanced)
    );
    // Restore the exact original bytes, never fabricate historical inputs or
    // rerun acquisition. A new nonce must inspect current evidence again.
    layout.store().create_strict(&path, original).await.unwrap();
    let restored = movement.inspect(249).await;
    assert!(matches!(
        restored.outcome().outcome,
        FleetOutcome::Activated(_)
    ));
    movement.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_recovery_requires_canonical_acquisition_even_with_a_live_actor() {
    inspect_acquisition(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_acquisition_cannot_certify_an_idle_source_recovery() {
    inspect_acquisition(true).await;
}

async fn inspect_acquisition(corrupt: bool) {
    let movement = Movement::new(128 << 20).await;
    movement.start_recovery(corrupt).await;
    let action = movement.action(MovementAction::Recover);
    let result = apply(&movement.receiver, action.clone()).await;
    assert!(result.committed && result.execution_error.is_none());
    let FleetOutcome::Recovered(recovered) = &result.outcome.outcome else {
        panic!("not recovered")
    };
    movement.event(AttemptEvent::Recovered(recovered.clone()));
    let cleaned = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(matches!(
        cleaned.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    movement.event(AttemptEvent::ReceiverCleaned);
    movement.inspect(241).await;
    let restored = recovered.recovery.restored();
    let layout = movement.inputs.authority.layout();
    let path = layout.acquisition_record_path(
        restored.cell.as_bytes(),
        restored.incarnation.as_bytes(),
        restored.epoch,
    );
    let (original, _) = layout.store().get_with_etag(&path).await.unwrap();
    layout.store().delete(&path).await.unwrap();
    if corrupt {
        layout
            .store()
            .create_strict(&path, bytes::Bytes::from_static(b"corrupt-acquisition"))
            .await
            .unwrap();
    }
    // Historical replay preserves its original committed outcome; it cannot
    // replace the independent fresh native inspection or restore missing proof.
    assert_eq!(
        apply(&movement.receiver, action).await.outcome,
        result.outcome
    );
    let error = movement
        .receiver
        .inspect_fleet_action(movement.inspection(242))
        .await
        .unwrap_err();
    if corrupt {
        assert!(matches!(error.as_ref(), Error::Control(_)));
        layout.store().delete(&path).await.unwrap();
    } else {
        assert!(
            matches!(error.as_ref(), Error::AcquisitionHistoryIncomplete { epoch, .. }
            if *epoch==restored.epoch)
        );
    }
    layout.store().create_strict(&path, original).await.unwrap();
    assert!(matches!(
        movement.inspect(243).await.outcome().outcome,
        FleetOutcome::Recovered(_)
    ));
    movement.shutdown().await;
}
