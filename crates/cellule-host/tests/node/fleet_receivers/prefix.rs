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
