use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_idle_continuation_requires_its_original_sealed_suffix() {
    for corrupt in [false, true] {
        let (movement, action, idle, receipt) = failed(1, 42, true).await;
        let basis = movement
            .source
            .journal
            .recovery_basis(action.key().unwrap())
            .unwrap();
        let overlay = basis.control().recovery.as_ref().unwrap();
        assert!(idle.recovery.is_none());
        assert_ne!(idle.root, basis.control().root);
        let layout = movement.inputs.authority.layout();
        let path = layout.node_log_recovery_path(
            overlay.leader_session.as_bytes(),
            overlay.log_epoch,
            overlay.manifest_digest.as_bytes(),
        );
        let (body, _) = layout.store().get_with_etag(&path).await.unwrap();
        layout.store().delete(&path).await.unwrap();
        if corrupt {
            layout
                .store()
                .create_strict(&path, Bytes::from_static(b"corrupt-original-suffix"))
                .await
                .unwrap();
        }
        let refused = apply(&movement.receiver, action.clone()).await;
        assert!(refused.committed && matches!(refused.outcome.outcome, FleetOutcome::Unknown));
        assert!(refused.execution_error.is_some());
        assert_eq!(
            movement
                .inputs
                .authority
                .load(idle.cell)
                .await
                .unwrap()
                .unwrap()
                .value(),
            &idle
        );
        assert_eq!(movement.receiver.stats().active_cells(), 0);
        assert_eq!(movement.receiver.stats().worker_jobs(), 0);
        assert_eq!(movement.receiver.stats().file_descriptors(), 0);
        // Metadata records the actual native materialization, but cannot admit
        // a writer without the original suffix and complete prefix proof.
        let evidence = movement
            .source
            .journal
            .recovery_evidence(action.key().unwrap())
            .unwrap();
        assert_eq!(evidence.basis(), &basis);
        assert_eq!(evidence.restored().root, idle.root);
        assert!(
            movement
                .receiver
                .inspect_fleet_action(movement.inspection(251))
                .await
                .is_err()
        );
        assert_eq!(
            movement.source.journal.current_attempt().spec().cost,
            movement.spec.cost
        );
        if corrupt {
            layout.store().delete(&path).await.unwrap();
        }
        layout.store().create_strict(&path, body).await.unwrap();
        let completed = apply(&movement.receiver, action.clone()).await;
        assert!(
            completed.committed && completed.execution_error.is_none(),
            "{completed:?}"
        );
        let FleetOutcome::Recovered(result) = &completed.outcome.outcome else {
            panic!("{completed:?}");
        };
        assert_eq!(result.recovery, evidence);
        finish(movement, action, &idle, 43, true, receipt).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_suffix_evidence_repair_preserves_an_ordinary_writer_and_receipt() {
    let (movement, action, idle, receipt) = failed(1, 42, true).await;
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
        .acquire_idle_restored(
            movement.inputs.catalog.clone(),
            movement.inputs.replica.clone(),
            movement.inputs.authority.clone(),
            current,
            movement.inputs.destination.clone(),
            movement.inputs.owner.clone(),
        )
        .await
        .unwrap();
    assert_eq!(counter(&handle).await, 43);
    finish(movement, action, &idle, 43, true, receipt).await;
}
