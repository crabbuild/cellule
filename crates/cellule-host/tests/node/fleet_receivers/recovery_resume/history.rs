use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_missing_or_corrupt_history_blocks_idle_and_ordinary_winner() {
    for (corrupt, ordinary) in [(false, false), (true, false), (false, true), (true, true)] {
        let (movement, action, idle, receipt) = failed(1, 42, false).await;
        let observed = movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap();
        if ordinary {
            let handle = movement
                .receiver
                .runtime()
                .acquire_idle_restored(
                    movement.inputs.catalog.clone(),
                    movement.inputs.replica.clone(),
                    movement.inputs.authority.clone(),
                    observed,
                    movement.inputs.destination.clone(),
                    movement.inputs.owner.clone(),
                )
                .await
                .unwrap();
            assert_eq!(counter(&handle).await, 42);
        }
        let selected = movement
            .inputs
            .authority
            .load(idle.cell)
            .await
            .unwrap()
            .unwrap();
        let layout = movement.inputs.authority.layout();
        let path = layout.acquisition_record_path(
            idle.cell.as_bytes(),
            idle.incarnation.as_bytes(),
            idle.epoch,
        );
        let (body, _) = layout.store().get_with_etag(&path).await.unwrap();
        layout.store().delete(&path).await.unwrap();
        if corrupt {
            layout
                .store()
                .create_strict(&path, Bytes::from_static(b"corrupt-source-acquisition"))
                .await
                .unwrap();
        }
        let refused = apply(&movement.receiver, action.clone()).await;
        assert!(refused.committed && matches!(refused.outcome.outcome, FleetOutcome::Unknown));
        let error = refused.execution_error.as_ref().unwrap().as_ref();
        if corrupt {
            assert!(matches!(error, Error::Control(_)), "{error:?}");
        } else {
            assert!(
                matches!(error, Error::AcquisitionHistoryIncomplete { epoch, .. } if *epoch == idle.epoch),
                "{error:?}"
            );
        }
        assert_eq!(
            movement
                .inputs
                .authority
                .load(idle.cell)
                .await
                .unwrap()
                .unwrap()
                .value(),
            selected.value()
        );
        assert_eq!(
            movement.receiver.stats().active_cells(),
            usize::from(ordinary)
        );
        assert!(
            movement
                .source
                .journal
                .recovery_evidence(action.key().unwrap())
                .is_none()
        );
        if corrupt {
            layout.store().delete(&path).await.unwrap();
        }
        layout.store().create_strict(&path, body).await.unwrap();
        finish(movement, action, &idle, 42, ordinary, receipt).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_source_valid_substituted_history_cannot_reconstruct_evidence() {
    let (foreign, _, foreign_idle, _) = failed(1, 99, false).await;
    let layout = foreign.inputs.authority.layout();
    let path = layout.acquisition_record_path(
        foreign_idle.cell.as_bytes(),
        foreign_idle.incarnation.as_bytes(),
        foreign_idle.epoch,
    );
    let (foreign_body, _) = layout.store().get_with_etag(&path).await.unwrap();
    let foreign_input = foreign
        .inputs
        .authority
        .acquisition_record(
            foreign_idle.cell,
            foreign_idle.incarnation,
            foreign_idle.epoch,
        )
        .await
        .unwrap()
        .unwrap()
        .input()
        .clone();
    foreign.shutdown().await;
    let (movement, action, idle, receipt) = failed(1, 42, false).await;
    let basis = movement
        .source
        .journal
        .recovery_basis(action.key().unwrap())
        .unwrap();
    assert_ne!(basis.control(), &foreign_input);
    let layout = movement.inputs.authority.layout();
    let path = layout.acquisition_record_path(
        idle.cell.as_bytes(),
        idle.incarnation.as_bytes(),
        idle.epoch,
    );
    let (body, _) = layout.store().get_with_etag(&path).await.unwrap();
    layout.store().delete(&path).await.unwrap();
    layout
        .store()
        .create_strict(&path, foreign_body)
        .await
        .unwrap();
    // It decodes as a valid native record in the same Cell/epoch scope.
    assert_eq!(
        movement
            .inputs
            .authority
            .acquisition_record(idle.cell, idle.incarnation, idle.epoch)
            .await
            .unwrap()
            .unwrap()
            .input(),
        &foreign_input
    );
    let refused = apply(&movement.receiver, action.clone()).await;
    assert!(refused.committed && matches!(refused.outcome.outcome, FleetOutcome::Unknown));
    assert!(matches!(
        refused.execution_error.as_ref().unwrap().as_ref(),
        Error::Control("recovery input differs from canonical acquisition")
    ));
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
    assert!(
        movement
            .source
            .journal
            .recovery_evidence(action.key().unwrap())
            .is_none()
    );
    layout.store().delete(&path).await.unwrap();
    layout.store().create_strict(&path, body).await.unwrap();
    finish(movement, action, &idle, 42, false, receipt).await;
}
