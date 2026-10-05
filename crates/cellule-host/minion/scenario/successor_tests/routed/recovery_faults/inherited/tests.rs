use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_inherited_suffix_survives_missing_evidence_and_idle_reacquisition() {
    let inherited = Inherited::new().await;
    let original = inherited.original.clone();
    let value = inherited.value;
    let fixture = FaultFixture::pause_claim(
        inherited.native,
        RecoveryWrite::Evidence,
        RecoveryWriteBoundary::BeforeCommit,
        true,
    )
    .await
    .release(false)
    .await;
    assert_original_error(fixture.completion.as_ref().unwrap());
    assert_eq!(fixture.original.epoch, original.epoch + 1);
    assert_eq!(fixture.original.recovery, original.recovery);
    let idle = fixture.current().await;
    assert_eq!(idle.value().state, ControlState::Idle);
    assert!(idle.value().recovery.is_none());
    assert_eq!(idle.value().epoch, original.epoch + 2);
    assert_ne!(idle.value().root, original.root);
    let completion = fixture.replay().await;
    assert!(
        completion.committed && completion.execution_error.is_none(),
        "{completion:?}"
    );
    assert!(matches!(
        completion.outcome.outcome,
        FleetOutcome::Activated(_)
    ));
    assert_eq!(
        fixture.current().await.value().epoch,
        idle.value().epoch + 1
    );
    let record = &fixture.native.records[&idle.value().cell];
    let overlay = original.recovery.as_ref().unwrap();
    let manifest =
        RecoveryManifestStore::new(record.authority.layout().clone(), record.replica.limits())
            .load_manifest(
                overlay.leader_session,
                overlay.log_epoch,
                overlay.manifest_digest,
            )
            .await
            .unwrap();
    let required = &manifest.cells()[0];
    assert_eq!(required.cell_epoch, original.epoch);
    let current = fixture.current().await;
    let proof = fixture.native.nodes[2]
        .runtime()
        .verify_recovered_prefix(
            &record.catalog,
            &record.authority,
            record.replica.clone(),
            required,
            current.value().ltx_root().unwrap(),
            128,
        )
        .await
        .unwrap();
    assert_eq!(proof.acquisition_epoch(), idle.value().epoch);
    assert_eq!(proof.required().cell_epoch, original.epoch);
    assert!(
        record
            .authority
            .acquisition_record(original.cell, original.incarnation, original.epoch + 1)
            .await
            .unwrap()
            .is_none()
    );
    fixture.finish_value(Some(value)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_inherited_manifest_faults_keep_idle_charged_then_a_new_controller_joins() {
    for corrupt in [false, true] {
        let inherited = Inherited::new().await;
        let fixture = FaultFixture::pause_claim(
            inherited.native,
            RecoveryWrite::Evidence,
            RecoveryWriteBoundary::BeforeCommit,
            true,
        )
        .await
        .release(false)
        .await;
        let idle = fixture.current().await.value().clone();
        let record = &fixture.native.records[&idle.cell];
        let layout = record.authority.layout();
        layout.store().delete(&inherited.path).await.unwrap();
        if corrupt {
            layout
                .store()
                .create_strict(
                    &inherited.path,
                    Bytes::from_static(b"corrupt inherited routed manifest"),
                )
                .await
                .unwrap();
        }
        let refused = fixture.replay().await;
        assert!(refused.committed && matches!(refused.outcome.outcome, FleetOutcome::Unknown));
        let error = refused.execution_error.as_ref().unwrap().as_ref();
        if corrupt {
            assert!(matches!(error, Error::Node(_)), "{error:?}");
        } else {
            assert!(matches!(error, Error::Storage(_)), "{error:?}");
        }
        assert_eq!(fixture.current().await.value(), &idle);
        assert_eq!(fixture.native.nodes[2].stats().active_cells(), 0);
        assert_eq!(fixture.native.nodes[2].stats().file_descriptors(), 0);
        assert_eq!(
            fixture
                .native
                .journal
                .load_snapshot(scope())
                .await
                .unwrap()
                .head()
                .reserved_restore_bytes(),
            fixture.native.spec.cost.disk_bytes
        );
        let evidence = fixture
            .native
            .journal
            .load_receiver_recovery_evidence(&fixture.accepted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            evidence.basis().control().recovery,
            inherited.original.recovery
        );
        assert_eq!(
            evidence.basis().control().epoch,
            inherited.original.epoch + 1
        );
        if corrupt {
            layout.store().delete(&inherited.path).await.unwrap();
        }
        layout
            .store()
            .create_strict(&inherited.path, inherited.manifest)
            .await
            .unwrap();
        let completion = fixture.replay().await;
        assert!(
            completion.committed && completion.execution_error.is_none(),
            "{completion:?}"
        );
        assert_eq!(
            fixture
                .native
                .journal
                .load_receiver_recovery_evidence(&fixture.accepted)
                .await
                .unwrap(),
            Some(evidence)
        );
        fixture.finish_after_controller_loss(inherited.value).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_inherited_claim_resumes_without_cas_after_manifest_outage_and_cancelled_waiter() {
    let inherited = Inherited::new().await;
    let fixture = FaultFixture::pause_claim(
        inherited.native,
        RecoveryWrite::Basis,
        RecoveryWriteBoundary::AfterCommit,
        true,
    )
    .await
    .release(false)
    .await;
    let before = fixture.current().await;
    let record = &fixture.native.records[&before.value().cell];
    let layout = record.authority.layout();
    layout.store().delete(&inherited.path).await.unwrap();
    let failed = fixture.replay().await;
    assert!(failed.committed && matches!(failed.outcome.outcome, FleetOutcome::Unknown));
    assert!(matches!(
        failed.execution_error.as_ref().unwrap().as_ref(),
        Error::Storage(_)
    ));
    let claimed = fixture.current().await.value().clone();
    assert_eq!(claimed.state, ControlState::Recovering);
    assert_eq!(claimed.epoch, before.value().epoch + 1);
    assert_eq!(claimed.recovery, before.value().recovery);
    assert_eq!(fixture.native.nodes[2].stats().active_cells(), 0);
    assert!(
        record
            .authority
            .acquisition_record(claimed.cell, claimed.incarnation, claimed.epoch)
            .await
            .unwrap()
            .is_none()
    );
    layout
        .store()
        .create_strict(&inherited.path, inherited.manifest)
        .await
        .unwrap();
    let (entered, resume) = fixture.native.journal.pause_receiver_recovery_write(
        RecoveryWrite::Evidence,
        RecoveryWriteBoundary::BeforeCommit,
        false,
    );
    let node = fixture.native.nodes[2].clone();
    let action = fixture.accepted.action().clone();
    let waiter = tokio::spawn(async move {
        node.apply_fleet_action(action, clock().unwrap())
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    let canonical = record
        .authority
        .acquisition_record(claimed.cell, claimed.incarnation, claimed.epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(canonical.input(), &fixture.original);
    assert_eq!(canonical.materialized().epoch, claimed.epoch);
    assert!(canonical.materialized().recovery.is_none());
    let inventory = fixture.native.nodes[2]
        .runtime()
        .fleet_cells_page(None, 128)
        .await
        .unwrap();
    assert!(
        inventory
            .entries()
            .iter()
            .all(|row| !matches!(row, CellInventoryEntry::Owned(_)))
    );
    drop(inventory);
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    let completed = fixture.replay().await;
    assert!(
        completed.committed && completed.execution_error.is_none(),
        "{completed:?}"
    );
    assert_eq!(fixture.current().await.value().epoch, claimed.epoch);
    assert_eq!(
        fixture
            .native
            .journal
            .load_receiver_recovery_evidence(&fixture.accepted)
            .await
            .unwrap()
            .unwrap()
            .restored(),
        canonical.materialized()
    );
    fixture.finish_after_controller_loss(inherited.value).await;
}
