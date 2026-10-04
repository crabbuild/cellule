use super::*;

#[derive(Clone)]
pub(super) enum CanonicalFault {
    Missing,
    Corrupt,
    Substituted { body: bytes::Bytes, input: Control },
}

pub(super) async fn evidence_failure(
    boundary: RecoveryWriteBoundary,
    fault: Option<CanonicalFault>,
    ordinary_winner: bool,
) {
    let fixture = FaultFixture::paused(RecoveryWrite::Evidence, boundary, true).await;
    let basis = fixture
        .native
        .journal
        .load_receiver_recovery_basis(&fixture.accepted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(basis.control(), &fixture.original);
    let evidence = fixture
        .native
        .journal
        .load_receiver_recovery_evidence(&fixture.accepted)
        .await
        .unwrap();
    assert_eq!(
        evidence.is_some(),
        boundary == RecoveryWriteBoundary::AfterCommit
    );
    assert_reserved_without_actor(&fixture.native).await;
    let fixture = fixture.release(false).await;
    let failed = fixture.completion.as_ref().unwrap();
    assert!(failed.committed && matches!(failed.outcome.outcome, FleetOutcome::Unknown));
    assert_original_error(failed);
    let idle = fixture.current().await;
    assert_eq!(idle.value().state, ControlState::Idle);
    assert!(idle.value().owner.is_none());
    assert_eq!(idle.value().epoch, fixture.original.epoch + 1);
    assert_eq!(idle.value().root, fixture.original.root);
    assert_eq!(fixture.native.nodes[2].stats().active_cells(), 0);
    assert_eq!(fixture.native.nodes[2].stats().worker_jobs(), 0);
    assert_eq!(fixture.native.nodes[2].stats().file_descriptors(), 0);
    assert_eq!(
        fixture.native.nodes[2].stats().local_disk_reserved_bytes(),
        0
    );
    let record = &fixture.native.records[&fixture.native.spec.target.cell_id()];
    let canonical = record
        .authority
        .acquisition_record(
            record.target.cell_id(),
            record.incarnation,
            idle.value().epoch,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(canonical.input(), &fixture.original);
    if let Some(evidence) = &evidence {
        assert_eq!(evidence.restored(), canonical.materialized());
    }
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let inspection = FleetInspectionRequest::new(
        snapshot
            .head()
            .movement_action(
                fixture.native.spec.id,
                MovementAction::Inspect,
                clock().unwrap(),
            )
            .unwrap(),
        snapshot.registry(),
        Digest::from_bytes([229; 32]),
        node_id(2),
        session(2),
        clock().unwrap() + 1_000,
    )
    .unwrap();
    assert!(
        fixture.native.nodes[2]
            .inspect_fleet_action(inspection)
            .await
            .is_err()
    );
    assert_eq!(fixture.current().await.value(), idle.value());
    assert_eq!(
        fixture
            .native
            .journal
            .load_receiver_recovery_evidence(&fixture.accepted)
            .await
            .unwrap(),
        evidence
    );
    assert!(
        fixture
            .native
            .journal
            .load_acquisition_basis(&fixture.accepted)
            .await
            .unwrap()
            .is_none()
    );
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
    // Replay normally resumes the safe rollback root itself. Also race an
    // ordinary acquisition winner against replay without rewriting history.
    if ordinary_winner {
        fixture.native.acquire_rolled_back_replacement().await;
    }
    if let Some(fault) = fault {
        let layout = record.authority.layout();
        let path = layout.acquisition_record_path(
            record.target.cell_id().as_bytes(),
            record.incarnation.as_bytes(),
            canonical.materialized().epoch,
        );
        let (original, _) = layout.store().get_with_etag(&path).await.unwrap();
        let before = fixture.current().await.value().clone();
        layout.store().delete(&path).await.unwrap();
        match &fault {
            CanonicalFault::Missing => {}
            CanonicalFault::Corrupt => {
                layout
                    .store()
                    .create_strict(
                        &path,
                        bytes::Bytes::from_static(b"corrupt-receiver-acquisition"),
                    )
                    .await
                    .unwrap();
            }
            CanonicalFault::Substituted { body, input } => {
                assert_ne!(input, &fixture.original);
                layout
                    .store()
                    .create_strict(&path, body.clone())
                    .await
                    .unwrap();
                assert_eq!(
                    record
                        .authority
                        .acquisition_record(
                            record.target.cell_id(),
                            record.incarnation,
                            canonical.materialized().epoch
                        )
                        .await
                        .unwrap()
                        .unwrap()
                        .input(),
                    input
                );
            }
        }
        let refused = fixture.replay().await;
        assert!(refused.committed && matches!(refused.outcome.outcome, FleetOutcome::Unknown));
        let error = refused.execution_error.as_ref().unwrap().as_ref();
        match &fault {
            CanonicalFault::Missing => assert!(
                matches!(error, cellule_runtime::Error::AcquisitionHistoryIncomplete { epoch, .. } if *epoch == canonical.materialized().epoch)
            ),
            CanonicalFault::Corrupt => assert!(matches!(error, cellule_runtime::Error::Control(_))),
            CanonicalFault::Substituted { .. } => assert!(matches!(
                error,
                cellule_runtime::Error::Control(
                    "receiver recovery input differs from canonical acquisition"
                )
            )),
        }
        assert_eq!(fixture.current().await.value(), &before);
        assert_eq!(
            fixture
                .native
                .journal
                .load_receiver_recovery_evidence(&fixture.accepted)
                .await
                .unwrap(),
            evidence
        );
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
        assert_eq!(
            fixture.native.nodes[2].stats().active_cells(),
            usize::from(ordinary_winner)
        );
        if !matches!(fault, CanonicalFault::Missing) {
            layout.store().delete(&path).await.unwrap();
        }
        layout.store().create_strict(&path, original).await.unwrap();
    }
    let activated = fixture.replay().await;
    assert!(
        activated.committed && activated.execution_error.is_none(),
        "{activated:?}"
    );
    assert!(matches!(
        activated.outcome.outcome,
        FleetOutcome::Activated(_)
    ));
    let retained = fixture
        .native
        .journal
        .load_receiver_recovery_evidence(&fixture.accepted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.basis(), &basis);
    assert_eq!(retained.restored(), canonical.materialized());
    if let Some(evidence) = evidence {
        assert_eq!(retained, evidence);
    }
    fixture.finish().await;
}
