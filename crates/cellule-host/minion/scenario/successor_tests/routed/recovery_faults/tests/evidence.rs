use super::*;
use continuation::evidence_failure;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_receiver_evidence_reply_keeps_idle_root_and_retained_proof_until_native_serving() {
    evidence_failure(RecoveryWriteBoundary::AfterCommit, None, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_receiver_evidence_write_reconstructs_only_from_canonical_acquisition() {
    evidence_failure(RecoveryWriteBoundary::BeforeCommit, None, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_acquisition_winner_retains_the_original_receiver_recovery() {
    for boundary in [
        RecoveryWriteBoundary::BeforeCommit,
        RecoveryWriteBoundary::AfterCommit,
    ] {
        evidence_failure(boundary, None, true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_reconstructed_evidence_write_prevents_idle_reacquisition() {
    for boundary in [
        RecoveryWriteBoundary::BeforeCommit,
        RecoveryWriteBoundary::AfterCommit,
    ] {
        let fixture = FaultFixture::paused(
            RecoveryWrite::Evidence,
            RecoveryWriteBoundary::BeforeCommit,
            true,
        )
        .await
        .release(false)
        .await;
        let idle = fixture.current().await.value().clone();
        assert_eq!(idle.state, ControlState::Idle);
        let (entered, resume) = fixture.native.journal.pause_receiver_recovery_write(
            RecoveryWrite::Evidence,
            boundary,
            true,
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
        assert_eq!(fixture.current().await.value(), &idle);
        assert_eq!(fixture.native.nodes[2].stats().active_cells(), 0);
        let retained = fixture
            .native
            .journal
            .load_receiver_recovery_evidence(&fixture.accepted)
            .await
            .unwrap();
        assert_eq!(
            retained.is_some(),
            boundary == RecoveryWriteBoundary::AfterCommit
        );
        resume.send(()).unwrap();
        let failed = waiter.await.unwrap();
        assert!(failed.committed && matches!(failed.outcome.outcome, FleetOutcome::Unknown));
        assert_original_error(&failed);
        assert_eq!(fixture.current().await.value(), &idle);
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
        let activated = fixture.replay().await;
        assert!(
            activated.committed && activated.execution_error.is_none(),
            "{activated:?}"
        );
        assert!(matches!(
            activated.outcome.outcome,
            FleetOutcome::Activated(_)
        ));
        assert_eq!(fixture.current().await.value().epoch, idle.epoch + 1);
        if let Some(retained) = retained {
            assert_eq!(
                fixture
                    .native
                    .journal
                    .load_receiver_recovery_evidence(&fixture.accepted)
                    .await
                    .unwrap(),
                Some(retained)
            );
        }
        fixture.finish().await;
    }
}
