use super::*;
use fixture::FaultFixture;

async fn assert_reserved_without_actor(native: &super::super::fixture::Fixture) {
    // active_cells includes the held affine activation credit. The actual
    // native writer inventory must remain empty before the journal confirms.
    assert_eq!(native.nodes[2].stats().active_cells(), 1);
    assert_eq!(native.nodes[2].stats().worker_jobs(), 0);
    let page = native.nodes[2]
        .runtime()
        .fleet_cells_page(None, 128)
        .await
        .unwrap();
    assert!(
        !page
            .entries()
            .iter()
            .any(|row| matches!(row, CellInventoryEntry::Owned(_)))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_receiver_basis_write_prevents_cas_and_replays_exact_input() {
    for boundary in [
        RecoveryWriteBoundary::BeforeCommit,
        RecoveryWriteBoundary::AfterCommit,
    ] {
        let fixture = FaultFixture::paused(RecoveryWrite::Basis, boundary, true).await;
        assert_eq!(fixture.current().await, fixture.original);
        assert_reserved_without_actor(&fixture.native).await;
        let basis = fixture
            .native
            .journal
            .load_receiver_recovery_basis(&fixture.accepted)
            .await
            .unwrap();
        assert_eq!(
            basis.is_some(),
            boundary == RecoveryWriteBoundary::AfterCommit
        );
        if let Some(basis) = &basis {
            assert_eq!(basis.control(), &fixture.original);
        }
        let fixture = fixture.release(false).await;
        let failed = fixture.completion.as_ref().unwrap();
        assert!(failed.committed);
        assert!(matches!(failed.outcome.outcome, FleetOutcome::Unknown));
        assert_original_error(failed);
        assert_eq!(fixture.current().await.value(), &fixture.original);
        let activated = fixture.replay().await;
        assert!(
            activated.committed && activated.execution_error.is_none(),
            "{activated:?}"
        );
        assert!(matches!(
            activated.outcome.outcome,
            FleetOutcome::Activated(_)
        ));
        if let Some(basis) = basis {
            assert_eq!(
                fixture
                    .native
                    .journal
                    .load_receiver_recovery_basis(&fixture.accepted)
                    .await
                    .unwrap(),
                Some(basis)
            );
        }
        fixture.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_receiver_waiter_keeps_the_original_basis_and_takeover_owned() {
    for write in [RecoveryWrite::Basis, RecoveryWrite::Evidence] {
        let fixture = FaultFixture::paused(write, RecoveryWriteBoundary::BeforeCommit, false).await;
        let current = fixture.current().await;
        assert_eq!(
            current.epoch,
            fixture.original.epoch + u64::from(write == RecoveryWrite::Evidence)
        );
        assert_reserved_without_actor(&fixture.native).await;
        let basis = fixture
            .native
            .journal
            .load_receiver_recovery_basis(&fixture.accepted)
            .await
            .unwrap();
        assert_eq!(basis.is_some(), write == RecoveryWrite::Evidence);
        assert!(
            fixture
                .native
                .journal
                .load_receiver_recovery_evidence(&fixture.accepted)
                .await
                .unwrap()
                .is_none()
        );
        let fixture = fixture.release(true).await;
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
            fixture.original.epoch + 1
        );
        if let Some(basis) = basis {
            assert_eq!(
                fixture
                    .native
                    .journal
                    .load_receiver_recovery_basis(&fixture.accepted)
                    .await
                    .unwrap(),
                Some(basis)
            );
        }
        fixture.finish().await;
    }
}

mod continuation;
mod evidence;
mod history;
