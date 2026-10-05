//! A lost settlement reply must be refreshed at the reconstructed driver's head.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetActionJournal, FleetAdapterFuture, FleetReconciler,
    FleetRoleSettlement, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest, FleetOutcome, MaintenancePhase,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstructed_controller_refreshes_native_settlement_after_a_lost_reply() {
    run(Restart::Renew, Publication::LostTransportReply).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_claimant_after_real_expiry_refreshes_settlement_and_fences_old_controller() {
    run(Restart::ReplaceAfterExpiry, Publication::LostTransportReply).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_role_result_write_reconstructs_current_settlement() {
    run(
        Restart::Renew,
        Publication::Failure(ResultWriteBoundary::BeforeCommit),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfirmed_role_result_commit_reconstructs_current_settlement() {
    run(
        Restart::Renew,
        Publication::Failure(ResultWriteBoundary::AfterCommit),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_claimant_after_expiry_resumes_failed_role_result_write() {
    run(
        Restart::ReplaceAfterExpiry,
        Publication::Failure(ResultWriteBoundary::BeforeCommit),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_claimant_after_expiry_resumes_unconfirmed_role_result_commit() {
    run(
        Restart::ReplaceAfterExpiry,
        Publication::Failure(ResultWriteBoundary::AfterCommit),
    )
    .await;
}

use crate::journal::ResultWriteBoundary;

#[derive(Clone, Copy)]
enum Publication {
    LostTransportReply,
    Failure(ResultWriteBoundary),
}

#[derive(Clone, Copy)]
enum Restart {
    Renew,
    ReplaceAfterExpiry,
}

async fn run(restart: Restart, publication_fault: Publication) {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let fleet = Arc::new(crate::scenario::adapters::LocalFleet {
        nodes: fixture.nodes.clone(),
        journal: fixture.native.journal.clone(),
        boots: fixture.boots.clone(),
        records: fixture.records.clone(),
        reader_verifier: None,
        capture_sequence: std::sync::atomic::AtomicU64::new(0),
        lose_release_replies: false,
        lost_release_replies: AtomicUsize::new(0),
        drop_closed_finalize_replies: AtomicUsize::new(0),
        expired_receiver_cleanups: AtomicUsize::new(0),
    });
    if let Publication::Failure(boundary) = publication_fault {
        fixture.native.journal.fail_next_role_result(boundary);
    }
    let transport = Arc::new(LostSettlementReply {
        inner: fleet.clone(),
        publication: publication_fault,
        original: Mutex::new(None),
    });
    let first = FleetReconciler::new(
        scope(),
        session(9),
        FleetProfile::default(),
        fixture.native.journal.clone(),
        fleet.clone(),
        transport.clone(),
    )
    .unwrap();
    let before = first
        .reconcile_once(clock, restart_deadline())
        .await
        .unwrap();
    assert!(before.maintenance_failure.is_some());
    assert_eq!(
        before.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    let original = transport.original.lock().unwrap().take().unwrap();
    let action = original.accepted.action().clone();
    assert_eq!(
        original.committed,
        matches!(publication_fault, Publication::LostTransportReply)
    );
    if let Publication::Failure(boundary) = publication_fault {
        let expected = match boundary {
            ResultWriteBoundary::BeforeCommit => "injected role result before commit",
            ResultWriteBoundary::AfterCommit => "injected role result after commit",
        };
        assert!(
            matches!(original.journal_error.as_deref(), Some(Error::Facility { name: "fleet-action-journal", source })
            if source.downcast_ref::<std::io::Error>().is_some_and(|error| error.to_string() == expected))
        );
        let accepted = fixture
            .native
            .journal
            .accept_action(&action, node_id(1), session(1), clock().unwrap())
            .await
            .unwrap();
        let cellule_host::fleet::FleetActionAcceptance::Existing { result, .. } = accepted else {
            panic!("original acceptance missing");
        };
        assert_eq!(
            result.is_some(),
            boundary == ResultWriteBoundary::AfterCommit
        );
        if let Some(result) = result {
            assert_eq!(*result, original.outcome);
        }
        let work = fixture.nodes[1].fleet_action_work().unwrap().unwrap();
        assert_eq!(work.entries().len(), 1);
        assert_eq!(work.entries()[0].committed(), Some(false));
        assert_eq!(work.entries()[0].outcome_known(), Some(true));
        assert!(work.entries()[0].journal_error().is_some());
    }
    assert!(
        matches!(original.outcome.outcome, FleetOutcome::RolesSettledAt { head_revision, .. } if head_revision == before.snapshot.head().revision())
    );
    // The transport dropped the successful reply before ReadyToClose. A fresh
    // driver over an independent client must renew and recheck the same intent.
    let claimant = match restart {
        Restart::Renew => session(9),
        Restart::ReplaceAfterExpiry => {
            let expires = before.snapshot.head().controller().unwrap().expires_at_ms;
            // Advance real wall time while the application's original lease
            // owners publish actual heartbeats. No logical clock jump or
            // fabricated advertisement keeps native participants admitted.
            while clock().unwrap() <= expires {
                for (index, boot) in fixture.boots.iter().enumerate() {
                    boot.refresh_capacity(index, fixture.native.journal.as_ref(), deadline())
                        .await
                        .unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(clock().unwrap() > expires);
            session(10)
        }
    };
    let client = Arc::new(client(&fixture).await);
    let driver = FleetReconciler::new(
        scope(),
        claimant,
        FleetProfile::default(),
        client.clone(),
        fleet.clone(),
        fleet.clone(),
    )
    .unwrap();
    if matches!(publication_fault, Publication::Failure(_)) {
        // The old read-only proof cannot be published at this renewed head.
        // Preserve that refusal, join its original owner, and require a wholly
        // fresh observation on the next pass rather than restamping its rows.
        let refused = driver
            .reconcile_once(clock, restart_deadline())
            .await
            .unwrap_err();
        assert!(
            has_conflict(&refused),
            "wrong publication refusal: {refused:?}"
        );
        let snapshot = client.load_snapshot(scope()).await.unwrap();
        assert_eq!(
            snapshot.head().maintenance().unwrap().phase(),
            MaintenancePhase::Evacuating
        );
        let work = fixture.nodes[1].fleet_action_work().unwrap().unwrap();
        assert!(
            work.entries().is_empty(),
            "joined stale role proof blocked fresh observation: {:?}",
            work.entries()
        );
        assert!(work.failure().is_none());
        assert_eq!(fixture.nodes[1].state(), NodeState::Ready);
    }
    let next = driver
        .reconcile_once(clock, restart_deadline())
        .await
        .unwrap();
    assert!(
        next.maintenance_failure.is_none(),
        "fresh settlement failed: {next:?}"
    );
    assert_eq!(
        next.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Closing
    );
    assert!(next.snapshot.head().revision() > before.snapshot.head().revision());
    let controller = next.snapshot.head().controller().unwrap();
    assert_eq!(controller.claimant, claimant);
    assert_eq!(
        controller.epoch,
        match restart {
            Restart::Renew => 1,
            Restart::ReplaceAfterExpiry => 2,
        }
    );
    if matches!(restart, Restart::ReplaceAfterExpiry) {
        let current = client.load_snapshot(scope()).await.unwrap();
        let rejected = first
            .reconcile_once(clock, restart_deadline())
            .await
            .unwrap_err();
        assert!(
            matches!(&rejected, Error::Facility { name: "fleet-journal", source }
            if matches!(source.downcast_ref::<cellule_runtime::fleet::operations::OperationError>(),
                Some(cellule_runtime::fleet::operations::OperationError::Fenced))),
            "old controller returned the wrong refusal: {rejected:?}"
        );
        assert_eq!(client.load_snapshot(scope()).await.unwrap(), current);
    }
    drop(first);
    let latest = fixture
        .native
        .journal
        .accept_action(&action, node_id(1), session(1), clock().unwrap())
        .await
        .unwrap();
    let cellule_host::fleet::FleetActionAcceptance::Existing {
        accepted,
        result: Some(result),
    } = latest
    else {
        panic!("settlement result missing")
    };
    assert_eq!(accepted.action(), &action);
    assert_eq!(accepted, original.accepted);
    assert!(
        matches!(result.outcome, FleetOutcome::RolesSettledAt { head_revision, .. } if head_revision > before.snapshot.head().revision())
    );
    assert_eq!(
        super::latest(&fixture, publication.record().unwrap()).await,
        *publication.record().unwrap()
    );
    let completed = driver
        .reconcile_once(clock, restart_deadline())
        .await
        .unwrap();
    assert_eq!(
        completed.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Completed
    );
    assert_eq!(fixture.nodes[1].state(), NodeState::Stopped);
    assert!(
        fixture
            .native
            .directory
            .is_withdrawn(session(1))
            .await
            .unwrap()
    );
    readback(&fixture).await;
    drop((driver, fleet, capture, original));
    client.close().await.unwrap();
    fixture.finish().await;
}

pub(super) async fn readback(fixture: &ManagedFixture) {
    // Rotation drained the original actor. Restore the exact current canonical
    // root instead of reading a stale handle or bypassing its admission gate.
    assert_eq!(fixture.records.len(), 1);
    let record = fixture.records.values().next().unwrap();
    let control = record
        .authority
        .load(record.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let root = control.value().ltx_root().unwrap();
    let destination = fixture.native.root.path().join("restart-readback.sqlite");
    assert_eq!(
        record
            .replica
            .open_root(&root)
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap(),
        root.position
    );
    let database = rusqlite::Connection::open_with_flags(
        destination,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    assert_eq!(
        database
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        29
    );
    assert_eq!(
        database
            .query_row(
                "SELECT result FROM sys_requests WHERE request_id=?1",
                [RequestId::from_bytes([1; 16]).as_bytes().as_slice()],
                |row| row.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
        29i64.to_be_bytes()
    );
    drop(database);
}

fn restart_deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

struct LostSettlementReply {
    inner: Arc<crate::scenario::adapters::LocalFleet>,
    publication: Publication,
    original: Mutex<Option<Arc<FleetActionCompletion>>>,
}
impl FleetTransport for LostSettlementReply {
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        end: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        self.inner.dispatch(action, end)
    }
    fn inspect<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        end: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        self.inner.inspect(request, end)
    }
    fn settle_roles<'a>(
        &'a self,
        action: &'a FleetAction,
        proof: &'a FleetRoleSettlement,
        end: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            let completion = self.inner.settle_roles(action, proof, end).await?;
            assert_eq!(
                completion.committed,
                matches!(self.publication, Publication::LostTransportReply)
            );
            assert!(matches!(
                completion.outcome.outcome,
                FleetOutcome::RolesSettledAt { .. }
            ));
            assert!(
                self.original
                    .lock()
                    .unwrap()
                    .replace(completion.clone())
                    .is_none()
            );
            match self.publication {
                Publication::LostTransportReply => {
                    Err(invalid("injected loss after committed role settlement"))
                }
                Publication::Failure(_) => Ok(completion),
            }
        })
    }
}

pub(super) fn has_conflict(mut source: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if let Some(error) = source.downcast_ref::<Arc<Error>>() {
            return has_conflict(error.as_ref());
        }
        if let Some(Error::FleetOperation(error)) = source.downcast_ref::<Error>() {
            return matches!(
                error.as_ref(),
                cellule_runtime::fleet::operations::OperationError::Conflict
            );
        }
        if matches!(
            source.downcast_ref::<cellule_runtime::fleet::operations::OperationError>(),
            Some(cellule_runtime::fleet::operations::OperationError::Conflict)
        ) {
            return true;
        }
        match source.source() {
            Some(next) => source = next,
            None => return false,
        }
    }
}
