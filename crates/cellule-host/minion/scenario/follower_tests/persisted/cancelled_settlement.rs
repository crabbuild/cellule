//! The original native owner survives a cancelled controller publication waiter.
use super::*;
use crate::journal::ResultWriteBoundary;
use cellule_host::fleet::{
    FleetActionAcceptance, FleetActionCompletion, FleetActionJournal, FleetActionWorkState,
    FleetAdapterFuture, FleetReconciler, FleetRoleSettlement, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest, FleetOutcome, MaintenancePhase,
};
use std::future::Future;
use std::task::Poll;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_controller_waiter_before_role_result_write_keeps_original_owner() {
    run(ResultWriteBoundary::BeforeCommit).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_controller_waiter_after_role_result_commit_keeps_original_owner() {
    run(ResultWriteBoundary::AfterCommit).await;
}

async fn run(boundary: ResultWriteBoundary) {
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
    let transport = Arc::new(IssuedSettlement {
        inner: fleet.clone(),
        issued: Mutex::new(None),
    });
    let first = Arc::new(
        FleetReconciler::new(
            scope(),
            session(9),
            FleetProfile::default(),
            fixture.native.journal.clone(),
            fleet.clone(),
            transport.clone(),
        )
        .unwrap(),
    );
    let (entered, resume) = fixture.native.journal.pause_next_role_result(boundary);
    let task = tokio::spawn({
        let first = first.clone();
        async move { first.reconcile_once(clock, pass_deadline()).await }
    });
    super::super::captured(entered).await;
    let (action, proof) = transport.issued.lock().unwrap().take().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    {
        let work = fixture.nodes[1].fleet_action_work().unwrap().unwrap();
        assert_eq!(work.entries().len(), 1);
        assert_eq!(work.entries()[0].state(), FleetActionWorkState::Running);
        assert!(!work.entries()[0].response_received());
        assert_eq!(work.entries()[0].committed(), None);
    }
    let old = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(
        old.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    let original = fixture
        .native
        .journal
        .accept_action(&action, node_id(1), session(1), clock().unwrap())
        .await
        .unwrap();
    let FleetActionAcceptance::Existing { accepted, result } = original else {
        panic!("original acceptance missing");
    };
    assert_eq!(
        result.is_some(),
        boundary == ResultWriteBoundary::AfterCommit
    );
    // An exact duplicate joins the retained native task, rather than executing
    // the observation again. Poll it while the journal gate is still closed.
    let mut replay = Box::pin(fixture.nodes[1].apply_fleet_role_settlement(
        action.clone(),
        proof,
        clock().unwrap(),
    ));
    std::future::poll_fn(|cx| {
        assert!(matches!(replay.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    let client = Arc::new(client(&fixture).await);
    let driver = FleetReconciler::new(
        scope(),
        session(9),
        FleetProfile::default(),
        client.clone(),
        fleet.clone(),
        fleet.clone(),
    )
    .unwrap();
    // The new request carries fresh evidence at a renewed head. It cannot
    // replace a running original capture or use its historical receipt to close.
    let blocked = driver.reconcile_once(clock, pass_deadline()).await.unwrap();
    assert_eq!(blocked.allocated, 0);
    assert_eq!(
        blocked.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    assert!(
        blocked
            .blockers
            .contains(&cellule_runtime::fleet::operations::DrainBlocker::OutcomeUnknown)
    );
    let refusal = blocked.maintenance_failure.as_ref().unwrap();
    assert!(
        super::restart::has_conflict(refusal.as_ref()),
        "wrong running-owner refusal: {refusal:?}"
    );
    let current = client.load_snapshot(scope()).await.unwrap();
    assert!(current.head().revision() > old.head().revision());
    assert_eq!(
        current.head().maintenance().unwrap().phase(),
        MaintenancePhase::Evacuating
    );
    {
        let work = fixture.nodes[1].fleet_action_work().unwrap().unwrap();
        assert_eq!(work.entries().len(), 1);
        assert_eq!(work.entries()[0].state(), FleetActionWorkState::Running);
    }
    resume.send(()).unwrap();
    let completion = replay.await.unwrap();
    assert_eq!(completion.accepted, accepted);
    assert!(!completion.committed);
    assert!(
        matches!(completion.outcome.outcome, FleetOutcome::RolesSettledAt { head_revision, .. }
        if head_revision == old.head().revision())
    );
    assert!(
        matches!(completion.journal_error.as_deref(), Some(Error::Facility { name: "fleet-action-journal", source })
        if source.downcast_ref::<std::io::Error>().is_some())
    );
    assert_eq!(client.load_snapshot(scope()).await.unwrap(), current);
    // Join/removal is its own failed publication boundary. Return the original
    // Conflict and require another full capture before publishing a new receipt.
    let refusal = driver
        .reconcile_once(clock, pass_deadline())
        .await
        .unwrap_err();
    assert!(super::restart::has_conflict(&refusal));
    let next = driver.reconcile_once(clock, pass_deadline()).await.unwrap();
    assert!(next.maintenance_failure.is_none());
    assert_eq!(
        next.snapshot.head().maintenance().unwrap().phase(),
        MaintenancePhase::Closing
    );
    let published = client
        .accept_action(&action, node_id(1), session(1), clock().unwrap())
        .await
        .unwrap();
    let FleetActionAcceptance::Existing {
        accepted: unchanged,
        result: Some(published),
    } = published
    else {
        panic!("fresh settlement missing");
    };
    assert_eq!(unchanged, accepted);
    assert!(
        matches!(published.outcome, FleetOutcome::RolesSettledAt { head_revision, .. }
        if head_revision > old.head().revision())
    );
    assert_eq!(
        super::latest(&fixture, publication.record().unwrap()).await,
        *publication.record().unwrap()
    );
    let completed = driver.reconcile_once(clock, pass_deadline()).await.unwrap();
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
    super::restart::readback(&fixture).await;
    drop((first, driver, transport, fleet, capture, completion));
    client.close().await.unwrap();
    fixture.finish().await;
}

fn pass_deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

struct IssuedSettlement {
    inner: Arc<crate::scenario::adapters::LocalFleet>,
    issued: Mutex<Option<(FleetAction, FleetRoleSettlement)>>,
}

impl FleetTransport for IssuedSettlement {
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
        assert!(
            self.issued
                .lock()
                .unwrap()
                .replace((action.clone(), proof.clone()))
                .is_none()
        );
        self.inner.settle_roles(action, proof, end)
    }
}
