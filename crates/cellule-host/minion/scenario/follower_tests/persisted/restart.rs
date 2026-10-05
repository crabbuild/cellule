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
    let transport = Arc::new(LostSettlementReply {
        inner: fleet.clone(),
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
    assert!(original.committed);
    assert!(
        matches!(original.outcome.outcome, FleetOutcome::RolesSettledAt { head_revision, .. } if head_revision == before.snapshot.head().revision())
    );
    // The transport dropped the successful reply before ReadyToClose. A fresh
    // driver over an independent client must renew and recheck the same intent.
    drop((first, original));
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
    drop((driver, fleet, capture));
    client.close().await.unwrap();
    fixture.finish().await;
}

fn restart_deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

struct LostSettlementReply {
    inner: Arc<crate::scenario::adapters::LocalFleet>,
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
            assert!(completion.committed);
            assert!(matches!(
                completion.outcome.outcome,
                FleetOutcome::RolesSettledAt { .. }
            ));
            assert!(self.original.lock().unwrap().replace(completion).is_none());
            Err(invalid("injected loss after committed role settlement"))
        })
    }
}
