//! A real actor activation invalidates the follower-policy inventory interval.
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetNodeSnapshot, FleetSnapshotRequest, FleetSnapshotSubject,
    FleetSnapshotTransport,
};

struct HeldInventory {
    native: Snapshots,
    source_cells: AtomicUsize,
    gate: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

fn fleet(fixture: &ManagedFixture) -> crate::scenario::adapters::LocalFleet {
    crate::scenario::adapters::LocalFleet {
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
    }
}

impl FleetSnapshotTransport for HeldInventory {
    fn capture<'a>(
        &'a self,
        request: &'a FleetSnapshotRequest,
        end: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetNodeSnapshot>> {
        Box::pin(async move {
            // Hold the fresh category check after the original complete page.
            // Both replies come from the real node; no proof bytes are altered.
            if request.node() == node_id(0)
                && request.subject() == &FleetSnapshotSubject::Cells(None)
                && self.source_cells.fetch_add(1, Ordering::AcqRel) == 1
            {
                let gate = self.gate.lock().unwrap().take().unwrap();
                gate.0.send(()).unwrap();
                gate.1.await.unwrap();
            }
            self.native.capture(request, end).await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_observer_changed_inventory_blocks_policy_until_a_fresh_complete_scan() {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let record = publication.record().unwrap().clone();
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        scope().application,
        application::NAMESPACE,
        &[2],
    )
    .unwrap();
    let app = application::compile().unwrap();
    let catalog = CellCatalog::new(fixture.native.layout.clone(), target.tenant())
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Sql,
                app.registry().module_digests()[0],
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let incarnation = IncarnationId::from_bytes([2; 16]);
    let authority = CellAuthority::new(fixture.native.layout.clone());
    let initial = authority
        .create_initial(&catalog, incarnation, owner(0))
        .await
        .unwrap();
    let replica = CellReplica::new(
        fixture.native.layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        limits(),
    )
    .unwrap();
    let mut records = fixture
        .records
        .iter()
        .map(|(cell, record)| {
            (
                *cell,
                Record {
                    target: record.target.clone(),
                    incarnation: record.incarnation,
                    catalog: record.catalog.clone(),
                    replica: record.replica.clone(),
                    authority: record.authority.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    records.insert(
        target.cell_id(),
        Record {
            target,
            incarnation,
            catalog: catalog.clone(),
            replica: replica.clone(),
            authority: authority.clone(),
        },
    );
    let mut fleet = fleet(&fixture);
    let roster = aggregate::roster(&fixture).await;
    let (entered, paused) = tokio::sync::oneshot::channel();
    let (resume, released) = tokio::sync::oneshot::channel();
    let snapshots = Arc::new(HeldInventory {
        native: Snapshots::new(&fixture),
        source_cells: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered, released))),
    });
    let mut observation = Box::pin(crate::scenario::observation::observe_with_snapshots(
        &fleet,
        &roster,
        deadline(),
        snapshots.clone(),
    ));
    tokio::select! {
        result = &mut observation => panic!("completed before held recheck: {}", result.is_ok()),
        result = paused => result.unwrap(),
    }
    let handle = fixture.nodes[0]
        .runtime()
        .bootstrap(
            catalog,
            replica,
            authority,
            initial,
            fixture.native.root.path().join("second.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (31)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    resume.send(()).unwrap();
    let changed = observation.await;
    fleet.records = Arc::new(records);
    let fresh = crate::scenario::observation::observe(&fleet, &roster, deadline()).await;
    let unchanged = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let stored_record = stored(&fixture, record.digest().unwrap()).await;
    let pages = snapshots.source_cells.load(Ordering::Acquire);
    drop((handle, capture, fleet, snapshots));
    fixture.finish().await;

    // Join all native owners before asserting either observer result.
    assert_eq!(unchanged, *roster.snapshot());
    assert_eq!(stored_record, record);
    assert_eq!(pages, 2);
    let changed = changed.unwrap();
    assert!(changed.role_coverage().is_none());
    let missing = changed.maintenance_policy_coverage().unwrap();
    assert!(!missing.is_complete());
    assert_eq!(
        (missing.progress().required, missing.progress().checked),
        (1, 0)
    );
    let fresh = fresh.unwrap();
    let checked = fresh.maintenance_policy_coverage().unwrap();
    assert!(checked.is_complete());
    assert_eq!(
        (checked.progress().required, checked.progress().checked),
        (1, 1)
    );
    assert_eq!(
        checked.obligations()[0].status(),
        cellule_host::fleet::FleetMaintenancePolicyStatus::Follower(record.digest().unwrap()),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_observer_fenced_inventory_preserves_the_original_source_error() {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let record = publication.record().unwrap().clone();
    let fleet = fleet(&fixture);
    let roster = aggregate::roster(&fixture).await;
    let (entered, paused) = tokio::sync::oneshot::channel();
    let (resume, released) = tokio::sync::oneshot::channel();
    let snapshots = Arc::new(HeldInventory {
        native: Snapshots::new(&fixture),
        source_cells: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered, released))),
    });
    let mut observation = Box::pin(crate::scenario::observation::observe_with_snapshots(
        &fleet,
        &roster,
        deadline(),
        snapshots.clone(),
    ));
    tokio::select! {
        result = &mut observation => panic!("completed before held recheck: {}", result.is_ok()),
        result = paused => result.unwrap(),
    }
    fixture.boots[0].guard.as_ref().unwrap().fence();
    resume.send(()).unwrap();
    let result = observation.await;
    let stored_record = stored(&fixture, record.digest().unwrap()).await;
    drop((capture, fleet, snapshots));
    fixture.finish().await;
    assert_eq!(stored_record, record);
    let error = result.err().unwrap().downcast::<Error>().unwrap();
    let Error::Facility { name, source } = *error else {
        panic!("native snapshot source error was flattened");
    };
    assert_eq!(name, "fleet-native-snapshot-transport");
    assert!(matches!(
        **source.downcast_ref::<Arc<Error>>().unwrap(),
        Error::Fenced,
    ));
}
