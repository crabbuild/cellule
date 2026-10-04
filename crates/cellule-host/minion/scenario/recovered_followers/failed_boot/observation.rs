//! Complete recovered role coverage consumes fresh original retirement evidence.
//! This fixture has no writers and uses a joined process lifetime stand-in.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetFailedBootClosure, FleetFollowerReferences, FleetNodeInventory,
    FleetNodeInventoryScan, FleetObservation, FleetObserver, FleetRoleCoverage,
    FleetSnapshotRequest, FleetSnapshotSubject, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};

struct Observed {
    base: Fixture,
    nodes: Vec<Arc<CellNode>>,
    sequence: AtomicUsize,
}
impl Observed {
    async fn new() -> Self {
        let base = confirmation::settled(crate::scenario::clock().unwrap() - 10_005, true).await;
        let before = base.journal.load_snapshot(scope()).await.unwrap();
        base.journal
            .set_scheduling(before.registry(), true)
            .await
            .unwrap();
        let before = base.journal.load_snapshot(scope()).await.unwrap();
        base.journal
            .claim_controller(
                scope(),
                before.head().revision(),
                session(1),
                crate::scenario::clock().unwrap(),
            )
            .await
            .unwrap();
        let roster = base.roster().await;
        let mut nodes = Vec::new();
        for index in 1..3 {
            let intent = roster
                .intents()
                .iter()
                .find(|intent| intent.node() == node_id(index))
                .unwrap()
                .clone();
            // Reopen the actual canonically retired receiver stores. No live
            // lane, producer or replacement is inferred from their empty rows.
            let follower = base
                .sealed
                .log()
                .members()
                .iter()
                .position(|member| *member == node_id(index))
                .unwrap();
            let node = Arc::new(
                CellNodeBuilder::new(crate::scenario::application::compile().unwrap())
                    .with_runtime(SqlWorkerPool::new(2, 8).unwrap(), 128 << 20)
                    .with_replica_host(Host::default())
                    .with_follower_store(
                        base._root.path().join(format!("follower-{follower}")),
                        Limits::default(),
                        DiskBudget::new(1 << 30),
                    )
                    .with_session(session(index))
                    .with_fleet_startup_intent(intent.clone())
                    .build()
                    .unwrap(),
            );
            node.install_task_group(CancellationToken::new(), CancellationToken::new())
                .unwrap();
            node.install_fleet_actions(
                scope(),
                node_id(index),
                base.journal.clone(),
                Arc::new(crate::scenario::adapters::Cells {
                    records: Arc::new(HashMap::new()),
                    local: index,
                    root: base._root.path().into(),
                }),
            )
            .unwrap();
            let previous = base
                .directory
                .load_if_live(session(index), crate::scenario::clock().unwrap())
                .await
                .unwrap()
                .unwrap();
            let ad = startup::advertisement(index, &node, &intent).await.unwrap();
            let now = crate::scenario::clock().unwrap();
            let current = base.directory.refresh(&previous, ad, now).await.unwrap();
            node.install_node_lease_for_startup(
                NodeLeaseGuard::new(now, current.advertisement().expires_at_ms()).unwrap(),
            )
            .unwrap();
            node.confirm_fleet_startup(
                base.journal.as_ref(),
                startup::spec(&intent).unwrap().key().unwrap(),
            )
            .await
            .unwrap();
            node.start().unwrap();
            nodes.push(node);
        }
        Self {
            base,
            nodes,
            sequence: AtomicUsize::new(0),
        }
    }
    async fn closure(
        &self,
        roster: &FleetRoster,
    ) -> cellule_runtime::Result<FleetFailedBootClosure> {
        FleetFailedBootRetirement::capture_retained(
            self.base.journal.as_ref(),
            &self.base.directory,
            roster,
            self.base.process_request.as_ref().unwrap(),
            session(1),
            deadline(),
            crate::scenario::clock,
        )
        .await?
        .confirm(
            self.base.journal.as_ref(),
            &self.base.directory,
            &Processes::new(self.base.process_path()),
            session(1),
            deadline(),
            crate::scenario::clock,
        )
        .await
    }
    async fn page(
        &self,
        roster: &FleetRoster,
        index: usize,
        subject: FleetSnapshotSubject,
    ) -> Arc<cellule_host::fleet::FleetNodeSnapshot> {
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel) + 1;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.failed-boot-observation-test.v1\0");
        hash.update(&(sequence as u64).to_be_bytes());
        hash.update(session(index).as_bytes());
        let now = crate::scenario::clock().unwrap();
        let request = FleetSnapshotRequest::new(
            roster.snapshot().clone(),
            Digest::from_bytes(*hash.finalize().as_bytes()),
            node_id(index),
            session(index),
            subject,
            1,
            now,
            (now + 5_000).min(roster.snapshot().head().controller().unwrap().expires_at_ms),
        )
        .unwrap();
        self.nodes[index - 1].fleet_snapshot(request).await.unwrap()
    }
    async fn graph(&self, roster: &FleetRoster) -> FleetRoleCoverage {
        let mut native: Vec<FleetNodeInventory> = Vec::new();
        for index in 1..3 {
            let mut scan =
                FleetNodeInventoryScan::new(roster, node_id(index), session(index)).unwrap();
            while let Some(subject) = scan.next_subject().unwrap() {
                let page = self.page(roster, index, subject).await;
                scan.accept(page.request(), &page, crate::scenario::clock().unwrap())
                    .unwrap();
            }
            native.push(scan.finish().unwrap());
        }
        let mut foreign = Vec::new();
        for index in 0..3 {
            foreign.push(
                FleetFollowerReferences::collect(
                    &self.base.directory,
                    roster,
                    node_id(index),
                    1,
                    deadline(),
                    crate::scenario::clock,
                )
                .await
                .unwrap(),
            );
        }
        for (offset, inventory) in native.iter_mut().enumerate() {
            let mut check = inventory.recheck();
            while let Some(subject) = check.next_subject().unwrap() {
                let page = self.page(roster, offset + 1, subject).await;
                check
                    .accept(page.request(), &page, crate::scenario::clock().unwrap())
                    .unwrap();
            }
            check.finish().unwrap();
        }
        for refs in &mut foreign {
            refs.recheck(
                &self.base.directory,
                roster,
                1,
                deadline(),
                crate::scenario::clock,
            )
            .await
            .unwrap();
        }
        let graph = FleetRoleCoverage::check(
            roster,
            &native.iter().collect::<Vec<_>>(),
            &foreign.iter().collect::<Vec<_>>(),
            crate::scenario::clock().unwrap(),
        )
        .unwrap();
        assert_eq!(graph.native_boots(), 2);
        assert_eq!(graph.physical_nodes(), 3);
        assert_eq!(graph.pending_enrollments(), 0);
        assert!(native.iter().all(|inventory| inventory.cells().is_empty()));
        graph
    }
    async fn advertisements(&self) -> Vec<NodeAdvertisement> {
        let mut ads = Vec::new();
        for index in 1..3 {
            ads.push(
                self.base
                    .directory
                    .load_if_live(session(index), crate::scenario::clock().unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .advertisement()
                    .clone(),
            );
        }
        ads
    }
    async fn observe(&self, roster: &FleetRoster) -> cellule_runtime::Result<FleetObservation> {
        let closure = self.closure(roster).await?;
        let graph = self.graph(roster).await;
        let started = graph.interval().0.min(closure.interval().0);
        roster
            .confirm(self.base.journal.as_ref(), deadline())
            .await?;
        FleetObservation::new(
            scope(),
            roster.snapshot().registry(),
            roster.snapshot().registry().revision(),
            started,
            crate::scenario::clock().unwrap(),
            true,
            self.advertisements().await,
            Vec::new(),
        )?
        .with_role_coverage(graph)?
        .with_failed_boot_closures(vec![closure])
    }
    async fn finish(&self) {
        for node in &self.nodes {
            node.shutdown().await.unwrap();
            assert_eq!(node.stats().active_cells(), 0);
            assert_eq!(node.stats().retained_bytes(), 0);
        }
        self.base.journal.close().await.unwrap();
    }
}
struct Observer {
    fixture: Arc<Observed>,
    stale: Mutex<Option<FleetObservation>>,
}
impl FleetObserver for Observer {
    fn observe<'a>(
        &'a self,
        roster: &'a FleetRoster,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            if let Some(stale) = self.stale.lock().unwrap().take() {
                return Ok(stale);
            }
            Ok(self.fixture.observe(roster).await?)
        })
    }
}
struct NoEffects;
impl FleetTransport for NoEffects {
    fn dispatch<'a>(
        &'a self,
        _: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async {
            Err(std::io::Error::other("observation must not dispatch an effect").into())
        })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        Box::pin(async {
            Err(std::io::Error::other("observation must not inspect a movement").into())
        })
    }
}
#[tokio::test]
async fn failed_boot_observation_reconciler_consumes_complete_surviving_graph_without_effects() {
    let fixture = Arc::new(Observed::new().await);
    let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    let observation = fixture.observe(&fixture.base.roster().await).await.unwrap();
    assert_eq!(observation.failed_boot_closures().unwrap().len(), 1);
    let closure = &observation.failed_boot_closures().unwrap()[0];
    assert_eq!(closure.boot().spec().target.node, node_id(0));
    assert_eq!(
        closure.snapshot(),
        observation.role_coverage().unwrap().snapshot()
    );
    assert!(observation.roster().is_none());
    let observer = Arc::new(Observer {
        fixture: fixture.clone(),
        stale: Mutex::new(None),
    });
    let driver = FleetReconciler::new(
        scope(),
        session(1),
        FleetProfile::default(),
        fixture.base.journal.clone(),
        observer,
        Arc::new(NoEffects),
    )
    .unwrap();
    let report = driver
        .reconcile_once(crate::scenario::clock, deadline())
        .await
        .unwrap();
    assert_eq!(report.allocated, 0);
    assert_eq!(report.dispatched, 0);
    assert!(
        !report
            .blockers
            .contains(&cellule_runtime::fleet::operations::DrainBlocker::IncompleteObservation)
    );
    assert_eq!(report.snapshot.registry(), before.registry());
    assert_eq!(fixture.base.failed_boot().await, *closure.boot());
    fixture.finish().await;
}
#[tokio::test]
async fn failed_boot_observation_refuses_duplicate_and_restamped_capsules() {
    let fixture = Observed::new().await;
    let roster = fixture.base.roster().await;
    let first = fixture.closure(&roster).await.unwrap();
    let second = fixture.closure(&roster).await.unwrap();
    let started = first.interval().0;
    let observation = FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        1,
        started,
        crate::scenario::clock().unwrap(),
        false,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    assert!(
        observation
            .with_failed_boot_closures(vec![first, second])
            .is_err()
    );
    let observation = fixture.observe(&roster).await.unwrap();
    assert!(matches!(
        observation.with_failed_boot_closures(Vec::new()),
        Err(Error::Control("failed boot closures already retained"))
    ));
    for later_start in [false, true] {
        let closure = fixture.closure(&roster).await.unwrap();
        let (start, end) = closure.interval();
        let observation = FleetObservation::new(
            scope(),
            roster.snapshot().registry(),
            1,
            if later_start { start + 1 } else { start - 1 },
            if later_start {
                crate::scenario::clock().unwrap().max(start + 1)
            } else {
                end - 1
            },
            false,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert!(matches!(
            observation.with_failed_boot_closures(vec![closure]),
            Err(Error::Node("failed boot observation barrier differs"))
        ));
    }
    fixture.finish().await;
}
#[tokio::test]
async fn failed_boot_observation_reconciler_refuses_stale_full_head_with_same_registry() {
    let fixture = Arc::new(Observed::new().await);
    let original = fixture.base.roster().await;
    let observation = fixture.observe(&original).await.unwrap();
    let before = fixture.base.journal.load_snapshot(scope()).await.unwrap();
    let next = fixture
        .base
        .journal
        .claim_controller(
            scope(),
            before.head().revision(),
            session(1),
            crate::scenario::clock().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next.registry(), before.registry());
    assert_ne!(next.head(), before.head());
    let observer = Arc::new(Observer {
        fixture: fixture.clone(),
        stale: Mutex::new(Some(observation)),
    });
    let driver = FleetReconciler::new(
        scope(),
        session(1),
        FleetProfile::default(),
        fixture.base.journal.clone(),
        observer,
        Arc::new(NoEffects),
    )
    .unwrap();
    assert!(
        driver
            .reconcile_once(crate::scenario::clock, deadline())
            .await
            .is_err()
    );
    assert_eq!(
        fixture.base.failed_boot().await.status(),
        EnrollmentStatus::Retired
    );
    fixture.finish().await;
}

#[tokio::test]
async fn failed_boot_observation_refuses_retired_session_advertisement() {
    let fixture = Observed::new().await;
    let roster = fixture.base.roster().await;
    let closure = fixture.closure(&roster).await.unwrap();
    let current = fixture.advertisements().await;
    let original = &current[0];
    let now = crate::scenario::clock().unwrap();
    let key = SigningKey::from_bytes(&[1; 32]);
    // Shape-valid signed adapter input cannot revive a permanently fenced boot.
    // No canonical advertisement is created or refreshed for the old session.
    let old = NodeAdvertisement::sign(
        node_id(0),
        session(0),
        owner(0).endpoint,
        scope().fleet,
        original.certificate(),
        original.image(),
        original.release(),
        &key,
        1,
        now,
        now + 10_000,
        original.module_digests().to_vec(),
        original.peer_versions().to_vec(),
        original.failure_domain().clone(),
        original.capacity(),
    )
    .unwrap()
    .with_operational_placement(
        original.placement_capacity().unwrap(),
        cellule_runtime::node::NodeOperationalSample {
            sequence: 1,
            observed_at_ms: now,
            mode: cellule_runtime::node::NodeMode::Active,
            pressure: NodePressure::Normal,
        },
        &key,
    )
    .unwrap();
    let observation = FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        1,
        closure.interval().0,
        now,
        false,
        vec![old],
        Vec::new(),
    )
    .unwrap();
    assert!(matches!(
        observation.with_failed_boot_closures(vec![closure]),
        Err(Error::Node("failed boot observation barrier differs"))
    ));
    fixture.finish().await;
}
