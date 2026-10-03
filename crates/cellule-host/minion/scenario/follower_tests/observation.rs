//! Retained role evidence through the exported observation and reconciler paths.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetAdapterFuture, FleetObservation, FleetObserver, FleetOwnedCell,
    FleetRoleCoverage, FleetRoster, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};

async fn nodes(fixture: &ManagedFixture) -> Vec<NodeAdvertisement> {
    let mut nodes = Vec::new();
    for index in 0..fixture.nodes.len() {
        nodes.push(
            fixture
                .native
                .directory
                .load(session(index), clock().unwrap())
                .await
                .unwrap()
                .unwrap()
                .advertisement()
                .clone(),
        );
    }
    nodes
}

async fn enable(fixture: &ManagedFixture) {
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .native
        .journal
        .set_scheduling(snapshot.registry(), true)
        .await
        .unwrap();
}

fn observation(
    roster: &FleetRoster,
    coverage: &FleetRoleCoverage,
    nodes: Vec<NodeAdvertisement>,
    cells: Vec<FleetOwnedCell>,
) -> FleetObservation {
    FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        coverage.interval().0,
        clock().unwrap(),
        false,
        nodes,
        cells,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exported_observation_retains_original_role_graph_and_rejects_replacement_or_restamping() {
    let fixture = ManagedFixture::new().await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = coverage::captures(&fixture, &roster).await;
    coverage::native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    coverage::foreign_rechecks(&fixture, &roster, &mut foreign).await;
    let graph = coverage::check(&roster, &native, &foreign).unwrap();
    let digest = graph.digest();
    let interval = graph.interval();
    let advertised = nodes(&fixture).await;
    let cells = native
        .iter()
        .flat_map(|i| i.cells().iter().cloned())
        .collect::<Vec<_>>();
    let observation = observation(&roster, &graph, advertised.clone(), cells.clone())
        .with_role_coverage(graph)
        .unwrap();
    assert_eq!(observation.role_coverage().unwrap().digest(), digest);
    assert_eq!(observation.role_coverage().unwrap().interval(), interval);
    assert_eq!(
        observation.role_coverage().unwrap().snapshot(),
        roster.snapshot()
    );
    assert!(
        observation
            .with_role_coverage(coverage::check(&roster, &native, &foreign).unwrap())
            .is_err()
    );
    for (started, finished, registry) in [
        (
            interval.0 + 1,
            clock().unwrap(),
            roster.snapshot().registry(),
        ),
        (interval.0, interval.1 - 1, roster.snapshot().registry()),
        (
            interval.0,
            clock().unwrap(),
            roster
                .snapshot()
                .registry()
                .advance(roster.snapshot().registry().revision())
                .unwrap(),
        ),
    ] {
        let observation = FleetObservation::new(
            scope(),
            registry,
            registry.revision(),
            started,
            finished,
            false,
            advertised.clone(),
            cells.clone(),
        )
        .unwrap();
        assert!(
            observation
                .with_role_coverage(coverage::check(&roster, &native, &foreign).unwrap())
                .is_err()
        );
    }
    drop((native, foreign));
    fixture.finish().await;
}

struct FreshObserver(Arc<ManagedFixture>);
impl FleetObserver for FreshObserver {
    fn observe<'a>(
        &'a self,
        roster: &'a FleetRoster,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            let (mut native, mut foreign, mut sequence) = coverage::captures(&self.0, roster).await;
            coverage::native_rechecks(&self.0, roster, &mut native, &mut sequence).await;
            coverage::foreign_rechecks(&self.0, roster, &mut foreign).await;
            let graph = coverage::check(roster, &native, &foreign)?;
            let cells = native
                .iter()
                .flat_map(|i| i.cells().iter().cloned())
                .collect();
            Ok(observation(roster, &graph, nodes(&self.0).await, cells)
                .with_role_coverage(graph)?)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_role_graph_cannot_upgrade_partial_adapter_observation_to_count_coverage() {
    let fixture = Arc::new(ManagedFixture::new().await);
    enable(&fixture).await;
    let transport = Arc::new(RetainedObserver {
        observation: Mutex::new(None),
        calls: AtomicUsize::new(0),
    });
    let observer = Arc::new(FreshObserver(fixture.clone()));
    let driver = FleetReconciler::new(
        scope(),
        session(9),
        FleetProfile::default(),
        fixture.native.journal.clone(),
        observer.clone(),
        transport.clone(),
    )
    .unwrap();
    let report = driver
        .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        report
            .blockers
            .contains(&cellule_runtime::fleet::operations::DrainBlocker::IncompleteObservation)
    );
    assert_eq!(report.allocated, 0);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    drop((driver, observer, transport));
    Arc::try_unwrap(fixture).ok().unwrap().finish().await;
}

struct RetainedObserver {
    observation: Mutex<Option<FleetObservation>>,
    calls: AtomicUsize,
}
impl FleetObserver for RetainedObserver {
    fn observe<'a>(
        &'a self,
        _: &'a FleetRoster,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move { Ok(self.observation.lock().unwrap().take().unwrap()) })
    }
}
impl FleetTransport for RetainedObserver {
    fn dispatch<'a>(
        &'a self,
        _: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(invalid("unexpected effect before role barrier validation")) })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(invalid(
                "unexpected inspection before role barrier validation",
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciler_refuses_role_graph_from_an_earlier_head_with_the_same_registry() {
    let fixture = ManagedFixture::new().await;
    enable(&fixture).await;
    let roster = aggregate::roster(&fixture).await;
    let (mut native, mut foreign, mut sequence) = coverage::captures(&fixture, &roster).await;
    coverage::native_rechecks(&fixture, &roster, &mut native, &mut sequence).await;
    coverage::foreign_rechecks(&fixture, &roster, &mut foreign).await;
    let graph = coverage::check(&roster, &native, &foreign).unwrap();
    let cells = native
        .iter()
        .flat_map(|i| i.cells().iter().cloned())
        .collect();
    let observation = observation(&roster, &graph, nodes(&fixture).await, cells)
        .with_role_coverage(graph)
        .unwrap();
    let observer = Arc::new(RetainedObserver {
        observation: Mutex::new(Some(observation)),
        calls: AtomicUsize::new(0),
    });
    let driver = FleetReconciler::new(
        scope(),
        session(9),
        FleetProfile::default(),
        fixture.native.journal.clone(),
        observer.clone(),
        observer.clone(),
    )
    .unwrap();
    assert!(matches!(
        driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await,
        Err(Error::Node("fleet role coverage roster differs"))
    ));
    let changed = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(changed.registry(), roster.snapshot().registry());
    assert_ne!(changed.head(), roster.snapshot().head());
    assert!(changed.head().attempts().is_empty());
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    drop((driver, observer, native, foreign));
    fixture.finish().await;
}
