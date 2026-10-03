//! Real ready readers and immutable policy history through public reconciliation.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetAdapterFuture, FleetObservation, FleetObserver,
    FleetReaderEvacuationCheck, FleetRoster, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};
use cellule_runtime::node::NodeAdvertisement;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

async fn advertisements(directory: &NodeDirectory) -> Vec<NodeAdvertisement> {
    let mut nodes = Vec::new();
    for index in 0..3 {
        nodes.push(
            directory
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

fn observation(
    check: &FleetReaderEvacuationCheck,
    start: i64,
    nodes: Vec<NodeAdvertisement>,
) -> FleetObservation {
    FleetObservation::new(
        scope(),
        check.snapshot().registry(),
        check.snapshot().registry().revision(),
        start,
        clock().unwrap(),
        false,
        nodes,
        Vec::new(),
    )
    .unwrap()
}

async fn enabled(fixture: &Fixture) {
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .set_scheduling(snapshot.registry(), true)
        .await
        .unwrap();
}

struct Observer {
    journal: Arc<SqliteJournal>,
    verifier: FleetReaderEvacuationVerifier,
    directory: NodeDirectory,
    record: ReaderEvacuationRecord,
    retained: Mutex<Option<FleetObservation>>,
    observations: AtomicUsize,
    effects: AtomicUsize,
}
impl Observer {
    fn new(
        fixture: &Fixture,
        record: ReaderEvacuationRecord,
        retained: Option<FleetObservation>,
    ) -> Self {
        Self {
            journal: fixture.journal.clone(),
            verifier: fixture.verifier(),
            directory: fixture.directory.clone(),
            record,
            retained: Mutex::new(retained),
            observations: AtomicUsize::new(0),
            effects: AtomicUsize::new(0),
        }
    }
}
impl FleetObserver for Observer {
    fn observe<'a>(
        &'a self,
        _: &'a FleetRoster,
        _: i64,
        end: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            self.observations.fetch_add(1, Ordering::AcqRel);
            if let Some(observation) = self.retained.lock().unwrap().take() {
                return Ok(observation);
            }
            let check = self
                .verifier
                .recheck(self.journal.as_ref(), &self.record, end, clock)
                .await?;
            let start = check.interval().0;
            assert_eq!(check.record(), &self.record);
            Ok(
                observation(&check, start, advertisements(&self.directory).await)
                    .with_role_evacuations(vec![check], Vec::new())?,
            )
        })
    }
}
impl FleetTransport for Observer {
    fn dispatch<'a>(
        &'a self,
        _: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        self.effects.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Err(invalid("unexpected role-observation effect")) })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        self.effects.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Err(invalid("unexpected role-observation inspection")) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_evacuation_observation_retains_reconstructed_policy_through_public_reconciliation()
{
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap().clone();
    enabled(&fixture).await;
    let client = fixture.client().await;
    let check = fixture
        .verifier()
        .recheck(&client, &record, deadline(), clock)
        .await
        .unwrap();
    let interval = check.interval();
    let snapshot = check.snapshot().clone();
    let retained = observation(&check, interval.0, advertisements(&fixture.directory).await)
        .with_role_evacuations(vec![check], Vec::new())
        .unwrap();
    let check = &retained.reader_evacuations().unwrap()[0];
    assert_eq!(check.record(), &record);
    assert_eq!(check.record_digest(), record.digest().unwrap());
    assert_eq!(check.snapshot(), &snapshot);
    assert_eq!(check.interval(), interval);
    assert_eq!(check.replacements().len(), 1);
    assert_eq!(check.replacements()[0].session, session(2));
    assert!(retained.follower_evacuations().unwrap().is_empty());
    client.close().await.unwrap();
    drop(retained);
    let observer = Arc::new(Observer::new(&fixture, record.clone(), None));
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([206; 16]),
        FleetProfile::default(),
        fixture.journal.clone(),
        observer.clone(),
        observer.clone(),
    )
    .unwrap();
    let report = driver.reconcile_once(clock, deadline()).await.unwrap();
    assert_eq!(observer.observations.load(Ordering::Acquire), 1);
    assert_eq!(observer.effects.load(Ordering::Acquire), 0);
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&cellule_runtime::fleet::operations::DrainBlocker::IncompleteObservation)
    );
    assert_ne!(
        report.snapshot.head().maintenance().unwrap().phase(),
        cellule_runtime::fleet::operations::MaintenancePhase::Completed
    );
    assert_eq!(fixture.original_row().await, *record.retired());
    drop((driver, observer, capture));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_evacuation_observation_refuses_duplicates_restamping_and_missing_signed_replacement()
 {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap();
    let verifier = fixture.verifier();
    let first = verifier
        .recheck(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let second = verifier
        .recheck(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let base = observation(
        &first,
        first.interval().0,
        advertisements(&fixture.directory).await,
    );
    assert!(matches!(
        base.with_role_evacuations(vec![first, second], Vec::new()),
        Err(Error::Node(
            "fleet role evacuation obligation is duplicated"
        ))
    ));
    let check = verifier
        .recheck(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let base = observation(
        &check,
        check.interval().0 + 1,
        advertisements(&fixture.directory).await,
    );
    assert!(matches!(
        base.with_role_evacuations(vec![check], Vec::new()),
        Err(Error::Node("fleet role evacuation barrier differs"))
    ));
    let check = verifier
        .recheck(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let mut nodes = advertisements(&fixture.directory).await;
    nodes.retain(|node| node.session() != session(2));
    let base = observation(&check, check.interval().0, nodes);
    assert!(matches!(
        base.with_role_evacuations(vec![check], Vec::new()),
        Err(Error::Node("fleet role evacuation boot is missing"))
    ));
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_evacuation_observation_refuses_an_earlier_full_head_in_public_reconciliation() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap().clone();
    enabled(&fixture).await;
    let check = fixture
        .verifier()
        .recheck(fixture.journal.as_ref(), &record, deadline(), clock)
        .await
        .unwrap();
    let before = check.snapshot().clone();
    let retained = observation(
        &check,
        check.interval().0,
        advertisements(&fixture.directory).await,
    )
    .with_role_evacuations(vec![check], Vec::new())
    .unwrap();
    let observer = Arc::new(Observer::new(&fixture, record, Some(retained)));
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([206; 16]),
        FleetProfile::default(),
        fixture.journal.clone(),
        observer.clone(),
        observer.clone(),
    )
    .unwrap();
    assert!(matches!(
        driver.reconcile_once(clock, deadline()).await,
        Err(Error::Node("fleet role evacuation barrier differs"))
    ));
    let after = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(after.registry(), before.registry());
    assert_ne!(after.head(), before.head());
    assert_eq!(observer.observations.load(Ordering::Acquire), 1);
    assert_eq!(observer.effects.load(Ordering::Acquire), 0);
    drop((driver, observer, capture));
    fixture.finish().await;
}
