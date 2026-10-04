//! Real ready readers and immutable policy history through public reconciliation.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetAdapterFuture, FleetMaintenanceEnrollments, FleetObservation,
    FleetObserver, FleetReaderEvacuationCheck, FleetRoster, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};
use cellule_runtime::node::NodeAdvertisement;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) async fn advertisements(directory: &NodeDirectory) -> Vec<NodeAdvertisement> {
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
        roster: &'a FleetRoster,
        _: i64,
        end: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            self.observations.fetch_add(1, Ordering::AcqRel);
            if let Some(observation) = self.retained.lock().unwrap().take() {
                return Ok(observation);
            }
            let start = clock()?;
            let original =
                FleetMaintenanceEnrollments::collect(self.journal.as_ref(), roster, end, clock)
                    .await?;
            assert_eq!(original.original().enrollment_count(), 1);
            assert_eq!(original.entries().count(), 1);
            let checks = self
                .verifier
                .collect_maintenance(self.journal.as_ref(), &original, roster, end, clock)
                .await?;
            assert_eq!(checks.len(), 1);
            assert_eq!(checks[0].record(), &self.record);
            let observation = observation(&checks[0], start, advertisements(&self.directory).await)
                .with_role_evacuations(checks, Vec::new())?
                .with_maintenance_enrollments(original)?
                .check_maintenance_policies(roster, clock()?)?;
            assert!(
                observation
                    .maintenance_policy_coverage()
                    .unwrap()
                    .is_complete()
            );
            Ok(observation)
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
    let policy = report.maintenance_policy.unwrap();
    assert_eq!(policy.required, 1);
    assert_eq!(policy.checked, 1);
    assert_eq!(
        policy.pending
            + policy.established
            + policy.missing_policy
            + policy.source_successors
            + policy.unproven_nonexecution,
        0
    );
    assert_eq!(policy.registry, report.snapshot.registry());
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

async fn original_set(fixture: &Fixture) -> FleetMaintenanceEnrollments {
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(fixture.journal.as_ref(), &snapshot, deadline())
        .await
        .unwrap();
    FleetMaintenanceEnrollments::collect(fixture.journal.as_ref(), &roster, deadline(), clock)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_reader_set_retains_retired_history_in_both_attachment_orders() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap();
    assert_eq!(fixture.original_row().await, *record.retired());
    for original_first in [false, true] {
        let start = clock().unwrap();
        let original = original_set(&fixture).await;
        let check = fixture
            .verifier()
            .recheck(fixture.journal.as_ref(), record, deadline(), clock)
            .await
            .unwrap();
        let base = observation(&check, start, advertisements(&fixture.directory).await);
        let retained = if original_first {
            base.with_maintenance_enrollments(original)
                .unwrap()
                .with_role_evacuations(vec![check], Vec::new())
                .unwrap()
        } else {
            base.with_role_evacuations(vec![check], Vec::new())
                .unwrap()
                .with_maintenance_enrollments(original)
                .unwrap()
        };
        let original = retained.maintenance_enrollments().unwrap();
        assert_eq!(original.original().enrollment_count(), 1);
        assert_eq!(
            original.entries().collect::<Vec<_>>(),
            vec![&fixture.original]
        );
        assert_eq!(
            original.entries().next().unwrap().status(),
            EnrollmentStatus::Established
        );
        assert_eq!(retained.reader_evacuations().unwrap()[0].record(), record);
        let duplicate = original_set(&fixture).await;
        assert!(matches!(
            retained.with_maintenance_enrollments(duplicate),
            Err(Error::Control(
                "original maintenance enrollments already retained"
            ))
        ));
    }
    let original = original_set(&fixture).await;
    let check = fixture
        .verifier()
        .recheck(fixture.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let base = observation(
        &check,
        original.interval().0 + 1,
        advertisements(&fixture.directory).await,
    );
    assert!(matches!(
        base.with_maintenance_enrollments(original),
        Err(Error::Node(
            "original maintenance enrollment observation barrier differs"
        ))
    ));
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_reader_set_refuses_same_registry_with_an_earlier_head_in_public_reconciliation() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap().clone();
    enabled(&fixture).await;
    let start = clock().unwrap();
    let original = original_set(&fixture).await;
    let before = original.snapshot().clone();
    let check = fixture
        .verifier()
        .recheck(fixture.journal.as_ref(), &record, deadline(), clock)
        .await
        .unwrap();
    let retained = observation(&check, start, advertisements(&fixture.directory).await)
        .with_maintenance_enrollments(original)
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
        Err(Error::Node(
            "original maintenance enrollment observation barrier differs"
        ))
    ));
    let after = fixture.journal.load_snapshot(scope()).await.unwrap();
    assert_eq!(after.registry(), before.registry());
    assert_ne!(after.head(), before.head());
    assert_eq!(observer.observations.load(Ordering::Acquire), 1);
    assert_eq!(observer.effects.load(Ordering::Acquire), 0);
    drop((driver, observer, capture));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_retired_reader_cannot_be_hidden_by_a_subset_of_policy_checks() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap();
    let mut digests = Vec::new();
    for attach_policy in [false, true] {
        let start = clock().unwrap();
        let original = original_set(&fixture).await;
        let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
        let roster = FleetRoster::collect(fixture.journal.as_ref(), &snapshot, deadline())
            .await
            .unwrap();
        let check = fixture
            .verifier()
            .recheck(fixture.journal.as_ref(), record, deadline(), clock)
            .await
            .unwrap();
        let base = observation(&check, start, advertisements(&fixture.directory).await);
        let base = if attach_policy {
            base.with_role_evacuations(vec![check], Vec::new()).unwrap()
        } else {
            base
        };
        let checked = base
            .with_maintenance_enrollments(original)
            .unwrap()
            .check_maintenance_policies(&roster, clock().unwrap())
            .unwrap();
        let coverage = checked.maintenance_policy_coverage().unwrap();
        assert_eq!(coverage.is_complete(), attach_policy);
        assert_eq!(coverage.obligations().len(), 1);
        let row = &coverage.obligations()[0];
        assert_eq!(row.original(), Some(&fixture.original));
        assert_eq!(row.current(), record.retired());
        assert_eq!(
            row.status(),
            if attach_policy {
                cellule_host::fleet::FleetMaintenancePolicyStatus::Reader(record.digest().unwrap())
            } else {
                cellule_host::fleet::FleetMaintenancePolicyStatus::MissingPolicy
            }
        );
        digests.push(coverage.digest());
    }
    assert_ne!(digests[0], digests[1]);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checked_reader_policy_cannot_substitute_a_different_original_acceptance_digest() {
    let fixture = Fixture::new().await;
    fixture.spare().await;
    let capture = fixture.evacuate().await.unwrap();
    let publication = fixture.publish(&capture).await;
    let record = publication.record().unwrap();
    let mut replacements = Vec::new();
    for digest in record.pages() {
        replacements.extend(
            fixture
                .journal
                .load_reader_evacuation_page(scope(), *digest)
                .await
                .unwrap()
                .unwrap()
                .entries()
                .iter()
                .cloned(),
        );
    }
    let (changed, pages) = ReaderEvacuationRecord::new(
        record.operation().clone(),
        (record.head_digest(), record.registry()),
        record.retired().clone(),
        Digest::from_bytes([222; 32]),
        record.authority().clone(),
        record.policy_revision(),
        record.desired_readers(),
        record.minimum_sequence(),
        record.interval(),
        replacements,
    )
    .unwrap();
    let replacement = changed.clone();
    let database = fixture.root.path().join("evacuation.sqlite");
    tokio::task::spawn_blocking(move || {
        let mut connection = rusqlite::Connection::open(database).unwrap();
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        // Inject self-consistent damaged historical metadata. Current authority,
        // native reader readiness, request identity and the original set stay intact.
        for page in pages {
            tx.execute(
                "INSERT INTO reader_evacuation_pages(key,body) VALUES(?1,?2)",
                rusqlite::params![page.digest()?.as_bytes().as_slice(), page.to_bytes()?],
            )?;
        }
        tx.execute(
            "INSERT INTO reader_evacuations(key,body) VALUES(?1,?2)",
            rusqlite::params![
                replacement.digest()?.as_bytes().as_slice(),
                replacement.to_bytes()?
            ],
        )?;
        tx.execute(
            "UPDATE latest_reader_evacuations SET witness=?1 WHERE operation=?2 AND original=?3",
            rusqlite::params![
                replacement.digest()?.as_bytes().as_slice(),
                replacement.operation().id().as_bytes().as_slice(),
                replacement.retired().spec().key()?.as_bytes().as_slice()
            ],
        )?;
        tx.commit()?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .unwrap()
    .unwrap();
    let start = clock().unwrap();
    let original = original_set(&fixture).await;
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(fixture.journal.as_ref(), &snapshot, deadline())
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .verifier()
            .collect_maintenance(
                fixture.journal.as_ref(),
                &original,
                &roster,
                deadline(),
                clock
            )
            .await,
        Err(Error::Fenced)
    ));
    let check = fixture
        .verifier()
        .recheck(fixture.journal.as_ref(), &changed, deadline(), clock)
        .await
        .unwrap();
    let base = observation(&check, start, advertisements(&fixture.directory).await)
        .with_role_evacuations(vec![check], Vec::new())
        .unwrap()
        .with_maintenance_enrollments(original)
        .unwrap();
    assert!(matches!(
        base.check_maintenance_policies(&roster, clock().unwrap()),
        Err(Error::Fenced)
    ));
    drop(capture);
    fixture.finish().await;
}
