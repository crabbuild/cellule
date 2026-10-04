//! Actual rejected native opening, joined producer and retained original proof.
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetEnrollmentNonexecution, FleetEnrollmentNonexecutionEvidence,
    FleetEnrollmentNonexecutionRequest, FleetMaintenanceEnrollments, FleetMaintenanceNonexecution,
    FleetMaintenancePolicyStatus, FleetObservation, FleetRoster,
};
use cellule_runtime::fleet::operations::{
    JournalTransition, MaintenanceEvent, MaintenanceOperation, OperationId,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

pub(in crate::scenario) struct Proofs {
    path: PathBuf,
    reads: AtomicUsize,
    fault: bool,
    pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}
impl Proofs {
    pub(in crate::scenario) fn new(path: PathBuf) -> Self {
        Self {
            path,
            reads: AtomicUsize::new(0),
            fault: false,
            pause: Mutex::new(None),
        }
    }
}
impl FleetEnrollmentNonexecution for Proofs {
    fn confirm_unexecuted<'a>(
        &'a self,
        request: &'a FleetEnrollmentNonexecutionRequest,
    ) -> FleetAdapterFuture<'a, Option<FleetEnrollmentNonexecutionEvidence>> {
        Box::pin(async move {
            let read = self.reads.fetch_add(1, Ordering::AcqRel);
            if self.fault && read == 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "retained nonexecution read failed",
                )
                .into());
            }
            let bytes = match std::fs::read(&self.path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let evidence = match bytes {
                None => None,
                Some(bytes) => {
                    if bytes.len() != 64 || bytes[..32] != *request.digest().as_bytes() {
                        return Err(
                            std::io::Error::other("original nonexecution binding differs").into(),
                        );
                    }
                    Some(FleetEnrollmentNonexecutionEvidence::new(
                        request,
                        Digest::from_bytes(bytes[32..].try_into()?),
                    )?)
                }
            };
            let pause = self.pause.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.await;
            }
            Ok(evidence)
        })
    }
}

async fn joined(source_maintenance: bool) -> (ReaderFixture, EnrollmentRecord) {
    let fixture = ReaderFixture::new().await;
    let (accepted, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let opening = tokio::spawn(async move { manager.activate(target, session(0)).await });
    accepted.await.unwrap();
    let original = fixture.rows().await[0].clone();
    assert_eq!(original.status(), EnrollmentStatus::Pending);
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert!(!page.entries()[0].opening_started);
    assert_eq!(page.jobs().running(), 1);
    drop(page);
    let mut snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .journal
        .bootstrap_registry(snapshot.registry())
        .await
        .unwrap();
    snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    snapshot = fixture
        .journal
        .claim_controller(
            scope(),
            snapshot.head().revision(),
            SessionId::from_bytes([210; 16]),
            clock().unwrap(),
        )
        .await
        .unwrap();
    let index = if source_maintenance { 0 } else { 1 };
    let now = clock().unwrap();
    for transition in [
        JournalTransition::BeginMaintenance(
            MaintenanceOperation::new(
                OperationId::from_bytes([211; 16]).unwrap(),
                Digest::from_bytes([212; 32]),
                node_id(index),
                session(index),
                2,
                now,
                now + 60_000,
            )
            .unwrap(),
        ),
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    ] {
        snapshot = fixture
            .journal
            .compare_exchange(
                &snapshot,
                snapshot.head().controller().unwrap().epoch,
                clock().unwrap(),
                &transition,
            )
            .await
            .unwrap();
    }
    // This is the real native admission refusal, before any reader SQL/VFS work.
    // Join the exact producer job before retaining its original terminal witness.
    fixture.node.runtime().node_admission().cordon().unwrap();
    resume.send(()).unwrap();
    let error = opening.await.unwrap().err().unwrap();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut refused = false;
    while let Some(error) = source {
        refused |= matches!(error.downcast_ref::<Error>(), Some(Error::CellDraining));
        source = error.source();
    }
    assert!(
        refused,
        "opening must retain the native admission refusal: {error:?}"
    );
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .is_err()
    );
    let terminal = fixture.rows().await[0].clone();
    assert_eq!(terminal.status(), EnrollmentStatus::Retired);
    assert!(terminal.established_evidence().is_none());
    assert_eq!(terminal.spec(), original.spec());
    (fixture, terminal)
}
async fn capture(fixture: &ReaderFixture) -> (FleetRoster, FleetMaintenanceEnrollments) {
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(fixture.journal.as_ref(), &snapshot, deadline())
        .await
        .unwrap();
    let original =
        FleetMaintenanceEnrollments::collect(fixture.journal.as_ref(), &roster, deadline(), clock)
            .await
            .unwrap();
    (roster, original)
}
pub(in crate::scenario) fn retain(path: &PathBuf, request: &FleetEnrollmentNonexecutionRequest) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(request.digest().as_bytes()).unwrap();
    file.write_all(request.terminal().settlement_evidence().unwrap().as_bytes())
        .unwrap();
    file.sync_all().unwrap();
}
fn observation(original: &FleetMaintenanceEnrollments) -> FleetObservation {
    FleetObservation::new(
        scope(),
        original.snapshot().registry(),
        original.snapshot().registry().revision(),
        original.interval().0,
        clock().unwrap(),
        false,
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_confirms_actual_joined_reader_after_provider_and_client_restart()
{
    for source in [false, true] {
        let (fixture, terminal) = joined(source).await;
        let (roster, original) = capture(&fixture).await;
        assert_eq!(original.entries().count(), 1);
        assert_eq!(
            original.entries().next().unwrap().status(),
            EnrollmentStatus::Pending
        );
        let request = FleetEnrollmentNonexecutionRequest::new(
            &original,
            &roster,
            terminal.spec().key().unwrap(),
        )
        .unwrap();
        let path = fixture.root.path().join("joined-original-reader.proof");
        retain(&path, &request);
        let provider = Proofs::new(path);
        let independent = SqliteJournal::open(
            fixture.root.path().join("journal.sqlite"),
            scope(),
            FleetProfile::default(),
            clock().unwrap(),
        )
        .await
        .unwrap();
        let before = independent.load_snapshot(scope()).await.unwrap();
        let checks = FleetMaintenanceNonexecution::collect(
            &independent,
            &original,
            &roster,
            &provider,
            deadline(),
            clock,
        )
        .await
        .unwrap();
        assert_eq!(provider.reads.load(Ordering::Acquire), 2);
        assert_eq!(checks.checks().count(), 1);
        assert_eq!(checks.checks().next().unwrap().0.terminal(), &terminal);
        let observation = observation(&original)
            .with_maintenance_nonexecution(checks)
            .unwrap()
            .with_maintenance_enrollments(original)
            .unwrap()
            .check_maintenance_policies(&roster, clock().unwrap())
            .unwrap();
        let coverage = observation.maintenance_policy_coverage().unwrap();
        assert!(coverage.is_complete());
        assert_eq!(coverage.progress().required, 1);
        assert_eq!(coverage.progress().checked, 1);
        assert_eq!(coverage.progress().nonexecution, 1);
        assert_eq!(coverage.progress().unproven_nonexecution, 0);
        assert!(matches!(
            coverage.obligations()[0].status(),
            FleetMaintenancePolicyStatus::Nonexecution(_)
        ));
        assert_eq!(independent.load_snapshot(scope()).await.unwrap(), before);
        assert_eq!(fixture.rows().await, vec![terminal]);
        independent.close().await.unwrap();
        fixture.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_missing_retained_proof_stays_unknown_despite_terminal_row() {
    let (fixture, _) = joined(false).await;
    let (roster, original) = capture(&fixture).await;
    let provider = Proofs::new(fixture.root.path().join("missing-original.proof"));
    let checks = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &original,
        &roster,
        &provider,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(provider.reads.load(Ordering::Acquire), 2);
    assert_eq!(checks.checks().count(), 0);
    let observation = observation(&original)
        .with_maintenance_enrollments(original)
        .unwrap()
        .with_maintenance_nonexecution(checks)
        .unwrap()
        .check_maintenance_policies(&roster, clock().unwrap())
        .unwrap();
    let coverage = observation.maintenance_policy_coverage().unwrap();
    assert!(!coverage.is_complete());
    assert_eq!(coverage.progress().unproven_nonexecution, 1);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_preserves_second_provider_error_and_rejects_substituted_witness()
{
    let (fixture, terminal) = joined(false).await;
    let (roster, original) = capture(&fixture).await;
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &roster, terminal.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("reader.proof");
    retain(&path, &request);
    let mut provider = Proofs::new(path.clone());
    provider.fault = true;
    let error = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &original,
        &roster,
        &provider,
        deadline(),
        clock,
    )
    .await
    .err()
    .unwrap();
    assert!(
        matches!(error, Error::Facility { name: "fleet-enrollment-nonexecution-provider", ref source }
        if source.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind()==std::io::ErrorKind::ConnectionReset))
    );
    assert!(
        FleetEnrollmentNonexecutionEvidence::new(&request, Digest::from_bytes([213; 32])).is_err()
    );
    let mut changed = std::fs::read(&path).unwrap();
    changed[32] ^= 1;
    std::fs::write(&path, changed).unwrap();
    assert!(
        FleetMaintenanceNonexecution::collect(
            fixture.journal.as_ref(),
            &original,
            &roster,
            &Proofs::new(path),
            deadline(),
            clock
        )
        .await
        .is_err()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_refuses_changed_full_head_during_provider_read() {
    let (fixture, terminal) = joined(false).await;
    let (roster, original) = capture(&fixture).await;
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &roster, terminal.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("reader.proof");
    retain(&path, &request);
    let provider = Proofs::new(path);
    let (entered, paused) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered, wait));
    let journal = fixture.journal.clone();
    let old = roster.snapshot().clone();
    let lookup = tokio::spawn(async move {
        FleetMaintenanceNonexecution::collect(
            journal.as_ref(),
            &original,
            &roster,
            &provider,
            deadline(),
            clock,
        )
        .await
    });
    paused.await.unwrap();
    let next = fixture
        .journal
        .claim_controller(
            scope(),
            old.head().revision(),
            old.head().controller().unwrap().claimant,
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old.registry(), next.registry());
    assert_ne!(old.head(), next.head());
    resume.send(()).unwrap();
    assert!(lookup.await.unwrap().is_err());
    assert_eq!(fixture.rows().await, vec![terminal]);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_bounds_whole_capture_and_refuses_late_attachment() {
    let (fixture, terminal) = joined(false).await;
    let (roster, original) = capture(&fixture).await;
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &roster, terminal.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("reader.proof");
    retain(&path, &request);
    let provider = Proofs::new(path);
    let now = clock().unwrap();
    for times in [[now, now - 1], [now, now + 30_001]] {
        let mut times = times.into_iter();
        assert!(matches!(
            FleetMaintenanceNonexecution::collect(
                fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                || Ok(times.next().unwrap_or(now + 30_001))
            )
            .await,
            Err(Error::Deadline)
        ));
    }
    assert_eq!(provider.reads.load(Ordering::Acquire), 0);
    assert!(matches!(
        FleetMaintenanceNonexecution::collect(
            fixture.journal.as_ref(),
            &original,
            &roster,
            &provider,
            Instant::now(),
            clock
        )
        .await,
        Err(Error::Deadline)
    ));
    let checks = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &original,
        &roster,
        &provider,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let observation = observation(&original)
        .with_maintenance_enrollments(original)
        .unwrap()
        .check_maintenance_policies(&roster, clock().unwrap())
        .unwrap();
    assert!(matches!(
        observation.with_maintenance_nonexecution(checks),
        Err(Error::Node("maintenance policy coverage inputs differ"))
    ));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_rejects_missing_to_present_proof_during_collection() {
    let (fixture, terminal) = joined(false).await;
    let (roster, original) = capture(&fixture).await;
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &roster, terminal.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("late-proof");
    let provider = Proofs::new(path.clone());
    let (entered, paused) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered, wait));
    let journal = fixture.journal.clone();
    let lookup = tokio::spawn(async move {
        FleetMaintenanceNonexecution::collect(
            journal.as_ref(),
            &original,
            &roster,
            &provider,
            deadline(),
            clock,
        )
        .await
    });
    paused.await.unwrap();
    retain(&path, &request);
    resume.send(()).unwrap();
    assert!(matches!(
        lookup.await.unwrap(),
        Err(Error::Control("original nonexecution evidence changed"))
    ));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_proof_survives_head_change_but_old_capsule_cannot_be_restamped() {
    let (fixture, terminal) = joined(false).await;
    let (before, original) = capture(&fixture).await;
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &before, terminal.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("reader-proof");
    retain(&path, &request);
    let provider = Proofs::new(path);
    let old_checks = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &original,
        &before,
        &provider,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let old = before.snapshot();
    let next = fixture
        .journal
        .claim_controller(
            scope(),
            old.head().revision(),
            old.head().controller().unwrap().claimant,
            clock().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old.registry(), next.registry());
    let (after, current) = capture(&fixture).await;
    let replay =
        FleetEnrollmentNonexecutionRequest::new(&current, &after, terminal.spec().key().unwrap())
            .unwrap();
    assert_eq!(request.digest(), replay.digest());
    assert!(matches!(
        FleetObservation::new(
            scope(),
            after.snapshot().registry(),
            after.snapshot().registry().revision(),
            original.interval().0,
            clock().unwrap(),
            false,
            Vec::new(),
            Vec::new()
        )
        .unwrap()
        .with_maintenance_enrollments(current)
        .unwrap()
        .with_maintenance_nonexecution(old_checks),
        Err(Error::Node(
            "maintenance nonexecution original capture differs"
        ))
    ));
    let (_, current) = capture(&fixture).await;
    let checks = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &current,
        &after,
        &provider,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let duplicate = FleetMaintenanceNonexecution::collect(
        fixture.journal.as_ref(),
        &current,
        &after,
        &provider,
        deadline(),
        clock,
    )
    .await
    .unwrap();
    let observation = observation(&current)
        .with_maintenance_nonexecution(checks)
        .unwrap()
        .with_maintenance_enrollments(current)
        .unwrap();
    assert!(matches!(
        observation.with_maintenance_nonexecution(duplicate),
        Err(Error::Control("maintenance nonexecution already retained"))
    ));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_nonexecution_deadline_drops_only_read_and_joins_no_new_effect() {
    let (fixture, terminal) = joined(false).await;
    let (roster, original) = capture(&fixture).await;
    let request =
        FleetEnrollmentNonexecutionRequest::new(&original, &roster, terminal.spec().key().unwrap())
            .unwrap();
    let path = fixture.root.path().join("reader-proof");
    retain(&path, &request);
    let provider = Proofs::new(path);
    let (entered, paused) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered, wait));
    let journal = fixture.journal.clone();
    let lookup = tokio::spawn(async move {
        FleetMaintenanceNonexecution::collect(
            journal.as_ref(),
            &original,
            &roster,
            &provider,
            Instant::now() + Duration::from_millis(100),
            clock,
        )
        .await
    });
    paused.await.unwrap();
    let result = lookup.await.unwrap();
    let _ = resume.send(());
    assert!(matches!(
        result,
        Err(Error::Facility {
            name: "fleet-maintenance-nonexecution-deadline",
            ..
        })
    ));
    assert_eq!(fixture.rows().await, vec![terminal]);
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
    fixture.finish().await;
}
