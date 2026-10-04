//! Public reconciliation retains the checked source request without completing roles.
use super::*;
use cellule_host::fleet::{FleetActionCompletion, FleetObserver, FleetReconciler, FleetTransport};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};
struct Observer {
    journal: Arc<SqliteJournal>,
    verifier: FleetReaderEvacuationVerifier,
    provider: Provider,
    directory: NodeDirectory,
    effects: AtomicUsize,
}
impl FleetObserver for Observer {
    fn observe<'a>(
        &'a self,
        roster: &'a FleetRoster,
        _: i64,
        end: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            let started = clock()?;
            let original =
                FleetMaintenanceEnrollments::collect(self.journal.as_ref(), roster, end, clock)
                    .await?;
            let policies = self
                .verifier
                .collect_source_readers(
                    self.journal.as_ref(),
                    &original,
                    roster,
                    &self.provider,
                    end,
                    clock,
                )
                .await?;
            let mut ads = Vec::new();
            for index in 0..3 {
                ads.push(
                    self.directory
                        .load(session(index), clock()?)
                        .await?
                        .ok_or(Error::Fenced)?
                        .advertisement()
                        .clone(),
                );
            }
            Ok(FleetObservation::new(
                scope(),
                roster.snapshot().registry(),
                roster.snapshot().registry().revision(),
                started,
                clock()?,
                false,
                ads,
                Vec::new(),
            )?
            .with_source_reader_policies(policies)?
            .with_maintenance_enrollments(original)?
            .check_maintenance_policies(roster, clock()?)?)
        })
    }
}
impl FleetTransport for Observer {
    fn dispatch<'a>(
        &'a self,
        _: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(invalid("unexpected source-reader action")) })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(invalid("unexpected source-reader inspection")) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_reconstructed_journal_and_public_reconciler_preserve_native_inputs() {
    let source = SourceFixture::new().await;
    let client = SqliteJournal::open(
        source.fixture.root.path().join("evacuation.sqlite"),
        scope(),
        FleetProfile::default(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    let snapshot = client.load_snapshot(scope()).await.unwrap();
    client
        .set_scheduling(snapshot.registry(), true)
        .await
        .unwrap();
    let snapshot = client.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&client, &snapshot, deadline())
        .await
        .unwrap();
    let original = FleetMaintenanceEnrollments::collect(&client, &roster, deadline(), clock)
        .await
        .unwrap();
    let provider = Provider::stable(source.inputs.clone());
    let policies = source
        .verifier()
        .collect_source_readers(&client, &original, &roster, &provider, deadline(), clock)
        .await
        .unwrap();
    assert_eq!(policies.checks().len(), 1);
    drop((policies, provider, original, roster));
    client.close().await.unwrap();
    let observer = Arc::new(Observer {
        journal: source.fixture.journal.clone(),
        verifier: source.verifier(),
        provider: Provider::stable(source.inputs.clone()),
        directory: source.fixture.directory.clone(),
        effects: AtomicUsize::new(0),
    });
    let driver = FleetReconciler::new(
        scope(),
        SessionId::from_bytes([206; 16]),
        FleetProfile::default(),
        source.fixture.journal.clone(),
        observer.clone(),
        observer.clone(),
    )
    .unwrap();
    let report = driver.reconcile_once(clock, deadline()).await.unwrap();
    let progress = report.maintenance_policy.unwrap();
    assert_eq!(
        (
            progress.required,
            progress.checked,
            progress.source_successors
        ),
        (1, 1, 0)
    );
    assert_eq!(progress.registry, report.snapshot.registry());
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&cellule_runtime::fleet::operations::DrainBlocker::IncompleteObservation)
    );
    assert_eq!(observer.effects.load(Ordering::SeqCst), 0);
    assert_eq!(
        source
            .fixture
            .journal
            .load_snapshot(scope())
            .await
            .unwrap()
            .head()
            .maintenance()
            .unwrap()
            .phase(),
        cellule_runtime::fleet::operations::MaintenancePhase::Evacuating
    );
    drop((driver, observer));
    source.finish().await;
}
