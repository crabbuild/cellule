//! Actual managed writer handoff, exact native joining and current source policy.
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetFailedBootProcessEvidence, FleetFailedBootProcessRequest,
    FleetFailedBootProcesses, FleetFailedBootRetirement, FleetFailedReaderRetirement,
    FleetJournalSnapshot, FleetMaintenanceEnrollments, FleetObservation,
    FleetReaderEvacuationVerifier, FleetRoster, FleetSourceReaderInputs, FleetSourceReaderPolicies,
    FleetSourceReaderRetirement, FleetSourceReaderSuccessors,
};
use cellule_runtime::{fleet::operations::EnrollmentRole, read_policy::ReadPolicyStore};
use std::sync::atomic::{AtomicUsize, Ordering};

mod fixture;
mod observation;
mod races;
mod tests;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
struct SourceFixture {
    fixture: Fixture,
    inputs: Arc<FleetSourceReaderInputs>,
    successor: CellHandle,
}
struct Provider {
    inputs: Option<Arc<FleetSourceReaderInputs>>,
    later: Option<Option<Arc<FleetSourceReaderInputs>>>,
    calls: AtomicUsize,
}
impl Provider {
    fn stable(inputs: Arc<FleetSourceReaderInputs>) -> Self {
        Self {
            inputs: Some(inputs),
            later: None,
            calls: AtomicUsize::new(0),
        }
    }
}
impl FleetSourceReaderSuccessors for Provider {
    fn successor<'a>(
        &'a self,
        row: &'a EnrollmentRecord,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<Arc<FleetSourceReaderInputs>>> {
        Box::pin(async move {
            assert_eq!(expected.head().scope(), scope());
            assert_eq!(row.spec().source.unwrap().node, node_id(0));
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(if call > 0 {
                self.later.clone().unwrap_or_else(|| self.inputs.clone())
            } else {
                self.inputs.clone()
            })
        })
    }
}
impl SourceFixture {
    fn verifier(&self) -> FleetReaderEvacuationVerifier {
        FleetReaderEvacuationVerifier::new(
            self.fixture.directory.clone(),
            CellAuthority::new(self.fixture.layout.clone()),
            ReadPolicyStore::new(self.fixture.layout.clone()),
            self.fixture.peer.clone(),
        )
    }
    async fn basis(&self) -> (FleetRoster, FleetMaintenanceEnrollments) {
        let snapshot = self.fixture.journal.load_snapshot(scope()).await.unwrap();
        let roster = FleetRoster::collect(self.fixture.journal.as_ref(), &snapshot, deadline())
            .await
            .unwrap();
        let original = FleetMaintenanceEnrollments::collect(
            self.fixture.journal.as_ref(),
            &roster,
            deadline(),
            clock,
        )
        .await
        .unwrap();
        assert_eq!(original.entries().count(), 1);
        (roster, original)
    }
    async fn collect(
        &self,
        provider: &Provider,
    ) -> cellule_runtime::Result<FleetSourceReaderPolicies> {
        let (roster, original) = self.basis().await;
        self.verifier()
            .collect_source_readers(
                self.fixture.journal.as_ref(),
                &original,
                &roster,
                provider,
                deadline(),
                clock,
            )
            .await
    }
    async fn finish(self) {
        drop(self.inputs);
        drop(self.successor);
        self.fixture.finish().await;
    }
}
