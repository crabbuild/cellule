//! Journal eligibility and binding cases; actual producer joining is separate.
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetEnrollmentNonexecution, FleetEnrollmentNonexecutionEvidence,
    FleetEnrollmentNonexecutionRequest, FleetMaintenanceNonexecution,
};
use std::sync::atomic::{AtomicUsize, Ordering};
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
async fn freeze(fixture: &Fixture) {
    for transition in [
        JournalTransition::BeginMaintenance(request(3, 3)),
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    ] {
        fixture.transition(transition).await;
    }
}
async fn capture(fixture: &Fixture) -> (FleetRoster, FleetMaintenanceEnrollments) {
    let snapshot = fixture.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(&fixture.journal, &snapshot, deadline())
        .await
        .unwrap();
    let original =
        FleetMaintenanceEnrollments::collect(&fixture.journal, &roster, deadline(), || Ok(1))
            .await
            .unwrap();
    (roster, original)
}
struct Provider {
    reads: AtomicUsize,
    evidence: Option<FleetEnrollmentNonexecutionEvidence>,
}
impl FleetEnrollmentNonexecution for Provider {
    fn confirm_unexecuted<'a>(
        &'a self,
        _: &'a FleetEnrollmentNonexecutionRequest,
    ) -> FleetAdapterFuture<'a, Option<FleetEnrollmentNonexecutionEvidence>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::AcqRel);
            Ok(self.evidence.clone())
        })
    }
}
#[tokio::test]
async fn maintenance_nonexecution_never_certifies_pending_or_installed_roles() {
    let fixture = Fixture::new().await;
    let FleetEnrollmentAcceptance::New(accepted) = fixture
        .journal
        .accept_enrollment(&enrollment(216, 1), 0)
        .await
        .unwrap()
    else {
        panic!("new required");
    };
    freeze(&fixture).await;
    let provider = Provider {
        reads: AtomicUsize::new(0),
        evidence: None,
    };
    for event in [
        None,
        Some(EnrollmentEvent::Established(Digest::from_bytes([217; 32]))),
        Some(EnrollmentEvent::Retired(Digest::from_bytes([218; 32]))),
    ] {
        if let Some(event) = event {
            fixture
                .journal
                .publish_enrollment_result(&accepted, event, 0)
                .await
                .unwrap();
        }
        let (roster, original) = capture(&fixture).await;
        assert!(
            FleetEnrollmentNonexecutionRequest::new(
                &original,
                &roster,
                accepted.spec().key().unwrap()
            )
            .is_err()
        );
        let checks = FleetMaintenanceNonexecution::collect(
            &fixture.journal,
            &original,
            &roster,
            &provider,
            deadline(),
            || Ok(1),
        )
        .await
        .unwrap();
        assert_eq!(checks.checks().count(), 0);
        let observation = FleetObservation::new(
            scope(),
            roster.snapshot().registry(),
            roster.snapshot().registry().revision(),
            0,
            1,
            false,
            Vec::new(),
            Vec::new(),
        )
        .unwrap()
        .with_maintenance_enrollments(original)
        .unwrap()
        .with_maintenance_nonexecution(checks)
        .unwrap()
        .check_maintenance_policies(&roster, 1)
        .unwrap();
        assert!(
            !observation
                .maintenance_policy_coverage()
                .unwrap()
                .is_complete()
        );
    }
    assert_eq!(provider.reads.load(Ordering::Acquire), 0);
    fixture.journal.close().await.unwrap();
}
#[tokio::test]
async fn maintenance_nonexecution_refuses_foreign_original_request_even_with_two_equal_provider_reads()
 {
    let fixture = Fixture::new().await;
    let mut accepted = Vec::new();
    for id in [219, 220] {
        let FleetEnrollmentAcceptance::New(row) = fixture
            .journal
            .accept_enrollment(&enrollment(id, 1), 0)
            .await
            .unwrap()
        else {
            panic!("new required");
        };
        accepted.push(row);
    }
    freeze(&fixture).await;
    for row in &accepted {
        fixture
            .journal
            .refuse_unexecuted_enrollment(row.spec(), Digest::from_bytes([221; 32]), 0)
            .await
            .unwrap();
    }
    let (roster, original) = capture(&fixture).await;
    let request = FleetEnrollmentNonexecutionRequest::new(
        &original,
        &roster,
        accepted[0].spec().key().unwrap(),
    )
    .unwrap();
    let provider = Provider {
        reads: AtomicUsize::new(0),
        evidence: Some(
            FleetEnrollmentNonexecutionEvidence::new(&request, Digest::from_bytes([221; 32]))
                .unwrap(),
        ),
    };
    assert!(matches!(
        FleetMaintenanceNonexecution::collect(
            &fixture.journal,
            &original,
            &roster,
            &provider,
            deadline(),
            || Ok(1)
        )
        .await,
        Err(cellule_runtime::Error::Fenced)
    ));
    assert!(provider.reads.load(Ordering::Acquire) >= 2);
    fixture.journal.close().await.unwrap();
}
#[tokio::test]
async fn maintenance_nonexecution_empty_confirmation_does_not_erase_new_unknown_source_work() {
    let fixture = Fixture::new().await;
    freeze(&fixture).await;
    let mut request = enrollment(222, 1);
    request.source.as_mut().unwrap().intent_revision = 2;
    fixture
        .journal
        .accept_enrollment(&request, 0)
        .await
        .unwrap();
    let (roster, original) = capture(&fixture).await;
    assert_eq!(original.entries().count(), 0);
    let provider = Provider {
        reads: AtomicUsize::new(0),
        evidence: None,
    };
    let checks = FleetMaintenanceNonexecution::collect(
        &fixture.journal,
        &original,
        &roster,
        &provider,
        deadline(),
        || Ok(1),
    )
    .await
    .unwrap();
    let observation = FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        0,
        1,
        false,
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
    .with_maintenance_enrollments(original)
    .unwrap()
    .with_maintenance_nonexecution(checks)
    .unwrap()
    .check_maintenance_policies(&roster, 1)
    .unwrap();
    let coverage = observation.maintenance_policy_coverage().unwrap();
    assert_eq!(coverage.progress().required, 1);
    assert_eq!(coverage.progress().pending, 1);
    assert!(!coverage.is_complete());
    fixture.journal.close().await.unwrap();
}
