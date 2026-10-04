//! Journal metadata stays blocking until actual native policy checks are attached.
use super::*;

async fn freeze(fixture: &Fixture) {
    for transition in [
        JournalTransition::BeginMaintenance(request(3, 3)),
        JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
        JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
    ] {
        fixture.transition(transition).await;
    }
}
async fn capture(journal: &SqliteJournal) -> (FleetRoster, FleetMaintenanceEnrollments) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let snapshot = journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(journal, &snapshot, deadline)
        .await
        .unwrap();
    let original = FleetMaintenanceEnrollments::collect(journal, &roster, deadline, || Ok(1))
        .await
        .unwrap();
    (roster, original)
}
fn observation(roster: &FleetRoster) -> FleetObservation {
    FleetObservation::new(
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
}

#[tokio::test]
async fn absent_original_history_cannot_supply_policy_coverage_and_committed_zero_is_explicit() {
    let fixture = Fixture::new().await;
    freeze(&fixture).await;
    let (roster, original) = capture(&fixture.journal).await;
    assert!(matches!(
        observation(&roster).check_maintenance_policies(&roster, 1),
        Err(cellule_runtime::Error::Control(
            "original maintenance enrollments are required for policy coverage"
        ))
    ));
    let checked = observation(&roster)
        .with_maintenance_enrollments(original)
        .unwrap()
        .check_maintenance_policies(&roster, 1)
        .unwrap();
    let coverage = checked.maintenance_policy_coverage().unwrap();
    assert!(coverage.is_complete());
    assert!(coverage.obligations().is_empty());
    assert_eq!(coverage.snapshot(), roster.snapshot());
    assert_eq!(coverage.roster_digest(), roster.digest().unwrap());
    assert!(matches!(
        checked.check_maintenance_policies(&roster, 1),
        Err(cellule_runtime::Error::Control(
            "maintenance policy coverage already retained"
        ))
    ));
    let (_, original) = capture(&fixture.journal).await;
    let checked = observation(&roster)
        .with_maintenance_enrollments(original)
        .unwrap()
        .check_maintenance_policies(&roster, 1)
        .unwrap();
    assert!(matches!(
        checked.with_role_evacuations(Vec::new(), Vec::new()),
        Err(cellule_runtime::Error::Node(
            "maintenance policy coverage inputs differ"
        ))
    ));
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn terminal_originals_cannot_disappear_from_missing_policy_and_source_successor_gaps() {
    let fixture = Fixture::new().await;
    let reader = EnrollmentSpec {
        scope: scope(),
        request: Digest::from_bytes([84; 32]),
        source: Some(endpoint(1)),
        target: endpoint(3),
        role: EnrollmentRole::Reader {
            target: spec(1).target,
            position: position(),
        },
    };
    let FleetEnrollmentAcceptance::New(reader) =
        fixture.journal.accept_enrollment(&reader, 0).await.unwrap()
    else {
        panic!("new expected")
    };
    let reader = fixture
        .journal
        .publish_enrollment_result(
            &reader,
            EnrollmentEvent::Established(Digest::from_bytes([85; 32])),
            0,
        )
        .await
        .unwrap();
    let FleetEnrollmentAcceptance::New(source) = fixture
        .journal
        .accept_enrollment(&enrollment(86, 1), 0)
        .await
        .unwrap()
    else {
        panic!("new expected")
    };
    let mut pending_spec = reader.spec().clone();
    pending_spec.request = Digest::from_bytes([91; 32]);
    let FleetEnrollmentAcceptance::New(pending) = fixture
        .journal
        .accept_enrollment(&pending_spec, 0)
        .await
        .unwrap()
    else {
        panic!("new expected")
    };
    freeze(&fixture).await;
    fixture
        .journal
        .publish_enrollment_result(
            &pending,
            EnrollmentEvent::Refused(Digest::from_bytes([92; 32])),
            0,
        )
        .await
        .unwrap();
    fixture
        .journal
        .publish_enrollment_result(
            &reader,
            EnrollmentEvent::Retired(Digest::from_bytes([87; 32])),
            0,
        )
        .await
        .unwrap();
    fixture
        .journal
        .publish_enrollment_result(
            &source,
            EnrollmentEvent::Refused(Digest::from_bytes([88; 32])),
            0,
        )
        .await
        .unwrap();
    let (roster, original) = capture(&fixture.journal).await;
    assert!(
        roster
            .enrollments()
            .iter()
            .all(|row| !row.unresolved() || matches!(row.spec().role, EnrollmentRole::Node { .. }))
    );
    let checked = observation(&roster)
        .with_maintenance_enrollments(original)
        .unwrap()
        .check_maintenance_policies(&roster, 1)
        .unwrap();
    let coverage = checked.maintenance_policy_coverage().unwrap();
    assert!(!coverage.is_complete());
    assert_eq!(coverage.obligations().len(), 3);
    for (old, status) in [
        (&reader, FleetMaintenancePolicyStatus::MissingPolicy),
        (&source, FleetMaintenancePolicyStatus::SourceSuccessor),
        (&pending, FleetMaintenancePolicyStatus::UnprovenNonexecution),
    ] {
        let obligation = coverage
            .obligations()
            .iter()
            .find(|row| row.current().spec() == old.spec())
            .unwrap();
        assert_eq!(obligation.original(), Some(old));
        assert_eq!(obligation.status(), status);
    }
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn new_pending_and_established_source_roles_are_required_after_empty_original_capture() {
    let fixture = Fixture::new().await;
    freeze(&fixture).await;
    let mut request = enrollment(89, 1);
    request.source.as_mut().unwrap().intent_revision = 2;
    let FleetEnrollmentAcceptance::New(pending) = fixture
        .journal
        .accept_enrollment(&request, 0)
        .await
        .unwrap()
    else {
        panic!("new expected")
    };
    for status in [
        FleetMaintenancePolicyStatus::Pending,
        FleetMaintenancePolicyStatus::Established,
    ] {
        if status == FleetMaintenancePolicyStatus::Established {
            fixture
                .journal
                .publish_enrollment_result(
                    &pending,
                    EnrollmentEvent::Established(Digest::from_bytes([90; 32])),
                    0,
                )
                .await
                .unwrap();
        }
        let (roster, original) = capture(&fixture.journal).await;
        assert_eq!(original.original().enrollment_count(), 0);
        let checked = observation(&roster)
            .with_maintenance_enrollments(original)
            .unwrap()
            .check_maintenance_policies(&roster, 1)
            .unwrap();
        let coverage = checked.maintenance_policy_coverage().unwrap();
        assert!(!coverage.is_complete());
        assert_eq!(coverage.obligations().len(), 1);
        assert!(coverage.obligations()[0].original().is_none());
        assert_eq!(coverage.obligations()[0].current().spec(), &request);
        assert_eq!(coverage.obligations()[0].status(), status);
    }
    fixture.journal.close().await.unwrap();
}

#[tokio::test]
async fn policy_matching_refuses_an_older_full_head_with_the_same_registry() {
    let fixture = Fixture::new().await;
    freeze(&fixture).await;
    let (before, original) = capture(&fixture.journal).await;
    fixture
        .transition(JournalTransition::Maintenance(MaintenanceEvent::Blocked(
            DrainBlocker::IncompleteObservation,
        )))
        .await;
    let (after, _) = capture(&fixture.journal).await;
    assert_eq!(before.snapshot().registry(), after.snapshot().registry());
    assert_ne!(before.snapshot().head(), after.snapshot().head());
    let observation = observation(&before)
        .with_maintenance_enrollments(original)
        .unwrap();
    assert!(matches!(
        observation.check_maintenance_policies(&after, 1),
        Err(cellule_runtime::Error::Fenced)
    ));
    fixture.journal.close().await.unwrap();
}
