//! Current canonical/native policy and full role graph in one public observation.
use super::super::{aggregate, coverage};
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetAdapterFuture, FleetFollowerEvacuationCheck,
    FleetMaintenanceEnrollments, FleetObservation, FleetObserver, FleetReconciler,
    FleetRoleCoverage, FleetRoster, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};

pub(super) async fn advertisements(fixture: &ManagedFixture) -> Vec<NodeAdvertisement> {
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
async fn graph(fixture: &ManagedFixture) -> FleetRoleCoverage {
    let roster = aggregate::roster(fixture).await;
    let (mut native, mut foreign, mut sequence) = coverage::captures(fixture, &roster).await;
    coverage::native_rechecks(fixture, &roster, &mut native, &mut sequence).await;
    coverage::foreign_rechecks(fixture, &roster, &mut foreign).await;
    roster
        .confirm(fixture.native.journal.as_ref(), deadline())
        .await
        .unwrap();
    coverage::check(&roster, &native, &foreign).unwrap()
}
fn observation(
    check: &FleetFollowerEvacuationCheck,
    graph: &FleetRoleCoverage,
    nodes: Vec<NodeAdvertisement>,
    start: i64,
) -> FleetObservation {
    FleetObservation::new(
        scope(),
        graph.snapshot().registry(),
        graph.snapshot().registry().revision(),
        check.interval().0.min(graph.interval().0).min(start),
        clock().unwrap(),
        false,
        nodes,
        Vec::new(),
    )
    .unwrap()
}

struct Observer {
    fixture: Arc<ManagedFixture>,
    record: FollowerEvacuationRecord,
    observations: AtomicUsize,
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
            self.observations.fetch_add(1, Ordering::AcqRel);
            let start = clock()?;
            let original = FleetMaintenanceEnrollments::collect(
                self.fixture.native.journal.as_ref(),
                roster,
                end,
                clock,
            )
            .await?;
            let checks = verifier(&self.fixture)
                .collect_maintenance(
                    self.fixture.native.journal.as_ref(),
                    &original,
                    roster,
                    end,
                    clock,
                )
                .await?;
            assert_eq!(checks.len(), 1);
            assert_eq!(checks[0].record(), &self.record);
            let graph = graph(&self.fixture).await;
            let observation = observation(
                &checks[0],
                &graph,
                advertisements(&self.fixture).await,
                start,
            )
            .with_role_coverage(graph)?
            .with_role_evacuations(Vec::new(), checks)?
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
        Box::pin(async { Err(invalid("unexpected follower-policy effect")) })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        self.effects.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Err(invalid("unexpected follower-policy inspection")) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_evacuation_observation_retains_native_policy_and_role_graph_in_both_orders() {
    let (fixture, capture, policy) = setup().await;
    let fixture = Arc::new(fixture);
    let publication = publish(&fixture, &capture, policy).await;
    let record = publication.record().unwrap().clone();
    for policy_first in [false, true] {
        let check = verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), &record, deadline(), clock)
            .await
            .unwrap();
        let graph = graph(&fixture).await;
        assert_eq!(graph.native_boots(), 4);
        assert_eq!(graph.physical_nodes(), 4);
        let interval = check.interval();
        let roster = aggregate::roster(&fixture).await;
        let original = FleetMaintenanceEnrollments::collect(
            fixture.native.journal.as_ref(),
            &roster,
            deadline(),
            clock,
        )
        .await
        .unwrap();
        let base = observation(
            &check,
            &graph,
            advertisements(&fixture).await,
            check.interval().0,
        );
        let retained = if policy_first {
            base.with_role_evacuations(Vec::new(), vec![check])
                .unwrap()
                .with_role_coverage(graph)
                .unwrap()
        } else {
            base.with_role_coverage(graph)
                .unwrap()
                .with_role_evacuations(Vec::new(), vec![check])
                .unwrap()
        };
        let retained = retained
            .with_maintenance_enrollments(original)
            .unwrap()
            .check_maintenance_policies(&roster, clock().unwrap())
            .unwrap();
        let policy = retained.maintenance_policy_coverage().unwrap();
        assert!(policy.is_complete());
        assert_eq!(policy.obligations().len(), 1);
        assert_eq!(
            policy.obligations()[0].status(),
            cellule_host::fleet::FleetMaintenancePolicyStatus::Follower(record.digest().unwrap())
        );
        let proof = &retained.follower_evacuations().unwrap()[0];
        assert_eq!(proof.record(), &record);
        assert_eq!(proof.record_digest(), record.digest().unwrap());
        assert_eq!(proof.interval(), interval);
        assert_eq!(proof.native().len(), 3);
        assert_eq!(proof.record().replacements().len(), 2);
        assert_eq!(
            proof.snapshot(),
            retained.role_coverage().unwrap().snapshot()
        );
        assert_eq!(
            proof.roster_digest(),
            retained.role_coverage().unwrap().roster_digest()
        );
        assert!(retained.reader_evacuations().unwrap().is_empty());
    }
    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    fixture
        .native
        .journal
        .set_scheduling(snapshot.registry(), true)
        .await
        .unwrap();
    let observer = Arc::new(Observer {
        fixture: fixture.clone(),
        record: record.clone(),
        observations: AtomicUsize::new(0),
        effects: AtomicUsize::new(0),
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
    drop((driver, observer, capture));
    Arc::try_unwrap(fixture).ok().unwrap().finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_observer_reconciles_follower_only_maintenance_to_completion() {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let record = publication.record().unwrap().clone();
    let fleet = Arc::new(crate::scenario::adapters::LocalFleet {
        nodes: fixture.nodes.clone(),
        journal: fixture.native.journal.clone(),
        boots: fixture.boots.clone(),
        records: fixture.records.clone(),
        reader_verifier: None,
        capture_sequence: std::sync::atomic::AtomicU64::new(0),
        lose_release_replies: false,
        lost_release_replies: AtomicUsize::new(0),
        drop_closed_finalize_replies: std::sync::atomic::AtomicUsize::new(0),
        expired_receiver_cleanups: AtomicUsize::new(0),
    });

    let snapshot = fixture.native.journal.load_snapshot(scope()).await.unwrap();
    let roster = FleetRoster::collect(fixture.native.journal.as_ref(), &snapshot, deadline())
        .await
        .unwrap();
    let observation = fleet
        .observe(&roster, clock().unwrap(), deadline())
        .await
        .unwrap();
    let graph = observation.role_coverage().unwrap();
    assert_eq!(graph.native_boots(), 4);
    assert_eq!(graph.physical_nodes(), 4);
    let policies = observation.maintenance_policy_coverage().unwrap();
    assert!(policies.is_complete());
    assert_eq!(policies.progress().required, 1);
    assert_eq!(policies.progress().checked, 1);
    assert_eq!(
        policies.obligations()[0].status(),
        cellule_host::fleet::FleetMaintenancePolicyStatus::Follower(record.digest().unwrap())
    );

    fixture
        .native
        .journal
        .set_scheduling(roster.snapshot().registry(), true)
        .await
        .unwrap();
    let driver = FleetReconciler::new(
        scope(),
        session(9),
        FleetProfile::default(),
        fixture.native.journal.clone(),
        fleet.clone(),
        fleet.clone(),
    )
    .unwrap();
    let mut report = None;
    let mut policy_progress = None;
    for _ in 0..3 {
        let next = driver.reconcile_once(clock, deadline()).await.unwrap();
        if let Some(progress) = next.maintenance_policy {
            policy_progress = Some(progress);
        }
        let completed = next.snapshot.head().maintenance().is_some_and(|operation| {
            operation.phase() == cellule_runtime::fleet::operations::MaintenancePhase::Completed
        });
        report = Some(next);
        if completed {
            break;
        }
    }
    let report = report.unwrap();
    assert_eq!(
        report.snapshot.head().maintenance().unwrap().phase(),
        cellule_runtime::fleet::operations::MaintenancePhase::Completed
    );
    let progress = policy_progress.unwrap();
    assert_eq!(progress.required, 1);
    assert_eq!(progress.checked, 1);
    assert_eq!(fixture.nodes[1].state(), NodeState::Stopped);
    assert!(
        fixture
            .native
            .directory
            .is_withdrawn(session(1))
            .await
            .unwrap()
    );
    let rows = fixture.native.rows().await;
    let donor = rows
        .iter()
        .find(|row| {
            row.spec().target.session == session(1)
                && matches!(
                    row.spec().role,
                    cellule_runtime::fleet::operations::EnrollmentRole::Follower { log_epoch: 1 }
                )
        })
        .unwrap();
    assert_eq!(donor.status(), EnrollmentStatus::Retired);
    let leader = fixture
        .native
        .directory
        .load(session(0), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    let current_log = leader.advertisement().log().unwrap();
    assert_eq!(current_log.epoch(), 2);
    assert_eq!(current_log.members(), [node_id(2), node_id(3)]);

    drop((driver, fleet, capture));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_evacuation_observation_refuses_duplicate_ensembles_and_stale_full_head_in_both_orders()
 {
    let (fixture, capture, policy) = setup().await;
    let publication = publish(&fixture, &capture, policy).await;
    let record = publication.record().unwrap();
    let first = verifier(&fixture)
        .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let second = verifier(&fixture)
        .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
        .await
        .unwrap();
    let current = graph(&fixture).await;
    let base = observation(
        &first,
        &current,
        advertisements(&fixture).await,
        first.interval().0,
    );
    assert!(matches!(
        base.with_role_evacuations(Vec::new(), vec![first, second]),
        Err(Error::Node(
            "fleet role evacuation obligation is duplicated"
        ))
    ));
    for policy_first in [false, true] {
        let check = verifier(&fixture)
            .recheck(fixture.native.journal.as_ref(), record, deadline(), clock)
            .await
            .unwrap();
        let old = check.snapshot().clone();
        let next = fixture
            .native
            .journal
            .claim_controller(scope(), old.head().revision(), session(9), clock().unwrap())
            .await
            .unwrap();
        assert_eq!(next.registry(), old.registry());
        assert_ne!(next.head(), old.head());
        let graph = graph(&fixture).await;
        assert_eq!(graph.snapshot(), &next);
        let base = observation(
            &check,
            &graph,
            advertisements(&fixture).await,
            check.interval().0,
        );
        let result = if policy_first {
            base.with_role_evacuations(Vec::new(), vec![check])
                .unwrap()
                .with_role_coverage(graph)
        } else {
            base.with_role_coverage(graph)
                .unwrap()
                .with_role_evacuations(Vec::new(), vec![check])
        };
        assert!(matches!(
            result,
            Err(Error::Node("fleet role evacuation barrier differs"))
        ));
    }
    drop(capture);
    fixture.finish().await;
}
