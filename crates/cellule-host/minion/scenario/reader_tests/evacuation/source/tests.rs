use super::*;
use cellule_host::fleet::FleetMaintenancePolicyStatus;

async fn ads(fixture: &Fixture) -> Vec<cellule_runtime::node::NodeAdvertisement> {
    let mut nodes = Vec::new();
    for index in 0..3 {
        nodes.push(
            fixture
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
async fn outer(
    source: &SourceFixture,
    registry: cellule_runtime::fleet::operations::RegistryVersion,
    started: i64,
) -> FleetObservation {
    FleetObservation::new(
        scope(),
        registry,
        registry.revision(),
        started,
        clock().unwrap(),
        false,
        ads(&source.fixture).await,
        Vec::new(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_binds_actual_final_root_writer_and_new_request_in_both_orders() {
    let source = SourceFixture::new().await;
    let provider = Provider::stable(source.inputs.clone());
    for source_first in [false, true] {
        let started = clock().unwrap();
        let (roster, original) = source.basis().await;
        let policies = source
            .verifier()
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                clock,
            )
            .await
            .unwrap();
        assert_eq!(policies.checks().len(), 1);
        let check = &policies.checks()[0];
        assert_eq!(check.retirement().original(), &source.fixture.original);
        assert_eq!(check.origin().prefix(), source.inputs.retirement.root());
        assert_eq!(check.origin().root(), source.inputs.retirement.root());
        assert_eq!(check.serving().owner().session, session(2));
        let EnrollmentRole::Reader { position, .. } = &source.fixture.original.spec().role else {
            panic!("not reader")
        };
        assert!(check.serving().position().epoch > position.epoch);
        assert_eq!(check.desired_readers(), 1);
        assert_eq!(check.policy_revision(), Some(1));
        assert_eq!(check.replacements().len(), 1);
        assert_eq!(check.replacements()[0].node, node_id(1));
        assert_ne!(
            check.replacements()[0].enrollment_key,
            source.fixture.original.spec().key().unwrap()
        );
        let observation = outer(&source, roster.snapshot().registry(), started).await;
        let observation = if source_first {
            observation
                .with_source_reader_policies(policies)
                .unwrap()
                .with_maintenance_enrollments(original)
                .unwrap()
        } else {
            observation
                .with_maintenance_enrollments(original)
                .unwrap()
                .with_source_reader_policies(policies)
                .unwrap()
        }
        .check_maintenance_policies(&roster, clock().unwrap())
        .unwrap();
        let coverage = observation.maintenance_policy_coverage().unwrap();
        assert!(coverage.is_complete());
        assert_eq!(coverage.progress().required, 1);
        assert_eq!(coverage.progress().checked, 1);
        assert_eq!(coverage.progress().source_successors, 0);
        assert!(matches!(
            coverage.obligations()[0].status(),
            FleetMaintenancePolicyStatus::SourceReader(_)
        ));
        assert_eq!(
            coverage.obligations()[0].original(),
            Some(&source.fixture.original)
        );
        assert_eq!(
            coverage.obligations()[0].current(),
            source.inputs.retirement.retired()
        );
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    drop(provider);
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_missing_native_capsule_keeps_complete_original_obligation_unknown() {
    let source = SourceFixture::new().await;
    let provider = Provider {
        inputs: None,
        later: None,
        calls: AtomicUsize::new(0),
    };
    let started = clock().unwrap();
    let (roster, original) = source.basis().await;
    let policies = source
        .verifier()
        .collect_source_readers(
            source.fixture.journal.as_ref(),
            &original,
            &roster,
            &provider,
            deadline(),
            clock,
        )
        .await
        .unwrap();
    assert!(policies.checks().is_empty());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    let observation = outer(&source, roster.snapshot().registry(), started)
        .await
        .with_source_reader_policies(policies)
        .unwrap()
        .with_maintenance_enrollments(original)
        .unwrap()
        .check_maintenance_policies(&roster, clock().unwrap())
        .unwrap();
    let progress = observation
        .maintenance_policy_coverage()
        .unwrap()
        .progress();
    assert_eq!(
        (
            progress.required,
            progress.checked,
            progress.source_successors
        ),
        (1, 0, 1)
    );
    drop(observation);
    source.finish().await;
}

fn mapping(source: &SourceFixture, node: usize) -> Arc<FleetSourceReaderInputs> {
    Arc::new(FleetSourceReaderInputs {
        retirement: source.inputs.retirement.clone(),
        node: node_id(node),
        host: source.inputs.host.clone(),
        catalog: source.inputs.catalog.clone(),
        authority: source.inputs.authority.clone(),
        replica: source.inputs.replica.clone(),
    })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_refuses_changed_presence_allocation_and_physical_mapping() {
    let source = SourceFixture::new().await;
    for (first, later) in [
        (Some(source.inputs.clone()), None),
        (None, Some(source.inputs.clone())),
        (Some(source.inputs.clone()), Some(mapping(&source, 2))),
    ] {
        let provider = Provider {
            inputs: first,
            later: Some(later),
            calls: AtomicUsize::new(0),
        };
        assert!(matches!(
            source.collect(&provider).await,
            Err(Error::Fenced)
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }
    for node in [0, 1] {
        let provider = Provider::stable(mapping(&source, node));
        assert!(matches!(
            source.collect(&provider).await,
            Err(Error::Fenced)
        ));
    }
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_refuses_duplicate_late_and_restamped_original_attachment() {
    let source = SourceFixture::new().await;
    let provider = Provider::stable(source.inputs.clone());
    for late in [false, true] {
        let started = clock().unwrap();
        let (roster, original) = source.basis().await;
        let verifier = source.verifier();
        let check = verifier
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                clock,
            )
            .await
            .unwrap();
        let other = verifier
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                clock,
            )
            .await
            .unwrap();
        let observation = outer(&source, roster.snapshot().registry(), started)
            .await
            .with_maintenance_enrollments(original)
            .unwrap();
        let observation = if late {
            observation
                .check_maintenance_policies(&roster, clock().unwrap())
                .unwrap()
        } else {
            observation.with_source_reader_policies(check).unwrap()
        };
        assert!(observation.with_source_reader_policies(other).is_err());
    }
    let (roster, original) = source.basis().await;
    let policies = source
        .verifier()
        .collect_source_readers(
            source.fixture.journal.as_ref(),
            &original,
            &roster,
            &provider,
            deadline(),
            clock,
        )
        .await
        .unwrap();
    let after = clock().unwrap().max(policies.interval().1 + 1);
    let observation = FleetObservation::new(
        scope(),
        roster.snapshot().registry(),
        roster.snapshot().registry().revision(),
        after,
        after,
        false,
        ads(&source.fixture).await,
        Vec::new(),
    )
    .unwrap();
    assert!(observation.with_source_reader_policies(policies).is_err());
    drop(provider);
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_policy_outer_boots_and_native_writer_must_match_checked_evidence() {
    let source = SourceFixture::new().await;
    let provider = Provider::stable(source.inputs.clone());
    for change in 0..3 {
        let started = clock().unwrap();
        let (roster, original) = source.basis().await;
        let policies = source
            .verifier()
            .collect_source_readers(
                source.fixture.journal.as_ref(),
                &original,
                &roster,
                &provider,
                deadline(),
                clock,
            )
            .await
            .unwrap();
        let mut nodes = ads(&source.fixture).await;
        let mut cells = Vec::new();
        match change {
            0 => nodes.retain(|node| node.node() != node_id(1)),
            1 => nodes.retain(|node| node.node() != node_id(2)),
            _ => {
                let mut observation = policies.checks()[0].serving().native().clone();
                observation.generation += 1;
                cells.push(cellule_host::fleet::FleetOwnedCell {
                    node: node_id(2),
                    session: session(2),
                    observation,
                });
            }
        }
        let outer = FleetObservation::new(
            scope(),
            roster.snapshot().registry(),
            roster.snapshot().registry().revision(),
            started,
            clock().unwrap(),
            false,
            nodes,
            cells,
        )
        .unwrap()
        .with_maintenance_enrollments(original)
        .unwrap();
        assert!(outer.with_source_reader_policies(policies).is_err());
    }
    drop(provider);
    source.finish().await;
}
