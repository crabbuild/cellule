use super::*;
use cellule_runtime::fleet::operations::{
    EnrollmentEndpoint, EnrollmentPage, EnrollmentSpec, FleetHead, FleetScope, IntentPage,
    RegistryVersion,
};
use cellule_runtime::identity::ApplicationId;
use cellule_runtime::node::NodeMode;

fn scope() -> FleetScope {
    FleetScope {
        fleet: Digest::from_bytes([1; 32]),
        application: ApplicationId::from_bytes([2; 16]),
    }
}
fn intent(n: u8) -> NodeIntent {
    NodeIntent::initial(
        scope(),
        NodeId::from_bytes([n; 16]),
        SessionId::from_bytes([n; 16]),
    )
    .unwrap()
}
fn endpoint(n: u8) -> EnrollmentEndpoint {
    EnrollmentEndpoint {
        node: intent(n).node(),
        session: intent(n).session(),
        intent_revision: 1,
    }
}
fn version() -> RegistryVersion {
    RegistryVersion::new(scope()).unwrap().advance(0).unwrap()
}
fn record(n: u8) -> EnrollmentRecord {
    EnrollmentRecord::pending(
        EnrollmentSpec {
            scope: scope(),
            request: Digest::from_bytes([n; 32]),
            role: EnrollmentRole::Follower { log_epoch: 7 },
            source: Some(endpoint(1)),
            target: endpoint(2),
        },
        Some(&intent(1)),
        None,
        &intent(2),
        1,
    )
    .unwrap()
}

#[test]
fn scans_require_original_version_and_exact_continuation() {
    let version = version();
    let mut scan = Scan::new(version);
    let first = IntentPage::new(version, None, vec![intent(1)], Some(intent(1).node())).unwrap();
    let after = scan.intents(first, None).unwrap();
    assert!(
        scan.intents(
            IntentPage::new(version, None, vec![intent(2)], None).unwrap(),
            None
        )
        .is_err()
    );
    assert!(
        scan.intents(
            IntentPage::new(version.advance(1).unwrap(), after, vec![intent(2)], None).unwrap(),
            after
        )
        .is_err()
    );
    assert_eq!(scan.intents.len(), 1);
    assert_eq!(
        scan.intents(
            IntentPage::new(version, after, vec![intent(2)], None).unwrap(),
            after
        )
        .unwrap(),
        None
    );
    assert_eq!(scan.intents, vec![intent(1), intent(2)]);
}

#[test]
fn enrollment_scan_rejects_reset_cursor_and_revision_change_without_appending() {
    let mut rows = [record(3), record(4)];
    rows.sort_by_key(|record| *record.spec().key().unwrap().as_bytes());
    let mut scan = Scan::new(version());
    let key = rows[0].spec().key().unwrap();
    let after = scan
        .enrollments(
            EnrollmentPage::new(version(), None, vec![rows[0].clone()], Some(key)).unwrap(),
            None,
        )
        .unwrap();
    assert!(
        scan.enrollments(
            EnrollmentPage::new(version(), None, vec![rows[1].clone()], None).unwrap(),
            None
        )
        .is_err()
    );
    assert!(
        scan.enrollments(
            EnrollmentPage::new(
                version().advance(1).unwrap(),
                after,
                vec![rows[1].clone()],
                None
            )
            .unwrap(),
            after
        )
        .is_err()
    );
    assert_eq!(scan.enrollments.len(), 1);
    scan.enrollments(
        EnrollmentPage::new(version(), after, vec![rows[1].clone()], None).unwrap(),
        after,
    )
    .unwrap();
    assert_eq!(scan.enrollments, rows);
}

#[test]
fn aggregate_limits_fail_before_appending_a_page() {
    let mut scan = Scan::new(version());
    scan.intents = vec![intent(1); MAX_ROSTER_ENTRIES];
    let after = Some(intent(1).node());
    assert!(matches!(
        scan.intents(
            IntentPage::new(version(), after, vec![intent(2)], None).unwrap(),
            after
        ),
        Err(Error::Capacity(_))
    ));
    assert_eq!(scan.intents.len(), MAX_ROSTER_ENTRIES);
    let mut rows = [record(3), record(4)];
    rows.sort_by_key(|record| *record.spec().key().unwrap().as_bytes());
    scan.enrollments = vec![rows[0].clone(); MAX_ROSTER_ENTRIES];
    let after = Some(rows[0].spec().key().unwrap());
    assert!(matches!(
        scan.enrollments(
            EnrollmentPage::new(version(), after, vec![rows[1].clone()], None).unwrap(),
            after
        ),
        Err(Error::Capacity(_))
    ));
    assert_eq!(scan.enrollments.len(), MAX_ROSTER_ENTRIES);
}

#[test]
fn original_failed_boot_remains_required_after_intent_replacement() {
    let old = record(3);
    let replaced = intent(1)
        .rebind_active(SessionId::from_bytes([9; 16]), 2)
        .unwrap();
    let mut roster = FleetRoster {
        snapshot: FleetJournalSnapshot::new(FleetHead::new(scope(), 0).unwrap(), version())
            .unwrap(),
        intents: vec![replaced, intent(2)],
        enrollments: vec![old.clone()],
        _memory: Vec::new(),
    };
    assert_eq!(
        roster.required_boots(),
        vec![
            FleetRosterBoot {
                node: endpoint(1).node,
                session: endpoint(1).session
            },
            FleetRosterBoot {
                node: endpoint(2).node,
                session: endpoint(2).session
            }
        ]
    );
    let pending_digest = roster.digest().unwrap();
    roster.enrollments[0] = old.refuse(Digest::from_bytes([8; 32]), 2).unwrap();
    assert!(roster.required_boots().is_empty());
    assert_ne!(roster.digest().unwrap(), pending_digest);
    assert_eq!(roster.enrollments().len(), 1);
    assert!(!roster.covers_advertisements(&[], 100).unwrap());
    // Even a known initial boot must not become a coverage claim before bootstrap.
    let boot = EnrollmentSpec {
        scope: scope(),
        request: Digest::from_bytes([10; 32]),
        role: EnrollmentRole::Node {
            mode: NodeMode::Active,
        },
        source: None,
        target: endpoint(2),
    };
    roster
        .enrollments
        .push(EnrollmentRecord::pending(boot, None, None, &intent(2), 1).unwrap());
    assert!(!roster.covers_advertisements(&[], 100).unwrap());
}

#[tokio::test]
async fn expired_deadline_does_not_construct_or_dispatch_a_journal_request() {
    let mut called = false;
    let result = call::<(), _>(Instant::now(), || {
        called = true;
        Box::pin(async { Ok(()) })
    })
    .await;
    assert!(result.is_err());
    assert!(!called);
}

#[tokio::test]
async fn journal_source_errors_remain_available_without_reclassification() {
    let result = call::<(), _>(Instant::now() + std::time::Duration::from_secs(1), || {
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "original journal failure",
            )
            .into())
        })
    })
    .await;
    let Err(Error::Facility { name, source }) = result else {
        panic!("missing source error")
    };
    assert_eq!(name, "fleet-roster-journal");
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(source.to_string(), "original journal failure");
}

fn signed(n: u8) -> NodeAdvertisement {
    use cellule_runtime::node::{
        NodeCapacity, NodeFailureDomain, NodeOperationalSample, NodePlacementCapacity, NodePressure,
    };
    let key = ed25519_dalek::SigningKey::from_bytes(&[n; 32]);
    NodeAdvertisement::sign(
        endpoint(n).node,
        endpoint(n).session,
        format!("https://node-{n}.internal:8789"),
        scope().fleet,
        Digest::from_bytes([5; 32]),
        Digest::from_bytes([6; 32]),
        Digest::from_bytes([7; 32]),
        &key,
        1,
        100,
        30_100,
        vec![Digest::from_bytes([8; 32])],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            free_memory_bytes: 1000,
            free_disk_bytes: 1000,
            job_credits: 2,
            log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
            ..NodeCapacity::default()
        },
    )
    .unwrap()
    .with_operational_placement(
        NodePlacementCapacity {
            memory_capacity_bytes: 1000,
            disk_capacity_bytes: 1000,
            max_active_cells: 2,
            job_capacity: 2,
            ..NodePlacementCapacity::default()
        },
        NodeOperationalSample {
            sequence: 1,
            observed_at_ms: 100,
            mode: NodeMode::Active,
            pressure: NodePressure::Normal,
        },
        &key,
    )
    .unwrap()
}
fn boot(n: u8) -> EnrollmentRecord {
    EnrollmentRecord::pending(
        EnrollmentSpec {
            scope: scope(),
            request: Digest::from_bytes([n + 10; 32]),
            role: EnrollmentRole::Node {
                mode: NodeMode::Active,
            },
            source: None,
            target: endpoint(n),
        },
        None,
        None,
        &intent(n),
        1,
    )
    .unwrap()
}

#[test]
fn boot_coverage_requires_exact_established_roster_and_fresh_signed_samples() {
    let record = boot(1).establish(Digest::from_bytes([22; 32]), 2).unwrap();
    let mut roster = FleetRoster {
        snapshot: FleetJournalSnapshot::new(
            FleetHead::new(scope(), 0).unwrap(),
            version().bootstrap(1).unwrap(),
        )
        .unwrap(),
        intents: vec![intent(1), intent(2)],
        enrollments: vec![record.clone()],
        _memory: Vec::new(),
    };
    assert!(roster.covers_advertisements(&[signed(1)], 100).unwrap());
    assert!(!roster.covers_advertisements(&[], 100).unwrap());
    assert!(
        !roster
            .covers_advertisements(&[signed(1), signed(1)], 100)
            .unwrap()
    );
    assert!(
        !roster
            .covers_advertisements(&[signed(1), signed(2)], 100)
            .unwrap()
    );
    assert!(roster.covers_advertisements(&[signed(1)], 30_101).is_err());
    roster.enrollments[0] = boot(1);
    assert!(!roster.covers_advertisements(&[signed(1)], 100).unwrap());
    roster.enrollments[0] = record.retire(Digest::from_bytes([23; 32]), 3).unwrap();
    assert!(!roster.covers_advertisements(&[signed(1)], 100).unwrap());
    roster.enrollments[0] = record.clone();
    roster.enrollments.push(record);
    assert!(!roster.covers_advertisements(&[signed(1)], 100).unwrap());
}

#[test]
fn unresolved_role_requires_both_original_boots_even_without_a_boot_row() {
    let mut roster = FleetRoster {
        snapshot: FleetJournalSnapshot::new(
            FleetHead::new(scope(), 0).unwrap(),
            version().bootstrap(1).unwrap(),
        )
        .unwrap(),
        intents: vec![intent(1), intent(2)],
        enrollments: vec![
            boot(1).establish(Digest::from_bytes([22; 32]), 2).unwrap(),
            record(3),
        ],
        _memory: Vec::new(),
    };
    assert!(!roster.covers_advertisements(&[signed(1)], 100).unwrap());
    roster
        .enrollments
        .push(boot(2).establish(Digest::from_bytes([23; 32]), 2).unwrap());
    assert!(
        roster
            .covers_advertisements(&[signed(1), signed(2)], 100)
            .unwrap()
    );
    roster.intents[0] = intent(1)
        .rebind_active(SessionId::from_bytes([9; 16]), 2)
        .unwrap();
    assert!(
        !roster
            .covers_advertisements(&[signed(1), signed(2)], 100)
            .unwrap()
    );
    assert_eq!(roster.required_boots()[0].session, endpoint(1).session);
}

#[test]
fn selected_boot_requires_one_current_established_original_session() {
    let established = boot(1).establish(Digest::from_bytes([22; 32]), 2).unwrap();
    let mut roster = FleetRoster {
        snapshot: FleetJournalSnapshot::new(FleetHead::new(scope(), 0).unwrap(), version())
            .unwrap(),
        intents: vec![intent(1)],
        enrollments: vec![established.clone()],
        _memory: Vec::new(),
    };
    let node = endpoint(1).node;
    let session = endpoint(1).session;
    assert_eq!(
        roster.boot(node, session).unwrap().enrollment(),
        &established
    );
    assert!(roster.boot(node, SessionId::from_bytes([9; 16])).is_err());
    roster.enrollments.push(established.clone());
    assert!(roster.boot(node, session).is_err());
    roster.enrollments = vec![boot(1)];
    assert!(roster.boot(node, session).is_err());
    roster.enrollments = vec![established.retire(Digest::from_bytes([23; 32]), 3).unwrap()];
    assert!(roster.boot(node, session).is_err());
    roster.enrollments = vec![established];
    roster.intents[0] = intent(1)
        .rebind_active(SessionId::from_bytes([9; 16]), 2)
        .unwrap();
    assert!(roster.boot(node, session).is_err());
}

#[tokio::test]
async fn native_roster_rows_hold_shared_credit_until_the_original_scan_drops() {
    let runtime = CellRuntime::new(
        cellule_runtime::cell::worker::SqlWorkerPool::new(1, 2).unwrap(),
        8 << 20,
        SessionId::from_bytes([90; 16]),
    )
    .unwrap();
    let baseline = runtime.stats().retained_bytes();
    let mut scan = Scan::new(version());
    scan.runtime = Some(&runtime);
    scan.intents(
        IntentPage::new(version(), None, vec![intent(1)], None).unwrap(),
        None,
    )
    .unwrap();
    let after_intent = runtime.stats().retained_bytes();
    assert!(after_intent > baseline);
    scan.enrollments(
        EnrollmentPage::new(version(), None, vec![record(3)], None).unwrap(),
        None,
    )
    .unwrap();
    assert!(runtime.stats().retained_bytes() > after_intent);
    let page = reserve_page(Some(&runtime)).unwrap();
    assert!(runtime.stats().retained_bytes() >= baseline + 4 * MAX_PAGE_BYTES as usize);
    drop(page);
    drop(scan);
    assert_eq!(runtime.stats().retained_bytes(), baseline);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_roster_capacity_refusal_does_not_append_or_leak_retained_rows() {
    let runtime = CellRuntime::new(
        cellule_runtime::cell::worker::SqlWorkerPool::new(1, 2).unwrap(),
        1 << 20,
        SessionId::from_bytes([91; 16]),
    )
    .unwrap();
    assert!(matches!(
        reserve_page(Some(&runtime)),
        Err(Error::Capacity(_))
    ));
    let baseline = runtime.stats().retained_bytes();
    let held = runtime
        .try_reserve_node_metadata_bytes(runtime.stats().retained_capacity_bytes() - baseline)
        .unwrap();
    let mut scan = Scan::new(version());
    scan.runtime = Some(&runtime);
    assert!(matches!(
        scan.intents(
            IntentPage::new(version(), None, vec![intent(1)], None).unwrap(),
            None
        ),
        Err(Error::Capacity(_))
    ));
    assert!(scan.intents.is_empty() && scan.memory.is_empty());
    drop(held);
    assert_eq!(runtime.stats().retained_bytes(), baseline);
    scan.intents(
        IntentPage::new(version(), None, vec![intent(1)], None).unwrap(),
        None,
    )
    .unwrap();
    drop(scan);
    assert_eq!(runtime.stats().retained_bytes(), baseline);
    runtime.shutdown().await.unwrap();
}
