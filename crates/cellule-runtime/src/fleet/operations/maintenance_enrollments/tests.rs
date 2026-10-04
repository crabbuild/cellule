use super::*;
use crate::identity::{ApplicationId, NodeId, SessionId};

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
        SessionId::from_bytes([n + 10; 16]),
    )
    .unwrap()
}
fn endpoint(intent: &NodeIntent) -> EnrollmentEndpoint {
    EnrollmentEndpoint {
        node: intent.node(),
        session: intent.session(),
        intent_revision: intent.revision(),
    }
}
fn row(n: usize) -> EnrollmentRecord {
    EnrollmentRecord::pending(
        EnrollmentSpec {
            scope: scope(),
            request: Digest::from_bytes(*blake3::hash(&n.to_be_bytes()).as_bytes()),
            role: EnrollmentRole::Follower { log_epoch: 7 },
            source: Some(endpoint(&intent(1))),
            target: endpoint(&intent(2)),
        },
        Some(&intent(1)),
        None,
        &intent(2),
        10,
    )
    .unwrap()
}
fn operation() -> MaintenanceOperation {
    let mut op = MaintenanceOperation::new(
        OperationId::from_bytes([3; 16]).unwrap(),
        Digest::from_bytes([4; 32]),
        intent(2).node(),
        intent(2).session(),
        2,
        0,
        60_000,
    )
    .unwrap();
    op.apply(MaintenanceEvent::Cordoned, 0).unwrap();
    op
}
fn record(
    count: usize,
) -> (
    MaintenanceEnrollmentInventory,
    Vec<MaintenanceEnrollmentPage>,
) {
    MaintenanceEnrollmentInventory::new(
        operation(),
        (
            Digest::from_bytes([5; 32]),
            RegistryVersion::new(scope()).unwrap().bootstrap(0).unwrap(),
        ),
        30,
        (0..count).map(row).collect(),
    )
    .unwrap()
}

#[test]
fn original_pages_round_trip_without_losing_pending_or_established_history() {
    let (record, pages) = record(259);
    assert_eq!(
        pages.iter().map(|p| p.entries().len()).collect::<Vec<_>>(),
        [128, 128, 3]
    );
    assert_eq!(
        MaintenanceEnrollmentInventory::from_bytes(&record.to_bytes().unwrap()).unwrap(),
        record
    );
    for page in &pages {
        assert_eq!(
            MaintenanceEnrollmentPage::from_bytes(&page.to_bytes().unwrap()).unwrap(),
            *page
        );
    }
    record.validate_pages(&pages).unwrap();
    let mut bytes = record.to_bytes().unwrap();
    bytes.push(0);
    assert!(MaintenanceEnrollmentInventory::from_bytes(&bytes).is_err());
    let established = row(0).establish(Digest::from_bytes([6; 32]), 20).unwrap();
    let (record, pages) = MaintenanceEnrollmentInventory::new(
        operation(),
        (
            Digest::from_bytes([5; 32]),
            RegistryVersion::new(scope()).unwrap().bootstrap(0).unwrap(),
        ),
        30,
        vec![established.clone(), row(1)],
    )
    .unwrap();
    assert!(pages[0].entries().contains(&established));
    assert!(
        pages[0]
            .entries()
            .iter()
            .any(|row| row.status() == EnrollmentStatus::Pending)
    );
    record.validate_pages(&pages).unwrap();
}

#[test]
fn complete_original_set_refuses_missing_reordered_and_duplicate_boundary_rows() {
    let (record, pages) = record(259);
    assert!(record.validate_pages(&pages[..2]).is_err());
    let mut reordered = pages.clone();
    reordered.swap(0, 1);
    assert!(record.validate_pages(&reordered).is_err());
    for change in 0..4 {
        let mut modified = pages.clone();
        match change {
            0 => modified[1].basis = Digest::from_bytes([90; 32]),
            1 => modified[1].ordinal = 0,
            2 => {
                modified[1].entries.remove(0);
            }
            _ => modified[1].entries[0] = modified[0].entries.last().unwrap().clone(),
        }
        assert!(record.validate_pages(&modified).is_err());
        if change == 3 {
            let mut manifest = record.clone();
            manifest.pages[1] = modified[1].digest().unwrap();
            assert!(manifest.validate_pages(&modified).is_err());
        }
    }
    assert!(
        MaintenanceEnrollmentInventory::new(
            operation(),
            (Digest::from_bytes([5; 32]), record.registry()),
            30,
            vec![row(1), row(1)]
        )
        .is_err()
    );
}

#[test]
fn original_selection_covers_both_physical_endpoints_and_earlier_boots() {
    let pending = row(1);
    assert!(MaintenanceEnrollmentInventory::includes(
        intent(1).node(),
        &pending
    ));
    assert!(MaintenanceEnrollmentInventory::includes(
        intent(2).node(),
        &pending
    ));
    assert!(!MaintenanceEnrollmentInventory::includes(
        intent(3).node(),
        &pending
    ));
    let old =
        NodeIntent::initial(scope(), intent(2).node(), SessionId::from_bytes([91; 16])).unwrap();
    let mut spec = pending.spec().clone();
    spec.target = endpoint(&old);
    let original = EnrollmentRecord::pending(spec, Some(&intent(1)), None, &old, 10).unwrap();
    let (record, pages) = MaintenanceEnrollmentInventory::new(
        operation(),
        (
            Digest::from_bytes([5; 32]),
            RegistryVersion::new(scope()).unwrap().bootstrap(0).unwrap(),
        ),
        30,
        vec![original.clone()],
    )
    .unwrap();
    assert_eq!(pages[0].entries(), &[original]);
    record.validate_pages(&pages).unwrap();
    let retired = pending.retire(Digest::from_bytes([7; 32]), 20).unwrap();
    assert!(!MaintenanceEnrollmentInventory::includes(
        intent(2).node(),
        &retired
    ));
    assert!(
        MaintenanceEnrollmentInventory::new(
            operation(),
            (Digest::from_bytes([5; 32]), record.registry()),
            30,
            vec![retired]
        )
        .is_err()
    );
}

#[test]
fn absent_history_cannot_be_encoded_as_valid_pages_or_unchecked_capture() {
    let (empty, pages) = record(0);
    assert!(pages.is_empty());
    empty.validate_pages(&[]).unwrap();
    assert_eq!(empty.enrollment_count(), 0);
    let mut requested = operation();
    requested.phase = MaintenancePhase::Requested;
    assert!(
        MaintenanceEnrollmentInventory::new(
            requested,
            (Digest::from_bytes([5; 32]), empty.registry()),
            30,
            vec![]
        )
        .is_err()
    );
    assert!(
        MaintenanceEnrollmentInventory::new(
            operation(),
            (
                Digest::from_bytes([5; 32]),
                RegistryVersion::new(scope()).unwrap()
            ),
            30,
            vec![]
        )
        .is_err()
    );
    assert!(
        MaintenanceEnrollmentInventory::new(
            operation(),
            (Digest::from_bytes([5; 32]), empty.registry()),
            9,
            vec![row(1)]
        )
        .is_err()
    );
    let mut extra = empty.clone();
    extra.pages.push(Digest::from_bytes([9; 32]));
    assert!(extra.to_bytes().is_err());
    assert!(
        MaintenanceEnrollmentInventory::new(
            operation(),
            (Digest::from_bytes([5; 32]), empty.registry()),
            30,
            vec![row(1); MAX_MAINTENANCE_ENROLLMENTS + 1]
        )
        .is_err()
    );
}
