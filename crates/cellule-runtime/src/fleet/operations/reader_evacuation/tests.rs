use super::*;
use crate::{
    control::{Owner, RootRef},
    identity::{ApplicationId, CellTarget, IncarnationId, NamespaceId, TenantId},
};

fn record(desired: u16) -> (ReaderEvacuationRecord, Vec<ReaderEvacuationPage>) {
    let scope = FleetScope {
        fleet: Digest::from_bytes([100; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    };
    let source = NodeIntent::initial(
        scope,
        NodeId::from_bytes([1; 16]),
        SessionId::from_bytes([11; 16]),
    )
    .unwrap();
    let donor = NodeIntent::initial(
        scope,
        NodeId::from_bytes([2; 16]),
        SessionId::from_bytes([12; 16]),
    )
    .unwrap();
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        scope.application,
        NamespaceId::from_bytes([9; 16]),
        &[1],
    )
    .unwrap();
    let incarnation = IncarnationId::from_bytes([6; 16]);
    let root = RootRef {
        digest: Digest::from_bytes([9; 32]),
        txid: 1,
        checksum: cellule_ltx::types::CHECKSUM_FLAG | 1,
        commit_sequence: 1,
    };
    let original = EnrollmentRecord::pending(
        EnrollmentSpec {
            scope,
            request: Digest::from_bytes([70; 32]),
            source: Some(EnrollmentEndpoint {
                node: source.node(),
                session: source.session(),
                intent_revision: 1,
            }),
            target: EnrollmentEndpoint {
                node: donor.node(),
                session: donor.session(),
                intent_revision: 1,
            },
            role: EnrollmentRole::Reader {
                target: target.clone(),
                position: PublishedPosition {
                    incarnation,
                    epoch: 1,
                    root: root.clone(),
                },
            },
        },
        Some(&source),
        None,
        &donor,
        10,
    )
    .unwrap()
    .establish(Digest::from_bytes([71; 32]), 20)
    .unwrap();
    let retired = original.retire(Digest::from_bytes([72; 32]), 30).unwrap();
    let mut authority = Control::initial(
        target.cell_id(),
        incarnation,
        Owner {
            session: source.session(),
            endpoint: "https://source.example".into(),
        },
        Digest::from_bytes([7; 32]),
        1,
    )
    .unwrap();
    authority.state = ControlState::Serving;
    authority.root = Some(root);
    let mut operation = MaintenanceOperation::new(
        OperationId::from_bytes([80; 16]).unwrap(),
        Digest::from_bytes([81; 32]),
        donor.node(),
        donor.session(),
        2,
        0,
        60_000,
    )
    .unwrap();
    operation.apply(MaintenanceEvent::Cordoned, 0).unwrap();
    operation
        .apply(MaintenanceEvent::BeginEvacuation, 0)
        .unwrap();
    let replacements = (0..u32::from(desired))
        .map(|number| {
            let mut node = [0; 16];
            node[..4].copy_from_slice(&(number + 100).to_be_bytes());
            let mut session = node;
            session[15] = 1;
            ReaderReplacementWitness {
                node: NodeId::from_bytes(node),
                session: SessionId::from_bytes(session),
                boot_identity: Digest::from_bytes([90; 32]),
                enrollment_key: Digest::from_bytes(*blake3::hash(&node).as_bytes()),
                enrollment_digest: Digest::from_bytes([92; 32]),
                commit_sequence: 1,
            }
        })
        .collect();
    ReaderEvacuationRecord::new(
        operation,
        (
            Digest::from_bytes([89; 32]),
            RegistryVersion::new(scope).unwrap().bootstrap(0).unwrap(),
        ),
        retired,
        Digest::from_bytes(*blake3::hash(&original.to_bytes().unwrap()).as_bytes()),
        authority,
        Some(1),
        desired,
        1,
        (25, 35),
        replacements,
    )
    .unwrap()
}

#[test]
fn reader_policy_manifest_roundtrips_the_complete_ten_thousand_reader_bound() {
    let (record, pages) = record(MAX_READERS);
    assert_eq!(pages.len(), 79);
    assert_eq!(pages.last().unwrap().entries().len(), 16);
    assert_eq!(
        ReaderEvacuationRecord::from_bytes(&record.to_bytes().unwrap()).unwrap(),
        record
    );
    for page in &pages {
        let bytes = page.to_bytes().unwrap();
        assert!(bytes.len() <= MAX_RECORD_BYTES as usize);
        assert_eq!(ReaderEvacuationPage::from_bytes(&bytes).unwrap(), *page);
    }
    record.validate_pages(&pages).unwrap();
    assert_eq!(record.retired().status(), EnrollmentStatus::Retired);
    assert_eq!(record.retired().accepted_at_ms(), 10);
    assert_eq!(
        record.retired().established_evidence(),
        Some(Digest::from_bytes([71; 32]))
    );
}

#[test]
fn reader_policy_manifest_requires_every_exact_page_and_original_capture_basis() {
    let (record, pages) = record(129);
    assert!(record.validate_pages(&pages[..1]).is_err());
    let mut changed = pages.clone();
    changed.swap(0, 1);
    assert!(record.validate_pages(&changed).is_err());
    let mut changed = pages.clone();
    changed[1].basis = Digest::from_bytes([93; 32]);
    assert!(record.validate_pages(&changed).is_err());
    let mut changed = pages;
    changed[1].ordinal = 2;
    assert!(record.validate_pages(&changed).is_err());
}

#[test]
fn reader_policy_manifest_rejects_duplicate_physical_nodes_sessions_and_regressed_prefixes() {
    let (record, pages) = record(129);
    for kind in 0..5 {
        let mut changed = pages.clone();
        let mut record = record.clone();
        match kind {
            0 => changed[1].entries[0].node = changed[0].entries[0].node,
            1 => changed[1].entries[0].session = changed[0].entries[0].session,
            2 => changed[1].entries[0].node = record.operation.node(),
            _ => changed[1].entries[0].commit_sequence = 0,
        }
        record.pages[1] = changed[1].digest().unwrap();
        assert!(record.validate_pages(&changed).is_err());
    }
}

#[test]
fn reader_policy_manifest_preserves_absent_and_explicit_zero_policy_separately() {
    let (mut record, pages) = record(0);
    assert!(pages.is_empty());
    let explicit = record.digest().unwrap();
    record.policy_revision = None;
    let absent = record.digest().unwrap();
    assert_ne!(absent, explicit);
    record.validate_pages(&[]).unwrap();
    record.desired_readers = 1;
    assert!(record.to_bytes().is_err());
}

#[test]
fn reader_policy_manifest_rejects_scope_lifetime_history_and_interval_mismatches() {
    let (record, _) = record(1);
    for kind in 0..12 {
        let mut changed = record.clone();
        match kind {
            0 => changed.operation.phase = MaintenancePhase::Completed,
            1 => changed.authority.state = ControlState::Idle,
            2 => changed.authority.incarnation = IncarnationId::from_bytes([99; 16]),
            3 => changed.policy_revision = Some(0),
            4 => changed.finished_at_ms = changed.started_at_ms + 30_001,
            5 => changed.finished_at_ms = changed.started_at_ms - 1,
            6 => changed.operation.node = NodeId::from_bytes([99; 16]),
            7 => changed.original_digest = Digest::from_bytes([0; 32]),
            8 => changed.minimum_sequence = 2,
            9 => changed.head_digest = Digest::from_bytes([0; 32]),
            10 => changed.registry = RegistryVersion::new(changed.retired.spec().scope).unwrap(),
            _ => {
                let mut scope = changed.retired.spec().scope;
                scope.fleet = Digest::from_bytes([99; 32]);
                changed.registry = RegistryVersion::new(scope).unwrap().bootstrap(0).unwrap();
            }
        }
        assert!(changed.to_bytes().is_err());
    }
}

#[test]
fn reader_policy_codecs_reject_truncation_trailing_wrong_kind_and_oversized_data() {
    let (record, pages) = record(1);
    let manifest = record.to_bytes().unwrap();
    let page = pages[0].to_bytes().unwrap();
    for end in 0..manifest.len() {
        assert!(ReaderEvacuationRecord::from_bytes(&manifest[..end]).is_err());
    }
    for end in 0..page.len() {
        assert!(ReaderEvacuationPage::from_bytes(&page[..end]).is_err());
    }
    let mut extra = manifest.clone();
    extra.push(0);
    assert!(ReaderEvacuationRecord::from_bytes(&extra).is_err());
    let mut extra = page.clone();
    extra.push(0);
    assert!(ReaderEvacuationPage::from_bytes(&extra).is_err());
    assert!(ReaderEvacuationRecord::from_bytes(&page).is_err());
    assert!(ReaderEvacuationPage::from_bytes(&manifest).is_err());
    assert!(ReaderEvacuationRecord::from_bytes(&vec![0; MAX_PAGE_BYTES as usize + 1]).is_err());
    assert!(ReaderEvacuationPage::from_bytes(&vec![0; MAX_RECORD_BYTES as usize + 1]).is_err());
}

#[test]
fn reader_policy_manifest_retains_original_retirement_after_operation_boot_adoption_and_closing() {
    let (original, pages) = record(1);
    let mut operation = original.operation().clone();
    operation
        .apply(
            MaintenanceEvent::SessionReplaced(SessionId::from_bytes([99; 16])),
            40,
        )
        .unwrap();
    operation.apply(MaintenanceEvent::Cordoned, 40).unwrap();
    operation
        .apply(MaintenanceEvent::BeginEvacuation, 40)
        .unwrap();
    let replacements = pages
        .iter()
        .flat_map(|page| page.entries().iter().cloned())
        .collect::<Vec<_>>();
    let (adopted, _) = ReaderEvacuationRecord::new(
        operation.clone(),
        (original.head_digest(), original.registry()),
        original.retired().clone(),
        original.original_digest(),
        original.authority().clone(),
        original.policy_revision(),
        1,
        1,
        (40, 50),
        replacements.clone(),
    )
    .unwrap();
    assert_eq!(adopted.retired(), original.retired());
    assert_ne!(
        adopted.operation().session(),
        adopted.retired().spec().target.session
    );
    operation
        .apply(
            MaintenanceEvent::ReadyToClose(DrainEvidence {
                node: operation.node(),
                session: operation.session(),
                remaining_cells: 0,
                unresolved_attempts: 0,
                relocated: true,
                readers_settled: true,
                followers_settled: true,
                facilities_closed: false,
                stopped: false,
                withdrawn: false,
            }),
            51,
        )
        .unwrap();
    let (closing, _) = ReaderEvacuationRecord::new(
        operation,
        (original.head_digest(), original.registry()),
        original.retired().clone(),
        original.original_digest(),
        original.authority().clone(),
        original.policy_revision(),
        1,
        1,
        (55, 60),
        replacements,
    )
    .unwrap();
    assert_eq!(closing.operation().phase(), MaintenancePhase::Closing);
    assert_eq!(closing.retired(), original.retired());
    assert_eq!(
        ReaderEvacuationRecord::from_bytes(&closing.to_bytes().unwrap()).unwrap(),
        closing
    );
}
