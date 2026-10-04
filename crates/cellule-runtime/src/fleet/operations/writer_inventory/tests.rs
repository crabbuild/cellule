use super::*;
use crate::{
    control::{ControlState, Owner},
    identity::{IncarnationId, NamespaceId, NodeId, SessionId},
    node::NodeMode,
};

fn fixture(
    count: usize,
) -> (
    OriginalWriterInventoryRecord,
    Vec<OriginalWriterInventoryPage>,
) {
    let scope = FleetScope {
        fleet: Digest::from_bytes([9; 32]),
        application: ApplicationId::from_bytes([3; 16]),
    };
    let intent = NodeIntent::initial(
        scope,
        NodeId::from_bytes([1; 16]),
        SessionId::from_bytes([2; 16]),
    )
    .unwrap();
    let boot = EnrollmentRecord::pending(
        EnrollmentSpec {
            scope,
            request: Digest::from_bytes([10; 32]),
            source: None,
            target: EnrollmentEndpoint {
                node: intent.node(),
                session: intent.session(),
                intent_revision: 1,
            },
            role: EnrollmentRole::Node {
                mode: NodeMode::Active,
            },
        },
        None,
        None,
        &intent,
        10,
    )
    .unwrap()
    .establish(Digest::from_bytes([11; 32]), 20)
    .unwrap();
    let basis = OriginalWriterInventoryBasis {
        operation: MaintenanceOperation::new(
            OperationId::from_bytes([12; 16]).unwrap(),
            Digest::from_bytes([13; 32]),
            intent.node(),
            intent.session(),
            2,
            0,
            60_000,
        )
        .unwrap(),
        head_digest: Digest::from_bytes([14; 32]),
        registry: RegistryVersion::new(scope).unwrap().bootstrap(0).unwrap(),
        boot,
        process_request: Digest::from_bytes([15; 32]),
        process_witness: Digest::from_bytes([16; 32]),
        catalog_witness: Digest::from_bytes([17; 32]),
        interval: (30, 40),
    };
    let tenant = TenantId::from_bytes([4; 16]);
    let rows = (0..count)
        .map(|n| {
            let target = CellTarget::new(
                tenant,
                scope.application,
                NamespaceId::from_bytes([5; 16]),
                &(n as u64).to_be_bytes(),
            )
            .unwrap();
            let control = Control::initial(
                target.cell_id(),
                IncarnationId::from_bytes([6; 16]),
                Owner {
                    session: intent.session(),
                    endpoint: "https://original.example".into(),
                },
                Digest::from_bytes([7; 32]),
                1,
            )
            .unwrap();
            OriginalWriterObservation { target, control }
        })
        .collect();
    OriginalWriterInventoryRecord::new(
        basis,
        vec![OriginalCatalogWitness {
            application: scope.application,
            tenant,
            source: Digest::from_bytes([18; 32]),
            heads: Digest::from_bytes([19; 32]),
            histories: Digest::from_bytes([20; 32]),
            cells: count as u64,
            owners: count as u64,
        }],
        rows,
    )
    .unwrap()
}
#[test]
fn original_writer_manifest_and_all_pages_roundtrip_full_rootless_controls() {
    let (record, pages) = fixture(129);
    assert_eq!(
        pages.iter().map(|p| p.entries.len()).collect::<Vec<_>>(),
        [64, 64, 1]
    );
    assert_eq!(
        OriginalWriterInventoryRecord::from_bytes(&record.to_bytes().unwrap()).unwrap(),
        record
    );
    for page in &pages {
        assert_eq!(
            OriginalWriterInventoryPage::from_bytes(&page.to_bytes().unwrap()).unwrap(),
            *page
        );
        assert!(
            page.entries
                .iter()
                .all(|row| row.control.root.is_none()
                    && row.control.state == ControlState::Recovering)
        );
    }
    record.validate_pages(&pages).unwrap();
}
#[test]
fn empty_original_writer_set_is_explicit_complete_metadata() {
    let (record, pages) = fixture(0);
    assert_eq!(record.owner_count(), 0);
    assert!(pages.is_empty());
    record.validate_pages(&pages).unwrap();
    let mut basis = record.basis.clone();
    basis.catalog_witness = Digest::from_bytes([0; 32]);
    assert!(OriginalWriterInventoryRecord::new(basis, Vec::new(), Vec::new()).is_err());
}
#[test]
fn incomplete_reordered_duplicate_and_foreign_writer_pages_refuse() {
    let (record, pages) = fixture(65);
    assert!(record.validate_pages(&pages[..1]).is_err());
    let mut changed = pages.clone();
    changed.reverse();
    assert!(record.validate_pages(&changed).is_err());
    let mut changed = pages.clone();
    changed[0].entries[1] = changed[0].entries[0].clone();
    assert!(record.validate_pages(&changed).is_err());
    let mut rows = pages
        .into_iter()
        .flat_map(|p| p.entries)
        .collect::<Vec<_>>();
    rows[0].control.owner.as_mut().unwrap().session = SessionId::from_bytes([99; 16]);
    assert!(OriginalWriterInventoryRecord::new(record.basis, record.catalogs, rows).is_err());
}
#[test]
fn original_inventory_rejects_noncanonical_scope_counts_intervals_and_limits() {
    let (record, pages) = fixture(1);
    let rows = pages[0].entries.clone();
    let mut catalogs = record.catalogs.clone();
    catalogs[0].owners = 0;
    assert!(
        OriginalWriterInventoryRecord::new(record.basis.clone(), catalogs, rows.clone()).is_err()
    );
    let mut catalogs = record.catalogs.clone();
    catalogs.push(catalogs[0].clone());
    assert!(
        OriginalWriterInventoryRecord::new(record.basis.clone(), catalogs, rows.clone()).is_err()
    );
    let mut basis = record.basis.clone();
    basis.interval = (40, 30);
    assert!(
        OriginalWriterInventoryRecord::new(basis, record.catalogs.clone(), rows.clone()).is_err()
    );
    let mut basis = record.basis.clone();
    basis.registry = RegistryVersion::new(basis.registry.scope()).unwrap();
    assert!(
        OriginalWriterInventoryRecord::new(basis, record.catalogs.clone(), rows.clone()).is_err()
    );
    assert!(
        OriginalWriterInventoryRecord::new(
            record.basis,
            record.catalogs,
            vec![rows[0].clone(); MAX_ORIGINAL_WRITERS + 1]
        )
        .is_err()
    );
}
#[test]
fn original_inventory_decoders_refuse_every_truncation_trailing_unknown_and_oversize_body() {
    let (record, pages) = fixture(1);
    let body = record.to_bytes().unwrap();
    for length in 0..body.len() {
        assert!(OriginalWriterInventoryRecord::from_bytes(&body[..length]).is_err());
    }
    let mut changed = body.clone();
    changed.push(0);
    assert!(OriginalWriterInventoryRecord::from_bytes(&changed).is_err());
    let mut changed = body;
    changed[0] ^= 1;
    assert!(OriginalWriterInventoryRecord::from_bytes(&changed).is_err());
    let body = pages[0].to_bytes().unwrap();
    for length in 0..body.len() {
        assert!(OriginalWriterInventoryPage::from_bytes(&body[..length]).is_err());
    }
    let mut changed = body;
    changed.push(0);
    assert!(OriginalWriterInventoryPage::from_bytes(&changed).is_err());
    assert!(
        OriginalWriterInventoryPage::from_bytes(&vec![0; MAX_PAGE_BYTES as usize + 1]).is_err()
    );
}
#[test]
fn original_controls_bind_code_schema_roots_and_observation_target() {
    let (record, pages) = fixture(1);
    let mut rows = pages[0].entries.clone();
    let mut changed = rows.clone();
    changed[0].control.cell = crate::identity::CellId::from_bytes([88; 32]);
    assert!(
        OriginalWriterInventoryRecord::new(record.basis.clone(), record.catalogs.clone(), changed)
            .is_err()
    );
    rows[0].control.state = ControlState::Serving;
    rows[0].control.root = Some(crate::control::RootRef {
        digest: Digest::from_bytes([89; 32]),
        txid: 1,
        checksum: cellule_ltx::types::CHECKSUM_FLAG | 1,
        commit_sequence: 1,
    });
    rows[0].control.code = Digest::from_bytes([88; 32]);
    rows[0].control.schema = 2;
    let (later, _) =
        OriginalWriterInventoryRecord::new(record.basis.clone(), record.catalogs.clone(), rows)
            .unwrap();
    assert_ne!(later.digest().unwrap(), record.digest().unwrap());
    let encoded = later.to_bytes().unwrap();
    assert_eq!(
        OriginalWriterInventoryRecord::from_bytes(&encoded).unwrap(),
        later
    );
}

#[test]
fn repeated_original_epochs_preserve_one_ordered_incarnation_across_pages() {
    let (record, pages) = fixture(64);
    let mut rows = pages[0].entries.clone();
    let original = rows.last().unwrap().clone();
    let mut later = original.clone();
    later.control = original
        .control
        .takeover(Owner {
            session: SessionId::from_bytes([88; 16]),
            endpoint: "https://intermediate.example".into(),
        })
        .unwrap()
        .takeover(original.control.owner.clone().unwrap())
        .unwrap();
    rows.push(later.clone());
    let mut catalogs = record.catalogs.clone();
    catalogs[0].owners = 65;
    let (complete, pages) =
        OriginalWriterInventoryRecord::new(record.basis.clone(), catalogs.clone(), rows.clone())
            .unwrap();
    assert_eq!(pages.len(), 2);
    complete.validate_pages(&pages).unwrap();
    for change in 0..3 {
        let mut changed = rows.clone();
        let control = &mut changed.last_mut().unwrap().control;
        match change {
            0 => control.incarnation = IncarnationId::from_bytes([7; 16]),
            1 => control.revision = original.control.revision,
            _ => control.progress = original.control.progress,
        }
        assert!(
            OriginalWriterInventoryRecord::new(record.basis.clone(), catalogs.clone(), changed,)
                .is_err()
        );
    }
}
