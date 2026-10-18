//! Concurrent enrollment must share catalog work without admitting an unpinned Cell.
use super::*;
use crate::node::durability::{NodeBundleAuthority, NodeDurability};
use crate::node::log_shipper::NodeLogShipper;

#[tokio::test]
async fn simultaneous_enrollment_amortizes_catalog_writes() {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..12 {
        cells.push(f.unbound_cell_at_commit(number, [9; 16], 0).await);
    }
    let authority = Arc::new(super::actor::Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let peers = super::actor::transport(&f, true);
    let shipper = NodeLogShipper::new(f.gate.clone(), peers.clone(), Limits::default()).unwrap();
    let durability = Arc::new(NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        peers,
        f.lease.clone(),
    ));
    let feed = durability
        .enable_bundle_publication(authority.clone())
        .unwrap();
    f.count.reset();
    let results = futures_util::future::join_all(
        cells
            .iter()
            .map(|cell| durability.bind_bundle_cell(&cell.authority, &cell.control)),
    )
    .await;
    let puts = f.count.put_requests();
    for (cell, control) in cells.iter_mut().zip(results) {
        cell.control = control.unwrap();
        assert!(cell.control.value().bundle_binding.is_some());
    }
    eprintln!("concurrent_enrollment cells={} puts={puts}", cells.len());
    for cell in &cells {
        let issued = f
            .gate
            .close_cell_issuance(
                Fixture::scope(cell),
                cell.control.value().ltx_root().unwrap(),
            )
            .unwrap();
        authority
            .close(&cell.authority, &cell.control, issued)
            .await
            .unwrap();
    }
    drop(feed);
    durability.shutdown().await.unwrap();
    assert_eq!(
        puts,
        cells.len() + 4,
        "eight pins share two catalog PUTs and two node CASes"
    );
}

#[tokio::test]
async fn cohort_enrollment_is_ordered_idempotent_and_separates_applications() {
    let mut f = Fixture::new().await;
    let a = f.unbound_cell_at_commit(4, [9; 16], 0).await;
    let b = f.unbound_cell_at_commit(4, [8; 16], 0).await;
    let input = [(&a.authority, &a.control), (&b.authority, &b.control)];
    f.count.reset();
    let (node, pinned) = f
        .directory
        .bind_bundle_cells(&f.node, &input, NOW)
        .await
        .unwrap();
    assert_eq!(f.count.put_requests(), 6);
    assert_ne!(
        pinned[0].value().bundle_binding,
        pinned[1].value().bundle_binding
    );
    let input = [(&a.authority, &pinned[0]), (&b.authority, &pinned[1])];
    f.count.reset();
    let (same, replay) = f
        .directory
        .bind_bundle_cells(&node, &input, NOW)
        .await
        .unwrap();
    assert_eq!(same.advertisement(), node.advertisement());
    assert_eq!(f.count.put_requests(), 0);
    for (actual, expected) in replay.iter().zip(&pinned) {
        assert_eq!(actual.value(), expected.value());
    }
    let catalog = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        node.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.bindings.len(), 2);
    assert!(
        catalog
            .bindings
            .iter()
            .all(|binding| binding.phase == BindingPhase::Open)
    );
    for cell in [a, b] {
        let path = f.scratch.path().join(format!(
            "enrolled-{}.sqlite",
            cell.authority.layout().application_id()[0]
        ));
        cell.replica
            .open_root(&cell.control.value().ltx_root().unwrap())
            .await
            .unwrap()
            .restore(&path)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(path).unwrap();
        let value: String = db
            .query_row(
                "SELECT result FROM outcomes WHERE request='seed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "original");
    }
}

#[tokio::test]
async fn invalid_enrollment_cohorts_publish_nothing() {
    let mut f = Fixture::new().await;
    let cell = f.unbound_cell_at_commit(4, [9; 16], 0).await;
    f.count.reset();
    assert!(
        f.directory
            .bind_bundle_cells(&f.node, &[], NOW)
            .await
            .is_err()
    );
    let duplicate = vec![(&cell.authority, &cell.control); 2];
    assert!(
        f.directory
            .bind_bundle_cells(&f.node, &duplicate, NOW)
            .await
            .is_err()
    );
    let excessive = vec![(&cell.authority, &cell.control); 9];
    assert!(
        f.directory
            .bind_bundle_cells(&f.node, &excessive, NOW)
            .await
            .is_err()
    );
    assert_eq!(f.count.put_requests(), 0);
    assert!(
        cell.authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .bundle_binding
            .is_none()
    );
}

#[tokio::test]
async fn wide_catalog_enrollment_declines_the_cohort_without_refusing_individual_cells() {
    use crate::node::bundle::index as catalog_index;
    let mut f = Fixture::new().await;
    let a = f.cell(4).await;
    let shard_a = catalog_index::shard(&[9; 16], &[4; 32]);
    let byte_b = (5..=200)
        .find(|byte| catalog_index::shard(&[9; 16], &[*byte; 32]) != shard_a)
        .unwrap();
    let b = f.cell(byte_b).await;
    for seed in [&a, &b] {
        let cell = seed.control.value().cell;
        let wanted = catalog_index::shard(&[9; 16], cell.as_bytes());
        let head = f.node.advertisement().bundle_head().unwrap();
        let keys = [([9; 16], *cell.as_bytes())].into_iter().collect();
        let mut catalog =
            store::load_catalog_cells(&f.layout, SessionId::from_bytes([1; 16]), head, &keys)
                .await
                .unwrap();
        let template = catalog.bindings[0].clone();
        // As in the density fixtures, unrelated rows have no origin roots and
        // grant no native authority. Enrollment must inspect only its targets.
        for number in 0_u64..2_000_000 {
            let mut id = [255; 32];
            id[..8].copy_from_slice(&number.to_le_bytes());
            if catalog_index::shard(&[9; 16], &id) != wanted {
                continue;
            }
            let mut row = template.clone();
            row.control.cell = CellId::from_bytes(id);
            row.control.owner.as_mut().unwrap().endpoint = format!("https://{}", "a".repeat(504));
            row.control.bundle_binding.as_mut().unwrap().digest =
                Digest::from_bytes(*blake3::hash(&id).as_bytes());
            catalog.bindings.push(row);
            if catalog.bindings.len() == 2_000 {
                break;
            }
        }
        assert_eq!(catalog.bindings.len(), 2_000);
        catalog
            .bindings
            .sort_unstable_by_key(|row| *row.control.bundle_binding.unwrap().digest.as_bytes());
        let prepared = f
            .directory
            .upload_catalog(Some(head), catalog, &[])
            .await
            .unwrap();
        f.node = f
            .directory
            .select_catalog(&f.node, &prepared, NOW)
            .await
            .unwrap();
    }
    let application_for = |cell: CellId, skip: usize| {
        let wanted = catalog_index::shard(&[9; 16], cell.as_bytes());
        (10_u64..100_000)
            .map(|number| {
                let mut app = [0; 16];
                app[..8].copy_from_slice(&number.to_le_bytes());
                app
            })
            .filter(|app| catalog_index::shard(app, cell.as_bytes()) == wanted)
            .nth(skip)
            .unwrap()
    };
    let cells = [
        f.unbound_cell_at_commit(4, application_for(a.control.value().cell, 0), 0)
            .await,
        f.unbound_cell_at_commit(byte_b, application_for(b.control.value().cell, 0), 0)
            .await,
        f.unbound_cell_at_commit(4, application_for(a.control.value().cell, 1), 0)
            .await,
        f.unbound_cell_at_commit(4, application_for(a.control.value().cell, 2), 0)
            .await,
    ];
    let authority = super::actor::Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    };
    for (pair, reason) in [
        (&cells[..2], "bundle selected shard bytes"),
        (&cells[2..], "bundle enrollment pin metadata"),
    ] {
        let input = pair
            .iter()
            .map(|cell| (&cell.authority, &cell.control))
            .collect::<Vec<_>>();
        let node = authority.observed.lock().await.clone();
        f.count.reset();
        assert!(
            matches!(f.directory.bind_bundle_cells(&node, &input, NOW).await, Err(Error::Capacity(actual)) if actual == reason)
        );
        assert!(authority.bind_batch(&input).await.is_none());
        assert_eq!(
            f.count.put_requests(),
            0,
            "declining must not mutate authority or upload a proposal"
        );
        let pinned =
            futures_util::future::join_all(input.into_iter().map(|(a, c)| authority.bind(a, c)))
                .await;
        assert!(
            pinned
                .into_iter()
                .all(|control| control.unwrap().value().bundle_binding.is_some())
        );
        assert_eq!(
            f.count.put_requests(),
            10,
            "two individually admitted catalog cohorts"
        );
    }
}

#[tokio::test]
async fn partial_pin_failure_preserves_every_provisional_cell_and_retries_exactly() {
    cohort_reply_failure(3).await;
}

#[tokio::test]
async fn failed_open_selection_preserves_all_pins_and_retries_exactly() {
    cohort_reply_failure(4).await;
}

#[tokio::test]
async fn lost_cohort_selection_and_pin_replies_reconcile_without_duplicate_bindings() {
    cohort_reply_failure(2).await;
    cohort_reply_failure(8).await;
}

async fn cohort_reply_failure(mode: u8) {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(backend.clone()).await;
    let mut cells = Vec::new();
    for number in 4..12 {
        cells.push(f.unbound_cell_at_commit(number, [9; 16], 0).await);
    }
    let input = cells
        .iter()
        .map(|cell| (&cell.authority, &cell.control))
        .collect::<Vec<_>>();
    backend.mode.store(mode, Ordering::SeqCst);
    let result = f.directory.bind_bundle_cells(&f.node, &input, NOW).await;
    assert_eq!(result.is_ok(), matches!(mode, 2 | 8));
    drop(input);
    backend.mode.store(0, Ordering::SeqCst);
    f.node = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    let catalog = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        f.node.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.bindings.len(), 8);
    for cell in &mut cells {
        cell.control = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
    }
    if matches!(mode, 3 | 4) {
        assert!(
            catalog
                .bindings
                .iter()
                .all(|binding| binding.phase == BindingPhase::Provisional)
        );
        assert_eq!(
            cells
                .iter()
                .filter(|cell| cell.control.value().bundle_binding.is_some())
                .count(),
            if mode == 3 { 7 } else { 8 }
        );
    }
    let pins = catalog
        .bindings
        .iter()
        .map(|binding| binding.control.bundle_binding.unwrap())
        .collect::<Vec<_>>();
    let input = cells
        .iter()
        .map(|cell| (&cell.authority, &cell.control))
        .collect::<Vec<_>>();
    let (node, pinned) = f
        .directory
        .bind_bundle_cells(&f.node, &input, NOW)
        .await
        .unwrap();
    assert_eq!(pinned.len(), 8);
    for control in pinned {
        assert!(pins.contains(&control.value().bundle_binding.unwrap()));
    }
    let catalog = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        node.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(catalog.bindings.len(), 8);
    assert!(
        catalog
            .bindings
            .iter()
            .all(|binding| binding.phase == BindingPhase::Open)
    );
}
