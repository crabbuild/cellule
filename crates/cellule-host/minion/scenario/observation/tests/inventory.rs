//! Public collector over real managed boots, actors and request-bound pages.
use super::*;

async fn capture_node(fixture: &Fixture, roster: &FleetRoster, index: usize) -> FleetNodeInventory {
    let mut scan = FleetNodeInventoryScan::new(roster, node_id(index), session(index)).unwrap();
    while let Some(subject) = scan.next_subject().unwrap() {
        let response = page(
            &fixture.fleet,
            roster,
            index,
            subject,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        // Force original native continuations; the routine profile uses 32 rows.
        let old = response.request();
        let request = FleetSnapshotRequest::new(
            old.expected().clone(),
            old.nonce(),
            old.node(),
            old.session(),
            old.subject().clone(),
            1,
            old.issued_at_ms(),
            old.deadline_ms(),
        )
        .unwrap();
        drop(response);
        let response = fixture.fleet.nodes[index]
            .fleet_snapshot(request.clone())
            .await
            .unwrap();
        scan.accept(&request, &response, clock().unwrap()).unwrap();
    }
    scan.finish().unwrap()
}

#[tokio::test]
async fn full_native_traversal_uses_continuations_and_global_recheck() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    let mut inventory = capture_node(&fixture, &roster, 0).await;
    assert_eq!(inventory.cells().len(), CELL_COUNT);
    assert!(inventory.transitioning_cells().is_empty());
    assert!(!inventory.bindings().readers);
    assert_eq!(inventory.readers_closed(), None);
    assert!(inventory.reader_jobs().is_none());
    assert_eq!(inventory.follower_store_state(), None);
    assert_eq!(inventory.follower_producer_state(), None);
    inventory.validate_enrollments(&roster).unwrap();
    let original = inventory.interval();
    let mut check = inventory.recheck();
    let mut pages = 0;
    while let Some(subject) = check.next_subject().unwrap() {
        let response = page(
            &fixture.fleet,
            &roster,
            0,
            subject,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        check
            .accept(response.request(), &response, clock().unwrap())
            .unwrap();
        pages += 1;
    }
    assert_eq!(pages, 7);
    let interval = check.finish().unwrap();
    assert_eq!(interval.0, original.0);
    assert!(interval.1 >= original.1);
    drop(inventory);
    fixture.close().await;
}

#[tokio::test]
async fn duplicated_nonce_poisoning_and_partial_traversals_cannot_confirm() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    let mut scan = FleetNodeInventoryScan::new(&roster, node_id(0), session(0)).unwrap();
    let host = page(
        &fixture.fleet,
        &roster,
        0,
        FleetSnapshotSubject::Host,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    scan.accept(host.request(), &host, clock().unwrap())
        .unwrap();
    let original = host.request();
    let repeated = FleetSnapshotRequest::new(
        original.expected().clone(),
        original.nonce(),
        original.node(),
        original.session(),
        FleetSnapshotSubject::Cells(None),
        1,
        original.issued_at_ms(),
        original.deadline_ms(),
    )
    .unwrap();
    let repeated_page = fixture.fleet.nodes[0]
        .fleet_snapshot(repeated.clone())
        .await
        .unwrap();
    assert!(
        scan.accept(&repeated, &repeated_page, clock().unwrap())
            .is_err()
    );
    drop(repeated_page);
    assert!(scan.next_subject().is_err());
    assert!(scan.finish().is_err());
    let mut inventory = capture_node(&fixture, &roster, 0).await;
    assert!(inventory.recheck().finish().is_err());
    let mut check = inventory.recheck();
    assert!(
        check
            .accept(host.request(), &host, clock().unwrap())
            .is_err()
    );
    assert!(check.finish().is_err());
    drop(host);
    drop(inventory);
    fixture.close().await;
}

#[tokio::test]
async fn changed_complete_category_after_remote_scan_refuses_recheck() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    let mut inventory = capture_node(&fixture, &roster, 0).await;
    // A real actor admission changes topology without changing the roster.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture.fleet.nodes[0]
                .runtime()
                .evict_idle(1)
                .await
                .unwrap()
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut check = inventory.recheck();
    while let Some(subject) = check.next_subject().unwrap() {
        let response = page(
            &fixture.fleet,
            &roster,
            0,
            subject.clone(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        let accepted = check.accept(response.request(), &response, clock().unwrap());
        if matches!(subject, FleetSnapshotSubject::Cells(_)) {
            assert!(accepted.is_err());
            break;
        }
        accepted.unwrap();
    }
    assert!(check.finish().is_err());
    drop(inventory);
    fixture.close().await;
}

#[tokio::test]
async fn topology_change_retains_partial_rows_for_independent_pressure_checks() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    let mut scan = FleetNodeInventoryScan::new(&roster, node_id(0), session(0)).unwrap();
    while let Some(subject) = scan.next_subject().unwrap() {
        if matches!(subject, FleetSnapshotSubject::Cells(None)) && !scan.cells().is_empty() {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if fixture.fleet.nodes[0]
                        .runtime()
                        .evict_idle(1)
                        .await
                        .unwrap()
                        == 1
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let response = page(
                &fixture.fleet,
                &roster,
                0,
                subject,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert!(matches!(
                scan.accept(response.request(), &response, clock().unwrap()),
                Err(cellule_runtime::Error::Node(
                    "native inventory category changed"
                ))
            ));
            break;
        }
        let response = page(
            &fixture.fleet,
            &roster,
            0,
            subject,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        scan.accept(response.request(), &response, clock().unwrap())
            .unwrap();
    }
    assert_eq!(scan.cells().len(), CELL_COUNT);
    assert!(scan.next_subject().is_err());
    assert!(scan.finish().is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.fleet.nodes[0]
            .runtime()
            .unreleased_cell_count()
            .await
            .unwrap()
            != CELL_COUNT - 1
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let fresh = fixture.capture().await;
    assert!(fresh.complete);
    assert_eq!(fresh.cells.len(), CELL_COUNT - 1);
    fixture.close().await;
}

#[tokio::test]
async fn canonical_owner_renewal_invalidates_the_exact_authority_interval() {
    let fixture = Fixture::new().await;
    let roster = fixture.roster().await;
    let inventory = capture_node(&fixture, &roster, 0).await;
    let mut cells = inventory.cells().to_vec();
    let cell = cells[0].observation.target.cell_id();
    let record = fixture.fleet.records.get(&cell).unwrap();
    let original = record.authority.load(cell).await.unwrap().unwrap();
    let mut renewal = original.value().clone();
    renewal.revision += 1;
    renewal.progress += 1;
    let renewed = record
        .authority
        .transition(
            &original,
            renewal,
            cellule_runtime::control::Transition::Renew,
        )
        .await
        .unwrap();
    assert_eq!(
        renewed.value().owner_fence(),
        original.value().owner_fence()
    );
    assert_eq!(renewed.value().root, original.value().root);
    let unchanged = recheck_authority(
        &fixture.fleet,
        &HashMap::from([(cell, original.value().clone())]),
        &mut cells,
    )
    .await
    .unwrap();
    assert!(!unchanged);
    assert_eq!(cells.len(), CELL_COUNT - 1);
    assert!(
        cells
            .iter()
            .all(|owned| owned.observation.target.cell_id() != cell)
    );
    drop(inventory);
    fixture.close().await;
}
