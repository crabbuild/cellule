use super::*;
use std::sync::atomic::Ordering;

async fn interrupted(mode: u8) -> (Fixture, Cell, Arc<Followers>) {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let mut node = f.node.advertisement().clone();
    node.log = Some(
        NodeLogStatus::open(
            node.node,
            EPOCH,
            vec![NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
        )
        .unwrap(),
    );
    node.generation += 1;
    f.node = f
        .directory
        .update_advertisement(&f.node, node, NOW)
        .await
        .unwrap();
    let mut cell = f.unbound_cell_for_application(4, [9; 16]).await;
    faults.mode.store(mode, Ordering::SeqCst);
    assert!(
        f.directory
            .bind_bundle_cell(&f.node, &cell.authority, &cell.control, NOW)
            .await
            .is_err()
    );
    faults.mode.store(0, Ordering::SeqCst);
    f.node = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    cell.control = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cell.control.value().bundle_binding.is_some(), mode == 4);
    let followers = Arc::new(Followers(
        [2, 3]
            .into_iter()
            .map(|number| {
                (
                    NodeId::from_bytes([number; 16]),
                    FollowerStore::open(
                        f.scratch.path().join(format!("provisional-{number}")),
                        Limits::default(),
                        cellule_ltx::DiskBudget::new(1 << 30),
                    )
                    .unwrap(),
                )
            })
            .collect(),
    ));
    (f, cell, followers)
}

#[tokio::test]
async fn failed_boot_closes_an_unpinned_quiet_reservation_without_a_cell_cas() {
    quiet(3, false).await;
}

#[tokio::test]
async fn failed_boot_closes_a_pinned_quiet_reservation_before_departure() {
    quiet(4, false).await;
}

#[tokio::test]
async fn failed_boot_closes_an_unpinned_reservation_after_the_cell_released() {
    quiet(3, true).await;
}

async fn quiet(mode: u8, released: bool) {
    let (f, mut cell, followers) = interrupted(mode).await;
    if released {
        let next = cell.control.value().release().unwrap();
        cell.control = cell
            .authority
            .transition(&cell.control, next, Transition::Release)
            .await
            .unwrap();
    }
    let original = cell.control.clone();
    let reservation = load_catalog(
        &f.layout,
        SessionId::from_bytes([1; 16]),
        f.node.advertisement().bundle_head().unwrap(),
    )
    .await
    .unwrap()
    .bindings
    .remove(0);
    if mode == 4 {
        assert!(matches!(
            cell.authority
                .transition(
                    &original,
                    original.value().release().unwrap(),
                    Transition::Release
                )
                .await,
            Err(Error::PendingPublication)
        ));
    }
    let fenced = fence(&f).await;
    f.lease.fence();
    if mode == 3 && !released {
        assert!(
            cell.authority
                .transition(&original, reservation.control, Transition::BindBundle)
                .await
                .is_err(),
            "a new enrollment authorization cannot cross the original node fence"
        );
    }
    let discovery = super::super::super::recovery::inventory_for_owner(
        &f.layout,
        SessionId::from_bytes([1; 16]),
    )
    .await
    .unwrap();
    assert_eq!(discovery.controls.len(), usize::from(mode == 4));
    assert_eq!(discovery.unpinned.len(), usize::from(mode == 3));
    assert_eq!(discovery.control_reads, 1);
    if mode == 3 {
        let catalog = crate::cell::catalog::CellCatalog::new(
            f.layout.clone(),
            crate::TenantId::from_bytes([9; 16]),
        );
        let discovered = crate::node::log_recovery::recoverable_cells_from_scopes_with_summary(
            &catalog,
            &cell.authority,
            SessionId::from_bytes([1; 16]),
            &[],
            1,
        )
        .await
        .unwrap();
        assert!(discovered.cells.is_empty());
        assert_eq!(discovered.summary.control_reads, 1);
        assert_eq!(discovered.summary.catalog_pages, 0);
    }
    // An unpinned reservation never selected Cell authority. Its original
    // catalog root remains an obligation, independently of a later owner.
    let inventory = if mode == 4 {
        vec![RecoveryCell {
            application: ApplicationId::from_bytes([9; 16]),
            authority: cell.authority.clone(),
            observed: cell.control.clone(),
        }]
    } else {
        Vec::new()
    };
    let recovery = NodeLogRecovery::from_fenced(followers, &fenced, Limits::default()).unwrap();
    let manifests = RecoveryManifestStore::new(f.layout.clone(), Limits::default())
        .with_recovery_scratch(f.scratch.path().to_owned());
    f.count.reset();
    let completed = RecoveryCoordinator::new(recovery, manifests)
        .recover_and_seal(&f.directory, fenced, inventory, NOW + 31_000)
        .await
        .unwrap();
    assert_eq!(completed.controls.len(), usize::from(mode == 4));
    assert_eq!(
        f.count.put_requests(),
        3,
        "catalog upload, terminal CAS and log seal; no Cell root CAS"
    );
    let current = cell
        .authority
        .load(original.value().cell)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.value(), original.value());
    let archived = super::super::super::recovery::inventory_for_owner(
        &f.layout,
        SessionId::from_bytes([1; 16]),
    )
    .await
    .unwrap();
    assert!(archived.controls.is_empty());
    assert_eq!(archived.closed.len(), 1);
    let destination = f.scratch.path().join("provisional-cold.sqlite");
    cell.replica
        .open_root(&original.value().ltx_root().unwrap())
        .await
        .unwrap()
        .restore(&destination)
        .await
        .unwrap();
    let restored = cellule_ltx::rusqlite::Connection::open(destination).unwrap();
    let result: String = restored
        .query_row(
            "SELECT result FROM outcomes WHERE request='seed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(result, "original");
    if mode == 4 {
        cell.authority
            .transition(
                &current,
                current.value().release().unwrap(),
                Transition::Release,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn provisional_native_frames_cannot_be_discarded_as_quiet_reservations() {
    for mode in [3, 4] {
        let (mut f, mut cell, followers) = interrupted(mode).await;
        let (_, frames, assigned) = f.append(&mut cell, 2);
        for member in [2, 3] {
            let id = NodeId::from_bytes([member; 16]);
            let receipt = followers
                .append(
                    id,
                    AppendRequest {
                        leader_session: SessionId::from_bytes([1; 16]),
                        log_epoch: EPOCH,
                        frames: frames.iter().map(|frame| frame.encoded().clone()).collect(),
                        covered_through: 0,
                    },
                )
                .await
                .unwrap();
            f.gate.acknowledge(id, receipt.durable_through).unwrap();
        }
        f.gate.activate_fleet().unwrap();
        assert_eq!(
            f.gate.prove(assigned.ticket()).await.unwrap().source(),
            DurabilitySource::Fleet
        );
        f.node = f.directory.activate_log(&f.node, NOW).await.unwrap();
        let fenced = fence(&f).await;
        let original_head = fenced.bundle_head();
        f.lease.fence();
        let recovery = NodeLogRecovery::from_fenced(followers, &fenced, Limits::default()).unwrap();
        let manifests = RecoveryManifestStore::new(f.layout.clone(), Limits::default())
            .with_recovery_scratch(f.scratch.path().to_owned());
        f.count.reset();
        let error = RecoveryCoordinator::new(recovery, manifests)
            .recover_and_seal(
                &f.directory,
                fenced.clone(),
                vec![RecoveryCell {
                    application: ApplicationId::from_bytes([9; 16]),
                    authority: cell.authority.clone(),
                    observed: cell.control.clone(),
                }],
                NOW + 31_000,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            Error::Node("provisional Cell issued before activation")
        ));
        assert_eq!(
            f.count.put_requests(),
            0,
            "a pre-activation frame cannot select terminal state"
        );
        let (record, _) = f
            .directory
            .load_record_at(
                &f.layout
                    .node_path(SessionId::from_bytes([1; 16]).as_bytes()),
            )
            .await
            .unwrap()
            .unwrap();
        let crate::node::directory::NodeRecord::Tombstone(current) = record else {
            panic!("failed original boot must remain fenced");
        };
        assert_eq!(current.bundle, original_head);
        assert_eq!(
            current.log.unwrap().phase(),
            crate::node::log_state::NodeLogPhase::Recovering
        );
        let current = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.value(), cell.control.value());
    }
}

#[tokio::test]
async fn a_pin_cas_already_authorized_before_fencing_cannot_reopen_the_closed_reservation() {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    let mut node = f.node.advertisement().clone();
    node.log = Some(
        NodeLogStatus::open(
            node.node,
            EPOCH,
            vec![NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
        )
        .unwrap(),
    );
    node.generation += 1;
    f.node = f
        .directory
        .update_advertisement(&f.node, node, NOW)
        .await
        .unwrap();
    let cell = f.unbound_cell_for_application(4, [9; 16]).await;
    let original_root = cell.control.value().ltx_root().unwrap();
    faults.mode.store(9, Ordering::SeqCst);
    let directory = f.directory.clone();
    let authority = cell.authority.clone();
    let control = cell.control.clone();
    let observed = f.node.clone();
    let task = tokio::spawn(async move {
        directory
            .bind_bundle_cell(&observed, &authority, &control, NOW)
            .await
    });
    faults.pin_started.notified().await;
    f.node = f
        .directory
        .load(SessionId::from_bytes([1; 16]), NOW)
        .await
        .unwrap()
        .unwrap();
    let fenced = fence(&f).await;
    f.lease.fence();
    let followers = Arc::new(Followers(
        [2, 3]
            .into_iter()
            .map(|member| {
                (
                    NodeId::from_bytes([member; 16]),
                    FollowerStore::open(
                        f.scratch.path().join(format!("held-pin-{member}")),
                        Limits::default(),
                        cellule_ltx::DiskBudget::new(1 << 30),
                    )
                    .unwrap(),
                )
            })
            .collect(),
    ));
    let recovery = NodeLogRecovery::from_fenced(followers, &fenced, Limits::default()).unwrap();
    let manifests = RecoveryManifestStore::new(f.layout.clone(), Limits::default())
        .with_recovery_scratch(f.scratch.path().to_owned());
    let completed = RecoveryCoordinator::new(recovery, manifests)
        .recover_and_seal(&f.directory, fenced, Vec::new(), NOW + 31_000)
        .await
        .unwrap();
    assert!(completed.controls.is_empty());
    faults.pin_resume.notify_one();
    assert!(task.await.unwrap().is_err());
    let current = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    assert!(current.value().bundle_binding.is_some());
    assert_eq!(current.value().ltx_root().unwrap(), original_root);
    let proof = f
        .directory
        .load_bundle_coverage(&cell.authority, &current, Limits::default())
        .await
        .unwrap();
    assert_eq!(proof.locator_count(), 0);
    cell.authority
        .transition(
            &current,
            current.value().release().unwrap(),
            Transition::Release,
        )
        .await
        .unwrap();
}
