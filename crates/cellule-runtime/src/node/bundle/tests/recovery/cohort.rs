use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn full_recovery_stages_65_real_cell_checkpoints_before_one_terminal_node_cas() {
    let faults = Arc::new(super::super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    enroll(&mut f).await;
    let mut cells = Vec::new();
    let mut prefix = Vec::new();
    let mut assignments = Vec::new();
    for byte in 1..=65 {
        let mut cell = f.cell(byte).await;
        let (_, frames, assigned) = f.append(&mut cell, 2);
        assert_eq!(
            frames.len(),
            1,
            "the small-image fixture has one native frame per command"
        );
        prefix.extend(frames);
        assignments.push(assigned);
        cells.push(cell);
    }
    for (frames, assigned) in prefix.chunks(64).zip(assignments.chunks(64)) {
        let prepared = f
            .directory
            .prepare_node_bundle(&f.node, frames, assigned, NOW)
            .await
            .unwrap();
        f.node = f
            .directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .unwrap()
            .0;
    }
    let through = f
        .node
        .advertisement()
        .bundle_head()
        .unwrap()
        .selected_through();
    f.node = f
        .directory
        .advance_log_coverage(&f.node, through, NOW)
        .await
        .unwrap();
    let mut suffix = Vec::new();
    let mut fleet = Vec::new();
    for cell in &mut cells {
        let (_, frames, assigned) = f.append(cell, 3);
        suffix.extend(frames);
        fleet.push(assigned);
    }
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let followers = Arc::new(Followers(
        dirs.iter()
            .enumerate()
            .map(|(i, dir)| {
                (
                    NodeId::from_bytes([i as u8 + 2; 16]),
                    FollowerStore::open(
                        dir.path().to_owned(),
                        Limits::default(),
                        cellule_ltx::DiskBudget::new(1 << 30),
                    )
                    .unwrap(),
                )
            })
            .collect(),
    ));
    for member in [2, 3] {
        let id = NodeId::from_bytes([member; 16]);
        for frames in prefix.chunks(64) {
            followers
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
        }
        let mut receipt = None;
        for frames in suffix.chunks(64) {
            receipt = Some(
                followers
                    .append(
                        id,
                        AppendRequest {
                            leader_session: SessionId::from_bytes([1; 16]),
                            log_epoch: EPOCH,
                            frames: frames.iter().map(|frame| frame.encoded().clone()).collect(),
                            covered_through: through,
                        },
                    )
                    .await
                    .unwrap(),
            );
        }
        let receipt = receipt.unwrap();
        assert!(receipt.base_sequence > through);
        f.gate.acknowledge(id, receipt.durable_through).unwrap();
    }
    f.gate.activate_fleet().unwrap();
    for assigned in fleet {
        assert_eq!(
            f.gate.prove(assigned.ticket()).await.unwrap().source(),
            DurabilitySource::Fleet
        );
    }
    let fenced = fence(&f).await;
    f.lease.fence();
    let transport: Arc<dyn NodeLogTransport> = followers;
    let recovery = NodeLogRecovery::from_fenced(transport, &fenced, Limits::default()).unwrap();
    let manifests = RecoveryManifestStore::new(f.layout.clone(), Limits::default())
        .with_recovery_scratch(f.scratch.path().to_owned());
    let inventory = cells
        .iter()
        .map(|cell| RecoveryCell {
            application: ApplicationId::from_bytes(*cell.authority.layout().application_id()),
            authority: cell.authority.clone(),
            observed: cell.control.clone(),
        })
        .collect();
    faults.node_updates.store(0, Ordering::SeqCst);
    faults.coverage_puts.store(0, Ordering::SeqCst);
    let completed = RecoveryCoordinator::new(recovery, manifests)
        .recover_and_seal(&f.directory, fenced, inventory, NOW + 31_000)
        .await
        .unwrap();
    assert_eq!(completed.controls.len(), 65);
    assert_eq!(
        faults.coverage_puts.load(Ordering::SeqCst),
        2,
        "65 roots stage in bounded 64-root cohorts"
    );
    assert_eq!(
        faults.node_updates.load(Ordering::SeqCst),
        2,
        "one complete terminal catalog CAS, then one log seal"
    );
    for cell in &cells {
        let current = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
        assert!(current.value().recovery.is_none());
        assert_eq!(current.value().root.as_ref().unwrap().commit_sequence, 3);
        let cold = CellReplica::new(
            cell.authority.layout().clone(),
            *current.value().cell.as_bytes(),
            *current.value().incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let destination = f.scratch.path().join(format!(
            "cohort-cold-{}.sqlite",
            current.value().cell.as_bytes()[0]
        ));
        cold.open_root(&current.value().ltx_root().unwrap())
            .await
            .unwrap()
            .restore(&destination)
            .await
            .unwrap();
        let restored = cellule_ltx::rusqlite::Connection::open(destination).unwrap();
        for command in [2, 3] {
            let result: String = restored
                .query_row(
                    "SELECT result FROM outcomes WHERE request=?1",
                    [format!("request-{command}")],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(result, format!("result-{command}"));
        }
    }
}
