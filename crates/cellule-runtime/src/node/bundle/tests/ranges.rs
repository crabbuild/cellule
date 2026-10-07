use super::*;
use object_store::ObjectStoreExt as _;

#[tokio::test]
async fn missing_base_chunk_cannot_mint_a_selected_reconstruction_proof() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let base = cell.control.value().ltx_root().unwrap();
    let objects = cell.replica.reachable_objects(&base).await.unwrap();
    let dependency = objects
        .into_iter()
        .find(|object| object.kind != cellule_ltx::CellObjectKind::Root)
        .unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let path = f.layout.incarnation_object_path(
        cell.control.value().cell.as_bytes(),
        cell.control.value().incarnation.as_bytes(),
        &dependency.digest,
        dependency.kind,
    );
    f.layout.store().delete(&path).await.unwrap();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert_eq!(
        f.directory
            .load(SessionId::from_bytes([1; 16]), NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .bundle_head(),
        f.node.advertisement().bundle_head()
    );
}

#[tokio::test]
async fn same_cell_ids_in_two_applications_keep_separate_bases_and_proofs() {
    let mut f = Fixture::new().await;
    let mut a = f.cell(4).await;
    let mut b = f.cell_for_application(4, [10; 16]).await;
    let (_, mut frames, first) = f.append(&mut a, 2);
    let (_, extra, second) = f.append(&mut b, 2);
    frames.extend(extra);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[first, second], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs.len(), 2);
    for cell in [a, b] {
        let proof = proofs
            .iter()
            .find(|proof| Some(proof.binding()) == cell.control.value().bundle_binding)
            .unwrap();
        let mut publisher = f.publisher(&cell);
        assert_eq!(
            publisher
                .materialize_bundle(proof)
                .await
                .unwrap()
                .commit_sequence,
            2
        );
    }
}

#[tokio::test]
async fn forward_native_issuance_cannot_hide_a_missing_object_only_command() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    f.node = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    cell.db
        .transaction(|tx| tx.execute_batch("INSERT INTO outcomes VALUES ('request-3', 'result-3')"))
        .unwrap();
    let _object_only = cell.db.capture().unwrap();
    let (_, next, assigned) = f.append(&mut cell, 4);
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &next, &[assigned], NOW)
            .await
            .is_err()
    );
    assert_eq!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .unwrap()
            .commit_sequence(),
        2
    );
}

#[tokio::test]
async fn a_native_capture_group_cannot_be_selected_at_a_partial_endpoint() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let mut cuts = Vec::new();
    for commit in [2, 3] {
        cell.db
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO outcomes VALUES (?1,?2)",
                    [format!("request-{commit}"), format!("result-{commit}")],
                )
            })
            .unwrap();
        cuts.push(cell.db.capture().unwrap());
    }
    let segments: Vec<_> = cuts.iter().flat_map(|cut| &cut.segments).collect();
    assert!(segments.len() > 1);
    let ticket = f.gate.preview(segments.len() as u64).unwrap();
    let frames: Vec<_> = segments
        .iter()
        .enumerate()
        .map(|(offset, segment)| {
            cellule_ltx::encode_node_frame_range(
                cellule_ltx::NodeFrameScope {
                    leader_session: [1; 16],
                    log_epoch: EPOCH,
                    node_sequence: ticket.first_sequence() + offset as u64,
                    application: [9; 16],
                    cell: *cell.control.value().cell.as_bytes(),
                    incarnation: *cell.control.value().incarnation.as_bytes(),
                    cell_epoch: cell.control.value().epoch,
                    commit_sequence: 3,
                },
                2,
                segment.info().clone(),
                Bytes::from(std::fs::read(segment.path()).unwrap()),
                Limits::default(),
            )
            .unwrap()
        })
        .collect();
    let assigned = f.gate.commit_frames(ticket, &frames).unwrap().unwrap();
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &frames[..frames.len() - 1], &[assigned], NOW)
            .await
            .is_err()
    );
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs[0].commit_sequence(), 3);
    assert_eq!(proofs[0].position(), cuts.last().unwrap().position);
    let mut partial = proofs[0].base().unwrap();
    partial.commit_sequence = 3;
    partial.position = frames[0].segment().position();
    assert!(checkpoint_prefix(&proofs[0].binding, &frames, &partial).is_err());
}

#[tokio::test]
async fn corrupt_origin_dependency_and_overlapping_ranges_fail_closed() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let mut duplicate = frames.clone();
    duplicate.extend(frames.clone());
    assert!(
        f.directory
            .prepare_node_bundle(&f.node, &duplicate, &[assigned, assigned], NOW)
            .await
            .is_err()
    );
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    let (selected, _) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = selected;
    let (_, next, assigned) = f.append(&mut cell, 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &next, &[assigned], NOW)
        .await
        .unwrap();
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, prepared.head.digest.as_bytes());
    let mut corrupt = prepared.body.to_vec();
    let last = corrupt.last_mut().unwrap();
    *last ^= 1;
    f.layout
        .store()
        .inner()
        .put(&path, Bytes::from(corrupt).into())
        .await
        .unwrap();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert_eq!(
        f.directory
            .load(SessionId::from_bytes([1; 16]), NOW)
            .await
            .unwrap()
            .unwrap()
            .advertisement()
            .bundle_head(),
        f.node.advertisement().bundle_head()
    );
    assert!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn locator_pressure_refuses_selection_without_dropping_the_last_proof() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    for commit in 2..=(MAX_LOCATORS as u64 + 1) {
        let (_, frames, assigned) = f.append(&mut cell, commit);
        let prepared = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await
            .unwrap();
        f.node = f
            .directory
            .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
            .await
            .unwrap()
            .0;
    }
    let last = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(last.locator_count(), MAX_LOCATORS);
    let (_, frames, assigned) = f.append(&mut cell, MAX_LOCATORS as u64 + 2);
    assert!(matches!(
        f.directory
            .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
            .await,
        Err(Error::PendingPublication)
    ));
    assert_eq!(last.commit_sequence(), MAX_LOCATORS as u64 + 1);
    assert_eq!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .unwrap()
            .commit_sequence(),
        last.commit_sequence()
    );
    f.count.reset();
    let root = f.publisher(&cell).materialize_bundle(&last).await.unwrap();
    eprintln!(
        "bounded suffix materialization: locators={} puts={}",
        MAX_LOCATORS,
        f.count.put_requests()
    );
    assert_eq!(
        f.count.put_requests(),
        4,
        "small retained suffix uses one canonical coalesced pack"
    );
    let restored = f.scratch.path().join("locator-pressure-restored.sqlite");
    cell.replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(restored).unwrap();
    let count: usize = db
        .query_row("SELECT count(*) FROM outcomes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count,
        MAX_LOCATORS + 1,
        "the later unselected command is absent"
    );
}

#[tokio::test]
async fn version_cutover_rejects_erased_and_unknown_authority_fields() {
    let mut f = Fixture::new().await;
    let cell = f.cell(4).await;
    let mut control: serde_json::Value =
        serde_json::from_slice(&cell.control.value().encode().unwrap()).unwrap();
    assert_eq!(control["version"], 2);
    control["version"] = 1.into();
    assert!(Control::decode(&serde_json::to_vec(&control).unwrap()).is_err());
    control["version"] = 2.into();
    control.as_object_mut().unwrap().remove("bundle_binding");
    assert!(Control::decode(&serde_json::to_vec(&control).unwrap()).is_err());
    let bytes = f.node.advertisement().encode().unwrap();
    let mut node: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(node["version"], 2);
    node["version"] = 1.into();
    assert!(NodeAdvertisement::decode_canonical(&serde_json::to_vec(&node).unwrap()).is_err());
    node["version"] = 2.into();
    node["bundle"]["unrecognized"] = 1.into();
    assert!(NodeAdvertisement::decode_canonical(&serde_json::to_vec(&node).unwrap()).is_err());
}
