use super::*;

#[tokio::test]
async fn dense_history_retains_215_exact_commands_before_checkpoint_and_cold_restores() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    inventory(&mut f, &cell, 2_000).await;
    let mut latest = None;
    let mut segments = Vec::new();
    let mut metadata_bytes = 0;
    for commit in 2..=216 {
        let (cuts, frames, assignment) = f.append(&mut cell, commit);
        segments.extend(cuts.segments);
        let proposal = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
            .await
            .unwrap();
        metadata_bytes = proposal.body.len()
            - frames
                .iter()
                .map(|frame| frame.encoded().len())
                .sum::<usize>();
        let (selected, mut proofs) = f
            .directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .unwrap();
        f.node = selected;
        latest = proofs.pop();
    }
    let proof = latest.unwrap();
    assert_eq!(proof.commit_sequence(), 216);
    assert_eq!(proof.locator_count(), 215);
    // A following local command is deliberately outside the selected endpoint.
    let _unselected = f.append(&mut cell, 217);
    f.count.reset();
    let root = f.publisher(&cell).materialize_bundle(&proof).await.unwrap();
    let puts = f.count.put_requests();
    eprintln!(
        "dense checkpoint: catalog_bindings=2000 commands=215 materialization_puts={puts} selection_metadata_bytes={metadata_bytes}"
    );
    assert_eq!(puts, 4, "the small-tail cost must be measured, not assumed");
    let direct = cell
        .replica
        .prepare(
            Some(&proof.base().unwrap()),
            &cellule_ltx::CaptureBatch {
                segments,
                position: proof.position(),
                timing: Default::default(),
            },
            216,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        root,
        direct.root(),
        "same canonical root digest and byte image as original native captures"
    );
    let restored = f.scratch.path().join("dense-cold.sqlite");
    cell.replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(&restored).unwrap();
    let count: u64 = db
        .query_row("SELECT count(*) FROM outcomes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count, 216,
        "seed plus every selected command, no later command"
    );
    let outcomes: Vec<(String, String)> = db
        .prepare("SELECT request, result FROM outcomes ORDER BY request")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let mut expected: Vec<_> = (2..=216)
        .map(|commit| (format!("request-{commit}"), format!("result-{commit}")))
        .collect();
    expected.push(("seed".into(), "original".into()));
    expected.sort();
    assert_eq!(outcomes, expected);
    f.directory
        .checkpoint_bundle_cell(&f.node, &cell.authority, &proof, Limits::default(), NOW)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_point_update_neither_fetches_nor_rewrites_a_siblings_detached_history() {
    let mut f = Fixture::new().await;
    let mut a = f.cell(4).await;
    let wanted = catalog_index::shard(&[9; 16], &[4; 32]);
    let application = (0_u64..10_000)
        .map(|number| {
            let mut application = [0; 16];
            application[..8].copy_from_slice(&number.to_le_bytes());
            application
        })
        .find(|application| catalog_index::shard(application, &[4; 32]) == wanted)
        .unwrap();
    let mut b = f.cell_for_application(4, application).await;
    for cell in [&mut a, &mut b] {
        let (_, frames, assignment) = f.append(cell, 2);
        let proposal = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
            .await
            .unwrap();
        f.node = f
            .directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .unwrap()
            .0;
    }
    let head = f.node.advertisement().bundle_head().unwrap();
    let cells = [([9; 16], [4; 32])].into_iter().collect();
    let catalog = store::load_catalog_cells(&f.layout, head_session(&f), head, &cells)
        .await
        .unwrap();
    let history =
        catalog_index::history_extent(&catalog, b.control.value().bundle_binding.unwrap().digest)
            .unwrap();
    let object = history.object.unwrap();
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, object.as_bytes());
    let (body, _) = f
        .layout
        .store()
        .get_with_etag_bounded(&path, MAX_BUNDLE_BYTES)
        .await
        .unwrap();
    let mut corrupt = body.to_vec();
    corrupt[history.offset as usize + 8] ^= 1;
    f.layout
        .store()
        .put_overwrite(&path, Bytes::from(corrupt))
        .await
        .unwrap();
    assert!(
        f.directory
            .load_bundle_coverage(&b.authority, &b.control, Limits::default())
            .await
            .is_err()
    );
    f.count.reset();
    let (_, frames, assignment) = f.append(&mut a, 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(proofs.len(), 1);
    assert_eq!(
        proofs[0].binding(),
        a.control.value().bundle_binding.unwrap()
    );
    assert_eq!(object, head.digest());
    assert_eq!(
        f.count
            .requests()
            .iter()
            .filter(|read| read.location == path.as_ref())
            .count(),
        2,
        "only the previous header and shared shard are read; no sibling history"
    );
    let selected_catalog = store::load_catalog_cells(
        &f.layout,
        head_session(&f),
        selected.advertisement().bundle_head().unwrap(),
        &cells,
    )
    .await
    .unwrap();
    assert_eq!(
        catalog_index::history_extent(
            &selected_catalog,
            b.control.value().bundle_binding.unwrap().digest
        ),
        Some(history)
    );
    // Complete reconstruction inventory still refuses the corrupt sibling;
    // point selection grants no cross-Cell collection or drain authority.
    assert!(
        load_catalog(
            &f.layout,
            head_session(&f),
            selected.advertisement().bundle_head().unwrap()
        )
        .await
        .is_err()
    );
}
