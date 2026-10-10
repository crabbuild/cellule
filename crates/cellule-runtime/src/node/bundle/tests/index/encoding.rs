use super::*;

struct FixedClock;

impl cellule_ltx::environment::Clock for FixedClock {
    fn unix_millis(&self) -> i64 {
        NOW
    }

    fn file_age(&self, _: &std::path::Path) -> std::io::Result<std::time::Duration> {
        Ok(std::time::Duration::ZERO)
    }
}

#[tokio::test]
async fn dense_multi_cell_encoding_preserves_the_original_native_order_and_bytes() {
    let mut f = Fixture::with_capture_host(
        Arc::new(InMemory::new()),
        cellule_ltx::Host::default().with_clock(Arc::new(FixedClock)),
    )
    .await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..68 {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    // Populate the real authenticated index with unrelated neighbors. Their
    // original roots are deliberately unavailable: encoding may retain their
    // references, but selecting this cohort must verify only its own bases.
    inventory(&mut f, &cells[0], 1_937).await;
    let original_sizes = [504_123, 511_931, 518_715, 525_755, 532_603, 539_643];
    let mut latest = Vec::new();
    for commit in 2..=7 {
        let now = f.heartbeat().await;
        let mut frames = Vec::new();
        let mut assignments = Vec::new();
        for cell in &mut cells {
            let (_, capture, assignment) = f.append(cell, commit);
            frames.extend(capture);
            assignments.push(assignment);
        }
        let proposal = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &assignments, now)
            .await
            .unwrap();
        assert_eq!(proposal.body.len(), original_sizes[(commit - 2) as usize]);
        // Compare this exact capture with the pre-optimization encoder. Native
        // roots include generated identities, so a stored digest is not a
        // stable cross-run wire fixture. Both encoders must agree byte for byte
        // over the same 64-Cell cohort and its unrelated catalog siblings.
        let mut original_catalog = proposal.catalog.clone();
        let (original_body, original_digest) =
            catalog_index::encode_original(&mut original_catalog, &frames).unwrap();
        assert_eq!(proposal.body, original_body);
        assert_eq!(proposal.head.digest, original_digest);
        assert_eq!(proposal.catalog, original_catalog);
        let mut next_offset = None;
        for frame in &frames {
            let locators: Vec<_> = proposal
                .catalog
                .bindings
                .iter()
                .flat_map(|binding| &binding.locators)
                .filter(|locator| {
                    locator.object.is_none() && locator.frame_digest.as_bytes() == &frame.digest()
                })
                .collect();
            assert_eq!(locators.len(), 1);
            let locator = locators[0];
            assert_eq!(*next_offset.get_or_insert(locator.offset), locator.offset);
            let end = locator.offset + locator.bytes;
            assert_eq!(
                &proposal.body[locator.offset as usize..end as usize],
                frame.encoded().as_ref(),
            );
            next_offset = Some(end);
        }
        let (selected, proofs) = f
            .directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), now)
            .await
            .unwrap();
        f.node = selected;
        assert_eq!(proofs.len(), cells.len());
        latest = proofs;
    }
    for proof in &latest {
        assert_eq!(proof.commit_sequence(), 7);
        assert_eq!(proof.locator_count(), 6);
        let cell = cells
            .iter()
            .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
            .unwrap();
        let overlay = proof
            .recovery_overlay(&f.layout, Limits::default())
            .await
            .unwrap();
        let recovered = cell
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let cold = f.scratch.path().join(format!(
            "encoded-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&recovered.root())
            .await
            .unwrap()
            .restore(&cold)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(cold).unwrap();
        let outcomes: Vec<(String, String)> = db
            .prepare("SELECT request, result FROM outcomes ORDER BY request")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let mut expected: Vec<_> = (2..=7)
            .map(|commit| (format!("request-{commit}"), format!("result-{commit}")))
            .collect();
        expected.push(("seed".into(), "original".into()));
        expected.sort();
        assert_eq!(outcomes, expected);
    }
}
