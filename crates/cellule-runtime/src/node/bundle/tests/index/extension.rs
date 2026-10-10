//! Live prefix induction reads only new bytes; cold or mismatched facts cannot.
use super::*;

#[tokio::test]
async fn live_extension_reads_only_new_origin_and_cold_recovery_checks_the_whole_chain() {
    extend(0).await;
}

#[tokio::test]
async fn cold_prefix_cannot_skip_fresh_dependencies() {
    extend(1).await;
}

#[tokio::test]
async fn another_lease_cannot_reuse_a_live_prefix() {
    extend(2).await;
}

#[tokio::test]
async fn checkpoint_base_change_requires_fresh_dependency_verification() {
    extend(3).await;
}

#[tokio::test]
async fn an_unseen_intermediate_capture_requires_fresh_history_verification() {
    extend(4).await;
}

#[tokio::test]
async fn changed_limits_require_fresh_dependency_verification() {
    extend(5).await;
}

#[tokio::test]
async fn another_store_identity_cannot_reuse_a_live_prefix() {
    extend(6).await;
}

async fn extend(mode: u8) {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let (node, mut proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    let mut previous = proofs.remove(0);
    if mode == 1 {
        previous = f
            .directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .unwrap();
    }
    if mode == 3 {
        f.publisher(&cell)
            .materialize_bundle(&previous)
            .await
            .unwrap();
        f.node = f
            .directory
            .checkpoint_bundle_cell(&f.node, &cell.authority, &previous, Limits::default(), NOW)
            .await
            .unwrap();
        cell.control = cell
            .authority
            .load(cell.control.value().cell)
            .await
            .unwrap()
            .unwrap();
    }
    let lease = if mode == 2 {
        NodeLeaseGuard::new(NOW, NOW + 60_000).unwrap()
    } else {
        f.lease.clone()
    };
    let commit = if mode == 4 {
        let (_, frames, assignment) = f.append(&mut cell, 3);
        let prepared = f
            .directory
            .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
            .await
            .unwrap();
        f.node = f
            .directory
            .select_node_bundle(&f.node, &prepared, &lease, Limits::default(), NOW)
            .await
            .unwrap()
            .0;
        4
    } else {
        3
    };
    let limits = if mode == 5 {
        Limits {
            max_plan_bytes: 512 << 20,
            ..Limits::default()
        }
    } else {
        Limits::default()
    };
    let (_, frames, assignment) = f.append(&mut cell, commit);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let directory = if mode == 6 {
        NodeDirectory::new(
            CellStorageLayout::new(
                Store::new(f.count.clone()),
                Path::from("bundle-test"),
                [9; 16],
            ),
            Digest::from_bytes([6; 32]),
            Digest::from_bytes([7; 32]),
            Digest::from_bytes([8; 32]),
        )
    } else {
        f.directory.clone()
    };
    let path = f.layout.node_coverage_bundle_path(
        prepared.catalog.session.as_bytes(),
        prepared.head.epoch,
        prepared.head.digest.as_bytes(),
    );
    f.count.reset();
    f.count.block_body_reads_for(&path);
    assert!(
        directory
            .select_node_bundle_extending(
                &f.node,
                &prepared,
                &lease,
                &[&previous],
                Limits::default(),
                NOW,
            )
            .await
            .is_err(),
        "prior proof cannot substitute for new origin bytes"
    );
    assert_eq!(f.count.put_requests(), 0);
    f.count.unblock_body_reads_for(&path);
    f.count.reset();
    let (node, proofs) = directory
        .select_node_bundle_extending(&f.node, &prepared, &lease, &[&previous], limits, NOW)
        .await
        .unwrap();
    if mode == 0 {
        assert_eq!(
            f.count.requests().len(),
            1,
            "live continuation reread old dependencies"
        );
    } else {
        assert!(
            f.count.requests().len() > 1,
            "unmatched prefix bypassed fresh verification"
        );
    }
    assert_eq!(f.count.put_requests(), 1);
    f.node = node;
    let proof = &proofs[0];
    assert_eq!(proof.commit_sequence(), commit);
    let original = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(original.binding, proof.binding);

    // A live induction supplies no cached availability to restoration. Deleting
    // a required extent still fails the canonical cold/recovery path.
    let locator = &proof.binding.locators[0];
    let old_path = f.layout.node_coverage_bundle_path(
        proof.session.as_bytes(),
        proof.head.epoch,
        locator.object.unwrap().as_bytes(),
    );
    f.count.block_body_reads_for(&old_path);
    assert!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .is_err()
    );
    f.count.unblock_body_reads_for(&old_path);
    let root = f.publisher(&cell).materialize_bundle(proof).await.unwrap();
    let destination = f.scratch.path().join("extended.sqlite");
    cell.replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&destination)
        .await
        .unwrap();
    let db = rusqlite::Connection::open(destination).unwrap();
    let outcomes: Vec<(String, String)> = db
        .prepare("SELECT request, result FROM outcomes ORDER BY request")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let mut expected = (2..=commit)
        .map(|n| (format!("request-{n}"), format!("result-{n}")))
        .collect::<Vec<_>>();
    expected.push(("seed".into(), "original".into()));
    assert_eq!(outcomes, expected);
    lease.fence();
    f.count.reset();
    assert!(matches!(
        directory
            .select_node_bundle_extending(&f.node, &prepared, &lease, &[&previous], limits, NOW,)
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(f.count.put_requests(), 0);
}
