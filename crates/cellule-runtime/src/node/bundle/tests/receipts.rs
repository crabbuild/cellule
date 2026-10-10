use super::*;
use crate::cell::worker::SqlWorkerPool;
use crate::node::durability::NodeDurability;
use crate::node::log_shipper::{NodeLogShipper, NodeLogSubmission};

#[tokio::test]
async fn selection_receipts_require_complete_cohort_and_admission_and_share_one_cell_charge() {
    let mut f = Fixture::new().await;
    super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let transport = super::actor::transport(&f, true);
    let authority = Arc::new(super::actor::Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(f.node.clone()),
    });
    let shipper =
        NodeLogShipper::new(f.gate.clone(), transport.clone(), Limits::default()).unwrap();
    let durability = NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        transport,
        f.lease.clone(),
    );
    let mut feed = durability.take_publication_feed().unwrap();
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(64).unwrap();
    durability
        .attach_selection_resources(pool.resource_ledger())
        .unwrap();
    let mut captures = Vec::new();
    let mut submitted = Vec::new();
    let mut cuts_by_commit = Vec::new();
    for commit in [2, 3] {
        cell.db
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO outcomes VALUES(?1,?2)",
                    [format!("request-{commit}"), format!("result-{commit}")],
                )
            })
            .unwrap();
        let cuts = cell.db.capture().unwrap();
        cuts_by_commit.push(cuts.clone());
        submitted.push(
            durability
                .submit_capture(
                    NodeLogSubmission::new(
                        ApplicationId::from_bytes([9; 16]),
                        cell.control.value().cell,
                        cell.control.value().incarnation,
                        cell.control.value().epoch,
                        commit,
                        &cuts,
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
        );
        captures.push(feed.recv().await.unwrap());
    }
    let frames = captures
        .iter()
        .flat_map(|capture| capture.frames().iter().cloned())
        .collect::<Vec<_>>();
    let assignments = captures
        .iter()
        .map(|capture| capture.assignment())
        .collect::<Vec<_>>();
    let scope = assignments[0].scope();
    assert!(assignments[0].matches_capture(scope.cell, scope.incarnation, 2, &cuts_by_commit[0]));
    assert!(!assignments[0].matches_capture(scope.cell, scope.incarnation, 3, &cuts_by_commit[1]));
    let mut altered = cuts_by_commit[0].clone();
    let original = &altered.segments[0];
    let mut metadata = original.info().clone();
    metadata.blake3[0] ^= 1;
    altered.segments[0] = cellule_ltx::LocalSegment::new(original.path().to_owned(), metadata);
    assert!(!assignments[0].matches_capture(scope.cell, scope.incarnation, 2, &altered));
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &assignments, NOW)
        .await
        .unwrap();
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = selected;
    assert_eq!(proofs.len(), 1);
    assert!(matches!(
        durability.confirm_selected_captures(&captures[..1], proofs),
        Err(Error::Node("selection omits original captured assignments"))
    ));
    assert_eq!(f.gate.tiered_through(), 0);
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert!(matches!(
        durability.confirm_selected_captures(&captures, proofs),
        Err(Error::Capacity(_))
    ));
    assert_eq!(f.gate.tiered_through(), 0);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    let cold = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert!(matches!(
        durability.confirm_selected_captures(&captures, vec![cold]),
        Err(Error::Node("selection omits original captured assignments"))
    ));
    pool.configure_retained_capacity(1 << 20).unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let expected = proofs[0].retained_metadata_bytes().unwrap();
    let publication = durability
        .confirm_selected_captures(&captures, proofs)
        .unwrap();
    assert_eq!(publication.selected_through(), 2);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        expected
    );
    let first = submitted[0]
        .selection
        .as_ref()
        .unwrap()
        .selected()
        .await
        .unwrap();
    let second = submitted[1]
        .selection
        .as_ref()
        .unwrap()
        .selected()
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(first.proof.contains_assignment(&submitted[0].assignment));
    assert!(second.proof.contains_assignment(&submitted[1].assignment));
    let prefix = durability
        .capture_prefix(Arc::clone(&first), &submitted[0].assignment)
        .unwrap();
    assert_eq!(prefix.proof.commit_sequence(), 2);
    assert_eq!(prefix.proof.position(), cuts_by_commit[0].position);
    assert_eq!(
        prefix.proof.locator_count(),
        cuts_by_commit[0].segments.len()
    );
    assert!(prefix.proof.contains_assignment(&submitted[0].assignment));
    assert!(!prefix.proof.contains_assignment(&submitted[1].assignment));
    let first_root = f
        .publisher(&cell)
        .materialize_bundle(&prefix.proof)
        .await
        .unwrap();
    assert_eq!(first_root.commit_sequence, 2);
    cell.control = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    drop(prefix);
    drop(first);
    drop(second);
    drop(publication);
    drop(captures);
    drop(submitted);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    let proof = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    let root = f.publisher(&cell).materialize_bundle(&proof).await.unwrap();
    let current = cell
        .authority
        .load(cell.control.value().cell)
        .await
        .unwrap()
        .unwrap();
    f.node = f
        .directory
        .checkpoint_bundle_cell(&f.node, &cell.authority, &proof, Limits::default(), NOW)
        .await
        .unwrap();
    let issued = f
        .gate
        .close_cell_issuance(Fixture::scope(&cell), root)
        .unwrap();
    f.node = f
        .directory
        .begin_bundle_close(&f.node, proof.binding(), issued, NOW)
        .await
        .unwrap();
    f.node = f
        .directory
        .finish_bundle_close(&f.node, proof.binding(), issued, NOW)
        .await
        .unwrap();
    assert_eq!(current.value().ltx_root(), Some(root));
    *authority.observed.lock().await = f.node;
    durability.shutdown().await.unwrap();
    assert!(feed.recv().await.is_none());
    pool.shutdown().await.unwrap();
}
