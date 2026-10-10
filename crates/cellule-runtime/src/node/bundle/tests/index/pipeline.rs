use super::*;

#[tokio::test]
async fn unselected_predecessor_currently_blocks_successor_preparation() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, first, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &first, &[assigned], NOW)
        .await
        .unwrap();
    assert_eq!(
        prepared.head.selected_through,
        assigned.ticket().last_sequence()
    );
    let (_, next, assigned) = f.append(&mut cell, 3);
    let error = f
        .directory
        .prepare_node_bundle(&f.node, &next, &[assigned], NOW)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        Error::Node("bundle native range is not contiguous in its lane")
    ));
    assert_ne!(f.node.advertisement().bundle_head(), Some(prepared.head));
}

#[tokio::test]
async fn successor_stages_before_upload_but_requires_ordered_selection_and_full_history() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cells = Vec::new();
    for number in 4..68 {
        cells.push(f.cell(number).await);
        f.heartbeat().await;
    }
    inventory(&mut f, &cells[0], 1_937).await;
    let now = f.heartbeat().await;
    let mut first_frames = Vec::new();
    let mut first_assignments = Vec::new();
    for cell in &mut cells {
        let (_, frames, assignment) = f.append(cell, 2);
        first_frames.extend(frames);
        first_assignments.push(assignment);
    }
    f.count.reset();
    let first = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            None,
            &first_frames,
            &first_assignments,
            &[],
            None,
            Limits::default(),
            now,
        )
        .await
        .unwrap();
    let first_path = f.layout.node_coverage_bundle_path(
        f.node.advertisement().session().as_bytes(),
        first.head.epoch,
        first.head.digest.as_bytes(),
    );
    let mut second_frames = Vec::new();
    let mut second_assignments = Vec::new();
    for cell in &mut cells {
        let (_, frames, assignment) = f.append(cell, 3);
        second_frames.extend(frames);
        second_assignments.push(assignment);
    }
    let second = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            Some(&first),
            &second_frames,
            &second_assignments,
            &[],
            None,
            Limits::default(),
            now,
        )
        .await
        .unwrap();
    assert_eq!(f.count.put_requests(), 0, "neither proposal is uploaded");
    assert!(
        f.count
            .requests()
            .iter()
            .all(|read| read.location != first_path.as_ref())
    );
    assert_eq!(second.original, Some(first.head));
    assert_eq!(
        second.head.selected_through,
        first.head.selected_through + 64
    );
    let original_head = f.node.advertisement().bundle_head();
    let second = f.directory.upload_node_bundle(second).await.unwrap();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &second, &f.lease, Limits::default(), now,)
            .await
            .is_err(),
        "uploaded successor cannot cover an absent predecessor"
    );
    assert_eq!(f.node.advertisement().bundle_head(), original_head);
    assert_eq!(f.count.put_requests(), 1, "no selection CAS escaped");
    let first = f.directory.upload_node_bundle(first).await.unwrap();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &second, &f.lease, Limits::default(), now,)
            .await
            .is_err(),
        "both uploads still do not permit out-of-order selection"
    );
    assert_eq!(f.count.put_requests(), 2);
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &first, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    f.node = selected;
    assert_eq!(proofs.len(), 64);
    assert!(proofs.iter().all(|proof| proof.commit_sequence() == 2));
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &second, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    f.node = selected;
    assert_eq!(proofs.len(), 64);
    assert!(
        proofs
            .iter()
            .all(|proof| proof.commit_sequence() == 3 && proof.locator_count() == 2)
    );
    for proof in &proofs {
        let cell = cells
            .iter()
            .find(|cell| cell.control.value().bundle_binding == Some(proof.binding()))
            .unwrap();
        let overlay = proof
            .recovery_overlay(&f.layout, Limits::default())
            .await
            .unwrap();
        let restored = cell
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let cold = f.scratch.path().join(format!(
            "pipeline-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&restored.root())
            .await
            .unwrap()
            .restore(&cold)
            .await
            .unwrap();
        let db = rusqlite::Connection::open(cold).unwrap();
        let count: u64 = db
            .query_row("SELECT count(*) FROM outcomes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
        for commit in [2, 3] {
            let result: String = db
                .query_row(
                    "SELECT result FROM outcomes WHERE request=?1",
                    [format!("request-{commit}")],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(result, format!("result-{commit}"));
        }
    }
    f.count.block_body_reads_for(&first_path);
    let puts = f.count.put_requests();
    assert!(
        f.directory
            .select_node_bundle(&f.node, &second, &f.lease, Limits::default(), now,)
            .await
            .is_err(),
        "fresh verification must still reject a missing required predecessor"
    );
    assert_eq!(f.count.put_requests(), puts);
}

#[tokio::test]
async fn intervening_catalog_change_rejects_both_staged_cohorts() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, first_frames, first_range) = f.append(&mut cell, 2);
    let first = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            None,
            &first_frames,
            &[first_range],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (_, second_frames, second_range) = f.append(&mut cell, 3);
    let second = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            Some(&first),
            &second_frames,
            &[second_range],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let _sibling = f.cell(5).await;
    let original = f.node.advertisement().bundle_head();
    let first = f.directory.upload_node_bundle(first).await.unwrap();
    let second = f.directory.upload_node_bundle(second).await.unwrap();
    let puts = f.count.put_requests();
    for staged in [&first, &second] {
        assert!(matches!(
            f.directory
                .select_node_bundle(&f.node, staged, &f.lease, Limits::default(), NOW)
                .await,
            Err(Error::Fenced)
        ));
    }
    assert_eq!(f.count.put_requests(), puts);
    assert_eq!(f.node.advertisement().bundle_head(), original);
}

#[tokio::test]
async fn fenced_writer_cannot_credit_late_staged_uploads() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, first_frames, first_range) = f.append(&mut cell, 2);
    let first = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            None,
            &first_frames,
            &[first_range],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (_, second_frames, second_range) = f.append(&mut cell, 3);
    let second = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            Some(&first),
            &second_frames,
            &[second_range],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let original = f.node.advertisement().bundle_head();
    f.lease.fence();
    let (first, second) = tokio::try_join!(
        f.directory.upload_node_bundle(first),
        f.directory.upload_node_bundle(second)
    )
    .unwrap();
    let puts = f.count.put_requests();
    for staged in [&first, &second] {
        assert!(matches!(
            f.directory
                .select_node_bundle(&f.node, staged, &f.lease, Limits::default(), NOW)
                .await,
            Err(Error::Fenced)
        ));
    }
    assert_eq!(f.count.put_requests(), puts);
    assert_eq!(f.node.advertisement().bundle_head(), original);
}

use futures_util::stream::BoxStream;
use object_store::ObjectStore;
use std::sync::atomic::{AtomicBool, Ordering};
#[derive(Debug, Default)]
struct PausedBundleStore {
    inner: InMemory,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl std::fmt::Display for PausedBundleStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausedBundleStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PausedBundleStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if path.as_ref().ends_with(".cnb") && self.armed.swap(false, Ordering::AcqRel) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn later_upload_can_finish_while_first_upload_is_held_without_early_coverage() {
    let store = Arc::new(PausedBundleStore::default());
    let mut f = Fixture::with_store(store.clone()).await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, first_frames, first_range) = f.append(&mut cell, 2);
    let first = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            None,
            &first_frames,
            &[first_range],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (_, second_frames, second_range) = f.append(&mut cell, 3);
    let original = f.node.advertisement().bundle_head();
    store.armed.store(true, Ordering::Release);
    let first_upload = f.directory.upload_node_bundle(Arc::clone(&first));
    tokio::pin!(first_upload);
    tokio::select! {
        _ = store.entered.notified() => {},
        result = &mut first_upload => panic!("first PUT should be held: {}", result.is_ok()),
    }
    let second = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            Some(&first),
            &second_frames,
            &[second_range],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let second = f.directory.upload_node_bundle(second).await.unwrap();
    assert_eq!(f.gate.tiered_through(), 0);
    assert!(
        f.directory
            .select_node_bundle(&f.node, &second, &f.lease, Limits::default(), NOW)
            .await
            .is_err()
    );
    assert_eq!(f.node.advertisement().bundle_head(), original);
    assert_eq!(f.gate.tiered_through(), 0);
    store.resume.notify_one();
    let first = first_upload.await.unwrap();
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &first, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs).unwrap(),
        first_range.ticket().last_sequence()
    );
    let (node, proofs) = f
        .directory
        .select_node_bundle(&f.node, &second, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = node;
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs).unwrap(),
        second_range.ticket().last_sequence()
    );
    assert_eq!(f.gate.tiered_through(), 2);
}

#[tokio::test]
async fn a_third_unselected_cohort_cannot_extend_the_two_stage_window() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let first = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            None,
            &frames,
            &[assigned],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 3);
    let second = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            Some(&first),
            &frames,
            &[assigned],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    let (_, frames, assigned) = f.append(&mut cell, 4);
    f.count.reset();
    assert!(matches!(
        f.directory
            .stage_node_bundle_after(
                &f.node,
                Some(&second),
                &frames,
                &[assigned],
                &[],
                None,
                Limits::default(),
                NOW,
            )
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(f.count.put_requests(), 0);
    assert_eq!(f.count.requests().len(), 0, "reject before metadata I/O");
    assert_eq!(f.gate.tiered_through(), 0);
}

#[tokio::test]
async fn foreign_session_proposal_cannot_supply_predecessor_metadata() {
    let mut f = Fixture::new().await;
    super::super::coverage::enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let mut first = f
        .directory
        .stage_node_bundle_after(
            &f.node,
            None,
            &frames,
            &[assigned],
            &[],
            None,
            Limits::default(),
            NOW,
        )
        .await
        .unwrap();
    // Private test corruption simulates an otherwise exact-looking proposal
    // from another owner. No public API can alter the encoded proposal.
    Arc::get_mut(&mut first).unwrap().catalog.session = SessionId::from_bytes([8; 16]);
    let (_, frames, assigned) = f.append(&mut cell, 3);
    f.count.reset();
    assert!(matches!(
        f.directory
            .stage_node_bundle_after(
                &f.node,
                Some(&first),
                &frames,
                &[assigned],
                &[],
                None,
                Limits::default(),
                NOW,
            )
            .await,
        Err(Error::Fenced)
    ));
    assert_eq!(f.count.requests().len(), 0);
    assert_eq!(f.count.put_requests(), 0);
}
