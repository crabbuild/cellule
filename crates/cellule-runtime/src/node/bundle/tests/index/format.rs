use super::*;

#[tokio::test]
async fn node_bundle_v1_uses_the_named_format_and_exact_object_path() {
    let mut f = Fixture::new().await;
    let mut cell = f.cell(4).await;
    let (_, frames, assigned) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assigned], NOW)
        .await
        .unwrap();
    assert!(prepared.body.starts_with(b"cellule.node-bundle.v1\0"));
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, prepared.head.digest.as_bytes());
    assert!(path.as_ref().contains("/coverage/node-bundle/v1/"));
    assert!(path.as_ref().ends_with(".nodebundle"));
    f.node = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap()
        .0;
    let recovered = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert_eq!(recovered.commit_sequence(), 2);
    assert_eq!(
        recovered.position(),
        frames.last().unwrap().segment().position()
    );
}

#[tokio::test]
async fn point_recovery_does_not_search_the_previous_object_path() {
    let mut f = Fixture::new().await;
    let cell = f.cell(4).await;
    let head = f.node.advertisement().bundle_head().unwrap();
    let path = f
        .layout
        .node_coverage_bundle_path(&[1; 16], EPOCH, head.digest.as_bytes());
    let previous = Path::from(
        path.as_ref()
            .replace("/coverage/node-bundle/v1/", "/coverage/v1/")
            .replace(".nodebundle", ".cnb"),
    );
    assert_ne!(path, previous);
    let (body, _) = f
        .layout
        .store()
        .get_with_etag_bounded(&path, MAX_BUNDLE_BYTES)
        .await
        .unwrap();
    // Even supported bytes at the previous path cannot satisfy the pinned head.
    f.layout.store().put_exact(&previous, body).await.unwrap();
    f.layout.store().delete(&path).await.unwrap();
    f.count.reset();
    assert!(
        f.directory
            .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
            .await
            .is_err()
    );
    let reads = f.count.requests();
    assert!(reads.iter().any(|read| read.location == path.as_ref()));
    assert!(reads.iter().all(|read| read.location != previous.as_ref()));
}

#[tokio::test]
async fn previous_format_identifiers_fail_point_recovery_and_maintenance_without_fallback() {
    for magic in [b"\0\0\0\x04CNB1", b"\0\0\0\x04CNB2", b"\0\0\0\x04CNB3"] {
        let mut f = Fixture::new().await;
        let cell = f.cell(4).await;
        let original = f.node.advertisement().bundle_head().unwrap();
        let mut body = vec![0; catalog_index::HEADER_BYTES];
        body[..magic.len()].copy_from_slice(magic);
        let head = NodeBundleHead {
            epoch: EPOCH,
            digest: Digest::from_bytes(*blake3::hash(&body).as_bytes()),
            selected_through: original.selected_through(),
        };
        assert!(catalog_index::body_digest(&Bytes::from(body.clone())).is_err());
        f.layout
            .store()
            .put_exact(
                &f.layout
                    .node_coverage_bundle_path(&[1; 16], EPOCH, head.digest.as_bytes()),
                Bytes::from(body),
            )
            .await
            .unwrap();
        // Simulate an authority-pinned old head. No new writer can select these
        // bytes, but recovery must also refuse them without a compatibility GET.
        let mut advertisement = f.node.advertisement().clone();
        advertisement.bundle = Some(head);
        advertisement.generation += 1;
        f.node = f
            .directory
            .update_advertisement(&f.node, advertisement, NOW)
            .await
            .unwrap();
        f.count.reset();
        assert!(matches!(
            f.directory
                .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
                .await,
            Err(Error::Node("unsupported bundle index header"))
        ));
        assert_eq!(
            f.count
                .requests()
                .iter()
                .filter(|read| read.location.ends_with(".nodebundle"))
                .count(),
            1
        );
        f.count.reset();
        assert!(matches!(
            catalog_index::ensure_drained(&f.layout, head_session(&f), head).await,
            Err(Error::Node("unsupported bundle index header"))
        ));
        assert_eq!(f.count.requests().len(), 1);
        f.count.reset();
        assert!(matches!(
            catalog_index::binding_inventory(&f.layout, head_session(&f), head).await,
            Err(Error::Node("unsupported bundle index header"))
        ));
        assert_eq!(f.count.requests().len(), 1);
    }
}
