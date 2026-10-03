//! Native staged Blob visibility, retained phase evidence, immutable content, and cold restoration.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_release_pipeline::*;
use cellule_cookbook_support::{LocalNode, NodeConfig};
use cellule_runtime::{ApplicationId, BlobArtifactStore, Resolution, TenantId};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;
fn id(v: u128) -> ReleaseId {
    ReleaseId::from_bytes(v.to_be_bytes()).unwrap()
}
async fn open(
    store: Store,
    parts: Store,
    path: &std::path::Path,
) -> (LocalNode, ReleaseArtifacts<2>, ReleaseClient<2>) {
    let node = LocalNode::start(
        compile_release::<2>().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("release-artifact-test"),
            application_id: ApplicationId::from_bytes([0x99; 16]),
        },
    )
    .await
    .unwrap();
    let h = node
        .application_handle::<ReleaseApplication<2>>(TenantId::from_bytes([0x9a; 16]))
        .unwrap()
        .with_blob_artifact_store(BlobArtifactStore::new(parts));
    let c = open_release(&node, &h).await.unwrap();
    (node, ReleaseArtifacts::new(h).unwrap(), c)
}
#[test]
fn frozen_artifact_plan_rejects_changed_content_identity_and_window() {
    let plan = ArtifactUpload::new(id(1), b"source").unwrap();
    let roundtrip: ArtifactUpload =
        serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
    assert_eq!(plan, roundtrip);
    let mut bad = plan.clone();
    bad.version = 2;
    assert!(bad.validate().is_err());
    let mut bad = plan.clone();
    bad.bytes[36] ^= 1;
    assert!(bad.validate().is_err());
    let mut bad = plan.clone();
    bad.phases[1] = bad.phases[0].clone();
    assert!(bad.validate().is_err());
    let mut bad = plan.clone();
    bad.phases[2].expires_at_ms += 1;
    assert!(bad.validate().is_err());
    let mut bad = plan.clone();
    bad.upload_expires_ms += 1;
    assert!(bad.validate().is_err());
    let mut bad = plan.clone();
    bad.release = id(2);
    assert!(bad.validate().is_err());
    assert!(ArtifactUpload::new(id(1), &[]).is_err());
    assert!(ArtifactUpload::new(id(1), &vec![0xff; MAX_SOURCE_BYTES + 1]).is_err());
}
#[tokio::test]
async fn staged_parts_remain_invisible_and_original_completion_receipt_survives_cold_restore() {
    let root = tempfile::tempdir().unwrap();
    let local = root.path().join("working");
    let store = Store::new(Arc::new(InMemory::new()));
    let parts = Store::new(Arc::new(InMemory::new()));
    let (node, a, c) = open(store.clone(), parts.clone(), &local).await;
    let plan = ArtifactUpload::new(id(1), &vec![0xff; MAX_SOURCE_BYTES]).unwrap();
    assert!(
        a.read(plan.artifact.key, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    let begin = a.prepare(&plan, ArtifactPhase::Begin).await.unwrap();
    let first = begin.clone().execute().await.unwrap();
    assert_eq!(begin.execute().await.unwrap().receipt, first.receipt);
    let part = a.prepare(&plan, ArtifactPhase::Part).await.unwrap();
    let staged = part.execute().await.unwrap();
    assert!(
        a.read(plan.artifact.key, Some(staged.receipt))
            .await
            .unwrap()
            .output
            .is_none()
    );
    let complete = a.prepare(&plan, ArtifactPhase::Complete).await.unwrap();
    let evidence = complete.evidence().clone();
    assert!(c.resolve(&evidence).await.is_err());
    let committed = complete.clone().execute().await.unwrap();
    assert_eq!(committed.receipt, complete.execute().await.unwrap().receipt);
    let object = a
        .read(plan.artifact.key, Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(object.bytes, plan.bytes);
    assert_eq!(object.publication.artifact, plan.artifact);
    assert_eq!(
        a.publish(&plan).await.unwrap().publication,
        object.publication
    );
    node.shutdown().await.unwrap();
    drop(a);
    drop(c);
    drop(node);
    std::fs::remove_dir_all(&local).unwrap();
    let (node, a, _c) = open(store, parts, &local).await;
    let restored = a
        .read(plan.artifact.key, Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(restored.bytes, object.bytes);
    assert_eq!(restored.publication, object.publication);
    assert!(
        matches!(a.resolve(&evidence).await.unwrap(),Resolution::Committed(v) if v.commit_sequence()==committed.receipt.commit_sequence)
    );
    assert_eq!(
        a.publish(&plan).await.unwrap().publication,
        object.publication
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn concurrent_identical_builds_converge_on_one_verified_native_manifest() {
    let root = tempfile::tempdir().unwrap();
    let (node, a, _) = open(
        Store::new(Arc::new(InMemory::new())),
        Store::new(Arc::new(InMemory::new())),
        root.path(),
    )
    .await;
    let left = ArtifactUpload::new(id(1), b"same").unwrap();
    let right = ArtifactUpload::new(id(1), b"same").unwrap();
    assert_ne!(left.upload, right.upload);
    let (l, r) = tokio::join!(a.publish(&left), a.publish(&right));
    let l = l.unwrap();
    let r = r.unwrap();
    assert_eq!(l.publication, r.publication);
    assert_eq!(l.bytes, r.bytes);
    let changed = ArtifactUpload::new(id(1), b"different").unwrap();
    assert_ne!(changed.artifact.key, left.artifact.key);
    a.publish(&changed).await.unwrap();
    assert_eq!(
        a.read(left.artifact.key, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .bytes,
        left.bytes
    );
    node.shutdown().await.unwrap();
}
