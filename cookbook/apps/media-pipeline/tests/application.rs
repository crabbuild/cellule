//! Public immutable publication, retained outcomes, pinned runs, and authoritative recovery.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_media_pipeline::{
    Artifacts, Kind, MediaApplication, MediaClient, Request, StartOutcome, Upload, compile, open,
    sample_png,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{ApplicationId, BlobArtifactStore, InvocationError, Resolution, TenantId};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;
async fn node(store: Store, state: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: state.into(),
            storage_prefix: Path::from("media-test/cells"),
            application_id: ApplicationId::from_bytes([0x35; 16]),
        },
    )
    .await
    .unwrap()
}
async fn clients(node: &LocalNode, store: &Store) -> (Artifacts, MediaClient) {
    let artifact_store = BlobArtifactStore::new(Store::new(Arc::new(
        object_store::prefix::PrefixStore::new(store.inner().clone(), "media-test/artifacts"),
    )));
    let handle = node
        .application_handle::<MediaApplication>(TenantId::from_bytes([0x45; 16]))
        .unwrap()
        .with_blob_artifact_store(artifact_store);
    open(node, &handle).await.unwrap();
    (Artifacts::new(handle.clone()), MediaClient::new(handle))
}
fn request(source: cellule_cookbook_media_pipeline::Artifact) -> Request {
    Request {
        id: *uuid::Uuid::now_v7().as_bytes(),
        source,
        side: 128,
        deadline_ms: now_ms().unwrap() + 3_600_000,
        endpoint: "http://127.0.0.1:19021/".into(),
    }
}
#[tokio::test]
async fn immutable_blob_publication_replays_and_reuses_verified_content() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), temporary.path()).await;
    let (artifacts, _) = clients(&node, &store).await;
    let bytes = sample_png().unwrap();
    let key = *blake3::hash(&bytes).as_bytes();
    let plan = Upload::new(Kind::Source, key, bytes.clone()).unwrap();
    assert!(
        artifacts
            .read(Kind::Source, key, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    let original = artifacts.publish(&plan).await.unwrap();
    assert_eq!(artifacts.publish(&plan).await.unwrap(), original);
    assert_eq!(
        artifacts
            .publish(&Upload::new(Kind::Source, key, bytes.clone()).unwrap())
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        artifacts
            .read(Kind::Source, key, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .1,
        bytes
    );
    assert!(
        artifacts
            .read(Kind::Result, key, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn permanent_run_binding_rejects_changed_transform_and_resolves_original() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), temporary.path()).await;
    let (artifacts, client) = clients(&node, &store).await;
    let bytes = sample_png().unwrap();
    let source = artifacts
        .publish(&Upload::new(Kind::Source, *blake3::hash(&bytes).as_bytes(), bytes).unwrap())
        .await
        .unwrap();
    let request = request(source);
    let prepared = client
        .prepare(new_identity().unwrap(), request.clone())
        .await
        .unwrap();
    let original = prepared.clone().execute().await.unwrap();
    assert!(matches!(original.output, StartOutcome::Started(_)));
    assert_eq!(prepared.clone().execute().await.unwrap(), original);
    assert!(matches!(
        client.resolve(prepared.evidence()).await.unwrap(),
        Resolution::Committed(_)
    ));
    assert_eq!(
        client
            .prepare(new_identity().unwrap(), request.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        StartOutcome::AlreadyBound
    );
    let mut changed = request.clone();
    changed.side = 64;
    let changed = client
        .prepare(new_identity().unwrap(), changed)
        .await
        .unwrap();
    assert!(
        matches!(changed.clone().execute().await,Err(InvocationError::Rejected(value))if value.output==StartOutcome::Conflict)
    );
    assert!(
        matches!(changed.clone().execute().await,Err(InvocationError::Rejected(value))if value.output==StartOutcome::Conflict)
    );
    let view = client
        .get(request.id, Some(original.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.state.request, request);
    assert!(view.state.result.is_none());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn output_key_collision_is_verified_and_cannot_replace_existing_png() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), temporary.path()).await;
    let (artifacts, _) = clients(&node, &store).await;
    let original = sample_png().unwrap();
    let key = [0x71; 32];
    let plan = Upload::new(Kind::Result, key, original.clone()).unwrap();
    let metadata = artifacts.publish(&plan).await.unwrap();
    let changed = cellule_cookbook_media_pipeline::thumbnail(&original, 64).unwrap();
    assert!(
        artifacts
            .publish(&Upload::new(Kind::Result, key, changed).unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        artifacts
            .read(Kind::Result, key, None)
            .await
            .unwrap()
            .output
            .unwrap(),
        (metadata, original)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn acknowledged_source_and_queued_workflow_restore_from_authoritative_objects() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let state = temporary.path().join("state");
    let first = node(store.clone(), &state).await;
    let (artifacts, client) = clients(&first, &store).await;
    let bytes = sample_png().unwrap();
    let source = artifacts
        .publish(
            &Upload::new(
                Kind::Source,
                *blake3::hash(&bytes).as_bytes(),
                bytes.clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let request = request(source.clone());
    let prepared = client
        .prepare(new_identity().unwrap(), request.clone())
        .await
        .unwrap();
    let receipt = prepared.clone().execute().await.unwrap().receipt;
    let evidence = prepared.evidence().clone();
    first.shutdown().await.unwrap();
    drop(first);
    let second = node(store.clone(), &state).await;
    let (artifacts, client) = clients(&second, &store).await;
    assert_eq!(
        artifacts
            .read(Kind::Source, source.key, None)
            .await
            .unwrap()
            .output
            .unwrap(),
        (source, bytes)
    );
    let view = client
        .get(request.id, Some(receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.state.request, request);
    assert!(view.state.action.is_some());
    assert!(view.state.result.is_none());
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    second.shutdown().await.unwrap();
}
#[test]
fn bounds_and_local_transport_are_validated_before_io() {
    let bytes = sample_png().unwrap();
    let source = cellule_cookbook_media_pipeline::Artifact {
        key: *blake3::hash(&bytes).as_bytes(),
        digest: *blake3::hash(&bytes).as_bytes(),
        etag: [1; 32],
        bytes: bytes.len() as u64,
        width: 320,
        height: 160,
    };
    let request = request(source);
    assert!(request.validate().is_ok());
    for endpoint in [
        "http://localhost:19021/",
        "https://127.0.0.1:19021/",
        "http://127.0.0.1:19021/?q=1",
        "http://user@127.0.0.1:19021/",
        "http://127.0.0.1:19021/source",
    ] {
        let mut changed = request.clone();
        changed.endpoint = endpoint.into();
        assert!(changed.validate().is_err());
    }
    let mut changed = request.clone();
    changed.side = 0;
    assert!(changed.validate().is_err());
    changed.side = 129;
    assert!(changed.validate().is_err());
    changed.side = 64;
    assert_ne!(changed.output_key(), request.output_key());
    let mut replay = request.clone();
    replay.id = [7; 16];
    replay.deadline_ms += 1;
    assert_eq!(replay.output_key(), request.output_key());
}
#[tokio::test]
async fn elapsed_deadline_records_uncertainty_without_dispatching_an_activity() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), temporary.path()).await;
    let (artifacts, client) = clients(&node, &store).await;
    let bytes = sample_png().unwrap();
    let source = artifacts
        .publish(&Upload::new(Kind::Source, *blake3::hash(&bytes).as_bytes(), bytes).unwrap())
        .await
        .unwrap();
    let mut request = request(source);
    request.deadline_ms = 1;
    let receipt = client
        .prepare(new_identity().unwrap(), request.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap()
        .receipt;
    let view = client
        .get(request.id, Some(receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.status, "completed");
    assert!(view.state.action.is_none());
    assert!(view.state.result.is_none());
    assert!(view.state.failure.unwrap().contains("unknown"));
    node.shutdown().await.unwrap();
}
#[test]
fn admission_outcome_codec_preserves_v1_tags_and_run_bytes() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    let mut encoder = BoundedEncoder::new(64).unwrap();
    StartOutcome::Started([7; 16]).encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut fixture = vec![0, 0, 0, 0, 16];
    fixture.extend([7; 16]);
    assert_eq!(bytes, fixture);
    for (tag, outcome) in [
        (1, StartOutcome::AlreadyBound),
        (2, StartOutcome::Conflict),
        (3, StartOutcome::Capacity),
    ] {
        let encoded = [tag];
        let mut decoder = BoundedDecoder::new(&encoded, 64).unwrap();
        assert_eq!(StartOutcome::decode(&mut decoder).unwrap(), outcome);
        decoder.finish().unwrap();
    }
}
#[tokio::test]
async fn run_resolution_rejects_foreign_blob_evidence_before_dispatch() {
    use cellule_runtime::primitives::blob::BlobMutation;
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), temporary.path()).await;
    let (_, client) = clients(&node, &store).await;
    let handle = node
        .application_handle::<MediaApplication>(TenantId::from_bytes([0x45; 16]))
        .unwrap()
        .with_blob_artifact_store(BlobArtifactStore::new(Store::new(Arc::new(
            object_store::prefix::PrefixStore::new(store.inner().clone(), "media-test/artifacts"),
        ))));
    let source = handle
        .blob::<cellule_cookbook_media_pipeline::Sources>()
        .unwrap()
        .prepare_mutation(
            new_identity().unwrap(),
            BlobMutation::Abort {
                key: vec![9; 32],
                upload_id: [8; 16],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        client.resolve(source.evidence()).await,
        Err(InvocationError::NotStarted(
            cellule_runtime::Error::Identity("foreign media Workflow evidence")
        ))
    ));
    node.shutdown().await.unwrap();
}
