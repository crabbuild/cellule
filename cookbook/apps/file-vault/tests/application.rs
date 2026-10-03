//! Public multipart resumption, publication conditions, bounds, and source verification.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cellule_cookbook_file_vault::{
    FileClient, FileKey, FileVault, Files, PART_BYTES, Publication, RetainedUpload, Write, compile,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, BlobArtifactStore, Committed, InvocationError, Resolution, TenantId,
    primitives::blob::BlobMutationOutcome,
};
use cellule_store::Store;
use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
use std::{path::Path as FsPath, sync::Arc};

async fn start(store: Store, root: &FsPath) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("test-vault"),
            application_id: ApplicationId::from_bytes([0x53; 16]),
        },
    )
    .await
    .unwrap()
}
async fn file(node: &LocalNode, store: Store, key: &str) -> FileClient {
    let client = FileClient::new(
        node.application_handle::<FileVault>(TenantId::from_bytes([0x63; 16]))
            .unwrap()
            .with_blob_artifact_store(BlobArtifactStore::new(store)),
        FileKey::new(key).unwrap(),
    )
    .unwrap();
    node.open_cell(client.target(), &Files).await.unwrap();
    client
}
async fn begin(client: &FileClient, publication: Publication) -> [u8; 16] {
    let upload = *uuid::Uuid::now_v7().as_bytes();
    client
        .write(
            new_identity().unwrap(),
            Write::Begin {
                upload,
                publication,
                expires_at_ms: now_ms().unwrap() + 3_600_000,
            },
        )
        .await
        .unwrap();
    upload
}
async fn stage(client: &FileClient, upload: [u8; 16], content: &[u8]) {
    for (index, bytes) in content.chunks(PART_BYTES).enumerate() {
        client
            .write(
                new_identity().unwrap(),
                Write::Part {
                    upload,
                    number: index as u32 + 1,
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .unwrap();
    }
}
async fn publish(
    client: &FileClient,
    publication: Publication,
    content: &[u8],
) -> Committed<BlobMutationOutcome> {
    let upload = begin(client, publication).await;
    stage(client, upload, content).await;
    client
        .write(
            new_identity().unwrap(),
            Write::Complete {
                upload,
                parts: content.len().div_ceil(PART_BYTES) as u32,
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn interrupted_upload_restores_staging_and_resumes_identical_bytes() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("working");
    let source = root.path().join("source.bin");
    let body: Vec<u8> = (0..PART_BYTES + 37)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&source, &body).unwrap();
    let plan = RetainedUpload::prepare(
        &source,
        FileKey::new("reports/result.bin").unwrap(),
        Publication::Missing,
        &root.path().join("plan"),
    )
    .unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), &state).await;
    let client = file(&node, store.clone(), "reports/result.bin").await;
    let (identity, write) = plan.begin().unwrap();
    client.write(identity, write).await.unwrap();
    let (identity, write) = plan.part(1).unwrap();
    let prepared = client.prepare(identity, write).await.unwrap();
    let evidence = prepared.evidence().clone();
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Absent
    ));
    let _discarded = prepared.execute().await.unwrap();
    assert!(client.head(None).await.unwrap().output.is_none());
    node.shutdown().await.unwrap();
    let successor = start(store.clone(), &state).await;
    let client = file(&successor, store, "reports/result.bin").await;
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    let loaded = RetainedUpload::load(&root.path().join("plan")).unwrap();
    assert_eq!(loaded.digest(), *blake3::hash(&body).as_bytes());
    let (identity, write) = loaded.part(2).unwrap();
    client.write(identity, write).await.unwrap();
    let (identity, write) = loaded.complete().unwrap();
    let committed = client.write(identity, write.clone()).await.unwrap();
    assert_eq!(client.write(identity, write).await.unwrap(), committed);
    let first = client
        .read(0, PART_BYTES as u32, Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    let second = client
        .read(PART_BYTES as u64, 64, Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!([first.bytes, second.bytes].concat(), body);
    let crossing = client
        .read(PART_BYTES as u64 - 7, 20, Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(crossing.bytes, body[PART_BYTES - 7..PART_BYTES + 13]);
    successor.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_replacements_check_the_etag_at_completion() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = file(&node, store, "files/contended.bin").await;
    publish(&client, Publication::Missing, b"original").await;
    let original = client.head(None).await.unwrap().output.unwrap();
    let first = begin(&client, Publication::Match(original.etag)).await;
    let second = begin(&client, Publication::Match(original.etag)).await;
    stage(&client, first, b"first").await;
    stage(&client, second, b"second").await;
    assert_eq!(
        client
            .read(0, 64, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .bytes,
        b"original"
    );
    let first_id = new_identity().unwrap();
    let second_id = new_identity().unwrap();
    let (first_result, second_result) = tokio::join!(
        client.write(
            first_id,
            Write::Complete {
                upload: first,
                parts: 1
            }
        ),
        client.write(
            second_id,
            Write::Complete {
                upload: second,
                parts: 1
            }
        )
    );
    let (loser_id, loser, rejected) = match (first_result, second_result) {
        (Ok(_), Err(InvocationError::Rejected(rejected))) => (second_id, second, rejected),
        (Err(InvocationError::Rejected(rejected)), Ok(_)) => (first_id, first, rejected),
        other => panic!("one completion must win: {other:?}"),
    };
    assert_eq!(rejected.output, BlobMutationOutcome::Conflict);
    assert!(
        matches!(client.write(loser_id, Write::Complete { upload: loser, parts: 1 }).await,
        Err(InvocationError::Rejected(replay)) if replay == rejected)
    );
    assert_ne!(
        client.head(None).await.unwrap().output.unwrap().etag,
        original.etag
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn conditional_delete_replay_does_not_remove_recreated_files() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = file(&node, store, "files/deleted.bin").await;
    publish(&client, Publication::Missing, b"first").await;
    let first = client.head(None).await.unwrap().output.unwrap();
    let identity = new_identity().unwrap();
    let deleted = client
        .write(identity, Write::Delete { etag: first.etag })
        .await
        .unwrap();
    publish(&client, Publication::Missing, b"recreated").await;
    assert_eq!(
        client
            .write(identity, Write::Delete { etag: first.etag })
            .await
            .unwrap(),
        deleted
    );
    assert!(matches!(
        client
            .write(new_identity().unwrap(), Write::Delete { etag: first.etag })
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert_eq!(
        client
            .read(0, 64, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .bytes,
        b"recreated"
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_preserves_published_content_and_bounds_reject_before_dispatch() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = file(&node, store, "files/abort.bin").await;
    publish(&client, Publication::Missing, b"published").await;
    let metadata = client.head(None).await.unwrap().output.unwrap();
    let upload = begin(&client, Publication::Match(metadata.etag)).await;
    stage(&client, upload, b"unpublished").await;
    client
        .write(new_identity().unwrap(), Write::Abort { upload })
        .await
        .unwrap();
    assert_eq!(client.head(None).await.unwrap().output.unwrap(), metadata);
    assert!(matches!(
        client
            .write(
                new_identity().unwrap(),
                Write::Complete { upload, parts: 1 }
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(matches!(
        client
            .prepare(
                new_identity().unwrap(),
                Write::Part {
                    upload,
                    number: 1,
                    bytes: vec![0; PART_BYTES + 1]
                }
            )
            .await,
        Err(InvocationError::NotStarted(_))
    ));
    assert!(client.read(0, PART_BYTES as u32 + 1, None).await.is_err());
    node.shutdown().await.unwrap();
}

#[test]
fn retained_parts_freeze_source_and_reject_changed_plan_bytes() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.bin");
    let directory = root.path().join("plan");
    std::fs::write(&source, b"frozen").unwrap();
    let prepared = RetainedUpload::prepare(
        &source,
        FileKey::new("files/frozen.bin").unwrap(),
        Publication::Missing,
        &directory,
    )
    .unwrap();
    std::fs::write(&source, b"changed original").unwrap();
    let loaded = RetainedUpload::load(&directory).unwrap();
    assert_eq!(loaded.digest(), prepared.digest());
    assert_eq!(loaded.begin().unwrap(), prepared.begin().unwrap());
    std::fs::write(directory.join("01.part"), b"tampered").unwrap();
    assert!(RetainedUpload::load(&directory).is_err());
    assert!(
        RetainedUpload::prepare(
            &source,
            FileKey::new("files/frozen.bin").unwrap(),
            Publication::Missing,
            &directory
        )
        .is_err()
    );
}

#[tokio::test]
async fn corrupt_artifact_fails_verified_read_after_restore() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = file(&node, store.clone(), "files/integrity.bin").await;
    let upload = begin(&client, Publication::Missing).await;
    let stored = client
        .write(
            new_identity().unwrap(),
            Write::Part {
                upload,
                number: 1,
                bytes: b"original".to_vec(),
            },
        )
        .await
        .unwrap();
    let BlobMutationOutcome::PartStored { digest } = stored.output else {
        panic!("part digest missing");
    };
    let committed = client
        .write(
            new_identity().unwrap(),
            Write::Complete { upload, parts: 1 },
        )
        .await
        .unwrap();
    node.shutdown().await.unwrap();
    // Fault injection targets the content path returned by the native part
    // outcome. Production vault code never constructs or deletes part paths.
    let hash = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let part =
        cellule_store::global_content_path(cellule_store::GLOBAL_PREFIX, "blob-parts", &hash);
    store
        .inner()
        .put(&part, b"corrupt!".as_slice().into())
        .await
        .unwrap();
    let successor = start(store.clone(), root.path()).await;
    let client = file(&successor, store, "files/integrity.bin").await;
    assert!(
        client
            .head(Some(committed.receipt))
            .await
            .unwrap()
            .output
            .is_some()
    );
    assert!(client.read(0, 64, Some(committed.receipt)).await.is_err());
    successor.shutdown().await.unwrap();
}

#[test]
fn canonical_file_and_publication_json_contracts_are_stable() {
    assert_eq!(
        FileKey::new("files/result.bin").unwrap().as_bytes(),
        b"files/result.bin"
    );
    for invalid in ["", "/file", "a//b", "a/../b", "a/./b", "File", "a\\b"] {
        assert!(FileKey::new(invalid).is_err());
    }
    assert_eq!(
        serde_json::to_string(&Publication::Missing).unwrap(),
        r#"{"kind":"missing"}"#
    );
    assert!(serde_json::from_str::<Publication>(r#"{"kind":"match","etag":"00"}"#).is_err());
}
