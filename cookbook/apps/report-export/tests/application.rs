//! Public sealed-version, pagination, immutable publication, and recovery contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_report_export::{
    Artifacts, Change, Completion, DataClient, DataOutcome, ExportApplication, ExportClient,
    ExportEngine, ExportError, PageRequest, Request, Row, StartOutcome, Version, Work, compile,
    dataset_digest, encode_report, open,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{ApplicationId, BlobArtifactStore, InvocationError, Resolution, TenantId};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;
async fn node(store: Store, path: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("export-test/cells"),
            application_id: ApplicationId::from_bytes([0x46; 16]),
        },
    )
    .await
    .unwrap()
}
async fn clients(node: &LocalNode, store: &Store) -> (DataClient, Artifacts, ExportClient) {
    let handle = node
        .application_handle::<ExportApplication>(TenantId::from_bytes([0x56; 16]))
        .unwrap()
        .with_blob_artifact_store(BlobArtifactStore::new(Store::new(Arc::new(
            object_store::prefix::PrefixStore::new(store.inner().clone(), "export-test/artifacts"),
        ))));
    open(node, &handle).await.unwrap();
    (
        DataClient::new(handle.clone()).unwrap(),
        Artifacts::new(handle.clone()),
        ExportClient::new(handle),
    )
}
fn rows(count: u32) -> Vec<Row> {
    (1..=count)
        .filter(|id| *id != 17)
        .map(|id| Row {
            id,
            label: if id == 3 {
                "Café, \"west\"\nqueue".into()
            } else {
                format!("Region {id}")
            },
            units: u64::from(id) * 10,
        })
        .collect()
}
async fn seal(data: &DataClient, rows: Vec<Row>) -> cellule_cookbook_report_export::Snapshot {
    let revision = data.info(None).await.unwrap().output.revision;
    let changed = data
        .prepare(
            new_identity().unwrap(),
            Change::Replace {
                expected_revision: revision,
                rows,
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let DataOutcome::Applied(revision) = changed.output else {
        panic!("replacement failed")
    };
    let outcome = data
        .prepare(
            new_identity().unwrap(),
            Change::Seal {
                expected_revision: revision,
                version: Version(*uuid::Uuid::now_v7().as_bytes()),
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let DataOutcome::Sealed(snapshot) = outcome.output else {
        panic!("seal failed")
    };
    snapshot
}
fn request(data: &DataClient, snapshot: cellule_cookbook_report_export::Snapshot) -> Request {
    Request {
        id: *uuid::Uuid::now_v7().as_bytes(),
        source_cell: *data.target().cell_id().as_bytes(),
        snapshot,
        deadline_ms: now_ms().unwrap() + 3_600_000,
        endpoint: "http://127.0.0.1:19022/".into(),
    }
}
async fn pages(
    engine: &ExportEngine,
    request: &Request,
) -> Vec<cellule_cookbook_report_export::Chunk> {
    let mut cursor = 0;
    let mut chunks = Vec::new();
    loop {
        let Completion::Page(chunk) = engine
            .execute(Work::Page {
                request: request.clone(),
                after: cursor,
            })
            .await
            .unwrap()
        else {
            panic!("page result required")
        };
        cursor = chunk.last;
        let more = chunk.more;
        chunks.push(chunk);
        if !more {
            break;
        }
    }
    chunks
}
#[tokio::test]
async fn conditional_draft_commands_replay_and_durable_conflicts_do_not_change() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, _, _) = clients(&node, &store).await;
    let prepared = data
        .prepare(
            new_identity().unwrap(),
            Change::Put {
                expected_revision: 0,
                row: rows(1).remove(0),
            },
        )
        .await
        .unwrap();
    let original = prepared.clone().execute().await.unwrap();
    assert_eq!(original.output, DataOutcome::Applied(1));
    assert_eq!(prepared.clone().execute().await.unwrap(), original);
    assert!(matches!(
        data.resolve(prepared.evidence()).await.unwrap(),
        Resolution::Committed(_)
    ));
    let stale = data
        .prepare(
            new_identity().unwrap(),
            Change::Delete {
                expected_revision: 0,
                id: 1,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(stale.clone().execute().await,Err(InvocationError::Rejected(value))if value.output==DataOutcome::Conflict)
    );
    data.prepare(
        new_identity().unwrap(),
        Change::Delete {
            expected_revision: 1,
            id: 1,
        },
    )
    .await
    .unwrap()
    .execute()
    .await
    .unwrap();
    assert!(
        matches!(stale.execute().await,Err(InvocationError::Rejected(value))if value.output==DataOutcome::Conflict)
    );
    assert_eq!(data.info(None).await.unwrap().output.rows, 0);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn sealed_keyset_pages_remain_identical_while_draft_and_new_version_change() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, _, _) = clients(&node, &store).await;
    let original = rows(70);
    let snapshot = seal(&data, original.clone()).await;
    let mut after = 0;
    let mut observed = Vec::new();
    loop {
        let page = data
            .page(
                PageRequest {
                    snapshot: snapshot.clone(),
                    after,
                },
                None,
            )
            .await
            .unwrap()
            .output;
        assert!(page.rows.len() <= 32);
        after = page.rows.last().unwrap().id;
        observed.extend(page.rows);
        if !page.more {
            break;
        }
    }
    assert_eq!(observed, original);
    let newer = seal(&data, rows(2)).await;
    assert_ne!(newer.digest, snapshot.digest);
    let first = data
        .page(
            PageRequest {
                snapshot: snapshot.clone(),
                after: 0,
            },
            None,
        )
        .await
        .unwrap()
        .output;
    assert_eq!(first.rows, original[..32]);
    assert_eq!(
        data.snapshot(snapshot.version, None)
            .await
            .unwrap()
            .output
            .unwrap(),
        snapshot
    );
    let conflict = data
        .prepare(
            new_identity().unwrap(),
            Change::Seal {
                expected_revision: newer.revision,
                version: snapshot.version,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(conflict.execute().await,Err(InvocationError::Rejected(value))if value.output==DataOutcome::Conflict)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn three_page_export_reconstructs_exact_sealed_csv_and_reuses_manifests() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, files, _) = clients(&node, &store).await;
    let expected = rows(70);
    let snapshot = seal(&data, expected.clone()).await;
    let request = request(&data, snapshot);
    let engine = ExportEngine::new(data.clone(), files.clone());
    let chunks = pages(&engine, &request).await;
    assert_eq!(chunks.len(), 3);
    let first = engine
        .execute(Work::Page {
            request: request.clone(),
            after: 0,
        })
        .await
        .unwrap();
    assert!(matches!(first,Completion::Page(value)if value==chunks[0]));
    seal(&data, rows(2)).await;
    let Completion::Report(report) = engine
        .execute(Work::Finalize {
            request: request.clone(),
            chunks: chunks.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("report expected")
    };
    let (artifact, bytes) = files
        .read(report.artifact.key, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(artifact, report.artifact);
    assert_eq!(bytes, encode_report(&expected).unwrap());
    assert_eq!(report.dataset_digest, dataset_digest(&expected).unwrap());
    assert_eq!(report.rows, 69);
    assert!(
        matches!(engine.execute(Work::Finalize{request,chunks}).await.unwrap(),Completion::Report(value)if value==report)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn missing_repeated_reordered_and_changed_page_evidence_refuse_final_publication() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, files, _) = clients(&node, &store).await;
    let request = request(&data, seal(&data, rows(70)).await);
    let engine = ExportEngine::new(data.clone(), files.clone());
    let chunks = pages(&engine, &request).await;
    for invalid in [
        vec![chunks[0].clone(), chunks[2].clone()],
        vec![chunks[0].clone(), chunks[0].clone(), chunks[2].clone()],
        vec![chunks[1].clone(), chunks[0].clone(), chunks[2].clone()],
    ] {
        assert!(
            engine
                .execute(Work::Finalize {
                    request: request.clone(),
                    chunks: invalid
                })
                .await
                .is_err()
        );
    }
    let mut wrong = chunks.clone();
    wrong[0].artifact.digest = [9; 32];
    assert!(
        engine
            .execute(Work::Finalize {
                request: request.clone(),
                chunks: wrong
            })
            .await
            .is_err()
    );
    assert!(
        files
            .read(request.output_key(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    let mut verified = Vec::new();
    for chunk in chunks {
        let (_, bytes) = files
            .read(chunk.artifact.key, None)
            .await
            .unwrap()
            .output
            .unwrap();
        verified.push((chunk, bytes));
    }
    let mut page = cellule_cookbook_report_export::decode_page(&verified[0].1).unwrap();
    page[0].units += 1;
    verified[0].1 = cellule_cookbook_report_export::encode_page(&page).unwrap();
    verified[0].0.artifact.bytes = verified[0].1.len() as u32;
    verified[0].0.artifact.digest = *blake3::hash(&verified[0].1).as_bytes();
    assert!(cellule_cookbook_report_export::reconstruct(&request, &verified).is_err());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn nonexistent_or_changed_sealed_pin_is_explicit_and_does_not_publish() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, files, _) = clients(&node, &store).await;
    let mut request = request(&data, seal(&data, rows(1)).await);
    request.snapshot.digest = [9; 32];
    let engine = ExportEngine::new(data.clone(), files.clone());
    assert!(matches!(
        engine
            .execute(Work::Page {
                request: request.clone(),
                after: 0
            })
            .await,
        Err(ExportError::VersionMismatch)
    ));
    assert!(
        data.page(
            PageRequest {
                snapshot: request.snapshot.clone(),
                after: 0
            },
            None
        )
        .await
        .is_err()
    );
    assert!(
        files
            .read(request.output_key(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn retained_workflow_start_is_permanent_and_rejects_changed_input() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, _, client) = clients(&node, &store).await;
    let request = request(&data, seal(&data, rows(1)).await);
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
    let mut changed = request.clone();
    changed.snapshot.digest = [9; 32];
    let conflict = client
        .prepare(new_identity().unwrap(), changed)
        .await
        .unwrap();
    assert!(
        matches!(conflict.clone().execute().await,Err(InvocationError::Rejected(value))if value.output==StartOutcome::Conflict)
    );
    assert!(
        matches!(conflict.execute().await,Err(InvocationError::Rejected(value))if value.output==StartOutcome::Conflict)
    );
    let view = client
        .get(request.id, Some(original.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.state.request, request);
    assert_eq!(view.state.rows, 0);
    assert!(view.state.chunks.is_empty());
    assert!(view.state.action.is_some());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn empty_sealed_version_exports_a_header_without_an_empty_page() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, files, client) = clients(&node, &store).await;
    let request = request(&data, seal(&data, vec![]).await);
    client
        .prepare(new_identity().unwrap(), request.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let view = client.get(request.id, None).await.unwrap().output.unwrap();
    assert!(view.state.finalizing);
    assert!(view.state.chunks.is_empty());
    let Completion::Report(report) = ExportEngine::new(data.clone(), files.clone())
        .execute(Work::Finalize {
            request,
            chunks: vec![],
        })
        .await
        .unwrap()
    else {
        panic!("report required")
    };
    assert_eq!(report.rows, 0);
    assert_eq!(
        files
            .read(report.artifact.key, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .1,
        encode_report(&[]).unwrap()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn sealed_version_capacity_preserves_all_old_versions_and_rejection_outcomes() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, _, _) = clients(&node, &store).await;
    for id in 1..=16 {
        data.prepare(
            new_identity().unwrap(),
            Change::Seal {
                expected_revision: 0,
                version: Version([id; 16]),
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    }
    let full = data
        .prepare(
            new_identity().unwrap(),
            Change::Seal {
                expected_revision: 0,
                version: Version([17; 16]),
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(full.clone().execute().await,Err(InvocationError::Rejected(value))if value.output==DataOutcome::Capacity)
    );
    assert!(
        matches!(full.execute().await,Err(InvocationError::Rejected(value))if value.output==DataOutcome::Capacity)
    );
    assert_eq!(data.info(None).await.unwrap().output.versions, 16);
    assert!(
        data.snapshot(Version([1; 16]), None)
            .await
            .unwrap()
            .output
            .is_some()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_preserves_sealed_rows_queued_run_and_published_pages() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let store = Store::new(Arc::new(InMemory::new()));
    let first = node(store.clone(), &state).await;
    let (data, files, client) = clients(&first, &store).await;
    let request = request(&data, seal(&data, rows(40)).await);
    let prepared = client
        .prepare(new_identity().unwrap(), request.clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let receipt = prepared.execute().await.unwrap().receipt;
    let Completion::Page(chunk) = ExportEngine::new(data.clone(), files.clone())
        .execute(Work::Page {
            request: request.clone(),
            after: 0,
        })
        .await
        .unwrap()
    else {
        panic!("page required")
    };
    let original = files
        .read(chunk.artifact.key, None)
        .await
        .unwrap()
        .output
        .unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let second = node(store.clone(), &state).await;
    let (data, files, client) = clients(&second, &store).await;
    assert_eq!(
        data.snapshot(request.snapshot.version, None)
            .await
            .unwrap()
            .output
            .unwrap(),
        request.snapshot
    );
    assert_eq!(
        files
            .read(chunk.artifact.key, None)
            .await
            .unwrap()
            .output
            .unwrap(),
        original
    );
    let view = client
        .get(request.id, Some(receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.state.rows, 0);
    assert!(view.state.report.is_none());
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    second.shutdown().await.unwrap();
}
#[tokio::test]
async fn resolver_capabilities_reject_foreign_transaction_domains() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, _, client) = clients(&node, &store).await;
    let prepared = data
        .prepare(
            new_identity().unwrap(),
            Change::Replace {
                expected_revision: 0,
                rows: vec![],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        client.resolve(prepared.evidence()).await,
        Err(InvocationError::NotStarted(
            cellule_runtime::Error::Identity(_)
        ))
    ));
    node.shutdown().await.unwrap();
}
#[test]
fn draft_put_v1_wire_fixture_keeps_tag_revision_and_row_layout() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    let value = Change::Put {
        expected_revision: 0,
        row: Row {
            id: 1,
            label: "West".into(),
            units: 7,
        },
    };
    let fixture = [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 4, b'W', b'e', b's', b't', 0, 0, 0, 0, 0,
        0, 0, 7,
    ];
    let mut encoder = BoundedEncoder::new(1024).unwrap();
    value.encode(&mut encoder).unwrap();
    assert_eq!(encoder.finish(), fixture);
    let mut decoder = BoundedDecoder::new(&fixture, 1024).unwrap();
    assert!(
        matches!(Change::decode(&mut decoder).unwrap(),Change::Put{expected_revision:0,row}if row==Row{id:1,label:"West".into(),units:7})
    );
    decoder.finish().unwrap();
}
#[tokio::test]
async fn largest_declared_dataset_exports_all_sixteen_pages_within_byte_limits() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = node(store.clone(), root.path()).await;
    let (data, files, _) = clients(&node, &store).await;
    let rows = (1..=512)
        .map(|id| Row {
            id,
            label: format!("R{}", "\"".repeat(127)),
            units: 1_000_000_000,
        })
        .collect::<Vec<_>>();
    let request = request(&data, seal(&data, rows.clone()).await);
    let engine = ExportEngine::new(data.clone(), files.clone());
    let chunks = pages(&engine, &request).await;
    assert_eq!(chunks.len(), 16);
    assert!(chunks.iter().all(|chunk| chunk.rows == 32
        && chunk.artifact.bytes as usize <= cellule_cookbook_report_export::MAX_CHUNK));
    let Completion::Report(report) = engine
        .execute(Work::Finalize { request, chunks })
        .await
        .unwrap()
    else {
        panic!("report required")
    };
    assert_eq!(report.rows, 512);
    let (_, bytes) = files
        .read(report.artifact.key, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(bytes, encode_report(&rows).unwrap());
    assert!(bytes.len() <= cellule_cookbook_report_export::MAX_BYTES);
    node.shutdown().await.unwrap();
}
