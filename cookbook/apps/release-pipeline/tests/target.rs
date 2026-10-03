//! Public target publication, compensation, permanent identity, and restoration contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_release_pipeline::*;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;

fn id(value: u128) -> ReleaseId {
    ReleaseId::from_bytes(value.to_be_bytes()).unwrap()
}
fn name(value: &str) -> TargetName {
    TargetName::new(value.into()).unwrap()
}
fn work(value: u128, generation: u64) -> TargetWork {
    let (artifact, bytes) = Artifact::build(id(value), b"hello release\n").unwrap();
    TargetWork {
        deployment: Deployment {
            release: id(value),
            target: name("demo"),
            artifact,
            expected_generation: generation,
        },
        action: TargetAction::Deploy(bytes),
    }
}
fn rollback(value: &TargetWork) -> TargetWork {
    TargetWork {
        deployment: value.deployment.clone(),
        action: TargetAction::Rollback,
    }
}
async fn start(store: Store, root: &std::path::Path) -> (LocalNode, TargetClient) {
    let node = LocalNode::start(
        compile_target().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("release-target-test"),
            application_id: ApplicationId::from_bytes([0x95; 16]),
        },
    )
    .await
    .unwrap();
    let handle = node
        .application_handle::<TargetApplication>(TenantId::from_bytes([0x96; 16]))
        .unwrap();
    let client = TargetClient::new(handle);
    node.open_cell(&client.target().unwrap(), &Targets)
        .await
        .unwrap();
    (node, client)
}
async fn setup() -> (tempfile::TempDir, LocalNode, TargetClient) {
    let root = tempfile::tempdir().unwrap();
    let (node, client) = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    (root, node, client)
}
async fn apply(client: &TargetClient, input: TargetWork) -> TargetOutcome {
    match client
        .prepare(new_identity().unwrap(), input)
        .await
        .unwrap()
        .execute()
        .await
    {
        Ok(value) => value.output,
        Err(InvocationError::Rejected(value)) => value.output,
        Err(error) => panic!("unexpected target publication failure: {error:?}"),
    }
}
async fn record(client: &TargetClient, release: u128) -> TargetRecord {
    client
        .record(id(release), None)
        .await
        .unwrap()
        .output
        .unwrap()
}

#[test]
fn identity_artifact_envelope_and_versioned_wire_are_fixed_contracts() {
    assert_eq!(id(1).to_string(), "00000000000000000000000000000001");
    assert_eq!(
        "00000000000000000000000000000001"
            .parse::<ReleaseId>()
            .unwrap(),
        id(1)
    );
    for invalid in [
        "00000000000000000000000000000000",
        "0000000000000000000000000000000A",
        "1",
        "01a000000000000000000000000000001",
    ] {
        assert!(invalid.parse::<ReleaseId>().is_err());
    }
    for invalid in [
        "",
        "Demo",
        "../demo",
        "a/b",
        "-demo",
        "demo-",
        "demo--two",
        "1demo",
        "é",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        assert!(TargetName::new(invalid.into()).is_err());
    }
    let (artifact, bytes) = Artifact::build(id(1), b"a").unwrap();
    let fixture = [
        b"CELLULE-RELEASE\x01".as_slice(),
        &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        &[0, 0, 0, 1],
        b"a",
    ]
    .concat();
    assert_eq!(bytes, fixture);
    assert_eq!(artifact.bytes, 37);
    assert!(artifact.verify(id(2), &bytes).is_err());
    assert!(Artifact::build(id(1), &[]).is_err());
    assert!(Artifact::build(id(1), &vec![0; MAX_SOURCE_BYTES + 1]).is_err());
    let mut changed = bytes.clone();
    changed[36] = b'b';
    assert!(artifact.verify(id(1), &changed).is_err());
    assert!(artifact.verify(id(1), &[]).is_err());
    assert_ne!(Artifact::build(id(1), b"b").unwrap().0, artifact);
    let mut encoded = BoundedEncoder::new(64).unwrap();
    id(1).encode(&mut encoded).unwrap();
    assert_eq!(
        encoded.finish(),
        [
            &[1, 0, 0, 0, 16][..],
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        ]
        .concat()
    );
    let mut decoder = BoundedDecoder::new(&[2, 0, 0, 0, 0], 64).unwrap();
    assert!(ReleaseId::decode(&mut decoder).is_err());
    let mut decoder = BoundedDecoder::new(&[1, 0, 0, 0, 1, 1], 64).unwrap();
    assert!(ReleaseId::decode(&mut decoder).is_err());
    let a = work(1, 0);
    let b = work(1, 99);
    assert_eq!(
        a.deployment.operation_key(false),
        b.deployment.operation_key(false)
    );
    assert_ne!(
        a.deployment.operation_key(false),
        a.deployment.operation_key(true)
    );
    assert_ne!(
        a.deployment.operation_key(false),
        work(2, 0).deployment.operation_key(false)
    );
    let mut encoded = BoundedEncoder::new(8).unwrap();
    TargetOutcome::Superseded.encode(&mut encoded).unwrap();
    assert_eq!(encoded.finish(), [1, 3]);
}

#[tokio::test]
async fn maximum_artifact_fits_declared_command_and_verified_download_bounds() {
    let (_root, node, client) = setup().await;
    let (artifact, bytes) = Artifact::build(id(1), &vec![0xff; MAX_SOURCE_BYTES]).unwrap();
    assert_eq!(bytes.len(), MAX_ARTIFACT_BYTES);
    let mut input = work(1, 0);
    input.deployment.artifact = artifact;
    input.action = TargetAction::Deploy(bytes.clone());
    assert_eq!(apply(&client, input.clone()).await, TargetOutcome::Deployed);
    assert_eq!(
        client.artifact(id(1), None).await.unwrap().output,
        Some(bytes.clone())
    );
    assert_eq!(
        apply(&client, rollback(&input)).await,
        TargetOutcome::RolledBack
    );
    assert_eq!(
        client.artifact(id(1), None).await.unwrap().output,
        Some(bytes)
    );
    assert_eq!(
        client
            .state(name("demo"), None)
            .await
            .unwrap()
            .output
            .selected,
        None
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn target_restores_captured_predecessor_and_retries_never_reapply() {
    let (_root, node, client) = setup().await;
    let first = work(1, 0);
    let second = work(2, 1);
    assert_eq!(apply(&client, first.clone()).await, TargetOutcome::Deployed);
    assert_eq!(
        apply(&client, second.clone()).await,
        TargetOutcome::Deployed
    );
    let before = client.state(name("demo"), None).await.unwrap();
    for _ in 0..3 {
        assert_eq!(
            apply(&client, second.clone()).await,
            TargetOutcome::Deployed
        );
    }
    assert_eq!(
        client.state(name("demo"), None).await.unwrap().output,
        before.output
    );
    assert_eq!(
        apply(&client, rollback(&second)).await,
        TargetOutcome::RolledBack
    );
    let selected = client.state(name("demo"), None).await.unwrap().output;
    assert_eq!(selected.generation, 3);
    assert_eq!(selected.selected.as_ref().unwrap().release, id(1));
    for _ in 0..3 {
        assert_eq!(
            apply(&client, rollback(&second)).await,
            TargetOutcome::RolledBack
        );
        assert_eq!(
            apply(&client, second.clone()).await,
            TargetOutcome::RolledBack
        );
    }
    assert_eq!(
        client.state(name("demo"), None).await.unwrap().output,
        selected
    );
    let done = record(&client, 2).await;
    assert_eq!((done.deploys, done.rollbacks), (1, 1));
    assert_eq!(done.previous.unwrap().artifact, first.deployment.artifact);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn rollback_before_deploy_is_a_permanent_request_bound_tombstone() {
    let (_root, node, client) = setup().await;
    let input = work(1, 0);
    assert_eq!(
        apply(&client, rollback(&input)).await,
        TargetOutcome::Cancelled
    );
    assert_eq!(
        apply(&client, input.clone()).await,
        TargetOutcome::Cancelled
    );
    assert_eq!(
        apply(&client, rollback(&input)).await,
        TargetOutcome::Cancelled
    );
    assert_eq!(record(&client, 1).await.deploys, 0);
    assert_eq!(record(&client, 1).await.rollbacks, 1);
    let mut changed = input.clone();
    changed.deployment.target = name("other");
    assert_eq!(apply(&client, changed).await, TargetOutcome::Conflict);
    assert_eq!(
        client
            .state(name("demo"), None)
            .await
            .unwrap()
            .output
            .generation,
        0
    );
    assert_eq!(
        client
            .state(name("other"), None)
            .await
            .unwrap()
            .output
            .generation,
        0
    );
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_deployments_have_one_winner_and_permanent_generation_rejection() {
    let (_root, node, client) = setup().await;
    let (a, b) = tokio::join!(apply(&client, work(1, 0)), apply(&client, work(2, 0)));
    assert!(matches!(
        (a, b),
        (TargetOutcome::Deployed, TargetOutcome::Conflict)
            | (TargetOutcome::Conflict, TargetOutcome::Deployed)
    ));
    let winner = if a == TargetOutcome::Deployed { 1 } else { 2 };
    assert_eq!(
        client
            .state(name("demo"), None)
            .await
            .unwrap()
            .output
            .generation,
        1
    );
    assert_eq!(
        apply(&client, rollback(&work(winner, 0))).await,
        TargetOutcome::RolledBack
    );
    assert_eq!(
        client
            .state(name("demo"), None)
            .await
            .unwrap()
            .output
            .generation,
        2
    );
    let premature = work(3, 3);
    let refused = client
        .prepare(new_identity().unwrap(), premature.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(refused.output, TargetOutcome::Conflict);
    assert_eq!(
        client
            .record(id(3), Some(refused.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .outcome,
        TargetOutcome::Conflict
    );
    assert_eq!(apply(&client, work(4, 2)).await, TargetOutcome::Deployed);
    assert_eq!(
        client
            .state(name("demo"), None)
            .await
            .unwrap()
            .output
            .generation,
        3
    );
    assert_eq!(apply(&client, premature).await, TargetOutcome::Conflict);
    assert_eq!(record(&client, 3).await.deploys, 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn rollback_refuses_newer_content_and_a_restored_old_artifact_cannot_create_aba() {
    let (_root, node, client) = setup().await;
    let first = work(1, 0);
    let second = work(2, 1);
    apply(&client, first.clone()).await;
    apply(&client, second.clone()).await;
    let current = client.state(name("demo"), None).await.unwrap().output;
    assert_eq!(
        apply(&client, rollback(&first)).await,
        TargetOutcome::Superseded
    );
    assert_eq!(
        client.state(name("demo"), None).await.unwrap().output,
        current
    );
    assert_eq!(
        apply(&client, rollback(&second)).await,
        TargetOutcome::RolledBack
    );
    let restored = client.state(name("demo"), None).await.unwrap().output;
    assert_eq!(restored.selected.as_ref().unwrap().release, id(1));
    assert_eq!(restored.generation, 3);
    assert_eq!(
        apply(&client, rollback(&first)).await,
        TargetOutcome::Superseded
    );
    assert_eq!(apply(&client, first).await, TargetOutcome::Superseded);
    assert_eq!(
        client.state(name("demo"), None).await.unwrap().output,
        restored
    );
    assert_eq!(record(&client, 1).await.rollbacks, 0);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_content_generation_or_target_cannot_rebind_a_permanent_release() {
    let (_root, node, client) = setup().await;
    let original = work(1, 0);
    apply(&client, original.clone()).await;
    let mut different = original.clone();
    different.deployment.expected_generation = 1;
    assert_eq!(apply(&client, different).await, TargetOutcome::Conflict);
    let mut different = original.clone();
    different.deployment.target = name("other");
    assert_eq!(apply(&client, different).await, TargetOutcome::Conflict);
    let mut different = original.clone();
    let (artifact, bytes) = Artifact::build(id(1), b"changed").unwrap();
    different.deployment.artifact = artifact;
    different.action = TargetAction::Deploy(bytes);
    assert_eq!(apply(&client, different).await, TargetOutcome::Conflict);
    assert_eq!(record(&client, 1).await.deployment, original.deployment);
    assert_eq!(
        client
            .state(name("other"), None)
            .await
            .unwrap()
            .output
            .generation,
        0
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_artifact_is_rejected_before_publication_and_target_reads_are_read_only() {
    let (_root, node, client) = setup().await;
    let before = client.state(name("demo"), None).await.unwrap();
    let mut input = work(1, 0);
    let TargetAction::Deploy(ref mut bytes) = input.action else {
        panic!()
    };
    bytes[36] ^= 1;
    assert!(matches!(
        client.prepare(new_identity().unwrap(), input).await,
        Err(InvocationError::NotStarted(_))
    ));
    assert!(client.record(id(1), None).await.unwrap().output.is_none());
    let after = client.state(name("demo"), None).await.unwrap();
    assert_eq!(before.receipt, after.receipt);
    assert_eq!(before.output, after.output);
    let mut invalid = work(1, 0);
    invalid.deployment.expected_generation = i64::MAX as u64;
    assert!(matches!(
        client.prepare(new_identity().unwrap(), invalid).await,
        Err(InvocationError::NotStarted(_))
    ));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn cold_restore_keeps_original_receipt_tombstone_and_compensation_evidence() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let (first, client) = start(store.clone(), root.path()).await;
    let input = work(1, 0);
    let prepared = client
        .prepare(new_identity().unwrap(), input.clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let original = prepared.execute().await.unwrap();
    assert_eq!(
        apply(&client, rollback(&input)).await,
        TargetOutcome::RolledBack
    );
    assert_eq!(
        apply(&client, rollback(&work(2, 2))).await,
        TargetOutcome::Cancelled
    );
    let before = client.state(name("demo"), None).await.unwrap().output;
    let operation = record(&client, 1).await;
    first.shutdown().await.unwrap();
    tokio::fs::remove_dir_all(root.path()).await.unwrap();
    let (restored, client) = start(store, root.path()).await;
    assert_eq!(
        client.state(name("demo"), None).await.unwrap().output,
        before
    );
    assert_eq!(record(&client, 1).await, operation);
    let TargetAction::Deploy(bytes) = &input.action else {
        panic!()
    };
    assert_eq!(
        client.artifact(id(1), None).await.unwrap().output.as_ref(),
        Some(bytes)
    );
    assert!(client.artifact(id(2), None).await.unwrap().output.is_none());
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(value) if value.commit_sequence()==original.receipt.commit_sequence)
    );
    assert_eq!(
        apply(&client, input.clone()).await,
        TargetOutcome::RolledBack
    );
    assert_eq!(
        apply(&client, rollback(&input)).await,
        TargetOutcome::RolledBack
    );
    assert_eq!(apply(&client, work(2, 2)).await, TargetOutcome::Cancelled);
    assert_eq!(
        client.state(name("demo"), None).await.unwrap().output,
        before
    );
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_slot_capacity_cannot_evict_old_content_or_admit_an_extra_target() {
    let (_root, node, client) = setup().await;
    for n in 0..MAX_TARGETS {
        let mut input = work(n as u128 + 1, 0);
        input.deployment.target = name(&format!("target-{n}"));
        assert_eq!(apply(&client, input).await, TargetOutcome::Deployed);
    }
    let mut extra = work(1000, 0);
    extra.deployment.target = name("extra");
    assert_eq!(apply(&client, extra.clone()).await, TargetOutcome::Capacity);
    assert!(
        client
            .record(id(1000), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert_eq!(
        client
            .state(name("extra"), None)
            .await
            .unwrap()
            .output
            .generation,
        0
    );
    assert_eq!(
        client
            .state(name("target-0"), None)
            .await
            .unwrap()
            .output
            .selected
            .unwrap()
            .release,
        id(1)
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn permanent_history_capacity_does_not_discard_cancellation_tombstones() {
    let (_root, node, client) = setup().await;
    for n in 1..=MAX_DEPLOYMENTS {
        assert_eq!(
            apply(&client, rollback(&work(n as u128, 0))).await,
            TargetOutcome::Cancelled
        );
    }
    assert_eq!(apply(&client, work(2000, 0)).await, TargetOutcome::Capacity);
    assert_eq!(apply(&client, work(1, 0)).await, TargetOutcome::Cancelled);
    assert_eq!(record(&client, 1).await.deploys, 0);
    assert_eq!(
        client
            .state(name("demo"), None)
            .await
            .unwrap()
            .output
            .generation,
        0
    );
    node.shutdown().await.unwrap();
}
