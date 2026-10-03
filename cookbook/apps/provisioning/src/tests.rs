#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use cellule_store::Store;
use object_store::memory::InMemory;
use std::sync::Arc;
async fn local(store: Store, path: &Path, prefix: &str) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: object_store::path::Path::from(prefix),
            application_id: APPLICATION,
        },
    )
    .await
    .unwrap()
}
async fn provider(node: &LocalNode, fault: PathBuf) -> (ProvisioningClient, String) {
    let handle = node
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = ProvisioningClient::new(handle);
    node.open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    let address = provider_server::install(node, client.clone(), 0, Some(fault))
        .await
        .unwrap();
    spawn_provider_lifecycle(node, client.clone()).unwrap();
    (client, format!("http://{address}/"))
}
fn spec(endpoint: &str) -> Spec {
    Spec {
        id: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap(),
        name: "test-volume".into(),
        capacity_mib: 32,
        provider_endpoint: endpoint.into(),
    }
}
fn token() -> String {
    std::env::var("CELLULE_PROVISIONING_PROVIDER_TOKEN")
        .unwrap_or_else(|_| "cookbook-local-provider".into())
}
async fn change(client: &ProvisioningClient, change: Change) {
    client
        .prepare(new_identity().unwrap(), change)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
}
#[tokio::test]
async fn actual_lost_creation_reply_and_retrying_cleanup_keep_one_resource_and_signed_lifetime() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let source = local(store.clone(), &root.path().join("source"), "source").await;
    let receiver = local(store, &root.path().join("provider"), "provider").await;
    let fixture = root.path().join("fault");
    write(&fixture, b"drop-create-reply\n", false).unwrap();
    let (external, endpoint) = provider(&receiver, fixture.clone()).await;
    let handle = source
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = open(&source, &handle).await.unwrap();
    let value = spec(&endpoint);
    change(&client, Change::Request(value.clone())).await;
    spawn_workers(&source, handle).await.unwrap();
    let active = wait(&client, &source, value.id, Phase::Active)
        .await
        .unwrap();
    assert_eq!(active.status, "running");
    let original = external
        .provider(value.id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(
        (original.creates, original.deletes, original.phase),
        (1, 0, ProviderPhase::Ready)
    );
    write(&fixture, b"fail-delete-once\n", true).unwrap();
    change(&client, Change::Delete(value.id)).await;
    let completed = wait(&client, &source, value.id, Phase::Completed)
        .await
        .unwrap();
    assert_eq!(completed.run_id, active.run_id);
    assert_eq!(completed.status, "completed");
    assert!(completed.state.cleanup_attempts >= 2);
    let deleted = external
        .provider(value.id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(deleted.resource_id, original.resource_id);
    assert_eq!(
        (deleted.creates, deleted.deletes, deleted.phase),
        (1, 1, ProviderPhase::Deleted)
    );
    let replay = observe_provider(
        &Work {
            spec: value.clone(),
            stage: Stage::Create,
        },
        &token(),
    )
    .await
    .unwrap();
    assert_eq!(replay, Observation::Known(deleted));
    change(&client, Change::Delete(value.id)).await;
    assert_eq!(
        client
            .workflow(value.id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state,
        completed.state
    );
    source.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_provider_outage_projects_review_and_operator_reconciles_same_resource() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let source = local(store.clone(), &root.path().join("source"), "source").await;
    let receiver = local(store, &root.path().join("provider"), "provider").await;
    let fixture = root.path().join("fault");
    write(&fixture, b"down\n", false).unwrap();
    let (external, endpoint) = provider(&receiver, fixture.clone()).await;
    let handle = source
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = open(&source, &handle).await.unwrap();
    let value = spec(&endpoint);
    change(&client, Change::Request(value.clone())).await;
    spawn_workers(&source, handle).await.unwrap();
    let review = wait(&client, &source, value.id, Phase::NeedsReview)
        .await
        .unwrap();
    assert_eq!(review.state.provider_attempts, 8);
    assert!(
        external
            .provider(value.id, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    write(&fixture, b"up\n", true).unwrap();
    let input = Reconcile {
        resource: value.id,
        token: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap(),
    };
    client
        .prepare_reconcile(new_identity().unwrap(), input.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let active = wait(&client, &source, value.id, Phase::Active)
        .await
        .unwrap();
    assert_eq!(active.run_id, review.run_id);
    assert_eq!(active.state.reconciliations, 1);
    client
        .prepare_reconcile(new_identity().unwrap(), input)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    change(&client, Change::Delete(value.id)).await;
    wait(&client, &source, value.id, Phase::Completed)
        .await
        .unwrap();
    let deleted = external
        .provider(value.id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!((deleted.creates, deleted.deletes), (1, 1));
    source.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}
#[tokio::test]
async fn http_ingress_binds_credentials_origin_body_and_permanent_provider_request() {
    let root = tempfile::tempdir().unwrap();
    let receiver = local(
        Store::new(Arc::new(InMemory::new())),
        root.path(),
        "provider",
    )
    .await;
    let fixture = root.path().join("fault");
    write(&fixture, b"up\n", false).unwrap();
    let (external, endpoint) = provider(&receiver, fixture).await;
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let value = spec(&endpoint);
    let work = ProviderWork::new(value.clone(), ProviderAction::Create);
    let url = format!("{endpoint}resource");
    assert_eq!(
        http.post(&url).json(&work).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let mut wrong = work.clone();
    wrong.operation_key[0] ^= 1;
    assert_eq!(
        http.post(&url)
            .bearer_auth(token())
            .json(&wrong)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let mut foreign = value.clone();
    foreign.provider_endpoint = "http://127.0.0.1:1/".into();
    assert_eq!(
        http.post(&url)
            .bearer_auth(token())
            .json(&ProviderWork::new(foreign, ProviderAction::Create))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(
        http.post(&url)
            .bearer_auth(token())
            .header("content-type", "application/json")
            .body(vec![b' '; 4097])
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );
    assert!(
        external
            .provider(value.id, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert_eq!(
        http.post(&url)
            .bearer_auth(token())
            .json(&work)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    let mut changed = value.clone();
    changed.capacity_mib += 1;
    assert_eq!(
        http.post(&url)
            .bearer_auth(token())
            .json(&ProviderWork::new(changed, ProviderAction::Create))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resource = external
            .provider(value.id, None)
            .await
            .unwrap()
            .output
            .unwrap();
        if resource.phase == ProviderPhase::Ready {
            assert_eq!(resource.creates, 1);
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    receiver.shutdown().await.unwrap();
}
#[tokio::test]
async fn independently_restarted_provider_resumes_durable_creation_and_deletion_deadlines() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = local(store.clone(), &root.path().join("first"), "provider").await;
    let handle = first
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = ProvisioningClient::new(handle);
    first
        .open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    let value = spec("http://127.0.0.1:19026/");
    client
        .prepare_provider(
            new_identity().unwrap(),
            ProviderWork::new(value.clone(), ProviderAction::Create),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = local(store.clone(), &root.path().join("second"), "provider").await;
    let handle = second
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = ProvisioningClient::new(handle);
    second
        .open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    let creating = client
        .provider(value.id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(creating.phase, ProviderPhase::Creating);
    spawn_provider_lifecycle(&second, client.clone()).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if client
            .provider(value.id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .phase
            == ProviderPhase::Ready
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    second.shutdown().await.unwrap();
    let third = local(store.clone(), &root.path().join("third"), "provider").await;
    let handle = third
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = ProvisioningClient::new(handle);
    third
        .open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    client
        .prepare_provider(
            new_identity().unwrap(),
            ProviderWork::new(value.clone(), ProviderAction::Delete),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    third.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let fourth = local(store, &root.path().join("fourth"), "provider").await;
    let handle = fourth
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = ProvisioningClient::new(handle);
    fourth
        .open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    assert_eq!(
        client
            .provider(value.id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .phase,
        ProviderPhase::Deleting
    );
    spawn_provider_lifecycle(&fourth, client.clone()).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let deleted = client
            .provider(value.id, None)
            .await
            .unwrap()
            .output
            .unwrap();
        if deleted.phase == ProviderPhase::Deleted {
            assert_eq!(deleted.resource_id, creating.resource_id);
            assert_eq!((deleted.creates, deleted.deletes), (1, 1));
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    fourth.shutdown().await.unwrap();
}
#[tokio::test]
async fn idle_provider_lifecycle_does_not_publish_empty_advancement_commands() {
    let root = tempfile::tempdir().unwrap();
    let node = local(
        Store::new(Arc::new(InMemory::new())),
        root.path(),
        "provider",
    )
    .await;
    let handle = node
        .application_handle::<ProvisioningApplication>(TENANT)
        .unwrap();
    let client = ProvisioningClient::new(handle);
    node.open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    let before = client
        .provider(spec("http://127.0.0.1:19026/").id, None)
        .await
        .unwrap()
        .receipt;
    spawn_provider_lifecycle(&node, client.clone()).unwrap();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let after = client
        .provider(spec("http://127.0.0.1:19026/").id, None)
        .await
        .unwrap()
        .receipt;
    assert_eq!(
        after.commit_sequence, before.commit_sequence,
        "idle provider scanning must not create durable command evidence"
    );
    node.shutdown().await.unwrap();
}
