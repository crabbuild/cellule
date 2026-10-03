#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use cellule_cookbook_webhook_delivery::{
    Acknowledgement, DeliveryTicket, ReceiverMode, ReceiverPolicy, receiver_token,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path as ObjectPath};
use std::sync::Arc;
async fn start_test(store: Store, state: &Path) -> (LocalNode, WebhookClient) {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: state.into(),
            storage_prefix: ObjectPath::from("webhook-http-tests"),
            application_id: APPLICATION,
        },
    )
    .await
    .unwrap();
    let client = client(&node).await.unwrap();
    (node, client)
}
async fn setup() -> (LocalNode, WebhookClient, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let (node, client) = start_test(Store::new(Arc::new(InMemory::new())), directory.path()).await;
    (node, client, directory)
}
async fn ticket(client: &WebhookClient, port: u16, mode: ReceiverMode) -> DeliveryTicket {
    let name = Key::new("http-test").unwrap();
    let topic = Key::new("orders").unwrap();
    client
        .change(
            new_identity().unwrap(),
            Action::Subscribe {
                id: name.clone(),
                topic: topic.clone(),
                endpoint: Endpoint::new(format!("http://127.0.0.1:{port}/deliver")).unwrap(),
                enabled: true,
                expected_revision: 0,
            },
        )
        .await
        .unwrap();
    client
        .policy(
            new_identity().unwrap(),
            ReceiverPolicy {
                subscription: name,
                mode,
            },
        )
        .await
        .unwrap();
    client
        .change(
            new_identity().unwrap(),
            Action::Publish {
                id: EventId::from_bytes(1u128.to_be_bytes()).unwrap(),
                topic,
                payload: "\u{0001}".repeat(1024),
            },
        )
        .await
        .unwrap()
        .output
        .event
        .unwrap()
        .deliveries
        .remove(0)
        .ticket
}
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}
fn post(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    ticket: &DeliveryTicket,
) -> reqwest::RequestBuilder {
    client
        .post(format!("http://{address}/deliver"))
        .bearer_auth(receiver_token().unwrap())
        .header("Idempotency-Key", ticket.key_hex())
        .json(ticket)
}
#[test]
fn retained_evidence_and_ingress_identifiers_are_canonical() {
    let identity = new_identity().unwrap();
    let record = MutationFile {
        version: 1,
        request_id: uuid::Uuid::from_bytes(*identity.request_id.as_bytes()).to_string(),
        issued_at_ms: identity.issued_at_ms,
        expires_at_ms: identity.expires_at_ms,
        action: Action::Publish {
            id: EventId::from_bytes(1u128.to_be_bytes()).unwrap(),
            topic: Key::new("orders").unwrap(),
            payload: "order".into(),
        },
    };
    assert_eq!(record.identity().unwrap(), identity);
    let mut record = record;
    record.expires_at_ms += 1;
    assert!(record.identity().is_err());
    assert!(canonical_uuid("00000000-0000-0000-0000-000000000000").is_err());
    assert!(canonical_uuid("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA").is_err());
    assert!(delivery_key(&"A".repeat(64)).is_err());
    assert!(delivery_key(&"a".repeat(63)).is_err());
    assert_eq!(delivery_key(&"ab".repeat(32)).unwrap(), [0xab; 32]);
}
#[test]
fn preparation_never_overwrites_existing_evidence_and_preserves_wire_payload() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("input.json");
    let output = directory.path().join("mutation.json");
    let id = uuid::Uuid::now_v7();
    std::fs::write(&input,serde_json::json!({"kind":"publish","id":id.to_string(),"topic":"orders","payload":"original"}).to_string()).unwrap();
    prepare(&input, &output).unwrap();
    let original = std::fs::read(&output).unwrap();
    assert!(prepare(&input, &output).is_err());
    assert_eq!(std::fs::read(&output).unwrap(), original);
    let record: MutationFile = read_json(&output).unwrap();
    record.identity().unwrap();
    assert!(
        matches!(record.action,Action::Publish{id:found,payload,..} if found.bytes()==*id.as_bytes() && payload=="original")
    );
}
#[tokio::test]
async fn runnable_demo_exercises_native_fanout_and_actual_http_faults() {
    let (node, _client, _directory) = setup().await;
    demo::run(&node).await.unwrap();
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_reply_drop_commits_once_and_admission_rejects_foreign_or_changed_tickets() {
    let (node, client, _directory) = setup().await;
    let address = http::install(&node, client.clone(), 0, http::Options::default())
        .await
        .unwrap();
    let ticket = ticket(&client, address.port(), ReceiverMode::DropOnce).await;
    let http = http_client();
    let error = post(&http, address, &ticket).send().await.unwrap_err();
    assert!(!error.is_connect());
    let first = client
        .received(ticket.key(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(first.applied);
    assert_eq!(first.requests, 1);
    let reply = post(&http, address, &ticket).send().await.unwrap();
    assert_eq!(reply.status(), reqwest::StatusCode::OK);
    let ack: Acknowledgement = reply.json().await.unwrap();
    assert_eq!(ack.key, ticket.key_hex());
    assert_eq!(ack.content_digest, ticket.content_digest().unwrap());
    assert_eq!(ack.applied_count, 1);
    let mut changed = ticket.clone();
    changed.payload = "different".into();
    assert_eq!(
        post(&http, address, &changed)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let mut foreign = ticket.clone();
    foreign.source_cell[0] ^= 1;
    assert_eq!(
        post(&http, address, &foreign)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        post(&http, address, &ticket)
            .header("Idempotency-Key", ticket.key_hex().to_uppercase())
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let mut unauthorized = http
        .post(format!("http://{address}/deliver"))
        .header("Idempotency-Key", ticket.key_hex())
        .json(&ticket)
        .build()
        .unwrap();
    unauthorized.headers_mut().insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_static("Basic unsupported-credential"),
    );
    assert_eq!(
        http.execute(unauthorized).await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        http.get(format!("http://{address}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    let oversize = http
        .post(format!("http://{address}/deliver"))
        .bearer_auth(receiver_token().unwrap())
        .header("Idempotency-Key", ticket.key_hex())
        .header("Content-Type", "application/json")
        .body("x".repeat(8193))
        .send()
        .await
        .unwrap();
    assert_eq!(oversize.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert!(oversize.bytes().await.unwrap().len() < 128);
    let record = client
        .received(ticket.key(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(record.applied);
    assert_eq!(record.requests, 2);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn graceful_shutdown_drains_admitted_mutation_after_http_client_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let (node, client) = start_test(store.clone(), &directory.path().join("first")).await;
    let (sender, mut progress) = tokio::sync::mpsc::channel(8);
    let address = http::install(
        &node,
        client.clone(),
        0,
        http::Options {
            after_publication: Duration::from_secs(10),
            progress: Some(sender),
        },
    )
    .await
    .unwrap();
    let ticket = ticket(&client, address.port(), ReceiverMode::Good).await;
    let request = post(&http_client(), address, &ticket);
    let caller = tokio::spawn(async move { request.send().await });
    let committed = tokio::time::timeout(Duration::from_secs(5), progress.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(committed.applied);
    assert_eq!(committed.key, ticket.key_hex());
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(8), node.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(client);
    drop(node);
    let (node, client) = start_test(store, &directory.path().join("restored")).await;
    let record = client
        .received(ticket.key(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(record.applied);
    assert_eq!(record.requests, 1);
    node.shutdown().await.unwrap();
}
