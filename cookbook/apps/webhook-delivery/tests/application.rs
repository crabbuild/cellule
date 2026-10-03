//! Public source, delivery inbox, receiver idempotency, and exact-root recovery contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_cookbook_webhook_delivery::*;
use cellule_runtime::{ApplicationId, InvocationError, Resolution, TenantId};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0x24; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x25; 16]);
fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}
fn id(value: u128) -> EventId {
    EventId::from_bytes(value.to_be_bytes()).unwrap()
}
async fn start(store: Store, path: &std::path::Path) -> (LocalNode, WebhookClient) {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("webhook-tests"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = node
        .application_handle::<WebhookApplication>(TENANT)
        .unwrap();
    let client = open(&node, handle).await.unwrap();
    (node, client)
}
async fn setup() -> (LocalNode, WebhookClient, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let (node, client) = start(Store::new(Arc::new(InMemory::new())), directory.path()).await;
    (node, client, directory)
}
async fn subscribe(
    client: &WebhookClient,
    name: &str,
    topic: &str,
    port: u16,
    enabled: bool,
    revision: i64,
) -> Subscription {
    client
        .change(
            new_identity().unwrap(),
            Action::Subscribe {
                id: key(name),
                topic: key(topic),
                endpoint: Endpoint::new(format!("http://127.0.0.1:{port}/deliver")).unwrap(),
                enabled,
                expected_revision: revision,
            },
        )
        .await
        .unwrap()
        .output
        .subscription
        .unwrap()
}
async fn publish(client: &WebhookClient, n: u128, payload: &str) -> PublishedEvent {
    client
        .change(
            new_identity().unwrap(),
            Action::Publish {
                id: id(n),
                topic: key("orders"),
                payload: payload.into(),
            },
        )
        .await
        .unwrap()
        .output
        .event
        .unwrap()
}
#[test]
fn canonical_keys_and_loopback_endpoints_reject_aliases() {
    for invalid in [
        "",
        "A",
        " leading",
        "trailing ",
        "-left",
        "right-",
        "a/b",
        "a_b",
        "a\0b",
    ] {
        assert!(Key::new(invalid).is_err(), "{invalid:?}");
    }
    assert!(Key::new("a".repeat(65)).is_err());
    assert!(Key::new("orders-v1").is_ok());
    assert!(EventId::from_bytes([0; 16]).is_err());
    for invalid in [
        "http://localhost:9000/deliver",
        "http://127.0.0.1:0/deliver",
        "http://127.0.0.1:80/deliver",
        "http://127.0.0.1:9000/deliver?x=1",
        "http://127.0.0.1:9000/deliver#x",
        "http://u@127.0.0.1:9000/deliver",
        "http://127.0.0.1:9000/other",
        "https://127.0.0.1:9000/deliver",
        "http://127.0.0.2:9000/deliver",
        "http://127.0.0.1:09000/deliver",
    ] {
        assert!(Endpoint::new(invalid).is_err(), "{invalid}");
    }
    assert!(Endpoint::new("http://127.0.0.1:9000/deliver").is_ok());
}
#[tokio::test]
async fn source_replay_retains_original_subscribers_and_effect_ids() {
    let (node, client, _dir) = setup().await;
    subscribe(&client, "alice", "orders", 19020, true, 0).await;
    subscribe(&client, "unrelated", "payments", 19020, true, 0).await;
    subscribe(&client, "disabled", "orders", 19020, false, 0).await;
    let first = publish(&client, 1, "order-created").await;
    assert_eq!(first.deliveries.len(), 1);
    let frozen = &first.deliveries[0];
    assert_eq!(frozen.effect_id.len(), 32);
    subscribe(&client, "alice", "orders", 19021, false, 1).await;
    subscribe(&client, "bob", "orders", 19021, true, 0).await;
    let replay = client
        .change(
            new_identity().unwrap(),
            Action::Publish {
                id: id(1),
                topic: key("orders"),
                payload: "order-created".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(replay.output.decision, Decision::ExistingEvent);
    assert_eq!(replay.output.event.unwrap(), first);
    let next = publish(&client, 2, "order-created").await;
    assert_eq!(next.deliveries.len(), 1);
    assert_eq!(next.deliveries[0].ticket.subscription, key("bob"));
    assert_ne!(frozen.ticket.key(), next.deliveries[0].ticket.key());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn source_revision_and_event_conflicts_are_durable_native_rejections() {
    let (node, client, _dir) = setup().await;
    subscribe(&client, "alice", "orders", 19020, true, 0).await;
    let outcome = subscribe(&client, "alice", "orders", 19020, true, 1).await;
    assert_eq!(outcome.revision, 1);
    let action = Action::Subscribe {
        id: key("alice"),
        topic: key("orders"),
        endpoint: Endpoint::new("http://127.0.0.1:19021/deliver").unwrap(),
        enabled: true,
        expected_revision: 0,
    };
    let prepared = client
        .prepare(new_identity().unwrap(), action)
        .await
        .unwrap();
    let Err(InvocationError::Rejected(first)) = prepared.clone().execute().await else {
        panic!("stale revision accepted")
    };
    assert_eq!(first.output.decision, Decision::Conflict);
    let Err(InvocationError::Rejected(replay)) = prepared.clone().execute().await else {
        panic!("durable revision rejection missing")
    };
    assert_eq!(first.receipt, replay.receipt);
    assert!(matches!(
        client.resolve(prepared.evidence()).await.unwrap(),
        Resolution::Committed(_)
    ));
    let original = publish(&client, 10, "first").await;
    let Err(InvocationError::Rejected(conflict)) = client
        .change(
            new_identity().unwrap(),
            Action::Publish {
                id: id(10),
                topic: key("orders"),
                payload: "changed".into(),
            },
        )
        .await
    else {
        panic!("changed event accepted")
    };
    assert_eq!(conflict.output.decision, Decision::Conflict);
    assert_eq!(conflict.output.event.unwrap(), original);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn source_bounds_do_not_mutate_on_invalid_payload_or_exhausted_subscribers() {
    let (node, client, _dir) = setup().await;
    for n in 0..MAX_SUBSCRIBERS {
        subscribe(&client, &format!("sub-{n}"), "orders", 19020, true, 0).await;
    }
    let Err(InvocationError::Rejected(capacity)) = client
        .change(
            new_identity().unwrap(),
            Action::Subscribe {
                id: key("overflow"),
                topic: key("orders"),
                endpoint: Endpoint::new("http://127.0.0.1:19020/deliver").unwrap(),
                enabled: true,
                expected_revision: 0,
            },
        )
        .await
    else {
        panic!("subscriber bound missing")
    };
    assert_eq!(capacity.output.decision, Decision::Capacity);
    for payload in [String::new(), "x".repeat(MAX_PAYLOAD + 1)] {
        let Err(InvocationError::Rejected(rejected)) = client
            .change(
                new_identity().unwrap(),
                Action::Publish {
                    id: id(1),
                    topic: key("orders"),
                    payload,
                },
            )
            .await
        else {
            panic!("invalid payload accepted")
        };
        assert_eq!(rejected.output.decision, Decision::Invalid);
    }
    assert!(client.event(id(1), None).await.unwrap().output.is_none());
    let event = publish(&client, 1, &"\u{0001}".repeat(MAX_PAYLOAD)).await;
    assert_eq!(event.deliveries.len(), MAX_SUBSCRIBERS);
    assert_eq!(
        client
            .subscriptions(None)
            .await
            .unwrap()
            .output
            .subscriptions
            .len(),
        MAX_SUBSCRIBERS
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn receiver_applies_once_and_preserves_actual_drop_reply_fact() {
    let (node, client, _dir) = setup().await;
    subscribe(&client, "alice", "orders", 19020, true, 0).await;
    client
        .policy(
            new_identity().unwrap(),
            ReceiverPolicy {
                subscription: key("alice"),
                mode: ReceiverMode::DropOnce,
            },
        )
        .await
        .unwrap();
    let ticket = publish(&client, 1, "order")
        .await
        .deliveries
        .remove(0)
        .ticket;
    let prepared = client
        .prepare_receive(new_identity().unwrap(), ticket.clone())
        .await
        .unwrap();
    let first = prepared.clone().execute().await.unwrap();
    assert!(first.output.drop_reply);
    assert_eq!(first.output.status, 200);
    assert!(first.output.record.as_ref().unwrap().applied);
    let replay = prepared.clone().execute().await.unwrap();
    assert_eq!(first.receipt, replay.receipt);
    assert_eq!(first.output, replay.output);
    let second = client
        .prepare_receive(new_identity().unwrap(), ticket.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(!second.output.drop_reply);
    assert_eq!(second.output.record.unwrap().requests, 2);
    let record = client
        .received(ticket.key(), Some(second.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(record.applied);
    assert_eq!(record.requests, 2);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn receiver_transient_terminal_conflict_and_foreign_scope_are_independent() {
    let (node, client, _dir) = setup().await;
    for name in ["transient", "terminal"] {
        subscribe(&client, name, "orders", 19020, true, 0).await;
    }
    client
        .policy(
            new_identity().unwrap(),
            ReceiverPolicy {
                subscription: key("transient"),
                mode: ReceiverMode::TransientOnce,
            },
        )
        .await
        .unwrap();
    client
        .policy(
            new_identity().unwrap(),
            ReceiverPolicy {
                subscription: key("terminal"),
                mode: ReceiverMode::Terminal,
            },
        )
        .await
        .unwrap();
    let event = publish(&client, 1, "order").await;
    for item in event.deliveries {
        let ticket = item.ticket;
        let first = client
            .prepare_receive(new_identity().unwrap(), ticket.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        let second = client
            .prepare_receive(new_identity().unwrap(), ticket.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        if ticket.subscription == key("transient") {
            assert_eq!(first.output.status, 503);
            assert!(!first.output.record.unwrap().applied);
            assert_eq!(second.output.status, 200);
            assert!(second.output.record.unwrap().applied);
        } else {
            assert_eq!(first.output.status, 422);
            assert_eq!(second.output.status, 422);
            assert!(!second.output.record.unwrap().applied);
        }
        let mut changed = ticket.clone();
        changed.payload = "tampered".into();
        assert_eq!(changed.key(), ticket.key());
        assert!(
            matches!(client.prepare_receive(new_identity().unwrap(),changed).await.unwrap().execute().await,Err(InvocationError::Rejected(v)) if v.output.status==409 && v.output.record.is_none())
        );
        let mut foreign = ticket;
        foreign.source_cell[0] ^= 1;
        assert!(
            matches!(client.prepare_receive(new_identity().unwrap(),foreign).await.unwrap().execute().await,Err(InvocationError::Rejected(v)) if v.output.status==403)
        );
    }
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn source_effect_runner_uses_native_delivery_inbox_and_pinned_workflow() {
    let (node, client, _dir) = setup().await;
    spawn_fanout(
        &node,
        node.application_handle::<WebhookApplication>(TENANT)
            .unwrap(),
    )
    .await
    .unwrap();
    subscribe(&client, "alice", "orders", 19020, true, 0).await;
    let ticket = publish(&client, 1, &"\u{0001}".repeat(MAX_PAYLOAD))
        .await
        .deliveries
        .remove(0)
        .ticket;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    let view = loop {
        assert!(node.is_ready());
        if let Some(view) = client.delivery(ticket.key(), None).await.unwrap().output {
            break view;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "source fan-out did not start native Workflow"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(view.state.ticket, ticket);
    assert_eq!(view.state.phase, Phase::InFlight);
    assert_eq!(view.state.round, 1);
    assert!(view.state.may_have_applied());
    let duplicate = client
        .prepare_delivery(new_identity().unwrap(), ticket.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(matches!(
        duplicate.output,
        cellule_runtime::primitives::workflow::WorkflowOutcome::AlreadyExists
    ));
    let mut changed = ticket;
    changed.endpoint = Endpoint::new("http://127.0.0.1:19021/deliver").unwrap();
    assert!(matches!(
        client
            .prepare_delivery(new_identity().unwrap(), changed)
            .await
            .unwrap()
            .execute()
            .await,
        Err(InvocationError::NotStarted(_))
    ));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn native_http_rounds_exhaust_conservatively_and_keep_stable_business_identity() {
    let (node, client, _dir) = setup().await;
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = unavailable.local_addr().unwrap().port();
    drop(unavailable);
    subscribe(&client, "alice", "orders", port, true, 0).await;
    let ticket = publish(&client, 1, &"\u{0001}".repeat(MAX_PAYLOAD))
        .await
        .deliveries
        .remove(0)
        .ticket;
    client
        .prepare_delivery(new_identity().unwrap(), ticket.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    spawn_http_activities(
        &node,
        node.application_handle::<WebhookApplication>(TENANT)
            .unwrap(),
    )
    .unwrap();
    let until = tokio::time::Instant::now() + Duration::from_secs(15);
    let view = loop {
        assert!(node.is_ready());
        let view = client
            .delivery(ticket.key(), None)
            .await
            .unwrap()
            .output
            .unwrap();
        if view.state.phase == Phase::Exhausted {
            break view;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "HTTP retries did not exhaust: {:?}",
            view.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(view.state.attempts.len(), MAX_ROUNDS as usize);
    assert_eq!(view.state.ticket, ticket);
    for (index, attempt) in view.state.attempts.iter().enumerate() {
        assert_eq!(attempt.round, index as u32 + 1);
        assert_eq!(attempt.classification, Classification::Retryable);
        assert_eq!(attempt.status, None);
        assert!(!attempt.may_have_applied);
        assert!(!attempt.details.is_empty());
    }
    assert!(!view.state.may_have_applied());
    assert!(
        client
            .received(ticket.key(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_keeps_receiver_deduplication_and_frozen_event() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let (node, client) = start(store.clone(), &directory.path().join("first")).await;
    subscribe(&client, "alice", "orders", 19020, true, 0).await;
    client
        .policy(
            new_identity().unwrap(),
            ReceiverPolicy {
                subscription: key("alice"),
                mode: ReceiverMode::DropOnce,
            },
        )
        .await
        .unwrap();
    let original = publish(&client, 1, "order").await;
    let ticket = original.deliveries[0].ticket.clone();
    let receipt = client
        .prepare_receive(new_identity().unwrap(), ticket.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap()
        .receipt;
    node.shutdown().await.unwrap();
    drop(client);
    drop(node);
    let (node, client) = start(store, &directory.path().join("restored")).await;
    assert_eq!(
        client.event(id(1), None).await.unwrap().output.unwrap(),
        original
    );
    assert_eq!(
        client
            .received(ticket.key(), Some(receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .requests,
        1
    );
    let replay = client
        .prepare_receive(new_identity().unwrap(), ticket.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(replay.output.status, 200);
    assert!(!replay.output.drop_reply);
    assert_eq!(replay.output.record.unwrap().requests, 2);
    assert_eq!(publish(&client, 1, "order").await, original);
    node.shutdown().await.unwrap();
}
