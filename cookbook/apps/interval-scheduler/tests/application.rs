//! Public reminder controls, receipt domains, restart, and signed delivery evidence.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_interval_scheduler::{
    Change, DeliveryOptions, DeliveryPageRequest, INBOX, IntervalScheduler, RecordOutcome,
    RecordReminder, Reminder, SCHEDULES, ScheduleId, SchedulerClient, compile, open,
    spawn_delivery,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, InvocationError, MutationIdentity, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    primitives::cron::{CronInvocation, CronMutationOutcome},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0x85; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x86; 16]);
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
fn id() -> ScheduleId {
    ScheduleId::from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap()
}
fn content() -> Reminder {
    Reminder {
        title: "Review".into(),
        message: "Review the published report".into(),
    }
}
async fn start(
    store: Store,
    root: &std::path::Path,
) -> (
    LocalNode,
    ApplicationHandle<IntervalScheduler>,
    SchedulerClient,
) {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("test-interval-scheduler"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = open(&node, TENANT, APP).await.unwrap();
    let client = SchedulerClient::new(handle.clone());
    (node, handle, client)
}
fn upsert(id: ScheduleId, identity: MutationIdentity, interval_ms: u64, delay: i64) -> Change {
    Change::Upsert {
        id,
        reminder: content(),
        interval_ms,
        next_due_ms: identity.issued_at_ms + delay,
    }
}
async fn wait_source(
    node: &LocalNode,
    client: &SchedulerClient,
    id: ScheduleId,
    minimum: u64,
) -> u64 {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(node.is_ready());
            let schedule = client.get(id, None).await.unwrap().output.unwrap();
            if schedule.occurrence >= minimum {
                return schedule.occurrence;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}
async fn deliveries(
    client: &SchedulerClient,
    id: ScheduleId,
) -> cellule_cookbook_interval_scheduler::DeliveryPage {
    client
        .deliveries(
            DeliveryPageRequest {
                schedule: id,
                after: None,
                limit: 100,
            },
            None,
        )
        .await
        .unwrap()
        .output
}
async fn wait_delivered(
    node: &LocalNode,
    client: &SchedulerClient,
    id: ScheduleId,
    count: u64,
) -> cellule_cookbook_interval_scheduler::DeliveryPage {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            assert!(node.is_ready());
            let page = deliveries(client, id).await;
            if page.deliveries.len() as u64 == count {
                return page;
            }
            assert!((page.deliveries.len() as u64) < count);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn owned_ticks_and_effects_progress_controls_without_manual_tick_calls() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle, client) = start(store(), root.path()).await;
    let id = id();
    let identity = new_identity().unwrap();
    let first = client
        .change(identity, upsert(id, identity, 1000, 50))
        .await
        .unwrap();
    assert_eq!(
        client
            .change(identity, upsert(id, identity, 1000, 50))
            .await
            .unwrap(),
        first
    );
    spawn_delivery(&node, handle, DeliveryOptions::default())
        .await
        .unwrap();
    wait_source(&node, &client, id, 2).await;
    let paused = client
        .change(new_identity().unwrap(), Change::Pause { id })
        .await
        .unwrap();
    let paused_state = client
        .get(id, Some(paused.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(!paused_state.enabled);
    let initial = wait_delivered(&node, &client, id, paused_state.occurrence).await;
    let mut actual_due = initial
        .deliveries
        .iter()
        .map(|delivery| delivery.scheduled_at_ms)
        .collect::<Vec<_>>();
    actual_due.sort_unstable();
    assert_eq!(
        actual_due,
        (0..paused_state.occurrence)
            .map(|offset| identity.issued_at_ms + 50 + offset as i64 * 1000)
            .collect::<Vec<_>>()
    );
    // A source receipt proves only the source. The destination rejects it.
    assert!(
        client
            .deliveries(
                DeliveryPageRequest {
                    schedule: id,
                    after: None,
                    limit: 100
                },
                Some(first.receipt)
            )
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(1150)).await;
    assert_eq!(
        client
            .get(id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .occurrence,
        paused_state.occurrence
    );
    let identity = new_identity().unwrap();
    client
        .change(
            identity,
            Change::Resume {
                id,
                next_due_ms: identity.issued_at_ms + 50,
            },
        )
        .await
        .unwrap();
    wait_source(&node, &client, id, paused_state.occurrence + 1).await;
    let paused = client
        .change(new_identity().unwrap(), Change::Pause { id })
        .await
        .unwrap();
    let next = client
        .get(id, Some(paused.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(next.generation > paused_state.generation);
    wait_delivered(&node, &client, id, next.occurrence).await;
    let identity = new_identity().unwrap();
    let deleted = client
        .change(identity, Change::Delete { id })
        .await
        .unwrap();
    assert_eq!(deleted.output, CronMutationOutcome::Deleted);
    assert!(
        client
            .get(id, Some(deleted.receipt))
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert_eq!(
        client
            .change(identity, Change::Delete { id })
            .await
            .unwrap(),
        deleted
    );
    let missing = new_identity().unwrap();
    let prepared = client.prepare(missing, Change::Pause { id }).await.unwrap();
    let evidence = prepared.evidence().clone();
    assert!(
        matches!(prepared.execute().await,Err(InvocationError::Rejected(result)) if result.output==CronMutationOutcome::NotFound)
    );
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn restart_restores_published_tick_intents_before_delivery_and_catches_up_all_due_occurrences()
 {
    let root = tempfile::tempdir().unwrap();
    let store = store();
    let (node, _, client) = start(store.clone(), root.path()).await;
    let id = id();
    let mut identity = new_identity().unwrap();
    identity.issued_at_ms -= 4000;
    let prepared = client
        .prepare(identity, upsert(id, identity, 1000, 0))
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let created = prepared.execute().await.unwrap();
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(stored) if stored.commit_sequence()==created.receipt.commit_sequence)
    );
    wait_source(&node, &client, id, 4).await;
    client
        .change(new_identity().unwrap(), Change::Pause { id })
        .await
        .unwrap();
    let paused = client.get(id, None).await.unwrap().output.unwrap();
    assert!(paused.occurrence >= 4);
    assert!(deliveries(&client, id).await.deliveries.is_empty());
    node.shutdown().await.unwrap();
    drop(node);
    std::fs::remove_dir_all(root.path()).unwrap();
    std::fs::create_dir_all(root.path()).unwrap();
    let (node, handle, client) = start(store, root.path()).await;
    assert_eq!(client.get(id, None).await.unwrap().output.unwrap(), paused);
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(stored) if stored.commit_sequence()==created.receipt.commit_sequence)
    );
    spawn_delivery(&node, handle, DeliveryOptions::default())
        .await
        .unwrap();
    let received = wait_delivered(&node, &client, id, paused.occurrence).await;
    let mut occurrences = received
        .deliveries
        .iter()
        .map(|delivery| delivery.occurrence)
        .collect::<Vec<_>>();
    occurrences.sort_unstable();
    assert_eq!(occurrences, (1..=paused.occurrence).collect::<Vec<_>>());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn reply_loss_resolves_the_signed_inbox_before_source_ack_without_repeating_domain_work() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle, client) = start(store(), root.path()).await;
    let id = id();
    let identity = new_identity().unwrap();
    client
        .change(identity, upsert(id, identity, 60_000, 50))
        .await
        .unwrap();
    let (sender, mut events) = tokio::sync::mpsc::channel(8);
    spawn_delivery(
        &node,
        handle,
        DeliveryOptions {
            drop_reply_once: true,
            progress: Some(sender),
            ..DeliveryOptions::default()
        },
    )
    .await
    .unwrap();
    let checkpoint = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((checkpoint.schedule, checkpoint.occurrence), (id, 1));
    let page = wait_delivered(&node, &client, id, 1).await;
    assert_eq!(page.deliveries[0].row, checkpoint.row);
    // Drain waits for the native supervisor's resolve and source acknowledgement.
    node.shutdown().await.unwrap();
    assert!(events.try_recv().is_err());
}
#[tokio::test]
async fn deleting_and_recreating_the_same_id_uses_a_new_definition_lifetime() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle, client) = start(store(), root.path()).await;
    let id = id();
    spawn_delivery(&node, handle, DeliveryOptions::default())
        .await
        .unwrap();
    let first = new_identity().unwrap();
    client
        .change(first, upsert(id, first, 60_000, 50))
        .await
        .unwrap();
    let initial = wait_delivered(&node, &client, id, 1).await;
    client
        .change(new_identity().unwrap(), Change::Delete { id })
        .await
        .unwrap();
    let second = new_identity().unwrap();
    client
        .change(second, upsert(id, second, 60_000, 50))
        .await
        .unwrap();
    let final_page = wait_delivered(&node, &client, id, 2).await;
    assert_eq!(initial.deliveries[0].generation, 1);
    assert_eq!(final_page.deliveries[1].generation, 1);
    assert_eq!(initial.deliveries[0].occurrence, 1);
    assert_eq!(final_page.deliveries[1].occurrence, 1);
    assert_ne!(
        initial.deliveries[0].definition,
        final_page.deliveries[1].definition
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn published_intents_survive_delete_and_domain_receiver_dedup_detects_payload_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle, client) = start(store(), root.path()).await;
    let id = id();
    let identity = new_identity().unwrap();
    client
        .change(identity, upsert(id, identity, 60_000, 50))
        .await
        .unwrap();
    wait_source(&node, &client, id, 1).await;
    client
        .change(new_identity().unwrap(), Change::Delete { id })
        .await
        .unwrap();
    assert!(client.get(id, None).await.unwrap().output.is_none());
    spawn_delivery(&node, handle.clone(), DeliveryOptions::default())
        .await
        .unwrap();
    let page = wait_delivered(&node, &client, id, 1).await;
    let mut payload = BoundedEncoder::new(1024).unwrap();
    page.deliveries[0].definition.encode(&mut payload).unwrap();
    content().encode(&mut payload).unwrap();
    let mut invocation = CronInvocation {
        schedule_id: *id.as_bytes(),
        generation: 1,
        occurrence: 1,
        scheduled_at_ms: page.deliveries[0].scheduled_at_ms,
        payload: payload.finish(),
    };
    let inbox = handle.target_for_scope(INBOX, b"reminders").unwrap();
    assert_eq!(
        handle
            .command::<RecordReminder>(&inbox, new_identity().unwrap(), invocation.clone())
            .await
            .unwrap()
            .output,
        RecordOutcome::Recorded {
            row: page.deliveries[0].row
        }
    );
    invocation.scheduled_at_ms += 1;
    assert!(
        matches!(handle.command::<RecordReminder>(&inbox,new_identity().unwrap(),invocation).await,Err(InvocationError::Rejected(result)) if result.output==RecordOutcome::Conflict)
    );
    assert_eq!(deliveries(&client, id).await, page);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn native_schedule_and_receiver_pages_are_bounded_and_cursors_do_not_skip_or_repeat_rows() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle, client) = start(store(), root.path()).await;
    let mut expected = [Vec::new(), Vec::new()];
    for _ in 0..12 {
        let id = id();
        let identity = new_identity().unwrap();
        client
            .change(identity, upsert(id, identity, 1000, 60_000))
            .await
            .unwrap();
        let shard = cellule_runtime::shard_for_scope(SCHEDULES, id.as_bytes(), 2).unwrap();
        expected[shard as usize].push(id);
    }
    for shard in 0..2 {
        expected[shard as usize].sort_unstable();
        let mut actual = Vec::new();
        let mut after = None;
        loop {
            let page = client.list(shard, after, 2, None).await.unwrap().output;
            assert!(page.schedules.len() <= 2);
            actual.extend(page.schedules.iter().map(|schedule| schedule.id));
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        assert_eq!(actual, expected[shard as usize]);
    }
    assert!(client.list(2, None, 1, None).await.is_err());
    assert!(client.list(0, None, 0, None).await.is_err());
    assert!(client.list(0, None, 101, None).await.is_err());
    let id = id();
    let mut identity = new_identity().unwrap();
    identity.issued_at_ms -= 4000;
    client
        .change(identity, upsert(id, identity, 1000, 0))
        .await
        .unwrap();
    wait_source(&node, &client, id, 4).await;
    client
        .change(new_identity().unwrap(), Change::Pause { id })
        .await
        .unwrap();
    let count = client
        .get(id, None)
        .await
        .unwrap()
        .output
        .unwrap()
        .occurrence;
    spawn_delivery(&node, handle, DeliveryOptions::default())
        .await
        .unwrap();
    let all = wait_delivered(&node, &client, id, count).await;
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = client
            .deliveries(
                DeliveryPageRequest {
                    schedule: id,
                    after,
                    limit: 2,
                },
                None,
            )
            .await
            .unwrap()
            .output;
        actual.extend(page.deliveries);
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(actual, all.deliveries);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn graceful_drain_settles_delivery_after_the_destination_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let store = store();
    let (node, handle, client) = start(store.clone(), root.path()).await;
    let id = id();
    let identity = new_identity().unwrap();
    client
        .change(identity, upsert(id, identity, 60_000, 50))
        .await
        .unwrap();
    let (sender, mut events) = tokio::sync::mpsc::channel(8);
    spawn_delivery(
        &node,
        handle,
        DeliveryOptions {
            after_publication: Duration::from_secs(10),
            progress: Some(sender),
            ..DeliveryOptions::default()
        },
    )
    .await
    .unwrap();
    let checkpoint = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), node.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(node);
    let (node, handle, client) = start(store, root.path()).await;
    let (sender, mut events) = tokio::sync::mpsc::channel(8);
    spawn_delivery(
        &node,
        handle,
        DeliveryOptions {
            progress: Some(sender),
            ..DeliveryOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        deliveries(&client, id).await.deliveries[0].row,
        checkpoint.row
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(events.try_recv().is_err());
    node.shutdown().await.unwrap();
}
#[test]
fn version_one_identity_payload_and_ingress_bounds_have_stable_fixtures() {
    let id = ScheduleId::parse("018f7ce0-67d0-7000-8000-000000000001").unwrap();
    assert_eq!(
        serde_json::to_string(&id).unwrap(),
        "\"018f7ce0-67d0-7000-8000-000000000001\""
    );
    assert!(ScheduleId::parse("018F7CE0-67D0-7000-8000-000000000001").is_err());
    assert!(ScheduleId::from_bytes([0; 16]).is_err());
    let reminder = content();
    let mut encoder = BoundedEncoder::new(1024).unwrap();
    id.encode(&mut encoder).unwrap();
    reminder.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut expected = vec![
        0, 0, 0, 16, 0x01, 0x8f, 0x7c, 0xe0, 0x67, 0xd0, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01,
    ];
    expected.extend_from_slice(b"\0\0\0\x06Review\0\0\0\x1bReview the published report");
    assert_eq!(bytes, expected);
    let mut decoder = BoundedDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(ScheduleId::decode(&mut decoder).unwrap(), id);
    assert_eq!(Reminder::decode(&mut decoder).unwrap(), reminder);
    decoder.finish().unwrap();
    let identity = new_identity().unwrap();
    assert!(
        upsert(id, identity, 999, 0)
            .validate(identity.issued_at_ms)
            .is_err()
    );
    assert!(
        upsert(id, identity, 1000, -1)
            .validate(identity.issued_at_ms)
            .is_err()
    );
    assert!(
        upsert(id, identity, 1000, 0)
            .validate(identity.issued_at_ms)
            .is_ok()
    );
    assert!(
        Reminder {
            title: " padded ".into(),
            message: "ok".into()
        }
        .validate()
        .is_err()
    );
}
