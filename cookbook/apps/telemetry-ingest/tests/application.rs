//! Public typed application behavior with real native SQL, Queue, signed Effects, and cold recovery.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_cookbook_telemetry_ingest::*;
use cellule_runtime::{
    ApplicationId, Committed, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    primitives::queue::QueueSendOutcome,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0x39; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x49; 16]);
struct Harness {
    node: LocalNode,
    handle: ApplicationHandle<TelemetryIngest>,
}
async fn start(store: Store, root: &std::path::Path) -> Harness {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("telemetry-tests"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = node.application_handle::<TelemetryIngest>(TENANT).unwrap();
    Harness { node, handle }
}
async fn harness(root: &std::path::Path) -> Harness {
    start(Store::new(Arc::new(InMemory::new())), root).await
}
fn key(name: &str) -> DeviceKey {
    DeviceKey::new(name).unwrap()
}
fn window() -> Window {
    Window {
        start_ms: 120000,
        minutes: 3,
    }
}
fn event(sequence: i64, at_ms: i64, value: i64) -> Event {
    Event {
        device: key("thermometer"),
        sequence,
        at_ms,
        value_milli: value,
    }
}
fn batch(events: Vec<Event>) -> Batch {
    Batch {
        id: BatchId::parse(&uuid::Uuid::now_v7().to_string()).unwrap(),
        events,
    }
}
async fn device(h: &Harness) -> DeviceClient {
    let client = DeviceClient::new(h.handle.clone(), key("thermometer")).unwrap();
    h.node.open_cell(client.target(), &Devices).await.unwrap();
    client
}
fn rejected(error: InvocationError<DeviceOutcome>, expected: Decision) -> Committed<DeviceOutcome> {
    match error {
        InvocationError::Rejected(v) => {
            assert_eq!(v.output.decision, expected);
            assert!(v.output.version.is_none());
            *v
        }
        other => panic!("expected durable domain rejection: {other}"),
    }
}
fn roundtrip<T: WireValue + PartialEq + std::fmt::Debug>(value: &T, limit: u32) {
    let mut encoder = BoundedEncoder::new(limit).unwrap();
    value.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut decoder = BoundedDecoder::new(&bytes, limit).unwrap();
    assert_eq!(T::decode(&mut decoder).unwrap(), *value);
    decoder.finish().unwrap();
}
#[tokio::test]
async fn permanent_sequences_preserve_latest_gaps_and_durable_original_answers() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let client = device(&h).await;
    rejected(
        client
            .record(new_identity().unwrap(), event(1, 120500, 10))
            .await
            .unwrap_err(),
        Decision::NotRegistered,
    );
    let registration = client
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    assert_eq!(registration.output.version.unwrap().snapshot.revision, 1);
    let prepared = client
        .prepare_event(new_identity().unwrap(), event(3, 180500, 20000))
        .await
        .unwrap();
    let pending = prepared.evidence().clone();
    let first = prepared.execute().await.unwrap();
    let snapshot = &first.output.version.as_ref().unwrap().snapshot;
    assert_eq!(
        (snapshot.max_sequence, snapshot.contiguous_sequence),
        (3, 0)
    );
    let lower = client
        .record(new_identity().unwrap(), event(1, 120500, 10000))
        .await
        .unwrap();
    let lower_snapshot = lower.output.version.as_ref().unwrap().snapshot.clone();
    assert_eq!(
        (
            lower_snapshot.accepted,
            lower_snapshot.reordered,
            lower_snapshot.contiguous_sequence
        ),
        (2, 1, 1)
    );
    assert_eq!(lower_snapshot.latest, Some(event(3, 180500, 20000)));
    let duplicate = client
        .record(new_identity().unwrap(), event(3, 180500, 20000))
        .await
        .unwrap();
    assert_eq!(duplicate.output.decision, Decision::Duplicate);
    assert_eq!(duplicate.output.version, lower.output.version);
    let refusal = rejected(
        client
            .record(new_identity().unwrap(), event(3, 180500, 20001))
            .await
            .unwrap_err(),
        Decision::Conflict,
    );
    rejected(
        client
            .record(new_identity().unwrap(), event(2, 300000, 10))
            .await
            .unwrap_err(),
        Decision::OutsideWindow,
    );
    rejected(
        client
            .register(
                new_identity().unwrap(),
                Window {
                    start_ms: 180000,
                    minutes: 3,
                },
            )
            .await
            .unwrap_err(),
        Decision::Conflict,
    );
    assert_eq!(
        client
            .get(Some(refusal.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .snapshot()
            .unwrap(),
        lower_snapshot
    );
    let Resolution::Committed(original) = client.resolve(&pending).await.unwrap() else {
        panic!("original durable event evidence missing")
    };
    let mut decoder = BoundedDecoder::new(original.result(), 4096).unwrap();
    assert_eq!(DeviceOutcome::decode(&mut decoder).unwrap(), first.output);
    decoder.finish().unwrap();
    client
        .record(new_identity().unwrap(), event(2, 120501, -5000))
        .await
        .unwrap();
    let state = client.get(None).await.unwrap().output.unwrap();
    let snapshot = state.snapshot().unwrap();
    assert_eq!(snapshot.contiguous_sequence, 3);
    assert_eq!(snapshot.buckets[0].sum_milli, 5000);
    assert_eq!(snapshot.buckets[1].sum_milli, 20000);
    roundtrip(&state, 64 << 10);
    roundtrip(&snapshot, 4096);
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn monotonic_receiver_replaces_contributions_without_double_addition() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = device(&h).await;
    let summary = SummaryClient::for_device(h.handle.clone(), &key("thermometer")).unwrap();
    h.node
        .open_cell(summary.target(), &Summaries)
        .await
        .unwrap();
    let empty = source
        .register(new_identity().unwrap(), window())
        .await
        .unwrap()
        .output
        .version
        .unwrap()
        .snapshot;
    let older = source
        .record(new_identity().unwrap(), event(2, 180500, 20))
        .await
        .unwrap()
        .output
        .version
        .unwrap()
        .snapshot;
    let newest = source
        .record(new_identity().unwrap(), event(1, 120500, 10))
        .await
        .unwrap()
        .output
        .version
        .unwrap()
        .snapshot;
    let publish = |value| {
        h.handle
            .command::<PublishSummary>(summary.target(), new_identity().unwrap(), value)
    };
    assert_eq!(
        publish(newest.clone()).await.unwrap().output,
        ProjectionOutcome::Applied
    );
    assert_eq!(
        publish(older).await.unwrap().output,
        ProjectionOutcome::Stale
    );
    assert_eq!(
        publish(empty).await.unwrap().output,
        ProjectionOutcome::Stale
    );
    assert_eq!(
        publish(newest.clone()).await.unwrap().output,
        ProjectionOutcome::Duplicate
    );
    let mut conflict = newest.clone();
    conflict.digest[0] ^= 1;
    assert!(
        matches!(publish(conflict).await,Err(InvocationError::Rejected(v)) if v.output==ProjectionOutcome::Conflict)
    );
    assert_eq!(
        summary.get(key("thermometer"), None).await.unwrap().output,
        Some(newest)
    );
    let first = summary
        .list(
            BucketPageRequest {
                after_ms: None,
                limit: 1,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!((first.output.devices, first.output.accepted), (1, 2));
    assert_eq!(
        first.output.buckets[0],
        Bucket {
            start_ms: 120000,
            count: 1,
            sum_milli: 10
        }
    );
    assert_eq!(first.output.next_ms, Some(120000));
    let next = summary
        .list(
            BucketPageRequest {
                after_ms: first.output.next_ms,
                limit: 16,
            },
            Some(first.receipt),
        )
        .await
        .unwrap();
    assert_eq!(next.output.buckets.len(), 2);
    assert_eq!(next.output.buckets[0].count, 1);
    assert_eq!(next.output.buckets[0].sum_milli, 20);
    assert!(next.output.next_ms.is_none());
    assert!(
        summary
            .list(
                BucketPageRequest {
                    after_ms: None,
                    limit: 17
                },
                None
            )
            .await
            .is_err()
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn completed_audit_replays_original_outcomes_and_rejects_foreign_source_receipts() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = device(&h).await;
    source
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    let committed = source
        .record(new_identity().unwrap(), event(1, 120500, 10))
        .await
        .unwrap();
    let audit = AuditClient::new(h.handle.clone()).unwrap();
    h.node.open_cell(audit.target(), &Audit).await.unwrap();
    let completion = Completion {
        message: MessageId::parse(&uuid::Uuid::now_v7().to_string()).unwrap(),
        batch: batch(vec![event(1, 120500, 10)]),
        results: vec![EntryResult {
            event: event(1, 120500, 10),
            outcome: committed.output,
            source: Some(committed.receipt.into()),
        }],
    };
    roundtrip(&completion, 64 << 10);
    let first = audit
        .complete(new_identity().unwrap(), completion.clone())
        .await
        .unwrap();
    let mut recovery = completion.clone();
    recovery.results[0].outcome.decision = Decision::Duplicate;
    let replay = audit
        .complete(new_identity().unwrap(), recovery)
        .await
        .unwrap();
    assert_eq!(replay.output, first.output);
    assert_eq!(
        audit.get(completion.message, None).await.unwrap().output,
        Some(completion.clone())
    );
    let mut different = completion.clone();
    different.batch.events[0].value_milli = 11;
    different.results[0].event.value_milli = 11;
    assert!(
        matches!(audit.complete(new_identity().unwrap(),different).await,Err(InvocationError::Rejected(v)) if v.output==AuditOutcome::Conflict)
    );
    let mut forged = completion;
    forged.message = MessageId::parse(&uuid::Uuid::now_v7().to_string()).unwrap();
    forged.results[0].source.as_mut().unwrap().cell = [9; 32];
    assert!(
        audit
            .complete(new_identity().unwrap(), forged)
            .await
            .is_err()
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_queue_consumers_and_signed_delivery_match_authoritative_source() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let h = start(store.clone(), &root.path().join("source")).await;
    let receiver = start(store.clone(), &root.path().join("summary")).await;
    let source = device(&h).await;
    source
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    let producer = Producer::new(h.handle.clone()).unwrap();
    h.node.open_cell(producer.target(), &Ingress).await.unwrap();
    let input = batch(vec![
        event(3, 180500, 20),
        event(1, 120500, 10),
        Event {
            device: key("not-rostered"),
            sequence: 1,
            at_ms: 120500,
            value_milli: 99,
        },
    ]);
    let sent = producer
        .send(new_identity().unwrap(), &input, now_ms().unwrap())
        .await
        .unwrap();
    let QueueSendOutcome::Sent { message_id } = sent.output else {
        panic!("send refused")
    };
    let id = MessageId::parse(&uuid::Uuid::from_bytes(message_id).to_string()).unwrap();
    spawn_consumers(
        &h.node,
        h.handle.clone(),
        &[key("thermometer")],
        ConsumerOptions::default(),
    )
    .await
    .unwrap();
    spawn_delivery(
        &h.node,
        &receiver.node,
        h.handle.clone(),
        receiver.handle.clone(),
        &[key("thermometer")],
        DeliveryOptions {
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let audit = AuditClient::new(h.handle.clone()).unwrap();
    let completion = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(value) = audit.get(id, None).await.unwrap().output {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(completion.batch, input);
    assert_eq!(
        completion
            .results
            .iter()
            .map(|v| v.outcome.decision)
            .collect::<Vec<_>>(),
        vec![Decision::Applied, Decision::Applied, Decision::NotInRoster]
    );
    assert!(completion.results[2].source.is_none());
    let expected = source
        .get(None)
        .await
        .unwrap()
        .output
        .unwrap()
        .snapshot()
        .unwrap();
    let summary = SummaryClient::for_device(receiver.handle.clone(), &key("thermometer")).unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let progress = source.progress(None).await.unwrap().output.unwrap();
            if progress.state == ProjectionState::Delivered
                && summary.get(key("thermometer"), None).await.unwrap().output
                    == Some(expected.clone())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(expected.accepted, 2);
    assert_eq!(expected.contiguous_sequence, 1);
    assert_eq!(expected.latest, Some(event(3, 180500, 20)));
    assert_eq!(expected.reordered, 1);
    h.node.shutdown().await.unwrap();
    receiver.node.shutdown().await.unwrap();
    let restored = start(store, &root.path().join("cold")).await;
    let source = device(&restored).await;
    assert_eq!(
        source
            .get(None)
            .await
            .unwrap()
            .output
            .unwrap()
            .snapshot()
            .unwrap(),
        expected
    );
    let audit = AuditClient::new(restored.handle.clone()).unwrap();
    restored
        .node
        .open_cell(audit.target(), &Audit)
        .await
        .unwrap();
    assert_eq!(audit.get(id, None).await.unwrap().output, Some(completion));
    let summary = SummaryClient::for_device(restored.handle.clone(), &key("thermometer")).unwrap();
    restored
        .node
        .open_cell(summary.target(), &Summaries)
        .await
        .unwrap();
    assert_eq!(
        summary.get(key("thermometer"), None).await.unwrap().output,
        Some(expected)
    );
    let producer = Producer::new(restored.handle.clone()).unwrap();
    restored
        .node
        .open_cell(producer.target(), &Ingress)
        .await
        .unwrap();
    let info = restored
        .handle
        .queue::<Ingress>()
        .unwrap()
        .info(0, None)
        .await
        .unwrap();
    assert_eq!(info.output.acked, 1);
    restored.node.shutdown().await.unwrap();
}
#[test]
fn bounds_reject_noncanonical_and_overflowing_payloads() {
    for name in ["", "Upper", "a--b", "a-", "x/y"] {
        assert!(DeviceKey::new(name).is_err());
    }
    assert!(
        Window {
            start_ms: i64::MAX - 100,
            minutes: 16
        }
        .validate()
        .is_err()
    );
    assert!(
        batch(vec![event(1, 120000, 1), event(1, 120000, 1)])
            .validate()
            .is_err()
    );
    let state = DeviceState {
        device: key("thermometer"),
        window: window(),
        revision: 1,
        events: vec![],
        effect_id: [0; 32],
    };
    let mut snapshot = state.snapshot().unwrap();
    snapshot.buckets[0].sum_milli = i64::MIN;
    assert!(snapshot.validate().is_err());
    let mut encoder = BoundedEncoder::new(32).unwrap();
    encoder.write_u8(1).unwrap();
    encoder.write_bytes(&[1; 16]).unwrap();
    encoder.write_u32(u32::MAX).unwrap();
    let bytes = encoder.finish();
    assert!(Batch::decode(&mut BoundedDecoder::new(&bytes, 32).unwrap()).is_err());
}
#[tokio::test]
async fn full_source_retains_permanent_bindings_and_all_exact_window_totals() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let client = device(&h).await;
    client
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    for sequence in 1..=MAX_EVENTS as i64 {
        client
            .record(new_identity().unwrap(), event(sequence, 120500, MAX_VALUE))
            .await
            .unwrap();
    }
    let full = client.get(None).await.unwrap().output.unwrap();
    assert_eq!(full.revision, 129);
    assert_eq!(
        full.snapshot().unwrap().buckets[0].sum_milli,
        MAX_EVENTS as i64 * MAX_VALUE
    );
    rejected(
        client
            .record(new_identity().unwrap(), event(129, 120500, 1))
            .await
            .unwrap_err(),
        Decision::Capacity,
    );
    assert_eq!(
        client
            .record(new_identity().unwrap(), event(1, 120500, MAX_VALUE))
            .await
            .unwrap()
            .output
            .decision,
        Decision::Duplicate
    );
    rejected(
        client
            .record(new_identity().unwrap(), event(1, 120500, MAX_VALUE - 1))
            .await
            .unwrap_err(),
        Decision::Conflict,
    );
    assert_eq!(client.get(None).await.unwrap().output, Some(full));
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn target_scope_and_native_producer_identity_are_explicit_admission_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let client = device(&h).await;
    client
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    let mut foreign = event(1, 120500, 10);
    foreign.device = key("other-device");
    assert!(matches!(
        client.prepare_event(new_identity().unwrap(), foreign).await,
        Err(InvocationError::NotStarted(_))
    ));
    let producer = Producer::new(h.handle.clone()).unwrap();
    h.node.open_cell(producer.target(), &Ingress).await.unwrap();
    let input = batch(vec![event(1, 120500, 10)]);
    let timestamp = now_ms().unwrap();
    let first = producer
        .send(new_identity().unwrap(), &input, timestamp)
        .await
        .unwrap();
    let repeat = producer
        .send(new_identity().unwrap(), &input, timestamp)
        .await
        .unwrap();
    assert_eq!(repeat.output, first.output);
    let mut changed = input.clone();
    changed.events[0].value_milli = 11;
    assert!(
        matches!(producer.send(new_identity().unwrap(),&changed,timestamp).await,Err(InvocationError::Rejected(v)) if v.output==QueueSendOutcome::ProducerConflict)
    );
    let outsider = h
        .node
        .application_handle::<TelemetryIngest>(TenantId::from_bytes([0x50; 16]))
        .unwrap();
    let other = DeviceClient::new(outsider.clone(), key("thermometer")).unwrap();
    assert_ne!(client.target(), other.target());
    h.node.open_cell(other.target(), &Devices).await.unwrap();
    assert!(other.get(None).await.unwrap().output.is_none());
    assert!(
        spawn_delivery(
            &h.node,
            &h.node,
            h.handle.clone(),
            outsider,
            &[key("thermometer")],
            DeliveryOptions::default()
        )
        .await
        .is_err()
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn maximum_supported_consumer_delays_settle_the_original_native_claim() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = device(&h).await;
    source
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    let producer = Producer::new(h.handle.clone()).unwrap();
    h.node.open_cell(producer.target(), &Ingress).await.unwrap();
    let input = batch(vec![event(2, 180500, 20), event(1, 120500, 10)]);
    producer
        .send(new_identity().unwrap(), &input, now_ms().unwrap())
        .await
        .unwrap();
    let (tx, mut observations) = tokio::sync::mpsc::channel(8);
    spawn_consumers(
        &h.node,
        h.handle.clone(),
        &[key("thermometer")],
        ConsumerOptions {
            controlled_batch: Some(input.id),
            after_event: Duration::from_secs(10),
            before_ack: Duration::from_secs(10),
            progress: Some(tx),
        },
    )
    .await
    .unwrap();
    let started = tokio::time::Instant::now();
    let mut attempts = vec![];
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            while let Ok(progress) = observations.try_recv() {
                match progress {
                    ConsumerProgress::EventPublished { attempt, .. }
                    | ConsumerProgress::AuditPublished { attempt, .. } => attempts.push(attempt),
                }
            }
            let info = h
                .handle
                .queue::<Ingress>()
                .unwrap()
                .info(0, None)
                .await
                .unwrap();
            if info.output.acked == 1 {
                break;
            }
            assert!(h.node.is_ready());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(started.elapsed() >= Duration::from_secs(20));
    assert_eq!(attempts, vec![1, 1, 1]);
    assert_eq!(
        source.get(None).await.unwrap().output.unwrap().events.len(),
        2
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn excluded_first_entry_does_not_skip_the_actual_source_publication_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let h = start(store.clone(), &root.path().join("source")).await;
    let source = device(&h).await;
    source
        .register(new_identity().unwrap(), window())
        .await
        .unwrap();
    let producer = Producer::new(h.handle.clone()).unwrap();
    h.node.open_cell(producer.target(), &Ingress).await.unwrap();
    let input = batch(vec![
        Event {
            device: key("not-rostered"),
            sequence: 1,
            at_ms: 120500,
            value_milli: 99,
        },
        event(1, 120500, 10),
    ]);
    let result = producer
        .send(new_identity().unwrap(), &input, now_ms().unwrap())
        .await
        .unwrap();
    let QueueSendOutcome::Sent { message_id } = result.output else {
        panic!("producer refused")
    };
    let id = MessageId::parse(&uuid::Uuid::from_bytes(message_id).to_string()).unwrap();
    let (tx, mut observations) = tokio::sync::mpsc::channel(8);
    spawn_consumers(
        &h.node,
        h.handle.clone(),
        &[key("thermometer")],
        ConsumerOptions {
            controlled_batch: Some(input.id),
            after_event: Duration::from_secs(10),
            progress: Some(tx),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let publication = tokio::time::timeout(Duration::from_secs(5), observations.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        publication,
        ConsumerProgress::EventPublished { index: 1, .. }
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), observations.recv())
            .await
            .is_err(),
        "audit escaped the configured actual-source-publication pause"
    );
    assert_eq!(
        source.get(None).await.unwrap().output.unwrap().events.len(),
        1
    );
    // Graceful cancellation shortens the exercised pause and finishes accepted audit/ack work.
    h.node.shutdown().await.unwrap();
    let recovered = start(store, &root.path().join("cold")).await;
    let audit = AuditClient::new(recovered.handle.clone()).unwrap();
    recovered
        .node
        .open_cell(audit.target(), &Audit)
        .await
        .unwrap();
    let completion = audit.get(id, None).await.unwrap().output.unwrap();
    assert_eq!(
        completion
            .results
            .iter()
            .map(|v| v.outcome.decision)
            .collect::<Vec<_>>(),
        vec![Decision::NotInRoster, Decision::Applied]
    );
    let producer = Producer::new(recovered.handle.clone()).unwrap();
    recovered
        .node
        .open_cell(producer.target(), &Ingress)
        .await
        .unwrap();
    assert_eq!(
        recovered
            .handle
            .queue::<Ingress>()
            .unwrap()
            .info(0, None)
            .await
            .unwrap()
            .output
            .acked,
        1
    );
    recovered.node.shutdown().await.unwrap();
}
