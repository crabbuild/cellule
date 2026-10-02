//! Public queue ownership, idempotency, restart, dead-letter, and worker-drain contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_cookbook_work_queue::{
    DEAD, DeadLetters, Delivery, Inspect, InspectionCursor, InspectionPageRequest, JOBS, Job, Jobs,
    Producer, RECEIVER, Receiver, Record, RecordOutcome, SetEnabled, WorkQueue, WorkerOptions,
    WorkerProgress, compile, spawn_workers, target,
};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId,
    primitives::queue::{QueueClaimRequest, QueueLeaseOutcome, QueueSendOutcome, QueueState},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0x76; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x77; 16]);
async fn start(store: Store, root: &std::path::Path) -> (LocalNode, ApplicationHandle<WorkQueue>) {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("test-work-queue"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = node.application_handle::<WorkQueue>(TENANT).unwrap();
    for shard in 0..2 {
        node.open_cell(&target(TENANT, APP, JOBS, shard).unwrap(), &Jobs)
            .await
            .unwrap();
    }
    node.open_cell(&target(TENANT, APP, DEAD, 0).unwrap(), &DeadLetters)
        .await
        .unwrap();
    node.open_cell(&target(TENANT, APP, RECEIVER, 0).unwrap(), &Receiver)
        .await
        .unwrap();
    (node, handle)
}
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
fn job() -> Job {
    Job {
        id: uuid::Uuid::now_v7().to_string(),
        body: "publish a report".into(),
    }
}
fn shard(job: &Job) -> u32 {
    cellule_runtime::shard_for_scope(JOBS, &job.validate().unwrap(), 2).unwrap()
}
fn receiver() -> cellule_runtime::CellTarget {
    target(TENANT, APP, RECEIVER, 0).unwrap()
}

#[tokio::test]
async fn producer_identity_conflict_is_durable_and_resolvable() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let producer = Producer::new(handle);
    let job = job();
    let identity = new_identity().unwrap();
    let available = now_ms().unwrap();
    let prepared = producer.prepare(identity, &job, available).await.unwrap();
    let evidence = prepared.evidence().clone();
    assert!(matches!(
        producer.resolve(&evidence).await.unwrap(),
        Resolution::Absent
    ));
    let sent = prepared.execute().await.unwrap();
    assert!(matches!(sent.output, QueueSendOutcome::Sent { .. }));
    assert_eq!(
        producer.send(identity, &job, available).await.unwrap(),
        sent
    );
    assert!(
        matches!(producer.resolve(&evidence).await.unwrap(),Resolution::Committed(stored) if stored.commit_sequence()==sent.receipt.commit_sequence)
    );
    let changed = Job {
        body: "different report".into(),
        ..job
    };
    let conflict = new_identity().unwrap();
    let rejected = match producer.send(conflict, &changed, available).await {
        Err(InvocationError::Rejected(result)) => result,
        other => panic!("expected producer conflict: {other:?}"),
    };
    assert_eq!(rejected.output, QueueSendOutcome::ProducerConflict);
    assert!(
        matches!(producer.send(conflict,&changed,available).await,Err(InvocationError::Rejected(replay)) if replay==rejected)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn concurrent_consumers_lease_one_message_and_wrong_token_cannot_settle_it() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let job = job();
    let shard = shard(&job);
    Producer::new(handle.clone())
        .send(new_identity().unwrap(), &job, now_ms().unwrap())
        .await
        .unwrap();
    let queue = handle.queue::<Jobs>().unwrap();
    let request = QueueClaimRequest {
        limit: 1,
        lease_ms: 5000,
    };
    let (a, b) = tokio::join!(
        queue.claim(new_identity().unwrap(), shard, request),
        queue.claim(new_identity().unwrap(), shard, request)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.output.len() + b.output.len(), 1);
    let claimed = if a.output.is_empty() { b } else { a };
    let message = claimed.output[0].clone();
    assert!(
        queue
            .validate_claim(shard, claimed.output.clone(), Some(claimed.receipt))
            .await
            .unwrap()
            .output
    );
    let mut wrong = message.token;
    wrong[0] ^= 1;
    assert!(
        matches!(queue.ack(new_identity().unwrap(),shard,message.message_id,wrong).await,
        Err(InvocationError::Rejected(result)) if result.output==QueueLeaseOutcome::LeaseLost)
    );
    let extended = queue
        .extend(
            new_identity().unwrap(),
            shard,
            message.message_id,
            message.token,
            5000,
        )
        .await
        .unwrap();
    assert!(
        matches!(extended.output,QueueLeaseOutcome::Applied {state:QueueState::Leased,lease_until_ms:Some(expiry)} if expiry>message.lease_until_ms)
    );
    let retried = queue
        .retry(
            new_identity().unwrap(),
            shard,
            message.message_id,
            message.token,
            0,
        )
        .await
        .unwrap();
    assert!(matches!(
        retried.output,
        QueueLeaseOutcome::Applied {
            state: QueueState::Ready,
            ..
        }
    ));
    assert!(
        !queue
            .validate_claim(shard, claimed.output, Some(retried.receipt))
            .await
            .unwrap()
            .output
    );
    let next = queue
        .claim(new_identity().unwrap(), shard, request)
        .await
        .unwrap();
    assert_eq!(next.output[0].attempt, 2);
    assert_ne!(next.output[0].token, message.token);
    assert!(
        matches!(queue.ack(new_identity().unwrap(),shard,message.message_id,message.token).await,
        Err(InvocationError::Rejected(result)) if result.output==QueueLeaseOutcome::LeaseLost)
    );
    queue
        .ack(
            new_identity().unwrap(),
            shard,
            next.output[0].message_id,
            next.output[0].token,
        )
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn receiver_deduplicates_after_queue_redelivery_and_rejects_changed_business_payload() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let job = job();
    let input = Delivery {
        job: job.clone(),
        dead: false,
    };
    assert_eq!(
        handle
            .command::<Record>(&receiver(), new_identity().unwrap(), input.clone())
            .await
            .unwrap()
            .output,
        RecordOutcome::Recorded
    );
    handle
        .command::<SetEnabled>(&receiver(), new_identity().unwrap(), false)
        .await
        .unwrap();
    assert_eq!(
        handle
            .command::<Record>(&receiver(), new_identity().unwrap(), input)
            .await
            .unwrap()
            .output,
        RecordOutcome::Duplicate
    );
    let changed = Delivery {
        job: Job {
            body: "changed".into(),
            ..job
        },
        dead: false,
    };
    assert!(
        matches!(handle.command::<Record>(&receiver(),new_identity().unwrap(),changed).await,Err(InvocationError::Rejected(result)) if result.output==RecordOutcome::Conflict)
    );
    assert_eq!(
        handle
            .query::<Inspect>(&receiver(), None, InspectionPageRequest::default())
            .await
            .unwrap()
            .output
            .delivered,
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn pause_resume_and_cold_restore_preserve_pending_message_and_producer_receipt() {
    let root = tempfile::tempdir().unwrap();
    let store = store();
    let (node, handle) = start(store.clone(), root.path()).await;
    let job = job();
    let shard = shard(&job);
    let identity = new_identity().unwrap();
    let available = now_ms().unwrap();
    let sent = Producer::new(handle.clone())
        .send(identity, &job, available)
        .await
        .unwrap();
    let queue = handle.queue::<Jobs>().unwrap();
    queue.pause(new_identity().unwrap(), shard).await.unwrap();
    assert!(
        queue
            .claim(
                new_identity().unwrap(),
                shard,
                QueueClaimRequest {
                    limit: 1,
                    lease_ms: 5000
                }
            )
            .await
            .unwrap()
            .output
            .is_empty()
    );
    node.shutdown().await.unwrap();
    drop(node);
    let (node, handle) = start(store, root.path()).await;
    assert_eq!(
        Producer::new(handle.clone())
            .send(identity, &job, available)
            .await
            .unwrap(),
        sent
    );
    let queue = handle.queue::<Jobs>().unwrap();
    assert!(queue.info(shard, None).await.unwrap().output.paused);
    queue.resume(new_identity().unwrap(), shard).await.unwrap();
    spawn_workers(&node, handle.clone(), TENANT, APP, WorkerOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handle
                .query::<Inspect>(&receiver(), None, InspectionPageRequest::default())
                .await
                .unwrap()
                .output
                .delivered
                == 1
                && queue.info(shard, None).await.unwrap().output.acked == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn exhaustion_delivers_signed_native_dead_letter_and_redrive_succeeds() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let job = job();
    let shard = shard(&job);
    handle
        .command::<SetEnabled>(&receiver(), new_identity().unwrap(), false)
        .await
        .unwrap();
    Producer::new(handle.clone())
        .send(new_identity().unwrap(), &job, now_ms().unwrap())
        .await
        .unwrap();
    spawn_workers(&node, handle.clone(), TENANT, APP, WorkerOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let inspected = handle
                .query::<Inspect>(&receiver(), None, InspectionPageRequest::default())
                .await
                .unwrap()
                .output;
            if inspected.dead == 1 {
                assert_eq!(inspected.delivered, 0);
                break;
            }
            assert!(node.is_ready());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let queue = handle.queue::<Jobs>().unwrap();
    assert_eq!(queue.info(shard, None).await.unwrap().output.dead, 1);
    handle
        .command::<SetEnabled>(&receiver(), new_identity().unwrap(), true)
        .await
        .unwrap();
    queue
        .redrive(new_identity().unwrap(), shard, 1)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if queue.info(shard, None).await.unwrap().output.acked == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let inspected = handle
        .query::<Inspect>(&receiver(), None, InspectionPageRequest::default())
        .await
        .unwrap()
        .output;
    assert_eq!((inspected.delivered, inspected.dead), (1, 1));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn graceful_drain_settles_the_published_action_without_waiting_for_checkpoint_delay() {
    let root = tempfile::tempdir().unwrap();
    let store = store();
    let (node, handle) = start(store.clone(), root.path()).await;
    let job = job();
    let shard = shard(&job);
    Producer::new(handle.clone())
        .send(new_identity().unwrap(), &job, now_ms().unwrap())
        .await
        .unwrap();
    let (sender, mut events) = tokio::sync::mpsc::channel(8);
    spawn_workers(
        &node,
        handle,
        TENANT,
        APP,
        WorkerOptions {
            before_ack: Duration::from_secs(10),
            progress: Some(sender),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap(),
        WorkerProgress::ReceiverPublished { dead: false, .. }
    ));
    tokio::time::timeout(Duration::from_secs(5), node.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(node);
    let (node, handle) = start(store, root.path()).await;
    let info = handle
        .queue::<Jobs>()
        .unwrap()
        .info(shard, None)
        .await
        .unwrap()
        .output;
    assert_eq!((info.acked, info.leased), (1, 0));
    assert_eq!(
        handle
            .query::<Inspect>(&receiver(), None, InspectionPageRequest::default())
            .await
            .unwrap()
            .output
            .delivered,
        1
    );
    node.shutdown().await.unwrap();
}
#[test]
fn canonical_job_contract_rejects_unbounded_or_ambiguous_ingress() {
    let valid = Job {
        id: "018f7ce0-67d0-7000-8000-000000000001".into(),
        body: "report".into(),
    };
    assert!(valid.validate().is_ok());
    for invalid in [
        Job {
            id: valid.id.to_uppercase(),
            ..valid.clone()
        },
        Job {
            body: " padded ".into(),
            ..valid.clone()
        },
        Job {
            body: "a".repeat(513),
            ..valid.clone()
        },
        Job {
            id: uuid::Uuid::nil().to_string(),
            ..valid
        },
    ] {
        assert!(invalid.validate().is_err());
    }
    assert!(target(TENANT, APP, JOBS, 2).is_err());
    assert!(target(TENANT, APP, DEAD, 1).is_err());
}

#[tokio::test]
async fn inspection_pages_continue_between_delivery_kinds_without_gaps() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let mut expected = Vec::new();
    for suffix in 1..=3 {
        let job = Job {
            id: format!("018f7ce0-67d0-7000-8000-{suffix:012}"),
            body: "Inspect both kinds".into(),
        };
        for dead in [false, true] {
            let delivery = Delivery {
                job: job.clone(),
                dead,
            };
            handle
                .command::<Record>(&receiver(), new_identity().unwrap(), delivery.clone())
                .await
                .unwrap();
            expected.push(delivery);
        }
    }
    let mut after = None;
    let mut observed = Vec::new();
    loop {
        let page = handle
            .query::<Inspect>(&receiver(), None, InspectionPageRequest { after, limit: 1 })
            .await
            .unwrap()
            .output;
        assert_eq!((page.delivered, page.dead), (3, 3));
        assert_eq!(page.rows.len(), 1);
        observed.extend(page.rows);
        let Some(next) = page.next else {
            break;
        };
        after = Some(next);
    }
    assert_eq!(observed, expected);
    let page = handle
        .query::<Inspect>(
            &receiver(),
            None,
            InspectionPageRequest {
                after: Some(InspectionCursor {
                    job_id: expected[0].job.id.clone(),
                    dead: false,
                }),
                limit: 1,
            },
        )
        .await
        .unwrap()
        .output;
    assert_eq!(page.rows, vec![expected[1].clone()]);
    for limit in [0, 101] {
        assert!(
            handle
                .query::<Inspect>(
                    &receiver(),
                    None,
                    InspectionPageRequest { after: None, limit }
                )
                .await
                .is_err()
        );
    }
    assert!(
        handle
            .query::<Inspect>(
                &receiver(),
                None,
                InspectionPageRequest {
                    after: Some(InspectionCursor {
                        job_id: "invalid".into(),
                        dead: false
                    }),
                    limit: 1,
                }
            )
            .await
            .is_err()
    );
    node.shutdown().await.unwrap();
}

#[test]
fn inspection_version_two_request_has_a_fixed_wire_fixture() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    let fixture = [0, 0, 0, 0, 100];
    let mut encoder = BoundedEncoder::new(64).unwrap();
    InspectionPageRequest::default()
        .encode(&mut encoder)
        .unwrap();
    assert_eq!(encoder.finish(), fixture);
    let mut decoder = BoundedDecoder::new(&fixture, 64).unwrap();
    assert_eq!(
        InspectionPageRequest::decode(&mut decoder).unwrap(),
        InspectionPageRequest::default()
    );
    decoder.finish().unwrap();
}

#[tokio::test]
async fn owned_dead_letter_delivery_keeps_margin_under_slow_publication() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    let provider = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(Store::new(provider.clone()), root.path()).await;
    let job = job();
    let shard = shard(&job);
    Producer::new(handle.clone())
        .send(new_identity().unwrap(), &job, now_ms().unwrap())
        .await
        .unwrap();
    let queue = handle.queue::<Jobs>().unwrap();
    // Exhaust the native queue before introducing latency: the only pending
    // business work is its real, transactionally published dead-letter effect.
    for _ in 0..20 {
        let claimed = queue
            .claim(
                new_identity().unwrap(),
                shard,
                QueueClaimRequest {
                    limit: 1,
                    lease_ms: 15_000,
                },
            )
            .await
            .unwrap();
        let message = claimed.output.into_iter().next().unwrap();
        queue
            .retry(
                new_identity().unwrap(),
                shard,
                message.message_id,
                message.token,
                0,
            )
            .await
            .unwrap();
    }
    assert_eq!(queue.info(shard, None).await.unwrap().output.dead, 1);
    provider.config_mut(|config| config.wait_put_per_call = Duration::from_millis(900));
    spawn_workers(&node, handle.clone(), TENANT, APP, WorkerOptions::default())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if !node.is_ready() {
                return false;
            }
            let inspected = handle
                .query::<Inspect>(&receiver(), None, InspectionPageRequest::default())
                .await
                .unwrap()
                .output;
            if inspected.dead == 1 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    provider.config_mut(|config| config.wait_put_per_call = Duration::ZERO);
    let drained = node.shutdown().await;
    assert!(
        matches!(result, Ok(true)),
        "slow publication prevented delivery: {result:?}; {drained:?}"
    );
    drained.unwrap();
}
