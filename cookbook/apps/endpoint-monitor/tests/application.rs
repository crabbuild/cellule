//! Public scheduling, native probe, incident projection, delivery, and restore contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_endpoint_monitor::*;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    primitives::{cron::CronInvocation, workflow::ActivitySupervisor},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::Duration,
};
fn id(value: u128) -> Id {
    Id::from_bytes(value.to_be_bytes()).unwrap()
}
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
async fn node(store: Store, path: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("monitor-test"),
            application_id: ApplicationId::from_bytes([0x65; 16]),
        },
    )
    .await
    .unwrap()
}
async fn setup(
    node: &LocalNode,
) -> (
    cellule_app::ApplicationHandle<MonitorApplication>,
    MonitorClient,
) {
    let handle = node
        .application_handle::<MonitorApplication>(TenantId::from_bytes([0x66; 16]))
        .unwrap();
    let client = open(node, &handle).await.unwrap();
    (handle, client)
}
fn definition(version: u128) -> Definition {
    Definition {
        id: id(version),
        label: "Local API".into(),
        endpoint: "http://127.0.0.1:19023/probe".into(),
    }
}
fn check(client: &MonitorClient, occurrence: u64, health: Health) -> Check {
    Check {
        ticket: Ticket {
            source_cell: *client.target(SCHEDULES).unwrap().cell_id().as_bytes(),
            monitor: id(1),
            definition: definition(2),
            generation: 1,
            occurrence,
            scheduled_at_ms: now_ms().unwrap(),
        },
        probe: Probe {
            health,
            status: match health {
                Health::Up => Some(200),
                Health::Down => Some(503),
                Health::Unknown => None,
            },
            observed_at_ms: now_ms().unwrap(),
            reason: if health == Health::Unknown {
                "execution_unknown"
            } else {
                "http"
            }
            .into(),
        },
    }
}
fn page() -> PageRequest {
    PageRequest {
        monitor: id(1),
        definition: id(2),
        after: 0,
        limit: 100,
    }
}
async fn record(
    handle: &cellule_app::ApplicationHandle<MonitorApplication>,
    client: &MonitorClient,
    value: Check,
) -> RecordOutcome {
    handle
        .prepare_command::<RecordCheck>(
            &client.target(CHECKS).unwrap(),
            new_identity().unwrap(),
            value,
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap()
        .output
}
#[tokio::test]
async fn permanent_check_duplicates_reuse_row_edge_and_original_outcome() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let value = check(&client, 1, Health::Down);
    let identity = new_identity().unwrap();
    let original = handle
        .prepare_command::<RecordCheck>(&client.target(CHECKS).unwrap(), identity, value.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        handle
            .prepare_command::<RecordCheck>(
                &client.target(CHECKS).unwrap(),
                identity,
                value.clone()
            )
            .await
            .unwrap()
            .execute()
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        record(&handle, &client, value.clone()).await,
        original.output
    );
    let mut changed = value;
    changed.probe.health = Health::Up;
    changed.probe.status = Some(200);
    let conflict_id = new_identity().unwrap();
    let prepared = handle
        .prepare_command::<RecordCheck>(
            &client.target(CHECKS).unwrap(),
            conflict_id,
            changed.clone(),
        )
        .await
        .unwrap();
    let Err(InvocationError::Rejected(conflict)) = prepared.execute().await else {
        panic!("different observation under one key accepted")
    };
    assert_eq!(conflict.output, RecordOutcome::Conflict);
    let Err(InvocationError::Rejected(replay)) = handle
        .prepare_command::<RecordCheck>(&client.target(CHECKS).unwrap(), conflict_id, changed)
        .await
        .unwrap()
        .execute()
        .await
    else {
        panic!("conflict replay changed")
    };
    assert_eq!(replay, conflict);
    let view = client.inspect(page(), None).await.unwrap().output;
    assert_eq!(view.checks.len(), 1);
    assert_eq!(view.state.unwrap().health, Some(Health::Down));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn one_outage_has_exactly_one_open_and_close_with_atomic_notification_intent() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    record(&handle, &client, check(&client, 1, Health::Up)).await;
    let opened = record(&handle, &client, check(&client, 2, Health::Down)).await;
    let repeated = record(&handle, &client, check(&client, 3, Health::Down)).await;
    let closed = record(&handle, &client, check(&client, 4, Health::Up)).await;
    let RecordOutcome::Recorded {
        edge: Some(opened), ..
    } = opened
    else {
        panic!()
    };
    let RecordOutcome::Recorded {
        edge: Some(closed), ..
    } = closed
    else {
        panic!()
    };
    assert_eq!(opened.kind, EdgeKind::Opened);
    assert_eq!(closed.kind, EdgeKind::Closed);
    assert_eq!(opened.incident, closed.incident);
    assert!(matches!(
        repeated,
        RecordOutcome::Recorded { edge: None, .. }
    ));
    assert!(
        client
            .alerts(page(), None)
            .await
            .unwrap()
            .output
            .edges
            .is_empty()
    );
    spawn_workers(
        &node,
        handle,
        DeliveryOptions {
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let alerts = client.alerts(page(), None).await.unwrap().output;
        if alerts.edges.len() == 2 {
            assert_eq!(alerts.edges, vec![opened.clone(), closed.clone()]);
            break;
        }
        assert!(node.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let inspection = client.inspect(page(), None).await.unwrap().output;
    assert_eq!(inspection.checks.len(), 4);
    assert_eq!(inspection.state.unwrap().incident, None);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn late_checks_remain_in_history_but_cannot_reopen_recovered_incidents() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    record(&handle, &client, check(&client, 3, Health::Up)).await;
    assert!(matches!(
        record(&handle, &client, check(&client, 2, Health::Down)).await,
        RecordOutcome::Recorded {
            current: false,
            edge: None,
            ..
        }
    ));
    let opened = record(&handle, &client, check(&client, 4, Health::Down)).await;
    assert!(matches!(
        opened,
        RecordOutcome::Recorded { edge: Some(_), .. }
    ));
    record(&handle, &client, check(&client, 6, Health::Up)).await;
    assert!(matches!(
        record(&handle, &client, check(&client, 5, Health::Down)).await,
        RecordOutcome::Recorded {
            current: false,
            edge: None,
            ..
        }
    ));
    let inspection = client.inspect(page(), None).await.unwrap().output;
    let state = inspection.state.unwrap();
    assert_eq!(state.occurrence, 6);
    assert_eq!(state.health, Some(Health::Up));
    assert!(state.incident.is_none());
    assert_eq!(inspection.checks.len(), 5);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn unknown_observations_advance_the_watermark_without_claiming_recovery() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    record(&handle, &client, check(&client, 1, Health::Down)).await;
    assert!(matches!(
        record(&handle, &client, check(&client, 3, Health::Unknown)).await,
        RecordOutcome::Recorded {
            current: true,
            edge: None,
            ..
        }
    ));
    record(&handle, &client, check(&client, 2, Health::Up)).await;
    let state = client
        .inspect(page(), None)
        .await
        .unwrap()
        .output
        .state
        .unwrap();
    assert_eq!(state.occurrence, 3);
    assert_eq!(state.health, Some(Health::Down));
    assert!(state.incident.is_some());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn immutable_definition_replay_cannot_reactivate_or_roll_back_a_later_configuration() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let identity = new_identity().unwrap();
    let first = Change::Upsert {
        monitor: id(1),
        definition: definition(2),
        interval_ms: 1000,
        next_due_ms: identity.issued_at_ms + 100000,
    };
    let original = client
        .prepare(identity, first.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let second_id = new_identity().unwrap();
    client
        .prepare(
            second_id,
            Change::Upsert {
                monitor: id(1),
                definition: definition(3),
                interval_ms: 2000,
                next_due_ms: second_id.issued_at_ms + 100000,
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        client
            .prepare(new_identity().unwrap(), first.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        original.output
    );
    assert_eq!(
        client
            .schedule(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .definition
            .id,
        id(3)
    );
    client
        .prepare(new_identity().unwrap(), Change::Delete { monitor: id(1) })
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    client
        .prepare(new_identity().unwrap(), first)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(client.schedule(id(1), None).await.unwrap().output.is_none());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn changed_definition_bytes_are_durably_rejected_and_schedule_controls_preserve_evidence() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let identity = new_identity().unwrap();
    let change = Change::Upsert {
        monitor: id(1),
        definition: definition(2),
        interval_ms: 1000,
        next_due_ms: identity.issued_at_ms + 60000,
    };
    client
        .prepare(identity, change)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let second = new_identity().unwrap();
    let mut changed = definition(2);
    changed.label = "Changed".into();
    let change = Change::Upsert {
        monitor: id(1),
        definition: changed,
        interval_ms: 1000,
        next_due_ms: second.issued_at_ms + 60000,
    };
    let pending = client.prepare(second, change.clone()).await.unwrap();
    let evidence = pending.evidence().clone();
    assert!(
        matches!(pending.execute().await,Err(InvocationError::Rejected(value))if value.output==ScheduleOutcome::Conflict)
    );
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    assert!(
        matches!(client.prepare(second,change).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==ScheduleOutcome::Conflict)
    );
    client
        .prepare(new_identity().unwrap(), Change::Pause { monitor: id(1) })
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(
        !client
            .schedule(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .enabled
    );
    let resumed = new_identity().unwrap();
    client
        .prepare(
            resumed,
            Change::Resume {
                monitor: id(1),
                next_due_ms: resumed.issued_at_ms + 60000,
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(
        client
            .schedule(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .enabled
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn check_and_notification_reads_reject_foreign_receipts_and_invalid_pages() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let committed = handle
        .prepare_command::<RecordCheck>(
            &client.target(CHECKS).unwrap(),
            new_identity().unwrap(),
            check(&client, 1, Health::Down),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(
        client
            .alerts(page(), Some(committed.receipt))
            .await
            .is_err()
    );
    let mut invalid = page();
    invalid.limit = 101;
    assert!(client.inspect(invalid, None).await.is_err());
    let mut invalid = page();
    invalid.after = -1;
    assert!(client.alerts(invalid, None).await.is_err());
    let mut other = page();
    other.monitor = id(90);
    assert!(
        client
            .inspect(other, None)
            .await
            .unwrap()
            .output
            .checks
            .is_empty()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn bounded_keyset_pages_are_coherent_and_keep_immutable_definition_lifetimes_separate() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    for occurrence in 1..=7 {
        record(&handle, &client, check(&client, occurrence, Health::Up)).await;
    }
    let mut other = check(&client, 1, Health::Down);
    other.ticket.definition = definition(3);
    record(&handle, &client, other).await;
    let mut request = page();
    request.limit = 3;
    let first = client.inspect(request.clone(), None).await.unwrap().output;
    assert_eq!(first.checks.len(), 3);
    assert_eq!(first.state.unwrap().occurrence, 7);
    request.after = first.next.unwrap();
    let second = client.inspect(request.clone(), None).await.unwrap().output;
    assert_eq!(second.checks.len(), 3);
    request.after = second.next.unwrap();
    let last = client.inspect(request, None).await.unwrap().output;
    assert_eq!(last.checks.len(), 1);
    assert!(last.next.is_none());
    assert_eq!(first.checks[0].check.ticket.definition.id, id(2));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_retains_incident_edges_pending_intent_and_original_outcomes() {
    let store = store();
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let owner = node(store.clone(), &state).await;
    let (handle, client) = setup(&owner).await;
    let value = check(&client, 1, Health::Down);
    let identity = new_identity().unwrap();
    let original = handle
        .prepare_command::<RecordCheck>(&client.target(CHECKS).unwrap(), identity, value.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let expected = client.inspect(page(), None).await.unwrap().output;
    owner.shutdown().await.unwrap();
    std::fs::remove_dir_all(&state).unwrap();
    let successor = node(store, &state).await;
    let (handle, client) = setup(&successor).await;
    assert_eq!(client.inspect(page(), None).await.unwrap().output, expected);
    assert_eq!(
        handle
            .prepare_command::<RecordCheck>(&client.target(CHECKS).unwrap(), identity, value)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap(),
        original
    );
    spawn_workers(&successor, handle, DeliveryOptions::default())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client
        .alerts(page(), None)
        .await
        .unwrap()
        .output
        .edges
        .len()
        != 1
    {
        assert!(successor.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    successor.shutdown().await.unwrap();
}
fn wire<T: WireValue>(value: &T, limit: u32) -> Vec<u8> {
    let mut encoder = BoundedEncoder::new(limit).unwrap();
    value.encode(&mut encoder).unwrap();
    encoder.finish()
}
#[tokio::test]
async fn native_occurrence_binding_is_permanent_and_conflicting_input_cannot_replace_its_ticket() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let input = CronInvocation {
        schedule_id: id(1).bytes(),
        generation: 1,
        occurrence: 1,
        scheduled_at_ms: now_ms().unwrap(),
        payload: wire(&definition(2), 1024),
    };
    let target = client.target(PROBES).unwrap();
    let first = handle
        .prepare_command::<StartProbe>(&target, new_identity().unwrap(), input.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(matches!(first.output, StartOutcome::Started(_)));
    assert_eq!(
        handle
            .prepare_command::<StartProbe>(&target, new_identity().unwrap(), input.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        StartOutcome::AlreadyBound
    );
    let mut conflicting = input;
    conflicting.scheduled_at_ms += 1;
    assert!(
        matches!(handle.prepare_command::<StartProbe>(&target,new_identity().unwrap(),conflicting).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==StartOutcome::Conflict)
    );
    node.shutdown().await.unwrap();
}
struct Fixture {
    endpoint: String,
    status: Arc<AtomicU16>,
    cancel: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn new(status: u16, body: usize) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/probe", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU16::new(status));
        let observed = status.clone();
        let cancel = tokio_util::sync::CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    ()=stop.cancelled()=>break,
                    accepted=listener.accept()=>{let (stream,_)=accepted.unwrap();let observed=observed.clone();tasks.spawn(async move{
                        let service=hyper::service::service_fn(move |_|{let status=observed.load(Ordering::Acquire);async move{let mut response=hyper::Response::new(http_body_util::Full::new(hyper::body::Bytes::from(vec![b'x';body])));*response.status_mut()=hyper::StatusCode::from_u16(status).unwrap();Ok::<_,std::convert::Infallible>(response)}});
                        let _=hyper::server::conn::http1::Builder::new().keep_alive(false).serve_connection(hyper_util::rt::TokioIo::new(stream),service).await;
                    });},
                    Some(result)=tasks.join_next(),if !tasks.is_empty()=>{result.unwrap();},
                }
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        Self {
            endpoint,
            status,
            cancel,
            task,
        }
    }
    async fn stop(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}
#[tokio::test]
async fn bounded_http_probe_classifies_actual_success_failure_redirect_body_limit_and_transport() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let fixture = Fixture::new(200, 2).await;
    let mut ticket = check(&client, 1, Health::Up).ticket;
    ticket.definition.endpoint = fixture.endpoint.clone();
    assert_eq!(probe(&ticket).await.unwrap().health, Health::Up);
    fixture.status.store(503, Ordering::Release);
    assert_eq!(probe(&ticket).await.unwrap().status, Some(503));
    fixture.status.store(302, Ordering::Release);
    assert_eq!(probe(&ticket).await.unwrap().health, Health::Down);
    fixture.stop().await;
    assert_eq!(probe(&ticket).await.unwrap().reason, "transport");
    let large = Fixture::new(200, 1025).await;
    ticket.definition.endpoint = large.endpoint.clone();
    let result = probe(&ticket).await.unwrap();
    assert_eq!(result.health, Health::Down);
    assert_eq!(result.reason, "body_limit");
    large.stop().await;
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn native_activity_completion_retains_observation_before_separate_sql_projection() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let fixture = Fixture::new(503, 2).await;
    let mut ticket = check(&client, 1, Health::Down).ticket;
    ticket.definition.endpoint = fixture.endpoint.clone();
    let input = CronInvocation {
        schedule_id: ticket.monitor.bytes(),
        generation: ticket.generation,
        occurrence: ticket.occurrence,
        scheduled_at_ms: ticket.scheduled_at_ms,
        payload: wire(&ticket.definition, 1024),
    };
    handle
        .prepare_command::<StartProbe>(
            &client.target(PROBES).unwrap(),
            new_identity().unwrap(),
            input,
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let supervisor =
        ActivitySupervisor::new(handle.activities::<Probes>().unwrap(), 15000).unwrap();
    supervisor.run_once(0, None).await.unwrap();
    let view = client
        .workflow(&ticket, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.status, "completed");
    assert_eq!(view.state.check.unwrap().probe.health, Health::Down);
    assert!(
        client
            .check(ticket.clone(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    fixture.status.store(200, Ordering::Release);
    spawn_workers(&node, handle, DeliveryOptions::default())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(stored) = client.check(ticket.clone(), None).await.unwrap().output {
            assert_eq!(stored.check.probe.health, Health::Down);
            break;
        }
        assert!(node.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    node.shutdown().await.unwrap();
    fixture.stop().await;
}
#[tokio::test]
async fn owned_cron_probe_and_delivery_workers_open_and_close_one_incident() {
    let fixture = Fixture::new(503, 2).await;
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let identity = new_identity().unwrap();
    let mut def = definition(2);
    def.endpoint = fixture.endpoint.clone();
    client
        .prepare(
            identity,
            Change::Upsert {
                monitor: id(1),
                definition: def,
                interval_ms: 1000,
                next_due_ms: identity.issued_at_ms + 100,
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    spawn_workers(
        &node,
        handle,
        DeliveryOptions {
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while client
        .alerts(page(), None)
        .await
        .unwrap()
        .output
        .edges
        .len()
        != 1
    {
        assert!(node.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    fixture.status.store(200, Ordering::Release);
    while client
        .alerts(page(), None)
        .await
        .unwrap()
        .output
        .edges
        .len()
        != 2
    {
        assert!(node.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    client
        .prepare(new_identity().unwrap(), Change::Pause { monitor: id(1) })
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let edges = client.alerts(page(), None).await.unwrap().output.edges;
    assert_eq!(edges[0].kind, EdgeKind::Opened);
    assert_eq!(edges[1].kind, EdgeKind::Closed);
    assert_eq!(edges[0].incident, edges[1].incident);
    node.shutdown().await.unwrap();
    fixture.stop().await;
}
#[test]
fn wire_version_canonical_uuids_and_endpoint_authority_are_contracts() {
    let value = id(1);
    let bytes = wire(&value, 128);
    assert_eq!(bytes[0], 1);
    assert_eq!(&bytes[1..5], &38u32.to_be_bytes());
    assert_eq!(&bytes[5..], b"\"00000000-0000-0000-0000-000000000001\"");
    let mut decoder = BoundedDecoder::new(&bytes, 128).unwrap();
    assert_eq!(Id::decode(&mut decoder).unwrap(), value);
    decoder.finish().unwrap();
    assert!(Id::try_from("00000000000000000000000000000001".to_owned()).is_err());
    assert!(Id::from_bytes([0; 16]).is_err());
    for endpoint in [
        "https://127.0.0.1:9000/probe",
        "http://localhost:9000/probe",
        "http://example.com:9000/probe",
        "http://127.0.0.1:9000/probe?x=1",
        "http://user:pass@127.0.0.1:9000/probe",
        "http://127.0.0.1:9000/probe#x",
    ] {
        assert!(validate_endpoint(endpoint).is_err());
    }
}

#[tokio::test]
async fn permanent_schedule_and_definition_caps_preserve_original_bindings() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let mut original = None;
    for value in 1..=MAX_MONITORS as u128 {
        let identity = new_identity().unwrap();
        let change = Change::Upsert {
            monitor: id(value),
            definition: definition(100 + value),
            interval_ms: 1000,
            next_due_ms: identity.issued_at_ms + 600000,
        };
        let outcome = client
            .prepare(identity, change.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        if value == 1 {
            original = Some((change, outcome.output));
        }
    }
    let identity = new_identity().unwrap();
    assert!(
        matches!(client.prepare(identity,Change::Upsert{monitor:id(17),definition:definition(900),interval_ms:1000,next_due_ms:identity.issued_at_ms+600000}).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==ScheduleOutcome::Capacity)
    );
    for value in MAX_MONITORS..MAX_DEFINITIONS {
        let identity = new_identity().unwrap();
        client
            .prepare(
                identity,
                Change::Upsert {
                    monitor: id(1),
                    definition: definition(1000 + value as u128),
                    interval_ms: 1000,
                    next_due_ms: identity.issued_at_ms + 600000,
                },
            )
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
    }
    let identity = new_identity().unwrap();
    assert!(
        matches!(client.prepare(identity,Change::Upsert{monitor:id(1),definition:definition(9999),interval_ms:1000,next_due_ms:identity.issued_at_ms+600000}).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==ScheduleOutcome::Capacity)
    );
    let (change, outcome) = original.unwrap();
    assert_eq!(
        client
            .prepare(new_identity().unwrap(), change)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        outcome
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn stale_scheduled_probe_completes_unknown_without_claiming_a_current_http_observation() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let mut ticket = check(&client, 1, Health::Unknown).ticket;
    ticket.scheduled_at_ms = now_ms().unwrap() - PROBE_LIFETIME_MS - 1;
    let input = CronInvocation {
        schedule_id: ticket.monitor.bytes(),
        generation: ticket.generation,
        occurrence: ticket.occurrence,
        scheduled_at_ms: ticket.scheduled_at_ms,
        payload: wire(&ticket.definition, 1024),
    };
    handle
        .prepare_command::<StartProbe>(
            &client.target(PROBES).unwrap(),
            new_identity().unwrap(),
            input,
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let view = client
        .workflow(&ticket, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(view.status, "completed");
    assert_eq!(view.state.check.unwrap().probe.health, Health::Unknown);
    assert!(view.state.action.is_none());
    spawn_workers(&node, handle, DeliveryOptions::default())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client
        .check(ticket.clone(), None)
        .await
        .unwrap()
        .output
        .is_none()
    {
        assert!(node.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let state = client
        .inspect(page(), None)
        .await
        .unwrap()
        .output
        .state
        .unwrap();
    assert!(state.health.is_none() && state.incident.is_none());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn maximum_width_hundred_check_page_including_all_incident_edges_fits_declared_output() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    for index in 1..=100 {
        let mut value = check(
            &client,
            index,
            if index % 2 == 0 {
                Health::Up
            } else {
                Health::Down
            },
        );
        value.ticket.generation = i64::MAX as u64;
        value.ticket.occurrence = i64::MAX as u64 - 100 + index;
        value.ticket.definition.label = "x".repeat(80);
        let base = "http://127.0.0.1:65535/";
        value.ticket.definition.endpoint = format!("{base}{}", "x".repeat(256 - base.len()));
        record(&handle, &client, value).await;
    }
    let result = client.inspect(page(), None).await.unwrap().output;
    assert_eq!(result.checks.len(), 100);
    assert!(result.next.is_none());
    assert!(
        result
            .checks
            .iter()
            .all(|row| matches!(row.outcome, RecordOutcome::Recorded { edge: Some(_), .. }))
    );
    assert!(wire(&result, 262144).len() <= 262144);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn notification_business_identity_reuses_its_row_and_rejects_changed_edge_bytes() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let RecordOutcome::Recorded {
        edge: Some(edge), ..
    } = record(&handle, &client, check(&client, 1, Health::Down)).await
    else {
        panic!()
    };
    let original = handle
        .prepare_command::<RecordAlert>(
            &client.target(ALERTS).unwrap(),
            new_identity().unwrap(),
            edge.clone(),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap()
        .output;
    assert!(matches!(original,AlertOutcome::Recorded(row)if row>0));
    assert_eq!(
        handle
            .prepare_command::<RecordAlert>(
                &client.target(ALERTS).unwrap(),
                new_identity().unwrap(),
                edge.clone()
            )
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        original
    );
    let mut changed = edge;
    changed.monitor = id(10);
    assert!(
        matches!(handle.prepare_command::<RecordAlert>(&client.target(ALERTS).unwrap(),new_identity().unwrap(),changed).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==AlertOutcome::Conflict)
    );
    assert_eq!(
        client
            .alerts(page(), None)
            .await
            .unwrap()
            .output
            .edges
            .len(),
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn full_check_history_rejects_new_keys_but_preserves_old_observations_and_incident_state() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let first = check(&client, 1, Health::Up);
    let original = record(&handle, &client, first.clone()).await;
    for occurrence in 2..=MAX_CHECKS as u64 {
        record(&handle, &client, check(&client, occurrence, Health::Up)).await;
    }
    let identity = new_identity().unwrap();
    let input = check(&client, MAX_CHECKS as u64 + 1, Health::Down);
    assert!(
        matches!(handle.prepare_command::<RecordCheck>(&client.target(CHECKS).unwrap(),identity,input.clone()).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==RecordOutcome::Capacity)
    );
    assert!(
        matches!(handle.prepare_command::<RecordCheck>(&client.target(CHECKS).unwrap(),identity,input).await.unwrap().execute().await,Err(InvocationError::Rejected(value))if value.output==RecordOutcome::Capacity)
    );
    assert_eq!(record(&handle, &client, first).await, original);
    let state = client
        .inspect(page(), None)
        .await
        .unwrap()
        .output
        .state
        .unwrap();
    assert_eq!(state.occurrence, MAX_CHECKS as u64);
    assert_eq!(state.health, Some(Health::Up));
    assert!(state.incident.is_none());
    node.shutdown().await.unwrap();
}
