use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;

use super::*;
use crate::follower::FollowerReceipt;
use crate::follower::FollowerStore;
use crate::identity::SessionId;
use crate::node::log_transport::{LocalFollowerTransport, RetireRequest, SealRequest, TailRequest};

#[derive(Default)]
struct RecordingTransport {
    batches: Mutex<Vec<(NodeId, Vec<u64>)>>,
    fail: Option<NodeId>,
    delay: Option<Duration>,
    receipt: Option<FollowerReceipt>,
}

#[derive(Default)]
struct RecordingTelemetry {
    appends: Mutex<Vec<(bool, u64)>>,
    submissions: Mutex<Vec<(CellId, crate::fleet::telemetry::NodeLogSubmissionTiming)>>,
}

impl crate::fleet::telemetry::CellTelemetry for RecordingTelemetry {
    fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        self.appends.lock().unwrap().push((acknowledged, bytes));
    }
    fn node_log_submission(
        &self,
        cell: CellId,
        timing: crate::fleet::telemetry::NodeLogSubmissionTiming,
    ) {
        self.submissions.lock().unwrap().push((cell, timing));
    }
}

struct LostAckTransport {
    inner: LocalFollowerTransport,
}

impl NodeLogTransport for LostAckTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            self.inner.append(member, request).await?;
            Err(Error::Node("injected lost follower acknowledgement"))
        })
    }

    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        self.inner.seal(member, request)
    }

    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        self.inner.retire(member, request)
    }

    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        self.inner.tail(member, request)
    }
}

impl RecordingTransport {
    fn failing(member: NodeId) -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            fail: Some(member),
            delay: None,
            receipt: None,
        }
    }

    fn slow(delay: Duration) -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            fail: None,
            delay: Some(delay),
            receipt: None,
        }
    }

    fn batch_sizes(&self, member: NodeId) -> Vec<usize> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .filter(|(observed, _)| *observed == member)
            .map(|(_, sequences)| sequences.len())
            .collect()
    }
}

impl NodeLogTransport for RecordingTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            if self.fail == Some(member) {
                return Err(Error::Node("injected follower failure"));
            }
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            let sequences = request
                .frames
                .iter()
                .map(|frame| {
                    cellule_ltx::inspect_node_frame(frame.clone(), cellule_ltx::Limits::default())
                        .map(|frame| frame.scope().node_sequence)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            let first = *sequences.first().ok_or(Error::Node("empty test append"))?;
            let last = *sequences.last().ok_or(Error::Node("empty test append"))?;
            self.batches.lock().unwrap().push((member, sequences));
            Ok(self.receipt.unwrap_or(FollowerReceipt {
                base_sequence: first,
                durable_through: last,
            }))
        })
    }

    fn seal<'a>(
        &'a self,
        _member: NodeId,
        _request: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("unused test seal")) })
    }

    fn retire<'a>(
        &'a self,
        _member: NodeId,
        _request: RetireRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("unused test retire")) })
    }

    fn tail<'a>(
        &'a self,
        _member: NodeId,
        _request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        Box::pin(async { Err(Error::Node("unused test tail")) })
    }
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn node(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 16])
}

fn capture() -> (tempfile::TempDir, cellule_ltx::CaptureBatch) {
    let directory = tempfile::TempDir::new().unwrap();
    let mut database = cellule_ltx::Db::open(
        &directory.path().join("shipper.sqlite"),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE items(value)"))
        .unwrap();
    let cuts = database.capture().unwrap();
    database.close().unwrap();
    (directory, cuts)
}

fn submission(cuts: &cellule_ltx::CaptureBatch) -> NodeLogSubmission {
    NodeLogSubmission::new(
        ApplicationId::from_bytes([9; 16]),
        CellId::from_bytes([8; 32]),
        IncarnationId::from_bytes([7; 16]),
        3,
        4,
        cuts,
    )
    .unwrap()
}

#[tokio::test]
async fn intervening_object_only_commands_do_not_disable_native_issuance() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut db = cellule_ltx::Db::open(
        &directory.path().join("object-gap.sqlite"),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE items(value)"))
        .unwrap();
    let first = db.capture().unwrap();
    db.transaction(|tx| tx.execute_batch("INSERT INTO items VALUES (5)"))
        .unwrap();
    let _object_only = db.capture().unwrap();
    db.transaction(|tx| tx.execute_batch("INSERT INTO items VALUES (6)"))
        .unwrap();
    let last = db.capture().unwrap();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let shipper = NodeLogShipper::new_with_telemetry(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
    )
    .unwrap();
    let first = shipper.submit(submission(&first)).await.unwrap();
    let next = NodeLogSubmission::new(
        ApplicationId::from_bytes([9; 16]),
        CellId::from_bytes([8; 32]),
        IncarnationId::from_bytes([7; 16]),
        3,
        6,
        &last,
    )
    .unwrap();
    let (ticket, _) = shipper.submit_assigned(next).await.unwrap();
    assert_eq!(ticket.first_sequence(), first.last_sequence() + 1);
    assert_eq!(
        gate.prove(ticket).await.unwrap().source(),
        crate::node::log::DurabilitySource::Fleet
    );
    shipper.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_submissions_stay_ordered_and_require_every_member_ack() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2), node(3)]).unwrap();
    gate.activate_fleet().unwrap();
    let transport = Arc::new(RecordingTransport::default());
    let shipper = NodeLogShipper::start(
        gate.clone(),
        transport.clone(),
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
        Duration::from_millis(50),
    )
    .unwrap();

    let (first, second) = tokio::join!(
        shipper.submit(submission(&cuts)),
        shipper.submit(
            NodeLogSubmission::new(
                ApplicationId::from_bytes([9; 16]),
                CellId::from_bytes([6; 32]),
                IncarnationId::from_bytes([7; 16]),
                3,
                4,
                &cuts
            )
            .unwrap()
        )
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        gate.prove(first).await.unwrap().source(),
        crate::node::log::DurabilitySource::Fleet
    );
    assert_eq!(
        gate.prove(second).await.unwrap().source(),
        crate::node::log::DurabilitySource::Fleet
    );
    shipper.shutdown().await.unwrap();

    assert_eq!(transport.batch_sizes(node(2)), [2]);
    assert_eq!(transport.batch_sizes(node(3)), [2]);
    assert!(gate.issue(1).is_err());
}

#[tokio::test]
async fn append_telemetry_records_one_result_for_each_batch() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let transport = Arc::new(RecordingTransport::default());
    let telemetry = Arc::new(RecordingTelemetry::default());
    let handle = crate::fleet::telemetry::CellTelemetryHandle::default();
    handle.install(telemetry.clone()).unwrap();
    let shipper =
        NodeLogShipper::new_with_telemetry(gate, transport, cellule_ltx::Limits::default(), handle)
            .unwrap();

    shipper.submit(submission(&cuts)).await.unwrap();
    shipper.shutdown().await.unwrap();

    assert_eq!(telemetry.appends.lock().unwrap().len(), 1);
    assert!(telemetry.appends.lock().unwrap()[0].0);
    assert!(telemetry.appends.lock().unwrap()[0].1 > 0);
}

#[tokio::test]
async fn splits_large_submission_at_sixty_four_frames() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut database = cellule_ltx::Db::open(
        &directory.path().join("many-cuts.sqlite"),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    database
        .transaction(|tx| tx.execute_batch("CREATE TABLE items(value)"))
        .unwrap();
    let mut cuts = database.capture().unwrap();
    for value in 0..64 {
        database
            .transaction(|tx| tx.execute("INSERT INTO items VALUES (?1)", [value]))
            .unwrap();
        let next = database.capture().unwrap();
        cuts.segments.extend(next.segments);
        cuts.position = next.position;
    }
    assert_eq!(cuts.segments.len(), 65);
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let transport = Arc::new(RecordingTransport::default());
    let shipper = NodeLogShipper::start(
        gate.clone(),
        transport.clone(),
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
        Duration::from_millis(50),
    )
    .unwrap();

    let mut publication = shipper.take_publication_feed().unwrap();
    let (ticket, assignment) = shipper.submit_assigned(submission(&cuts)).await.unwrap();
    let capture = publication.recv().await.unwrap();
    assert_eq!(capture.assignment(), assignment);
    assert_eq!(capture.frames().len(), 65);
    assignment.verify(capture.frames()).unwrap();
    assert!(assignment.verify(&capture.frames()[..64]).is_err());
    assert_eq!(
        gate.prove(ticket).await.unwrap().source(),
        crate::node::log::DurabilitySource::Fleet
    );
    shipper.shutdown().await.unwrap();
    assert!(publication.recv().await.is_none());
    let retained = capture
        .frames()
        .iter()
        .map(|frame| frame.encoded().len())
        .sum::<usize>();
    assert_eq!(
        shipper.bytes.available_permits(),
        shipper.max_outstanding_bytes as usize - retained
    );
    drop(capture);
    assert_eq!(
        shipper.bytes.available_permits(),
        shipper.max_outstanding_bytes as usize
    );

    assert_eq!(ticket.first_sequence(), 1);
    assert_eq!(ticket.last_sequence(), 65);
    assert_eq!(transport.batch_sizes(node(2)), [64, 1]);
    assert!(gate.issue(1).is_err());
}

fn publication_submission(cuts: &cellule_ltx::CaptureBatch, index: u64) -> NodeLogSubmission {
    let mut cell = [8; 32];
    cell[..8].copy_from_slice(&index.to_le_bytes());
    NodeLogSubmission::new(
        ApplicationId::from_bytes([9; 16]),
        CellId::from_bytes(cell),
        IncarnationId::from_bytes([7; 16]),
        3,
        4,
        cuts,
    )
    .unwrap()
}

#[tokio::test]
async fn cancelled_full_publication_queue_does_not_issue_a_native_gap() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let telemetry = Arc::new(RecordingTelemetry::default());
    let shipper = NodeLogShipper::new_with_telemetry(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::from_sink(telemetry.clone()),
    )
    .unwrap();
    let mut feed = shipper.take_publication_feed().unwrap();
    assert!(shipper.take_publication_feed().is_err());
    for index in 0..MAX_QUEUED_SUBMISSIONS as u64 {
        shipper
            .submit(publication_submission(&cuts, index))
            .await
            .unwrap();
    }
    assert_eq!(gate.issued_through(), 512);
    let blocked = shipper.submit(publication_submission(&cuts, 512));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), blocked)
            .await
            .is_err()
    );
    assert_eq!(gate.issued_through(), 512);
    {
        let observed = telemetry.submissions.lock().unwrap();
        assert_eq!(observed.len(), 513);
        let (cell, blocked) = observed.last().unwrap();
        assert_eq!(*cell, publication_submission(&cuts, 512).cell);
        assert!(blocked.cancelled);
        assert!(!blocked.succeeded);
        assert!(blocked.publication_slot > Duration::ZERO);
        for (_, timing) in observed.iter() {
            assert_eq!(
                timing.validation
                    + timing.native_bytes
                    + timing.shipping_slot
                    + timing.local_load
                    + timing.ordered_lane
                    + timing.publication_slot
                    + timing.assignment,
                timing.total
            );
            assert_eq!(timing.first_commit_sequence, 4);
            assert_eq!(timing.commit_sequence, 4);
            assert_eq!(timing.frames, 1);
        }
    }
    let first = feed.recv().await.unwrap();
    assert_eq!(first.assignment().ticket().first_sequence(), 1);
    first.assignment().verify(first.frames()).unwrap();
    drop(first);
    let next = shipper
        .submit(publication_submission(&cuts, 512))
        .await
        .unwrap();
    assert_eq!(next.first_sequence(), 513);
    gate.wait_followers(next).await.unwrap();
    shipper.shutdown().await.unwrap();
    for expected in 2..=513 {
        let capture = feed.recv().await.unwrap();
        assert_eq!(capture.assignment().ticket().first_sequence(), expected);
        capture.assignment().verify(capture.frames()).unwrap();
    }
    assert!(feed.recv().await.is_none());
    assert_eq!(
        shipper.bytes.available_permits(),
        shipper.max_outstanding_bytes as usize
    );
}

#[tokio::test]
async fn shutdown_wakes_full_publication_admission_and_joins_accepted_frames() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let transport = Arc::new(RecordingTransport::default());
    let shipper = Arc::new(
        NodeLogShipper::new(
            gate.clone(),
            transport.clone(),
            cellule_ltx::Limits::default(),
        )
        .unwrap(),
    );
    let mut feed = shipper.take_publication_feed().unwrap();
    for index in 0..MAX_QUEUED_SUBMISSIONS as u64 {
        shipper
            .submit(publication_submission(&cuts, index))
            .await
            .unwrap();
    }
    let next = publication_submission(&cuts, 512);
    let running = Arc::clone(&shipper);
    let blocked = tokio::spawn(async move { running.submit(next).await });
    // The ordered lane is held only after native loading, while the full
    // publication queue refuses a slot. Observe that actual blocked state.
    tokio::time::timeout(Duration::from_secs(1), async {
        while shipper.order.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), shipper.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(blocked.await.unwrap(), Err(Error::RuntimeClosed)));
    assert_eq!(gate.issued_through(), 512);
    let mut captures = 0;
    while let Some(capture) = feed.recv().await {
        captures += 1;
        capture.assignment().verify(capture.frames()).unwrap();
    }
    assert_eq!(captures, 512);
    assert_eq!(transport.batch_sizes(node(2)).iter().sum::<usize>(), 512);
}

#[tokio::test]
async fn dropped_publication_consumer_refuses_issuance_and_cannot_be_replaced() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let shipper = NodeLogShipper::new(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    drop(shipper.take_publication_feed().unwrap());
    assert!(matches!(
        shipper.submit(submission(&cuts)).await,
        Err(Error::RuntimeClosed)
    ));
    assert_eq!(gate.issued_through(), 0);
    assert!(shipper.take_publication_feed().is_err());
    shipper.shutdown().await.unwrap();
    assert_eq!(
        shipper.bytes.available_permits(),
        shipper.max_outstanding_bytes as usize
    );
}

#[tokio::test]
async fn covered_queued_prefix_keeps_the_uncovered_suffix_fleet_durable() {
    let (_directory, cuts) = capture();
    let limits = cellule_ltx::Limits::default();
    let leader = session(1);
    let member = node(2);
    let gate = DurabilityGate::new(leader, node(1), 2, [member]).unwrap();
    gate.activate_fleet().unwrap();
    let first = gate.issue(1).unwrap();
    let second = gate.issue(1).unwrap();
    gate.prove_object(first).unwrap();
    let follower_directory = tempfile::TempDir::new().unwrap();
    let store = FollowerStore::open(
        follower_directory.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport = Arc::new(LocalFollowerTransport::new(member, store.clone()));
    let reservation = Arc::new(OutstandingBytes {
        _permit: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
    });
    let batch = [first, second]
        .into_iter()
        .flat_map(|ticket| {
            submission(&cuts)
                .load(ticket.leader_session(), ticket.log_epoch(), limits)
                .unwrap()
                .encode(ticket)
                .unwrap()
        })
        .enumerate()
        .map(|(offset, encoded)| QueuedFrame {
            sequence: offset as u64 + 1,
            encoded: encoded.encoded().clone(),
            _reservation: Arc::clone(&reservation),
        })
        .collect();

    append_batch(&gate, transport, leader, 2, &[member], batch)
        .await
        .unwrap();

    assert_eq!(
        gate.prove(second).await.unwrap().source(),
        crate::node::log::DurabilitySource::Fleet
    );
    assert_eq!(store.seal(leader, 2).await.unwrap().base_sequence, 2);
    assert_eq!(store.read_tail(leader, 2, 2).await.unwrap().len(), 1);
}

#[tokio::test]
async fn receipts_without_the_uncovered_frame_never_authorize_fleet_proof() {
    let (_directory, cuts) = capture();
    for (base_sequence, durable_through) in [(2, 1), (0, 1), (3, 1), (1, 0)] {
        let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
        gate.activate_fleet().unwrap();
        let transport = Arc::new(RecordingTransport {
            receipt: Some(FollowerReceipt {
                base_sequence,
                durable_through,
            }),
            ..RecordingTransport::default()
        });
        let shipper =
            NodeLogShipper::new(gate.clone(), transport, cellule_ltx::Limits::default()).unwrap();
        let ticket = shipper.submit(submission(&cuts)).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), gate.wait_followers(ticket))
                .await
                .unwrap()
                .is_err()
        );
        shipper.shutdown().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.prove(ticket))
                .await
                .is_err()
        );
        gate.prove_object(ticket).unwrap();
        assert_eq!(
            gate.prove(ticket).await.unwrap().source(),
            crate::node::log::DurabilitySource::Object
        );
    }
}

#[tokio::test]
async fn follower_failure_stops_fleet_issuance_but_preserves_object_proof() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2), node(3)]).unwrap();
    gate.activate_fleet().unwrap();
    let transport = Arc::new(RecordingTransport::failing(node(3)));
    let shipper = NodeLogShipper::start(
        gate.clone(),
        transport,
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
        Duration::from_millis(1),
    )
    .unwrap();

    let ticket = shipper.submit(submission(&cuts)).await.unwrap();
    for _ in 0..100 {
        if gate.shipping_scope().is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(gate.shipping_scope().is_err());
    let retry = tokio::time::timeout(Duration::from_secs(1), shipper.submit(submission(&cuts)))
        .await
        .expect("failed shipper must release blocked byte admission");
    assert!(retry.is_err());
    shipper.shutdown().await.unwrap();

    assert!(gate.issue(1).is_err());
    gate.prove_object(ticket).unwrap();
    assert_eq!(
        gate.prove(ticket).await.unwrap().source(),
        crate::node::log::DurabilitySource::Object
    );
}

#[tokio::test]
async fn slow_follower_delays_fleet_proof_until_every_member_acknowledges() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2), node(3)]).unwrap();
    gate.activate_fleet().unwrap();
    let delay = Duration::from_millis(40);
    let transport = Arc::new(RecordingTransport::slow(delay));
    let shipper = NodeLogShipper::start(
        gate.clone(),
        transport,
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
        Duration::from_millis(1),
    )
    .unwrap();

    let started = tokio::time::Instant::now();
    let ticket = shipper.submit(submission(&cuts)).await.unwrap();
    let proof = gate.prove(ticket).await.unwrap();
    assert_eq!(proof.source(), crate::node::log::DurabilitySource::Fleet);
    assert!(started.elapsed() >= delay);
    shipper.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_ack_keeps_the_durable_follower_tail_without_issuing_fleet_proof() {
    let (_directory, cuts) = capture();
    let leader = session(1);
    let member = node(2);
    let gate = DurabilityGate::new(leader, node(1), 2, [member]).unwrap();
    gate.activate_fleet().unwrap();
    let follower_directory = tempfile::TempDir::new().unwrap();
    let store = FollowerStore::open(
        follower_directory.path().to_owned(),
        cellule_ltx::Limits::default(),
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(LostAckTransport {
        inner: LocalFollowerTransport::new(member, store.clone()),
    });
    let shipper = NodeLogShipper::start(
        gate.clone(),
        transport,
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
        Duration::from_millis(1),
    )
    .unwrap();

    let ticket = shipper.submit(submission(&cuts)).await.unwrap();
    shipper.shutdown().await.unwrap();

    assert!(gate.issue(1).is_err());
    assert_eq!(store.seal(leader, 2).await.unwrap().durable_through, 1);
    assert_eq!(store.read_tail(leader, 2, 1).await.unwrap().len(), 1);
    gate.prove_object(ticket).unwrap();
    assert_eq!(
        gate.prove(ticket).await.unwrap().source(),
        crate::node::log::DurabilitySource::Object
    );
}

#[tokio::test]
async fn oversized_submission_is_rejected_before_waiting_for_capacity() {
    let (_directory, mut cuts) = capture();
    let mut info = cuts.segments[0].info().clone();
    info.size_bytes = cellule_ltx::Limits::default()
        .max_capture_bytes
        .saturating_add((MAX_BATCH_FRAMES as u64) * NODE_FRAME_HEADER_BYTES);
    cuts.segments[0] = cellule_ltx::LocalSegment::new(cuts.segments[0].path().to_owned(), info);
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let shipper = NodeLogShipper::new(
        gate,
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
    )
    .unwrap();

    assert!(matches!(
        shipper.submit(submission(&cuts)).await,
        Err(Error::Capacity("node-log submission"))
    ));
    shipper.shutdown().await.unwrap();
}

#[tokio::test]
async fn encoding_failure_does_not_consume_a_node_sequence() {
    let (_directory, cuts) = capture();
    let mut invalid = cuts.clone();
    let mut info = invalid.segments[0].info().clone();
    info.blake3 = [0; 32];
    invalid.segments[0] =
        cellule_ltx::LocalSegment::new(invalid.segments[0].path().to_owned(), info);
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let shipper = NodeLogShipper::new(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
    )
    .unwrap();

    assert!(shipper.submit(submission(&invalid)).await.is_err());
    let ticket = shipper.submit(submission(&cuts)).await.unwrap();

    assert_eq!(ticket.first_sequence(), 1);
    assert_eq!(
        gate.prove(ticket).await.unwrap().source(),
        crate::node::log::DurabilitySource::Fleet
    );
    shipper.shutdown().await.unwrap();
}
