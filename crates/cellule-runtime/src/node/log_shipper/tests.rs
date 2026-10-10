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
    held: Option<(NodeId, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

#[derive(Default)]
struct RecordingTelemetry {
    appends: Mutex<Vec<(bool, u64)>>,
    batches: Mutex<Vec<crate::fleet::telemetry::NodeLogBatchTiming>>,
    submissions: Mutex<Vec<(CellId, crate::fleet::telemetry::NodeLogSubmissionTiming)>>,
}

impl crate::fleet::telemetry::CellTelemetry for RecordingTelemetry {
    fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        self.appends.lock().unwrap().push((acknowledged, bytes));
    }

    fn node_log_batch(&self, timing: crate::fleet::telemetry::NodeLogBatchTiming) {
        self.batches.lock().unwrap().push(timing);
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
            held: None,
        }
    }

    fn slow(delay: Duration) -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            fail: None,
            delay: Some(delay),
            receipt: None,
            held: None,
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
            if let Some((held, entered, release)) = &self.held
                && member == *held
                && request.frames.first().is_some_and(|frame| {
                    cellule_ltx::inspect_node_frame(frame.clone(), cellule_ltx::Limits::default())
                        .is_ok_and(|frame| frame.scope().node_sequence == 1)
                })
            {
                entered.notify_one();
                release.notified().await;
            }
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

    let submissions = telemetry.submissions.lock().unwrap();
    assert_eq!(submissions.len(), 1);
    let (cell, timing) = submissions[0];
    assert_eq!(cell, submission(&cuts).cell);
    assert_eq!(timing.commit_sequence, 4);
    assert_eq!(timing.first_sequence, Some(1));
    assert_eq!(timing.enqueued, Some(true));
    let phases = [
        timing.byte_admission,
        timing.queue_admission,
        timing.capture_load,
        timing.ticket_order,
        timing.encoding,
    ];
    assert!(phases.iter().all(Option::is_some));
    assert!(phases.into_iter().flatten().sum::<Duration>() <= timing.total);

    assert_eq!(telemetry.appends.lock().unwrap().len(), 1);
    assert!(telemetry.appends.lock().unwrap()[0].0);
    assert!(telemetry.appends.lock().unwrap()[0].1 > 0);
    let batches = telemetry.batches.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].completed_captures, 1);
    assert_eq!(batches[0].frames, cuts.segments.len() as u64);
    assert_eq!(batches[0].members, 1);
    assert!(batches[0].succeeded);
    assert_eq!(
        batches[0].encoded_bytes * batches[0].members,
        telemetry.appends.lock().unwrap()[0].1
    );
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
    let retained = publication::retained_bytes(
        capture
            .frames()
            .iter()
            .map(|frame| frame.encoded().len() as u64)
            .sum(),
        capture.frames().len() as u64,
        1,
    )
    .unwrap() as usize;
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
async fn publication_backlog_does_not_block_follower_issuance_with_byte_credit() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let shipper = NodeLogShipper::new(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    let mut feed = shipper.take_publication_feed().unwrap();
    for index in 0..512 {
        let ticket = shipper
            .submit(publication_submission(&cuts, index))
            .await
            .unwrap();
        gate.wait_followers(ticket).await.unwrap();
    }
    assert!(shipper.bytes.available_permits() > submission(&cuts).encoded_bytes as usize);
    let next = tokio::time::timeout(
        Duration::from_millis(100),
        shipper.submit(publication_submission(&cuts, 512)),
    )
    .await;
    if let Ok(Ok(ticket)) = next.as_ref() {
        gate.wait_followers(*ticket).await.unwrap();
    }
    shipper.shutdown().await.unwrap();
    let mut accepted = 0;
    while let Some(capture) = feed.recv().await {
        accepted += 1;
        assert_eq!(capture.assignment().ticket().first_sequence(), accepted);
        capture.assignment().verify(capture.frames()).unwrap();
    }
    assert!(
        matches!(next, Ok(Ok(_))),
        "available native byte credit must let followers advance while publication is paused"
    );
    assert_eq!(accepted, 513);
    assert_eq!(
        shipper.bytes.available_permits(),
        shipper.max_outstanding_bytes as usize
    );
}

#[tokio::test]
async fn cancelled_full_native_byte_window_does_not_issue_a_native_gap() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let telemetry = Arc::new(RecordingTelemetry::default());
    let limits = cellule_ltx::Limits {
        max_capture_bytes: cuts.segments[0].info().size_bytes * 16,
        ..cellule_ltx::Limits::default()
    };
    let charge = publication::retained_bytes(
        submission(&cuts).encoded_bytes,
        cuts.segments.len() as u64,
        1,
    )
    .unwrap();
    let accepted = NodeLogShipper::validate_limits(limits).unwrap().0 / charge;
    let shipper = NodeLogShipper::new_with_telemetry(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        limits,
        crate::fleet::telemetry::CellTelemetryHandle::from_sink(telemetry.clone()),
    )
    .unwrap();
    let mut feed = shipper.take_publication_feed().unwrap();
    assert!(shipper.take_publication_feed().is_err());
    for index in 0..accepted {
        shipper
            .submit(publication_submission(&cuts, index))
            .await
            .unwrap();
    }
    assert_eq!(gate.issued_through(), accepted);
    let blocked = shipper.submit(publication_submission(&cuts, accepted));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), blocked)
            .await
            .is_err()
    );
    assert_eq!(gate.issued_through(), accepted);
    {
        let observed = telemetry.submissions.lock().unwrap();
        assert_eq!(observed.len() as u64, accepted + 1);
        let (cell, blocked) = observed.last().unwrap();
        assert_eq!(*cell, publication_submission(&cuts, accepted).cell);
        assert!(blocked.cancelled);
        assert!(!blocked.succeeded);
        assert!(blocked.native_bytes > Duration::ZERO);
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
        .submit(publication_submission(&cuts, accepted))
        .await
        .unwrap();
    assert_eq!(next.first_sequence(), accepted + 1);
    gate.wait_followers(next).await.unwrap();
    shipper.shutdown().await.unwrap();
    for expected in 2..=accepted + 1 {
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
async fn shutdown_wakes_full_native_byte_admission_and_joins_accepted_frames() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let transport = Arc::new(RecordingTransport::default());
    let limits = cellule_ltx::Limits {
        max_capture_bytes: cuts.segments[0].info().size_bytes * 16,
        ..cellule_ltx::Limits::default()
    };
    let charge = publication::retained_bytes(
        submission(&cuts).encoded_bytes,
        cuts.segments.len() as u64,
        1,
    )
    .unwrap();
    let accepted = NodeLogShipper::validate_limits(limits).unwrap().0 / charge;
    let shipper = Arc::new(NodeLogShipper::new(gate.clone(), transport.clone(), limits).unwrap());
    let mut feed = shipper.take_publication_feed().unwrap();
    for index in 0..accepted {
        shipper
            .submit(publication_submission(&cuts, index))
            .await
            .unwrap();
    }
    let next = publication_submission(&cuts, accepted);
    let blocked = shipper.submit(next);
    tokio::pin!(blocked);
    // Poll the actual admission waiter. A full byte window cannot hold the
    // global ordering lock or issue a ticket while this future is pending.
    assert!(futures_util::poll!(blocked.as_mut()).is_pending());
    assert!(shipper.order.try_lock().is_ok());
    tokio::time::timeout(Duration::from_secs(1), shipper.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(blocked.await, Err(Error::RuntimeClosed)));
    assert_eq!(gate.issued_through(), accepted);
    let mut captures = 0;
    while let Some(capture) = feed.recv().await {
        captures += 1;
        capture.assignment().verify(capture.frames()).unwrap();
    }
    assert_eq!(captures, accepted);
    assert_eq!(
        transport.batch_sizes(node(2)).iter().sum::<usize>() as u64,
        accepted
    );
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
            enqueued_at: None,
            completed_capture: true,
        })
        .collect();

    let lanes = replication::MemberLanes::start(transport, leader, 2, vec![member], 1 << 30);
    let round = lanes.enqueue(batch, gate.tiered_through()).unwrap().await;
    assert!(complete_round(
        &gate,
        &crate::fleet::telemetry::CellTelemetryHandle::default(),
        round,
    ));
    lanes.join().await.unwrap();

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
    let telemetry = Arc::new(RecordingTelemetry::default());
    let handle = crate::fleet::telemetry::CellTelemetryHandle::default();
    handle.install(telemetry.clone()).unwrap();
    let shipper = NodeLogShipper::new_with_telemetry(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
        handle,
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
    let traces = telemetry.submissions.lock().unwrap();
    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0].1.enqueued, Some(false));
    assert_eq!(traces[0].1.first_sequence, None);
    // Main validates immutable LTX captures before entering ticket order.
    assert!(traces[0].1.capture_load.is_none());
    assert!(traces[0].1.ticket_order.is_none());
    assert!(traces[0].1.encoding.is_none());
    assert_eq!(traces[1].1.enqueued, Some(true));
    assert_eq!(traces[1].1.first_sequence, Some(1));
}

#[tokio::test]
async fn cancelled_byte_wait_reports_once_without_fabricating_completed_phases() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let telemetry = Arc::new(RecordingTelemetry::default());
    let handle = crate::fleet::telemetry::CellTelemetryHandle::default();
    handle.install(telemetry.clone()).unwrap();
    let shipper = NodeLogShipper::new_with_telemetry(
        gate.clone(),
        Arc::new(RecordingTransport::default()),
        cellule_ltx::Limits::default(),
        handle,
    )
    .unwrap();
    let held = Arc::clone(&shipper.bytes)
        .acquire_many_owned(shipper.max_outstanding_bytes as u32)
        .await
        .unwrap();
    let mut attempt = Box::pin(shipper.submit(submission(&cuts)));
    assert!(futures_util::poll!(&mut attempt).is_pending());
    drop(attempt);
    let timing = {
        let traces = telemetry.submissions.lock().unwrap();
        assert_eq!(traces.len(), 1);
        traces[0].1
    };
    assert_eq!(timing.enqueued, None);
    assert_eq!(timing.first_sequence, None);
    assert!(
        [
            timing.byte_admission,
            timing.queue_admission,
            timing.capture_load,
            timing.ticket_order,
            timing.encoding
        ]
        .iter()
        .all(Option::is_none)
    );
    drop(held);
    assert_eq!(gate.preview(1).unwrap().first_sequence(), 1);
    shipper.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_follower_round_is_credited_during_next_batch_assembly() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    gate.activate_fleet().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let transport = Arc::new(RecordingTransport {
        held: Some((node(2), entered.clone(), release.clone())),
        ..RecordingTransport::default()
    });
    let shipper = NodeLogShipper::start(
        gate.clone(),
        transport,
        cellule_ltx::Limits::default(),
        crate::fleet::telemetry::CellTelemetryHandle::default(),
        Duration::from_millis(250),
    )
    .unwrap();
    let first = shipper
        .submit(publication_submission(&cuts, 0))
        .await
        .unwrap();
    entered.notified().await;
    let second = shipper
        .submit(publication_submission(&cuts, 1))
        .await
        .unwrap();
    let sender = shipper.sender.lock().unwrap().clone().unwrap();
    // The first member I/O is held. Reclaimed FIFO capacity establishes that
    // the dispatcher has received the second capture and begun its assembly.
    tokio::time::timeout(Duration::from_secs(1), async {
        while sender.capacity() != MAX_QUEUED_SUBMISSIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(sender);
    release.notify_one();
    let credited =
        tokio::time::timeout(Duration::from_millis(50), gate.wait_followers(first)).await;
    gate.wait_followers(second).await.unwrap();
    shipper.shutdown().await.unwrap();
    assert!(
        matches!(credited, Ok(Ok(_))),
        "a completed original round must not wait for the next assembly deadline"
    );
}

#[tokio::test]
async fn ordered_member_lanes_advance_independently_and_group_queued_rounds() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2), node(3)]).unwrap();
    gate.activate_fleet().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let transport = Arc::new(RecordingTransport {
        held: Some((node(2), entered.clone(), release.clone())),
        ..RecordingTransport::default()
    });
    let shipper = NodeLogShipper::new(
        gate.clone(),
        transport.clone(),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    let first = shipper
        .submit(publication_submission(&cuts, 0))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let mut tickets = vec![first];
    let mut independent = true;
    for index in 1..=5 {
        let ticket = shipper
            .submit(publication_submission(&cuts, index))
            .await
            .unwrap();
        tickets.push(ticket);
        let progress = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                let reached =
                    transport
                        .batches
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|(member, sequences)| {
                            *member == node(3)
                                && sequences
                                    .last()
                                    .is_some_and(|last| *last >= ticket.last_sequence())
                        });
                if reached {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if progress.is_err() {
            independent = false;
            break;
        }
    }
    // A fast member alone grants no Fleet proof past the held original round.
    let unproven = tokio::time::timeout(Duration::from_millis(10), gate.wait_followers(first))
        .await
        .is_err();
    release.notify_one();
    for ticket in tickets {
        gate.wait_followers(ticket).await.unwrap();
    }
    shipper.shutdown().await.unwrap();
    assert!(
        independent,
        "a slow member must not prevent the other ordered lane from accepting later rounds"
    );
    assert!(unproven);
    assert_eq!(transport.batch_sizes(node(2)), [1, 5]);
    assert_eq!(transport.batch_sizes(node(3)), [1, 1, 1, 1, 1, 1]);
}

#[tokio::test]
async fn cancelled_shutdown_waiter_does_not_make_a_later_join_complete_early() {
    let (_directory, cuts) = capture();
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let transport = Arc::new(RecordingTransport {
        held: Some((node(2), entered.clone(), release.clone())),
        ..RecordingTransport::default()
    });
    let shipper =
        Arc::new(NodeLogShipper::new(gate, transport, cellule_ltx::Limits::default()).unwrap());
    shipper.submit(submission(&cuts)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let closing = shipper.clone();
    let waiter = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while shipper.worker.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let next = shipper.shutdown();
    tokio::pin!(next);
    let premature = match futures_util::poll!(next.as_mut()) {
        std::task::Poll::Ready(result) => {
            result.unwrap();
            true
        }
        std::task::Poll::Pending => false,
    };
    release.notify_one();
    if !premature {
        tokio::time::timeout(Duration::from_secs(1), next)
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while shipper.bytes.available_permits() != shipper.max_outstanding_bytes as usize {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !premature,
        "a cancelled join waiter cannot manufacture a later successful shutdown before accepted member I/O finishes"
    );
}
