//! Shipping verified frames to follower lanes and retiring them.
use std::{collections::VecDeque, io::Read as _, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::{FutureExt, stream::StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::identity::{ApplicationId, CellId};
use crate::identity::{IncarnationId, NodeId};
use crate::node::log::{CommitTicket, DurabilityGate};
use crate::node::log_transport::{AppendRequest, NodeLogTransport};
use crate::{Error, Result};

mod publication;
mod replication;
mod submission;
pub use publication::{AssignedCapture, NodePublicationFeed, SelectedBundlePublication};
pub(crate) use publication::{SelectedBundle, SubmittedCapture};

const MAX_BATCH_FRAMES: usize = 64;
const MAX_QUEUED_SUBMISSIONS: usize = 512;
const NODE_FRAME_HEADER_BYTES: u64 = 240;
// An ordered pipeline no longer accumulates submissions while waiting for a
// preceding RPC. Give small captures a bounded group-commit window instead of
// sending every newly available round as another tiny follower request.
const BATCH_INTERVAL: Duration = Duration::from_millis(4);
type WorkerResult = std::result::Result<(), Arc<Error>>;

/// One captured Cell commit awaiting ordered node-log assignment.
pub struct NodeLogSubmission {
    application: ApplicationId,
    cell: CellId,
    incarnation: IncarnationId,
    cell_epoch: u64,
    commit_sequence: u64,
    first_commit_sequence: u64,
    segments: Vec<cellule_ltx::LocalSegment>,
    encoded_bytes: u64,
}

impl NodeLogSubmission {
    /// Binds one nonempty capture batch to its exact Cell authority generation.
    pub fn new(
        application: ApplicationId,
        cell: CellId,
        incarnation: IncarnationId,
        cell_epoch: u64,
        commit_sequence: u64,
        cuts: &cellule_ltx::CaptureBatch,
    ) -> Result<Self> {
        Self::new_range(
            application,
            cell,
            incarnation,
            cell_epoch,
            commit_sequence,
            commit_sequence,
            cuts,
        )
    }

    /// Binds one physical capture to every logical command committed together.
    ///
    /// The range is inclusive. Followers must support node-log protocol two
    /// before a non-singleton range is enrolled for shipping.
    pub fn new_range(
        application: ApplicationId,
        cell: CellId,
        incarnation: IncarnationId,
        cell_epoch: u64,
        first_commit_sequence: u64,
        commit_sequence: u64,
        cuts: &cellule_ltx::CaptureBatch,
    ) -> Result<Self> {
        let header_bytes =
            NODE_FRAME_HEADER_BYTES + u64::from(first_commit_sequence != commit_sequence) * 8;
        let encoded_bytes = cuts.segments.iter().try_fold(0_u64, |total, segment| {
            total
                .checked_add(segment.info().size_bytes)
                .and_then(|bytes| bytes.checked_add(header_bytes))
        });
        if application.as_bytes().iter().all(|byte| *byte == 0)
            || cell.as_bytes().iter().all(|byte| *byte == 0)
            || incarnation.as_bytes().iter().all(|byte| *byte == 0)
            || cell_epoch == 0
            || commit_sequence == 0
            || first_commit_sequence == 0
            || first_commit_sequence > commit_sequence
            || commit_sequence > i64::MAX as u64
            || cuts.segments.is_empty()
            || cuts
                .segments
                .last()
                .is_none_or(|segment| segment.info().position() != cuts.position)
            || encoded_bytes.is_none()
        {
            return Err(Error::Node("invalid node-log submission"));
        }
        Ok(Self {
            application,
            cell,
            incarnation,
            cell_epoch,
            commit_sequence,
            first_commit_sequence,
            segments: cuts.segments.clone(),
            encoded_bytes: encoded_bytes.ok_or(Error::Node("node-log byte count overflow"))?,
        })
    }

    fn frame_count(&self) -> Result<u64> {
        u64::try_from(self.segments.len()).map_err(|_| Error::Node("node-log frame count overflow"))
    }

    fn load(
        self,
        leader: crate::SessionId,
        log_epoch: u64,
        limits: cellule_ltx::Limits,
    ) -> Result<LoadedNodeLogSubmission> {
        let frames = self
            .segments
            .into_iter()
            .map(|segment| {
                let body = Bytes::from(read_segment(
                    segment.path(),
                    segment.info().size_bytes,
                    limits.max_capture_bytes,
                )?);
                cellule_ltx::encode_node_frame_range(
                    cellule_ltx::NodeFrameScope {
                        leader_session: *leader.as_bytes(),
                        log_epoch,
                        // The ordered lane assigns the final sequence only
                        // after all fallible file reads and verification finish.
                        node_sequence: 1,
                        application: *self.application.as_bytes(),
                        cell: *self.cell.as_bytes(),
                        incarnation: *self.incarnation.as_bytes(),
                        cell_epoch: self.cell_epoch,
                        commit_sequence: self.commit_sequence,
                    },
                    self.first_commit_sequence,
                    segment.info().clone(),
                    body,
                    limits,
                )
                .map_err(Error::from)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LoadedNodeLogSubmission { frames })
    }
}

struct LoadedNodeLogSubmission {
    frames: Vec<cellule_ltx::VerifiedNodeFrame>,
}

impl LoadedNodeLogSubmission {
    fn encode(self, ticket: CommitTicket) -> Result<Vec<cellule_ltx::VerifiedNodeFrame>> {
        self.frames
            .into_iter()
            .enumerate()
            .map(|(offset, frame)| {
                let offset = u64::try_from(offset)
                    .map_err(|_| Error::Node("node-log frame offset overflow"))?;
                let node_sequence = ticket
                    .first_sequence()
                    .checked_add(offset)
                    .ok_or(Error::Node("node-log sequence overflow"))?;
                frame.with_node_sequence(node_sequence).map_err(Error::from)
            })
            .collect()
    }
}

fn read_segment(path: &std::path::Path, expected_bytes: u64, limit: u64) -> Result<Vec<u8>> {
    let length = usize::try_from(expected_bytes)
        .ok()
        .filter(|_| expected_bytes <= limit)
        .ok_or(Error::Capacity("node-log frame bytes"))?;
    let mut file = std::fs::File::open(path)?;
    if file.metadata()?.len() != expected_bytes {
        return Err(Error::Node("node-log segment size changed"));
    }
    let mut body = vec![0_u8; length];
    file.read_exact(&mut body)?;
    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(Error::Node("node-log segment grew while reading"));
    }
    Ok(body)
}

/// Bounded node-wide multiplexer for one leader session and log epoch.
///
/// Submissions receive consecutive tickets before entering the ordered queue.
/// The worker batches frames across Cells and credits fleet durability only
/// after every selected member returns an fsynced contiguous watermark.
pub struct NodeLogShipper {
    sender: std::sync::Mutex<Option<mpsc::Sender<QueuedSubmission>>>,
    worker: std::sync::Mutex<Option<tokio::task::JoinHandle<WorkerResult>>>,
    bytes: Arc<Semaphore>,
    order: tokio::sync::Mutex<()>,
    max_outstanding_bytes: u64,
    member_count: usize,
    gate: DurabilityGate,
    limits: cellule_ltx::Limits,
    publication: publication::PublicationState,
    stopping: tokio::sync::watch::Sender<bool>,
    terminal: tokio::sync::watch::Receiver<Option<WorkerResult>>,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
}

impl NodeLogShipper {
    /// Starts one shipper for the exact ensemble owned by `gate`.
    pub fn new(
        gate: DurabilityGate,
        transport: Arc<dyn NodeLogTransport>,
        limits: cellule_ltx::Limits,
    ) -> Result<Self> {
        Self::start(
            gate,
            transport,
            limits,
            crate::fleet::telemetry::CellTelemetryHandle::default(),
            BATCH_INTERVAL,
        )
    }

    /// Starts a shipper with a bounded operational telemetry sink.
    pub fn new_with_telemetry(
        gate: DurabilityGate,
        transport: Arc<dyn NodeLogTransport>,
        limits: cellule_ltx::Limits,
        telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    ) -> Result<Self> {
        Self::start(gate, transport, limits, telemetry, BATCH_INTERVAL)
    }

    fn start(
        gate: DurabilityGate,
        transport: Arc<dyn NodeLogTransport>,
        limits: cellule_ltx::Limits,
        telemetry: crate::fleet::telemetry::CellTelemetryHandle,
        interval: Duration,
    ) -> Result<Self> {
        let (leader, log_epoch, members) = gate.shipping_scope()?;
        let (batch_bytes, permits) = Self::validate_limits(limits)?;
        let runtime = tokio::runtime::Handle::try_current().map_err(Error::RuntimeStart)?;
        let (sender, receiver) = mpsc::channel(MAX_QUEUED_SUBMISSIONS);
        let bytes = Arc::new(Semaphore::new(permits));
        let worker_gate = gate.clone();
        let (stopping, _) = tokio::sync::watch::channel(false);
        let member_count = members.len();
        let (completed, terminal) = tokio::sync::watch::channel(None);
        let worker_bytes = Arc::clone(&bytes);
        let worker_telemetry = telemetry.clone();
        let worker_stopping = stopping.clone();
        let worker = runtime.spawn(async move {
            let result = run_shipper(
                receiver,
                worker_gate,
                worker_bytes,
                transport,
                leader,
                log_epoch,
                members,
                batch_bytes,
                worker_telemetry,
                interval,
                worker_stopping,
            )
            .await
            .map_err(Arc::new);
            completed.send_replace(Some(result.clone()));
            result
        });
        Ok(Self {
            sender: std::sync::Mutex::new(Some(sender)),
            worker: std::sync::Mutex::new(Some(worker)),
            bytes,
            order: tokio::sync::Mutex::new(()),
            max_outstanding_bytes: batch_bytes,
            member_count,
            gate,
            limits,
            publication: publication::PublicationState::default(),
            stopping,
            terminal,
            telemetry,
        })
    }

    /// Installs the sole ordered publication consumer before any native issuance.
    /// The host must own and join this consumer with the original epoch. Taking
    /// the feed does not select objects, authorize responses or activate Fleet.
    pub fn take_publication_feed(&self) -> Result<NodePublicationFeed> {
        let _ordered = self
            .order
            .try_lock()
            .map_err(|_| Error::PendingPublication)?;
        if self.gate.issued_through() != 0 || *self.stopping.borrow() {
            return Err(Error::PendingPublication);
        }
        self.publication.take_feed(self.stopping.subscribe())
    }

    pub(crate) fn validate_limits(limits: cellule_ltx::Limits) -> Result<(u64, usize)> {
        let batch_bytes = limits
            .max_capture_bytes
            .checked_add(
                (MAX_BATCH_FRAMES as u64) * cellule_ltx::MAX_NODE_FRAME_HEADER_BYTES as u64,
            )
            .ok_or(Error::Capacity("node-log outstanding bytes"))?;
        let permits = usize::try_from(batch_bytes)
            .ok()
            .filter(|bytes| *bytes <= Semaphore::MAX_PERMITS && *bytes <= u32::MAX as usize)
            .ok_or(Error::Capacity("node-log outstanding bytes"))?;
        Ok((batch_bytes, permits))
    }

    /// Assigns a consecutive ticket and retains the encoded frames for shipping.
    ///
    /// Queue, byte admission, disk reads, and canonical encoding happen before
    /// the ticket reservation commits, so failures cannot create a sequence gap.
    pub async fn submit(&self, submission: NodeLogSubmission) -> Result<CommitTicket> {
        self.submit_assigned(submission)
            .await
            .map(|(ticket, _)| ticket)
    }

    /// Assigns the same canonical submission and returns its complete native
    /// range witness for node-wide object publication. There is one shipping lane.
    pub async fn submit_assigned(
        &self,
        submission: NodeLogSubmission,
    ) -> Result<(CommitTicket, crate::node::log::AssignedCommitRange)> {
        self.submit_capture(submission)
            .await
            .map(|capture| (capture.assignment.ticket(), capture.assignment))
    }

    pub(crate) async fn submit_capture(
        &self,
        submission: NodeLogSubmission,
    ) -> Result<SubmittedCapture> {
        let mut observation = submission::Observation::new(
            self.telemetry.clone(),
            submission.cell,
            submission.first_commit_sequence,
            submission.commit_sequence,
            submission.encoded_bytes,
        );
        let result = self.assign_capture(submission, &mut observation).await;
        observation.finish(result.is_ok());
        result
    }

    async fn assign_capture(
        &self,
        submission: NodeLogSubmission,
        observation: &mut submission::Observation,
    ) -> Result<SubmittedCapture> {
        use submission::Stage;
        let frame_count = submission.frame_count()?;
        observation.frames(frame_count);
        if submission
            .segments
            .iter()
            .any(|segment| segment.info().size_bytes > self.limits.max_capture_bytes)
            || frame_count > crate::node::log::MAX_TICKET_FRAMES
            || submission.encoded_bytes > self.max_outstanding_bytes
        {
            return Err(Error::Capacity("node-log submission"));
        }
        let retained_bytes =
            publication::retained_bytes(submission.encoded_bytes, frame_count, self.member_count)?;
        if retained_bytes > self.max_outstanding_bytes {
            return Err(Error::Capacity("node-log retained capture bytes"));
        }
        let permit_count = u32::try_from(retained_bytes)
            .ok()
            .filter(|bytes| *bytes != 0)
            .ok_or(Error::Capacity("node-log outstanding bytes"))?;
        observation.enter(Stage::NativeBytes);
        let reservation = Arc::clone(&self.bytes)
            .acquire_many_owned(permit_count)
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        observation.enter(Stage::ShippingSlot);
        let sender = self
            .sender
            .lock()
            .map_err(|_| Error::Node("node-log shipper lock poisoned"))?
            .clone()
            .ok_or(Error::RuntimeClosed)?;
        let slot = sender
            .reserve_owned()
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        observation.enter(Stage::LocalLoad);
        let limits = self.limits;
        let (leader, log_epoch, _) = self.gate.shipping_scope()?;
        let loaded =
            tokio::task::spawn_blocking(move || submission.load(leader, log_epoch, limits))
                .await
                .map_err(Error::FollowerWorkerJoin)??;
        // Expensive LTX validation is parallel and bounded by outstanding-byte
        // admission. This lane only patches exclusively owned envelopes and
        // atomically commits their consecutive ticket before enqueueing.
        observation.enter(Stage::OrderedLane);
        let _ordered = self.order.lock().await;
        // Native credit owns both consumers' complete capture until they join.
        // Checking the publication consumer is synchronous: storage capacity
        // never waits under this global ordered lane.
        observation.enter(Stage::PublicationSlot);
        let publication = self.publication.sender()?;
        if *self.stopping.borrow() {
            return Err(Error::RuntimeClosed);
        }
        observation.enter(Stage::Assignment);
        let ticket = self.gate.preview(frame_count)?;
        let encoded = loaded.encode(ticket)?;
        let assignment = self
            .gate
            .commit_frames(ticket, &encoded)?
            .ok_or(Error::Node("assigned capture is empty"))?;
        let reservation = Arc::new(OutstandingBytes {
            _permit: reservation,
        });
        let selection = if let Some(publication) = publication {
            let (capture, selection) =
                AssignedCapture::new(assignment, encoded.clone(), Arc::clone(&reservation));
            if publication.send(capture).is_err() {
                // A lost consumer cannot permit later Fleet acknowledgements
                // beyond this unpublished assignment. Fence before shipping.
                stop_shipper(&self.gate, &self.bytes, &self.stopping);
                return Err(Error::RuntimeClosed);
            }
            Some(selection)
        } else {
            None
        };
        let frames = encoded
            .into_iter()
            .enumerate()
            .map(|(offset, frame)| QueuedFrame {
                sequence: ticket.first_sequence().saturating_add(offset as u64),
                encoded: frame.encoded().clone(),
                _reservation: Arc::clone(&reservation),
            })
            .collect();
        slot.send(QueuedSubmission { frames });
        Ok(SubmittedCapture {
            assignment,
            selection,
        })
    }

    /// Closes admission and drains every accepted frame to the current epoch.
    pub async fn shutdown(&self) -> Result<()> {
        self.stopping.send_replace(true);
        let publication_closed = self.publication.close();
        self.bytes.close();
        self.sender
            .lock()
            .map_err(|_| Error::Node("node-log shipper lock poisoned"))?
            .take();
        let worker = self
            .worker
            .lock()
            .map_err(|_| Error::Node("node-log shipper lock poisoned"))?
            .take();
        let joined = match worker {
            Some(worker) => worker
                .await
                .map_err(Error::FollowerWorkerJoin)?
                .map_err(Error::Shared),
            None => {
                let mut terminal = self.terminal.clone();
                loop {
                    if let Some(result) = terminal.borrow_and_update().clone() {
                        break result.map_err(Error::Shared);
                    }
                    terminal.changed().await.map_err(|_| Error::RuntimeClosed)?;
                }
            }
        };
        self.gate.stop_shipping();
        publication_closed.and(joined)
    }
}

impl Drop for NodeLogShipper {
    fn drop(&mut self) {
        self.stopping.send_replace(true);
        self.gate.stop_shipping();
        self.bytes.close();
    }
}

struct OutstandingBytes {
    _permit: OwnedSemaphorePermit,
}

struct QueuedSubmission {
    frames: Vec<QueuedFrame>,
}

struct QueuedFrame {
    sequence: u64,
    encoded: Bytes,
    _reservation: Arc<OutstandingBytes>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the worker keeps the exact log epoch, ensemble and bounded admission explicit"
)]
async fn run_shipper(
    receiver: mpsc::Receiver<QueuedSubmission>,
    gate: DurabilityGate,
    bytes: Arc<Semaphore>,
    transport: Arc<dyn NodeLogTransport>,
    leader: crate::SessionId,
    log_epoch: u64,
    members: Vec<NodeId>,
    max_batch_bytes: u64,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    interval: Duration,
    stopping: tokio::sync::watch::Sender<bool>,
) -> Result<()> {
    let lanes =
        replication::MemberLanes::start(transport, leader, log_epoch, members, max_batch_bytes);
    run_rounds(
        receiver,
        &gate,
        &bytes,
        &lanes,
        max_batch_bytes,
        &telemetry,
        interval,
        &stopping,
    )
    .await;
    // Closing the round waiter never cancels a member's accepted native I/O.
    // Join every original lane before shutdown can release the epoch.
    let result = lanes.join().await;
    if result.is_err() {
        stop_shipper(&gate, &bytes, &stopping);
    }
    result
}

async fn run_rounds(
    mut receiver: mpsc::Receiver<QueuedSubmission>,
    gate: &DurabilityGate,
    bytes: &Semaphore,
    lanes: &replication::MemberLanes,
    max_batch_bytes: u64,
    telemetry: &crate::fleet::telemetry::CellTelemetryHandle,
    interval: Duration,
    stopping: &tokio::sync::watch::Sender<bool>,
) {
    let mut pending = VecDeque::<QueuedFrame>::new();
    let mut closed = false;
    let mut rounds = futures_util::stream::FuturesOrdered::new();
    loop {
        // Member tasks can have completed while this dispatcher was receiving
        // captures. Credit ready original rounds before assembling more work.
        while !rounds.is_empty() {
            let Some(Some(round)) = rounds.next().now_or_never() else {
                break;
            };
            if !complete_round(gate, telemetry, round) {
                stop_shipper(gate, bytes, stopping);
                receiver.close();
                return;
            }
        }
        if closed && pending.is_empty() && rounds.is_empty() {
            bytes.close();
            stopping.send_replace(true);
            return;
        }
        if rounds.len() == replication::PIPELINE || (closed && pending.is_empty()) {
            if let Some(round) = rounds.next().await
                && !complete_round(gate, telemetry, round)
            {
                stop_shipper(gate, bytes, stopping);
                receiver.close();
                return;
            }
            continue;
        }
        if pending.is_empty() {
            tokio::select! {
                round = rounds.next(), if !rounds.is_empty() => {
                    if let Some(round) = round
                        && !complete_round(gate, telemetry, round)
                    {
                        stop_shipper(gate, bytes, stopping);
                        receiver.close();
                        return;
                    }
                    continue;
                }
                submission = receiver.recv() => match submission {
                    Some(submission) => pending.extend(submission.frames),
                    None => { closed = true; continue; }
                }
            }
        }
        let deadline = tokio::time::Instant::now() + interval;
        let mut batch = Vec::<QueuedFrame>::new();
        let mut batch_bytes = 0_u64;
        loop {
            while batch.len() < MAX_BATCH_FRAMES {
                let Some(next) = pending.front() else {
                    break;
                };
                let Some(next_bytes) = batch_bytes.checked_add(next.encoded.len() as u64) else {
                    stop_shipper(gate, bytes, stopping);
                    return;
                };
                if !batch.is_empty() && next_bytes > max_batch_bytes {
                    break;
                }
                if next_bytes > max_batch_bytes {
                    stop_shipper(gate, bytes, stopping);
                    return;
                }
                let Some(next) = pending.pop_front() else {
                    stop_shipper(gate, bytes, stopping);
                    return;
                };
                batch_bytes = next_bytes;
                batch.push(next);
            }
            if batch.len() == MAX_BATCH_FRAMES
                || batch_bytes == max_batch_bytes
                || closed
                || !pending.is_empty()
            {
                break;
            }
            tokio::select! {
                round = rounds.next(), if !rounds.is_empty() => {
                    if let Some(round) = round
                        && !complete_round(gate, telemetry, round)
                    {
                        stop_shipper(gate, bytes, stopping);
                        receiver.close();
                        return;
                    }
                }
                submission = tokio::time::timeout_at(deadline, receiver.recv()) => match submission {
                    Ok(Some(submission)) => pending.extend(submission.frames),
                    Ok(None) => {
                        closed = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
        // Enqueue synchronously, in original sequence order, before returning a
        // future. Poll order can never reorder a member's accepted append lane.
        match lanes.enqueue(batch, gate.tiered_through()) {
            Ok(round) => rounds.push_back(round),
            Err(_) => {
                stop_shipper(gate, bytes, stopping);
                receiver.close();
                return;
            }
        }
    }
}

fn complete_round(
    gate: &DurabilityGate,
    telemetry: &crate::fleet::telemetry::CellTelemetryHandle,
    (bytes, result): replication::RoundResult,
) -> bool {
    let result = result.and_then(|acknowledgements| {
        for (member, through) in acknowledgements {
            gate.acknowledge(member, through)?;
        }
        Ok(())
    });
    telemetry.node_log_append(result.is_ok(), bytes);
    result.is_ok()
}

fn stop_shipper(
    gate: &DurabilityGate,
    bytes: &Semaphore,
    stopping: &tokio::sync::watch::Sender<bool>,
) {
    stopping.send_replace(true);
    gate.stop_shipping();
    bytes.close();
}

#[cfg(test)]
mod tests;
