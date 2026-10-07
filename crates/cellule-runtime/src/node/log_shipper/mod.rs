//! Shipping verified frames to follower lanes and retiring them.
use std::{collections::VecDeque, io::Read as _, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::future::join_all;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::identity::{ApplicationId, CellId};
use crate::identity::{IncarnationId, NodeId};
use crate::node::log::{CommitTicket, DurabilityGate};
use crate::node::log_transport::{AppendRequest, NodeLogTransport};
use crate::{Error, Result};

const MAX_BATCH_FRAMES: usize = 64;
const MAX_QUEUED_SUBMISSIONS: usize = 512;
const NODE_FRAME_HEADER_BYTES: u64 = 240;
const BATCH_INTERVAL: Duration = Duration::from_millis(1);

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
    worker: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    bytes: Arc<Semaphore>,
    order: tokio::sync::Mutex<()>,
    max_outstanding_bytes: u64,
    gate: DurabilityGate,
    limits: cellule_ltx::Limits,
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
        let worker = runtime.spawn(run_shipper(
            receiver,
            worker_gate,
            Arc::clone(&bytes),
            transport,
            leader,
            log_epoch,
            members,
            batch_bytes,
            telemetry.clone(),
            interval,
        ));
        Ok(Self {
            sender: std::sync::Mutex::new(Some(sender)),
            worker: std::sync::Mutex::new(Some(worker)),
            bytes,
            order: tokio::sync::Mutex::new(()),
            max_outstanding_bytes: batch_bytes,
            gate,
            limits,
        })
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
        let frame_count = submission.frame_count()?;
        if submission
            .segments
            .iter()
            .any(|segment| segment.info().size_bytes > self.limits.max_capture_bytes)
            || frame_count > crate::node::log::MAX_TICKET_FRAMES
            || submission.encoded_bytes > self.max_outstanding_bytes
        {
            return Err(Error::Capacity("node-log submission"));
        }
        let permit_count = u32::try_from(submission.encoded_bytes)
            .ok()
            .filter(|bytes| *bytes != 0)
            .ok_or(Error::Capacity("node-log outstanding bytes"))?;
        let reservation = Arc::clone(&self.bytes)
            .acquire_many_owned(permit_count)
            .await
            .map_err(|_| Error::RuntimeClosed)?;
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
        let limits = self.limits;
        let (leader, log_epoch, _) = self.gate.shipping_scope()?;
        let loaded =
            tokio::task::spawn_blocking(move || submission.load(leader, log_epoch, limits))
                .await
                .map_err(Error::FollowerWorkerJoin)??;
        // Expensive LTX validation is parallel and bounded by outstanding-byte
        // admission. This lane only patches exclusively owned envelopes and
        // atomically commits their consecutive ticket before enqueueing.
        let _ordered = self.order.lock().await;
        let ticket = self.gate.preview(frame_count)?;
        let encoded = loaded.encode(ticket)?;
        let assignment = self
            .gate
            .commit_frames(ticket, &encoded)?
            .ok_or(Error::Node("assigned capture is empty"))?;
        let reservation = Arc::new(OutstandingBytes {
            _permit: reservation,
        });
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
        Ok((ticket, assignment))
    }

    /// Closes admission and drains every accepted frame to the current epoch.
    pub async fn shutdown(&self) -> Result<()> {
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
        if let Some(worker) = worker {
            worker.await.map_err(Error::FollowerWorkerJoin)?;
        }
        self.gate.stop_shipping();
        Ok(())
    }
}

impl Drop for NodeLogShipper {
    fn drop(&mut self) {
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
    mut receiver: mpsc::Receiver<QueuedSubmission>,
    gate: DurabilityGate,
    bytes: Arc<Semaphore>,
    transport: Arc<dyn NodeLogTransport>,
    leader: crate::SessionId,
    log_epoch: u64,
    members: Vec<NodeId>,
    max_batch_bytes: u64,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    interval: Duration,
) {
    let mut pending = VecDeque::<QueuedFrame>::new();
    let mut closed = false;
    loop {
        if pending.is_empty() {
            if closed {
                bytes.close();
                return;
            }
            match receiver.recv().await {
                Some(submission) => pending.extend(submission.frames),
                None => {
                    bytes.close();
                    return;
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
                    stop_shipper(&gate, &bytes);
                    return;
                };
                if !batch.is_empty() && next_bytes > max_batch_bytes {
                    break;
                }
                if next_bytes > max_batch_bytes {
                    stop_shipper(&gate, &bytes);
                    return;
                }
                let Some(next) = pending.pop_front() else {
                    stop_shipper(&gate, &bytes);
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
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(submission)) => pending.extend(submission.frames),
                Ok(None) => {
                    closed = true;
                    break;
                }
                Err(_) => break,
            }
        }

        let append_bytes = batch
            .iter()
            .try_fold(0_u64, |total, frame| {
                total.checked_add(frame.encoded.len() as u64)
            })
            .and_then(|bytes| bytes.checked_mul(members.len() as u64))
            .unwrap_or(u64::MAX);
        let result = append_batch(
            &gate,
            Arc::clone(&transport),
            leader,
            log_epoch,
            &members,
            batch,
        )
        .await;
        telemetry.node_log_append(result.is_ok(), append_bytes);
        if result.is_err() {
            stop_shipper(&gate, &bytes);
            receiver.close();
            return;
        }
    }
}

fn stop_shipper(gate: &DurabilityGate, bytes: &Semaphore) {
    gate.stop_shipping();
    bytes.close();
}

async fn append_batch(
    gate: &DurabilityGate,
    transport: Arc<dyn NodeLogTransport>,
    leader: crate::SessionId,
    log_epoch: u64,
    members: &[NodeId],
    batch: Vec<QueuedFrame>,
) -> Result<()> {
    let first = batch
        .first()
        .ok_or(Error::Node("node-log append batch is empty"))?
        .sequence;
    let last = batch
        .last()
        .ok_or(Error::Node("node-log append batch is empty"))?
        .sequence;
    if !batch
        .windows(2)
        .all(|pair| pair[0].sequence.checked_add(1) == Some(pair[1].sequence))
    {
        return Err(Error::Node("node-log append batch is not contiguous"));
    }
    let frames = batch
        .iter()
        .map(|frame| frame.encoded.clone())
        .collect::<Vec<_>>();
    let covered_through = gate.tiered_through();
    let replies = join_all(members.iter().map(|member| {
        let transport = Arc::clone(&transport);
        let request = AppendRequest {
            leader_session: leader,
            log_epoch,
            frames: frames.clone(),
            covered_through,
        };
        let member = *member;
        async move { (member, transport.append(member, request).await) }
    }))
    .await;
    let mut acknowledgements = Vec::with_capacity(replies.len());
    for (member, reply) in replies {
        let receipt = reply?;
        // A queued batch can include an already object-covered prefix. Only
        // its uncovered suffix must remain on the follower for a fleet proof.
        let required_first = first.max(covered_through.saturating_add(1));
        if receipt.durable_through < last
            || receipt.base_sequence == 0
            || receipt.base_sequence > receipt.durable_through.saturating_add(1)
            || (last > covered_through && receipt.base_sequence > required_first)
        {
            return Err(Error::Node("node-log append receipt differs"));
        }
        acknowledgements.push((member, receipt.durable_through));
    }
    for (member, durable_through) in acknowledgements {
        gate.acknowledge(member, durable_through)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
