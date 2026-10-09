//! Ordered member lanes, bounded rounds, and group commit of queued native work.
use super::*;
use crate::follower::FollowerReceipt;
use futures_util::future::{BoxFuture, join_all};
use tokio::sync::oneshot;

pub(super) const PIPELINE: usize = 8;
pub(super) type RoundResult = (u64, Result<Vec<(NodeId, u64)>>);
type Round = BoxFuture<'static, RoundResult>;

struct MemberAppend {
    first: u64,
    last: u64,
    frames: Vec<Bytes>,
    covered_through: u64,
    // Member I/O keeps native credit even when its round waiter is cancelled.
    _retention: Vec<Arc<OutstandingBytes>>,
    completed: oneshot::Sender<Result<FollowerReceipt>>,
}

pub(super) struct MemberLanes {
    senders: Vec<(NodeId, mpsc::Sender<MemberAppend>)>,
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl MemberLanes {
    pub(super) fn start(
        transport: Arc<dyn NodeLogTransport>,
        leader: crate::SessionId,
        epoch: u64,
        members: Vec<NodeId>,
        max_bytes: u64,
    ) -> Self {
        let mut senders = Vec::with_capacity(members.len());
        let mut workers = Vec::with_capacity(members.len());
        for member in members {
            let (sender, receiver) = mpsc::channel(PIPELINE);
            senders.push((member, sender));
            workers.push(tokio::spawn(run_member(
                Arc::clone(&transport),
                member,
                leader,
                epoch,
                max_bytes,
                receiver,
            )));
        }
        Self { senders, workers }
    }

    pub(super) fn enqueue(&self, batch: Vec<QueuedFrame>, covered: u64) -> Result<Round> {
        let first = batch
            .first()
            .ok_or(Error::Node("empty append round"))?
            .sequence;
        let last = batch
            .last()
            .ok_or(Error::Node("empty append round"))?
            .sequence;
        if !batch
            .windows(2)
            .all(|pair| pair[0].sequence.checked_add(1) == Some(pair[1].sequence))
        {
            return Err(Error::Node("node-log append batch is not contiguous"));
        }
        let bytes = batch
            .iter()
            .try_fold(0_u64, |total, frame| {
                total.checked_add(frame.encoded.len() as u64)
            })
            .and_then(|bytes| bytes.checked_mul(self.senders.len() as u64))
            .ok_or(Error::Capacity("append round byte count"))?;
        let mut replies = Vec::with_capacity(self.senders.len());
        for (member, sender) in &self.senders {
            let (completed, reply) = oneshot::channel();
            let request = MemberAppend {
                first,
                last,
                covered_through: covered,
                frames: batch.iter().map(|frame| frame.encoded.clone()).collect(),
                _retention: batch
                    .iter()
                    .map(|frame| Arc::clone(&frame._reservation))
                    .collect(),
                completed,
            };
            // The global eight-round window bounds each member's FIFO. No
            // future is polled here, so request delivery preserves submission order.
            sender.try_send(request).map_err(|_| Error::RuntimeClosed)?;
            replies.push((*member, reply));
        }
        Ok(Box::pin(async move {
            let replies = join_all(replies.into_iter().map(|(member, reply)| async move {
                (
                    member,
                    reply
                        .await
                        .map_err(|_| Error::RuntimeClosed)
                        .and_then(|result| result),
                )
            }))
            .await;
            // Keep the original round's credit until every member answer joins.
            let _retention = batch;
            let mut acknowledgements = Vec::with_capacity(replies.len());
            let result = (|| {
                for (member, reply) in replies {
                    let receipt = reply?;
                    let required_first = first.max(covered.saturating_add(1));
                    if receipt.durable_through < last
                        || receipt.base_sequence == 0
                        || receipt.base_sequence > receipt.durable_through.saturating_add(1)
                        || (last > covered && receipt.base_sequence > required_first)
                    {
                        return Err(Error::Node("node-log append receipt differs"));
                    }
                    // A grouped receipt can cover later queued rounds. Credit
                    // only this original round; unresolved predecessors cannot be skipped.
                    acknowledgements.push((member, last));
                }
                Ok(acknowledgements)
            })();
            (bytes, result)
        }))
    }

    pub(super) async fn join(self) -> Result<()> {
        drop(self.senders);
        let joined = join_all(self.workers).await;
        for result in joined {
            result.map_err(Error::FollowerWorkerJoin)?;
        }
        Ok(())
    }
}

async fn run_member(
    transport: Arc<dyn NodeLogTransport>,
    member: NodeId,
    leader: crate::SessionId,
    epoch: u64,
    max_bytes: u64,
    mut receiver: mpsc::Receiver<MemberAppend>,
) {
    let mut carry = None;
    loop {
        let first = match carry.take() {
            Some(first) => first,
            None => match receiver.recv().await {
                Some(first) => first,
                None => return,
            },
        };
        let mut frame_count = first.frames.len();
        let mut bytes = first
            .frames
            .iter()
            .map(|frame| frame.len() as u64)
            .sum::<u64>();
        let mut last = first.last;
        let mut covered = first.covered_through;
        let mut requests = vec![first];
        while frame_count < MAX_BATCH_FRAMES {
            let Ok(next) = receiver.try_recv() else {
                break;
            };
            let next_bytes = next
                .frames
                .iter()
                .map(|frame| frame.len() as u64)
                .sum::<u64>();
            if last.checked_add(1) != Some(next.first)
                || frame_count + next.frames.len() > MAX_BATCH_FRAMES
                || bytes
                    .checked_add(next_bytes)
                    .is_none_or(|total| total > max_bytes)
            {
                carry = Some(next);
                break;
            }
            frame_count += next.frames.len();
            bytes += next_bytes;
            last = next.last;
            covered = covered.min(next.covered_through);
            requests.push(next);
        }
        let frames = requests
            .iter_mut()
            .flat_map(|request| std::mem::take(&mut request.frames))
            .collect();
        let result = transport
            .append(
                member,
                AppendRequest {
                    leader_session: leader,
                    log_epoch: epoch,
                    frames,
                    covered_through: covered,
                },
            )
            .await
            .map_err(Arc::new);
        // One canonical follower append covers the delivered group and its
        // single fsync. Every original request receives the same verified watermark.
        for request in requests {
            let result = result
                .as_ref()
                .copied()
                .map_err(|source| Error::Shared(Arc::clone(source)));
            let _ = request.completed.send(result);
        }
    }
}
