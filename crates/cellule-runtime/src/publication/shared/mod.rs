//! One bounded cohort lane. No Cell dirty permit is held while awaiting upload.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use cellule_ltx::{
    SHARED_PUBLICATION_BYTES, SHARED_PUBLICATION_ROWS, SharedAppend, SharedCaptures,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::{
    Error, Result,
    fleet::resource::{ResourceCost, ResourceLedger, ResourceReservation},
};

const COHORT_UPLOADS: usize = 8;
const COHORT_DEADLINE: Duration = Duration::from_millis(1);

#[cfg(test)]
mod tests;

pub(crate) enum PublicationPermit {
    Direct(Box<cellule_ltx::CellReplica>),
    Shared(OwnedSemaphorePermit),
}

pub(crate) struct SharedPrepared {
    pub(crate) append: SharedAppend,
    // Index/row tables remain charged through per-Cell root preparation.
    _memory: ResourceReservation,
}

struct Entry {
    captures: SharedCaptures,
    scratch: PathBuf,
    accepted_at: Instant,
    memory: ResourceReservation,
    _slot: OwnedSemaphorePermit,
    reply: oneshot::Sender<Result<SharedPrepared>>,
}

enum Message {
    Capture(Box<Entry>),
    Shutdown,
}

pub(crate) struct SharedPublication {
    sender: mpsc::Sender<Message>,
    slots: Arc<Semaphore>,
    resources: ResourceLedger,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    worker: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
}

impl SharedPublication {
    pub(crate) fn new(
        resources: ResourceLedger,
        telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(SHARED_PUBLICATION_ROWS);
        Arc::new(Self {
            sender,
            slots: Arc::new(Semaphore::new(SHARED_PUBLICATION_ROWS)),
            resources,
            telemetry: telemetry.clone(),
            worker: tokio::sync::Mutex::new(Some(tokio::spawn(run(receiver, telemetry)))),
        })
    }

    pub(crate) async fn admit(&self) -> Result<OwnedSemaphorePermit> {
        self.slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::RuntimeClosed)
    }

    pub(crate) async fn submit(
        &self,
        replica: &cellule_ltx::CellReplica,
        cuts: &cellule_ltx::CaptureBatch,
        scratch: PathBuf,
        slot: OwnedSemaphorePermit,
    ) -> Result<Option<SharedPrepared>> {
        let estimate = cuts.segments.iter().try_fold(16_usize, |total, segment| {
            let info = segment.info();
            let row = usize::try_from(info.size_bytes)
                .ok()
                .and_then(|bytes| {
                    (info.database_pages as usize)
                        .checked_mul(60)
                        .and_then(|index| bytes.checked_add(index))
                })
                .and_then(|bytes| bytes.checked_add(112))
                .ok_or(Error::Capacity("shared publication bytes"))?;
            total
                .checked_add(row)
                .ok_or(Error::Capacity("shared publication bytes"))
        })?;
        if estimate as u64 > SHARED_PUBLICATION_BYTES
            || cuts.segments.len() > SHARED_PUBLICATION_ROWS
        {
            self.telemetry.shared_publication_fallback(false);
            return Ok(None);
        }
        // Charge before pinning/indexing/coalescing. Multiple compressed cuts
        // can expand into a bounded 256 KiB page map before being re-encoded.
        let work = if cuts.segments.len() > 1 {
            estimate.max(SHARED_PUBLICATION_BYTES as usize)
        } else {
            estimate
        };
        let memory = work
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(132 << 10))
            .ok_or(Error::Capacity("shared publication memory"))?;
        let cost = ResourceCost::zero()
            .with_retained_bytes(memory)
            .with_publication_file_descriptors(cuts.segments.len() + 2);
        let memory = match self.resources.try_reserve(cost) {
            Ok(memory) => memory,
            // Sharing is optional representation reduction. Never wait for
            // memory held by roots which need this very lane to finish.
            Err(Error::Capacity(_)) => {
                self.telemetry.shared_publication_fallback(true);
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let Some(captures) = replica.shared_captures(cuts).await? else {
            self.telemetry.shared_publication_fallback(false);
            return Ok(None);
        };
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Message::Capture(Box::new(Entry {
                captures,
                scratch,
                accepted_at: Instant::now(),
                memory,
                _slot: slot,
                reply,
            })))
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        // The lane, rather than the waiter, owns dispatched storage and scratch.
        response.await.map_err(|_| Error::RuntimeClosed)?.map(Some)
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        // Called after actor publication tasks join. Closing earlier could
        // strand a committed Cell still waiting to enter its publication lane.
        self.slots.close();
        self.sender
            .send(Message::Shutdown)
            .await
            .map_err(|_| Error::RuntimeClosed)?;
        if let Some(worker) = self.worker.lock().await.take() {
            worker.await.map_err(|source| Error::PeerTransport {
                context: "shared publication worker",
                source: Box::new(source),
            })??;
        }
        Ok(())
    }
}

async fn run(
    mut receiver: mpsc::Receiver<Message>,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
) -> Result<()> {
    let mut pending = None;
    let mut uploads = tokio::task::JoinSet::new();
    let mut failure = None;
    loop {
        while uploads.len() >= COHORT_UPLOADS {
            joined_upload(uploads.join_next().await, &mut failure);
        }
        let first = match pending.take() {
            Some(entry) => entry,
            None => match receiver.recv().await {
                Some(Message::Capture(entry)) => *entry,
                Some(Message::Shutdown) | None => break,
            },
        };
        let binding = first.captures.storage_binding();
        let started = first.accepted_at;
        // Give already-prepared siblings one scheduler turn to enqueue. Never
        // add a timer to a lone bucket waiter: provider latency naturally fills
        // the bounded queue at load. The real assembly bound is independent of
        // an embedding application's paused or advanced Tokio clock.
        let deadline = Instant::now() + COHORT_DEADLINE;
        tokio::task::yield_now().await;
        let mut bytes = 16 + first.captures.encoded_bytes();
        let mut rows = first.captures.rows();
        let mut entries = vec![first];
        let mut shutdown = false;
        while rows < SHARED_PUBLICATION_ROWS && bytes < SHARED_PUBLICATION_BYTES {
            if Instant::now() >= deadline {
                break;
            }
            let message = match receiver.try_recv() {
                Ok(message) => Some(message),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            };
            let entry = match message {
                Some(Message::Capture(entry)) => *entry,
                Some(Message::Shutdown) | None => {
                    shutdown = true;
                    break;
                }
            };
            if entry.captures.storage_binding() != binding
                || bytes + entry.captures.encoded_bytes() > SHARED_PUBLICATION_BYTES
                || rows + entry.captures.rows() > SHARED_PUBLICATION_ROWS
            {
                pending = Some(entry);
                break;
            }
            bytes += entry.captures.encoded_bytes();
            rows += entry.captures.rows();
            entries.push(entry);
        }
        let age = started.elapsed();
        uploads.spawn(upload_cohort(entries, telemetry.clone(), rows, bytes, age));
        if shutdown {
            break;
        }
    }
    while let Some(result) = uploads.join_next().await {
        joined_upload(Some(result), &mut failure);
    }
    failure.map_or(Ok(()), Err)
}

fn joined_upload(
    result: Option<std::result::Result<(), tokio::task::JoinError>>,
    failure: &mut Option<Error>,
) {
    if let Some(Err(source)) = result
        && failure.is_none()
    {
        *failure = Some(Error::PeerTransport {
            context: "shared cohort upload task",
            source: Box::new(source),
        });
    }
}

async fn upload_cohort(
    entries: Vec<Entry>,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    rows: usize,
    bytes: u64,
    age: Duration,
) {
    let scratch = entries[0].scratch.clone();
    let mut inputs = Vec::with_capacity(entries.len());
    let mut replies = Vec::with_capacity(entries.len());
    for entry in entries {
        inputs.push(entry.captures);
        replies.push((entry.reply, entry.memory, entry._slot));
    }
    let cells = inputs.len() as u64;
    let upload_started = Instant::now();
    let result = cellule_ltx::CellReplica::upload_shared(inputs, &scratch).await;
    if cells == 1 && rows == 1 && result.is_ok() {
        // The native inputs stay pinned and charged through canonical root
        // preparation. This path performs no shared-object upload.
        telemetry.shared_publication_singleton(age);
    } else {
        telemetry.shared_publication(crate::fleet::telemetry::SharedPublicationTiming {
            cells,
            rows: rows as u64,
            bytes,
            queue: age,
            upload: upload_started.elapsed(),
            succeeded: result.is_ok(),
        });
    }
    tracing::debug!(
        rows,
        bytes,
        age_us = age.as_micros(),
        succeeded = result.is_ok(),
        "shared publication cohort"
    );
    match result {
        Ok(appends) if appends.len() == replies.len() => {
            for (append, (reply, memory, _slot)) in appends.into_iter().zip(replies) {
                let _ = reply.send(Ok(SharedPrepared {
                    append,
                    _memory: memory,
                }));
            }
        }
        result => {
            let error = Arc::new(match result {
                Err(error) => Error::Ltx(error),
                Ok(_) => Error::Control("shared publication result count"),
            });
            for (reply, _memory, _slot) in replies {
                let _ = reply.send(Err(Error::Shared(error.clone())));
            }
        }
    }
}
