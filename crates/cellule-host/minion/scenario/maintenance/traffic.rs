//! Bounded client lanes retain every accepted outcome across native quiescence.
use super::*;
use cellule_runtime::Error;
use tokio::{sync::oneshot, task::JoinHandle};

const LANES: usize = 2;
const MAX_COMMANDS_PER_LANE: u64 = 512;
type ClientError = Arc<dyn std::error::Error + Send + Sync>;

struct Receipt {
    identity: MutationIdentity,
    digest: Digest,
    outcome: StoredOutcome,
}
struct Lane {
    receipts: Vec<Receipt>,
    refused: bool,
}
pub(super) struct Traffic {
    cell: CellId,
    source: CellHandle,
    stop: CancellationToken,
    started: Vec<oneshot::Receiver<Result<(), ClientError>>>,
    tasks: Vec<JoinHandle<JournalResult<Lane>>>,
}
pub(super) struct JoinedTraffic {
    cell: CellId,
    source: CellHandle,
    lanes: Vec<Lane>,
}

impl Traffic {
    pub(super) fn start(acknowledged: &HashMap<CellId, Acknowledged>) -> JournalResult<Self> {
        let cell = acknowledged
            .keys()
            .min_by_key(|cell| *cell.as_bytes())
            .copied()
            .ok_or_else(|| invalid("busy maintenance lacks an original writer"))?;
        let source = acknowledged
            .get(&cell)
            .ok_or_else(|| invalid("busy maintenance original writer disappeared"))?
            .source
            .clone();
        let stop = CancellationToken::new();
        let mut started = Vec::with_capacity(LANES);
        let mut tasks = Vec::with_capacity(LANES);
        for lane in 0..LANES {
            let (signal, ready) = oneshot::channel();
            started.push(ready);
            let source = source.clone();
            let stop = stop.clone();
            tasks.push(tokio::spawn(async move {
                offer(source, stop, lane, signal).await
            }));
        }
        Ok(Self {
            cell,
            source,
            stop,
            started,
            tasks,
        })
    }

    pub(super) async fn started(&mut self) -> JournalResult<()> {
        for ready in self.started.drain(..) {
            let ready = tokio::time::timeout(Duration::from_secs(8), ready).await??;
            ready.map_err(|error| Box::new(ClientFailure(error)) as JournalError)?;
        }
        Ok(())
    }

    pub(super) async fn finish(mut self) -> JournalResult<JoinedTraffic> {
        self.stop.cancel();
        let mut lanes = Vec::with_capacity(LANES);
        let mut errors = Vec::new();
        // Join every sibling even after one failed. A dropped execute waiter
        // cannot establish whether its native SQL or durable response finished.
        for task in std::mem::take(&mut self.tasks) {
            match task.await {
                Ok(Ok(lane)) => lanes.push(lane),
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(Box::new(error) as JournalError),
            }
        }
        if !errors.is_empty() {
            return Err(Box::new(TrafficFailures(errors)));
        }
        Ok(JoinedTraffic {
            cell: self.cell,
            source: self.source.clone(),
            lanes,
        })
    }
}

impl Drop for Traffic {
    fn drop(&mut self) {
        // Cancellation stops new offering. Accepted execute calls stay with
        // their native actors and the finite client tasks until completion.
        self.stop.cancel();
    }
}

async fn offer(
    source: CellHandle,
    stop: CancellationToken,
    lane: usize,
    signal: oneshot::Sender<Result<(), ClientError>>,
) -> JournalResult<Lane> {
    let mut signal = Some(signal);
    match offer_commands(source, stop, lane, &mut signal).await {
        Err(error) if signal.is_some() => {
            let shared = ClientError::from(error);
            if let Some(signal) = signal.take() {
                let _ = signal.send(Err(shared.clone()));
            }
            Err(Box::new(ClientFailure(shared)))
        }
        other => other,
    }
}

async fn offer_commands(
    source: CellHandle,
    stop: CancellationToken,
    lane: usize,
    signal: &mut Option<oneshot::Sender<Result<(), ClientError>>>,
) -> JournalResult<Lane> {
    let mut receipts = Vec::new();
    for sequence in 1..=MAX_COMMANDS_PER_LANE {
        if stop.is_cancelled() {
            return Ok(Lane {
                receipts,
                refused: false,
            });
        }
        let now = clock()?;
        let mut id = [240; 16];
        id[1] = lane as u8;
        id[2..10].copy_from_slice(&sequence.to_be_bytes());
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes(id),
            issued_at_ms: now,
            expires_at_ms: now
                .checked_add(300_000)
                .ok_or_else(|| invalid("busy maintenance request deadline overflow"))?,
        };
        let digest = Digest::from_bytes(*blake3::hash(&id).as_bytes());
        match source
            .execute(identity, digest, now, 64, 64, move |transaction| {
                transaction.execute_batch(
                    "CREATE TABLE IF NOT EXISTS maintenance_commands(request BLOB PRIMARY KEY)",
                )?;
                transaction.execute(
                    "INSERT INTO maintenance_commands(request) VALUES (?1)",
                    [id.as_slice()],
                )?;
                Ok(HandlerOutcome::Success(id.to_vec()))
            })
            .await
        {
            Ok(outcome) => {
                receipts.push(Receipt {
                    identity,
                    digest,
                    outcome,
                });
                if let Some(signal) = signal.take() {
                    let _ = signal.send(Ok(()));
                }
            }
            Err(Error::CellDraining) if !receipts.is_empty() => {
                return Ok(Lane {
                    receipts,
                    refused: true,
                });
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
    Err(invalid(
        "busy maintenance exhausted offered load before closing admission",
    ))
}

impl JoinedTraffic {
    pub(super) async fn verify(
        self,
        nodes: &[Arc<CellNode>],
        records: &HashMap<CellId, Record>,
    ) -> JournalResult<usize> {
        if self.lanes.len() != LANES
            || self
                .lanes
                .iter()
                .any(|lane| !lane.refused || lane.receipts.is_empty())
        {
            return Err(invalid(
                "busy maintenance did not fence every sustained command lane",
            ));
        }
        let record = records
            .get(&self.cell)
            .ok_or_else(|| invalid("busy maintenance readback record is missing"))?;
        let current = record
            .authority
            .load(self.cell)
            .await?
            .ok_or_else(|| invalid("busy maintenance successor authority is missing"))?;
        let destination = current
            .value()
            .owner
            .as_ref()
            .ok_or_else(|| invalid("busy maintenance successor owner is missing"))?
            .session;
        let node = nodes
            .iter()
            .enumerate()
            .find(|(index, _)| session(*index) == destination)
            .map(|(_, node)| node)
            .ok_or_else(|| invalid("busy maintenance successor endpoint is missing"))?;
        if destination == session(0) || nodes[0].state() != NodeState::Stopped {
            return Err(invalid("busy maintenance retained its source writer"));
        }
        let handle = node
            .runtime()
            .local_handle(record.catalog.clone(), &current)
            .await?
            .ok_or_else(|| invalid("busy maintenance has no canonical successor actor"))?;
        let mut count = 0;
        for lane in self.lanes {
            for receipt in lane.receipts {
                if handle
                    .resolve(receipt.identity, receipt.digest, clock()?, 64)
                    .await?
                    != Resolution::Committed(receipt.outcome.clone())
                {
                    return Err(invalid("busy maintenance lost an accepted command outcome"));
                }
                let StoredOutcome::Success { result, .. } = receipt.outcome else {
                    return Err(invalid(
                        "busy maintenance accepted a non-success SQL outcome",
                    ));
                };
                if result != receipt.identity.request_id.as_bytes() {
                    return Err(invalid(
                        "busy maintenance changed an accepted command result",
                    ));
                }
                count += 1;
            }
        }
        let rows = handle
            .query(64, 64, |database| {
                Ok(database
                    .query_row("SELECT COUNT(*) FROM maintenance_commands", [], |row| {
                        row.get::<_, u64>(0)
                    })?
                    .to_be_bytes()
                    .to_vec())
            })
            .await?;
        if rows != (count as u64).to_be_bytes() {
            return Err(invalid("busy maintenance duplicated or lost SQL commands"));
        }
        match self.source.query(64, 64, |_| Ok(Vec::new())).await {
            Err(
                Error::Fenced | Error::CellDraining | Error::CellNotActive | Error::RuntimeClosed,
            ) => {}
            Err(error) => return Err(Box::new(error)),
            Ok(_) => return Err(invalid("busy maintenance retained source service")),
        }
        println!(
            "maintenance_traffic lanes={LANES} accepted_commands={count} admission_refusals={LANES} exact_audit_rows={count}"
        );
        Ok(count)
    }
}

#[derive(Debug)]
struct ClientFailure(ClientError);
impl std::fmt::Display for ClientFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
impl std::error::Error for ClientFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug)]
struct TrafficFailures(Vec<JournalError>);
impl std::fmt::Display for TrafficFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "busy maintenance client failures: {:?}", self.0)
    }
}
impl std::error::Error for TrafficFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error.as_ref() as _)
    }
}
