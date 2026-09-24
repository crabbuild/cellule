use std::{
    future::Future,
    pin::Pin,
    time::{SystemTime, UNIX_EPOCH},
};

use rand::RngCore;

use crate::queue::{
    MAX_BATCH_TIMEOUT_MS, QueueClaimRequest, QueueLeaseOutcome, QueueMessage, QueueModule,
    QueueNamespace, QueueState,
};
use crate::{
    Error, InvocationError, MutationIdentity, PendingMutation, Receipt, RegistryBuilder, RequestId,
    Result,
};

/// Default number of ready messages that closes a batch immediately.
pub const DEFAULT_MAX_BATCH_SIZE: u32 = 10;
/// Default wait that closes a partial batch.
pub const DEFAULT_MAX_BATCH_TIMEOUT_MS: u32 = 5_000;
/// Default delay before a failed batch becomes ready again.
pub const DEFAULT_RETRY_DELAY_MS: u32 = 10_000;
const MAX_IDENTITY_LIFETIME_MS: i64 = 60_000;
const MIN_LEASE_MS: u32 = 5_000;
const MAX_LEASE_MS: u32 = 300_000;

/// Per-message settlement returned by a native consumer for one batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueSettlement {
    Ack,
    Retry { delay_ms: u32 },
}

/// One claimed batch handed to a native consumer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueBatch {
    pub shard: u32,
    pub messages: Vec<QueueMessage>,
}

/// Future of one native consumer invocation.
pub type QueueConsumerFuture =
    Pin<Box<dyn Future<Output = Result<Vec<QueueSettlement>>> + Send + 'static>>;

/// Statically linked native consumer of one Queue module.
///
/// The consumer runs outside the SQL worker: the supervisor claims and
/// validates a batch, awaits this future, and settles every message through
/// ordinary commands. A handler that returns an error returns the whole batch
/// to the queue after [`Self::RETRY_DELAY_MS`].
pub trait QueueConsumer: QueueModule {
    /// Ready messages that close a batch immediately.
    const MAX_BATCH_SIZE: u32 = DEFAULT_MAX_BATCH_SIZE;
    /// Wait that closes a partial batch.
    const MAX_BATCH_TIMEOUT_MS: u32 = DEFAULT_MAX_BATCH_TIMEOUT_MS;
    /// Delay applied when this consumer fails a whole batch.
    const RETRY_DELAY_MS: u32 = DEFAULT_RETRY_DELAY_MS;

    fn consume(batch: QueueBatch) -> QueueConsumerFuture;
}

/// Validated batch policy compiled from one consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConsumerPolicy {
    max_batch_size: u32,
    max_batch_timeout_ms: u32,
    retry_delay_ms: u32,
}

impl QueueConsumerPolicy {
    /// Validates the compiled batch policy of one consumer.
    pub fn compiled<M: QueueConsumer>() -> Result<Self> {
        let policy = Self {
            max_batch_size: M::MAX_BATCH_SIZE,
            max_batch_timeout_ms: M::MAX_BATCH_TIMEOUT_MS,
            retry_delay_ms: M::RETRY_DELAY_MS,
        };
        if !(1..=MAX_CONSUMER_BATCH_SIZE).contains(&policy.max_batch_size) {
            return Err(Error::Registry(
                "queue consumer batch size must be in 1..=32",
            ));
        }
        if policy.max_batch_timeout_ms > MAX_BATCH_TIMEOUT_MS {
            return Err(Error::Registry(
                "queue consumer batch timeout must be within one minute",
            ));
        }
        if policy.retry_delay_ms > MAX_BATCH_TIMEOUT_MS * 60 {
            return Err(Error::Registry(
                "queue consumer retry delay must be within one hour",
            ));
        }
        Ok(policy)
    }

    #[must_use]
    pub const fn max_batch_size(self) -> u32 {
        self.max_batch_size
    }

    #[must_use]
    pub const fn max_batch_timeout_ms(self) -> u32 {
        self.max_batch_timeout_ms
    }

    #[must_use]
    pub const fn retry_delay_ms(self) -> u32 {
        self.retry_delay_ms
    }
}

/// Largest batch one consumer pass may claim.
pub const MAX_CONSUMER_BATCH_SIZE: u32 = 32;

/// Result of one consumer pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueConsumerOutcome {
    /// No ready message exists in this shard.
    Idle { receipt: Receipt },
    /// A partial batch is waiting for its batch timeout.
    Deferred { receipt: Receipt },
    /// Every claimed message was settled as the consumer requested.
    Completed {
        acked: u32,
        retried: u32,
        receipt: Receipt,
    },
    /// The consumer failed; the whole batch returns after the retry delay.
    HandlerFailed { retried: u32, receipt: Receipt },
    /// The published lease no longer matches, so nothing was settled.
    LeaseLost { receipt: Receipt },
}

/// Failure of one consumer pass.
#[derive(Debug)]
pub enum QueueConsumerError {
    /// The pass failed before or between settlements.
    Runtime(Error),
    /// A settlement outcome is unknown; the caller must resolve it.
    Pending(Box<PendingMutation>),
    /// A published result could not be interpreted.
    InvalidPublishedResult {
        receipt: Receipt,
        source: Box<Error>,
    },
}

impl std::fmt::Display for QueueConsumerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(formatter, "queue consumer failed: {error}"),
            Self::Pending(_) => formatter.write_str("queue consumer settlement is pending"),
            Self::InvalidPublishedResult { .. } => {
                formatter.write_str("queue consumer received an invalid published result")
            }
        }
    }
}

impl std::error::Error for QueueConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Pending(_) | Self::InvalidPublishedResult { .. } => None,
        }
    }
}

/// Runs one native consumer over one explicit Queue shard.
pub struct QueueConsumerSupervisor<M> {
    queue: QueueNamespace<M>,
    policy: QueueConsumerPolicy,
    lease_ms: u32,
}

/// Registers one compiled native Queue consumer.
pub fn register_queue_consumer<M: QueueConsumer>(
    registry: &mut RegistryBuilder,
) -> crate::Result<()> {
    registry.bind_queue_consumer::<M>()
}

impl<M: QueueConsumer> QueueConsumerSupervisor<M> {
    /// Creates a supervisor using a 5..=300 second delivery lease.
    pub fn new(queue: QueueNamespace<M>, lease_ms: u32) -> Result<Self> {
        if !(MIN_LEASE_MS..=MAX_LEASE_MS).contains(&lease_ms) {
            return Err(Error::Command(
                "queue consumer lease must be in 5..=300 seconds",
            ));
        }
        Ok(Self {
            queue,
            policy: QueueConsumerPolicy::compiled::<M>()?,
            lease_ms,
        })
    }

    /// Claims, runs, and settles at most one batch from one Queue shard.
    pub async fn run_once(
        &self,
        shard: u32,
    ) -> std::result::Result<QueueConsumerOutcome, QueueConsumerError> {
        let requested = self
            .queue
            .claim(
                internal_identity()?,
                shard,
                QueueClaimRequest {
                    limit: self.policy.max_batch_size(),
                    lease_ms: self.lease_ms,
                    max_batch_timeout_ms: self.policy.max_batch_timeout_ms(),
                },
            )
            .await
            .map_err(unexpected_invocation)?;
        if requested.output.is_empty() {
            let info = self
                .queue
                .info(shard, Some(requested.receipt))
                .await
                .map_err(unexpected_invocation)?;
            return Ok(if info.output.ready > 0 {
                QueueConsumerOutcome::Deferred {
                    receipt: info.receipt,
                }
            } else {
                QueueConsumerOutcome::Idle {
                    receipt: info.receipt,
                }
            });
        }
        let claimed = requested.output;
        let validation = self
            .queue
            .validate_claim(shard, claimed.clone(), Some(requested.receipt))
            .await
            .map_err(unexpected_invocation)?;
        if !validation.output {
            return Ok(QueueConsumerOutcome::LeaseLost {
                receipt: validation.receipt,
            });
        }

        let batch = QueueBatch {
            shard,
            messages: claimed.clone(),
        };
        let (failed, settlements) = match M::consume(batch).await {
            Ok(settlements) => (false, settlements),
            Err(_) => (
                true,
                vec![
                    QueueSettlement::Retry {
                        delay_ms: self.policy.retry_delay_ms(),
                    };
                    claimed.len()
                ],
            ),
        };
        if settlements.len() != claimed.len() {
            return Err(QueueConsumerError::Runtime(Error::Command(
                "queue consumer settlement count differs from its batch",
            )));
        }

        let mut acked = 0_u32;
        let mut retried = 0_u32;
        let mut receipt = validation.receipt;
        for (message, settlement) in claimed.iter().zip(settlements) {
            let settled = match settlement {
                QueueSettlement::Ack => {
                    self.queue
                        .ack(
                            internal_identity()?,
                            shard,
                            message.message_id,
                            message.token,
                        )
                        .await
                }
                QueueSettlement::Retry { delay_ms } => {
                    self.queue
                        .retry(
                            internal_identity()?,
                            shard,
                            message.message_id,
                            message.token,
                            delay_ms,
                        )
                        .await
                }
            };
            match settled {
                Ok(committed) => {
                    receipt = committed.receipt;
                    match committed.output {
                        QueueLeaseOutcome::Applied {
                            state: QueueState::Acked,
                            ..
                        } => acked += 1,
                        QueueLeaseOutcome::Applied {
                            state: QueueState::Dead,
                            ..
                        } => retried += 1,
                        QueueLeaseOutcome::Applied {
                            state: QueueState::Ready,
                            ..
                        } => retried += 1,
                        QueueLeaseOutcome::Applied {
                            state: QueueState::Leased,
                            ..
                        } => {
                            return Err(QueueConsumerError::Runtime(Error::Command(
                                "queue settlement returned a live lease",
                            )));
                        }
                        QueueLeaseOutcome::LeaseLost => {
                            return Ok(QueueConsumerOutcome::LeaseLost { receipt });
                        }
                    }
                }
                Err(InvocationError::Rejected(committed))
                    if committed.output == QueueLeaseOutcome::LeaseLost =>
                {
                    return Ok(QueueConsumerOutcome::LeaseLost {
                        receipt: committed.receipt,
                    });
                }
                Err(InvocationError::Pending(pending)) => {
                    return Err(QueueConsumerError::Pending(pending));
                }
                Err(error) => return Err(unexpected_invocation(error)),
            }
        }
        Ok(if failed {
            QueueConsumerOutcome::HandlerFailed { retried, receipt }
        } else {
            QueueConsumerOutcome::Completed {
                acked,
                retried,
                receipt,
            }
        })
    }
}

fn unexpected_invocation<T>(error: InvocationError<T>) -> QueueConsumerError {
    match error {
        InvocationError::Pending(pending) => QueueConsumerError::Pending(pending),
        InvocationError::InvalidPublishedResult { receipt, source } => {
            QueueConsumerError::InvalidPublishedResult { receipt, source }
        }
        InvocationError::NotStarted(error) => QueueConsumerError::Runtime(error),
        InvocationError::Rejected(_) => QueueConsumerError::Runtime(Error::Command(
            "queue consumer command was unexpectedly rejected",
        )),
    }
}

fn internal_identity() -> std::result::Result<MutationIdentity, QueueConsumerError> {
    let issued_at_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                QueueConsumerError::Runtime(Error::Command("system clock is before Unix epoch"))
            })?
            .as_millis(),
    )
    .map_err(|_| {
        QueueConsumerError::Runtime(Error::Command("system clock exceeds i64 milliseconds"))
    })?;
    let expires_at_ms =
        issued_at_ms
            .checked_add(MAX_IDENTITY_LIFETIME_MS)
            .ok_or(QueueConsumerError::Runtime(Error::Command(
                "queue consumer identity expiry overflow",
            )))?;
    let mut request_id = [0; 16];
    rand::rng().fill_bytes(&mut request_id);
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(request_id),
        issued_at_ms,
        expires_at_ms,
    })
}
