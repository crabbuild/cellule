//! A bounded native group commits once and remains hidden until root proof.

use super::*;

/// Mutations committed together in one SQLite transaction and one published root.
///
/// A group only ever takes commands already queued behind the head, so the
/// ceiling adds no wait of its own: a shallow queue groups few, and a deep queue
/// fills the group. Each member keeps its own savepoint and request identity, and
/// one publication covers the whole group, so a larger ceiling amortizes the
/// immutable root, directory, index and body uploads over more acknowledged
/// commands. Measured at 32 concurrent clients against a local object store:
/// 2.29x throughput and 2.75x lower p50 against a ceiling of four, unchanged at
/// four concurrent clients where the queue never fills.
pub(crate) const MAX_NATIVE_GROUP: usize = 16;

pub(crate) struct NativeCommand<F> {
    pub(crate) identity: MutationIdentity,
    pub(crate) operation_digest: Digest,
    pub(crate) now_ms: i64,
    pub(crate) max_result_bytes: usize,
    pub(crate) handler: F,
}

pub(crate) struct NativeGroupExecution {
    pub(crate) outcomes: Vec<Result<StoredOutcome>>,
    pub(crate) pending: Option<Box<PendingCommit>>,
    pub(crate) base_sequence: u64,
}

impl CellExecutor {
    pub(crate) fn execute_group<F>(
        &mut self,
        commands: Vec<NativeCommand<F>>,
        deadline: std::time::Instant,
    ) -> Result<NativeGroupExecution>
    where
        F: FnOnce(&cellule_ltx::rusqlite::Transaction<'_>) -> Result<HandlerOutcome>,
    {
        if commands.is_empty() || commands.len() > MAX_NATIVE_GROUP {
            return Err(Error::Capacity("native command group"));
        }
        if self.fenced {
            return Err(Error::Fenced);
        }
        // A group cannot extend a follower-proven or unproved pending head.
        // The worker owns one SQLite transaction throughout, and the actor
        // remains occupied until publication; public visibility guards stay intact.
        if self.has_pending() || !self.accepts_publication() {
            return Err(Error::PendingPublication);
        }
        let base_sequence = self.published_sequence;
        let cell = self.cell;
        let incarnation = self.incarnation;
        let schema = self.schema;
        let transaction = self.db.transaction_with(|transaction| {
            let mut outcomes = Vec::with_capacity(commands.len());
            let mut newest = None;
            for command in commands {
                if std::time::Instant::now() >= deadline {
                    return Err(Error::Deadline);
                }
                // This savepoint also covers request-ledger and runtime metadata
                // writes. A command error cannot roll back an earlier member.
                transaction.execute_batch("SAVEPOINT native_command")?;
                match apply_mutation(transaction, cell, incarnation, schema, command) {
                    Ok(result) => {
                        transaction.execute_batch("RELEASE native_command")?;
                        match result {
                            TransactionResult::Recorded(outcome) => outcomes.push(Ok(outcome)),
                            TransactionResult::Committed { ref outcome, .. } => {
                                outcomes.push(Ok(outcome.clone()));
                                newest = Some(result);
                            }
                        }
                    }
                    Err(error) => {
                        // SQLite can abort the entire transaction (for example
                        // ON CONFLICT ROLLBACK). Preserve that failure and let
                        // Db perform its canonical whole-transaction cleanup.
                        if transaction.is_autocommit() {
                            return Err(error);
                        }
                        transaction
                            .execute_batch("ROLLBACK TO native_command; RELEASE native_command")?;
                        outcomes.push(Err(error));
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            Ok((outcomes, newest))
        });
        let (outcomes, newest) = self.check_transaction(transaction)?;
        let pending = match newest {
            Some(committed) => {
                self.finish_transaction(Ok(committed))?;
                Some(Box::new(
                    self.latest_pending().cloned().ok_or(Error::Fenced)?,
                ))
            }
            None => None,
        };
        Ok(NativeGroupExecution {
            outcomes,
            pending,
            base_sequence,
        })
    }
}
