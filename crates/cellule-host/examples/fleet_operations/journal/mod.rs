//! Local durable reference adapter. One SQLite file is one journal scope.
//! This proves local transactions, not a distributed provider's linearizability.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cellule_host::fleet::*;
use cellule_runtime::fleet::operations::*;
use cellule_runtime::identity::{Digest, NodeId, SessionId};
use cellule_runtime::node::NodeMode;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use tokio::sync::{Notify, Semaphore};

mod actions;
mod controller;
mod enrollment;
mod records;
use records::{Db, blob, profile_bytes, scope_bytes};

#[cfg(test)]
mod tests;

pub type JournalError = Box<dyn std::error::Error + Send + Sync>;
pub type JournalResult<T> = std::result::Result<T, JournalError>;

const MAX_PENDING_JOBS: usize = 32;
const FORMAT: i64 = 1;

struct Inner {
    connection: Mutex<Option<Connection>>,
    activity: Mutex<Activity>,
    changed: Notify,
    slots: Arc<Semaphore>,
    scope: FleetScope,
    profile: FleetProfile,
    #[cfg(test)]
    lose_commit_reply: std::sync::atomic::AtomicBool,
}

struct Activity {
    accepting: bool,
    pending: usize,
}

/// Independently reconstructable client of the example's shared SQLite file.
/// Each call owns its bounded blocking job through commit, even if its waiter
/// disappears. `close` stops admission and joins all accepted jobs first.
#[derive(Clone)]
pub struct SqliteJournal {
    inner: Arc<Inner>,
}

struct ActiveJob {
    inner: Arc<Inner>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Drop for ActiveJob {
    fn drop(&mut self) {
        // A poisoned supervisor closes future admission, but still settles the
        // count so shutdown cannot strand a successfully accepted blocking job.
        drop(self.permit.take());
        let mut state = self
            .inner
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending = state.pending.saturating_sub(1);
        self.inner.changed.notify_waiters();
    }
}

impl SqliteJournal {
    pub async fn open(
        path: PathBuf,
        scope: FleetScope,
        profile: FleetProfile,
        now_ms: i64,
    ) -> JournalResult<Self> {
        let profile = profile.validate()?;
        let head = FleetHead::new(scope, now_ms)?.to_bytes()?;
        let registry = RegistryVersion::new(scope)?.to_bytes()?;
        let connection = tokio::runtime::Handle::try_current()?.spawn_blocking(move || -> JournalResult<Connection> {
            let mut connection = Connection::open(path)?;
            connection.busy_timeout(Duration::from_secs(5))?;
            connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(include_str!("schema.sql"))?;
            tx.execute("INSERT OR IGNORE INTO state (singleton, format, scope, profile, head, registry) VALUES (1, ?1, ?2, ?3, ?4, ?5)", params![FORMAT, scope_bytes(scope), profile_bytes(profile)?, head, registry])?;
            let (format, stored_scope, stored_profile) = tx.query_row("SELECT format, scope, profile FROM state WHERE singleton=1", [], |row| Ok((row.get::<_, i64>(0)?, blob(row, 1, 48)?, blob(row, 2, 32)?)))?;
            if format != FORMAT || stored_scope != scope_bytes(scope) || stored_profile != profile_bytes(profile)? {
                return Err(OperationError::Conflict.into());
            }
            Db { tx: &tx, scope, profile }.snapshot()?;
            tx.commit()?;
            Ok(connection)
        }).await??;
        Ok(Self {
            inner: Arc::new(Inner {
                connection: Mutex::new(Some(connection)),
                activity: Mutex::new(Activity {
                    accepting: true,
                    pending: 0,
                }),
                changed: Notify::new(),
                slots: Arc::new(Semaphore::new(MAX_PENDING_JOBS)),
                scope,
                profile,
                #[cfg(test)]
                lose_commit_reply: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Db<'_>) -> JournalResult<T> + Send + 'static,
    ) -> JournalResult<T> {
        let runtime = tokio::runtime::Handle::try_current()?;
        let active = {
            let mut state = self
                .inner
                .activity
                .lock()
                .map_err(|_| std::io::Error::other("journal supervisor lock poisoned"))?;
            if !state.accepting {
                return Err(cellule_runtime::Error::RuntimeClosed.into());
            }
            let permit = Arc::clone(&self.inner.slots)
                .try_acquire_owned()
                .map_err(|_| cellule_runtime::Error::Capacity("reference journal jobs"))?;
            state.pending += 1;
            ActiveJob {
                inner: Arc::clone(&self.inner),
                permit: Some(permit),
            }
        };
        let inner = Arc::clone(&self.inner);
        runtime
            .spawn_blocking(move || {
                let _active = active;
                let mut guard = inner
                    .connection
                    .lock()
                    .map_err(|_| std::io::Error::other("journal connection lock poisoned"))?;
                let connection = guard
                    .as_mut()
                    .ok_or(cellule_runtime::Error::RuntimeClosed)?;
                // Acceptance, intent checks and evidence publication all use this
                // transaction boundary. Never release it between validation/write.
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let value = work(&Db {
                    tx: &tx,
                    scope: inner.scope,
                    profile: inner.profile,
                })?;
                tx.commit()?;
                #[cfg(test)]
                if inner
                    .lose_commit_reply
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(
                        std::io::Error::other("injected lost reply after durable commit").into(),
                    );
                }
                Ok(value)
            })
            .await?
    }

    pub async fn close(&self) -> JournalResult<()> {
        let runtime = tokio::runtime::Handle::try_current()?;
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let pending = {
                let mut state = self
                    .inner
                    .activity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.accepting = false;
                state.pending
            };
            if pending == 0 {
                break;
            }
            changed.await;
        }
        let inner = Arc::clone(&self.inner);
        runtime
            .spawn_blocking(move || -> JournalResult<()> {
                let mut guard = inner
                    .connection
                    .lock()
                    .map_err(|_| std::io::Error::other("journal connection lock poisoned"))?;
                if let Some(connection) = guard.take()
                    && let Err((connection, error)) = connection.close()
                {
                    *guard = Some(connection);
                    return Err(error.into());
                }
                Ok(())
            })
            .await?
    }
}
