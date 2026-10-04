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
mod follower_evacuation;
mod maintenance_enrollments;
mod reader_evacuation;
mod records;
mod writer_inventory;
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
    #[cfg(test)]
    boot_reply: Mutex<Option<BootReplyPause>>,
    #[cfg(test)]
    enrollment_reply: Mutex<Option<EnrollmentReplyPause>>,
    #[cfg(test)]
    reader_evacuation_reply: Mutex<Option<EnrollmentReplyPause>>,
    #[cfg(test)]
    follower_evacuation_reply: Mutex<Option<EnrollmentReplyPause>>,
    #[cfg(test)]
    original_writer_reply: Mutex<Option<BootReplyPause>>,
}

#[cfg(test)]
struct BootReplyPause {
    captured: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
struct EnrollmentReplyPause {
    before_acceptance: bool,
    publication: bool,
    lose_reply: bool,
    captured: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
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
                #[cfg(test)]
                boot_reply: Mutex::new(None),
                #[cfg(test)]
                enrollment_reply: Mutex::new(None),
                #[cfg(test)]
                reader_evacuation_reply: Mutex::new(None),
                #[cfg(test)]
                follower_evacuation_reply: Mutex::new(None),
                #[cfg(test)]
                original_writer_reply: Mutex::new(None),
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

    #[cfg(test)]
    pub(crate) fn pause_next_boot_reply(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (captured, observed) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let mut slot = self.inner.boot_reply.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(BootReplyPause {
            captured,
            resume: paused,
        });
        (observed, resume)
    }

    #[cfg(test)]
    pub(crate) fn pause_next_original_writer_reply(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (captured, observed) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let mut slot = self.inner.original_writer_reply.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(BootReplyPause {
            captured,
            resume: paused,
        });
        (observed, resume)
    }

    #[cfg(test)]
    async fn original_writer_reply(&self) {
        let pause = self.inner.original_writer_reply.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.captured.send(());
            let _ = pause.resume.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_next_enrollment_reply(
        &self,
        publication: bool,
        lose_reply: bool,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (captured, observed) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let mut slot = self.inner.enrollment_reply.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(EnrollmentReplyPause {
            before_acceptance: false,
            publication,
            lose_reply,
            captured,
            resume: paused,
        });
        (observed, resume)
    }

    #[cfg(test)]
    pub(crate) fn pause_next_reader_evacuation_reply(
        &self,
        lose_reply: bool,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (captured, observed) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let mut slot = self.inner.reader_evacuation_reply.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(EnrollmentReplyPause {
            before_acceptance: false,
            publication: true,
            lose_reply,
            captured,
            resume: paused,
        });
        (observed, resume)
    }
    #[cfg(test)]
    async fn reader_evacuation_reply(&self) -> JournalResult<()> {
        let pause = self.inner.reader_evacuation_reply.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.captured.send(());
            let _ = pause.resume.await;
            if pause.lose_reply {
                return Err(std::io::Error::other(
                    "injected lost reader evacuation reply after durable commit",
                )
                .into());
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn pause_next_follower_evacuation_reply(
        &self,
        lose_reply: bool,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (captured, observed) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let mut slot = self.inner.follower_evacuation_reply.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(EnrollmentReplyPause {
            before_acceptance: false,
            publication: true,
            lose_reply,
            captured,
            resume: paused,
        });
        (observed, resume)
    }
    #[cfg(test)]
    async fn follower_evacuation_reply(&self) -> JournalResult<()> {
        let pause = self.inner.follower_evacuation_reply.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.captured.send(());
            let _ = pause.resume.await;
            if pause.lose_reply {
                return Err(std::io::Error::other(
                    "injected lost follower evacuation reply after durable commit",
                )
                .into());
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn pause_before_enrollment_acceptance(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let result = self.pause_next_enrollment_reply(false, false);
        self.inner
            .enrollment_reply
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .before_acceptance = true;
        result
    }

    #[cfg(test)]
    async fn enrollment_reply(
        &self,
        publication: bool,
        before_acceptance: bool,
    ) -> JournalResult<()> {
        let pause = {
            let mut slot = self.inner.enrollment_reply.lock().unwrap();
            if slot.as_ref().is_some_and(|pause| {
                pause.publication == publication && pause.before_acceptance == before_acceptance
            }) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.captured.send(());
            let _ = pause.resume.await;
            if pause.lose_reply {
                return Err(std::io::Error::other(
                    "injected lost enrollment reply after durable commit",
                )
                .into());
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn lose_next_commit_reply(&self) {
        self.inner
            .lose_commit_reply
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
