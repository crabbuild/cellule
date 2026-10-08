//! Fleet-proven root cohorts wait outside scarce preparation admission.

use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use tokio::sync::Notify;

use super::{CellAdmission, MAX_PENDING_PUBLICATIONS, PendingDurability};

const BATCH_CAPTURES: usize = MAX_PENDING_PUBLICATIONS / 2;
const BATCH_AGE: Duration = Duration::from_secs(5);
const FOLLOWER_WAIT: Duration = Duration::from_millis(50);

#[derive(Default)]
pub(super) struct PublicationBatch {
    queued: AtomicUsize,
    flush: AtomicBool,
    changed: Notify,
}

impl PublicationBatch {
    pub(super) fn queued(&self, count: usize, object_fallback: bool) {
        self.queued.store(count, Ordering::Release);
        if object_fallback {
            self.flush.store(true, Ordering::Release);
        }
        self.changed.notify_one();
    }

    pub(super) fn flush(&self) {
        self.flush.store(true, Ordering::Release);
        self.changed.notify_one();
    }

    pub(super) fn reset(&self, count: usize, object_fallback: bool) {
        self.queued.store(count, Ordering::Release);
        self.flush.store(object_fallback, Ordering::Release);
    }

    fn ready(&self, admission: &CellAdmission) -> bool {
        admission.draining.load(Ordering::Acquire)
            || admission.fenced.load(Ordering::Acquire)
            || self.flush.load(Ordering::Acquire)
            || self.queued.load(Ordering::Acquire) >= BATCH_CAPTURES
    }
}

pub(super) async fn wait_for_batch(
    admission: &CellAdmission,
    durability: &PendingDurability,
    submitted_at: Instant,
) {
    // This is scheduling, never an ACK gate. Only an already-active original
    // Fleet lane can defer a root, and this particular complete capture must
    // first obtain its follower proof. Failure/stall starts ordinary fallback.
    if !durability.fleet_active()
        || admission.publication_batch.ready(admission)
        || !matches!(
            tokio::time::timeout(FOLLOWER_WAIT, durability.prove_fleet()).await,
            Ok(Ok(()))
        )
    {
        return;
    }
    let deadline = submitted_at + BATCH_AGE;
    loop {
        let changed = admission.publication_batch.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if admission.publication_batch.ready(admission)
            || !durability.fleet_active()
            || Instant::now() >= deadline
        {
            return;
        }
        tokio::select! {
            () = changed => {},
            () = tokio::time::sleep_until(deadline.into()) => return,
        }
    }
}
