//! Read-only lifetime diagnostics from the canonical reader admission word.

use super::*;

/// Local snapshot and accepted-work closure shared by every reader clone.
///
/// This is interval evidence, not current Cell authority, durable enrollment
/// retirement, replacement-policy satisfaction or permission to stop a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadReplicaLifecycleObservation {
    receipt: Receipt,
    root: cellule_ltx::RootRef,
    admission_closed: bool,
    snapshot_attached: bool,
    retained_lifetimes: usize,
}

impl ReadReplicaLifecycleObservation {
    /// Returns the last installed position, including after snapshot detachment.
    #[must_use]
    pub const fn receipt(&self) -> Receipt {
        self.receipt
    }

    /// Exact last installed immutable root, retained after native detachment.
    ///
    /// Receipt and root come from the same shared snapshot-state lock. This is
    /// historical prefix identity, not current authority or a retention pin.
    #[must_use]
    pub const fn root(&self) -> cellule_ltx::RootRef {
        self.root
    }

    /// Reports the canonical irreversible closure of new reader operations.
    #[must_use]
    pub const fn admission_closed(&self) -> bool {
        self.admission_closed
    }

    /// Reports a view still installed in the state shared by all reader clones.
    #[must_use]
    pub const fn snapshot_attached(&self) -> bool {
        self.snapshot_attached
    }

    /// Counts original lifetime guards for snapshots and accepted operations.
    ///
    /// Accepted native work retains its guard after caller cancellation. This
    /// count is neither a query count nor a count of retained handle copies.
    #[must_use]
    pub const fn retained_lifetimes(&self) -> usize {
        self.retained_lifetimes
    }

    /// Reports detached shared state and completion of all original lifetimes.
    ///
    /// Closed plus zero is stable: the same admission CAS rejects new operations
    /// and snapshots cannot acquire a guard after the final lifetime is gone.
    /// Remote authority, producer retirement and replacement policy still need
    /// independent evidence, even when peer handles remain retained locally.
    #[must_use]
    pub const fn locally_joined(&self) -> bool {
        self.admission_closed && !self.snapshot_attached && self.retained_lifetimes == 0
    }
}

impl CellReadReplica {
    /// Observes the original admission and join state without starting new work.
    ///
    /// The snapshot read lock prevents replacement/detachment during capture;
    /// one load reads closure and lifetime count from their shared CAS word.
    /// Open counts remain advisory because accepted work may begin afterwards.
    /// The observation remains available after close and runtime shutdown.
    pub async fn lifecycle_observation(&self) -> ReadReplicaLifecycleObservation {
        let snapshot = self.snapshot.read().await;
        let lifetime = self.lifetime.state.load(Ordering::Acquire);
        ReadReplicaLifecycleObservation {
            receipt: receipt(self.expected, snapshot.root.commit_sequence),
            root: snapshot.root,
            admission_closed: lifetime & CLOSED != 0,
            snapshot_attached: snapshot.snapshot.is_some(),
            retained_lifetimes: lifetime & !CLOSED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_zero_cannot_acquire_another_operation_or_snapshot_lifetime() {
        let lifetime = Arc::new(ReplicaLifetime::default());
        let operation = lifetime.acquire(true).unwrap();
        let snapshot = lifetime.acquire(false).unwrap();
        assert_eq!(lifetime.state.load(Ordering::Acquire), 2);
        lifetime.state.fetch_or(CLOSED, Ordering::AcqRel);
        assert!(matches!(lifetime.acquire(true), Err(Error::Fenced)));
        // An already accepted open may finish while its original lifetime lives.
        let finishing_snapshot = lifetime.acquire(false).unwrap();
        drop(operation);
        drop(snapshot);
        assert_eq!(lifetime.state.load(Ordering::Acquire), CLOSED | 1);
        drop(finishing_snapshot);
        assert_eq!(lifetime.state.load(Ordering::Acquire), CLOSED);
        assert!(matches!(lifetime.acquire(true), Err(Error::Fenced)));
        assert!(matches!(lifetime.acquire(false), Err(Error::Fenced)));
        assert_eq!(lifetime.state.load(Ordering::Acquire), CLOSED);
    }
}
