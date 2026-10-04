//! Dirty-memory and recovery admission without holding a partial new cohort.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, Weak},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{Host, HostPermit, HostResourceKind, LtxPhase};

pub(super) struct PairGate {
    _dirty: Arc<Semaphore>,
    _recovery: Arc<Semaphore>,
    queue: tokio::sync::Mutex<()>,
}
type PairGates = HashMap<(usize, usize), Weak<PairGate>>;

pub(super) fn shared_pair_gate(dirty: &Arc<Semaphore>, recovery: &Arc<Semaphore>) -> Arc<PairGate> {
    static GATES: OnceLock<std::sync::Mutex<PairGates>> = OnceLock::new();
    let mut gates = GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    gates.retain(|_, gate| gate.strong_count() > 0);
    // Every gate owner also owns both semaphores, so a live key cannot be
    // recycled. Weak entries do not retain retired pools. Sharing by pool
    // identity covers separate Host values configured with the same slots;
    // independent pools must not wait behind each other's admission queue.
    let key = (Arc::as_ptr(dirty) as usize, Arc::as_ptr(recovery) as usize);
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(PairGate {
        _dirty: dirty.clone(),
        _recovery: recovery.clone(),
        queue: tokio::sync::Mutex::new(()),
    });
    gates.insert(key, Arc::downgrade(&gate));
    gate
}

impl Host {
    pub(crate) async fn for_dirty(&self) -> crate::Result<Self> {
        let mut host = self.clone();
        if host.dirty.is_none() {
            let permit = self
                .acquire_memory_slot(&self.dirty_slots, LtxPhase::DirtyAdmission)
                .await?;
            host.dirty = Some(self.reserve_memory_permit(HostResourceKind::Dirty, permit)?);
        }
        Ok(host)
    }

    pub(crate) async fn for_recovery(&self) -> crate::Result<Self> {
        if self.dirty.is_none() && self.recovery.is_none() {
            let (dirty, recovery) = self.acquire_memory_pair().await?;
            let mut host = self.clone();
            host.dirty = Some(self.reserve_memory_permit(HostResourceKind::Dirty, dirty)?);
            host.recovery = Some(self.reserve_memory_permit(HostResourceKind::Recovery, recovery)?);
            return Ok(host);
        }

        // Existing scopes own real work and retain their reservations. In
        // particular, a dirty scope may request recovery while a new cohort
        // is waiting for dirty capacity; the new cohort must leave recovery
        // available to that scope rather than invert its acquisition order.
        let mut host = self.for_dirty().await?;
        if host.recovery.is_none() {
            let permit = self
                .acquire_memory_slot(&self.recovery_slots, LtxPhase::RecoveryAdmission)
                .await?;
            host.recovery = Some(self.reserve_memory_permit(HostResourceKind::Recovery, permit)?);
        }
        Ok(host)
    }

    async fn acquire_memory_pair(
        &self,
    ) -> crate::Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        // Only one new cohort negotiates a pair at a time. Otherwise opposing
        // semaphore wait queues can repeatedly hand each other partial pairs.
        // Existing charged scopes bypass this gate and can finish their work.
        let _gate = self.memory_pair_gate.queue.lock().await;
        loop {
            let recovery = self
                .acquire_memory_slot(&self.recovery_slots, LtxPhase::RecoveryAdmission)
                .await?;
            let started = self.now_monotonic();
            if let Ok(dirty) = self.dirty_slots.clone().try_acquire_owned() {
                self.observe_ltx_phase(LtxPhase::DirtyAdmission, started, true);
                return Ok((dirty, recovery));
            }
            // Never await the second slot with the first still held. If the
            // try failed because the semaphore closed, the following awaited
            // acquire preserves the original AcquireError source as well.
            drop(recovery);
            let dirty = self
                .acquire_memory_slot(&self.dirty_slots, LtxPhase::DirtyAdmission)
                .await?;
            let started = self.now_monotonic();
            if let Ok(recovery) = self.recovery_slots.clone().try_acquire_owned() {
                self.observe_ltx_phase(LtxPhase::RecoveryAdmission, started, true);
                return Ok((dirty, recovery));
            }
            drop(dirty);
        }
    }

    async fn acquire_memory_slot(
        &self,
        slots: &Arc<Semaphore>,
        phase: LtxPhase,
    ) -> crate::Result<OwnedSemaphorePermit> {
        let started = self.now_monotonic();
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| crate::LtxError::Other(Box::new(error)));
        self.observe_ltx_phase(phase, started, permit.is_ok());
        permit
    }

    fn reserve_memory_permit(
        &self,
        kind: HostResourceKind,
        semaphore: OwnedSemaphorePermit,
    ) -> crate::Result<Arc<HostPermit>> {
        Ok(Arc::new(HostPermit {
            _resource: self.reserve_resource(kind, 1)?,
            semaphore,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_pools_share_a_gate_and_independent_pools_do_not() {
        let dirty = Arc::new(Semaphore::new(1));
        let recovery = Arc::new(Semaphore::new(1));
        let first = shared_pair_gate(&dirty, &recovery);
        let same = shared_pair_gate(&dirty, &recovery);
        let other = shared_pair_gate(&Arc::new(Semaphore::new(1)), &recovery);
        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &other));
    }
}
