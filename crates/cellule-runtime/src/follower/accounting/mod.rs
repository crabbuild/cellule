//! Lane-scoped disk settlement without holding accounting across filesystem I/O.

use super::*;
use std::cell::Cell as ByteCount;

pub(super) struct DiskAccounting {
    reservation: cellule_ltx::DiskReservation,
    // A dispatched worker owns this charge until its lane can be recounted.
    // Failed recounts retain both the old bytes and every admitted growth byte.
    pending: HashMap<Lane, u64>,
    settled: HashMap<Lane, u64>,
}

#[cfg(test)]
mod tests;

impl DiskAccounting {
    pub fn new(reservation: cellule_ltx::DiskReservation) -> Self {
        Self {
            reservation,
            pending: HashMap::new(),
            settled: HashMap::new(),
        }
    }

    pub fn bytes(&self) -> u64 {
        self.reservation.bytes()
    }

    fn settle(&mut self, lane: Lane, actual: u64) -> Result<()> {
        let charged = self
            .pending
            .get(&lane)
            .copied()
            .ok_or(Error::Node("follower lane has no disk charge"))?;
        let next = self
            .bytes()
            .checked_sub(charged)
            .and_then(|bytes| bytes.checked_add(actual))
            .ok_or(Error::Node("follower disk settlement overflow"))?;
        self.reservation.resize(next)?;
        self.pending.remove(&lane);
        self.settled.insert(lane, actual);
        Ok(())
    }

    fn begin(&mut self, lane: Lane, actual: u64, growth: u64) -> Result<()> {
        // The caller holds the lane mutex. A previous failed settlement cannot
        // race this reconciliation, and other lanes' charges remain untouched.
        if self.pending.contains_key(&lane) {
            self.settle(lane, actual)?;
        }
        if let Some(previous) = self.settled.get(&lane).copied()
            && previous != actual
        {
            // Recovery can observe externally restored or truncated bytes.
            // Reconcile only this lane before reserving any new write growth.
            let next = self
                .bytes()
                .checked_sub(previous)
                .and_then(|bytes| bytes.checked_add(actual))
                .ok_or(Error::Node("follower disk settlement overflow"))?;
            self.reservation.resize(next)?;
            self.settled.insert(lane, actual);
        }
        let charged = actual
            .checked_add(growth)
            .ok_or(Error::Node("follower disk charge overflow"))?;
        self.reservation.try_grow(growth)?;
        self.pending.insert(lane, charged);
        Ok(())
    }

    fn grow(&mut self, lane: Lane, growth: u64) -> Result<()> {
        let charged = self
            .pending
            .get_mut(&lane)
            .ok_or(Error::Node("follower lane has no disk charge"))?;
        let next = charged
            .checked_add(growth)
            .ok_or(Error::Node("follower disk charge overflow"))?;
        self.reservation.try_grow(growth)?;
        *charged = next;
        Ok(())
    }
}

/// Used only while the caller holds the lane mutex and maintenance barrier.
pub(super) struct LaneAccounting {
    shared: Arc<Mutex<DiskAccounting>>,
    directory: PathBuf,
    lane: Lane,
    actual: ByteCount<Option<u64>>,
}

impl LaneAccounting {
    pub fn begin(
        shared: Arc<Mutex<DiskAccounting>>,
        root: &Path,
        lane: Lane,
        growth: u64,
        reconcile: bool,
        observation: &mut AppendObservation,
    ) -> Result<Self> {
        let operation = Self {
            shared,
            directory: lane_directory(root, lane),
            lane,
            actual: ByteCount::new(None),
        };
        let cached = operation.update(observation, |accounting| {
            Ok(if reconcile || accounting.pending.contains_key(&lane) {
                None
            } else {
                accounting.settled.get(&lane).copied()
            })
        })?;
        let actual = match cached {
            Some(bytes) => bytes,
            None => operation.recount(observation)?,
        };
        operation.update(observation, |accounting| {
            accounting.begin(lane, actual, growth)
        })?;
        operation.actual.set(Some(actual));
        Ok(operation)
    }

    pub fn invalidate(&self) {
        self.actual.set(None);
    }

    pub fn added(&self, bytes: u64) -> Result<()> {
        if let Some(actual) = self.actual.get() {
            self.actual.set(Some(
                actual
                    .checked_add(bytes)
                    .ok_or(Error::Node("follower retained byte count overflow"))?,
            ));
        }
        Ok(())
    }

    pub fn removed(&self, bytes: u64) -> Result<()> {
        if let Some(actual) = self.actual.get() {
            self.actual
                .set(Some(actual.checked_sub(bytes).ok_or(Error::Node(
                    "follower removed byte count exceeds retained bytes",
                ))?));
        }
        Ok(())
    }

    pub fn emptied(&self) {
        self.actual.set(Some(0));
    }

    pub fn forget_empty(&self, observation: &mut AppendObservation) -> Result<()> {
        self.update(observation, |accounting| {
            if accounting.settled.get(&self.lane) == Some(&0) {
                accounting.settled.remove(&self.lane);
            }
            Ok(())
        })
    }

    pub fn try_grow(&self, growth: u64, observation: &mut AppendObservation) -> Result<()> {
        self.update(observation, |accounting| accounting.grow(self.lane, growth))
    }

    pub fn finish<T>(&self, result: Result<T>, observation: &mut AppendObservation) -> Result<T> {
        let actual = if result.is_ok() {
            match self.actual.get() {
                Some(bytes) => Ok(bytes),
                None => self.recount(observation),
            }
        } else {
            // Even a failed write_all can have materialized a partial record.
            self.recount(observation)
        };
        let settlement = actual.and_then(|actual| {
            self.update(observation, |accounting| {
                accounting.settle(self.lane, actual)
            })
        });
        // A failed filesystem operation remains the primary error. The charge
        // survives a failed settlement and is reconciled before the next write.
        match result {
            Err(error) => Err(error),
            Ok(value) => settlement.map(|()| value),
        }
    }

    fn recount(&self, observation: &mut AppendObservation) -> Result<u64> {
        let started = observation.mark();
        observation.timing.recounts += 1;
        let result = match std::fs::symlink_metadata(&self.directory) {
            Ok(metadata) if metadata.is_dir() => directory_bytes(&self.directory),
            Ok(_) => Err(Error::Node("follower lane directory is invalid")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(Error::from(error)),
        };
        observation.timing.recount += AppendObservation::elapsed(started);
        result
    }

    fn update<T>(
        &self,
        observation: &mut AppendObservation,
        update: impl FnOnce(&mut DiskAccounting) -> Result<T>,
    ) -> Result<T> {
        let waiting = observation.mark();
        let accounting = self.shared.lock();
        observation.timing.accounting_wait += AppendObservation::elapsed(waiting);
        let mut accounting =
            accounting.map_err(|_| Error::Node("follower disk reservation lock poisoned"))?;
        let held = observation.mark();
        let result = update(&mut accounting);
        observation.timing.accounting_hold += AppendObservation::elapsed(held);
        result
    }
}
