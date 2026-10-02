//! Bounded read-only progress while the canonical activation owner awaits I/O.
use super::*;
use cellule_runtime::node::NodeMode;

const PAGE_BYTES: usize = 1 << 20;
const PAGE_ENTRIES: usize = 128;

#[cfg(test)]
mod tests;

/// Continuation bound to this manager and the original producer progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderEnrollmentInventoryCursor {
    topology: Digest,
    after: CellId,
}
impl ReaderEnrollmentInventoryCursor {
    /// Encodes a fixed-width application continuation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 64] {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(self.topology.as_bytes());
        bytes[32..].copy_from_slice(self.after.as_bytes());
        bytes
    }
    /// Decodes the fixed width; each page rechecks the original progress.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 64] = bytes
            .try_into()
            .map_err(|_| Error::Node("invalid reader enrollment cursor width"))?;
        let mut topology = [0; 32];
        topology.copy_from_slice(&bytes[..32]);
        let mut after = [0; 32];
        after.copy_from_slice(&bytes[32..]);
        if topology == [0; 32] || after == [0; 32] {
            return Err(Error::Node("zero reader enrollment cursor identity"));
        }
        Ok(Self {
            topology: Digest::from_bytes(topology),
            after: CellId::from_bytes(after),
        })
    }
}

/// Accepted finite jobs, including preparation before a Pending request exists.
/// These counts never infer native closure from a task handle's disappearance.
#[derive(Clone, Debug)]
pub struct ReaderEnrollmentJobs {
    retained: usize,
    running: usize,
    unobserved: usize,
    joining: usize,
    draining: bool,
    task_failure: Option<Arc<Error>>,
    protocol_failure: Option<Arc<Error>>,
}
impl ReaderEnrollmentJobs {
    /// Counts every retained join handle, including completed protocol jobs.
    #[must_use]
    pub const fn retained(&self) -> usize {
        self.retained
    }
    /// Counts jobs with no response that are still executing or preparing.
    #[must_use]
    pub const fn running(&self) -> usize {
        self.running
    }
    /// Counts finished jobs without their canonical protocol response.
    #[must_use]
    pub const fn unobserved(&self) -> usize {
        self.unobserved
    }
    /// Counts handles currently held by the retained join owner; state is unknown.
    #[must_use]
    pub const fn joining(&self) -> usize {
        self.joining
    }
    /// Reports permanent closure of this producer's job admission.
    #[must_use]
    pub const fn draining(&self) -> bool {
        self.draining
    }
    /// Returns the original retained task failure, without replacing its source.
    #[must_use]
    pub fn task_failure(&self) -> Option<&Arc<Error>> {
        self.task_failure.as_ref()
    }
    /// Returns the first original response failure observed in this capture.
    #[must_use]
    pub fn protocol_failure(&self) -> Option<&Arc<Error>> {
        self.protocol_failure.as_ref()
    }
}

/// Bounded original responsibilities, including unresolved acceptance/open/close.
/// The page retains one MiB from the shared node byte ledger. It is advisory:
/// current authority, replacement policy and durable settlement remain required.
pub struct ReaderEnrollmentInventoryPage {
    session: SessionId,
    mode: NodeMode,
    observed_at_ms: i64,
    topology: Digest,
    total_enrollments: usize,
    jobs: ReaderEnrollmentJobs,
    entries: Vec<ReaderEnrollmentCompletion>,
    next: Option<ReaderEnrollmentInventoryCursor>,
    _memory: NodeByteReservation,
}
impl ReaderEnrollmentInventoryPage {
    /// Returns the exact manager boot session.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns current shared admission mode, independent of local role counts.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Returns the supplied original capture time; no proof is refreshed.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
    /// Returns the manager and captured producer-state fingerprint.
    #[must_use]
    pub const fn topology(&self) -> Digest {
        self.topology
    }
    /// Counts every retained responsibility, including those outside this page.
    #[must_use]
    pub const fn total_enrollments(&self) -> usize {
        self.total_enrollments
    }
    /// Returns accepted job state, including pre-journal preparation and failure.
    #[must_use]
    pub const fn jobs(&self) -> &ReaderEnrollmentJobs {
        &self.jobs
    }
    /// Returns original requests, native states and source errors in Cell order.
    #[must_use]
    pub fn entries(&self) -> &[ReaderEnrollmentCompletion] {
        &self.entries
    }
    /// Continues only while producer topology/progress still matches.
    #[must_use]
    pub const fn next(&self) -> Option<ReaderEnrollmentInventoryCursor> {
        self.next
    }
}

impl ReadReplicaManager {
    /// Captures durable reader producer progress without awaiting its peer/journal
    /// calls or acquiring the manager's activation lane. Unbound managers return
    /// None, which supplies no enrollment coverage. Installed native views must
    /// also be scanned through `fleet_readers_page`. A page cannot authorize
    /// shutdown, retry unknown opening or erase an accepted job.
    pub fn fleet_reader_enrollments_page(
        &self,
        cursor: Option<ReaderEnrollmentInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> Result<Option<ReaderEnrollmentInventoryPage>> {
        if !(1..=PAGE_ENTRIES).contains(&limit) || now_ms < 0 {
            return Err(Error::Node("invalid reader enrollment inventory bounds"));
        }
        self.bound_enrollment()?
            .map(|binding| binding.page(self, cursor, limit, now_ms))
            .transpose()
    }
}

impl ReaderEnrollment {
    fn jobs_observation(&self) -> Result<ReaderEnrollmentJobs> {
        let (jobs, draining, failure) = {
            let bank = self
                .jobs
                .lock()
                .map_err(|_| Error::Control("reader job bank poisoned"))?;
            (bank.jobs.clone(), bank.draining, bank.failure.clone())
        };
        Ok(observe_jobs(jobs, draining, failure))
    }

    fn page(
        &self,
        manager: &ReadReplicaManager,
        cursor: Option<ReaderEnrollmentInventoryCursor>,
        limit: usize,
        now_ms: i64,
    ) -> Result<ReaderEnrollmentInventoryPage> {
        let memory = manager.runtime.try_reserve_node_bytes(PAGE_BYTES)?;
        let mut records = {
            let records = self.records()?;
            if records.len() > MAX_READ_VIEWS {
                return Err(Error::Capacity("reader enrollment inventory bound"));
            }
            records
                .iter()
                .map(|(cell, record)| (*cell, record.clone()))
                .collect::<Vec<_>>()
        };
        records.sort_unstable_by_key(|(cell, _)| *cell.as_bytes());
        let jobs = self.jobs_observation()?;
        let mode = manager.runtime.node_admission().mode()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.reader-enrollment-inventory.v1\0");
        hash.update(self.scope.fleet.as_bytes());
        hash.update(self.scope.application.as_bytes());
        hash.update(self.node.as_bytes());
        hash.update(manager.session.as_bytes());
        hash.update(&[
            mode as u8,
            u8::from(jobs.draining),
            u8::from(jobs.task_failure.is_some()),
            u8::from(jobs.protocol_failure.is_some()),
        ]);
        for count in [
            records.len(),
            jobs.retained,
            jobs.running,
            jobs.unobserved,
            jobs.joining,
        ] {
            hash.update(&(count as u64).to_be_bytes());
        }
        let mut entries = Vec::with_capacity(limit);
        // Account index and row storage before cloning dynamic request/source
        // bytes. Shared Arc errors and native ownership are not copied.
        let mut bytes = records
            .len()
            .checked_mul(std::mem::size_of::<(CellId, Record)>())
            .and_then(|v| {
                v.checked_add(
                    limit * std::mem::size_of::<ReaderEnrollmentCompletion>()
                        + 4096
                        + 2 * MAX_RECORD_BYTES as usize,
                )
            })
            .ok_or(Error::Capacity("reader inventory byte overflow"))?;
        let mut after_found = cursor.is_none();
        let mut page_full = false;
        for (cell, record) in &records {
            let progress = data(record)?;
            let spec = progress.spec.to_bytes().map_err(operation)?;
            let original = progress
                .original
                .as_ref()
                .map(EnrollmentRecord::to_bytes)
                .transpose()
                .map_err(operation)?;
            hash.update(cell.as_bytes());
            field(&mut hash, &spec);
            hash.update(&[u8::from(original.is_some())]);
            if let Some(original) = &original {
                field(&mut hash, original);
            }
            hash.update(&[
                u8::from(progress.published),
                u8::from(progress.opening_started),
                u8::from(progress.opening_joined),
                u8::from(progress.execution_error.is_some()),
                u8::from(progress.journal_error.is_some()),
            ]);
            let (tag, digest) = match progress.event {
                None => (0, None),
                Some(EnrollmentEvent::Established(d)) => (1, Some(d)),
                Some(EnrollmentEvent::Refused(d)) => (2, Some(d)),
                Some(EnrollmentEvent::Retired(d)) => (3, Some(d)),
            };
            hash.update(&[tag]);
            if let Some(digest) = digest {
                hash.update(digest.as_bytes());
            }
            hash.update(progress.source.description().code.as_bytes());
            hash.update(&progress.source.description().schema.to_be_bytes());
            field(&mut hash, progress.source.owner().endpoint.as_bytes());
            if cursor.is_some_and(|cursor| cursor.after == *cell) {
                after_found = true;
            }
            if page_full
                || entries.len() >= limit
                || cursor.is_some_and(|cursor| cell.as_bytes() <= cursor.after.as_bytes())
            {
                continue;
            }
            let dynamic = spec
                .len()
                .checked_mul(2)
                .and_then(|v| v.checked_add(original.as_ref().map_or(0, Vec::len) * 2))
                .and_then(|v| {
                    v.checked_add(
                        progress.source.target().partition().len()
                            + progress.source.owner().endpoint.len()
                            + 512,
                    )
                })
                .ok_or(Error::Capacity("reader inventory byte overflow"))?;
            if !admit_entry(&mut bytes, dynamic, entries.is_empty())? {
                // Stop copying before the first row that would exceed admission,
                // but hash every remaining original row for stable continuation.
                page_full = true;
                continue;
            }
            entries.push(progress.completion());
        }
        let topology = Digest::from_bytes(*hash.finalize().as_bytes());
        if !after_found || cursor.is_some_and(|cursor| cursor.topology != topology) {
            return Err(Error::Node(
                "reader enrollment inventory changed; restart scan",
            ));
        }
        let next = entries.last().and_then(|last| {
            let cell = last.source.description().cell;
            records
                .last()
                .is_some_and(|(last, _)| last.as_bytes() > cell.as_bytes())
                .then_some(ReaderEnrollmentInventoryCursor {
                    topology,
                    after: cell,
                })
        });
        Ok(ReaderEnrollmentInventoryPage {
            session: manager.session,
            mode,
            observed_at_ms: now_ms,
            topology,
            total_enrollments: records.len(),
            jobs,
            entries,
            next,
            _memory: memory,
        })
    }
}
fn field(hash: &mut blake3::Hasher, bytes: &[u8]) {
    hash.update(&(bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn admit_entry(bytes: &mut usize, dynamic: usize, empty: bool) -> Result<bool> {
    let next = bytes
        .checked_add(dynamic)
        .ok_or(Error::Capacity("reader inventory byte overflow"))?;
    if next > PAGE_BYTES {
        if empty {
            return Err(Error::Capacity("reader enrollment inventory page bytes"));
        }
        return Ok(false);
    }
    *bytes = next;
    Ok(true)
}

fn observe_jobs(
    jobs: Vec<Arc<Mutex<ReaderJob>>>,
    draining: bool,
    failure: Option<Arc<Error>>,
) -> ReaderEnrollmentJobs {
    let mut observation = ReaderEnrollmentJobs {
        retained: jobs.len(),
        running: 0,
        unobserved: 0,
        joining: 0,
        draining,
        task_failure: failure,
        protocol_failure: None,
    };
    for job in jobs {
        let Ok(job) = job.try_lock() else {
            observation.joining += 1;
            continue;
        };
        let response = job.response.borrow();
        match response.as_ref() {
            Some(Err(error)) => {
                observation
                    .protocol_failure
                    .get_or_insert_with(|| error.clone());
            }
            Some(Ok(_)) => {}
            None if job.task.as_ref().is_some_and(|task| !task.is_finished()) => {
                observation.running += 1
            }
            None => observation.unobserved += 1,
        }
        if let Some(error) = &job.failure {
            observation
                .task_failure
                .get_or_insert_with(|| error.clone());
        }
    }
    observation
}
