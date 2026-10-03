//! Exact original boot retirement after canonical and external process closure.
use super::{FleetAdapterFuture, FleetJournal, FleetJournalSnapshot, FleetRoster, operation};
use cellule_runtime::{
    Error, Result,
    fleet::operations::{EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentStatus},
    identity::{Digest, SessionId},
    node::{NodeDirectory, NodeSessionClosure, NodeSessionFence},
};
use std::{future::Future, sync::Arc};
use tokio::time::{Instant, timeout_at};

mod writers;
pub use writers::{
    FleetOriginalCatalogSet, FleetOriginalCatalogSource, FleetOriginalCatalogs,
    FleetOriginalWriterCapture, FleetOriginalWriterJournal,
};
mod process;
mod publication;
mod retained;
pub use process::FleetFailedBootProcessConfirmation;
mod readers;
pub use readers::{
    FleetFailedReaderClosure, FleetFailedReaderPublication, FleetFailedReaderRetirement,
};
mod records;

/// Immutable original boot and canonical fence to verify at the process provider.
/// Neither an expired lease nor successful data recovery answers this request.
/// Snapshot, mutable boot status and collection interval are read metadata;
/// providers bind retained original lifetime evidence to [`Self::digest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetFailedBootProcessRequest {
    boot: EnrollmentRecord,
    canonical: Option<NodeSessionClosure>,
    fence: NodeSessionFence,
    digest: Digest,
    snapshot: FleetJournalSnapshot,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetFailedBootProcessRequest {
    /// Full barrier used for the original boot/fence capture.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original process-request collection interval, without restamping.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }

    /// Original request, acceptance time and establishment evidence. Mutable
    /// journal status is not part of the immutable request digest.
    #[must_use]
    pub const fn boot(&self) -> &EnrollmentRecord {
        &self.boot
    }
    /// Original permanent physical/session fence, independent of log recovery.
    #[must_use]
    pub const fn fence(&self) -> &NodeSessionFence {
        &self.fence
    }
    /// Original terminal-log observation for a legacy terminal capture. A fenced
    /// capture supplies no such assertion; boot retirement checks it separately.
    #[must_use]
    pub fn canonical(&self) -> Option<&NodeSessionClosure> {
        self.canonical.as_ref()
    }
    /// Stable process request identity across original retirement/reconstruction.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Application-authenticated durable evidence of this original process lifetime.
/// The witness identifies retained termination/nonexecution evidence, not a PID,
/// timeout, lease expiry, native absence, or recovery result. Construct only
/// after satisfying [`FleetFailedBootProcesses`]'s contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetFailedBootProcessEvidence {
    request: Digest,
    witness: Digest,
}
impl FleetFailedBootProcessEvidence {
    /// Binds independently verified process evidence to the complete request.
    /// This constructor checks shape; application authentication and actual
    /// process/accepted external work joining remain the provider's duties.
    pub fn new(request: &FleetFailedBootProcessRequest, witness: Digest) -> Result<Self> {
        if witness.as_bytes().iter().all(|byte| *byte == 0) {
            return Err(Error::Fenced);
        }
        Ok(Self {
            request: request.digest,
            witness,
        })
    }
    /// Exact immutable boot/fence request confirmed by the provider.
    #[must_use]
    pub const fn request_digest(&self) -> Digest {
        self.request
    }
    /// Stable identity of the application's retained original process evidence.
    #[must_use]
    pub const fn witness(&self) -> Digest {
        self.witness
    }
}

/// Read-only provider of original-boot process termination/nonexecution evidence.
///
/// Authenticate the exact physical node/session and original establishment,
/// join termination of that process and all its accepted external jobs/producers,
/// and exclude later execution/restart of the same session. Retain the immutable
/// evidence durably outside canonical Cell storage; rereads after adapter or
/// controller restart return the same witness. A successor process, reusable PID,
/// missing inventory, expiry, sealed/Retired data, timeout or lost reply cannot
/// supply this evidence. This method must not start a new kill/drain operation.
/// Own such effects in the application's existing supervised finite work first.
/// Cell relocation and reader/follower policy remain separate evidence.
pub trait FleetFailedBootProcesses: Send + Sync {
    /// Reconfirms the original durable evidence, preserving source failures.
    fn confirm_stopped<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence>;
}

/// Original boot capsule after all related enrollment rows and leader log close.
/// Capturing it starts no process/native effects. Applications account bounded
/// metadata and own publication in their accepted finite work. This value does
/// not establish affected-writer relocation or grant maintenance finalization.
pub struct FleetFailedBootRetirement {
    snapshot: FleetJournalSnapshot,
    request: FleetFailedBootProcessRequest,
    canonical: NodeSessionClosure,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetFailedBootRetirement {
    /// Checks the exact original Established boot (or its replay), complete
    /// bootstrapped roster and fresh canonical terminal session. Any unresolved
    /// related reader/follower/Pending request prevents capture, even after a
    /// new physical intent or successful recovery. Foreign terminal references
    /// remain retained under the native grace/collection contracts.
    pub async fn capture(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        original: &EnrollmentRecord,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let request = FleetFailedBootProcessRequest::capture(
            journal, directory, roster, original, claimant, deadline, &mut clock,
        )
        .await?;
        Self::capture_retained(
            journal, directory, roster, &request, claimant, deadline, clock,
        )
        .await
    }
    /// Full original barrier checked before any publication.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Exact process request to confirm through the application provider.
    #[must_use]
    pub const fn request(&self) -> &FleetFailedBootProcessRequest {
        &self.request
    }
    /// Original capture interval, which publication cannot restamp.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

/// Durable boot publication plus its independently retained final check.
/// A successful write remains inspectable if a later barrier/provider read fails.
pub struct FleetFailedBootPublication {
    process: FleetFailedBootProcessEvidence,
    record: std::result::Result<EnrollmentRecord, Arc<Error>>,
    closure: std::result::Result<FleetFailedBootClosure, Arc<Error>>,
}
impl FleetFailedBootPublication {
    /// Exact immutable process evidence used for this original publication.
    #[must_use]
    pub const fn process(&self) -> &FleetFailedBootProcessEvidence {
        &self.process
    }
    /// Original returned retirement row or source error, including a lost reply.
    pub fn record(&self) -> std::result::Result<&EnrollmentRecord, Arc<Error>> {
        self.record.as_ref().map_err(Arc::clone)
    }
    /// Complete post-publication roster, canonical and process confirmation.
    pub fn confirmed(&self) -> Result<&FleetFailedBootClosure> {
        self.closure
            .as_ref()
            .map_err(|source| retained(Arc::clone(source)))
    }
    /// Original final-check failure without discarding the journal response.
    #[must_use]
    pub fn closure_error(&self) -> Option<Arc<Error>> {
        self.closure.as_ref().err().cloned()
    }
}

/// Checked original boot retirement. This is interval evidence, not a completed
/// operation: replacements, affected writers and finalization CAS remain required.
pub struct FleetFailedBootClosure {
    snapshot: FleetJournalSnapshot,
    boot: EnrollmentRecord,
    canonical: NodeSessionClosure,
    process: FleetFailedBootProcessEvidence,
    digest: Digest,
    started_at_ms: i64,
    finished_at_ms: i64,
}
impl FleetFailedBootClosure {
    /// Complete post-publication head and registry, rechecked around all evidence.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original retired request, including first acceptance and establishment.
    #[must_use]
    pub const fn boot(&self) -> &EnrollmentRecord {
        &self.boot
    }
    /// Permanent canonical physical/session fence and retained terminal log.
    #[must_use]
    pub const fn canonical(&self) -> &NodeSessionClosure {
        &self.canonical
    }
    /// Application's original durable process lifetime evidence.
    #[must_use]
    pub const fn process(&self) -> &FleetFailedBootProcessEvidence {
        &self.process
    }
    /// Identifies original retirement/evidence without refreshing timestamps.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Original capture through fresh final confirmation.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

fn interval(start: i64, end: i64) -> Result<()> {
    if start < 0 || end < start || end - start > 30_000 {
        return Err(Error::Deadline);
    }
    Ok(())
}
async fn bounded<T>(deadline: Instant, future: impl Future<Output = Result<T>>) -> Result<T> {
    if Instant::now() >= deadline {
        return Err(Error::Deadline);
    }
    timeout_at(deadline, future)
        .await
        .map_err(|source| Error::Facility {
            name: "fleet-failed-boot-deadline",
            source: Box::new(source),
        })?
}
fn adapter_error(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-failed-boot-adapter",
        source,
    }
}
#[derive(Debug)]
struct RetainedError(Arc<Error>);
impl std::fmt::Display for RetainedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(source: Arc<Error>) -> Error {
    Error::Facility {
        name: "fleet-failed-boot-publication",
        source: Box::new(RetainedError(source)),
    }
}
