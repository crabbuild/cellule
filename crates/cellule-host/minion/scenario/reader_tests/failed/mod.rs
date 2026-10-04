//! Application-owned enrollment around real native readers and joined lifetimes.
//! This proves in-process closure; OS crash/provider qualification is separate.
use super::super::startup;
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetEnrollmentAcceptance, FleetFailedBootProcessEvidence,
    FleetFailedBootProcessRequest, FleetFailedBootProcesses, FleetFailedBootRetirement,
    FleetFailedReaderRetirement, FleetRoster,
};
use cellule_runtime::{
    client::CellReadReplica,
    fleet::operations::{
        EnrollmentEndpoint, EnrollmentEvent, EnrollmentRole, EnrollmentSpec, PublishedPosition,
    },
    node::{NodeAdvertisement, NodeCapacity, NodeFailureDomain},
};
use ed25519_dalek::SigningKey;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

mod barriers;
mod faults;
mod fixture;
mod tests;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

struct Fixture {
    root: tempfile::TempDir,
    journal: Arc<SqliteJournal>,
    node: Arc<CellNode>,
    directory: NodeDirectory,
    source: CellRuntime,
    handle: CellHandle,
    manager: ReadReplicaManager,
    boot: EnrollmentRecord,
    readers: Vec<EnrollmentRecord>,
    views: Vec<CellReadReplica>,
}

struct Processes {
    path: PathBuf,
    reads: AtomicUsize,
    final_fault: Mutex<bool>,
    final_witness: Mutex<Option<Digest>>,
}
impl Processes {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            reads: AtomicUsize::new(0),
            final_fault: Mutex::new(false),
            final_witness: Mutex::new(None),
        }
    }
}
impl FleetFailedBootProcesses for Processes {
    fn confirm_stopped<'a>(
        &'a self,
        request: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence> {
        Box::pin(async move {
            if self.reads.fetch_add(1, Ordering::AcqRel) == 1 {
                if *self.final_fault.lock().unwrap() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "original native lifetime evidence read lost",
                    )
                    .into());
                }
                if let Some(witness) = *self.final_witness.lock().unwrap() {
                    return Ok(FleetFailedBootProcessEvidence::new(request, witness)?);
                }
            }
            let bytes = std::fs::read(&self.path)?;
            if bytes.len() != 64 || bytes[..32] != *request.digest().as_bytes() {
                return Err(
                    std::io::Error::other("original native lifetime request differs").into(),
                );
            }
            Ok(FleetFailedBootProcessEvidence::new(
                request,
                Digest::from_bytes(bytes[32..].try_into()?),
            )?)
        })
    }
}
