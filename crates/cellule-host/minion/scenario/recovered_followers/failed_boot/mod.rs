//! Provider-bound process evidence; this child is a lifetime stand-in, not a
//! multi-process Cell/provider qualification or an acknowledged-tail workload.
use super::*;
use cellule_host::fleet::{
    FleetAdapterFuture, FleetFailedBootProcessEvidence, FleetFailedBootProcessRequest,
    FleetFailedBootProcesses, FleetFailedBootRetirement,
};
use std::{
    process::{Child, Command},
    sync::Mutex,
};

mod process_tests;
mod tests;
mod writer_tests;

pub(super) struct Process {
    child: Child,
    evidence_path: PathBuf,
}
impl Process {
    pub(super) fn start(path: PathBuf) -> Self {
        Self {
            child: Command::new("sleep").arg("60").spawn().unwrap(),
            evidence_path: path,
        }
    }
    pub(super) fn stop_and_retain(&mut self, request: &FleetFailedBootProcessRequest) {
        assert!(self.child.try_wait().unwrap().is_none());
        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        let mut hash = blake3::Hasher::new();
        hash.update(request.digest().as_bytes());
        hash.update(&self.child.id().to_be_bytes());
        hash.update(status.to_string().as_bytes());
        let evidence = FleetFailedBootProcessEvidence::new(
            request,
            Digest::from_bytes(*hash.finalize().as_bytes()),
        )
        .unwrap();
        let mut bytes = evidence.request_digest().as_bytes().to_vec();
        bytes.extend_from_slice(evidence.witness().as_bytes());
        use std::io::Write;
        let mut file = std::fs::File::create(&self.evidence_path).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(super) struct Processes {
    path: PathBuf,
    reads: AtomicUsize,
    final_fault: Mutex<Option<std::io::ErrorKind>>,
    final_witness: Mutex<Option<Digest>>,
}
impl Processes {
    pub(super) fn new(path: PathBuf) -> Self {
        Self {
            path,
            reads: AtomicUsize::new(0),
            final_fault: Mutex::new(None),
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
            let read = self.reads.fetch_add(1, Ordering::AcqRel);
            if read == 1 {
                if let Some(kind) = self.final_fault.lock().unwrap().take() {
                    return Err(
                        std::io::Error::new(kind, "original process evidence read lost").into(),
                    );
                }
                if let Some(witness) = self.final_witness.lock().unwrap().take() {
                    return Ok(FleetFailedBootProcessEvidence::new(request, witness)?);
                }
            }
            let bytes = std::fs::read(&self.path)?;
            if bytes.len() != 64 || bytes[..32] != *request.digest().as_bytes() {
                return Err(
                    std::io::Error::other("original process evidence request differs").into(),
                );
            }
            let witness = Digest::from_bytes(bytes[32..].try_into()?);
            Ok(FleetFailedBootProcessEvidence::new(request, witness)?)
        })
    }
}

struct ForeignEvidence(FleetFailedBootProcessEvidence);
impl FleetFailedBootProcesses for ForeignEvidence {
    fn confirm_stopped<'a>(
        &'a self,
        _: &'a FleetFailedBootProcessRequest,
    ) -> FleetAdapterFuture<'a, FleetFailedBootProcessEvidence> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

impl Fixture {
    async fn failed_boot(&self) -> EnrollmentRecord {
        let spec =
            startup::spec(&NodeIntent::initial(scope(), node_id(0), session(0)).unwrap()).unwrap();
        self.journal
            .load_enrollment(scope(), spec.key().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
    async fn settle_followers(&self) {
        self.retire().await;
        self.capture()
            .await
            .publish(
                self.journal.as_ref(),
                &self.directory,
                session(1),
                deadline(),
                || Ok(CHECK),
            )
            .await
            .unwrap()
            .confirmed()
            .unwrap();
    }
    async fn failed_capture(&self, original: &EnrollmentRecord) -> FleetFailedBootRetirement {
        FleetFailedBootRetirement::capture(
            self.journal.as_ref(),
            &self.directory,
            &self.roster().await,
            original,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap()
    }
    fn process_path(&self) -> PathBuf {
        self._root.path().join("process-closure")
    }
}
