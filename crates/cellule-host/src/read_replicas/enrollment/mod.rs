//! Durable ownership around the manager's canonical activation/closure lane.

use super::*;
use crate::fleet::{FleetEnrollmentAcceptance, FleetJournal};
use cellule_runtime::cell::actor::NodeByteReservation;
use cellule_runtime::fleet::operations::{
    EnrollmentEndpoint, EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentSpec,
    EnrollmentStatus, FleetScope, MAX_RECORD_BYTES, PublishedPosition,
};
use cellule_runtime::identity::NodeId;
use std::sync::Mutex as StdMutex;
use tokio::task::JoinHandle;

mod jobs;

const MAX_JOBS: usize = 32;

pub(super) enum ActivationRequest {
    Hint(CellTarget, SessionId),
    Source(Box<ReadReplicaSource>),
}

pub(crate) struct ReaderEnrollment {
    scope: FleetScope,
    node: NodeId,
    journal: Arc<dyn FleetJournal>,
    records: Mutex<HashMap<CellId, Responsibility>>,
    jobs: StdMutex<JobBank>,
}

#[derive(Default)]
struct JobBank {
    draining: bool,
    jobs: Vec<Arc<Mutex<ReaderJob>>>,
    failure: Option<Arc<Error>>,
}

struct ReaderJob {
    task: Option<JoinHandle<()>>,
    failure: Option<Arc<Error>>,
}
/// Retained finite-protocol result for one locally owned reader obligation.
/// Errors preserve their original sources; absence is not retirement evidence.
#[derive(Clone)]
pub struct ReaderEnrollmentCompletion {
    /// Exact immutable request, including both physical intents and pinned root.
    pub spec: EnrollmentSpec,
    /// Original catalog/authority/signed-boot source used by canonical opening.
    pub source: ReadReplicaSource,
    /// Original acceptance; None means the acceptance reply remains ambiguous.
    pub accepted: Option<EnrollmentRecord>,
    /// Checked opening or joined-closure evidence awaiting/confirming publication.
    pub event: Option<EnrollmentEvent>,
    /// Whether the journal confirmed this exact event.
    pub published: bool,
    /// Original native opening failure, independent of publication failure.
    pub execution_error: Option<Arc<Error>>,
    /// Original unresolved result-publication failure.
    pub journal_error: Option<Arc<Error>>,
}

struct Responsibility {
    spec: EnrollmentSpec,
    source: ReadReplicaSource,
    original: Option<EnrollmentRecord>,
    // A publication failure retains the same evidence, never a new opening.
    event: Option<EnrollmentEvent>,
    published: bool,
    opening_started: bool,
    opening_joined: bool,
    execution_error: Option<Arc<Error>>,
    journal_error: Option<Arc<Error>>,
    _retained: NodeByteReservation,
}

#[derive(Debug)]
struct RetainedFailure(Arc<Error>);
impl std::fmt::Display for RetainedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: Arc<Error>) -> Error {
    Error::Facility {
        name: "fleet-reader-enrollment",
        source: Box::new(RetainedFailure(error)),
    }
}
fn journal(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-enrollment-journal",
        source,
    }
}
fn operation(error: cellule_runtime::fleet::operations::OperationError) -> Error {
    Error::FleetOperation(Box::new(error))
}

impl ReaderEnrollment {
    pub(crate) fn new(
        scope: FleetScope,
        node: NodeId,
        journal: Arc<dyn FleetJournal>,
    ) -> Result<Self> {
        cellule_runtime::fleet::operations::FleetHead::new(scope, 0).map_err(operation)?;
        if node.as_bytes().iter().all(|byte| *byte == 0) {
            return Err(Error::Node("invalid reader enrollment physical node"));
        }
        Ok(Self {
            scope,
            node,
            journal,
            records: Mutex::new(HashMap::new()),
            jobs: StdMutex::new(JobBank::default()),
        })
    }

    pub(super) fn matches_boot(
        &self,
        advertisement: &cellule_runtime::node::NodeAdvertisement,
    ) -> bool {
        advertisement.node() == self.node && advertisement.fleet() == self.scope.fleet
    }

    async fn spec(
        &self,
        manager: &ReadReplicaManager,
        source: &ReadReplicaSource,
    ) -> Result<EnrollmentSpec> {
        if source.fleet() != self.scope.fleet
            || source.target().application() != self.scope.application
        {
            return Err(Error::Fenced);
        }
        let receiver = manager
            .directory
            .load_if_live(manager.session, now_ms()?)
            .await?
            .ok_or(Error::Fenced)?;
        if !self.matches_boot(receiver.advertisement()) {
            return Err(Error::Fenced);
        }
        let version = self
            .journal
            .load_snapshot(self.scope)
            .await
            .map_err(journal)?
            .registry();
        let mut after = None;
        let mut endpoints = HashMap::new();
        let mut count = 0;
        loop {
            let page = self
                .journal
                .intents_page(version, after, 128)
                .await
                .map_err(journal)?;
            if page.version() != version || page.after() != after {
                return Err(Error::Fenced);
            }
            count += page.entries().len();
            if count > MAX_LIVE_NODES {
                return Err(Error::Capacity("reader intent inventory bound"));
            }
            for intent in page.entries() {
                if intent.node() == source.node() || intent.node() == self.node {
                    endpoints.insert(
                        intent.node(),
                        EnrollmentEndpoint {
                            node: intent.node(),
                            session: intent.session(),
                            intent_revision: intent.revision(),
                        },
                    );
                }
            }
            if endpoints.len() == 2 || page.next().is_none() {
                break;
            }
            if page.next().map(|id| *id.as_bytes()) <= after.map(|id| *id.as_bytes()) {
                return Err(Error::Fenced);
            }
            after = page.next();
        }
        let origin = endpoints
            .get(&source.node())
            .copied()
            .ok_or(Error::Fenced)?;
        let target = endpoints.get(&self.node).copied().ok_or(Error::Fenced)?;
        if origin.session != source.owner().session || target.session != manager.session {
            return Err(Error::Fenced);
        }
        Ok(EnrollmentSpec {
            scope: self.scope,
            request: Digest::from_bytes(*blake3::hash(Uuid::now_v7().as_bytes()).as_bytes()),
            source: Some(origin),
            target,
            role: EnrollmentRole::Reader {
                target: source.target().clone(),
                position: PublishedPosition {
                    incarnation: source.description().incarnation,
                    epoch: source.epoch(),
                    root: source.root().clone(),
                },
            },
        })
    }

    pub(super) async fn open(
        &self,
        manager: &ReadReplicaManager,
        source: ReadReplicaSource,
        path: PathBuf,
    ) -> Result<Receipt> {
        let cell = source.description().cell;
        let mut records = self.records.lock().await;
        if let Some(record) = records.get_mut(&cell) {
            self.publish(record).await?;
            return Err(Error::Control(
                "reader enrollment remains unresolved; close before a new opening",
            ));
        }
        if records.len() >= MAX_READ_VIEWS {
            return Err(Error::Capacity("reader enrollment inventory bound"));
        }
        let spec = self.spec(manager, &source).await?;
        let reservation = manager
            .runtime
            .try_reserve_node_bytes(3 * MAX_RECORD_BYTES as usize)?;
        records.insert(
            cell,
            Responsibility {
                spec,
                source: source.clone(),
                original: None,
                event: None,
                published: false,
                opening_started: false,
                opening_joined: false,
                execution_error: None,
                journal_error: None,
                _retained: reservation,
            },
        );
        let record = records
            .get_mut(&cell)
            .ok_or(Error::Control("reader enrollment owner is absent"))?;
        self.accept(record).await?;
        record.opening_started = true;
        let result = manager.open_source_locked(source, path).await;
        record.opening_joined = true;
        let receipt = result.as_ref().ok().copied();
        record.event = Some(match receipt {
            Some(receipt) => {
                EnrollmentEvent::Established(evidence(record, b"opened", Some(receipt))?)
            }
            None => EnrollmentEvent::Retired(evidence(record, b"joined-opening-refusal", None)?),
        });
        record.published = false;
        let publication = self.publish(record).await;
        if result.is_err() && publication.is_ok() {
            records.remove(&cell);
        }
        // Preserve the native failure when publication also failed. The retained
        // event remains available for closure/republication on the next drain.
        match result {
            Err(error) => {
                let error = Arc::new(error);
                if let Some(record) = records.get_mut(&cell) {
                    record.execution_error = Some(Arc::clone(&error));
                }
                Err(retained(error))
            }
            Ok(receipt) => {
                publication?;
                Ok(receipt)
            }
        }
    }

    async fn accept(&self, record: &mut Responsibility) -> Result<()> {
        if record.original.is_some() {
            return Ok(());
        }
        let acceptance = self
            .journal
            .accept_enrollment(&record.spec, now_ms()?)
            .await;
        let acceptance = match acceptance {
            Ok(acceptance) => acceptance,
            Err(source) => {
                let error = Arc::new(journal(source));
                if record.journal_error.is_none() {
                    record.journal_error = Some(Arc::clone(&error));
                }
                return Err(retained(error));
            }
        };
        match acceptance {
            FleetEnrollmentAcceptance::New(original) => {
                if original.spec() != &record.spec || original.status() != EnrollmentStatus::Pending
                {
                    return Err(Error::Fenced);
                }
                original.to_bytes().map_err(operation)?;
                record.original = Some(original);
                Ok(())
            }
            FleetEnrollmentAcceptance::Existing(original) => {
                if original.spec() != &record.spec {
                    return Err(Error::Fenced);
                }
                record.original = Some(original);
                // No successful first-acceptance reply: this owner never opened
                // a reader. Leave Pending inventoried until joined removal.
                Err(Error::Control(
                    "reader enrollment acceptance reply is unresolved",
                ))
            }
        }
    }

    async fn publish(&self, record: &mut Responsibility) -> Result<()> {
        if record.published {
            return Ok(());
        }
        let Some(event) = record.event else {
            return Ok(());
        };
        let original = record
            .original
            .as_ref()
            .ok_or(Error::Control("reader enrollment acceptance is unknown"))?;
        let result = self
            .journal
            .publish_enrollment_result(original, event, now_ms()?)
            .await;
        let result = match result {
            Ok(result) => result,
            Err(source) => {
                let error = Arc::new(journal(source));
                if record.journal_error.is_none() {
                    record.journal_error = Some(Arc::clone(&error));
                }
                return Err(retained(error));
            }
        };
        result.to_bytes().map_err(operation)?;
        let agrees = match event {
            EnrollmentEvent::Established(evidence) => {
                result.status() == EnrollmentStatus::Established
                    && result.established_evidence() == Some(evidence)
            }
            EnrollmentEvent::Retired(evidence) => {
                result.status() == EnrollmentStatus::Retired
                    && result.settlement_evidence() == Some(evidence)
            }
            EnrollmentEvent::Refused(evidence) => {
                result.status() == EnrollmentStatus::Refused
                    && result.settlement_evidence() == Some(evidence)
            }
        };
        if !agrees
            || result.spec() != &record.spec
            || result.accepted_at_ms() != original.accepted_at_ms()
        {
            return Err(Error::Fenced);
        }
        record.published = true;
        Ok(())
    }

    pub(super) async fn established(&self, cell: CellId) -> Result<()> {
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(&cell)
            .ok_or(Error::Control("reader enrollment owner is absent"))?;
        if !matches!(record.event, Some(EnrollmentEvent::Established(_))) {
            return Err(Error::Control("reader enrollment remains unresolved"));
        }
        self.publish(record).await
    }

    pub(super) async fn completion(&self, cell: CellId) -> Option<ReaderEnrollmentCompletion> {
        self.records
            .lock()
            .await
            .get(&cell)
            .map(|record| ReaderEnrollmentCompletion {
                spec: record.spec.clone(),
                source: record.source.clone(),
                accepted: record.original.clone(),
                event: record.event,
                published: record.published,
                execution_error: record.execution_error.clone(),
                journal_error: record.journal_error.clone(),
            })
    }

    pub(super) async fn retire(&self, cell: CellId, receipt: Option<Receipt>) -> Result<()> {
        let mut records = self.records.lock().await;
        if let Some(record) = records.get_mut(&cell) {
            if receipt.is_none() && record.opening_started && !record.opening_joined {
                return Err(Error::Control(
                    "reader native opening remains unproven after task failure",
                ));
            }
            if !record.opening_started {
                if receipt.is_some() {
                    return Err(Error::Fenced);
                }
                // The retained opening owner has joined and never dispatched
                // native work. Atomically fence acceptance, including a delayed
                // transaction whose committed reply was never observed.
                let digest = match record.event {
                    Some(EnrollmentEvent::Refused(digest)) => digest,
                    None => {
                        let mut hash = blake3::Hasher::new();
                        hash.update(b"cellule.fleet-reader-unexecuted.v1\0");
                        hash.update(&record.spec.to_bytes().map_err(operation)?);
                        Digest::from_bytes(*hash.finalize().as_bytes())
                    }
                    _ => return Err(Error::Fenced),
                };
                record.event = Some(EnrollmentEvent::Refused(digest));
                let original = match self
                    .journal
                    .refuse_unexecuted_enrollment(&record.spec, digest, now_ms()?)
                    .await
                {
                    Ok(original) => original,
                    Err(source) => {
                        let error = Arc::new(journal(source));
                        if record.journal_error.is_none() {
                            record.journal_error = Some(error.clone());
                        }
                        return Err(retained(error));
                    }
                };
                original.validate_replay(&record.spec).map_err(operation)?;
                original.to_bytes().map_err(operation)?;
                if original.status() != EnrollmentStatus::Refused
                    || original.settlement_evidence() != Some(digest)
                    || record.original.as_ref().is_some_and(|accepted| {
                        accepted.accepted_at_ms() != original.accepted_at_ms()
                    })
                {
                    return Err(Error::Fenced);
                }
                records.remove(&cell);
                return Ok(());
            }
            if !matches!(record.event, Some(EnrollmentEvent::Retired(_))) {
                record.event = Some(EnrollmentEvent::Retired(evidence(
                    record,
                    b"joined-closure",
                    receipt,
                )?));
                record.published = false;
            }
            self.publish(record).await?;
            records.remove(&cell);
        }
        Ok(())
    }

    pub(super) async fn unresolved_cells(&self) -> Vec<CellId> {
        self.records.lock().await.keys().copied().collect()
    }
}

fn evidence(record: &Responsibility, phase: &[u8], receipt: Option<Receipt>) -> Result<Digest> {
    let original = record
        .original
        .as_ref()
        .ok_or(Error::Control("reader acceptance is unknown"))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-reader-evidence.v1\0");
    hash.update(&original.to_bytes().map_err(operation)?);
    hash.update(phase);
    let description = record.source.description();
    hash.update(description.code.as_bytes());
    hash.update(&description.schema.to_be_bytes());
    hash.update(&(record.source.owner().endpoint.len() as u64).to_be_bytes());
    hash.update(record.source.owner().endpoint.as_bytes());
    if let Some(receipt) = receipt {
        let EnrollmentRole::Reader { target, position } = &record.spec.role else {
            return Err(Error::Fenced);
        };
        if receipt.cell != target.cell_id()
            || receipt.incarnation != position.incarnation
            || receipt.commit_sequence < position.root.commit_sequence
        {
            return Err(Error::Fenced);
        }
        if phase == b"opened" && receipt.commit_sequence != position.root.commit_sequence {
            return Err(Error::Fenced);
        }
        hash.update(receipt.cell.as_bytes());
        hash.update(receipt.incarnation.as_bytes());
        hash.update(&receipt.commit_sequence.to_be_bytes());
    }
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
