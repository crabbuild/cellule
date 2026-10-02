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

mod inventory;
mod jobs;
mod protocol;
pub use inventory::{
    ReaderEnrollmentInventoryCursor, ReaderEnrollmentInventoryPage, ReaderEnrollmentJobs,
};

const MAX_JOBS: usize = 32;

pub(super) enum ActivationRequest {
    Hint(CellTarget, SessionId),
    Source(Box<ReadReplicaSource>),
}

pub(crate) struct ReaderEnrollment {
    scope: FleetScope,
    node: NodeId,
    journal: Arc<dyn FleetJournal>,
    records: StdMutex<HashMap<CellId, Record>>,
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
    response: tokio::sync::watch::Receiver<Option<std::result::Result<Receipt, Arc<Error>>>>,
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
    /// Whether the retained owner dispatched canonical native opening.
    pub opening_started: bool,
    /// Whether canonical opening returned after joining its accepted native work.
    pub opening_joined: bool,
    /// Original native opening failure, independent of publication failure.
    pub execution_error: Option<Arc<Error>>,
    /// Original unresolved result-publication failure.
    pub journal_error: Option<Arc<Error>>,
}

type Record = Arc<StdMutex<Responsibility>>;

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
            records: StdMutex::new(HashMap::new()),
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

    fn records(&self) -> Result<std::sync::MutexGuard<'_, HashMap<CellId, Record>>> {
        self.records
            .lock()
            .map_err(|_| Error::Control("reader enrollment index poisoned"))
    }

    pub(super) fn completion(&self, cell: CellId) -> Result<Option<ReaderEnrollmentCompletion>> {
        let record = self.records()?.get(&cell).cloned();
        record
            .map(|record| Ok(data(&record)?.completion()))
            .transpose()
    }

    pub(super) fn unresolved_cells(&self) -> Result<Vec<CellId>> {
        Ok(self.records()?.keys().copied().collect())
    }
}

fn data(record: &Record) -> Result<std::sync::MutexGuard<'_, Responsibility>> {
    record
        .lock()
        .map_err(|_| Error::Control("reader enrollment progress poisoned"))
}

impl Responsibility {
    fn completion(&self) -> ReaderEnrollmentCompletion {
        ReaderEnrollmentCompletion {
            spec: self.spec.clone(),
            source: self.source.clone(),
            accepted: self.original.clone(),
            event: self.event,
            published: self.published,
            opening_started: self.opening_started,
            opening_joined: self.opening_joined,
            execution_error: self.execution_error.clone(),
            journal_error: self.journal_error.clone(),
        }
    }
    fn remember_journal(&mut self, source: Box<dyn std::error::Error + Send + Sync>) -> Arc<Error> {
        let error = Arc::new(journal(source));
        self.journal_error.get_or_insert_with(|| error.clone());
        error
    }
}
