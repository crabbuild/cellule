//! Frozen metadata from the original finite job owner, without retaining jobs.
use super::*;

const WORK_BYTES: usize = 4096;

/// Original accepted job category; read-only work still owns its native task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetActionWorkKind {
    /// Journal-bound native effect and result publication.
    Effect,
    /// Current authority/serving inspection.
    Inspection,
    /// Request-bound native page capture.
    Snapshot,
}

/// Original task ownership at observation time. None of these grants settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetActionWorkState {
    /// Original task has not returned a response or finished.
    Running,
    /// Original task finished without an observed response; it still needs joining.
    FinishedUnobserved,
    /// A response exists, but the original task has not been joined.
    Returned,
    /// Another caller owns the original join lane; its result is not inferred.
    Joining,
    /// Original task was joined. Response and failure flags remain independent.
    Joined,
}

/// Immutable metadata for one original job. No task handle or snapshot body is
/// retained: concurrent captures cannot form cycles through their result banks.
#[derive(Clone, Debug)]
pub struct FleetActionWorkEntry {
    key: Digest,
    kind: FleetActionWorkKind,
    state: FleetActionWorkState,
    response_received: bool,
    response_error: Option<Arc<Error>>,
    task_error: Option<Arc<Error>>,
    committed: Option<bool>,
    outcome_known: Option<bool>,
    result_digest: Option<Digest>,
    execution_error: Option<Arc<Error>>,
    journal_error: Option<Arc<Error>>,
}

impl FleetActionWorkEntry {
    /// Exact canonical original request key.
    #[must_use]
    pub const fn key(&self) -> Digest {
        self.key
    }
    /// Original request category.
    #[must_use]
    pub const fn kind(&self) -> FleetActionWorkKind {
        self.kind
    }
    /// Original join ownership, independently of response receipt.
    #[must_use]
    pub const fn state(&self) -> FleetActionWorkState {
        self.state
    }
    /// Whether the original response channel contained a result.
    #[must_use]
    pub const fn response_received(&self) -> bool {
        self.response_received
    }
    /// Original response failure, never converted to absent work.
    #[must_use]
    pub fn response_error(&self) -> Option<&Arc<Error>> {
        self.response_error.as_ref()
    }
    /// Original join failure when the join lane was observable.
    #[must_use]
    pub fn task_error(&self) -> Option<&Arc<Error>> {
        self.task_error.as_ref()
    }
    /// Effect publication confirmation; None for other categories or missing results.
    #[must_use]
    pub const fn committed(&self) -> Option<bool> {
        self.committed
    }
    /// Whether the returned effect outcome is known, independently of publication.
    #[must_use]
    pub const fn outcome_known(&self) -> Option<bool> {
        self.outcome_known
    }
    /// Canonical effect/inspection result digest; captures retain no recursive body.
    #[must_use]
    pub const fn result_digest(&self) -> Option<Digest> {
        self.result_digest
    }
    /// Original native execution failure.
    #[must_use]
    pub fn execution_error(&self) -> Option<&Arc<Error>> {
        self.execution_error.as_ref()
    }
    /// Original result publication failure.
    #[must_use]
    pub fn journal_error(&self) -> Option<&Arc<Error>> {
        self.journal_error.as_ref()
    }
}

/// Bounded original executor observation. Native response clones share immutable
/// metadata and its node allocation charge; full collectors copy into their
/// application-accounted buffers. Empty entries alone prove no durable journal closure,
/// external task settlement, or permission to finalize maintenance.
#[derive(Clone)]
pub struct FleetActionWorkSnapshot(Arc<WorkSnapshot>);

struct WorkSnapshot {
    scope: FleetScope,
    node: NodeId,
    session: SessionId,
    observed_at_ms: i64,
    excluded_capture: Option<Digest>,
    admission_closed: bool,
    work_revision: u64,
    capture_revision: u64,
    entries: Vec<FleetActionWorkEntry>,
    failure: Option<Arc<Error>>,
    digest: Digest,
    // Native responses carry the charge. A collector copies this bounded
    // metadata into its application-accounted buffer before releasing the page.
    _memory: Option<NodeByteReservation>,
}

impl FleetActionWorkSnapshot {
    /// Original authenticated fleet binding.
    #[must_use]
    pub fn scope(&self) -> FleetScope {
        self.0.scope
    }
    /// Original physical endpoint.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.0.node
    }
    /// Original boot identity.
    #[must_use]
    pub fn session(&self) -> SessionId {
        self.0.session
    }
    /// Actual original observation time; never restamped by a collector.
    #[must_use]
    pub fn observed_at_ms(&self) -> i64 {
        self.0.observed_at_ms
    }
    /// Only the exact currently executing native capture may exclude itself.
    #[must_use]
    pub fn excluded_capture(&self) -> Option<Digest> {
        self.0.excluded_capture
    }
    /// Original executor admission closure, independently of lifecycle/counts.
    #[must_use]
    pub fn admission_closed(&self) -> bool {
        self.0.admission_closed
    }
    /// Effect/inspection admissions and removals, including turnover between empty reads.
    #[must_use]
    pub fn work_revision(&self) -> u64 {
        self.0.work_revision
    }
    /// Every original retained job except the exact excluded capture.
    #[must_use]
    pub fn entries(&self) -> &[FleetActionWorkEntry] {
        &self.0.entries
    }
    /// First original retained bank failure, including after its job was removed.
    #[must_use]
    pub fn failure(&self) -> Option<&Arc<Error>> {
        self.0.failure.as_ref()
    }
    /// Canonical frozen work fingerprint. Fresh capture nonce/time are excluded;
    /// ordinary read-only traversal does not change the observed work itself.
    #[must_use]
    pub fn digest(&self) -> Digest {
        self.0.digest
    }
    pub(in crate::fleet) fn same_interval_work(&self, other: &Self) -> bool {
        self.digest() == other.digest() && self.0.capture_revision == other.0.capture_revision
    }
    pub(in crate::fleet) fn collector_copy(&self) -> Self {
        let work = &self.0;
        Self(Arc::new(WorkSnapshot {
            scope: work.scope,
            node: work.node,
            session: work.session,
            observed_at_ms: work.observed_at_ms,
            excluded_capture: work.excluded_capture,
            admission_closed: work.admission_closed,
            work_revision: work.work_revision,
            capture_revision: work.capture_revision,
            entries: work.entries.clone(),
            failure: work.failure.clone(),
            digest: work.digest,
            _memory: None,
        }))
    }
}

impl FleetActionExecutor {
    pub(crate) fn observe_current_work(&self) -> cellule_runtime::Result<FleetActionWorkSnapshot> {
        self.observe_work(None)
    }
    pub(crate) fn observe_work(
        &self,
        exclude: Option<&FleetSnapshotRequest>,
    ) -> cellule_runtime::Result<FleetActionWorkSnapshot> {
        // Admit both fixed retained metadata and bounded codec scratch before copying.
        let memory = self.runtime.try_reserve_node_metadata_bytes(WORK_BYTES)?;
        let _scratch = self
            .runtime
            .try_reserve_node_metadata_bytes(MAX_RECORD_BYTES as usize)?;
        let bank = self
            .bank
            .lock()
            .map_err(|_| Error::Control("fleet action bank poisoned"))?;
        let observed_at_ms = wall_time_ms()?;
        let excluded_capture = exclude
            .map(FleetSnapshotRequest::key)
            .transpose()
            .map_err(operation)?;
        if let Some(request) = exclude
            && !bank.jobs.iter().any(|job| Some(job.key) == excluded_capture
                && matches!(&job.request, JobRequest::Snapshot { request: original, .. } if original.as_ref() == request))
        { return Err(Error::Control("fleet work capture is not originally retained")); }
        let mut entries = Vec::with_capacity(MAX_ACTIVE_ATTEMPTS);
        for job in &bank.jobs {
            if excluded_capture == Some(job.key) {
                continue;
            }
            entries.push(observe_job(job)?);
        }
        entries.sort_by_key(|entry| *entry.key.as_bytes());
        let mut observed = WorkSnapshot {
            scope: self.scope,
            node: self.node,
            session: self.session,
            observed_at_ms,
            excluded_capture,
            admission_closed: bank.draining,
            work_revision: bank.work_revision,
            capture_revision: bank.capture_revision,
            entries,
            failure: bank.failure.clone(),
            digest: Digest::from_bytes([0; 32]),
            _memory: Some(memory),
        };
        observed.digest = work_digest(&observed);
        Ok(FleetActionWorkSnapshot(Arc::new(observed)))
    }
}

fn observe_job(job: &ActionJob) -> cellule_runtime::Result<FleetActionWorkEntry> {
    let kind = match &job.request {
        JobRequest::Effect { .. } => FleetActionWorkKind::Effect,
        JobRequest::Inspection(_) => FleetActionWorkKind::Inspection,
        JobRequest::Snapshot { .. } => FleetActionWorkKind::Snapshot,
    };
    observe_parts(job.key, kind, &job.completion, &job.task)
}

fn observe_parts(
    key: Digest,
    kind: FleetActionWorkKind,
    response: &watch::Receiver<Option<Completion>>,
    task: &tokio::sync::Mutex<ActionJoin>,
) -> cellule_runtime::Result<FleetActionWorkEntry> {
    let completion = response.borrow().clone();
    let (state, task_error) = match task.try_lock() {
        Err(_) => (FleetActionWorkState::Joining, None),
        Ok(join) => {
            let state = match &join.task {
                None => FleetActionWorkState::Joined,
                Some(_) if completion.is_some() => FleetActionWorkState::Returned,
                Some(task) if task.is_finished() => FleetActionWorkState::FinishedUnobserved,
                Some(_) => FleetActionWorkState::Running,
            };
            (state, join.failure.clone())
        }
    };
    let mut entry = FleetActionWorkEntry {
        key,
        kind,
        state,
        response_received: completion.is_some(),
        response_error: None,
        task_error,
        committed: None,
        outcome_known: None,
        result_digest: None,
        execution_error: None,
        journal_error: None,
    };
    match completion {
        Some(Err(error)) => entry.response_error = Some(error),
        Some(Ok(result)) => match result.as_ref() {
            JobCompletion::Effect(effect) => {
                entry.committed = Some(effect.committed);
                entry.outcome_known =
                    Some(!matches!(effect.outcome.outcome, FleetOutcome::Unknown));
                entry.result_digest = Some(Digest::from_bytes(
                    *blake3::hash(&effect.outcome.to_bytes().map_err(operation)?).as_bytes(),
                ));
                entry.execution_error = effect.execution_error.clone();
                entry.journal_error = effect.journal_error.clone();
            }
            JobCompletion::Inspection(inspection) => {
                entry.result_digest = Some(Digest::from_bytes(
                    *blake3::hash(&inspection.to_bytes().map_err(operation)?).as_bytes(),
                ));
            }
            JobCompletion::Snapshot(_) => {}
        },
        None => {}
    }
    Ok(entry)
}

fn work_digest(work: &WorkSnapshot) -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-action-work.v1\0");
    hash.update(work.scope.fleet.as_bytes());
    hash.update(work.scope.application.as_bytes());
    hash.update(work.node.as_bytes());
    hash.update(work.session.as_bytes());
    hash.update(&work.work_revision.to_be_bytes());
    hash.update(&[
        u8::from(work.admission_closed),
        u8::from(work.failure.is_some()),
    ]);
    hash.update(&(work.entries.len() as u64).to_be_bytes());
    for entry in &work.entries {
        hash.update(entry.key.as_bytes());
        hash.update(&[match entry.kind {
            FleetActionWorkKind::Effect => 1,
            FleetActionWorkKind::Inspection => 2,
            FleetActionWorkKind::Snapshot => 3,
        }]);
        hash.update(&[match entry.state {
            FleetActionWorkState::Running => 1,
            FleetActionWorkState::FinishedUnobserved => 2,
            FleetActionWorkState::Returned => 3,
            FleetActionWorkState::Joining => 4,
            FleetActionWorkState::Joined => 5,
        }]);
        hash.update(&[
            u8::from(entry.response_received),
            u8::from(entry.response_error.is_some()),
            u8::from(entry.task_error.is_some()),
            u8::from(entry.execution_error.is_some()),
            u8::from(entry.journal_error.is_some()),
        ]);
        for value in [entry.committed, entry.outcome_known] {
            hash.update(&[match value {
                None => 0,
                Some(false) => 1,
                Some(true) => 2,
            }]);
        }
        hash.update(&[u8::from(entry.result_digest.is_some())]);
        if let Some(digest) = entry.result_digest {
            hash.update(digest.as_bytes());
        }
    }
    Digest::from_bytes(*hash.finalize().as_bytes())
}

#[cfg(test)]
impl FleetActionWorkSnapshot {
    pub(in crate::fleet) fn for_request(request: &FleetSnapshotRequest, now: i64) -> Self {
        let mut work = WorkSnapshot {
            scope: request.expected().head().scope(),
            node: request.node(),
            session: request.session(),
            observed_at_ms: now,
            excluded_capture: Some(request.key().unwrap()),
            admission_closed: false,
            work_revision: 0,
            capture_revision: 1,
            entries: Vec::new(),
            failure: None,
            digest: Digest::from_bytes([0; 32]),
            _memory: None,
        };
        work.digest = work_digest(&work);
        Self(Arc::new(work))
    }
}

#[cfg(test)]
mod tests;
