use super::*;
use cellule_runtime::fleet::operations::{NodeIntent, OperationError};

/// One bounded native inventory through its existing continuation contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetSnapshotSubject {
    /// Fixed host binding/lifecycle metadata; this is not role-absence proof.
    Host,
    /// All live and transitioning actors, including unavailable owner metadata.
    Cells(Option<cellule_runtime::cell::actor::CellInventoryCursor>),
    /// Installed managed views; accepted producer/native work is separate.
    Readers(Option<crate::read_replicas::ReaderInventoryCursor>),
    /// Original reader requests and retained protocol jobs.
    ReaderEnrollments(Option<crate::read_replicas::ReaderEnrollmentInventoryCursor>),
    /// Persisted local lanes, including cold and retired lanes.
    FollowerLanes(Option<cellule_runtime::follower::FollowerInventoryCursor>),
    /// Original selected members and managed leader protocol progress.
    FollowerEnrollments(Option<crate::FollowerEnrollmentInventoryCursor>),
    /// The original durability supervisor and its bounded rotation bank.
    DurabilitySupervisor,
}

impl FleetSnapshotSubject {
    pub(super) fn tag(&self) -> u8 {
        match self {
            Self::Host => 1,
            Self::Cells(_) => 2,
            Self::Readers(_) => 3,
            Self::ReaderEnrollments(_) => 4,
            Self::FollowerLanes(_) => 5,
            Self::FollowerEnrollments(_) => 6,
            Self::DurabilitySupervisor => 7,
        }
    }
    fn cursor_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::Cells(cursor) => cursor.map(|c| c.to_bytes().to_vec()),
            Self::Readers(cursor) => cursor.map(|c| c.to_bytes().to_vec()),
            Self::ReaderEnrollments(cursor) => cursor.map(|c| c.to_bytes().to_vec()),
            Self::FollowerLanes(cursor) => cursor.map(|c| c.to_bytes().to_vec()),
            Self::FollowerEnrollments(cursor) => cursor.map(|c| c.to_bytes().to_vec()),
            Self::Host | Self::DurabilitySupervisor => None,
        }
    }
}

/// Exact read request binding a native page to the original journal barrier.
/// Applications authenticate the transport and retain this request unchanged
/// across retries. It is an in-process contract, not a persisted/wire codec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetSnapshotRequest {
    expected: FleetJournalSnapshot,
    nonce: Digest,
    node: NodeId,
    session: SessionId,
    subject: FleetSnapshotSubject,
    limit: usize,
    issued_at_ms: i64,
    deadline_ms: i64,
}

impl FleetSnapshotRequest {
    /// Creates a read-only page request; it can authorize no acquisition or close.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        expected: FleetJournalSnapshot,
        nonce: Digest,
        node: NodeId,
        session: SessionId,
        subject: FleetSnapshotSubject,
        limit: usize,
        issued_at_ms: i64,
        deadline_ms: i64,
    ) -> Result<Self, OperationError> {
        let request = Self {
            expected,
            nonce,
            node,
            session,
            subject,
            limit,
            issued_at_ms,
            deadline_ms,
        };
        request.validate()?;
        Ok(request)
    }
    /// Returns the exact original head and enrollment registry version.
    #[must_use]
    pub fn expected(&self) -> &FleetJournalSnapshot {
        &self.expected
    }
    /// Returns the caller's never-restamped capture nonce.
    #[must_use]
    pub const fn nonce(&self) -> Digest {
        self.nonce
    }
    /// Returns the exact physical endpoint.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Returns the exact original boot.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the original category and native continuation.
    #[must_use]
    pub fn subject(&self) -> &FleetSnapshotSubject {
        &self.subject
    }
    /// Returns the original per-page bound.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
    /// Returns the original request time.
    #[must_use]
    pub const fn issued_at_ms(&self) -> i64 {
        self.issued_at_ms
    }
    /// Returns the exclusive capture deadline, bounded by the controller lease.
    #[must_use]
    pub const fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }
    /// Identifies every execution input; a key never replaces full replay equality.
    pub fn key(&self) -> Result<Digest, OperationError> {
        self.validate()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-native-snapshot-request.v1\0");
        for bytes in [
            self.expected.head().to_bytes()?,
            self.expected.registry().to_bytes()?,
        ] {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(&bytes);
        }
        hash.update(self.nonce.as_bytes());
        hash.update(self.node.as_bytes());
        hash.update(self.session.as_bytes());
        hash.update(&[self.subject.tag()]);
        let cursor = self.subject.cursor_bytes();
        hash.update(&[u8::from(cursor.is_some())]);
        if let Some(cursor) = cursor {
            hash.update(&cursor);
        }
        hash.update(&(self.limit as u64).to_be_bytes());
        hash.update(&self.issued_at_ms.to_be_bytes());
        hash.update(&self.deadline_ms.to_be_bytes());
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }
    /// Checks fresh full head/registry and the exact current endpoint intent.
    /// Call inside the same journal transaction; a separate cached read is insufficient.
    pub fn authorize_against(
        &self,
        current: &FleetJournalSnapshot,
        intent: &NodeIntent,
        now_ms: i64,
    ) -> Result<(), OperationError> {
        self.validate()?;
        if now_ms < self.issued_at_ms || now_ms >= self.deadline_ms {
            return Err(OperationError::Deadline);
        }
        if current != &self.expected
            || intent.scope() != current.head().scope()
            || intent.node() != self.node
            || intent.session() != self.session
        {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
    fn validate(&self) -> Result<(), OperationError> {
        self.expected.head().to_bytes()?;
        self.expected.registry().to_bytes()?;
        let maximum = if matches!(self.subject, FleetSnapshotSubject::FollowerEnrollments(_)) {
            32
        } else {
            128
        };
        if [
            self.nonce.as_bytes().as_slice(),
            self.node.as_bytes().as_slice(),
            self.session.as_bytes().as_slice(),
        ]
        .iter()
        .any(|id| id.iter().all(|b| *b == 0))
            || !(1..=maximum).contains(&self.limit)
            || self.issued_at_ms < 0
            || self.deadline_ms <= self.issued_at_ms
            || self.deadline_ms - self.issued_at_ms > 30_000
        {
            return Err(OperationError::Invalid("invalid native snapshot request"));
        }
        let lease = self
            .expected
            .head()
            .controller()
            .ok_or(OperationError::Fenced)?;
        if self.deadline_ms > lease.expires_at_ms {
            return Err(OperationError::Deadline);
        }
        Ok(())
    }
}
