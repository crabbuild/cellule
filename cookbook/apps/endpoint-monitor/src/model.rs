use cellule_runtime::Error;
use serde::{Deserialize, Serialize};
/// Maximum permanent schedules in this reference installation.
pub const MAX_MONITORS: i64 = 16;
/// Maximum immutable schedule definitions, including deleted lifetimes.
pub const MAX_DEFINITIONS: i64 = 128;
/// Maximum retained probes, observations, and notification edges per installation.
pub const MAX_CHECKS: i64 = 2048;
/// Maximum time a scheduled probe can wait before becoming an unknown observation.
pub const PROBE_LIFETIME_MS: i64 = 3_600_000;
/// Canonical nonzero UUID identity, independent of native lease attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id([u8; 16]);
impl Id {
    /// Validates binary identity bytes.
    pub fn from_bytes(value: [u8; 16]) -> cellule_runtime::Result<Self> {
        if value == [0; 16] {
            return Err(Error::Identity("zero monitor UUID"));
        }
        Ok(Self(value))
    }
    /// Returns persistent routing bytes.
    pub fn bytes(self) -> [u8; 16] {
        self.0
    }
}
impl TryFrom<String> for Id {
    type Error = Error;
    fn try_from(text: String) -> Result<Self, Error> {
        let value =
            uuid::Uuid::parse_str(&text).map_err(|_| Error::Identity("invalid monitor UUID"))?;
        if text != value.to_string() {
            return Err(Error::Identity("monitor UUID must be canonical"));
        }
        Self::from_bytes(*value.as_bytes())
    }
}
impl From<Id> for String {
    fn from(value: Id) -> Self {
        uuid::Uuid::from_bytes(value.0).to_string()
    }
}
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        uuid::Uuid::from_bytes(self.0).fmt(f)
    }
}
/// One immutable endpoint configuration; a replacement needs a new version identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    /// Caller-retained permanent version identity.
    pub id: Id,
    /// Trimmed nonempty display label, at most 80 UTF-8 bytes.
    pub label: String,
    /// Canonical numeric loopback HTTP URL; no query, fragment, or credentials.
    pub endpoint: String,
}
impl Definition {
    /// Validates the declared local reference workload before dispatch.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.label.is_empty()
            || self.label.len() > 80
            || self.label.trim() != self.label
            || self.label.chars().any(char::is_control)
        {
            return Err(Error::Command("invalid monitor label"));
        }
        validate_endpoint(&self.endpoint)
    }
}
/// Checks the exact numeric loopback origin and bounded canonical path.
pub fn validate_endpoint(value: &str) -> cellule_runtime::Result<()> {
    let url = url::Url::parse(value).map_err(|_| Error::Command("invalid probe URL"))?;
    if value.len() > 256
        || url.as_str() != value
        || url.scheme() != "http"
        || !matches!(url.host(), Some(url::Host::Ipv4(ip)) if ip.is_loopback())
        || url.port().is_none_or(|port| port == 0)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Command(
            "probe URL must be canonical numeric IPv4 loopback HTTP with an explicit port",
        ));
    }
    Ok(())
}
/// Retain these exact absolute schedule operations before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Change {
    /// Installs a new immutable definition and resets its native occurrence counter.
    Upsert {
        /// Stable monitor identity.
        monitor: Id,
        /// Immutable endpoint and version.
        definition: Definition,
        /// Fixed interval, one second through one year.
        interval_ms: u64,
        /// Absolute first due time, frozen at preparation.
        next_due_ms: i64,
    },
    /// Stops future scheduled intent; published probes remain eligible.
    Pause {
        /// Stable monitor identity.
        monitor: Id,
    },
    /// Starts future ticks at a frozen absolute time, preserving the counter.
    Resume {
        /// Stable monitor identity.
        monitor: Id,
        /// Frozen next due timestamp.
        next_due_ms: i64,
    },
    /// Removes future scheduling while retaining all previous evidence.
    Delete {
        /// Stable monitor identity.
        monitor: Id,
    },
}
impl Change {
    /// Returns the controlled monitor.
    pub fn monitor(&self) -> Id {
        match self {
            Self::Upsert { monitor, .. }
            | Self::Pause { monitor }
            | Self::Resume { monitor, .. }
            | Self::Delete { monitor } => *monitor,
        }
    }
    /// Checks domain/time limits against original request issuance.
    pub fn validate(&self, issued: i64) -> cellule_runtime::Result<()> {
        if issued < 0 {
            return Err(Error::Identity("negative monitor issuance"));
        }
        let due = match self {
            Self::Upsert {
                definition,
                interval_ms,
                next_due_ms,
                ..
            } => {
                definition.validate()?;
                if !(1000..=31_536_000_000).contains(interval_ms) {
                    return Err(Error::Command("invalid probe interval"));
                }
                Some(*next_due_ms)
            }
            Self::Resume { next_due_ms, .. } => Some(*next_due_ms),
            _ => None,
        };
        if due.is_some_and(|time| time < issued || time > issued.saturating_add(157_680_000_000)) {
            return Err(Error::Command(
                "probe due time outside retained five-year window",
            ));
        }
        Ok(())
    }
}
/// Durable schedule mutation result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleOutcome {
    /// Exact native generation published by this mutation.
    Applied(u64),
    /// Future scheduling removed.
    Deleted,
    /// No native schedule exists.
    NotFound,
    /// An immutable definition identity was reused with different bytes.
    Conflict,
    /// Permanent installation capacity exhausted.
    Capacity,
}
/// Materialized native schedule with its immutable application payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedule {
    /// Stable monitor.
    pub monitor: Id,
    /// Immutable endpoint definition.
    pub definition: Definition,
    /// Current native generation.
    pub generation: u64,
    /// Published occurrence count within this definition.
    pub occurrence: u64,
    /// Next due timestamp.
    pub next_due_ms: i64,
    /// Fixed interval.
    pub interval_ms: u64,
    /// Whether future ticks are enabled.
    pub enabled: bool,
}
/// Immutable scheduled probe ticket, permanently bound in the Workflow Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ticket {
    /// Authorized source Cron Cell.
    pub source_cell: [u8; 32],
    /// Stable monitor.
    pub monitor: Id,
    /// Frozen endpoint and immutable lifetime.
    pub definition: Definition,
    /// Native source generation.
    pub generation: u64,
    /// Positive monotonic occurrence within this definition.
    pub occurrence: u64,
    /// Original due time; HTTP observation can happen later.
    pub scheduled_at_ms: i64,
}
impl Ticket {
    /// Validates fields before binding, dispatch, or projection.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.definition.validate()?;
        if self.source_cell == [0; 32]
            || self.generation == 0
            || self.generation > i64::MAX as u64
            || self.occurrence == 0
            || self.occurrence > i64::MAX as u64
            || self.scheduled_at_ms < 0
            || self
                .scheduled_at_ms
                .checked_add(PROBE_LIFETIME_MS)
                .is_none()
        {
            return Err(Error::Command("invalid scheduled probe ticket"));
        }
        Ok(())
    }
    /// Permanent business key excludes native run IDs and lease attempts.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.monitor.check.v1\0");
        hash.update(&self.source_cell);
        hash.update(&self.monitor.bytes());
        hash.update(&self.definition.id.bytes());
        hash.update(&self.generation.to_be_bytes());
        hash.update(&self.occurrence.to_be_bytes());
        *hash.finalize().as_bytes()
    }
}
/// First durably completed external observation for one native probe action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    /// Observed status; unknown leaves incident health unchanged.
    pub health: Health,
    /// HTTP response status, when a response was observed.
    pub status: Option<u16>,
    /// External observation time, separate from the original due time.
    pub observed_at_ms: i64,
    /// Bounded categorical reason, without URL credentials or arbitrary response text.
    pub reason: String,
}
/// Three-way observation classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Health {
    /// HTTP status 200 through 299 with a bounded readable body.
    Up,
    /// Non-success response, transport failure, timeout, or oversized body.
    Down,
    /// Native deadline or execution failure; no absence claim is made.
    Unknown,
}
impl Probe {
    /// Rejects contradictory or malformed observations.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        let valid = match (self.health, self.status, self.reason.as_str()) {
            (Health::Up, Some(status), "http") => (200..300).contains(&status),
            (Health::Down, Some(status), "http") => {
                (100..=599).contains(&status) && !(200..300).contains(&status)
            }
            (Health::Down, Some(status), "transport" | "body_limit") => {
                (100..=599).contains(&status)
            }
            (Health::Down, None, "transport")
            | (Health::Unknown, None, "deadline" | "execution_unknown") => true,
            _ => false,
        };
        if !valid
            || self.observed_at_ms < 0
            || !matches!(
                self.reason.as_str(),
                "http" | "transport" | "body_limit" | "deadline" | "execution_unknown"
            )
        {
            return Err(Error::Command("invalid probe observation"));
        }
        Ok(())
    }
}
/// One completed Workflow observation projected to SQL through a signed Effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    /// Immutable scheduled identity and source.
    pub ticket: Ticket,
    /// First native-completed external observation.
    pub probe: Probe,
}
impl Check {
    /// Verifies scheduled and external evidence bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.ticket.validate()?;
        self.probe.validate()
    }
}
/// One incident edge and its atomically published notification intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    /// Permanent edge key, derived from the accepted check and edge type.
    pub key: [u8; 32],
    /// Permanent incident key, derived from its opening check.
    pub incident: [u8; 32],
    /// Monitor whose definition produced the edge.
    pub monitor: Id,
    /// Immutable schedule lifetime.
    pub definition: Id,
    /// Opening or closing transition.
    pub kind: EdgeKind,
    /// Exact observation that caused this transition.
    pub check: [u8; 32],
}
/// Durable incident edge type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeKind {
    /// Incident opened on a down observation.
    Opened,
    /// Incident closed on an up observation.
    Closed,
}
impl Edge {
    /// Validates permanent identities and the versioned edge derivation.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.check == [0; 32]
            || self.incident == [0; 32]
            || self.key != edge_key(&self.check, self.kind)
            || (self.kind == EdgeKind::Opened && self.incident != self.key)
        {
            return Err(Error::Identity("invalid incident edge identity"));
        }
        Ok(())
    }
}
pub(crate) fn edge_key(check: &[u8; 32], kind: EdgeKind) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.monitor.edge.v1\0");
    hash.update(check);
    hash.update(&[if kind == EdgeKind::Opened { 0 } else { 1 }]);
    *hash.finalize().as_bytes()
}
/// Permanent projection result, reused by exact duplicate checks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordOutcome {
    /// Existing or newly published SQL check record and optional incident edge.
    Recorded {
        /// Receiver-local positive keyset cursor.
        row: i64,
        /// Edge published in the same transaction, if health changed.
        edge: Option<Edge>,
        /// Whether this observation advanced the incident watermark.
        current: bool,
    },
    /// Business key already binds different immutable bytes.
    Conflict,
    /// Permanent check capacity exhausted.
    Capacity,
}
/// Permanent Workflow admission result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartOutcome {
    /// Newly created native run.
    Started([u8; 16]),
    /// Identical permanently bound occurrence.
    AlreadyBound,
    /// Different bytes under one occurrence key.
    Conflict,
    /// Permanent probe capacity exhausted.
    Capacity,
}
/// One durable SQL check row, including whether it advanced incident state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCheck {
    /// Receiver-local positive cursor.
    pub row: i64,
    /// Immutable observation.
    pub check: Check,
    /// Original projection outcome, unchanged on duplicate delivery.
    pub outcome: RecordOutcome,
}
/// Current incident watermark for exactly one immutable definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorState {
    /// Stable monitor.
    pub monitor: Id,
    /// Immutable configuration lifetime.
    pub definition: Id,
    /// Largest accepted source occurrence; late checks never alter this state.
    pub occurrence: u64,
    /// Most recent known health; unknown observations preserve this value.
    pub health: Option<Health>,
    /// Open incident, if any.
    pub incident: Option<[u8; 32]>,
}
/// Bounded coherent SQL inspection request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    /// Exact monitor.
    pub monitor: Id,
    /// Exact immutable definition lifetime.
    pub definition: Id,
    /// Exclusive receiver-local cursor, zero to start.
    pub after: i64,
    /// Page size, one through 100.
    pub limit: u32,
}
impl PageRequest {
    /// Rejects invalid keyset and output bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.after < 0 || !(1..=100).contains(&self.limit) {
            return Err(Error::Command("invalid monitor page bounds"));
        }
        Ok(())
    }
}
/// Coherent incident state and one bounded check page from the monitor SQL Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inspection {
    /// Current incident state within this SQL transaction.
    pub state: Option<MonitorState>,
    /// Ordered check records in this definition.
    pub checks: Vec<StoredCheck>,
    /// Continuation cursor, or null at the observed end.
    pub next: Option<i64>,
}
/// Durable notification receiver outcome, with explicit business rejection reasons.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertOutcome {
    /// Newly published or identical original receiver row.
    Recorded(i64),
    /// Permanent edge key already binds different bytes.
    Conflict,
    /// Permanent notification capacity exhausted.
    Capacity,
}
/// One bounded notification inbox page, independent of the source receipt domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertPage {
    /// Permanent incident edges in receiver arrival order.
    pub edges: Vec<Edge>,
    /// Positive next cursor, or null at the observed end.
    pub next: Option<i64>,
}
/// Bounded native probe state and its retained external observation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeState {
    /// Frozen occurrence and endpoint.
    pub ticket: Ticket,
    /// Exact native action expected at completion.
    pub action: Option<[u8; 16]>,
    /// Retained terminal observation, including uncertainty.
    pub check: Option<Check>,
}
pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|source| Error::PeerTransport {
        context: "encode monitor data",
        source: Box::new(source),
    })?;
    if bytes.len() > 262144 {
        return Err(Error::Command("monitor encoded data exceeds 256 KiB"));
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 262144 {
        return Err(Error::Command("monitor data exceeds 256 KiB"));
    }
    serde_json::from_slice(bytes).map_err(|source| Error::PeerTransport {
        context: "decode monitor data",
        source: Box::new(source),
    })
}
