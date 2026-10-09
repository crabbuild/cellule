use cellule_runtime::{Error, Result};
use serde::{Deserialize, Serialize};

/// Maximum account Cells frozen into one billing period.
pub const MAX_ACCOUNTS: usize = 8;
/// Maximum permanently identified usage events accepted by one account in a period.
pub const MAX_EVENTS_PER_ACCOUNT: usize = 32;
/// Maximum encoded workflow, report, or reconciliation request size.
pub const MAX_WIRE_BYTES: usize = 256 << 10;
/// Maximum one account event's amount in synthetic microcredits.
pub const MAX_AMOUNT_MICROCREDITS: u64 = 1_000_000_000_000;

/// Canonical tenant-local account identity and entity partition key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AccountKey(String);

impl AccountKey {
    /// Accepts a 1..64 character lowercase ASCII slug.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(Error::Identity("invalid usage-ledger account key"));
        }
        Ok(Self(value))
    }

    /// Canonical bytes used to select this account's SQL Cell.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Canonical display value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AccountKey {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<AccountKey> for String {
    fn from(value: AccountKey) -> Self {
        value.0
    }
}

impl cellule_app::CellKey for AccountKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Immutable billing-period identity and explicit participating account roster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeriodSpec {
    /// Nonzero period identity, also used as the period Cell entity key.
    pub id: [u8; 16],
    /// Inclusive event-time lower bound in Unix milliseconds.
    pub start_ms: i64,
    /// Exclusive event-time upper bound in Unix milliseconds.
    pub end_ms: i64,
    /// Complete sorted set of source account Cells.
    pub accounts: Vec<AccountKey>,
}

impl PeriodSpec {
    /// Validates the immutable roster, time interval, and bounded work set.
    pub fn validate(&self) -> Result<()> {
        if self.id == [0; 16]
            || self.start_ms < 0
            || self.start_ms >= self.end_ms
            || self.accounts.is_empty()
            || self.accounts.len() > MAX_ACCOUNTS
            || self.accounts.windows(2).any(|v| v[0] >= v[1])
        {
            return Err(Error::Command("invalid usage-ledger period specification"));
        }
        Ok(())
    }
}

/// Permanent exact source fact; retries bind the event ID to all fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageEvent {
    /// Period this usage belongs to.
    pub period_id: [u8; 16],
    /// Permanent account-local event identity.
    pub id: [u8; 16],
    /// Source account Cell identity.
    pub account: AccountKey,
    /// Event time inside the period's half-open interval.
    pub occurred_at_ms: i64,
    /// Synthetic microcredits billed by this usage record.
    pub amount_microcredits: u64,
    /// Canonical usage category.
    pub category: String,
}

impl UsageEvent {
    /// Checks identity, category, and amount bounds before publication.
    pub fn validate(&self) -> Result<()> {
        if self.period_id == [0; 16]
            || self.id == [0; 16]
            || self.occurred_at_ms < 0
            || self.amount_microcredits == 0
            || self.amount_microcredits > MAX_AMOUNT_MICROCREDITS
            || self.category.is_empty()
            || self.category.len() > 32
            || !self
                .category
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(Error::Command("invalid usage-ledger event"));
        }
        Ok(())
    }
}

/// Stable outcome for business-level usage admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageDecision {
    /// New source fact and projection Effect committed together.
    Accepted,
    /// Exact event identity and bytes were already committed.
    Duplicate,
    /// A permanent identity was reused with different event bytes.
    Conflict,
    /// Account is absent or bound to another period.
    NotBound,
    /// The account close barrier already committed.
    Closed,
    /// Per-account permanent event limit has been reached.
    Capacity,
    /// Event is outside the configured period.
    OutsidePeriod,
}

/// Account-local state and complete bounded accepted-event set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshot {
    /// Bound account identity.
    pub account: AccountKey,
    /// Exact period identity.
    pub period_id: [u8; 16],
    /// Events ordered by permanent ID at the close fence.
    pub events: Vec<UsageEvent>,
    /// Canonical digest over the exact ordered event payloads.
    pub digest: [u8; 32],
    /// Exact source total in synthetic microcredits.
    pub total_microcredits: u64,
}

impl AccountSnapshot {
    /// Verifies complete-set bounds, ordering, account binding, and accounting total.
    pub fn validate(&self) -> Result<()> {
        if self.period_id == [0; 16]
            || self.events.len() > MAX_EVENTS_PER_ACCOUNT
            || self.events.windows(2).any(|v| v[0].id >= v[1].id)
            || self
                .events
                .iter()
                .any(|event| event.period_id != self.period_id || event.account != self.account)
        {
            return Err(Error::Command("invalid usage-ledger account snapshot"));
        }
        for event in &self.events {
            event.validate()?;
        }
        if self.total_microcredits != checked_total(&self.events)?
            || self.digest != crate::wire::events_digest(&self.events)?
        {
            return Err(Error::Identity(
                "usage-ledger source snapshot digest differs",
            ));
        }
        Ok(())
    }
}

/// Projection Effect payload, independently idempotent in the period Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    /// Full immutable source event.
    pub event: UsageEvent,
}

/// Period projection and source reconciliation status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeriodStatus {
    /// Roster and accounts are being bound; no usage has been accepted yet.
    Opening,
    /// Every roster account has activated and the period accepts usage.
    Open,
    /// New usage is fenced account by account and source sets are reconciling.
    Closing,
    /// All account source sets matched and the statement is immutable.
    Sealed,
}

/// Immutable report contents stored in the period SQL Cell before Blob publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerReport {
    /// Permanent period identity.
    pub period_id: [u8; 16],
    /// Immutable half-open event-time interval.
    pub start_ms: i64,
    /// Immutable half-open event-time interval.
    pub end_ms: i64,
    /// Complete sorted roster.
    pub accounts: Vec<AccountKey>,
    /// Complete sorted source event set across the roster.
    pub events: Vec<UsageEvent>,
    /// Number of events included in the statement.
    pub event_count: u32,
    /// Exact total in synthetic microcredits.
    pub total_microcredits: u64,
    /// Canonical digest over the ordered report contents.
    pub digest: [u8; 32],
    /// Logical timestamp fixed by the first successful seal transaction.
    pub sealed_at_ms: i64,
}

impl LedgerReport {
    /// Verifies complete roster coverage, ordering, byte identity, and total.
    pub fn validate(&self) -> Result<()> {
        if self.period_id == [0; 16]
            || self.start_ms < 0
            || self.start_ms >= self.end_ms
            || self.sealed_at_ms < 0
            || self.accounts.is_empty()
            || self.accounts.len() > MAX_ACCOUNTS
            || self.accounts.windows(2).any(|v| v[0] >= v[1])
            || self.events.len() > MAX_ACCOUNTS * MAX_EVENTS_PER_ACCOUNT
            || self
                .events
                .windows(2)
                .any(|v| event_order(&v[0], &v[1]) != std::cmp::Ordering::Less)
            || self.event_count as usize != self.events.len()
            || self.events.iter().any(|event| {
                event.period_id != self.period_id
                    || !self.accounts.contains(&event.account)
                    || event.occurred_at_ms < self.start_ms
                    || event.occurred_at_ms >= self.end_ms
            })
        {
            return Err(Error::Command("invalid usage-ledger report"));
        }
        for event in &self.events {
            event.validate()?;
        }
        if self.total_microcredits != checked_total(&self.events)?
            || self.digest != crate::wire::report_digest(self)?
        {
            return Err(Error::Identity("usage-ledger report digest differs"));
        }
        Ok(())
    }

    /// Stable content-addressed key for the immutable statement object.
    pub fn blob_key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule-cookbook-usage-ledger/report/v1\0");
        hash.update(&self.period_id);
        hash.update(&self.digest);
        *hash.finalize().as_bytes()
    }
}

/// Immutable Blob manifest receipt returned by the close Activity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Stable content-addressed key.
    pub key: [u8; 32],
    /// Digest of exact exported CSV bytes.
    pub digest: [u8; 32],
    /// Native Blob manifest ETag.
    pub etag: [u8; 32],
    /// Published byte length.
    pub bytes: u32,
}

impl Artifact {
    /// Requires a nonzero key, digest, ETag, and bounded nonempty object.
    pub fn validate(&self) -> Result<()> {
        if self.key == [0; 32]
            || self.digest == [0; 32]
            || self.etag == [0; 32]
            || self.bytes == 0
            || self.bytes as usize > crate::MAX_WIRE_BYTES
        {
            return Err(Error::Command("invalid usage-ledger Blob artifact"));
        }
        Ok(())
    }
}

/// Final native Activity result after source reconciliation and Blob verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseCompletion {
    /// Fully reconciled immutable source report.
    pub report: LedgerReport,
    /// Verified Blob manifest metadata.
    pub artifact: Artifact,
}

/// Durable native Workflow state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowState {
    /// Permanent period identity.
    pub period_id: [u8; 16],
    /// Frozen loopback Activity endpoint.
    pub endpoint: String,
    /// The active native Activity ID, if close work is in flight.
    pub action: Option<[u8; 16]>,
    /// Verified completed statement, present only after Activity completion.
    pub completion: Option<CloseCompletion>,
    /// Bounded nonretryable Activity diagnostic, if the native run failed.
    pub failure: Option<String>,
}

/// Typed period identity used by native query and close commands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeriodIdentity(pub [u8; 16]);

/// Permanent start request for one close workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseRequest {
    /// Period to close; its roster is read from the durable period Cell.
    pub period_id: [u8; 16],
    /// Frozen local adapter URL used by its Activity.
    pub endpoint: String,
}

impl CloseRequest {
    /// Requires a canonical numeric loopback adapter URL.
    pub fn validate(&self) -> Result<()> {
        let url = url::Url::parse(&self.endpoint)
            .map_err(|_| Error::Command("invalid usage-ledger Activity URL"))?;
        if self.period_id == [0; 16]
            || url.scheme() != "http"
            || url.host_str() != Some("127.0.0.1")
            || url.port().is_none()
            || url.path() != "/"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != self.endpoint
        {
            return Err(Error::Command(
                "usage-ledger Activity URL must be canonical loopback",
            ));
        }
        Ok(())
    }
}

/// Permanent account close work identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBinding {
    /// Immutable period specification used to fence event admission.
    pub spec: PeriodSpec,
    /// Bound account identity.
    pub account: AccountKey,
}

/// Period-side acknowledgment that a source account is ready.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountReady {
    /// Immutable period identity.
    pub period_id: [u8; 16],
    /// Source account whose binding is permanent.
    pub account: AccountKey,
}

/// Explicit close barrier command input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseAccount {
    /// Period whose event set is fenced and returned.
    pub period_id: [u8; 16],
    /// Source account identity.
    pub account: AccountKey,
}

/// Explicit source reconciliation command input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileAccount {
    /// Full immutable period specification.
    pub spec: PeriodSpec,
    /// Complete event set captured at the account's close barrier.
    pub snapshot: AccountSnapshot,
}

/// Command and query outcomes returned by both SQL domains.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountProgress {
    /// Source account identity.
    pub account: AccountKey,
    /// Bound period identity.
    pub period_id: [u8; 16],
    /// Whether account-local admission has been fenced.
    pub closed: bool,
    /// Accepted source event count.
    pub event_count: u32,
    /// Accepted source total in synthetic microcredits.
    pub total_microcredits: u64,
}

/// Durable period view and status.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeriodView {
    /// Immutable period specification.
    pub spec: PeriodSpec,
    /// Current period lifecycle.
    pub status: PeriodStatus,
    /// Number of account Cells acknowledged ready.
    pub ready_accounts: u32,
    /// Number of source snapshots reconciled.
    pub reconciled_accounts: u32,
    /// Number of distinct currently projected events.
    pub projected_events: u32,
    /// Sealed report, if all source sets have been reconciled.
    pub report: Option<LedgerReport>,
}

/// Operation outcomes preserve expected business decisions as typed values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeriodDecision {
    /// Period roster and account-ready state are durably bound.
    Created,
    /// Exact period specification was already bound.
    Existing,
    /// Exact account projection already exists.
    Projected,
    /// Exact account readiness acknowledgment already exists.
    Ready,
    /// Close barrier has been durably raised.
    Closing,
    /// Exact account snapshot reconciled.
    Reconciled,
    /// Immutable report sealed from every roster account.
    Sealed,
    /// Identity or payload differs from its permanent binding.
    Conflict,
    /// Period/account is not at a legal lifecycle step.
    InvalidState,
    /// Explicit roster or permanent operation bound is exhausted.
    Capacity,
}

/// Native Workflow start command result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartDecision {
    /// Workflow is running or has already completed.
    Started,
    /// Permanent period Workflow identity is bound to different inputs.
    Conflict,
}

pub(crate) fn checked_total(events: &[UsageEvent]) -> Result<u64> {
    events.iter().try_fold(0_u64, |sum, event| {
        sum.checked_add(event.amount_microcredits)
            .ok_or(Error::Command("usage-ledger total overflow"))
    })
}

pub(crate) fn event_order(left: &UsageEvent, right: &UsageEvent) -> std::cmp::Ordering {
    left.account
        .cmp(&right.account)
        .then_with(|| left.id.cmp(&right.id))
}
