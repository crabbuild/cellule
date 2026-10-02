use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Canonical employee identity selected by authenticated ingress.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Employee(String);
impl Employee {
    /// Validates 1–32 lowercase ASCII letters, digits, or hyphens.
    pub fn parse(value: &str) -> Result<Self, CodecError> {
        if value.is_empty()
            || value.len() > 32
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(CodecError::Invalid("invalid employee identity"));
        }
        Ok(Self(value.into()))
    }
    /// Returns the canonical principal subject.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for Employee {
    type Error = CodecError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<Employee> for String {
    fn from(value: Employee) -> Self {
        value.0
    }
}
/// Canonical nonzero UUID identifying one purchase request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PurchaseId([u8; 16]);
impl PurchaseId {
    /// Parses a lowercase hyphenated nonzero UUID.
    pub fn parse(value: &str) -> Result<Self, CodecError> {
        let id = uuid::Uuid::parse_str(value)
            .map_err(|_| CodecError::Invalid("invalid purchase UUID"))?;
        if id.is_nil() || id.to_string() != value {
            return Err(CodecError::Invalid(
                "purchase UUID must be canonical and nonzero",
            ));
        }
        Ok(Self(*id.as_bytes()))
    }
    /// Returns the stable workflow routing bytes.
    pub fn bytes(self) -> [u8; 16] {
        self.0
    }
}
impl TryFrom<String> for PurchaseId {
    type Error = CodecError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}
impl From<PurchaseId> for String {
    fn from(value: PurchaseId) -> Self {
        uuid::Uuid::from_bytes(value.0).to_string()
    }
}
impl std::fmt::Display for PurchaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        uuid::Uuid::from_bytes(self.0).fmt(f)
    }
}

/// Immutable, version-one submission frozen before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Purchase {
    /// Stable request identity.
    pub id: PurchaseId,
    /// Authenticated submitting employee; ingress stamps this field.
    pub requester: Employee,
    /// Canonically sorted distinct approvers, excluding the requester; 1–4.
    pub approvers: Vec<Employee>,
    /// Trimmed description of 1–128 bytes without control characters.
    pub title: String,
    /// Synthetic purchase units, 1–1,000,000,000.
    pub units: u64,
    /// Absolute logical deadline, frozen by preparation.
    pub deadline_ms: i64,
    /// One reminder due strictly before the deadline.
    pub remind_at_ms: i64,
    /// App-configured absolute private mailbox directory, frozen with the run.
    pub mailbox: String,
    /// Explicit fault checkpoint delay after first mailbox publication, 0–10000 ms.
    pub publication_delay_ms: u64,
}
impl Purchase {
    /// Checks canonical domain data and bounded delivery configuration.
    pub fn validate(&self) -> Result<(), CodecError> {
        Employee::parse(self.requester.as_str())?;
        if !(1..=4).contains(&self.approvers.len())
            || self.approvers.windows(2).any(|pair| pair[0] >= pair[1])
            || self.approvers.contains(&self.requester)
        {
            return Err(CodecError::Invalid(
                "approvers must be distinct, sorted, and exclude the requester",
            ));
        }
        for person in &self.approvers {
            Employee::parse(person.as_str())?;
        }
        if self.title.is_empty()
            || self.title.len() > 128
            || self.title.trim() != self.title
            || self.title.chars().any(char::is_control)
            || !(1..=1_000_000_000).contains(&self.units)
        {
            return Err(CodecError::Invalid("invalid purchase description or units"));
        }
        if self.remind_at_ms < 0
            || self.deadline_ms <= self.remind_at_ms
            || self.mailbox.len() > 1024
            || !std::path::Path::new(&self.mailbox).is_absolute()
            || self.mailbox.contains('\0')
            || self.publication_delay_ms > 10_000
        {
            return Err(CodecError::Invalid(
                "invalid deadline or mailbox configuration",
            ));
        }
        Ok(())
    }
}
/// One immutable human choice. An approver's first accepted vote wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Choice {
    /// Approve the purchase.
    Approve,
    /// Reject the purchase.
    Reject,
}
/// Definition-owned signal; the client stamps the authenticated actor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Vote {
    pub actor: Employee,
    pub choice: Choice,
}
/// Durable business outcome, separate from native run control status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Waiting for every assigned approver.
    Pending,
    /// Every assigned approver approved before the deadline.
    Approved,
    /// An assigned approver rejected before the deadline.
    Rejected,
    /// Logical time reached the deadline before a terminal decision.
    TimedOut,
}
/// Immutable external delivery receipt produced by the reminder Activity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailReceipt {
    /// Native Activity idempotency key, lowercase 64-digit hexadecimal.
    pub key: String,
    /// BLAKE3 digest of the exact durable mailbox record bytes.
    pub content_digest: String,
}
impl MailReceipt {
    pub(crate) fn validate(&self) -> Result<(), CodecError> {
        for value in [&self.key, &self.content_digest] {
            if value.len() != 64
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(CodecError::Invalid("invalid mailbox receipt"));
            }
        }
        Ok(())
    }
}
/// Delivery state; an external record may already exist while completion is pending.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Reminder {
    /// The reminder timer has not fired.
    NotDue,
    /// An Activity was published; delivery/completion is not yet observed.
    Queued {
        /// Exact native action identity.
        activity: [u8; 16],
    },
    /// The Activity completion and receipt are durably recorded.
    Delivered {
        /// Verified durable external receipt.
        receipt: MailReceipt,
    },
    /// Reminder execution failed terminally; approval may still proceed.
    Failed,
}
/// One business transition, bounded by four votes, two timers, and one Activity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditEntry {
    /// Native event sequence; ignored votes can leave sequence gaps.
    pub sequence: u64,
    /// Workflow logical transition time.
    pub at_ms: i64,
    /// Stable transition label.
    pub event: String,
    /// Actor for an accepted human choice.
    pub actor: Option<Employee>,
}
/// Version-one durable definition state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalState {
    /// Frozen submitted purchase.
    pub purchase: Purchase,
    /// First accepted vote per assigned approver.
    pub votes: BTreeMap<Employee, Choice>,
    /// Terminal business result or pending.
    pub phase: Phase,
    /// Reminder progress in its own external durability domain.
    pub reminder: Reminder,
    /// Bounded business-transition history for the current run.
    pub audit: Vec<AuditEntry>,
    pub(crate) reminder_timer: [u8; 16],
    pub(crate) deadline_timer: [u8; 16],
}
impl ApprovalState {
    pub(crate) fn validate(&self) -> Result<(), CodecError> {
        self.purchase.validate()?;
        if self.votes.len() > 4
            || self
                .votes
                .keys()
                .any(|actor| !self.purchase.approvers.contains(actor))
            || self.audit.len() > 12
            || self
                .audit
                .windows(2)
                .any(|entries| entries[0].sequence >= entries[1].sequence)
        {
            return Err(CodecError::Invalid("invalid approval state"));
        }
        if let Reminder::Delivered { receipt } = &self.reminder {
            receipt.validate()?;
        }
        Ok(())
    }
}
pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 8192 {
        return Err(CodecError::Limit.into());
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 8192 {
        return Err(CodecError::Limit.into());
    }
    Ok(serde_json::from_slice(bytes)?)
}
/// Bounded page request for business transitions of the current run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRequest {
    /// Exact purchase identity.
    pub id: PurchaseId,
    /// Exact run; restarting invalidates this page traversal.
    pub run_id: [u8; 16],
    /// Continue after this native event sequence.
    pub after: Option<u64>,
    /// Maximum entries, 1–10.
    pub limit: u32,
}
/// Current-read page; native request/signal deduplication is not a new business transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPage {
    /// Current exact run; restarting replaces this run's history.
    pub run_id: [u8; 16],
    /// Ordered business transitions.
    pub entries: Vec<AuditEntry>,
    /// Continue after this sequence; null ends the page traversal.
    pub next: Option<u64>,
}
impl WireValue for HistoryRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.id.bytes())?;
        e.write_bytes(&self.run_id)?;
        self.after.encode(e)?;
        self.limit.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let bytes = d
            .read_bytes()?
            .try_into()
            .map_err(|_| CodecError::Invalid("identity length must be 16 bytes"))?;
        let id = PurchaseId::parse(&uuid::Uuid::from_bytes(bytes).to_string())?;
        let run_id = d
            .read_bytes()?
            .try_into()
            .map_err(|_| CodecError::Invalid("run identity length must be 16 bytes"))?;
        Ok(Self {
            id,
            run_id,
            after: Option::<u64>::decode(d)?,
            limit: u32::decode(d)?,
        })
    }
}
impl WireValue for AuditEntry {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.sequence.encode(e)?;
        self.at_ms.encode(e)?;
        self.event.encode(e)?;
        self.actor
            .as_ref()
            .map(|actor| actor.as_str().to_string())
            .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            sequence: u64::decode(d)?,
            at_ms: i64::decode(d)?,
            event: String::decode(d)?,
            actor: Option::<String>::decode(d)?
                .map(|value| Employee::parse(&value))
                .transpose()?,
        })
    }
}
impl WireValue for HistoryPage {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.run_id)?;
        if self.entries.len() > 10 {
            return Err(CodecError::Limit);
        }
        e.write_count(self.entries.len())?;
        for entry in &self.entries {
            entry.encode(e)?;
        }
        self.next.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let run_id = d
            .read_bytes()?
            .try_into()
            .map_err(|_| CodecError::Invalid("identity length must be 16 bytes"))?;
        let count = u32::decode(d)?;
        if count > 10 {
            return Err(CodecError::Limit);
        }
        let entries = (0..count)
            .map(|_| AuditEntry::decode(d))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            run_id,
            entries,
            next: Option::<u64>::decode(d)?,
        })
    }
}
