use cellule_runtime::{Error, codec::CodecError};
use serde::{Deserialize, Serialize};

/// Permanent conversation history bound per ticket.
pub const MAX_MESSAGES: usize = 64;
/// Permanent immutable attachment links per ticket.
pub const MAX_ATTACHMENTS: usize = 16;
/// Complete private attachment byte bound.
pub const MAX_ATTACHMENT_BYTES: usize = 64 << 10;
/// Deadline generations are permanent and bounded, including resolved generations.
pub const MAX_GENERATIONS: i64 = 64;
/// Maximum deadline distance from the local publication time.
pub const MAX_DEADLINE_MS: i64 = 7 * 24 * 60 * 60 * 1000;

macro_rules! key {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            /// Canonical 1..48 lowercase ASCII slug beginning with a letter.
            pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
                let value = value.into();
                if !(1..=48).contains(&value.len())
                    || !value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
                    || value.ends_with('-')
                    || value.contains("--")
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                {
                    return Err(CodecError::Invalid("noncanonical support-desk key"));
                }
                Ok(Self(value))
            }
            /// Permanent routing bytes.
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
            /// Canonical display label.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = CodecError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}
key!(
    TicketKey,
    "Permanent ticket identity within one authorized tenant."
);
key!(
    Actor,
    "Authorized conversation participant or agent; membership belongs to the embedding."
);
key!(
    MessageId,
    "Permanent message identity; exact author and bytes remain bound after resolution."
);
key!(
    AttachmentId,
    "Permanent attachment identity within one ticket."
);
impl cellule_app::CellKey for TicketKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
pub(crate) fn text(value: &str, limit: usize, multiline: bool) -> cellule_runtime::Result<()> {
    if value.is_empty()
        || value.len() > limit
        || value.trim() != value
        || value
            .chars()
            .any(|c| c.is_control() && !(multiline && matches!(c, '\n' | '\t')))
    {
        return Err(Error::Command("invalid bounded support-desk text"));
    }
    Ok(())
}
pub(crate) fn revision(value: i64) -> cellule_runtime::Result<()> {
    if !(1..i64::MAX).contains(&value) {
        return Err(Error::Command("invalid ticket revision"));
    }
    Ok(())
}
/// Frozen local-development notification endpoint, without credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct NotificationEndpoint(String);
impl NotificationEndpoint {
    /// Only explicit loopback HTTP `/notifications` endpoints are admitted by this profile.
    pub fn new(value: impl Into<String>) -> cellule_runtime::Result<Self> {
        let value = value.into();
        let parsed = url::Url::parse(&value).map_err(|source| Error::PeerTransport {
            context: "parse notification endpoint",
            source: Box::new(source),
        })?;
        if value.len() > 256
            || parsed.as_str() != value
            || parsed.scheme() != "http"
            || !matches!(parsed.host_str(), Some("127.0.0.1") | Some("[::1]"))
            || parsed.port().is_none()
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.path() != "/notifications"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(Error::Command(
                "notification endpoint must be canonical loopback HTTP",
            ));
        }
        Ok(Self(value))
    }
    /// Frozen public endpoint. Authentication is supplied by the binary's configuration.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for NotificationEndpoint {
    type Error = Error;
    fn try_from(value: String) -> cellule_runtime::Result<Self> {
        Self::new(value)
    }
}
impl From<NotificationEndpoint> for String {
    fn from(value: NotificationEndpoint) -> Self {
        value.0
    }
}

/// Ticket life cycle; escalation is separate from resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Accepts conversation and current-generation escalation.
    Open,
    /// Terminal until an explicit revision-fenced reopen.
    Resolved,
}
/// Exact immutable conversation entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Permanent message binding.
    pub id: MessageId,
    /// Embedding-authenticated participant.
    pub author: Actor,
    /// Complete UTF-8 body, at most 2048 bytes.
    pub body: String,
}
impl Message {
    /// Rejects empty, padded, excessive, or unsupported control text.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.body, 2048, true)
    }
}
/// Complete attachment metadata bound to exact immutable bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentDescriptor {
    /// Owning ticket.
    pub ticket: TicketKey,
    /// Permanent ticket-local attachment ID.
    pub id: AttachmentId,
    /// Safe display label, never an operating-system path.
    pub name: String,
    /// Exact size in bytes.
    pub size: u32,
    /// Complete BLAKE3 byte digest.
    pub digest: [u8; 32],
}
impl AttachmentDescriptor {
    /// Constructs a complete bounded content binding.
    pub fn new(
        ticket: TicketKey,
        id: AttachmentId,
        name: String,
        bytes: &[u8],
    ) -> cellule_runtime::Result<Self> {
        let value = Self {
            ticket,
            id,
            name,
            size: u32::try_from(bytes.len())
                .map_err(|_| Error::Command("attachment size overflow"))?,
            digest: *blake3::hash(bytes).as_bytes(),
        };
        value.validate()?;
        Ok(value)
    }
    /// Checks permanent identity metadata and size bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.name, 160, false)?;
        if !(1..=MAX_ATTACHMENT_BYTES as u32).contains(&self.size) || self.digest == [0; 32] {
            return Err(Error::Command("invalid support attachment descriptor"));
        }
        Ok(())
    }
    /// Stable key depends on ticket and ID; rebinding content cannot select a new key.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.support-desk.attachment.v1\0");
        for value in [self.ticket.as_bytes(), self.id.as_bytes()] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        *hash.finalize().as_bytes()
    }
    /// Verifies complete bytes, rather than trusting a caller-supplied digest.
    pub fn verify(&self, bytes: &[u8]) -> cellule_runtime::Result<()> {
        self.validate()?;
        if bytes.len() != self.size as usize || blake3::hash(bytes).as_bytes() != &self.digest {
            return Err(Error::Command("support attachment bytes differ"));
        }
        Ok(())
    }
}
/// Published immutable native Blob reference; its receipt remains a separate domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentPublication {
    /// Complete content binding.
    pub descriptor: AttachmentDescriptor,
    /// Native committed manifest version.
    pub etag: [u8; 32],
}
/// Full verified object returned by the Blob facade.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentObject {
    /// Immutable publication metadata.
    pub publication: AttachmentPublication,
    /// Byte-identical verified content.
    pub bytes: Vec<u8>,
}
/// Generation-fenced deadline capability, carried through native signed Effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deadline {
    /// Permanent ticket.
    pub ticket: TicketKey,
    /// Frozen generation.
    pub generation: i64,
    /// Absolute durable due time.
    pub due_at_ms: i64,
}
impl Deadline {
    /// Validates retained values independently of current wall time.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if !(1..=MAX_GENERATIONS).contains(&self.generation)
            || !(1..i64::MAX).contains(&self.due_at_ms)
        {
            return Err(Error::Command("invalid support deadline"));
        }
        Ok(())
    }
    /// Stable workflow key includes ticket and generation, never the current process.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.support-desk.deadline.v1\0");
        hash.update(self.ticket.as_bytes());
        hash.update(&self.generation.to_be_bytes());
        *hash.finalize().as_bytes()
    }
}
/// Published escalation history, preserved when resolving or reopening a ticket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    /// Exact ticket deadline that authorized escalation.
    pub deadline: Deadline,
    /// Tenant/application-specific source Cell ID, preventing external key collision.
    pub source_cell: [u8; 32],
    /// Agent as observed by the escalation transaction, if assigned.
    pub agent: Option<Actor>,
    /// Frozen destination configuration without credentials.
    pub endpoint: NotificationEndpoint,
    /// Exact escalation publication time.
    pub escalated_at_ms: i64,
}
impl Notification {
    /// Validates immutable notification capability.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.deadline.validate()?;
        if self.source_cell == [0; 32]
            || self.escalated_at_ms < self.deadline.due_at_ms
            || self.escalated_at_ms == i64::MAX
        {
            return Err(Error::Command("invalid support notification"));
        }
        Ok(())
    }
    /// Stable external idempotency key includes source scope and the exact generation.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.support-desk.notification.v1\0");
        hash.update(&self.source_cell);
        hash.update(&self.deadline.key());
        *hash.finalize().as_bytes()
    }
    /// Canonical external operation ID.
    pub fn key_hex(&self) -> String {
        blake3::Hash::from_bytes(self.key()).to_hex().to_string()
    }
    /// Exact immutable JSON request digest verified by the receiver acknowledgment.
    pub fn content_digest(&self) -> cellule_runtime::Result<String> {
        Ok(blake3::hash(&crate::wire::json(self, 8192)?)
            .to_hex()
            .to_string())
    }
    /// Notification delivery expires seven days after escalation publication.
    pub fn expires_at_ms(&self) -> cellule_runtime::Result<i64> {
        self.escalated_at_ms
            .checked_add(MAX_DEADLINE_MS)
            .ok_or(Error::Command("notification expiry overflow"))
    }
}
/// Bounded external delivery rounds; each round has its own durable retry timer.
pub const MAX_ROUNDS: u32 = 4;
/// Explicit external outcome classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    /// Verified exact external acknowledgment.
    Delivered,
    /// Transient or unknown external outcome, requiring the same permanent key.
    Retryable,
    /// Refused or malformed reply; external application may still have happened.
    Permanent,
}
/// Independent external receiver acknowledgment, never a Cellule receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgement {
    /// Permanent external operation ID.
    pub key: String,
    /// Exact immutable notification input digest.
    pub content_digest: String,
    /// Always one logical application at the idempotent receiver.
    pub applied_count: u32,
}
/// One complete classified HTTP attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpAttempt {
    /// Definition-owned retry round, 1..4.
    pub round: u32,
    /// Native Activity lease attempt.
    pub native_attempt: u32,
    /// External outcome evidence.
    pub classification: Classification,
    /// HTTP status if a response was received.
    pub status: Option<u32>,
    /// Bounded diagnostic text, excluding secrets.
    pub details: String,
    /// Unknown reply may follow external application.
    pub may_have_applied: bool,
    /// Exact verified external proof, if delivered.
    pub acknowledgement: Option<Acknowledgement>,
}
impl HttpAttempt {
    /// Checks bounded diagnostics and the exact independently committed acknowledgment.
    pub fn validate(&self, ticket: &Notification) -> cellule_runtime::Result<()> {
        ticket.validate()?;
        if !(1..=MAX_ROUNDS).contains(&self.round)
            || self.native_attempt == 0
            || self.details.len() > 512
            || self.status.is_some_and(|s| !(100..=599).contains(&s))
            || (self.classification == Classification::Delivered) != self.acknowledgement.is_some()
            || self
                .acknowledgement
                .as_ref()
                .is_some_and(|a| a.key != ticket.key_hex() || a.applied_count != 1)
            || (self.classification == Classification::Delivered
                && (self.status != Some(200) || !self.may_have_applied))
        {
            return Err(Error::Command("invalid notification attempt evidence"));
        }
        if let Some(ack) = &self.acknowledgement
            && ack.content_digest != ticket.content_digest()?
        {
            return Err(Error::Command(
                "notification acknowledgment content differs",
            ));
        }
        Ok(())
    }
}
/// Durable notification progress remains independent from the source ticket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationPhase {
    /// Accepted Activity awaiting completion.
    InFlight,
    /// Durable timer before another delivery round.
    Backoff,
    /// Exact external acknowledgment verified.
    Delivered,
    /// Terminal failure preserving any uncertainty.
    Failed,
    /// Four rounds exhausted; external state must be inspected explicitly.
    Exhausted,
    /// Delivery retention exceeded; no external absence is claimed.
    Expired,
}
/// Definition-owned bounded notification state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationState {
    /// Frozen immutable input.
    pub ticket: Notification,
    /// Current delivery phase.
    pub phase: NotificationPhase,
    /// Current round.
    pub round: u32,
    /// Expected native timer or Activity ID.
    pub action_id: Option<Vec<u8>>,
    /// Complete bounded attempts.
    pub attempts: Vec<HttpAttempt>,
    /// Native terminal failure, bounded to 512 bytes.
    pub failure: Option<String>,
}
impl NotificationState {
    /// Checks native action shape and retained external attempt identities.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.ticket.validate()?;
        if self.round > MAX_ROUNDS
            || self.attempts.len() > MAX_ROUNDS as usize
            || self
                .action_id
                .as_ref()
                .is_some_and(|id| id.len() != 16 || id.iter().all(|b| *b == 0))
            || matches!(
                self.phase,
                NotificationPhase::InFlight | NotificationPhase::Backoff
            ) != self.action_id.is_some()
            || self.failure.as_ref().is_some_and(|v| v.len() > 512)
        {
            return Err(Error::Command("invalid support notification state"));
        }
        for (i, attempt) in self.attempts.iter().enumerate() {
            attempt.validate(&self.ticket)?;
            if attempt.round as usize != i + 1 || attempt.round > self.round {
                return Err(Error::Command("notification round history differs"));
            }
        }
        if self.phase == NotificationPhase::Delivered
            && !self
                .attempts
                .last()
                .is_some_and(|a| a.classification == Classification::Delivered)
        {
            return Err(Error::Command(
                "delivered notification has no acknowledgment",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpInput {
    pub ticket: Notification,
    pub round: u32,
}
/// Definition-owned immutable timer state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadlineState {
    /// Original conditional capability.
    pub ticket: Deadline,
    /// Exact native timer action ID.
    pub timer_id: Option<Vec<u8>>,
    /// Logical callback publication time once the timer fires.
    pub fired_at_ms: Option<i64>,
}
/// Independent native Workflow observation, separate from authoritative ticket state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadlineView {
    /// Native running, completed, paused, cancelled, or failed status.
    pub status: String,
    /// Exact retained executable definition digest.
    pub definition: String,
    /// Definition-owned conditional timer state.
    pub state: DeadlineState,
}
/// Native notification status and independently retained external delivery evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationView {
    /// Native running, completed, paused, cancelled, or failed status.
    pub status: String,
    /// Exact retained executable definition digest.
    pub definition: String,
    /// Bounded immutable inputs and classified external delivery attempts.
    pub state: NotificationState,
}
/// Current ticket metadata; message pages are read in its own transaction domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ticket {
    /// Permanent entity key.
    pub key: TicketKey,
    /// Immutable opening subject.
    pub subject: String,
    /// Authenticated opening participant.
    pub requester: Actor,
    /// Current status.
    pub status: Status,
    /// Current assigned agent.
    pub agent: Option<Actor>,
    /// Source revision; every applied domain change increments it.
    pub revision: i64,
    /// Generation fence includes resolve/reopen and assignment/deadline changes.
    pub generation: i64,
    /// Exact current or resolved deadline capability.
    pub deadline: Deadline,
    /// Escalation belongs only to the current generation.
    pub escalated: bool,
    /// Frozen notification destination.
    pub endpoint: NotificationEndpoint,
    /// Original current-generation deadline start intent; zero only for resolved generations.
    pub deadline_effect: [u8; 32],
    /// Accepted message count, bounded to 64.
    pub message_count: u32,
    /// Full immutable links, bounded to 16.
    pub attachments: Vec<AttachmentPublication>,
    /// Permanent accepted escalation records, bounded by generation capacity.
    pub notifications: Vec<EscalationRecord>,
}
impl Ticket {
    /// Checks the complete local invariant, independent of wall-clock scheduling.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        text(&self.subject, 160, false)?;
        revision(self.revision)?;
        self.deadline.validate()?;
        if self.deadline.ticket != self.key
            || !(1..=MAX_GENERATIONS + 1).contains(&self.generation)
            || self.deadline.generation > self.generation
            || self.message_count as usize > MAX_MESSAGES
            || self.attachments.len() > MAX_ATTACHMENTS
            || self.notifications.len() > MAX_GENERATIONS as usize
            || (self.status == Status::Open
                && (self.deadline.generation != self.generation || self.deadline_effect == [0; 32]))
            || (self.status == Status::Resolved
                && (self.escalated || self.deadline_effect != [0; 32]))
        {
            return Err(Error::Command("invalid ticket state"));
        }
        for (index, link) in self.attachments.iter().enumerate() {
            link.descriptor.validate()?;
            if link.descriptor.ticket != self.key
                || link.etag == [0; 32]
                || self.attachments[..index]
                    .iter()
                    .any(|v| v.descriptor.id == link.descriptor.id)
            {
                return Err(Error::Command("invalid ticket attachment roster"));
            }
        }
        for (index, record) in self.notifications.iter().enumerate() {
            let item = &record.notification;
            item.validate()?;
            if record.effect_id == [0; 32]
                || item.deadline.ticket != self.key
                || item.endpoint != self.endpoint
                || item.deadline.generation > self.generation
                || (index > 0
                    && self.notifications[index - 1]
                        .notification
                        .deadline
                        .generation
                        >= item.deadline.generation)
            {
                return Err(Error::Command("invalid escalation history"));
            }
        }
        if self.escalated
            != self.notifications.last().is_some_and(|n| {
                self.status == Status::Open && n.notification.deadline == self.deadline
            })
        {
            return Err(Error::Command(
                "ticket escalation flag differs from history",
            ));
        }
        Ok(())
    }
}
/// Accepted escalation and its atomically published native source intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EscalationRecord {
    /// Exact external notification input.
    pub notification: Notification,
    /// Original native notification-start Effect identity.
    pub effect_id: [u8; 32],
}
/// Embedding-authorized local domain edit. Native request identity is retained separately.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// Opens an immutable subject/requester/endpoint and generation-one deadline.
    Open {
        /// Opening subject.
        subject: String,
        /// Authenticated participant.
        requester: Actor,
        /// Deadline in the next seven days.
        due_at_ms: i64,
        /// Application-configured receiver.
        endpoint: NotificationEndpoint,
    },
    /// Permanently binds a message ID to exact author and body.
    Message {
        /// Original source revision.
        expected_revision: i64,
        /// Immutable conversation entry.
        message: Message,
    },
    /// Assigns an agent and creates a new deadline generation.
    Assign {
        /// Original source revision.
        expected_revision: i64,
        /// Authorized agent.
        agent: Actor,
        /// New deadline in the next seven days.
        due_at_ms: i64,
    },
    /// Makes outstanding callbacks obsolete, even if the timer has already fired.
    Resolve {
        /// Original source revision.
        expected_revision: i64,
    },
    /// Explicitly reopens resolved work with a fresh generation and timer.
    Reopen {
        /// Original source revision.
        expected_revision: i64,
        /// New bounded deadline.
        due_at_ms: i64,
    },
}
impl Action {
    /// Admission checks independent of dispatch time.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        match self {
            Self::Open { subject, .. } => text(subject, 160, false),
            Self::Message {
                expected_revision,
                message,
            } => {
                revision(*expected_revision)?;
                message.validate()
            }
            Self::Assign {
                expected_revision, ..
            }
            | Self::Resolve { expected_revision }
            | Self::Reopen {
                expected_revision, ..
            } => revision(*expected_revision),
        }
    }
}
/// Public request binds the entity key to the selected Cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    /// Authorized entity.
    pub ticket: TicketKey,
    /// Exact original action.
    pub action: Action,
}
/// Source-domain decisions; rejection is durably distinguishable from unknown outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Local mutation and any native intents committed together.
    Applied,
    /// Permanent identity already binds identical input.
    Duplicate,
    /// Ticket does not exist.
    NotFound,
    /// Revision or permanent binding differs.
    Conflict,
    /// Operation requires an open or resolved ticket, as appropriate.
    WrongState,
    /// Bounded permanent history is full.
    Capacity,
    /// Invalid routing, body, or deadline.
    Invalid,
}
/// Small source decision containing a coherent bounded metadata snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    /// Applied, duplicate, or durable rejection.
    pub decision: Decision,
    /// Source snapshot, when available.
    pub ticket: Option<Ticket>,
}
/// Native deadline callback; logical due time is verified separately from delivery time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Escalation {
    /// Exact source generation and deadline.
    pub deadline: Deadline,
    /// Native Workflow logical timer publication time.
    pub fired_at_ms: i64,
}
/// Stale deadline callbacks are successful harmless no-ops.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationOutcome {
    /// Current open generation escalated and notification intent committed.
    Escalated,
    /// Old generation, resolved state, or already escalated.
    Unchanged,
    /// Forged capability or early callback.
    Invalid,
}
/// Bounded conversation keyset page request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    /// Exclusive conversation sequence, preserving source arrival order.
    pub after: u32,
    /// Between one and sixteen entries.
    pub limit: u32,
}
/// A message's monotonic ticket-local position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageEntry {
    /// Ticket-local accepted order, unrelated to native Queue ordering.
    pub sequence: u32,
    /// Complete permanent entry.
    pub message: Message,
}
/// One coherent SQL read of ticket metadata and at most sixteen entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagePage {
    /// Current metadata in the same SQL read.
    pub ticket: Option<Ticket>,
    /// Bounded entries in accepted order.
    pub messages: Vec<MessageEntry>,
    /// Exclusive continuation; absent when no further rows were observed.
    pub next: Option<u32>,
}
/// Cross-Cell reference command; issued only after verified immutable publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentLink {
    /// Original revision retained before publication.
    pub expected_revision: i64,
    /// Exact verified native manifest reference.
    pub publication: AttachmentPublication,
}
