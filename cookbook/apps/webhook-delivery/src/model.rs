use cellule_runtime::{
    Error,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
};
use serde::{Deserialize, Serialize};

/// Maximum permanently registered subscribers per publisher.
pub const MAX_SUBSCRIBERS: usize = 16;
/// Maximum permanently accepted events per publisher installation.
pub const MAX_EVENTS: i64 = 1024;
/// Maximum payload size in UTF-8 bytes.
pub const MAX_PAYLOAD: usize = 1024;
/// Absolute delivery budget stamped by the source transaction.
pub const DELIVERY_MS: i64 = 300_000;
/// Workflow retry rounds; native lease redelivery does not consume another round.
pub const MAX_ROUNDS: u32 = 3;

macro_rules! fields {
    ($name:ident {$($field:ident:$ty:ty),* $(,)?}) => {
        impl WireValue for $name {
            fn encode(&self, e:&mut BoundedEncoder)->Result<(),CodecError> {
                $(self.$field.encode(e)?;)* Ok(())
            }
            fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError> {
                Ok(Self {$($field:<$ty>::decode(d)?),*})
            }
        }
    };
}

/// Canonical subscription or topic key: lowercase ASCII, digits, internal hyphens.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Key(String);
impl Key {
    /// Validates 1..64 canonical bytes; no normalization aliases are accepted.
    pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
        let value = value.into();
        let bytes = value.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 64
            || bytes[0] == b'-'
            || bytes[bytes.len() - 1] == b'-'
            || !bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        {
            return Err(CodecError::Invalid("invalid subscription or topic key"));
        }
        Ok(Self(value))
    }
    /// Canonical routing and SQL value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for Key {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}
impl From<Key> for String {
    fn from(v: Key) -> Self {
        v.0
    }
}
impl WireValue for Key {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
    }
}

/// Validated numeric loopback endpoint owned by this local reference deployment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Endpoint(String);
impl Endpoint {
    /// Accepts only the canonical spelling `http://127.0.0.1:PORT/deliver`.
    pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
        let value = value.into();
        if value.len() > 256 {
            return Err(CodecError::Limit);
        }
        let parsed = url::Url::parse(&value)
            .map_err(|_| CodecError::Invalid("invalid receiver endpoint"))?;
        let port = parsed.port().ok_or(CodecError::Invalid(
            "receiver endpoint requires explicit port",
        ))?;
        if port == 0 || value != format!("http://127.0.0.1:{port}/deliver") {
            return Err(CodecError::Invalid(
                "receiver endpoint must be canonical numeric loopback",
            ));
        }
        Ok(Self(value))
    }
    /// Frozen HTTP destination, with redirects and proxies disabled by the client.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for Endpoint {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}
impl From<Endpoint> for String {
    fn from(v: Endpoint) -> Self {
        v.0
    }
}
impl WireValue for Endpoint {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
    }
}

/// Nonzero permanent source event identity; business replay is independent of request deduplication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "[u8;16]", into = "[u8;16]")]
pub struct EventId([u8; 16]);
impl EventId {
    /// Constructs an explicit nonzero UUID-sized identifier.
    pub fn from_bytes(value: [u8; 16]) -> Result<Self, CodecError> {
        if value == [0; 16] {
            return Err(CodecError::Invalid("event identity must be nonzero"));
        }
        Ok(Self(value))
    }
    /// Canonical permanent identity bytes.
    pub fn bytes(self) -> [u8; 16] {
        self.0
    }
}
impl TryFrom<[u8; 16]> for EventId {
    type Error = CodecError;
    fn try_from(v: [u8; 16]) -> Result<Self, Self::Error> {
        Self::from_bytes(v)
    }
}
impl From<EventId> for [u8; 16] {
    fn from(v: EventId) -> Self {
        v.0
    }
}
impl WireValue for EventId {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.0)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::from_bytes(
            d.read_bytes()?
                .try_into()
                .map_err(|_| CodecError::Invalid("invalid event identity length"))?,
        )
    }
}

/// Revision-controlled source subscription. Disabling preserves permanent identity and history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    /// Permanent subscriber key.
    pub id: Key,
    /// Exact topic selected for future events.
    pub topic: Key,
    /// Destination frozen into each subsequently published ticket.
    pub endpoint: Endpoint,
    /// Whether future events include this subscriber.
    pub enabled: bool,
    /// Positive monotonically increasing business revision.
    pub revision: i64,
}
fields!(Subscription {
    id: Key,
    topic: Key,
    endpoint: Endpoint,
    enabled: bool,
    revision: i64
});

/// Application source mutations; source time is stamped inside the durable command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// Creates at revision zero or changes at the exact observed revision.
    Subscribe {
        /// Canonical permanent key.
        id: Key,
        /// Topic for future publications.
        topic: Key,
        /// Explicit numeric loopback receiver.
        endpoint: Endpoint,
        /// Disable without deleting replay protection.
        enabled: bool,
        /// Zero creates; otherwise exact revision is required.
        expected_revision: i64,
    },
    /// Accepts once per event identity and snapshots currently enabled subscribers.
    Publish {
        /// Permanent event identity.
        id: EventId,
        /// Canonical topic.
        topic: Key,
        /// Nonempty 1..1024 UTF-8 bytes; never interpreted as SQL.
        payload: String,
    },
}
impl WireValue for Action {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Subscribe {
                id,
                topic,
                endpoint,
                enabled,
                expected_revision,
            } => {
                1u8.encode(e)?;
                id.encode(e)?;
                topic.encode(e)?;
                endpoint.encode(e)?;
                enabled.encode(e)?;
                expected_revision.encode(e)
            }
            Self::Publish { id, topic, payload } => {
                2u8.encode(e)?;
                id.encode(e)?;
                topic.encode(e)?;
                payload.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Subscribe {
                id: Key::decode(d)?,
                topic: Key::decode(d)?,
                endpoint: Endpoint::decode(d)?,
                enabled: bool::decode(d)?,
                expected_revision: i64::decode(d)?,
            }),
            2 => Ok(Self::Publish {
                id: EventId::decode(d)?,
                topic: Key::decode(d)?,
                payload: String::decode(d)?,
            }),
            _ => Err(CodecError::Invalid("unknown publisher action")),
        }
    }
}

/// Immutable capability carried from SQL intent through Workflow and HTTP.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryTicket {
    /// Tenant and application scoped publisher Cell ID.
    pub source_cell: Vec<u8>,
    /// Permanent source event identity.
    pub event_id: EventId,
    /// Subscriber selected in the publishing transaction.
    pub subscription: Key,
    /// Exact subscription revision at publication.
    pub subscription_revision: i64,
    /// Frozen topic.
    pub topic: Key,
    /// Frozen payload.
    pub payload: String,
    /// Frozen receiver destination.
    pub endpoint: Endpoint,
    /// Recorded source logical time.
    pub created_at_ms: i64,
    /// Absolute source time plus delivery budget.
    pub deadline_ms: i64,
}
impl DeliveryTicket {
    /// Validates immutable bounds before creating any external work.
    pub fn validate(&self) -> Result<(), CodecError> {
        if self.source_cell.len() != 32
            || self.source_cell.iter().all(|b| *b == 0)
            || self.subscription_revision <= 0
            || self.payload.is_empty()
            || self.payload.len() > MAX_PAYLOAD
            || self.created_at_ms < 0
            || self.created_at_ms.checked_add(DELIVERY_MS) != Some(self.deadline_ms)
        {
            return Err(CodecError::Invalid("invalid frozen delivery ticket"));
        }
        Ok(())
    }
    /// Stable business delivery identity, shared by every Activity and HTTP retry.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.webhook.delivery.v1\0");
        hash.update(&self.source_cell);
        hash.update(&self.event_id.bytes());
        hash.update(&(self.subscription.as_str().len() as u32).to_be_bytes());
        hash.update(self.subscription.as_str().as_bytes());
        hash.update(&self.subscription_revision.to_be_bytes());
        *hash.finalize().as_bytes()
    }
    /// Canonical HTTP `Idempotency-Key` header.
    pub fn key_hex(&self) -> String {
        blake3::Hash::from_bytes(self.key()).to_hex().to_string()
    }
    /// Digest of the canonical v1 wire ticket, independent of HTTP JSON field order.
    pub fn content_digest(&self) -> cellule_runtime::Result<String> {
        Ok(blake3::hash(&encode_wire(self, 4096)?).to_hex().to_string())
    }
}
impl WireValue for DeliveryTicket {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.source_cell.encode(e)?;
        self.event_id.encode(e)?;
        self.subscription.encode(e)?;
        self.subscription_revision.encode(e)?;
        self.topic.encode(e)?;
        self.payload.encode(e)?;
        self.endpoint.encode(e)?;
        self.created_at_ms.encode(e)?;
        self.deadline_ms.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            source_cell: Vec::<u8>::decode(d)?,
            event_id: EventId::decode(d)?,
            subscription: Key::decode(d)?,
            subscription_revision: i64::decode(d)?,
            topic: Key::decode(d)?,
            payload: String::decode(d)?,
            endpoint: Endpoint::decode(d)?,
            created_at_ms: i64::decode(d)?,
            deadline_ms: i64::decode(d)?,
        };
        value.validate()?;
        Ok(value)
    }
}

/// Source ticket and native source Effect ID; neither is a destination acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedDelivery {
    /// Immutable subscriber snapshot.
    pub ticket: DeliveryTicket,
    /// Source receipt domain Effect ID, exactly 32 bytes.
    pub effect_id: Vec<u8>,
}
fields!(PublishedDelivery{ticket:DeliveryTicket,effect_id:Vec<u8>});
/// Permanent accepted event and its original fan-out snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedEvent {
    /// Immutable event identity.
    pub id: EventId,
    /// Immutable topic.
    pub topic: Key,
    /// Immutable payload.
    pub payload: String,
    /// Ordered original subscriber snapshot, at most 16.
    pub deliveries: Vec<PublishedDelivery>,
}
impl PublishedEvent {
    fn validate(&self) -> Result<(), CodecError> {
        if self.payload.is_empty()
            || self.payload.len() > MAX_PAYLOAD
            || self.deliveries.len() > MAX_SUBSCRIBERS
        {
            return Err(CodecError::Invalid("invalid permanent published event"));
        }
        let mut previous: Option<&Key> = None;
        for delivery in &self.deliveries {
            delivery.ticket.validate()?;
            if delivery.effect_id.len() != 32
                || delivery.ticket.event_id != self.id
                || delivery.ticket.topic != self.topic
                || delivery.ticket.payload != self.payload
                || previous.is_some_and(|key| key >= &delivery.ticket.subscription)
            {
                return Err(CodecError::Invalid(
                    "delivery snapshot differs from published event",
                ));
            }
            previous = Some(&delivery.ticket.subscription);
        }
        Ok(())
    }
}
impl WireValue for PublishedEvent {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.id.encode(e)?;
        self.topic.encode(e)?;
        self.payload.encode(e)?;
        e.write_count(self.deliveries.len())?;
        for item in &self.deliveries {
            item.encode(e)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let id = EventId::decode(d)?;
        let topic = Key::decode(d)?;
        let payload = String::decode(d)?;
        let count = d.read_count()?;
        if count > MAX_SUBSCRIBERS {
            return Err(CodecError::Limit);
        }
        let deliveries = (0..count)
            .map(|_| PublishedDelivery::decode(d))
            .collect::<Result<_, _>>()?;
        let value = Self {
            id,
            topic,
            payload,
            deliveries,
        };
        value.validate()?;
        Ok(value)
    }
}

macro_rules! tags {
    ($name:ident {$($variant:ident=$tag:literal),+ $(,)?})=>{
        impl WireValue for $name {
            fn encode(&self,e:&mut BoundedEncoder)->Result<(),CodecError>{(match self{$(Self::$variant=>$tag as u8),+}).encode(e)}
            fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError>{match u8::decode(d)?{$($tag=>Ok(Self::$variant),)+_=>Err(CodecError::Invalid("unknown v1 domain tag"))}}
        }
    };
}
/// Durable publisher business result, including retained rejections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Subscription created or changed.
    Subscribed,
    /// Identical subscription at the supplied revision.
    Unchanged,
    /// First accepted event and intents.
    Published,
    /// Business replay with original snapshot and no new intents.
    ExistingEvent,
    /// Revision or event content differs.
    Conflict,
    /// Updating a missing subscription.
    NotFound,
    /// Invalid domain input.
    Invalid,
    /// Permanent namespace bound reached.
    Capacity,
}
tags!(Decision{Subscribed=1,Unchanged=2,Published=3,ExistingEvent=4,Conflict=5,NotFound=6,Invalid=7,Capacity=8});
impl Decision {
    pub(crate) fn success(self) -> bool {
        matches!(
            self,
            Self::Subscribed | Self::Unchanged | Self::Published | Self::ExistingEvent
        )
    }
}
/// Source response. A success confirms intent publication, not HTTP delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    /// Durable source decision.
    pub decision: Decision,
    /// Subscription facts for registration commands.
    pub subscription: Option<Subscription>,
    /// Original publication facts for accepted events.
    pub event: Option<PublishedEvent>,
}
fields!(Outcome{decision:Decision,subscription:Option<Subscription>,event:Option<PublishedEvent>});

/// Local receiver fault policy, persisted independently of the publisher.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiverMode {
    /// Apply once and acknowledge.
    Good,
    /// First request applies durably, then the HTTP server closes without a reply.
    DropOnce,
    /// First request returns 503 without applying; later requests succeed.
    TransientOnce,
    /// Every request returns 422 without applying.
    Terminal,
}
tags!(ReceiverMode{Good=0,DropOnce=1,TransientOnce=2,Terminal=3});
/// Administrative receiver policy for a synthetic subscriber.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverPolicy {
    /// Subscriber whose future first requests exercise this policy.
    pub subscription: Key,
    /// Durable fault behavior.
    pub mode: ReceiverMode,
}
fields!(ReceiverPolicy {
    subscription: Key,
    mode: ReceiverMode
});
/// Permanent receiver idempotency record, retained even when no action was applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverRecord {
    /// Exact immutable delivery capability.
    pub ticket: DeliveryTicket,
    /// Accepted HTTP deliveries, saturating at 20 to bound untrusted repeat traffic.
    pub requests: u32,
    /// Whether the single synthetic application action committed.
    pub applied: bool,
}
fields!(ReceiverRecord {
    ticket: DeliveryTicket,
    requests: u32,
    applied: bool
});
/// Typed receiver reply after durable publication; server faults happen after this result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverOutcome {
    /// HTTP status selected by durable receiver facts.
    pub status: u32,
    /// Close the actual connection without serializing a response.
    pub drop_reply: bool,
    /// Permanent record; absent on conflicting or exhausted admission.
    pub record: Option<ReceiverRecord>,
}
fields!(ReceiverOutcome{status:u32,drop_reply:bool,record:Option<ReceiverRecord>});
/// HTTP acknowledgement proving this exact receiver action, separate from source receipts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgement {
    /// Exact immutable application delivery key.
    pub key: String,
    /// Canonical ticket digest.
    pub content_digest: String,
    /// Synthetic action count; the protocol requires exactly one.
    pub applied_count: u32,
}
/// Classification of a recorded HTTP attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    /// Valid exact acknowledgement.
    Delivered,
    /// Transport failure or transient HTTP status.
    Retryable,
    /// Definitive HTTP rejection or invalid acknowledgement protocol.
    Permanent,
}
/// One Workflow round, including native Activity redelivery attempt and uncertainty.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpAttempt {
    /// Logical Workflow round, 1..3.
    pub round: u32,
    /// Native Activity lease attempt, 1..20.
    pub native_attempt: u32,
    /// Retry decision frozen in the completion event.
    pub classification: Classification,
    /// HTTP status when headers were received.
    pub status: Option<u32>,
    /// Bounded original error chain or response classification.
    pub details: String,
    /// Delivery may have applied despite missing acknowledgement.
    pub may_have_applied: bool,
    /// Exact verified HTTP receipt when delivered.
    pub acknowledgement: Option<Acknowledgement>,
}
impl HttpAttempt {
    pub(crate) fn validate(&self, ticket: &DeliveryTicket) -> cellule_runtime::Result<()> {
        if !(1..=MAX_ROUNDS).contains(&self.round)
            || !(1..=20).contains(&self.native_attempt)
            || self.details.len() > 512
            || self.status.is_some_and(|v| !(100..=599).contains(&v))
        {
            return Err(Error::Command("invalid recorded HTTP attempt"));
        }
        match (&self.classification, &self.acknowledgement) {
            (Classification::Delivered, Some(ack))
                if self.status == Some(200)
                    && self.may_have_applied
                    && ack.key == ticket.key_hex()
                    && ack.content_digest == ticket.content_digest()?
                    && ack.applied_count == 1 =>
            {
                Ok(())
            }
            (Classification::Retryable | Classification::Permanent, None) => Ok(()),
            _ => Err(Error::Command(
                "HTTP acknowledgement differs from frozen ticket",
            )),
        }
    }
}
/// Observable delivery phase. Exhaustion never proves the receiver did not apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// One native Activity is pending or leased.
    InFlight,
    /// Bounded deterministic retry Timer is pending.
    Backoff,
    /// Exact acknowledgement is durable.
    Delivered,
    /// Definitive HTTP rejection or protocol failure.
    Failed,
    /// All logical rounds consumed; final receiver action may remain uncertain.
    Exhausted,
    /// Absolute source deadline reached; prior external action may remain uncertain.
    Expired,
}
/// Definition-owned bounded state. Only recorded events and logical time drive transitions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryState {
    /// Immutable source subscription snapshot.
    pub ticket: DeliveryTicket,
    /// Business phase, independent of native serving status.
    pub phase: Phase,
    /// Current logical round, or zero before any HTTP action at an expired start.
    pub round: u32,
    /// Exact native action ID expected next.
    pub action_id: Option<Vec<u8>>,
    /// At most three completed logical rounds.
    pub attempts: Vec<HttpAttempt>,
    /// Bounded native failure evidence. Such failure does not prove external absence.
    pub failure: Option<String>,
}
impl DeliveryState {
    /// Conservatively reports whether any recorded attempt may have applied.
    pub fn may_have_applied(&self) -> bool {
        self.phase == Phase::InFlight
            || self.failure.is_some()
            || self.attempts.iter().any(|v| v.may_have_applied)
    }
    pub(crate) fn validate(&self) -> cellule_runtime::Result<()> {
        self.ticket.validate()?;
        if self.round > MAX_ROUNDS
            || self.attempts.len() > MAX_ROUNDS as usize
            || self.failure.as_ref().is_some_and(|v| v.len() > 512)
            || self.action_id.as_ref().is_some_and(|v| v.len() != 16)
            || matches!(self.phase, Phase::InFlight | Phase::Backoff) != self.action_id.is_some()
            || (self.round == 0 && (self.phase != Phase::Expired || !self.attempts.is_empty()))
        {
            return Err(Error::Command("invalid delivery Workflow state"));
        }
        for (index, attempt) in self.attempts.iter().enumerate() {
            attempt.validate(&self.ticket)?;
            if attempt.round != index as u32 + 1 {
                return Err(Error::Command("HTTP attempt history is not contiguous"));
            }
        }
        let count = self.attempts.len() as u32;
        let last = self.attempts.last().map(|attempt| attempt.classification);
        let prefix = self
            .attempts
            .iter()
            .take(self.attempts.len().saturating_sub(1));
        if prefix
            .into_iter()
            .any(|attempt| attempt.classification != Classification::Retryable)
        {
            return Err(Error::Command(
                "terminal HTTP attempt precedes another round",
            ));
        }
        let consistent = match self.phase {
            Phase::InFlight => {
                self.round > 0
                    && count + 1 == self.round
                    && self.failure.is_none()
                    && last.is_none_or(|value| value == Classification::Retryable)
            }
            Phase::Backoff => {
                self.round < MAX_ROUNDS
                    && count == self.round
                    && last == Some(Classification::Retryable)
                    && self.failure.is_none()
            }
            Phase::Delivered => {
                count == self.round
                    && last == Some(Classification::Delivered)
                    && self.failure.is_none()
            }
            Phase::Failed if self.failure.is_some() => {
                self.round > 0
                    && count + 1 == self.round
                    && last.is_none_or(|value| value == Classification::Retryable)
            }
            Phase::Failed => count == self.round && last == Some(Classification::Permanent),
            Phase::Exhausted => {
                self.round == MAX_ROUNDS
                    && count == self.round
                    && last == Some(Classification::Retryable)
                    && self.failure.is_none()
            }
            Phase::Expired => {
                count == self.round
                    && self.failure.is_none()
                    && last.is_none_or(|value| value == Classification::Retryable)
            }
        };
        if !consistent {
            return Err(Error::Command(
                "delivery phase differs from bounded attempt history",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpInput {
    pub ticket: DeliveryTicket,
    pub round: u32,
}

pub(crate) fn encode_wire<T: WireValue>(value: &T, limit: u32) -> cellule_runtime::Result<Vec<u8>> {
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}
pub(crate) fn decode_wire<T: WireValue>(bytes: &[u8], limit: u32) -> cellule_runtime::Result<T> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
pub(crate) fn encode_json<T: Serialize>(
    value: &T,
    limit: usize,
) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > limit {
        return Err(CodecError::Limit.into());
    }
    Ok(bytes)
}
pub(crate) fn decode_json<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
) -> cellule_runtime::Result<T> {
    if bytes.len() > limit {
        return Err(CodecError::Limit.into());
    }
    Ok(serde_json::from_slice(bytes)?)
}

/// Complete bounded publisher subscriber roster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionList {
    /// At most sixteen permanent subscription keys, ordered canonically.
    pub subscriptions: Vec<Subscription>,
}
impl WireValue for SubscriptionList {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.subscriptions.len() > MAX_SUBSCRIBERS {
            return Err(CodecError::Limit);
        }
        e.write_count(self.subscriptions.len())?;
        for value in &self.subscriptions {
            value.encode(e)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = d.read_count()?;
        if count > MAX_SUBSCRIBERS {
            return Err(CodecError::Limit);
        }
        Ok(Self {
            subscriptions: (0..count)
                .map(|_| Subscription::decode(d))
                .collect::<Result<_, _>>()?,
        })
    }
}
