use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};
/// Permanent hold identities retained per event.
pub const MAX_HOLDS: i64 = 1024;
/// Seats per event in the local reference profile.
pub const MAX_SEATS: u32 = 100;
/// Maximum new hold lifetime, one hour.
pub const MAX_HOLD_MS: i64 = 3_600_000;
/// Stable canonical event slug, independent of capacity and serving node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EventKey(String);
impl EventKey {
    /// Accepts 1..64 lowercase ASCII letters, digits, and internal hyphens.
    pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(CodecError::Invalid(
                "event key must be a canonical lowercase slug",
            ));
        }
        Ok(Self(value))
    }
    /// Canonical version-one bytes used by framework entity partitioning.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    /// Event's canonical display value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for EventKey {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}
impl From<EventKey> for String {
    fn from(v: EventKey) -> Self {
        v.0
    }
}
impl cellule_app::CellKey for EventKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl WireValue for EventKey {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
    }
}
/// Permanent event-local business identity, separate from a mutation request ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HoldId([u8; 16]);
impl HoldId {
    /// Accepts a nonnil canonical lowercase hyphenated UUID.
    pub fn parse(value: &str) -> Result<Self, CodecError> {
        let uuid =
            uuid::Uuid::parse_str(value).map_err(|_| CodecError::Invalid("invalid hold UUID"))?;
        if uuid.is_nil() || uuid.to_string() != value {
            return Err(CodecError::Invalid(
                "hold UUID must be nonnil and canonical",
            ));
        }
        Ok(Self(*uuid.as_bytes()))
    }
    /// Constructs a nonnil identity from its stable 16 bytes.
    pub fn from_bytes(value: [u8; 16]) -> Result<Self, CodecError> {
        if value == [0; 16] {
            return Err(CodecError::Invalid("hold UUID must be nonnil"));
        }
        Ok(Self(value))
    }
    /// Canonical database and wire identity bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}
impl TryFrom<String> for HoldId {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::parse(&v)
    }
}
impl From<HoldId> for String {
    fn from(v: HoldId) -> Self {
        uuid::Uuid::from_bytes(v.0).to_string()
    }
}
impl WireValue for HoldId {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.0)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::from_bytes(
            d.read_bytes()?
                .try_into()
                .map_err(|_| CodecError::Invalid("hold identity requires 16 bytes"))?,
        )
    }
}
/// Stable canonical buyer slug, independent of capacity and serving node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BuyerKey(String);
impl BuyerKey {
    /// Accepts 1..64 lowercase ASCII letters, digits, and internal hyphens.
    pub fn new(value: impl Into<String>) -> Result<Self, CodecError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(CodecError::Invalid(
                "buyer key must be a canonical lowercase slug",
            ));
        }
        Ok(Self(value))
    }
    /// Canonical version-one bytes used by framework entity partitioning.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    /// Buyer's canonical display value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for BuyerKey {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}
impl From<BuyerKey> for String {
    fn from(v: BuyerKey) -> Self {
        v.0
    }
}
impl cellule_app::CellKey for BuyerKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl WireValue for BuyerKey {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
    }
}
/// One local inventory transition; native request identity is separate from hold identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// Initialize an absent event; existing inventory cannot be reset.
    Initialize {
        /// Consecutive seats numbered 1..seats.
        seats: u32,
    },
    /// Allocate one currently free seat and atomically request its deadline Workflow.
    Hold {
        /// Permanent event-local business identity.
        id: HoldId,
        /// Seat number, 1..100 and within the initialized event.
        seat: u32,
        /// Application-authorized buyer capability.
        buyer: BuyerKey,
        /// Frozen absolute deadline; the server requires a future time within one hour.
        deadline_ms: i64,
    },
    /// Confirm before the absolute deadline, checking exact ownership and generation.
    Confirm {
        /// Exact hold identity.
        id: HoldId,
        /// Observed seat generation.
        generation: i64,
        /// Application-authorized buyer capability.
        buyer: BuyerKey,
    },
    /// Cancel an unconfirmed hold; confirmation is permanent in this domain.
    Cancel {
        /// Exact hold identity.
        id: HoldId,
        /// Observed seat generation.
        generation: i64,
        /// Application-authorized buyer capability.
        buyer: BuyerKey,
    },
}
impl Action {
    /// Checks static admission bounds. The handler also checks recorded logical time.
    pub fn validate(&self) -> Result<(), CodecError> {
        let valid = match self {
            Self::Initialize { seats } => (1..=MAX_SEATS).contains(seats),
            Self::Hold {
                seat, deadline_ms, ..
            } => (1..=MAX_SEATS).contains(seat) && *deadline_ms > 0,
            Self::Confirm { generation, .. } | Self::Cancel { generation, .. } => *generation > 0,
        };
        if valid {
            Ok(())
        } else {
            Err(CodecError::Invalid("invalid seat, generation, or deadline"))
        }
    }
}
impl WireValue for Action {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Initialize { seats } => {
                1_u8.encode(e)?;
                seats.encode(e)
            }
            Self::Hold {
                id,
                seat,
                buyer,
                deadline_ms,
            } => {
                2_u8.encode(e)?;
                id.encode(e)?;
                seat.encode(e)?;
                buyer.encode(e)?;
                deadline_ms.encode(e)
            }
            Self::Confirm {
                id,
                generation,
                buyer,
            } => {
                3_u8.encode(e)?;
                id.encode(e)?;
                generation.encode(e)?;
                buyer.encode(e)
            }
            Self::Cancel {
                id,
                generation,
                buyer,
            } => {
                4_u8.encode(e)?;
                id.encode(e)?;
                generation.encode(e)?;
                buyer.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Initialize {
                seats: u32::decode(d)?,
            }),
            2 => Ok(Self::Hold {
                id: HoldId::decode(d)?,
                seat: u32::decode(d)?,
                buyer: BuyerKey::decode(d)?,
                deadline_ms: i64::decode(d)?,
            }),
            3 => Ok(Self::Confirm {
                id: HoldId::decode(d)?,
                generation: i64::decode(d)?,
                buyer: BuyerKey::decode(d)?,
            }),
            4 => Ok(Self::Cancel {
                id: HoldId::decode(d)?,
                generation: i64::decode(d)?,
                buyer: BuyerKey::decode(d)?,
            }),
            _ => Err(CodecError::Invalid("unknown reservation action")),
        }
    }
}
/// The server verifies this event against its selected entity target before reading SQL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    /// Authorized event scope.
    pub event: EventKey,
    /// Frozen local operation.
    pub action: Action,
}
impl WireValue for Change {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.event.encode(e)?;
        self.action.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            event: EventKey::decode(d)?,
            action: Action::decode(d)?,
        })
    }
}
/// Immutable expiration capability copied from the committed SQL hold.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadlineTicket {
    /// Source event entity.
    pub event: EventKey,
    /// Permanent hold ID.
    pub id: HoldId,
    /// Original seat number.
    pub seat: u32,
    /// Original seat generation.
    pub generation: i64,
    /// Original absolute deadline.
    pub deadline_ms: i64,
}
impl DeadlineTicket {
    /// Stable 32-byte Workflow key, scoped by both event and hold identity.
    pub fn workflow_id(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.reservation.deadline.v1\0");
        hash.update(&(self.event.as_bytes().len() as u32).to_be_bytes());
        hash.update(self.event.as_bytes());
        hash.update(self.id.as_bytes());
        *hash.finalize().as_bytes()
    }
    pub(crate) fn validate(&self) -> Result<(), CodecError> {
        if !(1..=MAX_SEATS).contains(&self.seat) || self.generation <= 0 || self.deadline_ms <= 0 {
            return Err(CodecError::Invalid("invalid deadline ticket"));
        }
        Ok(())
    }
}
macro_rules! wire_fields {
    ($type:ty {$($field:ident : $field_type:ty),+ $(,)?}) => {
        impl WireValue for $type {
            fn encode(&self,e:&mut BoundedEncoder)->Result<(),CodecError>{$ (self.$field.encode(e)?;)+ Ok(())}
            fn decode(d:&mut BoundedDecoder<'_>)->Result<Self,CodecError>{Ok(Self{$($field:<$field_type>::decode(d)?),+})}
        }
    };
}
wire_fields!(DeadlineTicket {
    event: EventKey,
    id: HoldId,
    seat: u32,
    generation: i64,
    deadline_ms: i64
});
/// Durable inventory state; terminal records and identities remain retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldState {
    /// Seat is temporarily held; time alone does not mutate this recorded state.
    Held,
    /// Buyer confirmed before the deadline; the seat cannot be released by a timeout.
    Confirmed,
    /// Buyer cancelled the hold.
    Cancelled,
    /// Deadline expiration released this hold.
    Expired,
}
impl WireValue for HoldState {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        (match self {
            Self::Held => 0_u8,
            Self::Confirmed => 1,
            Self::Cancelled => 2,
            Self::Expired => 3,
        })
        .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            0 => Ok(Self::Held),
            1 => Ok(Self::Confirmed),
            2 => Ok(Self::Cancelled),
            3 => Ok(Self::Expired),
            _ => Err(CodecError::Invalid("invalid hold state")),
        }
    }
}
/// Current hold facts and the native start intent published in the same transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hold {
    /// Frozen allocation and expiration capability.
    pub ticket: DeadlineTicket,
    /// Buyer bound when the hold was created.
    pub buyer: BuyerKey,
    /// Current recorded state.
    pub state: HoldState,
    /// Source Effect identity; native delivery status is a separate observation.
    pub start_effect: Vec<u8>,
}
impl WireValue for Hold {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.ticket.validate()?;
        if self.start_effect.len() != 32 {
            return Err(CodecError::Invalid("invalid start effect"));
        }
        self.ticket.encode(e)?;
        self.buyer.encode(e)?;
        self.state.encode(e)?;
        self.start_effect.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            ticket: DeadlineTicket::decode(d)?,
            buyer: BuyerKey::decode(d)?,
            state: HoldState::decode(d)?,
            start_effect: Vec::<u8>::decode(d)?,
        };
        value.ticket.validate()?;
        if value.start_effect.len() != 32 {
            return Err(CodecError::Invalid("invalid start effect"));
        }
        Ok(value)
    }
}
/// Explicit business decisions; invalid and competing commands remain durable rejections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Inventory initialized.
    Initialized,
    /// New hold acquired.
    Held,
    /// Matching permanent business ID already exists; inspect its current state.
    ExistingHold,
    /// Hold confirmed.
    Confirmed,
    /// Already confirmed; no new allocation.
    AlreadyConfirmed,
    /// Hold cancelled.
    Cancelled,
    /// Already cancelled; no additional release.
    AlreadyCancelled,
    /// Bounds or selected target are invalid.
    Invalid,
    /// Event or hold does not exist.
    NotFound,
    /// Event cannot be reset.
    AlreadyInitialized,
    /// Immutable hold fields or generation differ.
    Conflict,
    /// Another active or confirmed hold occupies this seat.
    Occupied,
    /// Hold deadline passed; confirmation is forbidden even when timer delivery is delayed.
    Expired,
    /// Opposite terminal operation is forbidden.
    Closed,
    /// Permanent hold history is full.
    Capacity,
    /// Buyer capability does not own this hold.
    Forbidden,
}
impl Decision {
    pub(crate) fn success(self) -> bool {
        matches!(
            self,
            Self::Initialized
                | Self::Held
                | Self::ExistingHold
                | Self::Confirmed
                | Self::AlreadyConfirmed
                | Self::Cancelled
                | Self::AlreadyCancelled
        )
    }
}
impl WireValue for Decision {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        (match self {
            Self::Initialized => 1_u8,
            Self::Held => 2,
            Self::ExistingHold => 3,
            Self::Confirmed => 4,
            Self::AlreadyConfirmed => 5,
            Self::Cancelled => 6,
            Self::AlreadyCancelled => 7,
            Self::Invalid => 8,
            Self::NotFound => 9,
            Self::AlreadyInitialized => 10,
            Self::Conflict => 11,
            Self::Occupied => 12,
            Self::Expired => 13,
            Self::Closed => 14,
            Self::Capacity => 15,
            Self::Forbidden => 16,
        })
        .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Initialized),
            2 => Ok(Self::Held),
            3 => Ok(Self::ExistingHold),
            4 => Ok(Self::Confirmed),
            5 => Ok(Self::AlreadyConfirmed),
            6 => Ok(Self::Cancelled),
            7 => Ok(Self::AlreadyCancelled),
            8 => Ok(Self::Invalid),
            9 => Ok(Self::NotFound),
            10 => Ok(Self::AlreadyInitialized),
            11 => Ok(Self::Conflict),
            12 => Ok(Self::Occupied),
            13 => Ok(Self::Expired),
            14 => Ok(Self::Closed),
            15 => Ok(Self::Capacity),
            16 => Ok(Self::Forbidden),
            _ => Err(CodecError::Invalid("unknown reservation decision")),
        }
    }
}
/// Consistent command facts, including native business rejection output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    /// Durable decision.
    pub decision: Decision,
    /// Selected hold facts, omitted for foreign buyer capabilities.
    pub hold: Option<Hold>,
}
wire_fields!(Outcome{decision:Decision,hold:Option<Hold>});
/// Current counters from one FIFO read of this event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inventory {
    /// Stable event identity.
    pub event: EventKey,
    /// Total seats.
    pub seats: u32,
    /// Currently recorded temporary allocations, including delayed expiration.
    pub held: u32,
    /// Permanent confirmed seats.
    pub confirmed: u32,
    /// Seats minus held and confirmed.
    pub available: u32,
    /// Actual domain changes, including expiration; native no-op publication is separate.
    pub revision: i64,
    /// Permanent hold history size.
    pub history_count: i64,
}
impl Inventory {
    fn validate(&self) -> Result<(), CodecError> {
        if !(1..=MAX_SEATS).contains(&self.seats)
            || self.revision <= 0
            || !(0..=MAX_HOLDS).contains(&self.history_count)
            || self
                .held
                .checked_add(self.confirmed)
                .and_then(|v| v.checked_add(self.available))
                != Some(self.seats)
        {
            return Err(CodecError::Invalid("invalid inventory facts"));
        }
        Ok(())
    }
}
impl WireValue for Inventory {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.event.encode(e)?;
        self.seats.encode(e)?;
        self.held.encode(e)?;
        self.confirmed.encode(e)?;
        self.available.encode(e)?;
        self.revision.encode(e)?;
        self.history_count.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            event: EventKey::decode(d)?,
            seats: u32::decode(d)?,
            held: u32::decode(d)?,
            confirmed: u32::decode(d)?,
            available: u32::decode(d)?,
            revision: i64::decode(d)?,
            history_count: i64::decode(d)?,
        };
        value.validate()?;
        Ok(value)
    }
}
/// Bounded current hold history request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    /// Exclusive permanent hold UUID cursor.
    pub after: Option<HoldId>,
    /// Number of records, 1..100.
    pub limit: u32,
}
wire_fields!(PageRequest{after:Option<HoldId>,limit:u32});
/// One coherent source inventory observation with a bounded hold page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    /// Inventory facts, absent before initialization.
    pub inventory: Option<Inventory>,
    /// Permanent history ordered by UUID bytes.
    pub holds: Vec<Hold>,
    /// Next exclusive cursor only when more records exist.
    pub next: Option<HoldId>,
}
impl WireValue for Page {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.inventory.encode(e)?;
        if self.holds.len() > 100 {
            return Err(CodecError::Limit);
        }
        e.write_count(self.holds.len())?;
        for hold in &self.holds {
            hold.encode(e)?;
        }
        self.next.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let inventory = Option::<Inventory>::decode(d)?;
        let count = d.read_count()?;
        if count > 100 {
            return Err(CodecError::Limit);
        }
        let mut holds = Vec::with_capacity(count);
        for _ in 0..count {
            holds.push(Hold::decode(d)?);
        }
        Ok(Self {
            inventory,
            holds,
            next: Option::<HoldId>::decode(d)?,
        })
    }
}
/// Definition-owned durable timer state; completion publishes intent, not destination proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadlineState {
    /// Original committed capability.
    pub ticket: DeadlineTicket,
    /// Exact native Timer action ID, absent if the start was already overdue.
    pub timer_id: Option<Vec<u8>>,
    /// Recorded logical firing time; absent while the Timer is pending.
    pub fired_at_ms: Option<i64>,
}
impl DeadlineState {
    pub(crate) fn validate(&self) -> Result<(), CodecError> {
        self.ticket.validate()?;
        if self.timer_id.as_ref().is_some_and(|v| v.len() != 16)
            || self
                .fired_at_ms
                .is_some_and(|time| time < self.ticket.deadline_ms)
            || (self.timer_id.is_none() && self.fired_at_ms.is_none())
        {
            return Err(CodecError::Invalid("invalid deadline workflow state"));
        }
        Ok(())
    }
}
impl WireValue for DeadlineState {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.ticket.encode(e)?;
        self.timer_id.encode(e)?;
        self.fired_at_ms.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            ticket: DeadlineTicket::decode(d)?,
            timer_id: Option::<Vec<u8>>::decode(d)?,
            fired_at_ms: Option::<i64>::decode(d)?,
        };
        value.validate()?;
        Ok(value)
    }
}
/// Internal signed expiration input; consumers must authorize the deadline worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expiration {
    /// Exact original hold capability.
    pub ticket: DeadlineTicket,
    /// Workflow's recorded logical time, at or after its deadline.
    pub fired_at_ms: i64,
}
wire_fields!(Expiration {
    ticket: DeadlineTicket,
    fired_at_ms: i64
});
/// Conditional timeout outcomes; stale and terminal holds are harmless no-ops.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpirationOutcome {
    /// This exact active generation was released.
    Expired,
    /// Original hold already ended or no longer occupies the seat.
    Unchanged,
    /// Capability differs from the committed ticket or logical deadline.
    Invalid,
}
impl WireValue for ExpirationOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        (match self {
            Self::Expired => 1_u8,
            Self::Unchanged => 2,
            Self::Invalid => 3,
        })
        .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Expired),
            2 => Ok(Self::Unchanged),
            3 => Ok(Self::Invalid),
            _ => Err(CodecError::Invalid("unknown expiration result")),
        }
    }
}
