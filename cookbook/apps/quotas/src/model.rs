use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use serde::{Deserialize, Serialize};
/// Largest allowance and individual reservation supported by the version-one domain.
pub const MAX_CREDITS: i64 = 1_000_000_000_000;
/// Permanent reservation records per customer; terminal identities are never recycled.
pub const MAX_RESERVATIONS: i64 = 1024;
/// Stable canonical customer slug, independent of allowance and serving node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CustomerKey(String);
impl CustomerKey {
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
                "customer key must be a canonical lowercase slug",
            ));
        }
        Ok(Self(value))
    }
    /// Canonical version-one bytes used by framework entity partitioning.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    /// Customer's canonical display value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for CustomerKey {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}
impl From<CustomerKey> for String {
    fn from(v: CustomerKey) -> Self {
        v.0
    }
}
impl cellule_app::CellKey for CustomerKey {
    fn canonical_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl WireValue for CustomerKey {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::new(String::decode(d)?)
    }
}
/// Permanent customer-local business identity, separate from a mutation request ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReservationId([u8; 16]);
impl ReservationId {
    /// Accepts a nonnil canonical lowercase hyphenated UUID.
    pub fn parse(value: &str) -> Result<Self, CodecError> {
        let uuid = uuid::Uuid::parse_str(value)
            .map_err(|_| CodecError::Invalid("invalid reservation UUID"))?;
        if uuid.is_nil() || uuid.to_string() != value {
            return Err(CodecError::Invalid(
                "reservation UUID must be nonnil and canonical",
            ));
        }
        Ok(Self(*uuid.as_bytes()))
    }
    /// Constructs a nonnil identity from its stable 16 bytes.
    pub fn from_bytes(value: [u8; 16]) -> Result<Self, CodecError> {
        if value == [0; 16] {
            return Err(CodecError::Invalid("reservation UUID must be nonnil"));
        }
        Ok(Self(value))
    }
    /// Canonical database and wire identity bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}
impl TryFrom<String> for ReservationId {
    type Error = CodecError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::parse(&v)
    }
}
impl From<ReservationId> for String {
    fn from(v: ReservationId) -> Self {
        uuid::Uuid::from_bytes(v.0).to_string()
    }
}
impl WireValue for ReservationId {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.0)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Self::from_bytes(
            d.read_bytes()?
                .try_into()
                .map_err(|_| CodecError::Invalid("reservation identity requires 16 bytes"))?,
        )
    }
}
/// One atomic domain operation; no external API calls occur inside its transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    /// Initializes an absent customer account.
    Open {
        /// Lifetime credit allowance, 0..MAX_CREDITS.
        allowance: i64,
    },
    /// Changes an allowance at its exact current account revision.
    SetAllowance {
        /// Revision observed by the administrator.
        expected_revision: i64,
        /// New allowance must cover consumed and reserved credits.
        allowance: i64,
    },
    /// Holds a fixed amount under a permanent business ID.
    Reserve {
        /// Reservation business identity.
        id: ReservationId,
        /// Positive credits to hold.
        credits: i64,
    },
    /// Spends all credits of an active reservation, once.
    Consume {
        /// Reservation to consume.
        id: ReservationId,
    },
    /// Returns all credits of an active reservation, once.
    Release {
        /// Reservation to release.
        id: ReservationId,
    },
}
impl Action {
    /// Validates request bounds before preparation; the handler repeats validation.
    pub fn validate(&self) -> Result<(), CodecError> {
        let valid = match self {
            Self::Open { allowance } => (0..=MAX_CREDITS).contains(allowance),
            Self::SetAllowance {
                expected_revision,
                allowance,
            } => {
                *expected_revision > 0
                    && *expected_revision < i64::MAX
                    && (0..=MAX_CREDITS).contains(allowance)
            }
            Self::Reserve { credits, .. } => (1..=MAX_CREDITS).contains(credits),
            Self::Consume { .. } | Self::Release { .. } => true,
        };
        if valid {
            Ok(())
        } else {
            Err(CodecError::Invalid(
                "quota action violates credit or revision bounds",
            ))
        }
    }
}
impl WireValue for Action {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Open { allowance } => {
                1_u8.encode(e)?;
                allowance.encode(e)
            }
            Self::SetAllowance {
                expected_revision,
                allowance,
            } => {
                2_u8.encode(e)?;
                expected_revision.encode(e)?;
                allowance.encode(e)
            }
            Self::Reserve { id, credits } => {
                3_u8.encode(e)?;
                id.encode(e)?;
                credits.encode(e)
            }
            Self::Consume { id } => {
                4_u8.encode(e)?;
                id.encode(e)
            }
            Self::Release { id } => {
                5_u8.encode(e)?;
                id.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Open {
                allowance: i64::decode(d)?,
            }),
            2 => Ok(Self::SetAllowance {
                expected_revision: i64::decode(d)?,
                allowance: i64::decode(d)?,
            }),
            3 => Ok(Self::Reserve {
                id: ReservationId::decode(d)?,
                credits: i64::decode(d)?,
            }),
            4 => Ok(Self::Consume {
                id: ReservationId::decode(d)?,
            }),
            5 => Ok(Self::Release {
                id: ReservationId::decode(d)?,
            }),
            _ => Err(CodecError::Invalid("unknown quota action tag")),
        }
    }
}
/// Scoped server input; the selected Cell must derive from this exact customer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    /// Authenticated application-selected customer.
    pub customer: CustomerKey,
    /// Immutable business action.
    pub action: Action,
}
impl WireValue for Change {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.customer.encode(e)?;
        self.action.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            customer: CustomerKey::decode(d)?,
            action: Action::decode(d)?,
        })
    }
}
/// Account facts from one transaction or FIFO source read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    /// Stable customer identity.
    pub customer: CustomerKey,
    /// Lifetime allowance; consumption is never reset by changing it.
    pub allowance: i64,
    /// Credits permanently spent by consumed reservations.
    pub consumed: i64,
    /// Credits held by active reservations.
    pub reserved: i64,
    /// Allowance minus consumption and active holds.
    pub available: i64,
    /// Monotonic revision of actual domain changes; idempotent no-ops leave it unchanged.
    pub revision: i64,
    /// Total permanent business records, including terminal reservations.
    pub reservation_count: i64,
}
impl Account {
    pub(crate) fn validate(&self) -> Result<(), CodecError> {
        if !(0..=MAX_CREDITS).contains(&self.allowance)
            || self.consumed < 0
            || self.reserved < 0
            || self.available < 0
            || self.revision <= 0
            || !(0..=MAX_RESERVATIONS).contains(&self.reservation_count)
            || self
                .allowance
                .checked_sub(self.consumed)
                .and_then(|v| v.checked_sub(self.reserved))
                != Some(self.available)
        {
            return Err(CodecError::Invalid("quota account invariant violated"));
        }
        Ok(())
    }
}
impl WireValue for Account {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.customer.encode(e)?;
        self.allowance.encode(e)?;
        self.consumed.encode(e)?;
        self.reserved.encode(e)?;
        self.available.encode(e)?;
        self.revision.encode(e)?;
        self.reservation_count.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let v = Self {
            customer: CustomerKey::decode(d)?,
            allowance: i64::decode(d)?,
            consumed: i64::decode(d)?,
            reserved: i64::decode(d)?,
            available: i64::decode(d)?,
            revision: i64::decode(d)?,
            reservation_count: i64::decode(d)?,
        };
        v.validate()?;
        Ok(v)
    }
}
/// Reservation state; terminal records cannot be reopened or reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationState {
    /// Credits are held.
    Active,
    /// Credits were spent.
    Consumed,
    /// Credits were returned.
    Released,
}
impl WireValue for ReservationState {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        (match self {
            Self::Active => 0_u8,
            Self::Consumed => 1,
            Self::Released => 2,
        })
        .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            0 => Ok(Self::Active),
            1 => Ok(Self::Consumed),
            2 => Ok(Self::Released),
            _ => Err(CodecError::Invalid("unknown reservation state")),
        }
    }
}
/// Fixed amount and permanent state for one business reservation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    /// Business identity.
    pub id: ReservationId,
    /// Original positive amount; immutable through settlement.
    pub credits: i64,
    /// Current terminal or active state.
    pub state: ReservationState,
}
impl WireValue for Reservation {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.id.encode(e)?;
        self.credits.encode(e)?;
        self.state.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let v = Self {
            id: ReservationId::decode(d)?,
            credits: i64::decode(d)?,
            state: ReservationState::decode(d)?,
        };
        if !(1..=MAX_CREDITS).contains(&v.credits) {
            return Err(CodecError::Invalid("invalid reservation credits"));
        }
        Ok(v)
    }
}
/// Stable business decision encoded alongside the transaction's observable facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Account was initialized.
    Opened,
    /// Allowance was changed at its expected revision.
    AllowanceChanged,
    /// A new business reservation was held.
    Reserved,
    /// Same ID and amount already exist, including terminal records; no new hold.
    ExistingReservation,
    /// Active reservation was spent.
    Consumed,
    /// Already spent; no additional consumption.
    AlreadyConsumed,
    /// Active reservation was returned.
    Released,
    /// Already returned; no additional credit.
    AlreadyReleased,
    /// Request violates bounds or selects the wrong customer Cell.
    Invalid,
    /// Account or reservation does not exist.
    NotFound,
    /// Customer was already initialized; opening cannot reset its balance.
    AlreadyOpen,
    /// Allowance revision or original reservation amount differs.
    Conflict,
    /// Available credits cannot cover a new hold.
    Insufficient,
    /// Opposite terminal operation is forbidden.
    Closed,
    /// Permanent reservation record limit is reached.
    Capacity,
    /// Lower allowance would violate outstanding holds or consumption.
    Overcommitted,
    /// Requested allowance already matches; revision remains unchanged.
    Unchanged,
}
impl Decision {
    pub(crate) fn success(self) -> bool {
        matches!(
            self,
            Self::Opened
                | Self::AllowanceChanged
                | Self::Reserved
                | Self::ExistingReservation
                | Self::Consumed
                | Self::AlreadyConsumed
                | Self::Released
                | Self::AlreadyReleased
                | Self::Unchanged
        )
    }
}
impl WireValue for Decision {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        (match self {
            Self::Opened => 1_u8,
            Self::AllowanceChanged => 2,
            Self::Reserved => 3,
            Self::ExistingReservation => 4,
            Self::Consumed => 5,
            Self::AlreadyConsumed => 6,
            Self::Released => 7,
            Self::AlreadyReleased => 8,
            Self::Invalid => 9,
            Self::NotFound => 10,
            Self::AlreadyOpen => 11,
            Self::Conflict => 12,
            Self::Insufficient => 13,
            Self::Closed => 14,
            Self::Capacity => 15,
            Self::Overcommitted => 16,
            Self::Unchanged => 17,
        })
        .encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match u8::decode(d)? {
            1 => Ok(Self::Opened),
            2 => Ok(Self::AllowanceChanged),
            3 => Ok(Self::Reserved),
            4 => Ok(Self::ExistingReservation),
            5 => Ok(Self::Consumed),
            6 => Ok(Self::AlreadyConsumed),
            7 => Ok(Self::Released),
            8 => Ok(Self::AlreadyReleased),
            9 => Ok(Self::Invalid),
            10 => Ok(Self::NotFound),
            11 => Ok(Self::AlreadyOpen),
            12 => Ok(Self::Conflict),
            13 => Ok(Self::Insufficient),
            14 => Ok(Self::Closed),
            15 => Ok(Self::Capacity),
            16 => Ok(Self::Overcommitted),
            17 => Ok(Self::Unchanged),
            _ => Err(CodecError::Invalid("unknown quota decision")),
        }
    }
}
/// Durable command decision and consistent facts, including business rejections.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    /// Domain decision; native rejection still carries this output and its receipt.
    pub decision: Decision,
    /// Customer facts, when the validated account exists.
    pub account: Option<Account>,
    /// Reservation facts, when the operation selected an existing or newly created ID.
    pub reservation: Option<Reservation>,
}
impl WireValue for Outcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.decision.encode(e)?;
        self.account.encode(e)?;
        self.reservation.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            decision: Decision::decode(d)?,
            account: Option::<Account>::decode(d)?,
            reservation: Option::<Reservation>::decode(d)?,
        })
    }
}
/// Bounded current reservation pagination, including permanent terminal records.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    /// Exclusive UUID cursor; None starts at the beginning.
    pub after: Option<ReservationId>,
    /// 1..100 records per read.
    pub limit: u32,
}
impl WireValue for PageRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.after.encode(e)?;
        self.limit.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            after: Option::<ReservationId>::decode(d)?,
            limit: u32::decode(d)?,
        })
    }
}
/// One source observation containing coherent counters and a bounded record page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    /// Account facts from this same read.
    pub account: Option<Account>,
    /// Reservations sorted by canonical UUID bytes.
    pub reservations: Vec<Reservation>,
    /// Next exclusive cursor, only when more records exist.
    pub next: Option<ReservationId>,
}
impl WireValue for Page {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.account.encode(e)?;
        if self.reservations.len() > 100 {
            return Err(CodecError::Limit);
        }
        e.write_count(self.reservations.len())?;
        for v in &self.reservations {
            v.encode(e)?;
        }
        self.next.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let account = Option::<Account>::decode(d)?;
        let count = d.read_count()?;
        if count > 100 {
            return Err(CodecError::Limit);
        }
        let mut reservations = Vec::with_capacity(count);
        for _ in 0..count {
            reservations.push(Reservation::decode(d)?);
        }
        Ok(Self {
            account,
            reservations,
            next: Option::<ReservationId>::decode(d)?,
        })
    }
}
