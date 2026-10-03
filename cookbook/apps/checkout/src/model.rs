use cellule_runtime::{CellTarget, Error, NamespaceId, partition_for_shard};
use serde::{Deserialize, Serialize};
/// Permanent order, reservation, saga, and payment identities per installation.
pub const MAX_ORDERS: i64 = 128;
/// Products in the one inventory transaction domain.
pub const MAX_PRODUCTS: i64 = 16;
/// Permanent internal command and callback bindings per domain.
pub const MAX_MESSAGES: i64 = 4096;
/// Operator reconciliation attempts per order; no automatic blind retries.
pub const MAX_RECONCILIATIONS: u8 = 8;
/// Canonical nonzero UUID identity, independent of native lease attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id([u8; 16]);
impl Id {
    /// Validates binary identity bytes.
    pub fn from_bytes(value: [u8; 16]) -> cellule_runtime::Result<Self> {
        if value == [0; 16] {
            return Err(Error::Identity("zero checkout UUID"));
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
            uuid::Uuid::parse_str(&text).map_err(|_| Error::Identity("invalid checkout UUID"))?;
        if text != value.to_string() {
            return Err(Error::Identity("checkout UUID must be canonical"));
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

/// One frozen checkout request; a business identity permanently binds every field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderSpec {
    /// Permanent business identity, independent of native run and Activity IDs.
    pub id: Id,
    /// Canonical lowercase ASCII product key, at most 64 bytes.
    pub sku: String,
    /// Units reserved atomically, 1 through 100.
    pub quantity: u32,
    /// Synthetic authorization amount, 1 through 1,000,000 minor units.
    pub amount: u64,
    /// Canonical numeric-loopback HTTP origin ending in a slash.
    pub payment_endpoint: String,
    /// Immutable simulated authorization decision.
    pub payment_policy: PaymentPolicy,
}
impl OrderSpec {
    /// Validates all immutable fields before publication or external dispatch.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        validate_sku(&self.sku)?;
        if !(1..=100).contains(&self.quantity) || !(1..=1_000_000).contains(&self.amount) {
            return Err(Error::Command("invalid checkout quantity or amount"));
        }
        validate_endpoint(&self.payment_endpoint)
    }
}
/// Checks canonical product keys without normalization aliases.
pub fn validate_sku(value: &str) -> cellule_runtime::Result<()> {
    if value.is_empty()
        || value.len() > 64
        || value.starts_with('-')
        || value.ends_with('-')
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(Error::Identity("invalid checkout product key"));
    }
    Ok(())
}
/// Restricts the included simulator adapter to an exact local HTTP origin.
pub fn validate_endpoint(value: &str) -> cellule_runtime::Result<()> {
    let url = url::Url::parse(value).map_err(|_| Error::Identity("invalid payment origin"))?;
    if value.len() > 256
        || url.as_str() != value
        || url.scheme() != "http"
        || !matches!(url.host(),Some(url::Host::Ipv4(ip)) if ip.is_loopback())
        || url.port().is_none_or(|port| port == 0)
        || url.path() != "/"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Identity(
            "payment origin must be canonical numeric loopback HTTP",
        ));
    }
    Ok(())
}
/// Stable fixed-shard routing within the source's tenant and application.
pub fn target(source: &CellTarget, namespace: NamespaceId) -> cellule_runtime::Result<CellTarget> {
    CellTarget::new(
        source.tenant(),
        source.application(),
        namespace,
        &partition_for_shard(0),
    )
}
/// Included payment simulator's immutable business decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentPolicy {
    /// Create one authorization.
    Approve,
    /// Persist a declined decision; retries cannot change it.
    Decline,
}
/// Public order ingress. Cancellation is an intent, not a completed compensation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderChange {
    /// Publish the order and its saga-start intent atomically.
    Place(OrderSpec),
    /// Set cancellation before the irreversible order decision.
    Cancel(Id),
}
/// Domain ingress outcome, retained separately from current state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderOutcome {
    /// Original accepted start intent; replay does not start another saga.
    Accepted {
        /// Stable native Effect identity.
        start_effect: [u8; 32],
    },
    /// Cancellation is durably requested; inspect compensation progress separately.
    CancellationRequested,
    /// Decision has committed, or the order already reached a terminal result.
    TooLate,
    /// No accepted order exists under this identity.
    NotFound,
    /// Existing immutable request differs.
    Conflict,
    /// Permanent identity bound is full.
    Capacity,
}
/// Canonical inventory lookup without initialization semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StockQuery {
    /// Exact lowercase product key.
    pub sku: String,
}
/// Public inventory initialization; identical replay cannot replenish sold stock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    /// Canonical product key.
    pub sku: String,
    /// Initial capacity, 1 through 10,000.
    pub units: u32,
}
impl Seed {
    /// Validates the reference inventory limits.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        validate_sku(&self.sku)?;
        if !(1..=10000).contains(&self.units) {
            return Err(Error::Command("invalid inventory seed"));
        }
        Ok(())
    }
}
/// Internal delivery settlement; business conflicts remain distinguishable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryOutcome {
    /// Request and its transition/response intent are durably bound.
    Applied,
    /// Changed bytes under a permanent business or message key.
    Conflict,
    /// Permanent message or business capacity is exhausted.
    Capacity,
    /// Required durable order, reservation, or run is absent.
    NotFound,
    /// The requested local state transition is not valid.
    InvalidState,
}
/// Terminal synthetic order result after the necessary inventory settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderResult {
    /// Authorized payment and committed stock.
    Fulfilled,
    /// Cancellation won before decision; any authorization and stock hold were compensated.
    Cancelled,
    /// No stock was reserved.
    OutOfStock,
    /// Payment was durably declined and stock released.
    Declined,
    /// Payment was already voided and stock released.
    PaymentVoided,
}
/// SQL order read model. Cross-Cell completion follows separate receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Accepted saga intent, not yet settled.
    Pending,
    /// An external outcome requires an explicit reconciliation attempt.
    NeedsReview,
    /// Fulfillment decision committed; cancellation can no longer win.
    Committing,
    /// Necessary external and inventory outcomes were acknowledged.
    Finished(OrderResult),
}
/// Permanent order record and observable local progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Order {
    /// Frozen business request.
    pub spec: OrderSpec,
    /// Local projection of the durable saga.
    pub status: OrderStatus,
    /// Cancellation intent; it never claims that payment or stock was reversed.
    pub cancel_requested: bool,
    /// Original saga-start intent.
    pub start_effect: [u8; 32],
}
/// Permanent stock reservation lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationStatus {
    /// Units are unavailable to other orders.
    Held,
    /// Units were consumed by the irrevocable fulfillment decision.
    Committed,
    /// Compensation released units; this identity cannot reacquire them.
    Released,
    /// Initial reservation failed; this identity cannot be reused after restock.
    Unavailable,
}
/// Business binding independent of native inbox retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    /// Original order request.
    pub spec: OrderSpec,
    /// Exact saga lifetime authorized to mutate this reservation.
    pub run_id: [u8; 16],
    /// Current local allocation state.
    pub status: ReservationStatus,
}
/// Coherent inventory counters from one SQL transaction domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stock {
    /// Canonical product key.
    pub sku: String,
    /// Immutable initial capacity.
    pub total: u32,
    /// Unallocated units.
    pub available: u32,
    /// Reserved units.
    pub held: u32,
    /// Consumed units.
    pub sold: u32,
}
/// Local SQL/Workflow request operation; every call has one causally pinned step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// Reserve inventory.
    Reserve,
    /// Consume an existing hold after an accepted order decision.
    Commit,
    /// Release a hold during compensation.
    Release,
    /// Decide whether cancellation or fulfillment wins in the order Cell.
    Decide,
    /// Publish the terminal result after inventory acknowledgment.
    Finish(OrderResult),
    /// Publish explicit external uncertainty without discarding held stock.
    Review,
}
/// One immutable cross-Cell request and callback correlation token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    /// Frozen order and external idempotency key.
    pub spec: OrderSpec,
    /// Native saga lifetime.
    pub run_id: [u8; 16],
    /// Native action identity, reused across delivery attempts.
    pub step: [u8; 16],
    /// Exact requested local transition.
    pub operation: Operation,
}
impl Call {
    /// Validates the immutable request envelope.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.run_id == [0; 16] || self.step == [0; 16] {
            return Err(Error::Identity("zero checkout run or step"));
        }
        Ok(())
    }
    /// Stable permanent callback identity, independent of lease attempts.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.checkout.reply.v1\0");
        hash.update(&self.spec.id.bytes());
        hash.update(&self.run_id);
        hash.update(&self.step);
        *hash.finalize().as_bytes()
    }
}
/// Causally accepted SQL response. It is distinct from a native delivery receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplyValue {
    /// Stock held.
    Held,
    /// Initial stock unavailable.
    Unavailable,
    /// Stock consumed.
    Committed,
    /// Stock released.
    Released,
    /// Fulfillment won the order decision.
    Accepted,
    /// Cancellation won the order decision.
    Cancelled,
    /// Terminal order result published.
    Finished(OrderResult),
    /// Uncertainty visible in the order read model.
    ReviewRecorded,
}
/// Permanent immutable callback into one exact saga run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    /// Original request and step.
    pub call: Call,
    /// Durable receiver decision.
    pub value: ReplyValue,
}
/// External operation; the order identity, not the Activity identity, is its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentAction {
    /// Reconcile existing state, then create an idempotent authorization if absent.
    Authorize,
    /// Persist a void tombstone, including when authorization is not yet visible.
    Void,
}
/// Durable external payment state; no absence is inferred from an unknown response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentStatus {
    /// One authorization exists.
    Authorized,
    /// Immutable declined decision.
    Declined,
    /// Durable tombstone preventing any later authorization under this key.
    Voided,
}
/// Simulator record and verifiable apply counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payment {
    /// Original immutable request.
    pub spec: OrderSpec,
    /// Current external state.
    pub status: PaymentStatus,
    /// Authorization applied zero or one times.
    pub authorizations: u32,
    /// Void tombstone applied zero or one times.
    pub voids: u32,
}
/// Bounded external operation input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaymentWork {
    /// Frozen request and permanent external idempotency key.
    pub spec: OrderSpec,
    /// Authorization or compensation.
    pub action: PaymentAction,
}
/// External observation retained by the first native Activity completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentObservation {
    /// Verified immutable business binding and durable external record.
    Known(Payment),
    /// HTTP or native execution could not establish the external outcome.
    Unknown,
}
/// Concrete saga progress, including compensation and operator review.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Await inventory response.
    Reserving,
    /// Await payment authorization observation.
    Authorizing,
    /// Await irrevocable order decision.
    Deciding,
    /// Await stock consumption.
    Committing,
    /// Await payment void observation.
    Voiding,
    /// Await inventory release.
    Releasing,
    /// Await terminal order publication.
    Finishing,
    /// Await uncertainty read-model publication.
    RecordingReview,
    /// Durable external uncertainty; explicit reconciliation is required.
    NeedsReview,
    /// All required acknowledgments retained.
    Completed,
}
/// Pure native Workflow state; stored acknowledgments survive process interruption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SagaState {
    /// Original business request.
    pub spec: OrderSpec,
    /// Exact native lifetime.
    pub run_id: [u8; 16],
    /// Current causal phase.
    pub phase: Phase,
    /// Outstanding cross-Cell request.
    pub waiting: Option<Call>,
    /// Outstanding native Activity identity.
    pub activity: Option<[u8; 16]>,
    /// Current external operation, retained across uncertainty.
    pub payment_action: Option<PaymentAction>,
    /// Last verified external state.
    pub payment: Option<PaymentStatus>,
    /// Planned terminal result; completion waits for necessary acknowledgments.
    pub result: Option<OrderResult>,
    /// Explicit attempts already accepted from the operator.
    pub reconciliations: u8,
}
/// Operator retry of the same permanent payment business identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconcile {
    /// Order whose saga is waiting for review.
    pub order: Id,
    /// Caller-retained permanent reconciliation identity.
    pub token: Id,
}
/// Bounded receiver-local keyset query; pages are current reads, not snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    /// Last observed row; zero starts a list.
    pub after: i64,
    /// One through 100 items.
    pub limit: u32,
}
impl Page {
    /// Validates keyset and output bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.after < 0 || !(1..=100).contains(&self.limit) {
            return Err(Error::Command("invalid checkout page"));
        }
        Ok(())
    }
}
/// Bounded order page with receiver-local continuation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersPage {
    /// Durable row positions and current local orders.
    pub orders: Vec<(i64, Order)>,
    /// Last returned row when another row exists.
    pub next: Option<i64>,
}
pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Command("checkout JSON encoding"))?;
    if bytes.len() > 262144 {
        return Err(Error::Command("checkout value exceeds bound"));
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 262144 {
        return Err(Error::Command("checkout value exceeds bound"));
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Command("invalid checkout JSON"))
}

impl Order {
    /// Checks the durable order projection and cancellation decision.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.start_effect == [0; 32]
            || (self.cancel_requested
                && matches!(
                    self.status,
                    OrderStatus::Committing | OrderStatus::Finished(OrderResult::Fulfilled)
                ))
            || (matches!(self.status, OrderStatus::Finished(OrderResult::Cancelled))
                && !self.cancel_requested)
        {
            return Err(Error::Command("invalid stored order state"));
        }
        Ok(())
    }
}
impl Reservation {
    /// Checks the permanent order and native run binding.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.run_id == [0; 16] {
            return Err(Error::Identity("zero reservation run"));
        }
        Ok(())
    }
}
impl Stock {
    /// Checks capacity conservation without overflowing on corrupt values.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        validate_sku(&self.sku)?;
        if !(1..=10000).contains(&self.total)
            || self
                .available
                .checked_add(self.held)
                .and_then(|x| x.checked_add(self.sold))
                != Some(self.total)
        {
            return Err(Error::Command("stock invariant violated"));
        }
        Ok(())
    }
}
impl Reply {
    /// Rejects response values that do not answer their exact operation.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.call.validate()?;
        let valid = matches!(
            (self.call.operation, self.value),
            (
                Operation::Reserve,
                ReplyValue::Held | ReplyValue::Unavailable
            ) | (Operation::Commit, ReplyValue::Committed)
                | (Operation::Release, ReplyValue::Released)
                | (
                    Operation::Decide,
                    ReplyValue::Accepted | ReplyValue::Cancelled
                )
                | (Operation::Review, ReplyValue::ReviewRecorded)
        ) || matches!((self.call.operation, self.value), (Operation::Finish(planned), ReplyValue::Finished(actual)) if actual == planned || (planned != OrderResult::Fulfilled && actual == OrderResult::Cancelled));
        if !valid {
            return Err(Error::Command("checkout reply does not answer operation"));
        }
        Ok(())
    }
}
impl Payment {
    /// Verifies immutable policy and external apply counts.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        let valid = match self.status {
            PaymentStatus::Authorized => {
                self.spec.payment_policy == PaymentPolicy::Approve
                    && self.authorizations == 1
                    && self.voids == 0
            }
            PaymentStatus::Declined => {
                self.spec.payment_policy == PaymentPolicy::Decline
                    && self.authorizations == 0
                    && self.voids == 0
            }
            PaymentStatus::Voided => {
                self.voids == 1
                    && self.authorizations
                        <= u32::from(self.spec.payment_policy == PaymentPolicy::Approve)
            }
        };
        if !valid {
            return Err(Error::Command("invalid payment state or apply counts"));
        }
        Ok(())
    }
}
impl SagaState {
    /// Checks phase-specific pending work and durable acknowledgments.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.run_id == [0; 16] || self.reconciliations > MAX_RECONCILIATIONS {
            return Err(Error::Command(
                "invalid checkout saga identity or attempt count",
            ));
        }
        let operation = match self.phase {
            Phase::Reserving => Some(Operation::Reserve),
            Phase::Deciding => Some(Operation::Decide),
            Phase::Committing => Some(Operation::Commit),
            Phase::Releasing => Some(Operation::Release),
            Phase::Finishing => Some(Operation::Finish(
                self.result
                    .ok_or(Error::Command("missing planned result"))?,
            )),
            Phase::RecordingReview => Some(Operation::Review),
            _ => None,
        };
        if let Some(operation) = operation {
            let call = self
                .waiting
                .as_ref()
                .ok_or(Error::Command("missing saga callback correlation"))?;
            call.validate()?;
            if call.spec != self.spec
                || call.run_id != self.run_id
                || call.operation != operation
                || self.activity.is_some()
            {
                return Err(Error::Command("saga pending callback differs"));
            }
        } else if self.waiting.is_some() {
            return Err(Error::Command("unexpected pending callback"));
        }
        let action = match self.phase {
            Phase::Authorizing => Some(PaymentAction::Authorize),
            Phase::Voiding => Some(PaymentAction::Void),
            _ => None,
        };
        if let Some(action) = action {
            if self.payment_action != Some(action) || self.activity.is_none_or(|id| id == [0; 16]) {
                return Err(Error::Command("saga pending Activity differs"));
            }
        } else if self.activity.is_some() {
            return Err(Error::Command("unexpected pending Activity"));
        }
        if matches!(self.phase, Phase::RecordingReview | Phase::NeedsReview)
            && self.payment_action.is_none()
            || matches!(self.phase, Phase::Deciding | Phase::Committing)
                && self.payment != Some(PaymentStatus::Authorized)
            || matches!(self.phase, Phase::Releasing | Phase::Voiding) && self.result.is_none()
            || self.phase == Phase::Completed && self.result.is_none()
        {
            return Err(Error::Command("missing saga settlement evidence"));
        }
        if self.phase == Phase::Releasing
            && !matches!(
                (self.result, self.payment),
                (
                    Some(OrderResult::Cancelled | OrderResult::PaymentVoided),
                    Some(PaymentStatus::Voided)
                ) | (Some(OrderResult::Declined), Some(PaymentStatus::Declined))
            )
        {
            return Err(Error::Command(
                "inventory release lacks external settlement",
            ));
        }
        if self.phase == Phase::Finishing
            && !matches!(
                (self.result, self.payment),
                (
                    Some(OrderResult::Fulfilled),
                    Some(PaymentStatus::Authorized)
                ) | (
                    Some(OrderResult::Cancelled | OrderResult::PaymentVoided),
                    Some(PaymentStatus::Voided)
                ) | (Some(OrderResult::Declined), Some(PaymentStatus::Declined))
                    | (Some(OrderResult::OutOfStock), None)
            )
        {
            return Err(Error::Command("order finish lacks external settlement"));
        }
        Ok(())
    }
}
