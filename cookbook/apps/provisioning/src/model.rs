use cellule_runtime::{CellTarget, Error, NamespaceId, partition_for_shard};
use serde::{Deserialize, Serialize};
/// Permanent resource, Workflow, and provider identities per installation.
pub const MAX_RESOURCES: i64 = 128;
/// Permanent callback bindings in each application transaction domain.
pub const MAX_MESSAGES: i64 = 4096;
/// Provider attempts per stage before explicit review is required.
pub const MAX_STAGE_ATTEMPTS: u8 = 8;
/// Distinct operator reconciliation cycles per resource lifetime.
pub const MAX_RECONCILIATIONS: u8 = 8;
/// Conservative lifetime bound on native provider Activity dispatches.
pub const MAX_ACTIVITIES: u32 = 512;
/// Canonical nonzero UUID identity, independent of native lease attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id([u8; 16]);
impl Id {
    /// Validates persistent identity bytes.
    pub fn from_bytes(value: [u8; 16]) -> cellule_runtime::Result<Self> {
        if value == [0; 16] {
            return Err(Error::Identity("zero provisioning UUID"));
        }
        Ok(Self(value))
    }
    /// Returns immutable routing and business identity bytes.
    pub fn bytes(self) -> [u8; 16] {
        self.0
    }
}
impl TryFrom<String> for Id {
    type Error = Error;
    fn try_from(text: String) -> Result<Self, Error> {
        let value = uuid::Uuid::parse_str(&text)
            .map_err(|_| Error::Identity("invalid provisioning UUID"))?;
        if text != value.to_string() {
            return Err(Error::Identity("provisioning UUID must be canonical"));
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
/// One immutable request for a simulated volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// Permanent business identity shared with the provider.
    pub id: Id,
    /// Canonical lowercase ASCII name, at most 64 bytes.
    pub name: String,
    /// Requested simulated capacity, 1 through 1,024 MiB.
    pub capacity_mib: u32,
    /// Exact canonical numeric-loopback HTTP provider origin.
    pub provider_endpoint: String,
}
impl Spec {
    /// Checks immutable request and concrete simulator bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.name.is_empty()
            || self.name.len() > 64
            || self.name.starts_with('-')
            || self.name.ends_with('-')
            || !self
                .name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || !(1..=1024).contains(&self.capacity_mib)
        {
            return Err(Error::Command("invalid provisioning name or capacity"));
        }
        validate_endpoint(&self.provider_endpoint)
    }
}
/// Restricts the reference provider adapter to an exact local HTTP origin.
pub fn validate_endpoint(value: &str) -> cellule_runtime::Result<()> {
    let url = url::Url::parse(value).map_err(|_| Error::Identity("invalid provider origin"))?;
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
            "provider origin must be canonical numeric loopback HTTP",
        ));
    }
    Ok(())
}
/// Resolves fixed-shard routing within one tenant and application.
pub fn target(source: &CellTarget, namespace: NamespaceId) -> cellule_runtime::Result<CellTarget> {
    CellTarget::new(
        source.tenant(),
        source.application(),
        namespace,
        &partition_for_shard(0),
    )
}
/// Public application intent. Deletion includes cancellation during creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    /// Publish one immutable resource request and its start Effect.
    Request(Spec),
    /// Request cleanup; this outcome does not assert provider deletion.
    Delete(Id),
}
/// Durable ingress result, distinct from resource lifecycle completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// Original saga-start intent; replay never starts a second resource lifetime.
    Accepted {
        /// Stable native Effect identity.
        start_effect: [u8; 32],
    },
    /// Deletion or cancellation is durably requested.
    DeletionRequested,
    /// No accepted resource exists under this identity.
    NotFound,
    /// Changed immutable request under a permanent business identity.
    Conflict,
    /// Permanent identity capacity is full.
    Capacity,
}
/// Typed internal and simulator command settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryOutcome {
    /// Local change and any response intent are durably bound.
    Applied,
    /// Immutable request, native run, or callback bytes differ.
    Conflict,
    /// Permanent capacity is full.
    Capacity,
    /// Required local request or run is absent.
    NotFound,
    /// Local causal transition is unavailable.
    InvalidState,
}
/// Provider lifecycle, stored independently from the application projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderPhase {
    /// Durable creation accepted; simulated allocation is pending.
    Creating,
    /// Durable simulated volume details are available.
    Ready,
    /// Permanent deletion intent accepted; cleanup is pending.
    Deleting,
    /// Permanent tombstone prevents later creation under the same identity.
    Deleted,
}
/// Permanent provider operation, independent of native Activity IDs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderAction {
    /// Idempotently create or reconcile one volume.
    Create,
    /// Idempotently delete, including before a delayed create arrives.
    Delete,
}
/// Stable provider mutation envelope. Every immutable field is bound permanently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWork {
    /// Frozen resource request.
    pub spec: Spec,
    /// Creation or cleanup operation.
    pub action: ProviderAction,
    /// Exact permanent operation key derived from request and action.
    pub operation_key: [u8; 32],
}
impl ProviderWork {
    /// Derives one permanent operation key, reused after unknown replies and retries.
    pub fn new(spec: Spec, action: ProviderAction) -> Self {
        let operation_key = operation_key(spec.id, action);
        Self {
            spec,
            action,
            operation_key,
        }
    }
    /// Rejects changed operation keys before provider publication.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.operation_key != operation_key(self.spec.id, self.action) {
            return Err(Error::Identity("provider operation key differs"));
        }
        Ok(())
    }
}
fn operation_key(id: Id, action: ProviderAction) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.provisioning.operation.v1\0");
    hash.update(&id.bytes());
    hash.update(&[match action {
        ProviderAction::Create => 0,
        ProviderAction::Delete => 1,
    }]);
    *hash.finalize().as_bytes()
}
/// Independently durable virtual volume and verifiable mutation apply counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderResource {
    /// Original immutable request.
    pub spec: Spec,
    /// Deterministic provider volume identity.
    pub resource_id: Id,
    /// Stable synthetic address, not a cloud or filesystem resource.
    pub address: String,
    /// Current independently committed lifecycle.
    pub phase: ProviderPhase,
    /// Creation applied zero or one times.
    pub creates: u32,
    /// Permanent deletion applied zero or one times.
    pub deletes: u32,
    /// Due time for asynchronous creation or cleanup; absent in stable states.
    pub due_at_ms: Option<i64>,
}
impl ProviderResource {
    /// Checks provider identity, phase, and apply-count contracts.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        let key = operation_key(self.spec.id, ProviderAction::Create);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&key[..16]);
        let expected = Id::from_bytes(bytes)?;
        let valid = match self.phase {
            ProviderPhase::Creating => {
                self.creates == 1 && self.deletes == 0 && self.due_at_ms.is_some_and(|due| due > 0)
            }
            ProviderPhase::Ready => {
                self.creates == 1 && self.deletes == 0 && self.due_at_ms.is_none()
            }
            ProviderPhase::Deleting => {
                self.creates == 1 && self.deletes == 1 && self.due_at_ms.is_some_and(|due| due > 0)
            }
            ProviderPhase::Deleted => {
                self.creates <= 1 && self.deletes == 1 && self.due_at_ms.is_none()
            }
        };
        if self.resource_id != expected
            || self.address != format!("sim://volumes/{expected}")
            || !valid
        {
            return Err(Error::Command(
                "invalid provider resource identity or lifecycle",
            ));
        }
        Ok(())
    }
    pub(crate) fn initial(spec: Spec, now: i64, delete: bool) -> cellule_runtime::Result<Self> {
        let key = operation_key(spec.id, ProviderAction::Create);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&key[..16]);
        let resource_id = Id::from_bytes(bytes)?;
        let value = Self {
            spec,
            resource_id,
            address: format!("sim://volumes/{resource_id}"),
            phase: if delete {
                ProviderPhase::Deleted
            } else {
                ProviderPhase::Creating
            },
            creates: u32::from(!delete),
            deletes: u32::from(delete),
            due_at_ms: if delete {
                None
            } else {
                Some(
                    now.checked_add(1000)
                        .ok_or(Error::Command("provider lifecycle due overflow"))?,
                )
            },
        };
        value.validate()?;
        Ok(value)
    }
}
/// Verified provider observation; unknown never proves resource absence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Observation {
    /// Exact immutable request and durable provider state verified.
    Known(ProviderResource),
    /// No outcome could be established, including a native execution failure.
    Unknown,
}
/// Durable directory projection, not an atomic view of the provider or Workflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceStatus {
    /// Accepted start intent.
    Requested,
    /// Verified ready details published and deletion not yet requested.
    Active,
    /// Cancellation or deprovisioning requested; cleanup is not yet proven.
    Deleting,
    /// Bounded automatic work exhausted; reconcile the same permanent identity.
    NeedsReview,
    /// Provider tombstone verified and terminal projection published.
    Deleted,
}
/// Application-owned resource directory entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    /// Immutable request.
    pub spec: Spec,
    /// Current local projection.
    pub status: ResourceStatus,
    /// Desired deletion, including cancellation before readiness.
    pub delete_requested: bool,
    /// Last verified provider observation; it can be older than provider state.
    pub observed: Option<ProviderResource>,
    /// Original native start intent.
    pub start_effect: [u8; 32],
}
impl Resource {
    /// Validates local projection and external identity bindings.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if let Some(value) = &self.observed {
            value.validate()?;
            if value.spec != self.spec {
                return Err(Error::Identity("directory provider binding differs"));
            }
        }
        if self.start_effect == [0; 32]
            || self.status == ResourceStatus::Active
                && (self.delete_requested
                    || self
                        .observed
                        .as_ref()
                        .is_none_or(|value| value.phase != ProviderPhase::Ready))
            || self.status == ResourceStatus::Deleted
                && (!self.delete_requested
                    || self
                        .observed
                        .as_ref()
                        .is_none_or(|value| value.phase != ProviderPhase::Deleted))
            || self.status == ResourceStatus::Deleting && !self.delete_requested
        {
            return Err(Error::Command("invalid resource projection"));
        }
        Ok(())
    }
}
/// Provider work stage retained across automatic retries and explicit review.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stage {
    /// Reconcile before idempotent create.
    Create,
    /// Poll creation without another mutation.
    PollCreation,
    /// Reconcile and submit idempotent deletion.
    Delete,
    /// Poll cleanup without treating an accepted delete as completed.
    PollDeletion,
}
/// Native provider Activity input; credentials are never persisted here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Work {
    /// Immutable business request.
    pub spec: Spec,
    /// Provider action or read-only poll.
    pub stage: Stage,
}
/// SQL projection operation pinned to one exact native causal step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Projection {
    /// Publish verified ready details or acknowledge prior cancellation.
    Ready(ProviderResource),
    /// Publish uncertainty and last verified observation.
    Review(Option<ProviderResource>),
    /// Publish terminal cleanup after a verified provider tombstone.
    Deleted(ProviderResource),
}
/// Immutable signed projection request and callback correlation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    /// Original resource request.
    pub spec: Spec,
    /// Exact native Workflow lifetime.
    pub run_id: [u8; 16],
    /// Exact native causal action identity.
    pub step: [u8; 16],
    /// Verified directory transition.
    pub projection: Projection,
}
impl Call {
    /// Checks external evidence and immutable causal scope.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.run_id == [0; 16] || self.step == [0; 16] {
            return Err(Error::Identity("zero provisioning run or step"));
        }
        let resource = match &self.projection {
            Projection::Ready(value) | Projection::Deleted(value) => Some(value),
            Projection::Review(value) => value.as_ref(),
        };
        if let Some(value) = resource {
            value.validate()?;
            if value.spec != self.spec {
                return Err(Error::Identity("projection provider request differs"));
            }
        }
        if matches!(&self.projection,Projection::Ready(value) if value.phase!=ProviderPhase::Ready)
            || matches!(&self.projection,Projection::Deleted(value) if value.phase!=ProviderPhase::Deleted)
        {
            return Err(Error::Command("projection lacks provider settlement"));
        }
        Ok(())
    }
    /// Stable permanent callback key, independent of Effect delivery attempts.
    pub fn key(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.provisioning.callback.v1\0");
        hash.update(&self.spec.id.bytes());
        hash.update(&self.run_id);
        hash.update(&self.step);
        *hash.finalize().as_bytes()
    }
}
/// Directory acknowledgment, distinct from an Effect delivery receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplyValue {
    /// Ready details were published.
    Published,
    /// Cancellation prevented active publication.
    DeleteRequested,
    /// Explicit uncertainty was projected.
    Reviewed,
    /// Terminal deletion was projected.
    Deleted,
}
/// Permanent callback into one exact Workflow causal step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    /// Exact original request.
    pub call: Call,
    /// Durable receiver decision.
    pub value: ReplyValue,
}
impl Reply {
    /// Rejects acknowledgments that do not answer their exact projection.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.call.validate()?;
        if !matches!(
            (&self.call.projection, self.value),
            (
                Projection::Ready(_),
                ReplyValue::Published | ReplyValue::DeleteRequested
            ) | (Projection::Review(_), ReplyValue::Reviewed)
                | (Projection::Deleted(_), ReplyValue::Deleted)
        ) {
            return Err(Error::Command(
                "provisioning callback does not answer projection",
            ));
        }
        Ok(())
    }
}
/// Native Workflow lifecycle, including the long-lived active resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Await create/reconciliation Activity.
    Creating,
    /// Await read-only creation poll.
    PollingCreation,
    /// Await durable directory decision.
    Publishing,
    /// Ready and retained until deprovisioning is requested.
    Active,
    /// Await idempotent cleanup Activity.
    Deleting,
    /// Await read-only cleanup poll.
    PollingDeletion,
    /// Await durable review projection.
    RecordingReview,
    /// Await explicit operator reconciliation or cancellation.
    NeedsReview,
    /// Await terminal directory acknowledgment.
    Finishing,
    /// Provider tombstone and directory terminal state acknowledged.
    Completed,
}
/// Pure, causally pinned native Workflow state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    /// Immutable business request.
    pub spec: Spec,
    /// Exact native lifetime.
    pub run_id: [u8; 16],
    /// Current causal phase.
    pub phase: Phase,
    /// Durable cancellation/deprovisioning intent observed by the Workflow.
    pub delete_requested: bool,
    /// Current or resumable provider stage.
    pub stage: Stage,
    /// Pending native Activity identity.
    pub activity: Option<[u8; 16]>,
    /// Pending directory callback correlation.
    pub waiting: Option<Call>,
    /// Last verified provider observation.
    pub observed: Option<ProviderResource>,
    /// Attempts dispatched in the current stage/cycle.
    pub stage_attempts: u8,
    /// Total native provider Activities, including polling.
    pub provider_attempts: u32,
    /// Native deletion Activities dispatched, including retries.
    pub cleanup_attempts: u32,
    /// Distinct explicit reconciliation cycles accepted.
    pub reconciliations: u8,
}
impl State {
    /// Checks phase, pending work, external identity, and lifetime budgets.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.run_id == [0; 16]
            || self.stage_attempts > MAX_STAGE_ATTEMPTS
            || self.provider_attempts > MAX_ACTIVITIES
            || self.cleanup_attempts > self.provider_attempts
            || self.reconciliations > MAX_RECONCILIATIONS
        {
            return Err(Error::Command("invalid provisioning lifetime or budgets"));
        }
        if let Some(value) = &self.observed {
            value.validate()?;
            if value.spec != self.spec {
                return Err(Error::Identity("Workflow provider identity differs"));
            }
        }
        let stage = match self.phase {
            Phase::Creating => Some(Stage::Create),
            Phase::PollingCreation => Some(Stage::PollCreation),
            Phase::Deleting => Some(Stage::Delete),
            Phase::PollingDeletion => Some(Stage::PollDeletion),
            _ => None,
        };
        if let Some(stage) = stage {
            if self.stage != stage
                || self.activity.is_none_or(|id| id == [0; 16])
                || self.waiting.is_some()
                || self.stage_attempts == 0
            {
                return Err(Error::Command("pending provisioning Activity differs"));
            }
        } else if self.activity.is_some() {
            return Err(Error::Command("unexpected provisioning Activity"));
        }
        if matches!(
            self.phase,
            Phase::Publishing | Phase::RecordingReview | Phase::Finishing
        ) {
            let call = self
                .waiting
                .as_ref()
                .ok_or(Error::Command("missing provisioning callback"))?;
            call.validate()?;
            let projected = match &call.projection {
                Projection::Ready(value) | Projection::Deleted(value) => Some(value),
                Projection::Review(value) => value.as_ref(),
            };
            if call.spec != self.spec
                || call.run_id != self.run_id
                || projected != self.observed.as_ref()
                || !matches!(
                    (self.phase, &call.projection),
                    (Phase::Publishing, Projection::Ready(_))
                        | (Phase::RecordingReview, Projection::Review(_))
                        | (Phase::Finishing, Projection::Deleted(_))
                )
            {
                return Err(Error::Command("pending provisioning callback differs"));
            }
        } else if self.waiting.is_some() {
            return Err(Error::Command("unexpected provisioning callback"));
        }
        if self.phase == Phase::Active
            && (self.delete_requested
                || self
                    .observed
                    .as_ref()
                    .is_none_or(|value| value.phase != ProviderPhase::Ready))
            || self.phase == Phase::Completed
                && (!self.delete_requested
                    || self
                        .observed
                        .as_ref()
                        .is_none_or(|value| value.phase != ProviderPhase::Deleted))
            || matches!(
                self.phase,
                Phase::Deleting | Phase::PollingDeletion | Phase::Finishing
            ) && !self.delete_requested
        {
            return Err(Error::Command(
                "provisioning phase lacks settlement evidence",
            ));
        }
        Ok(())
    }
}
/// Permanent caller identity for a bounded explicit reconciliation cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconcile {
    /// Resource whose Workflow is waiting for review.
    pub resource: Id,
    /// Caller-retained reconciliation identity.
    pub token: Id,
}
/// Bounded directory-local keyset page, not a cross-Cell snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    /// Last returned local row; zero starts a traversal.
    pub after: i64,
    /// One through 100 current records.
    pub limit: u32,
}
impl Page {
    /// Checks page and output bounds.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        if self.after < 0 || !(1..=100).contains(&self.limit) {
            return Err(Error::Command("invalid provisioning page"));
        }
        Ok(())
    }
}
/// Bounded current directory records and a local continuation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePage {
    /// Local row positions and current projections.
    pub resources: Vec<(i64, Resource)>,
    /// Last returned row when another record exists.
    pub next: Option<i64>,
}
/// Provider-owned bounded lifecycle advancement. It never accepts an external timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Advance {
    /// At most 16 independently due resources per transaction.
    pub limit: u32,
}
/// Number of provider lifecycles advanced durably in this transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Advanced {
    /// Zero through 16 due resources advanced.
    pub resources: u32,
}
/// Read-only provider-local scheduling hint, rechecked by the advancement transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDue {
    /// Earliest verified pending lifecycle deadline, or no pending resource.
    pub at_ms: Option<i64>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Start {
    pub spec: Spec,
    pub delete_requested: bool,
}
pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| Error::Command("provisioning JSON encoding"))?;
    if bytes.len() > 262144 {
        return Err(Error::Command("provisioning value exceeds bound"));
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 262144 {
        return Err(Error::Command("provisioning value exceeds bound"));
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Command("invalid provisioning JSON"))
}
