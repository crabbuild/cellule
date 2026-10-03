use crate::{
    Artifact, Deployment, ReleaseId, TargetName, TargetOutcome, TargetRecord, TargetState,
    model::{decode, encode},
};
use cellule_runtime::{CellTarget, Error, NamespaceId, partition_for_shard};
use serde::{Deserialize, Serialize};

/// Immutable request; credentials and authenticated principals belong to the embedding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseSpec {
    /// Permanent release identity.
    pub release: ReleaseId,
    /// Canonical independently versioned target slot.
    pub target: TargetName,
    /// Frozen bounded build input, retained once in Workflow state.
    pub source: Vec<u8>,
    /// Original target generation; retries must never replace it with a fresh read.
    pub expected_generation: u64,
    /// Pinned loopback deployment simulator origin.
    pub target_endpoint: String,
    /// Pinned loopback private artifact broker origin.
    pub artifact_endpoint: String,
    /// Absolute human approval deadline; approval at the deadline loses.
    pub approval_deadline_ms: i64,
}
impl ReleaseSpec {
    /// Verifies immutable input, both concrete local origins, and resource limits.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.deployment()?.validate()?;
        validate_origin(&self.target_endpoint)?;
        validate_origin(&self.artifact_endpoint)?;
        if self.approval_deadline_ms <= 0 {
            return Err(Error::Command("invalid approval deadline"));
        }
        Ok(())
    }
    /// Computes the exact deployment reference before any asynchronous build or target mutation.
    pub fn deployment(&self) -> cellule_runtime::Result<Deployment> {
        let (artifact, _) = Artifact::build(self.release, &self.source)?;
        Ok(Deployment {
            release: self.release,
            target: self.target.clone(),
            artifact,
            expected_generation: self.expected_generation,
        })
    }
    /// Stable full-input identity, including source, origins, generation, and approval deadline.
    pub fn digest(&self) -> cellule_runtime::Result<[u8; 32]> {
        self.validate()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.release.input.v1\0");
        hash.update(&crate::wire::encode_wire(self, 8192)?);
        Ok(*hash.finalize().as_bytes())
    }
}
/// Checks a bounded canonical loopback origin before sending an authenticated request.
pub fn validate_origin(value: &str) -> cellule_runtime::Result<()> {
    if value.len() > 256 {
        return Err(Error::Identity("release origin exceeds bound"));
    }
    let url =
        url::Url::parse(value).map_err(|_| Error::Identity("invalid release local origin"))?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none_or(|port| port == 0)
        || url.path() != "/"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str() != value
    {
        return Err(Error::Identity(
            "release origin must be a canonical loopback HTTP origin",
        ));
    }
    Ok(())
}
/// Published immutable Blob manifest proof, separate from native Activity completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPublication {
    /// Release-scoped exact content reference.
    pub artifact: Artifact,
    /// Native Blob manifest ETag verified after publication.
    pub etag: [u8; 32],
}
impl ArtifactPublication {
    pub(crate) fn validate(&self, spec: &ReleaseSpec) -> cellule_runtime::Result<()> {
        if self.artifact != spec.deployment()?.artifact || self.etag == [0; 32] {
            return Err(Error::Command(
                "release Blob publication differs from immutable input",
            ));
        }
        Ok(())
    }
}
/// Bounded external work stage; version two adds reproducible artifact verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStage {
    /// Build and publish immutable private Blob content.
    Build,
    /// Reconcile and install using the original target generation.
    Deploy,
    /// Read target operation, selection, and actual artifact bytes.
    Verify,
    /// Version-two independent rebuild and Blob-byte comparison.
    Rebuild,
    /// Restore the captured predecessor or install a cancellation tombstone.
    Rollback,
}
/// Observable native coordination phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelinePhase {
    /// Native Activity pending or leased.
    Working,
    /// Built input awaits a human decision.
    AwaitingApproval,
    /// A signed release-record callback is pending.
    Publishing,
    /// Verification acknowledged; retain the run until explicit compensation.
    Active,
    /// Automatic work exhausted; explicit reconciliation may resume the same stage.
    NeedsReview,
    /// External settlement and receiver-local record publication acknowledged.
    Done,
}
/// Receiver-local historical progress; it does not assert the target still has that selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReleaseStatus {
    /// Built artifact is available for approval.
    AwaitingApproval,
    /// Exact target bytes verified at the recorded generation.
    Active,
    /// External or publication outcome remains unresolved.
    NeedsReview,
    /// A target tombstone proves this release never installed.
    Cancelled,
    /// Original target deployment was compensated once.
    RolledBack,
    /// A newer target generation prevented compensation.
    Superseded,
    /// Original target generation was permanently refused.
    Refused,
}
/// Native adapter input with no stored secret or mutable generation lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineWork {
    /// Original frozen input.
    pub spec: ReleaseSpec,
    /// One bounded operation.
    pub stage: PipelineStage,
    /// Already published manifest when a stage requires Blob evidence.
    pub publication: Option<ArtifactPublication>,
}
/// Bounded verified adapter result; unknown never proves absence or compensation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivityReport {
    /// Broker verified immutable published bytes and manifest.
    Published(ArtifactPublication),
    /// Independent target's full permanent operation.
    Target(TargetRecord),
    /// Exact operation and target selection after actual-byte verification.
    Verified {
        /// Permanent immutable operation.
        record: TargetRecord,
        /// Historical target selection observed during verification.
        state: TargetState,
    },
    /// Extra version-two rebuild matched the original published bytes.
    Rebuilt(ArtifactPublication),
    /// No sufficient proof was obtained.
    Unknown,
}
/// Human decision binds the full original input; authorization precedes capability construction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    /// Release selected by the authorized approver.
    pub release: ReleaseId,
    /// Exact input shown to the approver, including the deterministic artifact.
    pub input_digest: [u8; 32],
    /// True approves; false requests compensation without deployment.
    pub approve: bool,
}
/// Caller-retained identity for a bounded operator reconciliation cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconcile {
    /// Original release.
    pub release: ReleaseId,
    /// Exact immutable input identity.
    pub input_digest: [u8; 32],
    /// Permanent token, independent of native command retry identity.
    pub token: ReleaseId,
}
/// Receiver-local progress snapshot, with immutable run, input, definition, and causal revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseRecord {
    /// Exact release identity.
    pub release: ReleaseId,
    /// Canonical target slot.
    pub target: TargetName,
    /// Original immutable input identity.
    pub input_digest: [u8; 32],
    /// Exact native run, never replaced by a domain restart.
    pub run_id: [u8; 16],
    /// Definition version pinned at start, including after code migration.
    pub definition_version: u8,
    /// Monotonic application progress revision, separate from native receipts.
    pub revision: u32,
    /// Historical progress at this revision.
    pub status: ReleaseStatus,
    /// Private Blob publication, when verified.
    pub publication: Option<ArtifactPublication>,
    /// Last verified independent target operation, when known.
    pub observed: Option<TargetRecord>,
    /// Target generation at successful actual-byte verification.
    pub verified_generation: Option<u64>,
    /// Extra reproducibility check required only by new version-two runs.
    pub rebuilt: bool,
}
impl ReleaseRecord {
    /// Validates progress evidence without treating an old verified selection as current.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        ReleaseId::from_bytes(self.release.bytes())?;
        TargetName::new(self.target.as_str().into())?;
        if self.input_digest == [0; 32]
            || self.run_id == [0; 16]
            || !matches!(self.definition_version, 1 | 2)
            || self.revision == 0
            || self.revision > 32
        {
            return Err(Error::Command(
                "invalid release record identity or revision",
            ));
        }
        if let Some(p) = &self.publication {
            p.artifact.validate(self.release)?;
            if p.etag == [0; 32] {
                return Err(Error::Command("zero publication ETag"));
            }
        }
        if let Some(r) = &self.observed {
            r.validate()?;
            if r.deployment.release != self.release
                || r.deployment.target != self.target
                || self
                    .publication
                    .as_ref()
                    .is_some_and(|p| p.artifact != r.deployment.artifact)
            {
                return Err(Error::Identity("release record target binding differs"));
            }
        }
        let expected = match self.status {
            ReleaseStatus::Cancelled => Some(TargetOutcome::Cancelled),
            ReleaseStatus::RolledBack => Some(TargetOutcome::RolledBack),
            ReleaseStatus::Superseded => Some(TargetOutcome::Superseded),
            ReleaseStatus::Refused => Some(TargetOutcome::Conflict),
            ReleaseStatus::Active => Some(TargetOutcome::Deployed),
            _ => None,
        };
        if expected.is_some_and(|e| self.observed.as_ref().is_none_or(|r| r.outcome != e))
            || self.status == ReleaseStatus::AwaitingApproval
                && (self.publication.is_none() || self.observed.is_some())
            || self.status == ReleaseStatus::Active
                && (self.publication.is_none()
                    || self.verified_generation
                        != self.observed.as_ref().and_then(|r| r.installed_generation)
                    || self.definition_version == 2 && !self.rebuilt)
            || self.rebuilt && (self.definition_version != 2 || self.publication.is_none())
        {
            return Err(Error::Command("release record lacks status proof"));
        }
        Ok(())
    }
}
/// Exact signed projection and callback correlation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    /// Bounded progress snapshot; raw source is retained only in Workflow state.
    pub record: ReleaseRecord,
    /// Exact native causal action.
    pub step: [u8; 16],
}
impl Projection {
    /// Validates the signed payload before publication or receipt.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.record.validate()?;
        if self.step == [0; 16] {
            return Err(Error::Identity("zero release projection step"));
        }
        Ok(())
    }
    /// Permanent message identity, independent of Effect attempts and payload changes.
    pub fn key(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(b"cookbook.release.projection.v1\0");
        h.update(&self.record.release.bytes());
        h.update(&self.record.run_id);
        h.update(&self.step);
        *h.finalize().as_bytes()
    }
}
/// Durable release-record acknowledgment into one exact native step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgment {
    /// Exact original projection, including every field of receiver evidence.
    pub projection: Projection,
}
/// Durable domain command outcome; Accepted means the signal or intent was committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlOutcome {
    /// Intent durably accepted; inspect native state for external settlement.
    Accepted,
    /// Caller changed a permanent identity or immutable input.
    Conflict,
    /// Release does not exist or is already terminal for this operation.
    InvalidState,
    /// Bounded permanent history is full.
    Capacity,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingActivity {
    pub id: [u8; 16],
    pub stage: PipelineStage,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Resume {
    AwaitingApproval,
    Active,
    Review,
    Done,
}
/// Pure native state; old runs keep their version and pending actions during rollout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineState {
    /// Original frozen build and deployment request.
    pub spec: ReleaseSpec,
    /// Exact native lifetime.
    pub run_id: [u8; 16],
    /// Pinned definition version, independent of the serving registry's current selection.
    pub version: u8,
    /// Current coordination phase.
    pub phase: PipelinePhase,
    /// Irreversible human approval intent observed before its absolute deadline.
    pub approved: bool,
    /// Explicit or timeout-driven compensation intent.
    pub rollback_requested: bool,
    /// Published immutable Blob proof.
    pub publication: Option<ArtifactPublication>,
    /// Last independently verified target operation.
    pub observed: Option<TargetRecord>,
    /// Generation at a successful actual-byte check.
    pub verified_generation: Option<u64>,
    /// Version-two reproducibility result.
    pub rebuilt: bool,
    /// Dispatched attempts in the current bounded stage.
    pub stage_attempts: u8,
    /// All dispatched native Activities, including explicit reconciliation.
    pub activities: u32,
    /// Accepted explicit reconciliation cycles.
    pub reconciliations: u8,
    /// Most recent receiver progress revision.
    pub revision: u32,
    /// Exact external stage to resume after operator review.
    pub stage: PipelineStage,
    pub(crate) activity: Option<PendingActivity>,
    pub(crate) timer: Option<[u8; 16]>,
    pub(crate) waiting: Option<Projection>,
    pub(crate) resume: Option<Resume>,
}
impl PipelineState {
    /// Exact receiver snapshot waiting for acknowledgment; a source receipt does not prove its delivery.
    pub fn pending_projection(&self) -> Option<&Projection> {
        self.waiting.as_ref()
    }
    /// Checks causal work, immutable evidence, and lifetime bounds before every decision.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.spec.validate()?;
        if self.run_id == [0; 16]
            || !matches!(self.version, 1 | 2)
            || self.stage_attempts > 3
            || self.activities > 32
            || self.reconciliations > 2
            || self.revision > 32
            || self.timer == Some([0; 16])
            || self.rebuilt && self.version != 2
        {
            return Err(Error::Command("invalid pipeline lifetime or budgets"));
        }
        if let Some(p) = &self.publication {
            p.validate(&self.spec)?;
        }
        if let Some(r) = &self.observed {
            r.validate()?;
            if r.deployment != self.spec.deployment()? {
                return Err(Error::Identity("pipeline target immutable input differs"));
            }
        }
        if self.phase == PipelinePhase::Working {
            if self
                .activity
                .as_ref()
                .is_none_or(|a| a.id == [0; 16] || a.stage != self.stage)
                || self.stage_attempts == 0
                || self.waiting.is_some()
                || self.resume.is_some()
            {
                return Err(Error::Command("pipeline pending Activity differs"));
            }
        } else if self.activity.is_some() {
            return Err(Error::Command("unexpected pipeline Activity"));
        }
        if self.phase == PipelinePhase::Publishing {
            let p = self
                .waiting
                .as_ref()
                .ok_or(Error::Command("missing pipeline projection"))?;
            p.validate()?;
            if p.record != self.snapshot(p.record.status)?
                || !matches!(
                    (self.resume, p.record.status),
                    (
                        Some(Resume::AwaitingApproval),
                        ReleaseStatus::AwaitingApproval
                    ) | (Some(Resume::Active), ReleaseStatus::Active)
                        | (Some(Resume::Review), ReleaseStatus::NeedsReview)
                        | (
                            Some(Resume::Done),
                            ReleaseStatus::Cancelled
                                | ReleaseStatus::RolledBack
                                | ReleaseStatus::Superseded
                                | ReleaseStatus::Refused
                        )
                )
            {
                return Err(Error::Command("pipeline projection snapshot differs"));
            }
        } else if self.waiting.is_some() || self.resume.is_some() {
            return Err(Error::Command("unexpected pipeline projection"));
        }
        if self.phase == PipelinePhase::AwaitingApproval
            && (self.approved || self.rollback_requested || self.publication.is_none())
            || self.phase == PipelinePhase::Active
                && (self.rollback_requested
                    || !self.approved
                    || self.publication.is_none()
                    || self.verified_generation.is_none()
                    || self.version == 2 && !self.rebuilt)
            || self.rebuilt && self.publication.is_none()
        {
            return Err(Error::Command("pipeline phase lacks evidence"));
        }
        if self.phase == PipelinePhase::Active {
            self.snapshot(ReleaseStatus::Active)?.validate()?;
        }
        if self.phase == PipelinePhase::Done
            && self.observed.as_ref().is_none_or(|r| {
                !matches!(
                    r.outcome,
                    TargetOutcome::Cancelled
                        | TargetOutcome::RolledBack
                        | TargetOutcome::Superseded
                        | TargetOutcome::Conflict
                )
            })
        {
            return Err(Error::Command(
                "terminal pipeline lacks external settlement",
            ));
        }
        Ok(())
    }
    pub(crate) fn snapshot(&self, status: ReleaseStatus) -> cellule_runtime::Result<ReleaseRecord> {
        Ok(ReleaseRecord {
            release: self.spec.release,
            target: self.spec.target.clone(),
            input_digest: self.spec.digest()?,
            run_id: self.run_id,
            definition_version: self.version,
            revision: self.revision,
            status,
            publication: self.publication.clone(),
            observed: self.observed.clone(),
            verified_generation: self.verified_generation,
            rebuilt: self.rebuilt,
        })
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Start {
    pub spec: ReleaseSpec,
    pub rollback_requested: bool,
}
pub(crate) fn target(
    source: &CellTarget,
    namespace: NamespaceId,
) -> cellule_runtime::Result<CellTarget> {
    CellTarget::new(
        source.tenant(),
        source.application(),
        namespace,
        &partition_for_shard(0),
    )
}
pub(crate) fn event<T: Serialize>(prefix: &[u8], value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    bytes.extend(encode(value)?);
    Ok(bytes)
}
pub(crate) fn read_event<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    prefix: &[u8],
) -> cellule_runtime::Result<T> {
    decode(
        bytes
            .strip_prefix(prefix)
            .ok_or(Error::Command("pipeline event prefix differs"))?,
    )
}
