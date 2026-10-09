use cellule_runtime::Error;
use serde::{Deserialize, Serialize};

/// Maximum immutable build input, before the binary artifact envelope.
pub const MAX_SOURCE_BYTES: usize = 4096;
/// Maximum built artifact, including its fixed envelope.
pub const MAX_ARTIFACT_BYTES: usize = MAX_SOURCE_BYTES + 36;
/// Permanent deployment identities retained by one local target simulator.
pub const MAX_DEPLOYMENTS: i64 = 1024;
/// Independent named deployment slots in the local target simulator.
pub const MAX_TARGETS: i64 = 64;
const MAGIC: &[u8; 16] = b"CELLULE-RELEASE\x01";

/// Nonzero permanent release identity; CLI text is exactly 32 lowercase hexadecimal digits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReleaseId([u8; 16]);
impl ReleaseId {
    /// Rejects a zero identity before application dispatch.
    pub fn from_bytes(bytes: [u8; 16]) -> cellule_runtime::Result<Self> {
        if bytes == [0; 16] {
            return Err(Error::Identity("zero release identity"));
        }
        Ok(Self(bytes))
    }
    /// Stable binary key used by both the pipeline and the independent target.
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
    pub(crate) fn validate(self) -> cellule_runtime::Result<()> {
        Self::from_bytes(self.0).map(|_| ())
    }
}
impl std::fmt::Display for ReleaseId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl std::str::FromStr for ReleaseId {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        fn digit(byte: u8) -> cellule_runtime::Result<u8> {
            match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err(Error::Identity("release identity requires lowercase hex")),
            }
        }
        if text.len() != 32 {
            return Err(Error::Identity("release identity requires 32 hex digits"));
        }
        let mut bytes = [0; 16];
        for (position, pair) in text.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            bytes[position] = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        Self::from_bytes(bytes)
    }
}

/// Canonical target name, with no URL, path, tenant, or authentication interpretation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TargetName(String);
impl TargetName {
    /// Validates a lowercase ASCII name before choosing the target slot.
    pub fn new(name: String) -> cellule_runtime::Result<Self> {
        let value = Self(name);
        value.validate()?;
        Ok(value)
    }
    /// Canonical unescaped name for a typed request or a local HTTP route segment.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub(crate) fn validate(&self) -> cellule_runtime::Result<()> {
        let bytes = self.0.as_bytes();
        if !(1..=32).contains(&bytes.len())
            || !bytes.first().is_some_and(u8::is_ascii_lowercase)
            || bytes.last() == Some(&b'-')
            || bytes.windows(2).any(|pair| pair == b"--")
            || bytes
                .iter()
                .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && *byte != b'-')
        {
            return Err(Error::Identity("noncanonical deployment target name"));
        }
        Ok(())
    }
}

/// Exact bounded content and immutable Blob key, independent of upload or Activity attempts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// Application-owned immutable key, derived from release and complete content.
    pub key: [u8; 32],
    /// BLAKE3 of the complete artifact bytes.
    pub digest: [u8; 32],
    /// Exact byte length, including the release envelope.
    pub bytes: u32,
}
impl Artifact {
    /// Deterministically compiles a local input into a version-one binary artifact.
    pub fn build(release: ReleaseId, source: &[u8]) -> cellule_runtime::Result<(Self, Vec<u8>)> {
        release.validate()?;
        if source.is_empty() || source.len() > MAX_SOURCE_BYTES {
            return Err(Error::Command("release source exceeds its declared bound"));
        }
        let mut bytes = Vec::with_capacity(source.len() + 36);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&release.bytes());
        bytes.extend_from_slice(&(source.len() as u32).to_be_bytes());
        bytes.extend_from_slice(source);
        let digest = *blake3::hash(&bytes).as_bytes();
        let value = Self {
            key: artifact_key(release, digest),
            digest,
            bytes: bytes.len() as u32,
        };
        value.verify(release, &bytes)?;
        Ok((value, bytes))
    }
    /// Checks the release-scoped immutable reference without fetching content.
    pub fn validate(&self, release: ReleaseId) -> cellule_runtime::Result<()> {
        release.validate()?;
        if self.digest == [0; 32]
            || !(37..=MAX_ARTIFACT_BYTES as u32).contains(&self.bytes)
            || self.key != artifact_key(release, self.digest)
        {
            return Err(Error::Command("invalid release artifact reference"));
        }
        Ok(())
    }
    /// Verifies content, release envelope, size, and digest before a target can install it.
    pub fn verify(&self, release: ReleaseId, bytes: &[u8]) -> cellule_runtime::Result<()> {
        self.validate(release)?;
        if bytes.len() != self.bytes as usize
            || bytes.get(..16) != Some(MAGIC.as_slice())
            || bytes.get(16..32) != Some(release.bytes().as_slice())
            || bytes.get(32..36) != Some(((bytes.len() - 36) as u32).to_be_bytes().as_slice())
            || blake3::hash(bytes).as_bytes() != &self.digest
        {
            return Err(Error::Command(
                "release artifact bytes differ from pinned input",
            ));
        }
        Ok(())
    }
}
fn artifact_key(release: ReleaseId, digest: [u8; 32]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cookbook.release.artifact.v1\0");
    hash.update(&release.bytes());
    hash.update(&digest);
    *hash.finalize().as_bytes()
}

/// Immutable target operation. Its generation precondition never changes on retry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    /// Permanent release identity, unique throughout this target simulator.
    pub release: ReleaseId,
    /// Canonical slot, independently versioned inside the target transaction domain.
    pub target: TargetName,
    /// Exact content built and published by this release.
    pub artifact: Artifact,
    /// Original generation observed before deployment intent was accepted.
    pub expected_generation: u64,
}
impl Deployment {
    /// Validates immutable input and leaves room for both deploy and rollback generations.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.release.validate()?;
        self.target.validate()?;
        self.artifact.validate(self.release)?;
        if self.expected_generation > i64::MAX as u64 - 2 {
            return Err(Error::Command(
                "deployment generation exceeds supported range",
            ));
        }
        Ok(())
    }
    /// Permanent external key, independent of native action IDs, leases, and attempts.
    pub fn operation_key(&self, rollback: bool) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cookbook.release.target-operation.v1\0");
        hash.update(&self.release.bytes());
        hash.update(&[u8::from(rollback)]);
        *hash.finalize().as_bytes()
    }
}
/// Verified artifact selected by a target, either installed or retained as a predecessor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    /// Release that produced these exact bytes.
    pub release: ReleaseId,
    /// Pinned content reference.
    pub artifact: Artifact,
}
impl Selection {
    pub(crate) fn validate(&self) -> cellule_runtime::Result<()> {
        self.artifact.validate(self.release)
    }
}
/// Current independently committed target slot; an empty target still retains its generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetState {
    /// Exact canonical slot.
    pub target: TargetName,
    /// Monotonic target generation, incremented only by an applied deploy or rollback.
    pub generation: u64,
    /// Current content or an explicitly empty target.
    pub selected: Option<Selection>,
}
impl TargetState {
    /// Checks the target's identity and retained content evidence.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.target.validate()?;
        if self.generation > i64::MAX as u64 || self.generation == 0 && self.selected.is_some() {
            return Err(Error::Command("invalid target generation"));
        }
        if let Some(value) = &self.selected {
            value.validate()?;
        }
        Ok(())
    }
}
/// External target action; rollback carries the original immutable deployment, not a new version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetAction {
    /// Install only these verified artifact bytes.
    Deploy(Vec<u8>),
    /// Restore the captured predecessor only if this deployment still owns the slot.
    Rollback,
}
/// Target mutation authenticated and bounded by the embedding process before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetWork {
    /// Full permanent request binding.
    pub deployment: Deployment,
    /// Requested external mutation.
    pub action: TargetAction,
}
impl TargetWork {
    /// Rejects changed or malformed content before any target publication.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.deployment.validate()?;
        if let TargetAction::Deploy(bytes) = &self.action {
            self.deployment
                .artifact
                .verify(self.deployment.release, bytes)?;
        }
        Ok(())
    }
}
/// Permanent independently durable outcome, observable after a dropped target reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetOutcome {
    /// Exact artifact installed once at its captured generation.
    Deployed,
    /// Rollback arrived before deployment; a tombstone prevents delayed installation.
    Cancelled,
    /// Captured predecessor restored once and target generation advanced.
    RolledBack,
    /// A newer target generation prevents compensation from changing its content.
    Superseded,
    /// Original target generation did not match, or a caller changed permanent input.
    Conflict,
    /// The simulator's permanent history or named-slot capacity is exhausted.
    Capacity,
}
/// Target-local permanent operation evidence, distinct from Workflow and SQL projection state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetRecord {
    /// Original immutable input, including generation and complete artifact identity.
    pub deployment: Deployment,
    /// Current permanent external result.
    pub outcome: TargetOutcome,
    /// Generation at which deployment was installed, absent for rejected or cancelled work.
    pub installed_generation: Option<u64>,
    /// Exact prior selection captured by the same deployment transaction.
    pub previous: Option<Selection>,
    /// Applied installation count; repeated calls never increment it.
    pub deploys: u32,
    /// Applied compensation or pre-deployment tombstone count.
    pub rollbacks: u32,
}
impl TargetRecord {
    /// Checks settlement evidence without claiming the target still selects this release.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.deployment.validate()?;
        if let Some(previous) = &self.previous {
            previous.validate()?;
            if previous.release == self.deployment.release
                || self.deployment.expected_generation == 0
            {
                return Err(Error::Identity("deployment cannot be its own predecessor"));
            }
        }
        let installed = self.installed_generation == Some(self.deployment.expected_generation + 1);
        let valid = match self.outcome {
            TargetOutcome::Deployed | TargetOutcome::Superseded => {
                installed && self.deploys == 1 && self.rollbacks == 0
            }
            TargetOutcome::RolledBack => installed && self.deploys == 1 && self.rollbacks == 1,
            TargetOutcome::Cancelled => {
                self.installed_generation.is_none()
                    && self.previous.is_none()
                    && self.deploys == 0
                    && self.rollbacks == 1
            }
            TargetOutcome::Conflict => {
                self.installed_generation.is_none()
                    && self.previous.is_none()
                    && self.deploys == 0
                    && self.rollbacks == 0
            }
            TargetOutcome::Capacity => false,
        };
        if !valid {
            return Err(Error::Command(
                "invalid target operation settlement evidence",
            ));
        }
        Ok(())
    }
}

pub(crate) fn encode<T: Serialize>(value: &T) -> cellule_runtime::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 32768 {
        return Err(Error::Command("release value exceeds storage bound"));
    }
    Ok(bytes)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> cellule_runtime::Result<T> {
    if bytes.len() > 32768 {
        return Err(Error::Command("release value exceeds storage bound"));
    }
    Ok(serde_json::from_slice(bytes)?)
}
