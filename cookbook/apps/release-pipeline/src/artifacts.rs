use crate::{
    Artifact, ArtifactObject, ArtifactPublication, Artifacts, BoxError, MAX_ARTIFACT_BYTES,
    ReleaseApplication, ReleaseId,
    model::{decode, encode},
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    BlobNamespace, CellTarget, Error, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution,
    identity::RequestId,
    primitives::blob::{
        BlobCommand, BlobCondition, BlobMutation, BlobMutationOutcome, BlobQuery, BlobQueryResult,
    },
};
use serde::{Deserialize, Serialize};
const CONTENT_TYPE: &str = "application/vnd.cellule.cookbook-release.v1";
/// Retainable exact request window for one Blob publication phase.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadIdentity {
    /// Nonzero native request identity.
    pub request: [u8; 16],
    /// Original issue time; retries never refresh it.
    pub issued_at_ms: i64,
    /// Original five-minute resolution window.
    pub expires_at_ms: i64,
}
impl From<MutationIdentity> for UploadIdentity {
    fn from(value: MutationIdentity) -> Self {
        Self {
            request: *value.request_id.as_bytes(),
            issued_at_ms: value.issued_at_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}
impl UploadIdentity {
    /// Restores the exact native identity after validating its bounded window.
    pub fn native(&self) -> cellule_runtime::Result<MutationIdentity> {
        if self.request == [0; 16]
            || self.issued_at_ms <= 0
            || self.issued_at_ms.checked_add(300000) != Some(self.expires_at_ms)
        {
            return Err(Error::Identity("invalid retained artifact phase identity"));
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(self.request),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
/// Immutable build bytes and original staging requests, frozen before any Cell dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUpload {
    /// Version of this application-owned retained plan.
    pub version: u8,
    /// Permanent release whose envelope and immutable key are verified.
    pub release: ReleaseId,
    /// Exact complete content reference.
    pub artifact: Artifact,
    /// Actual binary content, at most 4132 bytes.
    pub bytes: Vec<u8>,
    /// Native staging identity, independent of the immutable manifest key.
    pub upload: [u8; 16],
    /// Original bounded staging lifetime.
    pub upload_expires_ms: i64,
    /// Begin, part, and completion identities in order.
    pub phases: [UploadIdentity; 3],
}
impl ArtifactUpload {
    /// Freezes deterministic build bytes and every native phase identity.
    pub fn new(release: ReleaseId, source: &[u8]) -> Result<Self, BoxError> {
        let (artifact, bytes) = Artifact::build(release, source)?;
        let phases: [UploadIdentity; 3] = [
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
        ];
        let value = Self {
            version: 1,
            release,
            artifact,
            bytes,
            upload: *cellule_cookbook_support::new_identity()?
                .request_id
                .as_bytes(),
            upload_expires_ms: phases[0]
                .issued_at_ms
                .checked_add(3600000)
                .ok_or(Error::Identity("artifact upload expiry overflow"))?,
            phases,
        };
        value.validate()?;
        Ok(value)
    }
    /// Verifies retained content and all distinct original request identities.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.artifact.verify(self.release, &self.bytes)?;
        if self.version != 1
            || self.upload == [0; 16]
            || self.phases[0].issued_at_ms.checked_add(3600000) != Some(self.upload_expires_ms)
        {
            return Err(Error::Identity("invalid retained artifact upload"));
        }
        for (index, phase) in self.phases.iter().enumerate() {
            phase.native()?;
            if phase.request == self.upload
                || self.phases[..index]
                    .iter()
                    .any(|old| old.request == phase.request)
            {
                return Err(Error::Identity(
                    "artifact phase identities must be distinct",
                ));
            }
        }
        Ok(())
    }
}
/// Exact native staged publication phase; prepare it before retaining dispatch evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactPhase {
    /// Freeze an absent-key conditional manifest and staging metadata.
    Begin,
    /// Stage immutable bytes; this alone never exposes an artifact.
    Part,
    /// Publish the single-part immutable manifest.
    Complete,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u8,
    release: ReleaseId,
    artifact: Artifact,
}
/// Authorized private native Blob capabilities, without filesystem or HTTP policy.
#[derive(Clone)]
pub struct ReleaseArtifacts<const V: u8> {
    handle: ApplicationHandle<ReleaseApplication<V>>,
    blobs: BlobNamespace<Artifacts>,
}
impl<const V: u8> ReleaseArtifacts<V> {
    /// Binds the embedding's private artifact store and tenant.
    pub fn new(handle: ApplicationHandle<ReleaseApplication<V>>) -> cellule_runtime::Result<Self> {
        Ok(Self {
            blobs: handle.blob::<Artifacts>()?,
            handle,
        })
    }
    /// Returns the declared artifact Cell, separate from Workflow and progress receipts.
    pub fn target(&self) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(crate::ARTIFACTS, b"fixed-release-shard")
    }
    /// Freezes exact staged command evidence before native publication.
    pub async fn prepare(
        &self,
        plan: &ArtifactUpload,
        phase: ArtifactPhase,
    ) -> Result<PreparedCommand<BlobCommand<Artifacts>>, InvocationError<BlobMutationOutcome>> {
        plan.validate().map_err(InvocationError::NotStarted)?;
        let key = plan.artifact.key.to_vec();
        let (index, mutation) = match phase {
            ArtifactPhase::Begin => (
                0,
                BlobMutation::Begin {
                    key,
                    upload_id: plan.upload,
                    condition: BlobCondition::Missing,
                    content_type: Some(CONTENT_TYPE.into()),
                    metadata: encode(&Manifest {
                        version: 1,
                        release: plan.release,
                        artifact: plan.artifact.clone(),
                    })
                    .map_err(InvocationError::NotStarted)?,
                    expires_at_ms: plan.upload_expires_ms,
                },
            ),
            ArtifactPhase::Part => (
                1,
                BlobMutation::PutPart {
                    key,
                    upload_id: plan.upload,
                    part_number: 1,
                    payload: plan.bytes.clone(),
                },
            ),
            ArtifactPhase::Complete => (
                2,
                BlobMutation::Complete {
                    key,
                    upload_id: plan.upload,
                    part_count: 1,
                },
            ),
        };
        self.blobs
            .prepare_mutation(
                plan.phases[index]
                    .native()
                    .map_err(InvocationError::NotStarted)?,
                mutation,
            )
            .await
    }
    /// Resolves one original artifact phase; foreign receipt domains are rejected locally.
    pub async fn resolve(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if evidence.target() != &self.target().map_err(InvocationError::NotStarted)? {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign release artifact evidence",
            )));
        }
        self.handle.resolve(evidence).await
    }
    /// Verifies the published native manifest, complete range, application envelope, and full digest.
    /// Staged part bytes remain invisible until native completion publishes the manifest.
    pub async fn read(
        &self,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<ArtifactObject>>, BoxError> {
        if key == [0; 32] {
            return Err(Error::Identity("zero release artifact key").into());
        }
        let value = self
            .blobs
            .query(
                BlobQuery::Read {
                    key: key.to_vec(),
                    offset: 0,
                    limit: MAX_ARTIFACT_BYTES as u32,
                },
                minimum,
            )
            .await?;
        let BlobQueryResult::Read(range) = value.output else {
            return Err(Error::Command("unexpected release Blob read result").into());
        };
        let output = range
            .map(|range| -> Result<ArtifactObject, BoxError> {
                let manifest: Manifest = decode(&range.metadata.metadata)?;
                manifest.artifact.verify(manifest.release, &range.bytes)?;
                if manifest.version != 1
                    || manifest.artifact.key != key
                    || range.offset != 0
                    || range.metadata.key != key
                    || range.metadata.size != range.bytes.len() as u64
                    || range.metadata.part_count != 1
                    || range.metadata.content_type.as_deref() != Some(CONTENT_TYPE)
                    || range.metadata.etag == [0; 32]
                {
                    return Err(Error::Command(
                        "published release Blob manifest differs from content",
                    )
                    .into());
                }
                Ok(ArtifactObject {
                    publication: ArtifactPublication {
                        artifact: manifest.artifact,
                        etag: range.metadata.etag,
                    },
                    bytes: range.bytes,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: value.receipt,
            output,
        })
    }
    async fn verify_existing(
        &self,
        plan: &ArtifactUpload,
    ) -> Result<Option<ArtifactObject>, BoxError> {
        let output = self.read(plan.artifact.key, None).await?.output;
        if let Some(value) = &output
            && (value.bytes != plan.bytes || value.publication.artifact != plan.artifact)
        {
            return Err(Error::Command("immutable release artifact binding differs").into());
        }
        Ok(output)
    }
    /// Replays a retained upload through the one native durability path, or verifies an existing winner.
    /// Unknown publication remains an error; only a verified manifest proves build completion.
    pub async fn publish(&self, plan: &ArtifactUpload) -> Result<ArtifactObject, BoxError> {
        plan.validate()?;
        if let Some(value) = self.verify_existing(plan).await? {
            return Ok(value);
        }
        for phase in [
            ArtifactPhase::Begin,
            ArtifactPhase::Part,
            ArtifactPhase::Complete,
        ] {
            let result = self.prepare(plan, phase).await?.execute().await;
            match result {
                Ok(value)
                    if phase != ArtifactPhase::Complete
                        || matches!(value.output, BlobMutationOutcome::Committed { .. }) => {}
                Ok(_) => {
                    return Err(
                        Error::Command("release completion did not publish a manifest").into(),
                    );
                }
                Err(source) => {
                    if matches!(&source,InvocationError::Rejected(value) if value.output==BlobMutationOutcome::Conflict)
                        && let Some(value) = self.verify_existing(plan).await?
                    {
                        return Ok(value);
                    }
                    return Err(source.into());
                }
            }
        }
        self.verify_existing(plan)
            .await?
            .ok_or_else(|| Error::Command("published release artifact absent").into())
    }
}
