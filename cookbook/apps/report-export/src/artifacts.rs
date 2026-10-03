use crate::{Artifact, ExportApplication, Files, Identity, MAX_BYTES};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    BlobModule, BlobNamespace, CellTarget, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::blob::{
        BlobCondition, BlobMutation, BlobMutationOutcome, BlobQuery, BlobQueryResult,
    },
};
use serde::{Deserialize, Serialize};
/// Frozen one-part upload plan. Retain before dispatch and replay the original identities.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upload {
    /// Deterministic business key for a page or final sealed-version report.
    pub key: [u8; 32],
    /// Original upload identity.
    pub upload: [u8; 16],
    /// Exact frozen CSV bytes.
    pub bytes: Vec<u8>,
    /// Begin, part, and completion identities, in order.
    pub phases: [Identity; 3],
    /// Original staging lifetime.
    pub upload_expires_ms: i64,
}
impl Upload {
    /// Freezes bounded UTF-8 CSV bytes and command identities; ExportEngine verifies records.
    pub fn new(key: [u8; 32], bytes: Vec<u8>) -> Result<Self, crate::BoxError> {
        validate_bytes(&bytes)?;
        let phases: [Identity; 3] = [
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
        ];
        let upload_expires_ms = phases[0]
            .issued_at_ms
            .checked_add(3_600_000)
            .ok_or("upload deadline overflow")?;
        Ok(Self {
            key,
            upload: *uuid::Uuid::now_v7().as_bytes(),
            bytes,
            phases,
            upload_expires_ms,
        })
    }
    /// Checks retained bytes, identity ordering, and resource limits before contacting storage.
    pub fn validate(&self) -> Result<(), crate::BoxError> {
        validate_bytes(&self.bytes)?;
        if self.upload == [0; 16]
            || self.upload_expires_ms <= 0
            || self.phases[0].issued_at_ms.checked_add(3_600_000) != Some(self.upload_expires_ms)
            || self.phases.iter().any(|phase| {
                phase.request == [0; 16]
                    || phase.expires_at_ms <= phase.issued_at_ms
                    || phase
                        .expires_at_ms
                        .checked_sub(phase.issued_at_ms)
                        .is_none_or(|window| window > 300_000)
            })
            || self.phases[0].request == self.phases[1].request
            || self.phases[0].request == self.phases[2].request
            || self.phases[1].request == self.phases[2].request
        {
            return Err("invalid retained upload identity".into());
        }
        Ok(())
    }
}
/// Immutable artifact capabilities; embeddings install a private BlobArtifactStore first.
#[derive(Clone)]
pub struct Artifacts {
    handle: ApplicationHandle<ExportApplication>,
}
impl Artifacts {
    /// Binds the authorized application's immutable CSV namespace.
    pub fn new(handle: ApplicationHandle<ExportApplication>) -> Self {
        Self { handle }
    }
    /// Resolves a stable target without acquiring ownership.
    pub fn target(&self, key: &[u8; 32]) -> cellule_runtime::Result<CellTarget> {
        self.handle.target_for_scope(crate::FILES, key)
    }
    /// Reads a full bounded immutable artifact with native part verification.
    pub async fn read(
        &self,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<(Artifact, Vec<u8>)>>, crate::BoxError> {
        self.read_from::<Files>(key, minimum).await
    }
    async fn read_from<M: BlobModule>(
        &self,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<(Artifact, Vec<u8>)>>, crate::BoxError> {
        let blobs = self.handle.blob::<M>()?;
        let observed = blobs
            .query(
                BlobQuery::Read {
                    key: key.to_vec(),
                    offset: 0,
                    limit: MAX_BYTES as u32,
                },
                minimum,
            )
            .await?;
        let BlobQueryResult::Read(range) = observed.output else {
            return Err("unexpected export Blob range".into());
        };
        let output = range
            .map(|range| {
                if range.offset != 0
                    || range.metadata.key != key
                    || range.metadata.size != range.bytes.len() as u64
                    || range.bytes.is_empty()
                    || range.bytes.len() > MAX_BYTES
                    || range.metadata.part_count != 1
                    || range.metadata.content_type.as_deref() != Some("text/csv; charset=utf-8")
                {
                    return Err("stored export artifact exceeds declared bounds".into());
                }
                validate_bytes(&range.bytes)?;
                Ok::<_, crate::BoxError>((
                    Artifact {
                        key,
                        digest: *blake3::hash(&range.bytes).as_bytes(),
                        etag: range.metadata.etag,
                        bytes: range.metadata.size as u32,
                    },
                    range.bytes,
                ))
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output,
        })
    }
    /// Publishes through native staged Blob commands or verifies an identical existing artifact.
    /// A later retry can use fresh command identities after inspecting the immutable business key.
    /// Unknown invocation errors remain errors and never imply absence.
    pub async fn publish(&self, plan: &Upload) -> Result<Artifact, crate::BoxError> {
        plan.validate()?;
        if let Some((artifact, bytes)) = self.read(plan.key, None).await?.output {
            if bytes != plan.bytes {
                return Err("immutable artifact key is already bound to different bytes".into());
            }
            return Ok(artifact);
        }
        self.publish_to::<Files>(plan).await
    }
    async fn publish_to<M: BlobModule>(&self, plan: &Upload) -> Result<Artifact, crate::BoxError> {
        let blobs = self.handle.blob::<M>()?;
        phase(
            &blobs,
            plan.phases[0].native(),
            BlobMutation::Begin {
                key: plan.key.to_vec(),
                upload_id: plan.upload,
                condition: BlobCondition::Missing,
                content_type: Some("text/csv; charset=utf-8".into()),
                metadata: vec![],
                expires_at_ms: plan.upload_expires_ms,
            },
        )
        .await?;
        phase(
            &blobs,
            plan.phases[1].native(),
            BlobMutation::PutPart {
                key: plan.key.to_vec(),
                upload_id: plan.upload,
                part_number: 1,
                payload: plan.bytes.clone(),
            },
        )
        .await?;
        let outcome = phase(
            &blobs,
            plan.phases[2].native(),
            BlobMutation::Complete {
                key: plan.key.to_vec(),
                upload_id: plan.upload,
                part_count: 1,
            },
        )
        .await;
        match outcome {
            Ok(BlobMutationOutcome::Committed { .. }) => {}
            // An independently retained plan may have won the Missing condition. Verify its bytes.
            Err(source) if matches!(&source,InvocationError::Rejected(value) if value.output==BlobMutationOutcome::Conflict) =>
                {}
            Ok(_) => return Err("CSV completion did not publish a manifest".into()),
            Err(source) => return Err(source.into()),
        }
        let (artifact, bytes) = self
            .read(plan.key, None)
            .await?
            .output
            .ok_or("published export manifest disappeared")?;
        if bytes != plan.bytes {
            return Err("published artifact differs from retained CSV".into());
        }
        Ok(artifact)
    }
}
async fn phase<M: BlobModule>(
    blobs: &BlobNamespace<M>,
    identity: MutationIdentity,
    mutation: BlobMutation,
) -> Result<BlobMutationOutcome, InvocationError<BlobMutationOutcome>> {
    Ok(blobs
        .prepare_mutation(identity, mutation)
        .await?
        .execute()
        .await?
        .output)
}

fn validate_bytes(bytes: &[u8]) -> Result<(), crate::BoxError> {
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err("CSV artifact must be 1..256 KiB".into());
    }
    std::str::from_utf8(bytes)?;
    Ok(())
}
