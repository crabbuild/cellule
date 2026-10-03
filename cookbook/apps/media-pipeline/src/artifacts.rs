use crate::{Artifact, Identity, MAX_BYTES, MediaApplication, Results, Sources, dimensions};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    BlobModule, BlobNamespace, CellTarget, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::blob::{
        BlobCondition, BlobMutation, BlobMutationOutcome, BlobQuery, BlobQueryResult,
    },
};
use serde::{Deserialize, Serialize};
/// Namespace role; sources and generated results have independent receipt domains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    /// Content-addressed immutable uploaded PNGs.
    Source,
    /// Transformation-addressed immutable generated PNGs.
    Result,
}
/// Frozen one-part upload plan. Retain before dispatch and replay the original identities.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upload {
    /// Private application namespace role.
    pub kind: Kind,
    /// Canonical key, content-addressed for source or transformation-addressed for result.
    pub key: [u8; 32],
    /// Original upload identity.
    pub upload: [u8; 16],
    /// Exact frozen PNG bytes.
    pub bytes: Vec<u8>,
    /// Begin, part, and completion identities, in order.
    pub phases: [Identity; 3],
    /// Original staging lifetime.
    pub upload_expires_ms: i64,
}
impl Upload {
    /// Freezes a bounded validated PNG and all command identities before external publication.
    pub fn new(kind: Kind, key: [u8; 32], bytes: Vec<u8>) -> Result<Self, crate::BoxError> {
        dimensions(&bytes)?;
        if kind == Kind::Source && *blake3::hash(&bytes).as_bytes() != key {
            return Err("source key must match full PNG digest".into());
        }
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
            kind,
            key,
            upload: *uuid::Uuid::now_v7().as_bytes(),
            bytes,
            phases,
            upload_expires_ms,
        })
    }
    /// Checks retained bytes, identity ordering, and resource limits before contacting storage.
    pub fn validate(&self) -> Result<(), crate::BoxError> {
        dimensions(&self.bytes)?;
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
        if self.kind == Kind::Source && *blake3::hash(&self.bytes).as_bytes() != self.key {
            return Err("retained source digest differs".into());
        }
        Ok(())
    }
}
/// Immutable artifact capabilities; embeddings install a private BlobArtifactStore first.
#[derive(Clone)]
pub struct Artifacts {
    handle: ApplicationHandle<MediaApplication>,
}
impl Artifacts {
    /// Binds the authorized application's source and result namespaces.
    pub fn new(handle: ApplicationHandle<MediaApplication>) -> Self {
        Self { handle }
    }
    /// Resolves a stable target without acquiring ownership.
    pub fn target(&self, kind: Kind, key: &[u8; 32]) -> cellule_runtime::Result<CellTarget> {
        self.handle.target_for_scope(
            match kind {
                Kind::Source => crate::SOURCES,
                Kind::Result => crate::RESULTS,
            },
            key,
        )
    }
    /// Reads a full bounded immutable artifact with native part verification.
    pub async fn read(
        &self,
        kind: Kind,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<(Artifact, Vec<u8>)>>, crate::BoxError> {
        match kind {
            Kind::Source => self.read_from::<Sources>(key, minimum).await,
            Kind::Result => self.read_from::<Results>(key, minimum).await,
        }
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
            return Err("unexpected media Blob range".into());
        };
        let output = range
            .map(|range| {
                if range.offset != 0
                    || range.metadata.key != key
                    || range.metadata.size != range.bytes.len() as u64
                    || range.bytes.is_empty()
                    || range.bytes.len() > MAX_BYTES
                    || range.metadata.part_count != 1
                    || range.metadata.content_type.as_deref() != Some("image/png")
                {
                    return Err("stored media artifact exceeds declared bounds".into());
                }
                let (width, height) = dimensions(&range.bytes)?;
                Ok::<_, crate::BoxError>((
                    Artifact {
                        key,
                        digest: *blake3::hash(&range.bytes).as_bytes(),
                        etag: range.metadata.etag,
                        bytes: range.metadata.size,
                        width,
                        height,
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
        if let Some((artifact, bytes)) = self.read(plan.kind, plan.key, None).await?.output {
            if bytes != plan.bytes {
                return Err("immutable artifact key is already bound to different bytes".into());
            }
            return Ok(artifact);
        }
        match plan.kind {
            Kind::Source => self.publish_to::<Sources>(plan).await,
            Kind::Result => self.publish_to::<Results>(plan).await,
        }
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
                content_type: Some("image/png".into()),
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
            Ok(_) => return Err("thumbnail completion did not publish a manifest".into()),
            Err(source) => return Err(source.into()),
        }
        let (artifact, bytes) = self
            .read(plan.kind, plan.key, None)
            .await?
            .output
            .ok_or("published media manifest disappeared")?;
        if bytes != plan.bytes {
            return Err("published artifact differs from retained PNG".into());
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
