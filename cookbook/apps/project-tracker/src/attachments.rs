use crate::{
    ATTACHMENTS, AttachmentDescriptor, AttachmentLink, AttachmentObject, AttachmentPublication,
    Attachments, BoxError, ChangeOutcome, LinkAttachment, MAX_ATTACHMENT_BYTES, ProjectClient,
    ProjectTracker, model, wire,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    BlobNamespace, CellTarget, Committed, Error, InvocationError, MutationIdentity, Observed,
    PendingMutation, PreparedCommand, Receipt, Resolution,
    identity::RequestId,
    primitives::blob::{
        BlobCommand, BlobCondition, BlobMutation, BlobMutationOutcome, BlobQuery, BlobQueryResult,
    },
};
use serde::{Deserialize, Serialize};
const CONTENT_TYPE: &str = "application/vnd.cellule.project-tracker.attachment.v1";

/// Exact native request window retained before dispatch; recovery never refreshes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedIdentity {
    /// Nonzero request ID.
    pub request: [u8; 16],
    /// Original issue time.
    pub issued_at_ms: i64,
    /// Original five-minute evidence/admission window.
    pub expires_at_ms: i64,
}
impl From<MutationIdentity> for RetainedIdentity {
    fn from(value: MutationIdentity) -> Self {
        Self {
            request: *value.request_id.as_bytes(),
            issued_at_ms: value.issued_at_ms,
            expires_at_ms: value.expires_at_ms,
        }
    }
}
impl RetainedIdentity {
    /// Restores the exact nonzero identity and bounded original window.
    pub fn native(&self) -> cellule_runtime::Result<MutationIdentity> {
        if self.request == [0; 16]
            || self.issued_at_ms <= 0
            || self.issued_at_ms.checked_add(300000) != Some(self.expires_at_ms)
        {
            return Err(Error::Identity("invalid retained tracker identity"));
        }
        Ok(MutationIdentity {
            request_id: RequestId::from_bytes(self.request),
            issued_at_ms: self.issued_at_ms,
            expires_at_ms: self.expires_at_ms,
        })
    }
}
/// Immutable source bytes, complete binding, original issue precondition, and all publication/link identities.
/// The embedding must save and sync this plan before dispatching any native phase.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentPlan {
    /// Persisted application plan contract.
    pub version: u8,
    /// Full permanent identity and actual content binding.
    pub descriptor: AttachmentDescriptor,
    /// Exact original complete bytes, bounded to 64 KiB.
    pub bytes: Vec<u8>,
    /// Original issue revision; reconciliation preserves it.
    pub expected_revision: i64,
    /// Independent staging identity.
    pub upload: [u8; 16],
    /// Original one-hour staging lifetime.
    pub upload_expires_at_ms: i64,
    /// Begin, part, complete, then source link identities.
    pub identities: [RetainedIdentity; 4],
}
impl AttachmentPlan {
    /// Freezes exact bytes and all identities without dispatching or linking anything.
    pub fn new(
        descriptor: AttachmentDescriptor,
        bytes: Vec<u8>,
        expected_revision: i64,
    ) -> Result<Self, BoxError> {
        descriptor.verify(&bytes)?;
        model::revision(expected_revision, false)?;
        let identities: [RetainedIdentity; 4] = [
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
            cellule_cookbook_support::new_identity()?.into(),
        ];
        let value = Self {
            version: 1,
            descriptor,
            bytes,
            expected_revision,
            upload: *cellule_cookbook_support::new_identity()?
                .request_id
                .as_bytes(),
            upload_expires_at_ms: identities[0]
                .issued_at_ms
                .checked_add(3600000)
                .ok_or(Error::Identity("tracker staging expiry overflow"))?,
            identities,
        };
        value.validate()?;
        Ok(value)
    }
    /// Validates unchanged bytes, distinct phase identities, and the original revision/window.
    pub fn validate(&self) -> cellule_runtime::Result<()> {
        self.descriptor.verify(&self.bytes)?;
        model::revision(self.expected_revision, false)?;
        if self.version != 1
            || self.upload == [0; 16]
            || self.identities[0].issued_at_ms.checked_add(3600000)
                != Some(self.upload_expires_at_ms)
        {
            return Err(Error::Identity("invalid retained tracker attachment plan"));
        }
        for (index, identity) in self.identities.iter().enumerate() {
            identity.native()?;
            if identity.request == self.upload
                || self.identities[..index]
                    .iter()
                    .any(|old| old.request == identity.request)
            {
                return Err(Error::Identity(
                    "tracker attachment identities must be distinct",
                ));
            }
        }
        Ok(())
    }
}
/// Exact native staged-publication phase; part storage alone never links an issue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentPhase {
    /// Begins a missing-key conditional immutable manifest.
    Begin,
    /// Stores the complete bounded immutable part.
    Part,
    /// Publishes the immutable one-part manifest.
    Complete,
}
/// Authorized tenant's private Blob facade and cross-Cell linking coordinator.
#[derive(Clone)]
pub struct AttachmentClient {
    handle: ApplicationHandle<ProjectTracker>,
    blobs: BlobNamespace<Attachments>,
}
impl AttachmentClient {
    /// Binds the embedding's authorized tenant and private native artifact store.
    pub fn new(handle: ApplicationHandle<ProjectTracker>) -> cellule_runtime::Result<Self> {
        Ok(Self {
            blobs: handle.blob::<Attachments>()?,
            handle,
        })
    }
    /// Attachment receipt domain, independent from project and dashboard Cells.
    pub fn target(&self) -> cellule_runtime::Result<CellTarget> {
        self.handle
            .target_for_scope(ATTACHMENTS, b"private-attachments")
    }
    /// Prepares exact phase evidence before dispatch, without generating replacement requests.
    pub async fn prepare(
        &self,
        plan: &AttachmentPlan,
        phase: AttachmentPhase,
    ) -> Result<PreparedCommand<BlobCommand<Attachments>>, InvocationError<BlobMutationOutcome>>
    {
        plan.validate().map_err(InvocationError::NotStarted)?;
        let key = plan.descriptor.key().to_vec();
        let (index, mutation) = match phase {
            AttachmentPhase::Begin => (
                0,
                BlobMutation::Begin {
                    key,
                    upload_id: plan.upload,
                    condition: BlobCondition::Missing,
                    content_type: Some(CONTENT_TYPE.into()),
                    metadata: wire::encode(&plan.descriptor, 1024)
                        .map_err(InvocationError::NotStarted)?,
                    expires_at_ms: plan.upload_expires_at_ms,
                },
            ),
            AttachmentPhase::Part => (
                1,
                BlobMutation::PutPart {
                    key,
                    upload_id: plan.upload,
                    part_number: 1,
                    payload: plan.bytes.clone(),
                },
            ),
            AttachmentPhase::Complete => (
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
                plan.identities[index]
                    .native()
                    .map_err(InvocationError::NotStarted)?,
                mutation,
            )
            .await
    }
    /// Resolves original phase evidence in this Blob Cell; it never issues a replacement mutation.
    pub async fn resolve_phase(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        if pending.target() != &self.target().map_err(InvocationError::NotStarted)? {
            return Err(InvocationError::NotStarted(Error::Identity(
                "foreign tracker attachment phase evidence",
            )));
        }
        self.handle.resolve(pending).await
    }
    /// Verifies native metadata, manifest version, complete content, permanent identity, and byte digest.
    pub async fn read(
        &self,
        key: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<AttachmentObject>>, BoxError> {
        if key == [0; 32] {
            return Err(Error::Identity("zero tracker attachment key").into());
        }
        let value = self
            .blobs
            .query(
                BlobQuery::Read {
                    key: key.to_vec(),
                    offset: 0,
                    limit: MAX_ATTACHMENT_BYTES as u32,
                },
                minimum,
            )
            .await?;
        let BlobQueryResult::Read(range) = value.output else {
            return Err(Error::Command("unexpected tracker attachment query result").into());
        };
        let output = range
            .map(|range| -> Result<AttachmentObject, BoxError> {
                let descriptor: AttachmentDescriptor =
                    wire::decode(&range.metadata.metadata, 1024)?;
                descriptor.verify(&range.bytes)?;
                if descriptor.key() != key
                    || range.metadata.key != key
                    || range.offset != 0
                    || range.metadata.size != range.bytes.len() as u64
                    || range.metadata.part_count != 1
                    || range.metadata.content_type.as_deref() != Some(CONTENT_TYPE)
                    || range.metadata.etag == [0; 32]
                {
                    return Err(Error::Command(
                        "tracker manifest differs from complete immutable content",
                    )
                    .into());
                }
                Ok(AttachmentObject {
                    publication: AttachmentPublication {
                        descriptor,
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
    async fn existing(&self, plan: &AttachmentPlan) -> Result<Option<AttachmentObject>, BoxError> {
        let object = self.read(plan.descriptor.key(), None).await?.output;
        if object
            .as_ref()
            .is_some_and(|v| v.publication.descriptor != plan.descriptor || v.bytes != plan.bytes)
        {
            return Err(Error::Command(
                "tracker attachment identity already binds different content or metadata",
            )
            .into());
        }
        Ok(object)
    }
    /// Publishes only the Blob. An interrupted source link remains explicitly recoverable.
    /// Concurrent identical publication succeeds only after verifying the complete native winner.
    pub async fn publish(&self, plan: &AttachmentPlan) -> Result<AttachmentObject, BoxError> {
        plan.validate()?;
        if let Some(object) = self.existing(plan).await? {
            return Ok(object);
        }
        for phase in [
            AttachmentPhase::Begin,
            AttachmentPhase::Part,
            AttachmentPhase::Complete,
        ] {
            match self.prepare(plan, phase).await?.execute().await {
                Ok(value)
                    if phase != AttachmentPhase::Complete
                        || matches!(value.output, BlobMutationOutcome::Committed { .. }) => {}
                Ok(_) => {
                    return Err(
                        Error::Command("tracker completion did not publish a manifest").into(),
                    );
                }
                Err(source) => {
                    if matches!(&source,InvocationError::Rejected(value) if value.output==BlobMutationOutcome::Conflict)
                        && let Some(object) = self.existing(plan).await?
                    {
                        return Ok(object);
                    }
                    return Err(source.into());
                }
            }
        }
        self.existing(plan)
            .await?
            .ok_or_else(|| Error::Command("published tracker attachment absent").into())
    }
    /// Verifies the published object, then freezes the original source-link command and evidence.
    /// Immutable manifests and the absence of deletion keep this verified reference stable.
    pub async fn prepare_link(
        &self,
        plan: &AttachmentPlan,
    ) -> Result<PreparedCommand<LinkAttachment>, BoxError> {
        plan.validate()?;
        let object = self.existing(plan).await?.ok_or(Error::Command(
            "attachment is not published; issue was not linked",
        ))?;
        let project = ProjectClient::new(self.handle.clone(), plan.descriptor.project.clone())?;
        Ok(project
            .prepare_verified_link(
                plan.identities[3].native()?,
                AttachmentLink {
                    expected_revision: plan.expected_revision,
                    publication: object.publication,
                },
            )
            .await?)
    }
    /// Reconciles publication-before-link with the original identity and issue revision.
    /// A durable conflict is returned unchanged; callers explicitly choose any later edit.
    pub async fn reconcile(
        &self,
        plan: &AttachmentPlan,
    ) -> Result<Committed<ChangeOutcome>, BoxError> {
        Ok(self.prepare_link(plan).await?.execute().await?)
    }
    /// Resolves the exact original link without redispatch; expiry proves no absence.
    pub async fn resolve_link(&self, plan: &AttachmentPlan) -> Result<Resolution, BoxError> {
        plan.validate()?;
        if plan.identities[3].expires_at_ms <= cellule_cookbook_support::now_ms()? {
            return Ok(Resolution::Expired);
        }
        let prepared = self.prepare_link(plan).await?;
        let project = ProjectClient::new(self.handle.clone(), plan.descriptor.project.clone())?;
        Ok(project.resolve(prepared.evidence()).await?)
    }
}
