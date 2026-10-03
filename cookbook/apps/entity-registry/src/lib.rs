//! Stable device entities with atomic state/Effect publication and monotonic projections.
//! The embedding application owns authorization, enrollment, its source roster, and transport.
mod application;
mod domain;
mod model;
mod service;
pub use application::{DEVICES, DIRECTORY, Devices, Directory, EntityRegistry, compile};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, InvocationError, MutationIdentity, Observed, PendingMutation,
    PreparedCommand, Receipt, Resolution,
    codec::{BoundedDecoder, WireValue},
    primitives::effects::EffectState,
};
pub use domain::{ChangeDevice, GetDevice, ListDirectory, LookupDevice, ProjectDevice};
pub use model::{
    Attributes, Change, ChangeOutcome, Device, DeviceKey, Page, PageRequest, Progress,
    ProjectionOutcome, ProjectionState, PublishedDevice,
};
pub use service::{DeliveryOptions, DeliveryProgress, ServiceError, spawn_delivery};

/// Progress errors retain typed source-read and native ledger evidence.
#[derive(Debug, thiserror::Error)]
pub enum ProgressError {
    /// Device source read failure.
    #[error(transparent)]
    Source(#[from] InvocationError<Option<PublishedDevice>>),
    /// Native Effect status read failure.
    #[error(transparent)]
    Ledger(#[from] InvocationError<Option<cellule_runtime::primitives::effects::EffectStatus>>),
    /// Concurrent edits prevented a coherent observation within three attempts.
    #[error("device changed during bounded progress observation; retry the read")]
    Changed,
    /// Invalid native capability or source contract.
    #[error(transparent)]
    Runtime(#[from] cellule_runtime::Error),
}

/// Scoped authoritative device client. Construct only after application authorization.
#[derive(Clone)]
pub struct DeviceClient {
    handle: ApplicationHandle<EntityRegistry>,
    key: DeviceKey,
    target: CellTarget,
}
impl DeviceClient {
    /// Binds the authenticated tenant and application to one canonical device key.
    pub fn new(
        handle: ApplicationHandle<EntityRegistry>,
        key: DeviceKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(DEVICES, key.as_bytes())?;
        Ok(Self {
            handle,
            key,
            target,
        })
    }
    /// Stable target for application-controlled provisioning and ownership.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Validates and freezes a mutation before dispatch; retain its native evidence on cancellation.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<PreparedCommand<ChangeDevice>, InvocationError<ChangeOutcome>> {
        change
            .validate()
            .map_err(|error| InvocationError::NotStarted(cellule_runtime::Error::from(error)))?;
        if change.key != self.key {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("change targets a different device key"),
            ));
        }
        self.handle
            .prepare_command::<ChangeDevice>(&self.target, identity, change)
            .await
    }
    /// Publishes state and intent together. This receipt does not confirm directory publication.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<Committed<ChangeOutcome>, InvocationError<ChangeOutcome>> {
        self.prepare(identity, change).await?.execute().await
    }
    /// Reads source state at or beyond a receipt from this device Cell.
    pub async fn get(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<PublishedDevice>>, InvocationError<Option<PublishedDevice>>> {
        self.handle
            .query::<GetDevice>(&self.target, minimum, ())
            .await
    }
    /// Resolves retained evidence for this exact source, without issuing a new mutation.
    pub async fn resolve(
        &self,
        pending: &PendingMutation,
    ) -> Result<Resolution, InvocationError<Vec<u8>>> {
        // The native handle validates tenant/application. Also pin the narrower entity capability.
        if pending.target() != &self.target {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign device mutation evidence"),
            ));
        }
        self.handle.resolve(pending).await
    }
    /// Inspects the latest intent at its source; missing evidence is explicitly unavailable.
    pub async fn progress(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Progress>>, ProgressError> {
        for _ in 0..3 {
            let read = self.get(minimum).await?;
            let Some(published) = read.output else {
                return Ok(Observed {
                    output: None,
                    receipt: read.receipt,
                });
            };
            let ledger = self
                .handle
                .effects::<Devices>(self.target.clone())?
                .status(published.effect_id, Some(read.receipt))
                .await?;
            // Monotonic revisions and fresh intent identities prohibit an ABA. If the
            // state still matches after the status read, it was also current at the
            // intervening ledger receipt. Return that coherent source observation.
            let verified = self.get(Some(ledger.receipt)).await?;
            if verified.output.as_ref() != Some(&published) {
                continue;
            }
            let (state, attempts) =
                match ledger.output {
                    None => (ProjectionState::Unavailable, None),
                    Some(status) => (
                        match status.state {
                            EffectState::Ready | EffectState::Leased => ProjectionState::Pending,
                            EffectState::Delivered => {
                                // Native settlement retains receiver business rejections as answers.
                                // Projection success therefore requires decoding that durable answer.
                                let bytes = status.result.as_deref().ok_or(
                                    cellule_runtime::Error::Command(
                                        "settled projection has no receiver result",
                                    ),
                                )?;
                                let mut decoder = BoundedDecoder::new(bytes, 16)
                                    .map_err(cellule_runtime::Error::from)?;
                                let result = ProjectionOutcome::decode(&mut decoder)
                                    .map_err(cellule_runtime::Error::from)?;
                                decoder.finish().map_err(cellule_runtime::Error::from)?;
                                match result {
                                    ProjectionOutcome::Applied
                                    | ProjectionOutcome::Unchanged
                                    | ProjectionOutcome::Stale => ProjectionState::Delivered,
                                    ProjectionOutcome::Conflict | ProjectionOutcome::Capacity => {
                                        ProjectionState::Failed
                                    }
                                }
                            }
                            EffectState::Failed => ProjectionState::Failed,
                        },
                        Some(status.attempt),
                    ),
                };
            return Ok(Observed {
                output: Some(Progress {
                    published,
                    state,
                    attempts,
                }),
                receipt: ledger.receipt,
            });
        }
        Err(ProgressError::Changed)
    }
}
/// Scoped eventually consistent directory client with bounded keyset reads.
#[derive(Clone)]
pub struct DirectoryClient {
    handle: ApplicationHandle<EntityRegistry>,
    target: CellTarget,
}
impl DirectoryClient {
    /// Selects the one declared directory within the authorized tenant/application.
    pub fn new(handle: ApplicationHandle<EntityRegistry>) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(DIRECTORY, b"directory")?;
        Ok(Self { handle, target })
    }
    /// Directory target for explicit application provisioning.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }
    /// Looks up projected state, using only a directory receipt as a minimum.
    pub async fn lookup(
        &self,
        key: DeviceKey,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Device>>, InvocationError<Option<Device>>> {
        self.handle
            .query::<LookupDevice>(&self.target, minimum, key)
            .await
    }
    /// Reads 1..100 projected devices; each page observes its own directory commit.
    pub async fn list(
        &self,
        page: PageRequest,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Page>, InvocationError<Page>> {
        self.handle
            .query::<ListDirectory>(&self.target, minimum, page)
            .await
    }
}
