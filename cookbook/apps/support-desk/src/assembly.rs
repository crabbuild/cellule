use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store};
use cellule_cookbook_support_desk::*;
use cellule_runtime::{ApplicationId, BlobArtifactStore, TenantId};
use cellule_store::Store;
use object_store::path::Path;
use std::{path::PathBuf, sync::Arc};

const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x3a; 16]);

pub(crate) fn tenant(name: &str) -> Result<TenantId, BoxError> {
    TicketKey::new(name)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-cookbook-support-desk/tenant/v1\0");
    hash.update(name.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    if bytes == [0; 16] {
        return Err("zero support-desk tenant identity".into());
    }
    Ok(TenantId::from_bytes(bytes))
}

#[derive(Clone)]
pub(crate) struct Service {
    pub(crate) source: Arc<LocalNode>,
    pub(crate) coordinator: Arc<LocalNode>,
    pub(crate) handle: ApplicationHandle<SupportDesk>,
    pub(crate) coordination: ApplicationHandle<SupportDesk>,
}
impl Service {
    pub(crate) async fn start(state: PathBuf, name: &str) -> Result<Self, BoxError> {
        let tenant = tenant(name)?;
        let endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
            Err(source) => return Err(source.into()),
        };
        let store = local_s3_store(&endpoint, "cellule-cookbook")?;
        let parts = Store::new(Arc::new(object_store::prefix::PrefixStore::new(
            store.inner().clone(),
            "cookbook/support-desk/parts",
        )));
        let compiled = compile()?;
        let source = Arc::new(
            LocalNode::start(
                compiled.clone(),
                store.clone(),
                NodeConfig {
                    state_directory: state.join("source"),
                    storage_prefix: Path::from("cookbook/support-desk/cells"),
                    application_id: APPLICATION,
                },
            )
            .await?,
        );
        let result = async {
            let coordinator = Arc::new(
                LocalNode::start(
                    compiled,
                    store,
                    NodeConfig {
                        state_directory: state.join("coordination"),
                        storage_prefix: Path::from("cookbook/support-desk/cells"),
                        application_id: APPLICATION,
                    },
                )
                .await?,
            );
            match (
                source.application_handle::<SupportDesk>(tenant),
                coordinator.application_handle::<SupportDesk>(tenant),
            ) {
                (Ok(handle), Ok(coordination)) => Ok(Self {
                    source: source.clone(),
                    coordinator,
                    handle: handle.with_blob_artifact_store(BlobArtifactStore::new(parts)),
                    coordination,
                }),
                (left, right) => {
                    if let Err(cleanup) = coordinator.shutdown().await {
                        tracing::error!(%cleanup,"support coordinator startup drain failed");
                    }
                    Err(BoxError::from(
                        left.err()
                            .or_else(|| right.err())
                            .ok_or("missing support scope error")?,
                    ))
                }
            }
        }
        .await;
        if result.is_err()
            && let Err(cleanup) = source.shutdown().await
        {
            tracing::error!(%cleanup,"support source startup drain failed");
        }
        result
    }

    pub(crate) async fn ticket(&self, key: TicketKey) -> Result<TicketClient, BoxError> {
        let client = TicketClient::new(self.handle.clone(), key)?;
        self.source.open_cell(client.target(), &Tickets).await?;
        Ok(client)
    }

    pub(crate) async fn attachments(&self) -> Result<AttachmentClient, BoxError> {
        let client = AttachmentClient::new(self.handle.clone())?;
        self.source
            .open_cell(&client.target()?, &Attachments)
            .await?;
        Ok(client)
    }

    pub(crate) async fn coordinated(&self, key: TicketKey) -> Result<CoordinationClient, BoxError> {
        for (namespace, scope) in [
            (DEADLINES, b"deadlines".as_slice()),
            (NOTIFICATIONS, b"notifications".as_slice()),
        ] {
            let target = self.coordination.target_for_scope(namespace, scope)?;
            if namespace == DEADLINES {
                self.coordinator.open_cell(&target, &Deadlines).await?;
            } else {
                self.coordinator.open_cell(&target, &Notifications).await?;
            }
        }
        Ok(CoordinationClient::new(self.coordination.clone(), key)?)
    }

    pub(crate) async fn workers(
        &self,
        keys: &[TicketKey],
        options: DeliveryOptions,
    ) -> Result<(), BoxError> {
        spawn_coordination(
            &self.source,
            &self.coordinator,
            self.handle.clone(),
            self.coordination.clone(),
            keys,
            options,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn shutdown(&self) -> Result<(), BoxError> {
        // Every callback/Activity runner belongs to the source. Keep the
        // independently leased Workflow host alive until accepted work settles.
        let source = self.source.shutdown().await;
        let coordinator = self.coordinator.shutdown().await;
        if let Err(source) = source {
            if let Err(cleanup) = coordinator {
                tracing::error!(%cleanup,"support coordinator drain also failed");
            }
            return Err(source.into());
        }
        coordinator.map_err(Into::into)
    }
}
