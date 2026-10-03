use cellule_app::ApplicationHandle;
use cellule_cookbook_project_tracker::{
    AttachmentClient, Attachments, BoxError, Dashboard, DashboardClient, ProjectClient, ProjectKey,
    ProjectTracker, Projects, compile,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store};
use cellule_runtime::{ApplicationId, BlobArtifactStore, TenantId};
use cellule_store::Store;
use object_store::path::Path;
use std::{path::PathBuf, sync::Arc};
const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x38; 16]);
pub(crate) fn tenant(name: &str) -> Result<TenantId, BoxError> {
    ProjectKey::new(name)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-cookbook-project-tracker/tenant/v1\0");
    hash.update(name.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    if bytes == [0; 16] {
        return Err("invalid zero tracker tenant identity".into());
    }
    Ok(TenantId::from_bytes(bytes))
}
pub(crate) struct Service {
    pub(crate) node: Arc<LocalNode>,
    pub(crate) handle: ApplicationHandle<ProjectTracker>,
}
impl Service {
    pub(crate) async fn start(state: PathBuf, name: &str) -> Result<Self, BoxError> {
        let tenant = tenant(name)?;
        let endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
            Ok(v) => v,
            Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
            Err(source) => return Err(source.into()),
        };
        let store = local_s3_store(&endpoint, "cellule-cookbook")?;
        let parts = Store::new(Arc::new(object_store::prefix::PrefixStore::new(
            store.inner().clone(),
            "cookbook/project-tracker/parts",
        )));
        let node = Arc::new(
            LocalNode::start(
                compile()?,
                store,
                NodeConfig {
                    state_directory: state,
                    storage_prefix: Path::from("cookbook/project-tracker/cells"),
                    application_id: APPLICATION,
                },
            )
            .await?,
        );
        match node.application_handle::<ProjectTracker>(tenant) {
            Ok(handle) => Ok(Self {
                node,
                handle: handle.with_blob_artifact_store(BlobArtifactStore::new(parts)),
            }),
            Err(source) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(%cleanup,"tracker startup drain failed");
                }
                Err(source.into())
            }
        }
    }
    pub(crate) async fn project(&self, key: ProjectKey) -> Result<ProjectClient, BoxError> {
        let client = ProjectClient::new(self.handle.clone(), key)?;
        self.node.open_cell(client.target(), &Projects).await?;
        Ok(client)
    }
    pub(crate) async fn dashboard(&self) -> Result<DashboardClient, BoxError> {
        let client = DashboardClient::new(self.handle.clone())?;
        self.node.open_cell(client.target(), &Dashboard).await?;
        Ok(client)
    }
    pub(crate) async fn attachments(&self) -> Result<AttachmentClient, BoxError> {
        let client = AttachmentClient::new(self.handle.clone())?;
        self.node.open_cell(&client.target()?, &Attachments).await?;
        Ok(client)
    }
}
