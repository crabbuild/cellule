use cellule_app::ApplicationHandle;
use cellule_cookbook_release_pipeline::{
    BoxError, ReleaseApplication, ReleaseArtifacts, ReleaseClient, TargetApplication, TargetClient,
    Targets, compile_release, compile_target, open_release, open_release_after_rollout,
};
use cellule_cookbook_support::{LocalNode, NodeConfig};
use cellule_runtime::{ApplicationId, BlobArtifactStore, TenantId};
use cellule_store::Store;
use object_store::path::Path;
use std::{path::PathBuf, sync::Arc};
const RELEASE_APP: ApplicationId = ApplicationId::from_bytes([0x36; 16]);
const RELEASE_TENANT: TenantId = TenantId::from_bytes([0x46; 16]);
const TARGET_APP: ApplicationId = ApplicationId::from_bytes([0x37; 16]);
const TARGET_TENANT: TenantId = TenantId::from_bytes([0x47; 16]);
pub(crate) struct ReleaseService<const V: u8> {
    pub(crate) node: Arc<LocalNode>,
    pub(crate) handle: ApplicationHandle<ReleaseApplication<V>>,
    pub(crate) client: ReleaseClient<V>,
    pub(crate) artifacts: ReleaseArtifacts<V>,
    pub(crate) plans: PathBuf,
}
async fn base<const V: u8>(
    state: PathBuf,
    store: Store,
    parts: Store,
) -> Result<(Arc<LocalNode>, ApplicationHandle<ReleaseApplication<V>>), BoxError> {
    let node = Arc::new(
        LocalNode::start(
            compile_release::<V>()?,
            store,
            NodeConfig {
                state_directory: state,
                storage_prefix: Path::from("cookbook/release-pipeline/cells"),
                application_id: RELEASE_APP,
            },
        )
        .await?,
    );
    let result = node.application_handle::<ReleaseApplication<V>>(RELEASE_TENANT);
    match result {
        Ok(handle) => Ok((
            node,
            handle.with_blob_artifact_store(BlobArtifactStore::new(parts)),
        )),
        Err(source) => {
            if let Err(cleanup) = node.shutdown().await {
                tracing::error!(%cleanup,"release startup drain failed");
            }
            Err(source.into())
        }
    }
}
impl<const V: u8> ReleaseService<V> {
    pub(crate) async fn start(
        state: PathBuf,
        store: Store,
        parts: Store,
    ) -> Result<Self, BoxError> {
        let plans = state.join("requests/artifacts");
        let (node, handle) = base::<V>(state, store, parts).await?;
        let result = open_release(&node, &handle).await;
        Self::finish_start(node, handle, plans, result).await
    }
    async fn finish_start(
        node: Arc<LocalNode>,
        handle: ApplicationHandle<ReleaseApplication<V>>,
        plans: PathBuf,
        result: Result<ReleaseClient<V>, cellule_cookbook_release_pipeline::ServiceError>,
    ) -> Result<Self, BoxError> {
        let opened = result
            .map_err(|source| Box::new(source) as BoxError)
            .and_then(|client| Ok((client, ReleaseArtifacts::new(handle.clone())?)));
        match opened {
            Ok((client, artifacts)) => Ok(Self {
                node,
                handle,
                client,
                artifacts,
                plans,
            }),
            Err(source) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(%cleanup,"release open failure drain failed");
                }
                Err(source)
            }
        }
    }
}
impl ReleaseService<2> {
    pub(crate) async fn rollout(
        state: PathBuf,
        store: Store,
        parts: Store,
    ) -> Result<Self, BoxError> {
        let plans = state.join("requests/artifacts");
        let (node, handle) = base::<2>(state, store, parts).await?;
        let result = open_release_after_rollout(&node, &handle).await;
        Self::finish_start(node, handle, plans, result).await
    }
}
pub(crate) struct TargetService {
    pub(crate) node: Arc<LocalNode>,
    pub(crate) client: TargetClient,
}
impl TargetService {
    pub(crate) async fn start(state: PathBuf, store: Store) -> Result<Self, BoxError> {
        let node = Arc::new(
            LocalNode::start(
                compile_target()?,
                store,
                NodeConfig {
                    state_directory: state,
                    storage_prefix: Path::from("cookbook/release-pipeline/target"),
                    application_id: TARGET_APP,
                },
            )
            .await?,
        );
        let result = async {
            let client =
                TargetClient::new(node.application_handle::<TargetApplication>(TARGET_TENANT)?);
            node.open_cell(&client.target()?, &Targets).await?;
            Ok::<_, BoxError>(client)
        }
        .await;
        match result {
            Ok(client) => Ok(Self { node, client }),
            Err(source) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(%cleanup,"target startup drain failed");
                }
                Err(source)
            }
        }
    }
}
pub(crate) fn stores() -> Result<(Store, Store), BoxError> {
    let endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
        Ok(v) => v,
        Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
        Err(source) => return Err(source.into()),
    };
    let store = cellule_cookbook_support::local_s3_store(&endpoint, "cellule-cookbook")?;
    let parts = Store::new(Arc::new(object_store::prefix::PrefixStore::new(
        store.inner().clone(),
        "cookbook/release-pipeline/artifacts",
    )));
    Ok((store, parts))
}
