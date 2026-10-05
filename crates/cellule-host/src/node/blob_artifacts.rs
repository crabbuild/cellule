//! Original Blob I/O ownership through the existing ordered node drain.

use super::*;
use cellule_runtime::primitives::blob::BlobArtifactStore;

impl CellNode {
    /// Retains a configured Blob store before readiness and returns its shared
    /// capability for `CellClient::with_blob_artifact_store`.
    ///
    /// The existing drain lane closes every clone and joins original accepted
    /// operations before runtime shutdown. A deadline or cancelled drain waiter
    /// cannot cancel those operations. This local barrier does not establish
    /// Cell-scoped upload/pin coverage, remote success or global GC authority.
    pub fn install_blob_artifact_store(
        &self,
        store: BlobArtifactStore,
    ) -> cellule_runtime::Result<BlobArtifactStore> {
        self.require_task_group()?;
        if store.lifecycle_observation()?.admission_closed() {
            return Err(Error::CellDraining);
        }
        let retained = Arc::new(store.clone());
        self.install_owned_component_with_drain(BLOB_ARTIFACT_STORE_COMPONENT, retained, {
            let store = store.clone();
            move || {
                let store = store.clone();
                async move {
                    store.close_and_join().await.map(|_| ()).map_err(|source| {
                        Box::new(source) as Box<dyn std::error::Error + Send + Sync>
                    })
                }
            }
        })?;
        Ok(store)
    }
}
