//! Typed application factories over the node's canonical resources.

use super::*;
use cellule_runtime::ltx::CellStorageLayout;

impl CellNode {
    /// Binds a product-configured client to this node's compiled application.
    ///
    /// Construct once and create handles with [`ApplicationBinding::scope`] after
    /// authorizing each tenant. A different application type or registry is refused.
    pub fn bind_application<A: CellApplication>(
        &self,
        client: CellClient,
        application: ApplicationId,
    ) -> cellule_runtime::Result<ApplicationBinding<A>> {
        ApplicationBinding::new(client, Arc::clone(&self.application), application)
    }

    /// Binds a local application factory to the node's existing runtime and storage layout.
    ///
    /// The application supplies an already constructed authoritative layout.
    /// Its application ID determines all scoped targets, avoiding a separate
    /// identity argument. This does not start tasks, acquire Cells or open readiness.
    /// The node remains the sole runtime owner and controls admission and drain.
    pub fn bind_local_application<A: CellApplication>(
        &self,
        layout: CellStorageLayout,
    ) -> cellule_runtime::Result<ApplicationBinding<A>> {
        let application = ApplicationId::from_bytes(*layout.application_id());
        self.bind_application(
            CellClient::local_runtime(self.application.registry(), self.runtime.clone(), layout),
            application,
        )
    }
}
