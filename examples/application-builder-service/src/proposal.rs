//! Application-owned prototype of the proposed conveniences, using current APIs.

use std::sync::Arc;

use cellule_app::{
    ApplicationBuilder, ApplicationHandle, CellApplication, CellType, CompiledApplication,
};
use cellule_cookbook_support::{LocalNode, NodeConfig};
use cellule_runtime::{CellModule, Error, NamespaceId, TenantId};
use cellule_store::Store;

use crate::{
    AppResult,
    application::{ORDERS, Orders, OrdersApp},
};

pub struct CellBinding {
    namespace: NamespaceId,
    name: &'static str,
    database_bytes: u64,
    capture_bytes: u64,
}

impl CellBinding {
    // This prototype deliberately supports fixed-shard partition version one.
    pub fn sharded(namespace: NamespaceId, name: &'static str) -> Self {
        Self {
            namespace,
            name,
            database_bytes: 64 << 20,
            capture_bytes: 16 << 20,
        }
    }

    pub fn with_limits(mut self, database_bytes: u64, capture_bytes: u64) -> Self {
        self.database_bytes = database_bytes;
        self.capture_bytes = capture_bytes;
        self
    }
}

pub trait ApplicationBuilderExt {
    fn module<M: CellModule>(
        &mut self,
        module: M,
        bindings: impl IntoIterator<Item = CellBinding>,
    ) -> cellule_runtime::Result<()>;
}

impl ApplicationBuilderExt for ApplicationBuilder {
    fn module<M: CellModule>(
        &mut self,
        module: M,
        bindings: impl IntoIterator<Item = CellBinding>,
    ) -> cellule_runtime::Result<()> {
        let descriptor = module.descriptor();
        let cells: Vec<CellType> = bindings
            .into_iter()
            .map(|binding| {
                let namespace = descriptor
                    .namespaces
                    .iter()
                    .find(|namespace| namespace.id == binding.namespace)
                    .ok_or(Error::Registry(
                        "Cell binding namespace is absent from module",
                    ))?;
                CellType::new(
                    M::NAME,
                    binding.name,
                    namespace.id,
                    namespace.role,
                    namespace.shards,
                )?
                .with_schema_range(descriptor.schema_min, descriptor.schema_max)?
                .with_limits(binding.database_bytes, binding.capture_bytes)
            })
            .collect::<cellule_runtime::Result<_>>()?;
        self.register(module)?;
        for cell in cells {
            self.cell_type(cell)?;
        }
        // ApplicationBuilder::finish remains the canonical full-contract check.
        Ok(())
    }
}

pub struct ServiceNode {
    application: Arc<CompiledApplication>,
    node: LocalNode,
}

impl ServiceNode {
    pub async fn start(
        application: Arc<CompiledApplication>,
        store: Store,
        config: NodeConfig,
        provisioned_tenants: &[TenantId],
    ) -> AppResult<Self> {
        // LocalNode owns the only CellNode/runtime, directory enrollment,
        // renewable lease and supervised task group.
        let node = LocalNode::start(application.clone(), store, config).await?;
        let setup: AppResult<()> = async {
            for tenant in provisioned_tenants {
                let app = node.application_handle::<OrdersApp>(*tenant)?;
                let target = app.target_for_scope(ORDERS, b"orders")?;
                node.open_cell(&target, &Orders).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = setup {
            if let Err(cleanup) = node.shutdown().await {
                eprintln!("startup cleanup failed: {cleanup}");
            }
            return Err(error);
        }
        Ok(Self { application, node })
    }

    pub fn application(&self) -> &CompiledApplication {
        &self.application
    }

    pub fn scope<A: CellApplication>(
        &self,
        authorized_tenant: TenantId,
    ) -> cellule_cookbook_support::Result<ApplicationHandle<A>> {
        self.node.application_handle(authorized_tenant)
    }

    pub fn is_ready(&self) -> bool {
        self.node.is_ready()
    }

    pub async fn shutdown(&self) -> cellule_cookbook_support::Result<()> {
        // LocalNode drains accepted work with lease renewal still live, then
        // its lease-maintenance task withdraws the latest directory generation.
        self.node.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_runtime::{BuildDescriptor, CatalogRole, Digest};

    #[test]
    fn combined_registration_preserves_canonical_application_and_release_bytes()
    -> cellule_runtime::Result<()> {
        let build = BuildDescriptor {
            source_revision: "builder-example-test".into(),
            cargo_lock_digest: Digest::from_bytes([9; 32]),
        };
        let proposed = OrdersApp::compile(build.clone())?;
        let mut legacy = ApplicationBuilder::new(OrdersApp::NAME, build)?;
        legacy.register(Orders)?;
        legacy.cell_type(
            CellType::new(Orders::NAME, "orders", ORDERS, CatalogRole::Sql, 1)?
                .with_limits(64 << 20, 16 << 20)?,
        )?;
        let legacy = legacy.finish()?;
        assert_eq!(proposed.descriptor_bytes(), legacy.descriptor_bytes());
        assert_eq!(
            proposed.registry().release_bytes(),
            legacy.registry().release_bytes()
        );
        Ok(())
    }
}
