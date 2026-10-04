//! Application-owned startup and provisioning policy over native framework bindings.

use cellule_app::{ApplicationBinding, ApplicationHandle, CompiledApplication};
use cellule_cookbook_support::{LocalNode, NodeConfig};
use cellule_runtime::TenantId;
use cellule_store::Store;
use std::sync::Arc;

use crate::{
    AppResult,
    application::{ORDERS, Orders, OrdersApp},
};

pub struct ServiceNode {
    binding: ApplicationBinding<OrdersApp>,
    node: LocalNode,
}

impl ServiceNode {
    pub async fn start(
        application: Arc<CompiledApplication>,
        store: Store,
        config: NodeConfig,
        provisioned_tenants: &[TenantId],
    ) -> AppResult<Self> {
        let node = LocalNode::start(application, store, config).await?;
        let setup: AppResult<ApplicationBinding<OrdersApp>> = async {
            let binding = node.application_binding::<OrdersApp>()?;
            for tenant in provisioned_tenants {
                let target = binding.scope(*tenant).target_for_scope(ORDERS, b"orders")?;
                node.open_cell(&target, &Orders).await?;
            }
            Ok(binding)
        }
        .await;
        let binding = match setup {
            Ok(binding) => binding,
            Err(error) => {
                if let Err(cleanup) = node.shutdown().await {
                    eprintln!("startup cleanup failed: {cleanup}");
                }
                return Err(error);
            }
        };
        Ok(Self { binding, node })
    }

    pub fn application(&self) -> &CompiledApplication {
        self.binding.compiled()
    }

    pub fn scope(&self, authorized_tenant: TenantId) -> ApplicationHandle<OrdersApp> {
        self.binding.scope(authorized_tenant)
    }

    pub fn is_ready(&self) -> bool {
        self.node.is_ready()
    }

    pub async fn shutdown(&self) -> cellule_cookbook_support::Result<()> {
        self.node.shutdown().await
    }
}
