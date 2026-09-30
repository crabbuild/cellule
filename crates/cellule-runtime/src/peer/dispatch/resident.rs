//! Resident-first resolution for the node that receives a forwarded request.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::cell::actor::{CellHandle, CellRuntime};
use crate::cell::catalog::CellCatalog;
use crate::control::authority::CellAuthority;
use crate::identity::CellTarget;
use crate::ltx::CellStorageLayout;
use crate::registry::Registry;
use crate::{Error, Result};

use super::PeerCellResolver;

/// Resolves an inbound peer target from the live actor map before storage.
///
/// A resident hit reads no catalog or authority object, which removes the
/// per-hop metadata reads a forwarded invocation used to pay on the receiver.
/// A miss falls back to the same catalog and authority reads the storage path
/// always performed, so wiring this resolver changes no execution contract:
/// the dispatcher still rechecks the target, and the actor still validates the
/// shipped description before it dispatches anything.
#[derive(Clone)]
pub struct ResidentPeerCellResolver {
    runtime: CellRuntime,
    layout: CellStorageLayout,
    registry: Arc<Registry>,
}

impl ResidentPeerCellResolver {
    /// Binds one node runtime, its Cell storage layout, and the compiled registry.
    #[must_use]
    pub fn new(runtime: CellRuntime, layout: CellStorageLayout, registry: Arc<Registry>) -> Self {
        Self {
            runtime,
            layout,
            registry,
        }
    }
}

impl PeerCellResolver for ResidentPeerCellResolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellHandle>> + Send + 'static>> {
        let resolver = self.clone();
        Box::pin(async move {
            // The registry owns the namespace role, so a resident hit needs no
            // catalog page to rebuild the local proof.
            let role = resolver
                .registry
                .namespace_contract(target.namespace())
                .map(|(_, descriptor)| descriptor.role)
                .ok_or(Error::Peer("peer target namespace is not registered"))?;
            if let Some(handle) = resolver.runtime.resident_handle(&target, role).await? {
                return Ok(handle);
            }
            let catalog = CellCatalog::new(resolver.layout.clone(), target.tenant())
                .lookup(target.cell_id())
                .await?
                .ok_or(Error::Control("target Cell is not cataloged"))?;
            let control = CellAuthority::new(resolver.layout.clone())
                .load(target.cell_id())
                .await?
                .ok_or(Error::Control("target Cell has no authority record"))?;
            resolver
                .runtime
                .local_handle(catalog, &control)
                .await?
                .ok_or(Error::CellNotActive)
        })
    }
}
