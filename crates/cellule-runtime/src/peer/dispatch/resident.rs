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
use crate::cell::actor::routes::{ResidentRoute, ResidentRoutes};

/// Resolves an inbound peer target from the live actor map before storage.
///
/// A resident hit under a live node lease reads no catalog or authority object.
/// Object-only runtimes reuse catalog identity but always check fresh authority.
/// A miss falls back to the same catalog and authority reads the storage path
/// always performed, so wiring this resolver changes no execution contract:
/// the dispatcher still rechecks the target, and the actor still validates the
/// shipped description before it dispatches anything.
#[derive(Clone)]
pub struct ResidentPeerCellResolver {
    runtime: CellRuntime,
    authority: Arc<CellAuthority>,
    registry: Arc<Registry>,
    routes: ResidentRoutes,
}

impl ResidentPeerCellResolver {
    /// Binds one node runtime, its Cell storage layout, and the compiled registry.
    #[must_use]
    pub fn new(runtime: CellRuntime, layout: CellStorageLayout, registry: Arc<Registry>) -> Self {
        Self {
            routes: ResidentRoutes::new(runtime.clone()),
            runtime,
            // Clones share a stateless reader; every unleased resolve still loads
            // fresh authority rather than caching an ownership observation.
            authority: Arc::new(CellAuthority::new(layout)),
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
            match resolver
                .routes
                .resolve(&target, role, &resolver.authority)
                .await?
            {
                ResidentRoute::Owned(handle) => return Ok(handle),
                ResidentRoute::NotOwned => return Err(Error::CellNotActive),
                ResidentRoute::Missing => {}
            }
            let catalog = CellCatalog::new(resolver.authority.layout().clone(), target.tenant());
            let (catalog, control) = tokio::join!(
                catalog.lookup(target.cell_id()),
                resolver.authority.load(target.cell_id())
            );
            let catalog = catalog?.ok_or(Error::Control("target Cell is not cataloged"))?;
            let control = control?.ok_or(Error::Control("target Cell has no authority record"))?;
            resolver
                .runtime
                .local_handle(catalog, &control)
                .await?
                .ok_or(Error::CellNotActive)
        })
    }
}
