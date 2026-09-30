//! In-process routing for Cells admitted after a client was created.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::*;
use crate::cell::actor::CellRuntime;
use crate::cell::catalog::{CatalogProof, CellCatalog};
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::fleet::telemetry::RouteCacheOutcome;
use crate::identity::CellId;
use crate::ltx::CellStorageLayout;

/// Maximum local routes retained by one client transport.
///
/// One entry covers one Cell this process currently owns, so the bound matches
/// the documented per-node open-Cell target rather than one entry per target.
const MAX_LOCAL_ROUTES: usize = 4_096;

/// How long one cached local route is trusted without re-reading authority.
///
/// The bound stays inside the three-second owner renewal cadence and well
/// inside the ten-second self-fence threshold. A route that lost ownership is
/// revalidated from storage at least this often, and every dispatch is still
/// fenced by the actor, so the cache only ever removes redundant reads.
const LOCAL_ROUTE_TTL: Duration = Duration::from_secs(2);

/// Selects a local owner before an invocation can be forwarded to a peer.
///
/// The product may acquire an idle, cataloged Cell through runtime admission.
/// Returning `None` delegates to the remote transport; errors stop dispatch.
pub trait LocalCellResolver: Send + Sync + 'static {
    /// Returns the authorized local owner, or `None` when routing must continue remotely.
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CellHandle>>> + Send + 'static>>;
}

#[derive(Clone)]
pub(super) struct RuntimeCellTransport {
    registry: Arc<Registry>,
    resolver: Arc<dyn LocalCellResolver>,
    remote: Option<Arc<dyn CellTransport>>,
}

impl RuntimeCellTransport {
    pub(super) fn new(
        registry: Arc<Registry>,
        runtime: CellRuntime,
        layout: CellStorageLayout,
    ) -> Self {
        Self {
            registry,
            resolver: Arc::new(RuntimeLocalResolver::new(runtime, layout, false)),
            remote: None,
        }
    }

    pub(super) fn with_resolver(
        registry: Arc<Registry>,
        resolver: Arc<dyn LocalCellResolver>,
        remote: Arc<dyn CellTransport>,
    ) -> Self {
        Self {
            registry,
            resolver,
            remote: Some(remote),
        }
    }

    async fn owner(&self, target: &CellTarget) -> Result<Arc<dyn CellTransport>> {
        let local = self.resolver.resolve(target.clone()).await?;
        let Some(handle) = local else {
            return self
                .remote
                .clone()
                .ok_or(Error::Control("target Cell is not locally owned"));
        };
        Ok(Arc::new(LocalCellTransport {
            registry: self.registry.clone(),
            handles: Arc::new(HashMap::from([(handle.cell_id(), handle.clone())])),
            handle,
            telemetry: CellTelemetryHandle::default(),
        }))
    }
}

/// One verified local route: the catalog proof and control it was read from.
struct LocalRoute {
    catalog: CatalogProof,
    control: VersionedControl,
    observed_at: Instant,
}

/// Bounded, short-lived cache of verified local ownership routes.
///
/// A hit only skips the catalog and authority reads that produced the route.
/// The actor still revalidates the caller's expected description on every
/// dispatch, so a route that outlived its ownership fails closed and is
/// replaced by a storage read instead of executing against a stale owner.
#[derive(Default)]
struct LocalRoutes {
    routes: Mutex<HashMap<CellId, LocalRoute>>,
}

impl LocalRoutes {
    fn get(&self, cell: CellId) -> Option<(CatalogProof, VersionedControl)> {
        let routes = self.routes.lock().ok()?;
        let route = routes.get(&cell)?;
        if route.observed_at.elapsed() >= LOCAL_ROUTE_TTL {
            return None;
        }
        Some((route.catalog.clone(), route.control.clone()))
    }

    fn insert(&self, cell: CellId, catalog: CatalogProof, control: &VersionedControl) {
        let Ok(mut routes) = self.routes.lock() else {
            return;
        };
        if routes.len() >= MAX_LOCAL_ROUTES && !routes.contains_key(&cell) {
            // One entry covers one locally owned Cell. Evicting an arbitrary
            // route only costs the next caller a storage read.
            let Some(evicted) = routes.keys().next().copied() else {
                return;
            };
            routes.remove(&evicted);
        }
        routes.insert(
            cell,
            LocalRoute {
                catalog,
                control: control.clone(),
                observed_at: Instant::now(),
            },
        );
    }

    fn remove(&self, cell: CellId) {
        if let Ok(mut routes) = self.routes.lock() {
            routes.remove(&cell);
        }
    }
}

pub(super) struct RuntimeLocalResolver {
    runtime: CellRuntime,
    layout: CellStorageLayout,
    remote_on_miss: bool,
    routes: Arc<LocalRoutes>,
}

impl RuntimeLocalResolver {
    pub(super) fn new(
        runtime: CellRuntime,
        layout: CellStorageLayout,
        remote_on_miss: bool,
    ) -> Self {
        Self {
            runtime,
            layout,
            remote_on_miss,
            routes: Arc::new(LocalRoutes::default()),
        }
    }

    fn telemetry(&self) -> &CellTelemetryHandle {
        self.runtime.telemetry()
    }
}

impl Clone for RuntimeLocalResolver {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            layout: self.layout.clone(),
            remote_on_miss: self.remote_on_miss,
            routes: Arc::clone(&self.routes),
        }
    }
}

impl LocalCellResolver for RuntimeLocalResolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Option<CellHandle>>> + Send + 'static>> {
        let resolver = self.clone();
        Box::pin(async move {
            let cell = target.cell_id();
            if let Some((catalog, control)) = resolver.routes.get(cell) {
                if let Some(handle) = resolver.runtime.local_handle(catalog, &control).await? {
                    // The cached route still matches the live actor: no storage read.
                    resolver.telemetry().route_cache(RouteCacheOutcome::Hit);
                    return Ok(Some(handle));
                }
                // The actor no longer matches the cached route. Drop it so the
                // next call re-reads authority instead of repeating this probe.
                resolver.routes.remove(cell);
            }
            if resolver.remote_on_miss && !resolver.runtime.has_local_owner(cell).await? {
                // No local actor can execute this request. The peer route
                // resolves remote ownership and its receiver fences stale hints.
                resolver.telemetry().route_cache(RouteCacheOutcome::Miss);
                return Ok(None);
            }
            let catalog = CellCatalog::new(resolver.layout.clone(), target.tenant());
            let authority = CellAuthority::new(resolver.layout.clone());
            let (catalog, control) = tokio::join!(catalog.lookup(cell), authority.load(cell));
            let catalog = catalog?.ok_or(Error::Control("target Cell is not cataloged"))?;
            let control = control?.ok_or(Error::Control("target Cell has no authority record"))?;
            resolver.telemetry().route_cache(RouteCacheOutcome::Miss);
            resolver.routes.insert(cell, catalog.clone(), &control);
            resolver.runtime.local_handle(catalog, &control).await
        })
    }
}

impl CellTransport for RuntimeCellTransport {
    fn describe(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellDescription>> + Send + 'static>> {
        let client = self.clone();
        Box::pin(async move { client.owner(&target).await?.describe(target).await })
    }

    fn command(
        &self,
        command: EncodedCommand,
    ) -> Pin<Box<dyn Future<Output = Result<StoredOutcome>> + Send + 'static>> {
        let client = self.clone();
        Box::pin(async move { client.owner(&command.target).await?.command(command).await })
    }

    fn query(
        &self,
        query: EncodedQuery,
    ) -> Pin<Box<dyn Future<Output = Result<EncodedObservation>> + Send + 'static>> {
        let client = self.clone();
        Box::pin(async move { client.owner(&query.target).await?.query(query).await })
    }

    fn resolve(
        &self,
        resolve: EncodedResolve,
    ) -> Pin<Box<dyn Future<Output = Result<Resolution>> + Send + 'static>> {
        let client = self.clone();
        Box::pin(async move { client.owner(&resolve.target).await?.resolve(resolve).await })
    }
}
