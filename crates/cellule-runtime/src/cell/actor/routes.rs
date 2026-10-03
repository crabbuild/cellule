//! Resident catalog reuse, with lease or fresh-authority ownership checks.

use super::*;
use crate::control::authority::CellAuthority;
use std::sync::Mutex;

const MAX_ROUTES: usize = 4_096;

#[derive(Clone)]
pub(crate) struct ResidentRoutes {
    runtime: CellRuntime,
    handles: Arc<Mutex<HashMap<CellId, CellHandle>>>,
}

// Keep the capability inline: boxing it would allocate on every invocation.
#[expect(
    clippy::large_enum_variant,
    reason = "hot-path result carries an inline capability"
)]
pub(crate) enum ResidentRoute {
    Owned(CellHandle),
    NotOwned,
    Missing,
}

impl ResidentRoutes {
    pub(crate) fn new(runtime: CellRuntime) -> Self {
        Self {
            runtime,
            handles: Arc::default(),
        }
    }

    pub(crate) fn cached(&self, target: &CellTarget) -> crate::Result<Option<CellHandle>> {
        if matches!(
            self.runtime.inner.node_lease.as_ref(),
            RuntimeNodeLease::ObjectOnly
        ) {
            self.runtime.ensure_running()?;
            // An unleased capability is only a catalog hint. It never grants
            // ownership without a new authority observation for this request.
            return Ok(None);
        }
        self.candidate(target)
    }

    fn candidate(&self, target: &CellTarget) -> crate::Result<Option<CellHandle>> {
        self.runtime.ensure_running()?;
        let cell = target.cell_id();
        let cached = self.handles.lock().ok().and_then(|mut handles| {
            let handle = handles.get(&cell)?;
            if !handle.admission.fenced.load(Ordering::Acquire)
                && !handle.admission.draining.load(Ordering::Acquire)
            {
                return Some(handle.clone());
            }
            handles.remove(&cell);
            None
        });
        if let Some(handle) = cached {
            // Drain, migration and fencing close the exact admission token.
            // Dispatch still checks it again; this cache grants no SQL admission.
            self.runtime.ensure_running()?;
            return Ok(Some(handle));
        }
        Ok(None)
    }

    pub(crate) async fn resolve(
        &self,
        target: &CellTarget,
        role: CatalogRole,
        authority: &CellAuthority,
    ) -> crate::Result<ResidentRoute> {
        let cell = target.cell_id();
        let unleased = matches!(
            self.runtime.inner.node_lease.as_ref(),
            RuntimeNodeLease::ObjectOnly
        );
        let handle = match self.candidate(target)? {
            Some(handle) if !unleased => return Ok(ResidentRoute::Owned(handle)),
            Some(handle) => handle,
            None => {
                let Some(handle) = self.runtime.resident_handle(target, role).await? else {
                    return Ok(ResidentRoute::Missing);
                };
                handle
            }
        };
        let handle = if unleased {
            let current = authority
                .load(cell)
                .await?
                .ok_or(Error::Control("target Cell has no authority record"))?;
            // The actor already verified this immutable catalog identity at
            // activation. Reuse it, but never cache the ownership observation.
            // The lookup rechecks current actor identity/admission after I/O.
            let Some(handle) = self
                .runtime
                .local_handle(handle.catalog.clone(), &current)
                .await?
            else {
                return Ok(ResidentRoute::NotOwned);
            };
            handle
        } else {
            handle
        };
        self.runtime.ensure_running()?;
        if let Ok(mut handles) = self.handles.lock() {
            if handles.len() >= MAX_ROUTES
                && !handles.contains_key(&cell)
                && let Some(evicted) = handles.keys().next().copied()
            {
                handles.remove(&evicted);
            }
            handles.insert(cell, handle.clone());
        }
        Ok(ResidentRoute::Owned(handle))
    }
}
