//! Reuse of actor capabilities while their admission and node lease remain live.

use super::*;
use std::sync::Mutex;

const MAX_ROUTES: usize = 4_096;

#[derive(Clone)]
pub(crate) struct LeasedResidentRoutes {
    runtime: CellRuntime,
    handles: Arc<Mutex<HashMap<CellId, CellHandle>>>,
}

impl LeasedResidentRoutes {
    pub(crate) fn new(runtime: CellRuntime) -> Self {
        Self {
            runtime,
            handles: Arc::default(),
        }
    }

    pub(crate) fn cached(&self, target: &CellTarget) -> crate::Result<Option<CellHandle>> {
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
    ) -> crate::Result<Option<CellHandle>> {
        if let Some(handle) = self.cached(target)? {
            return Ok(Some(handle));
        }
        let cell = target.cell_id();
        let handle = self.runtime.leased_resident_handle(target, role).await?;
        // Object-only runtimes never produce a cacheable capability here.
        if let Some(handle) = &handle
            && let Ok(mut handles) = self.handles.lock()
        {
            if handles.len() >= MAX_ROUTES
                && !handles.contains_key(&cell)
                && let Some(evicted) = handles.keys().next().copied()
            {
                handles.remove(&evicted);
            }
            handles.insert(cell, handle.clone());
        }
        Ok(handle)
    }
}
