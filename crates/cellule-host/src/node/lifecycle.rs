//! Startup, drain, and shutdown.

use super::*;

impl CellNode {
    /// Opens readiness after all product startup probes have completed.
    pub fn start(&self) -> cellule_runtime::Result<()> {
        if !self.lease_installed.load(Ordering::Acquire) {
            return Err(Error::Control(
                "CellNode cannot become ready before its node lease is installed",
            ));
        }
        self.require_task_group()?;
        if !self
            .task_group
            .lock()
            .map_err(|_| Error::Control("CellNode task group lock poisoned"))?
            .as_ref()
            .is_some_and(|task_group| task_group.is_healthy())
        {
            return Err(Error::Control("CellNode task group is unhealthy"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Control("CellNode lifecycle lock poisoned"))?;
        if *state == NodeState::Starting {
            self.require_components_present()?;
            let startup = self
                .fleet_startup
                .lock()
                .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?;
            if let Some(startup) = startup.as_ref() {
                if self.runtime.node_durability().is_some()
                    && self
                        .owned_component::<crate::durability::enrollment::FleetFollowerEnrollment>(
                            NODE_DURABILITY_PROVIDER_COMPONENT,
                        )
                        .is_none()
                {
                    return Err(Error::Control(
                        "configured fleet durability requires managed follower enrollment",
                    ));
                }
                if startup.boot.is_none() {
                    return Err(Error::Control(
                        "CellNode fleet boot enrollment is unconfirmed",
                    ));
                }
                self.runtime
                    .node_admission()
                    .confirm_startup(startup.intent.mode())?;
                *state = if self.runtime.node_admission().mode()?
                    == cellule_runtime::node::NodeMode::Active
                {
                    NodeState::Ready
                } else {
                    NodeState::Maintenance
                };
            } else {
                *state = NodeState::Ready;
            }
            return Ok(());
        }
        if matches!(*state, NodeState::Ready | NodeState::Maintenance) {
            return Ok(());
        }
        Err(Error::Control(
            "CellNode cannot become ready after shutdown",
        ))
    }
    pub(super) fn require_components_present(&self) -> cellule_runtime::Result<()> {
        let required = self
            .required_components
            .lock()
            .map_err(|_| Error::Control("CellNode required-component lock poisoned"))?
            .clone();
        if required.is_empty() {
            return Ok(());
        }
        let facilities = self
            .facilities
            .lock()
            .map_err(|_| Error::Control("CellNode facility lock poisoned"))?;
        for name in required {
            if !facilities
                .iter()
                .any(|facility| facility.name == name && facility.owner.is_some())
            {
                return Err(Error::Control("CellNode required component is missing"));
            }
        }
        Ok(())
    }
    /// Stops admission, drains the runtime, and waits for its dispatcher.
    pub async fn drain(&self) -> cellule_runtime::Result<()> {
        self.drain_until(None).await
    }
    /// Stops admission and completes every owned drain phase by `deadline`.
    pub async fn drain_until(&self, deadline: Option<Instant>) -> cellule_runtime::Result<()> {
        let shutdown = Arc::clone(&self.shutdown_lock).lock_owned().await;
        self.drain_until_locked(shutdown, deadline).await
    }
    pub(super) async fn drain_until_locked(
        &self,
        shutdown: tokio::sync::OwnedMutexGuard<()>,
        deadline: Option<Instant>,
    ) -> cellule_runtime::Result<()> {
        self.drain_owner.drain(shutdown, deadline).await
    }

    /// Captures the retained original host closing attempt and failure history.
    /// This local diagnostic proves neither fleet relocation nor role settlement.
    pub fn drain_observation(&self) -> cellule_runtime::Result<Option<NodeDrainObservation>> {
        self.drain_owner.observe()
    }

    /// Idempotent alias for graceful drain used by process shutdown hooks.
    pub async fn shutdown(&self) -> cellule_runtime::Result<()> {
        self.drain().await
    }
    /// Deadline-aware alias for graceful shutdown hooks.
    pub async fn shutdown_until(&self, deadline: Instant) -> cellule_runtime::Result<()> {
        self.drain_until(Some(deadline)).await
    }
}
