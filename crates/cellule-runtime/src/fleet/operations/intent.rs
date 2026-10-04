use crate::identity::{NodeId, SessionId};
use crate::node::NodeMode;

use super::{
    FleetScope, MaintenanceOperation, MaintenancePhase, OperationError, OperationId, Result,
    nonzero,
};

/// Application-journal intent keyed by physical NodeId, retained across reboots.
///
/// The adapter commits maintenance intent with the fleet-head CAS, keeps older
/// nodes' intent after a later operation starts, and checks it before runtime
/// acquisition or readiness opens. An intent never grants Cell authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIntent {
    pub(super) scope: FleetScope,
    pub(super) node: NodeId,
    pub(super) session: SessionId,
    pub(super) revision: u64,
    pub(super) mode: NodeMode,
    pub(super) operation: Option<OperationId>,
}

impl NodeIntent {
    /// Constructs the first authorized serving intent in an empty node registry.
    /// Existing registry entries must be conditionally updated, never replaced
    /// with this constructor to erase a maintenance cordon.
    pub fn initial(scope: FleetScope, node: NodeId, session: SessionId) -> Result<Self> {
        let intent = Self {
            scope,
            node,
            session,
            revision: 1,
            mode: NodeMode::Active,
            operation: None,
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Derives the durable desired drain mode independently of local progress.
    pub fn maintenance(scope: FleetScope, operation: &MaintenanceOperation) -> Result<Self> {
        operation.validate()?;
        let intent = Self {
            scope,
            node: operation.node,
            session: operation.session,
            revision: operation.intent_revision,
            mode: NodeMode::Draining,
            operation: Some(operation.id),
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Advances a retained intent from the operation in the same journal CAS.
    /// The adapter must load this physical-node row inside that transaction.
    /// Progress alone does not change desired mode or reopen a completed node.
    pub fn advance_maintenance(&self, operation: &MaintenanceOperation) -> Result<Self> {
        self.validate()?;
        let next = Self::maintenance(self.scope, operation)?;
        if next == *self {
            return Ok(self.clone());
        }
        if next.node != self.node
            || next.revision <= self.revision
            || (self.mode != NodeMode::Active && self.operation != next.operation)
            || (self.mode == NodeMode::Active && next.session != self.session)
        {
            return Err(OperationError::Conflict);
        }
        Ok(next)
    }

    /// Rebinds an already Active physical node to a separately validated boot.
    /// The application must prove old-session withdrawal or complete failed-boot
    /// closure (including process/accepted external work joining and all original
    /// roles settled), and new-session enrollment. Expiry/takeover is insufficient.
    /// This cannot reopen a retained cordon after a reboot.
    pub fn rebind_active(&self, session: SessionId, revision: u64) -> Result<Self> {
        self.validate()?;
        if self.mode != NodeMode::Active || session == self.session || revision <= self.revision {
            return Err(OperationError::Conflict);
        }
        let mut next = self.clone();
        next.session = session;
        next.revision = revision;
        next.validate()?;
        Ok(next)
    }

    /// Calculates an authorized newer Active intent after confirmed maintenance.
    /// Returning to service uses a different validated boot session. Publishing
    /// this record and enrolling that session remain application responsibilities.
    pub fn return_to_service(
        &self,
        completed: &MaintenanceOperation,
        new_session: SessionId,
        revision: u64,
    ) -> Result<Self> {
        self.validate()?;
        completed.validate()?;
        if self.mode == NodeMode::Active
            || self.operation != Some(completed.id)
            || self.node != completed.node
            || self.session != completed.session
            || self.revision != completed.intent_revision
            || completed.phase != MaintenancePhase::Completed
            || revision <= self.revision
            || new_session == self.session
        {
            return Err(OperationError::Invalid("return to service is not proven"));
        }
        let next = Self {
            scope: self.scope,
            node: self.node,
            session: new_session,
            revision,
            mode: NodeMode::Active,
            operation: None,
        };
        next.validate()?;
        Ok(next)
    }

    /// Returns the journal authorization scope.
    #[must_use]
    pub const fn scope(&self) -> FleetScope {
        self.scope
    }
    /// Returns the physical identity used for startup cordon lookup.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Returns the session observed by the latest committed intent.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the monotonic desired-mode revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    /// Returns the mode a new boot must honor before readiness.
    #[must_use]
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Returns the maintenance operation retaining the cordon.
    #[must_use]
    pub const fn operation(&self) -> Option<OperationId> {
        self.operation
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if !nonzero(self.node.as_bytes())
            || !nonzero(self.session.as_bytes())
            || self.revision == 0
            || (self.mode == NodeMode::Active) != self.operation.is_none()
            || self.operation.is_some_and(|id| !nonzero(id.as_bytes()))
        {
            return Err(OperationError::Invalid("invalid physical-node intent"));
        }
        Ok(())
    }
}
