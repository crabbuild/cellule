use crate::identity::{NodeId, SessionId};

use super::{
    FleetAction, FleetActionKind, FleetActionOutcome, FleetHead, MovementAction, OperationError,
    Result, nonzero,
};

/// Immutable acceptance of one exact local fleet effect.
///
/// Construct and publish this record in the same journal transaction that
/// checks the current head. Constructing or decoding it confers no authority
/// and does not authenticate a caller. Retain it across controller replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedFleetAction {
    pub(super) action: FleetAction,
    pub(super) node: NodeId,
    pub(super) session: SessionId,
    pub(super) accepted_at_ms: i64,
}

impl AcceptedFleetAction {
    /// Checks first acceptance against the exact head and executing endpoint.
    /// The journal must atomically publish this check with acceptance.
    pub fn new(
        action: FleetAction,
        head: &FleetHead,
        node: NodeId,
        session: SessionId,
        now_ms: i64,
    ) -> Result<Self> {
        action.authorize_against(head, now_ms)?;
        let accepted = Self {
            action,
            node,
            session,
            accepted_at_ms: now_ms,
        };
        accepted.validate()?;
        Ok(accepted)
    }

    /// Returns the originally accepted envelope, including its authorization.
    #[must_use]
    pub const fn action(&self) -> &FleetAction {
        &self.action
    }
    /// Returns the physical node bound at acceptance.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Returns the boot session bound at acceptance.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the checked first-acceptance time.
    #[must_use]
    pub const fn accepted_at_ms(&self) -> i64 {
        self.accepted_at_ms
    }

    /// Checks replay identity while permitting refreshed controller authorization.
    ///
    /// The stable action key is an index, not a complete payload comparison.
    /// An existing acceptance may finish after its original deadline or lease.
    /// This check never authorizes first acceptance or another execution.
    pub fn validate_replay(
        &self,
        action: &FleetAction,
        node: NodeId,
        session: SessionId,
    ) -> Result<()> {
        self.validate()?;
        action.validate()?;
        if self.node != node
            || self.session != session
            || self.action.scope() != action.scope()
            || self.action.key()? != action.key()?
        {
            return Err(OperationError::Conflict);
        }
        self.action.validate_replay(action)
    }

    /// Binds a result to the accepted endpoint and original execution inputs.
    /// This validates shape; canonical execution and fresh serving checks remain
    /// the host's responsibility.
    pub fn validate_result(&self, result: &FleetActionOutcome) -> Result<()> {
        self.validate()?;
        result.validate_for(&self.action)?;
        if let super::FleetOutcome::Recovered(evidence) = &result.outcome
            && matches!(
                self.action.kind(),
                FleetActionKind::Movement {
                    action: MovementAction::Recover,
                    ..
                }
            )
        {
            evidence.recovery.basis().validate_acceptance(self)?;
        }
        if result.node != self.node
            || result.session != self.session
            || result.observed_at_ms < self.accepted_at_ms
        {
            return Err(OperationError::Invalid("fleet result acceptance mismatch"));
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.action.validate_endpoint(self.node, self.session)?;
        if self.accepted_at_ms < self.action.issued_at_ms() {
            return Err(OperationError::Invalid("invalid fleet acceptance identity"));
        }
        self.action.check_admission_deadline(self.accepted_at_ms)?;
        Ok(())
    }
}

impl FleetAction {
    /// Compares complete immutable execution inputs across authorization renewal.
    /// This does not authorize a side effect or establish durable acceptance.
    pub fn validate_replay(&self, action: &Self) -> Result<()> {
        self.validate()?;
        action.validate()?;
        if self.scope() != action.scope() || self.key()? != action.key()? {
            return Err(OperationError::Conflict);
        }
        let same = match (self.kind(), action.kind()) {
            (
                FleetActionKind::Movement {
                    action: original,
                    attempt: a,
                },
                FleetActionKind::Movement {
                    action: replay,
                    attempt: b,
                },
            ) => original == replay && a.spec() == b.spec(),
            (
                FleetActionKind::Maintenance {
                    action: original,
                    operation: a,
                },
                FleetActionKind::Maintenance {
                    action: replay,
                    operation: b,
                },
            ) => {
                original == replay
                    && a.id == b.id
                    && a.request_digest == b.request_digest
                    && a.node == b.node
                    && a.session == b.session
                    && a.intent_revision == b.intent_revision
                    && a.created_at_ms == b.created_at_ms
                    && a.deadline_ms == b.deadline_ms
            }
            _ => false,
        };
        if !same {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }

    /// Checks the exact local endpoint without conferring journal authorization.
    pub fn validate_endpoint(&self, node: NodeId, session: SessionId) -> Result<()> {
        self.validate()?;
        if !nonzero(node.as_bytes()) || !nonzero(session.as_bytes()) {
            return Err(OperationError::Invalid("invalid fleet acceptance identity"));
        }
        let endpoint = match self.kind() {
            FleetActionKind::Movement { action, attempt } => {
                let spec = attempt.spec();
                let source = node == spec.source_node && session == spec.source;
                let receiver = node == spec.destination_node && session == spec.destination;
                match action {
                    MovementAction::Release | MovementAction::ReleaseMaintenance => source,
                    MovementAction::Prepare
                    | MovementAction::Activate
                    | MovementAction::Cancel
                    | MovementAction::Recover => receiver,
                    MovementAction::Inspect => source || receiver,
                    MovementAction::Retire => false,
                }
            }
            FleetActionKind::Maintenance { operation, .. } => {
                node == operation.node() && session == operation.session()
            }
        };
        if !endpoint {
            return Err(OperationError::Fenced);
        }
        Ok(())
    }
}
