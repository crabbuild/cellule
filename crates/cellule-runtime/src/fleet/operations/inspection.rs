use crate::identity::{Digest, NodeId, SessionId};

use super::{
    AttemptPhase, FleetAction, FleetActionKind, FleetActionOutcome, FleetHead, FleetOutcome,
    MaintenanceAction, MovementAction, OperationError, RegistryVersion, Result, nonzero,
};

/// One caller-assigned nonce and bounded interval for a fresh, read-only check.
///
/// This request confers no authority. Authenticate its origin and authorize it
/// against the current journal before gathering evidence. A new reconciliation
/// pass uses a new nonce; retrying a pass keeps its exact request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetInspectionRequest {
    pub(super) action: FleetAction,
    pub(super) registry: RegistryVersion,
    pub(super) nonce: Digest,
    pub(super) node: NodeId,
    pub(super) session: SessionId,
    pub(super) deadline_ms: i64,
}

impl FleetInspectionRequest {
    /// Binds a current Inspect envelope to an exact physical node and boot.
    pub fn new(
        action: FleetAction,
        registry: RegistryVersion,
        nonce: Digest,
        node: NodeId,
        session: SessionId,
        deadline_ms: i64,
    ) -> Result<Self> {
        let request = Self {
            action,
            registry,
            nonce,
            node,
            session,
            deadline_ms,
        };
        request.validate()?;
        Ok(request)
    }

    /// Returns the exact journal envelope whose state is being inspected.
    #[must_use]
    pub const fn action(&self) -> &FleetAction {
        &self.action
    }
    /// Returns the exact retained intent/enrollment barrier for this capture.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.registry
    }
    /// Returns the caller's unique check identity, not an effect idempotency key.
    #[must_use]
    pub const fn nonce(&self) -> Digest {
        self.nonce
    }
    /// Returns the authenticated physical endpoint required for the reply.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    /// Returns the endpoint's exact boot.
    #[must_use]
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Returns the exclusive end of the requested capture interval.
    #[must_use]
    pub const fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    /// Checks the exact read-only endpoint, including a successor after release.
    /// This cannot authorize an effect on that endpoint. The adapter separately
    /// authenticates its boot and checks the retained registry and journal head.
    pub fn validate_endpoint(&self, node: NodeId, session: SessionId) -> Result<()> {
        self.validate()?;
        if node != self.node || session != self.session {
            return Err(OperationError::Fenced);
        }
        Ok(())
    }

    /// Identifies all request inputs, including nonce, endpoint and authorization.
    /// Different passes cannot reuse the stable effect key as fresh evidence.
    pub fn key(&self) -> Result<Digest> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-inspection-key.v1\0");
        hash.update(&self.to_bytes()?);
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }

    /// Checks the current head and capture interval in a journal read transaction.
    /// No acceptance/result record is created and no effect is authorized.
    pub fn authorize_against(
        &self,
        head: &FleetHead,
        registry: RegistryVersion,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        registry.validate()?;
        if registry != self.registry {
            return Err(OperationError::Conflict);
        }
        if now_ms >= self.deadline_ms {
            return Err(OperationError::Deadline);
        }
        self.action.authorize_against(head, now_ms)
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.registry.validate()?;
        if self.registry.scope() != self.action.scope() {
            return Err(OperationError::Conflict);
        }
        self.action.validate()?;
        if !matches!(
            self.action.kind(),
            FleetActionKind::Movement {
                action: MovementAction::Inspect,
                ..
            } | FleetActionKind::Maintenance {
                action: MaintenanceAction::Inspect,
                ..
            }
        ) || !nonzero(self.nonce.as_bytes())
            || self.deadline_ms <= self.action.issued_at_ms()
        {
            return Err(OperationError::Invalid("invalid fresh inspection request"));
        }
        if self
            .action
            .validate_endpoint(self.node, self.session)
            .is_err()
        {
            let FleetActionKind::Movement {
                action: MovementAction::Inspect,
                attempt,
            } = self.action.kind()
            else {
                return Err(OperationError::Fenced);
            };
            let spec = attempt.spec();
            // A clean release permits observation of an ordinary-acquisition
            // winner. Unknown release and failed-source recovery keep their
            // separate proof paths; another actor cannot establish either.
            if attempt.released().is_none()
                || !matches!(
                    attempt.phase(),
                    AttemptPhase::Released
                        | AttemptPhase::Activating
                        | AttemptPhase::Activated
                        | AttemptPhase::CleaningReceiver
                )
                || !nonzero(self.node.as_bytes())
                || !nonzero(self.session.as_bytes())
                || self.node == spec.source_node
                || self.session == spec.source
                || (self.session == spec.destination && self.node != spec.destination_node)
            {
                return Err(OperationError::Fenced);
            }
        }
        Ok(())
    }

    fn validate_outcome(&self, outcome: &FleetActionOutcome) -> Result<()> {
        if self
            .action
            .validate_endpoint(self.node, self.session)
            .is_ok()
        {
            outcome.validate_for(&self.action)?;
        } else {
            // Keep ordinary effect/result validation strict. Only this exact
            // request-bound read can report a different serving endpoint.
            outcome.validate()?;
            if outcome.scope != self.action.scope() || outcome.action_key != self.action.key()? {
                return Err(OperationError::Conflict);
            }
            if !matches!(
                outcome.outcome,
                FleetOutcome::Activated(_)
                    | FleetOutcome::Unknown
                    | FleetOutcome::Blocked(_)
                    | FleetOutcome::Rejected(_)
            ) {
                return Err(OperationError::Invalid(
                    "successor inspection cannot prove receiver effects",
                ));
            }
        }
        if let FleetOutcome::Activated(evidence) = &outcome.outcome
            && let FleetActionKind::Movement { attempt, .. } = self.action.kind()
            && attempt.released().is_some()
        {
            attempt.validate_activation(evidence)?;
        }
        Ok(())
    }
}

/// Checked observation captured for one exact request, separate from effect history.
///
/// A decoder verifies shape and binding only. The trusted node adapter must
/// actually inspect current authority and actor readiness within this interval.
/// Retained Released/cleanup facts remain historical; current serving requires
/// a fresh actor-backed Activated/Recovered observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetInspectionObservation {
    pub(super) request: FleetInspectionRequest,
    pub(super) capture_started_at_ms: i64,
    pub(super) outcome: FleetActionOutcome,
}

impl FleetInspectionObservation {
    /// Retains the actual interval and checked endpoint response.
    pub fn new(
        request: FleetInspectionRequest,
        capture_started_at_ms: i64,
        outcome: FleetActionOutcome,
    ) -> Result<Self> {
        let observation = Self {
            request,
            capture_started_at_ms,
            outcome,
        };
        observation.validate()?;
        Ok(observation)
    }
    /// Returns the exact request, including nonce and committed state.
    #[must_use]
    pub const fn request(&self) -> &FleetInspectionRequest {
        &self.request
    }
    /// Returns when this node began the current evidence capture.
    #[must_use]
    pub const fn capture_started_at_ms(&self) -> i64 {
        self.capture_started_at_ms
    }
    /// Returns the original completed capture time; delivery cannot refresh it.
    #[must_use]
    pub const fn capture_finished_at_ms(&self) -> i64 {
        self.outcome.observed_at_ms
    }
    /// Returns the bounded observation with independently checked effect facts.
    #[must_use]
    pub const fn outcome(&self) -> &FleetActionOutcome {
        &self.outcome
    }

    /// Checks full request identity, absence of future time, and a finite age bound.
    /// The caller also authenticates the response origin and rechecks its journal
    /// generation before publishing dependent decisions.
    pub fn validate_for(
        &self,
        request: &FleetInspectionRequest,
        now_ms: i64,
        max_age_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        request.validate()?;
        if self.request != *request {
            return Err(OperationError::Conflict);
        }
        if max_age_ms <= 0 || now_ms < self.capture_finished_at_ms() {
            return Err(OperationError::Invalid(
                "invalid inspection consumption time",
            ));
        }
        // Bound the entire capture interval, not just a freshly stamped delivery.
        if now_ms - self.capture_started_at_ms > max_age_ms || now_ms >= request.deadline_ms {
            return Err(OperationError::Deadline);
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.request.validate()?;
        self.request.validate_outcome(&self.outcome)?;
        if self.outcome.node != self.request.node
            || self.outcome.session != self.request.session
            || self.capture_started_at_ms < self.request.action.issued_at_ms()
            || self.outcome.observed_at_ms < self.capture_started_at_ms
            || self.outcome.observed_at_ms >= self.request.deadline_ms
        {
            return Err(OperationError::Invalid(
                "inspection origin or interval mismatch",
            ));
        }
        Ok(())
    }
}
