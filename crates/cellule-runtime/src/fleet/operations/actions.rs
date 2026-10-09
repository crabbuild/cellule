use crate::identity::{Digest, NodeId, SessionId};

use super::{
    ActivationEvidence, AttemptId, AttemptPhase, DrainBlocker, DrainEvidence, FleetHead,
    FleetScope, MAX_RECEIVER_HANDOFFS, MaintenanceOperation, MaintenancePhase, MoveAttempt,
    MovementAction, OperationError, PublishedPosition, ReceiverReservation, RecoveredActivation,
    RegistryVersion, Result, nonzero,
};

/// One exact, process-closed receiver replacement in an immutable attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverHandoff {
    pub(super) previous_node: NodeId,
    pub(super) previous_session: SessionId,
    pub(super) target_node: NodeId,
    pub(super) target_session: SessionId,
    pub(super) process_closure: Digest,
    pub(super) registry: RegistryVersion,
}

impl ReceiverHandoff {
    /// Returns the exact receiver boot whose process closure permits this hop.
    #[must_use]
    pub const fn previous(&self) -> (NodeId, SessionId) {
        (self.previous_node, self.previous_session)
    }

    /// Returns the exact newly selected receiver boot.
    #[must_use]
    pub const fn target(&self) -> (NodeId, SessionId) {
        (self.target_node, self.target_session)
    }

    /// Returns the digest of the checked process-closure evidence.
    #[must_use]
    pub const fn process_closure(&self) -> Digest {
        self.process_closure
    }

    /// Returns the current registry barrier captured with the handoff.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.registry
    }
}

/// Bounded, ordered receiver replacement history for one movement dispatch.
///
/// The embedding host constructs this from a fresh closed-boot proof and
/// current placement observations. This record is shape-only: the receiving
/// journal must match its final hop to the opaque proof and current registry
/// before first acceptance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverRoute {
    pub(super) hops: Vec<ReceiverHandoff>,
}

impl ReceiverRoute {
    /// Starts a route after the originally preferred receiver process closed.
    pub fn begin(
        scope: FleetScope,
        spec: &super::MoveAttemptSpec,
        target_node: NodeId,
        target_session: SessionId,
        process_closure: Digest,
        registry: RegistryVersion,
    ) -> Result<Self> {
        let route = Self {
            hops: vec![ReceiverHandoff {
                previous_node: spec.destination_node,
                previous_session: spec.destination,
                target_node,
                target_session,
                process_closure,
                registry,
            }],
        };
        route.validate_for(scope, spec)?;
        Ok(route)
    }

    /// Extends a route after the current receiver process has closed.
    pub fn extend(
        &self,
        scope: FleetScope,
        spec: &super::MoveAttemptSpec,
        target_node: NodeId,
        target_session: SessionId,
        process_closure: Digest,
        registry: RegistryVersion,
    ) -> Result<Self> {
        self.validate_for(scope, spec)?;
        if self.hops.len() >= MAX_RECEIVER_HANDOFFS {
            return Err(OperationError::Invalid("receiver handoff limit reached"));
        }
        let previous = self.endpoint(spec);
        let route = Self {
            hops: self
                .hops
                .iter()
                .cloned()
                .chain(std::iter::once(ReceiverHandoff {
                    previous_node: previous.0,
                    previous_session: previous.1,
                    target_node,
                    target_session,
                    process_closure,
                    registry,
                }))
                .collect(),
        };
        route.validate_for(scope, spec)?;
        Ok(route)
    }

    /// Returns the latest selected receiver boot.
    #[must_use]
    pub fn target(&self, spec: &super::MoveAttemptSpec) -> (NodeId, SessionId) {
        self.endpoint(spec)
    }

    /// Returns the number of process-closed hops represented by this route.
    #[must_use]
    pub const fn hop_count(&self) -> usize {
        self.hops.len()
    }

    /// Returns the final closed-boot proof in this route.
    #[must_use]
    pub fn latest_handoff(&self) -> Option<&ReceiverHandoff> {
        self.hops.last()
    }

    /// Checks whether this route is the same route or appends exactly one hop.
    #[must_use]
    pub fn follows(&self, previous: &Self) -> bool {
        self.hops == previous.hops
            || (self.hops.len() == previous.hops.len() + 1 && self.hops.starts_with(&previous.hops))
    }

    /// Returns the registry barrier for the latest handoff.
    #[must_use]
    pub fn registry(&self) -> Option<RegistryVersion> {
        self.hops.last().map(|hop| hop.registry)
    }

    pub(super) fn validate_for(
        &self,
        scope: FleetScope,
        spec: &super::MoveAttemptSpec,
    ) -> Result<()> {
        scope.validate()?;
        spec.validate()?;
        if self.hops.is_empty() || self.hops.len() > MAX_RECEIVER_HANDOFFS {
            return Err(OperationError::Invalid("invalid receiver route length"));
        }
        let mut previous = (spec.destination_node, spec.destination);
        let mut visited = vec![spec.source_node, spec.destination_node];
        let mut previous_revision = 0;
        for hop in &self.hops {
            hop.registry.validate()?;
            if hop.previous_node != previous.0
                || hop.previous_session != previous.1
                || !nonzero(hop.target_node.as_bytes())
                || !nonzero(hop.target_session.as_bytes())
                || !nonzero(hop.process_closure.as_bytes())
                || hop.target_node == spec.source_node
                || visited.contains(&hop.target_node)
                || hop.target_session == spec.source
                || hop.registry.scope() != scope
                || hop.registry.revision() == 0
                || hop.registry.bootstrap_revision().is_none()
                || hop.registry.revision() <= previous_revision
            {
                return Err(OperationError::Invalid("invalid receiver handoff"));
            }
            previous = (hop.target_node, hop.target_session);
            visited.push(hop.target_node);
            previous_revision = hop.registry.revision();
        }
        Ok(())
    }

    fn endpoint(&self, spec: &super::MoveAttemptSpec) -> (NodeId, SessionId) {
        self.hops
            .last()
            .map_or((spec.destination_node, spec.destination), |hop| {
                (hop.target_node, hop.target_session)
            })
    }

    fn digest(&self) -> Digest {
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-receiver-route.v1\0");
        hash.update(&(self.hops.len() as u64).to_be_bytes());
        for hop in &self.hops {
            hash.update(hop.previous_node.as_bytes());
            hash.update(hop.previous_session.as_bytes());
            hash.update(hop.target_node.as_bytes());
            hash.update(hop.target_session.as_bytes());
            hash.update(hop.process_closure.as_bytes());
            hash.update(hop.registry.scope().fleet.as_bytes());
            hash.update(hop.registry.scope().application.as_bytes());
            hash.update(&hop.registry.revision().to_be_bytes());
            hash.update(
                &hop.registry
                    .bootstrap_revision()
                    .unwrap_or_default()
                    .to_be_bytes(),
            );
            hash.update(&[u8::from(hop.registry.scheduling_enabled())]);
        }
        Digest::from_bytes(*hash.finalize().as_bytes())
    }
}

/// Node lifecycle work recorded by one maintenance operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MaintenanceAction {
    /// Apply the persisted physical-node intent before new role admission.
    Cordon = 1,
    /// Reconcile reader replacements and foreign follower obligations.
    SettleRoles = 2,
    /// Complete the existing host drain after relocation is proven.
    Finalize = 3,
    /// Reconstruct actual progress without starting a new role transition.
    Inspect = 4,
}

/// Exact action payload cloned from the current committed journal state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetActionKind {
    /// Execute or inspect one immutable session/generation-bound movement.
    Movement {
        /// Remote effect or inspection; retirement is journal-local.
        action: MovementAction,
        /// Current charged attempt and its dispatch/evidence state.
        attempt: Box<MoveAttempt>,
    },
    /// Execute or inspect a physical-node maintenance operation.
    Maintenance {
        /// Lifecycle effect to reconcile.
        action: MaintenanceAction,
        /// Current committed operation and targeted session.
        operation: Box<MaintenanceOperation>,
    },
}

/// Bounded journal-bound action envelope. It is not an authentication capability.
///
/// The receiving application authenticates the caller and reads the journal
/// before `authorize_against`. Existing accepted actions may finish after a
/// controller change; accepting a new effect requires the current live epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetAction {
    pub(super) scope: FleetScope,
    pub(super) journal_revision: u64,
    pub(super) controller: SessionId,
    pub(super) controller_epoch: u64,
    pub(super) issued_at_ms: i64,
    pub(super) kind: FleetActionKind,
    pub(super) receiver_route: Option<ReceiverRoute>,
}

impl FleetAction {
    /// Returns the exact fleet/application authorization scope.
    #[must_use]
    pub const fn scope(&self) -> FleetScope {
        self.scope
    }
    /// Returns the committed revision authorizing first acceptance.
    #[must_use]
    pub const fn journal_revision(&self) -> u64 {
        self.journal_revision
    }
    /// Returns the controller boot identity covered by the journal lease.
    #[must_use]
    pub const fn controller(&self) -> SessionId {
        self.controller
    }
    /// Returns the controller fencing epoch.
    #[must_use]
    pub const fn controller_epoch(&self) -> u64 {
        self.controller_epoch
    }
    /// Returns the logical issuance time; receivers recheck their current time.
    #[must_use]
    pub const fn issued_at_ms(&self) -> i64 {
        self.issued_at_ms
    }
    /// Returns the immutable exact target/effect payload.
    #[must_use]
    pub const fn kind(&self) -> &FleetActionKind {
        &self.kind
    }

    /// Returns the process-closed receiver route, when this is a continuation.
    #[must_use]
    pub const fn receiver_route(&self) -> Option<&ReceiverRoute> {
        self.receiver_route.as_ref()
    }

    /// Returns the exact receiver endpoint used by this action, if it is movement.
    #[must_use]
    pub fn receiver_endpoint(&self) -> Option<(NodeId, SessionId)> {
        match &self.kind {
            FleetActionKind::Movement { attempt, .. } => Some(self.receiver_endpoint_for(attempt)),
            FleetActionKind::Maintenance { .. } => None,
        }
    }

    fn receiver_endpoint_for(&self, attempt: &MoveAttempt) -> (NodeId, SessionId) {
        self.receiver_route.as_ref().map_or(
            (attempt.spec.destination_node, attempt.spec.destination),
            |route| route.endpoint(&attempt.spec),
        )
    }

    /// Returns a stable deduplication key across controller adoption and retries.
    /// Authorization revision/time are deliberately excluded; the current
    /// journal is still required to accept an effect for the first time.
    pub fn key(&self) -> Result<Digest> {
        self.validate()?;
        match &self.kind {
            FleetActionKind::Movement { action, attempt } => {
                let base = movement_key(self.scope, *action, &attempt.spec);
                Ok(self.receiver_route.as_ref().map_or(base, |route| {
                    let mut hash = blake3::Hasher::new();
                    hash.update(b"cellule.fleet-routed-action-key.v1\0");
                    hash.update(base.as_bytes());
                    hash.update(route.digest().as_bytes());
                    Digest::from_bytes(*hash.finalize().as_bytes())
                }))
            }
            FleetActionKind::Maintenance { action, operation } => {
                Self::maintenance_action_key(self.scope, *action, operation)
            }
        }
    }

    /// Returns a maintenance action's stable key without requiring the action
    /// to be dispatchable in the operation's current phase. Journals use this
    /// to verify the exact SettleRoles receipt atomically with ReadyToClose.
    pub fn maintenance_action_key(
        scope: FleetScope,
        action: MaintenanceAction,
        operation: &MaintenanceOperation,
    ) -> Result<Digest> {
        scope.validate()?;
        operation.validate()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-action-key.v1\0");
        hash.update(scope.fleet.as_bytes());
        hash.update(scope.application.as_bytes());
        hash.update(&[2, action as u8]);
        hash.update(operation.id.as_bytes());
        hash.update(operation.node.as_bytes());
        hash.update(operation.session.as_bytes());
        hash.update(&operation.intent_revision.to_be_bytes());
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }

    /// Verifies exact current journal state before first local acceptance.
    /// Caller identity and backend authenticity are checked by the application.
    pub fn authorize_against(&self, head: &FleetHead, now_ms: i64) -> Result<()> {
        self.validate()?;
        if self.receiver_route.is_some() {
            return Err(OperationError::Invalid(
                "receiver continuation requires a current registry",
            ));
        }
        head.check_action_controller(now_ms)?;
        if self.scope != head.scope
            || self.journal_revision != head.revision
            || self.issued_at_ms > now_ms
        {
            return Err(OperationError::Conflict);
        }
        let expected = match &self.kind {
            FleetActionKind::Movement { action, attempt } => {
                head.movement_action(attempt.spec.id, *action, self.issued_at_ms)?
            }
            FleetActionKind::Maintenance { action, .. } => {
                head.maintenance_action(*action, self.issued_at_ms)?
            }
        };
        if *self != expected {
            return Err(OperationError::Fenced);
        }
        self.check_admission_deadline(now_ms)
    }

    /// Verifies routed authorization against the exact current registry.
    /// Ordinary actions retain the registry-free validator and byte format.
    /// This shape check does not replace the journal's typed closure check.
    pub fn authorize_against_registry(
        &self,
        head: &FleetHead,
        registry: RegistryVersion,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        let Some(route) = &self.receiver_route else {
            return self.authorize_against(head, now_ms);
        };
        registry.validate()?;
        if route.registry() != Some(registry) {
            return Err(OperationError::Conflict);
        }
        let FleetActionKind::Movement { action, attempt } = &self.kind else {
            return Err(OperationError::Invalid("receiver route is not movement"));
        };
        route.validate_for(self.scope, &attempt.spec)?;
        if !matches!(
            action,
            MovementAction::Activate
                | MovementAction::Recover
                | MovementAction::Inspect
                | MovementAction::Cancel
        ) || attempt.released().is_none()
        {
            return Err(OperationError::Invalid(
                "receiver continuation is not a post-release action",
            ));
        }
        head.check_action_controller(now_ms)?;
        if self.scope != head.scope
            || self.journal_revision != head.revision
            || self.issued_at_ms > now_ms
        {
            return Err(OperationError::Conflict);
        }
        let expected = head.movement_action_with_receiver_route(
            attempt.spec.id,
            *action,
            route.clone(),
            self.issued_at_ms,
        )?;
        if *self != expected {
            return Err(OperationError::Fenced);
        }
        self.check_admission_deadline(now_ms)
    }

    pub(super) fn check_admission_deadline(&self, now_ms: i64) -> Result<()> {
        if let FleetActionKind::Movement { action, attempt } = &self.kind {
            if matches!(
                action,
                MovementAction::Prepare
                    | MovementAction::Release
                    | MovementAction::ReleaseMaintenance
            ) && now_ms >= attempt.spec.deadline_ms
            {
                return Err(OperationError::Deadline);
            }
            if action.is_source_release()
                && attempt
                    .reservation
                    .is_none_or(|r| r.expires_at_ms <= now_ms)
            {
                return Err(OperationError::Invalid("release reservation expired"));
            }
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self.journal_revision == 0
            || self.controller_epoch == 0
            || !nonzero(self.controller.as_bytes())
            || self.issued_at_ms < 0
        {
            return Err(OperationError::Invalid(
                "invalid fleet action authorization",
            ));
        }
        match &self.kind {
            FleetActionKind::Movement { action, attempt } => {
                attempt.validate()?;
                if attempt.spec.target.application() != self.scope.application
                    || !movement_allowed(attempt, *action, self.receiver_route.is_some())
                {
                    return Err(OperationError::Invalid("movement dispatch phase mismatch"));
                }
            }
            FleetActionKind::Maintenance { action, operation } => {
                operation.validate()?;
                if !maintenance_allowed(operation, *action) {
                    return Err(OperationError::Invalid(
                        "maintenance dispatch phase mismatch",
                    ));
                }
            }
        }
        if let Some(route) = &self.receiver_route {
            let FleetActionKind::Movement { action, attempt } = &self.kind else {
                return Err(OperationError::Invalid("receiver route is not movement"));
            };
            route.validate_for(self.scope, &attempt.spec)?;
            if !matches!(
                action,
                MovementAction::Activate
                    | MovementAction::Recover
                    | MovementAction::Inspect
                    | MovementAction::Cancel
            ) || attempt.released().is_none()
            {
                return Err(OperationError::Invalid(
                    "receiver continuation is not a post-release action",
                ));
            }
        }
        self.check_admission_deadline(self.issued_at_ms)
    }
}

impl FleetHead {
    fn check_action_controller(&self, now_ms: i64) -> Result<()> {
        if now_ms < self.last_observed_ms {
            return Err(OperationError::Invalid("action time regressed"));
        }
        if self
            .controller
            .is_none_or(|lease| now_ms >= lease.expires_at_ms)
        {
            return Err(OperationError::Fenced);
        }
        Ok(())
    }

    /// Builds a movement action only after its dispatch phase was CAS-published.
    pub fn movement_action(
        &self,
        id: AttemptId,
        action: MovementAction,
        now_ms: i64,
    ) -> Result<FleetAction> {
        let attempt = self
            .attempts
            .iter()
            .find(|attempt| attempt.spec.id == id)
            .ok_or(OperationError::NotFound)?;
        if action == MovementAction::ReleaseMaintenance {
            let operation = self.maintenance.as_ref().ok_or(OperationError::Invalid(
                "busy release lacks maintenance intent",
            ))?;
            if attempt.spec.id.operation != operation.id
                || attempt.spec.source_node != operation.node
                || attempt.spec.source != operation.session
                || operation.phase != MaintenancePhase::Evacuating
            {
                return Err(OperationError::Invalid(
                    "busy release maintenance identity mismatch",
                ));
            }
            if now_ms >= operation.deadline_ms {
                return Err(OperationError::Deadline);
            }
        }
        self.make_action(
            FleetActionKind::Movement {
                action,
                attempt: Box::new(attempt.clone()),
            },
            now_ms,
        )
    }

    /// Builds a post-release action bound to a closed receiver and its current
    /// replacement. First acceptance also requires the journal's typed closure
    /// check; a routed action must not reach a remote effect before that commit.
    pub fn movement_action_with_receiver_route(
        &self,
        id: AttemptId,
        action: MovementAction,
        route: ReceiverRoute,
        now_ms: i64,
    ) -> Result<FleetAction> {
        let attempt = self
            .attempts
            .iter()
            .find(|attempt| attempt.spec.id == id)
            .ok_or(OperationError::NotFound)?;
        self.make_action_with_route(
            FleetActionKind::Movement {
                action,
                attempt: Box::new(attempt.clone()),
            },
            Some(route),
            now_ms,
        )
    }

    /// Builds lifecycle work only from the current committed maintenance operation.
    pub fn maintenance_action(
        &self,
        action: MaintenanceAction,
        now_ms: i64,
    ) -> Result<FleetAction> {
        let operation = self.maintenance.as_ref().ok_or(OperationError::NotFound)?;
        self.make_action(
            FleetActionKind::Maintenance {
                action,
                operation: Box::new(operation.clone()),
            },
            now_ms,
        )
    }

    fn make_action(&self, kind: FleetActionKind, now_ms: i64) -> Result<FleetAction> {
        self.make_action_with_route(kind, None, now_ms)
    }

    fn make_action_with_route(
        &self,
        kind: FleetActionKind,
        receiver_route: Option<ReceiverRoute>,
        now_ms: i64,
    ) -> Result<FleetAction> {
        self.check_action_controller(now_ms)?;
        let controller = self.controller.ok_or(OperationError::Fenced)?;
        let action = FleetAction {
            scope: self.scope,
            journal_revision: self.revision,
            controller: controller.claimant,
            controller_epoch: controller.epoch,
            issued_at_ms: now_ms,
            kind,
            receiver_route,
        };
        action.validate()?;
        Ok(action)
    }
}

fn movement_allowed(attempt: &MoveAttempt, action: MovementAction, routed: bool) -> bool {
    if action == MovementAction::Inspect {
        return true;
    }
    if attempt.blocker == Some(DrainBlocker::OutcomeUnknown) && !routed {
        return false;
    }
    match action {
        MovementAction::Prepare => attempt.phase == AttemptPhase::Preparing,
        MovementAction::Release => attempt.phase == AttemptPhase::Releasing,
        MovementAction::ReleaseMaintenance => attempt.phase == AttemptPhase::MaintenanceReleasing,
        MovementAction::Activate => attempt.phase == AttemptPhase::Activating,
        MovementAction::Recover => attempt.phase == AttemptPhase::Recovering,
        MovementAction::Cancel => {
            attempt.phase == AttemptPhase::Cancelling
                || attempt.phase == AttemptPhase::CleaningReceiver
                || (matches!(
                    attempt.phase,
                    AttemptPhase::Activated | AttemptPhase::Recovered
                ) && !attempt.receiver_cleaned)
        }
        MovementAction::Inspect => true,
        MovementAction::Retire => false,
    }
}

fn maintenance_allowed(operation: &MaintenanceOperation, action: MaintenanceAction) -> bool {
    match action {
        MaintenanceAction::Cordon => operation.phase != MaintenancePhase::Completed,
        MaintenanceAction::SettleRoles => operation.phase == MaintenancePhase::Evacuating,
        MaintenanceAction::Finalize => operation.phase == MaintenancePhase::Closing,
        MaintenanceAction::Inspect => true,
    }
}

/// Bounded remote status; transport/source errors remain on the adapter result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetOutcome {
    /// Exact preferred receiver resources have been admitted.
    Reserved(ReceiverReservation),
    /// Exact source release and object-covered position are proven.
    Released(PublishedPosition),
    /// A current owner and actor-backed serving position are proven.
    Activated(ActivationEvidence),
    /// Failed-source recovery, distinct from a clean release followed by activation.
    Recovered(Box<RecoveredActivation>),
    /// No effect was accepted; this is distinct from an ambiguous transport error.
    Rejected(DrainBlocker),
    /// Accepted work or an obligation still blocks progress; retain its permit.
    Blocked(DrainBlocker),
    /// Accepted remote work has no confirmed result yet.
    Unknown,
    /// Receiver work joined and its unused reservation no longer exists.
    ReceiverCleaned,
    /// The exact local node session has applied its persistent admission closure.
    Cordoned,
    /// Legacy role inventory result retained for decoding; it lacks the registry
    /// barrier and therefore cannot authorize a new Closing transition.
    RolesSettled {
        /// Digest of the authoritative inventory used at the final barrier.
        inventory: Digest,
    },
    /// Role settlement bound to the exact registry version observed at completion.
    /// The digest must bind complete native/foreign inventories, joined accepted
    /// work, and the checked replacement-policy evidence for this same version.
    RolesSettledAt {
        /// Digest of the complete settlement evidence at `registry`.
        inventory: Digest,
        /// Exact journal head revision observed with the complete inventory.
        head_revision: u64,
        /// Exact intent/enrollment/evidence version captured at the barrier.
        registry: RegistryVersion,
    },
    /// Complete relocation, runtime/facility shutdown, and withdrawal evidence.
    Stopped(DrainEvidence),
}

/// Canonical bounded action result for an authenticated origin session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetActionOutcome {
    /// Exact fleet/application scope checked by the receiving adapter.
    pub scope: FleetScope,
    /// Stable action identity, independent of controller adoption.
    pub action_key: Digest,
    /// Authenticated physical node reporting the result.
    pub node: NodeId,
    /// Exact boot session that performed or inspected the effect.
    pub session: SessionId,
    /// Logical time the evidence was inspected, not merely republished.
    pub observed_at_ms: i64,
    /// Typed bounded status; this record itself grants no Cell authority.
    pub outcome: FleetOutcome,
}

impl FleetActionOutcome {
    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if !nonzero(self.action_key.as_bytes())
            || !nonzero(self.node.as_bytes())
            || !nonzero(self.session.as_bytes())
            || self.observed_at_ms < 0
        {
            return Err(OperationError::Invalid("invalid fleet result identity"));
        }
        match &self.outcome {
            FleetOutcome::Reserved(r)
                if r.session != self.session || r.expires_at_ms <= self.observed_at_ms =>
            {
                Err(OperationError::Invalid(
                    "reservation result is not admitted",
                ))
            }
            FleetOutcome::Released(p) => p.validate(),
            FleetOutcome::Activated(e) => {
                e.position.validate()?;
                if e.node != self.node || e.session != self.session {
                    return Err(OperationError::Invalid("activation result origin mismatch"));
                }
                Ok(())
            }
            FleetOutcome::Recovered(e) => {
                e.validate()?;
                if e.serving.node != self.node
                    || e.serving.session != self.session
                    || e.recovery.recorded_at_ms() > self.observed_at_ms
                {
                    return Err(OperationError::Invalid(
                        "recovery result origin or time mismatch",
                    ));
                }
                Ok(())
            }
            FleetOutcome::RolesSettled { inventory } if !nonzero(inventory.as_bytes()) => {
                Err(OperationError::Invalid("role result lacks inventory"))
            }
            FleetOutcome::RolesSettledAt {
                inventory,
                head_revision,
                registry,
            } if !nonzero(inventory.as_bytes())
                || *head_revision == 0
                || registry.scope() != self.scope
                || registry.revision() == 0
                || registry.bootstrap_revision().is_none() =>
            {
                Err(OperationError::Invalid(
                    "role result lacks a bootstrapped inventory barrier",
                ))
            }
            FleetOutcome::Stopped(e)
                if e.node != self.node
                    || e.session != self.session
                    || !e.ready_to_close()
                    || !e.facilities_closed
                    || !e.stopped
                    || !e.withdrawn =>
            {
                Err(OperationError::Invalid(
                    "stopped result lacks terminal proof",
                ))
            }
            FleetOutcome::Rejected(DrainBlocker::OutcomeUnknown)
            | FleetOutcome::Blocked(DrainBlocker::OutcomeUnknown) => Err(OperationError::Invalid(
                "an unknown result must use the unknown outcome",
            )),
            _ => Ok(()),
        }
    }

    /// Binds a reply to the exact issued action and authenticated local endpoint.
    /// Readiness/fresh authority checks remain required before counting relocation.
    pub fn validate_for(&self, action: &FleetAction) -> Result<()> {
        self.validate()?;
        if self.scope != action.scope || self.action_key != action.key()? {
            return Err(OperationError::Invalid("fleet reply action mismatch"));
        }
        if let FleetOutcome::RolesSettledAt { head_revision, .. } = &self.outcome
            && *head_revision < action.journal_revision()
        {
            return Err(OperationError::Invalid(
                "role result predates its accepted journal head",
            ));
        }
        let permitted = match &action.kind {
            FleetActionKind::Movement {
                action: kind,
                attempt,
            } => {
                let receiver_endpoint = action
                    .receiver_endpoint()
                    .ok_or(OperationError::Invalid("movement action lacks receiver"))?;
                let source =
                    self.node == attempt.spec.source_node && self.session == attempt.spec.source;
                let receiver = receiver_endpoint == (self.node, self.session);
                match &self.outcome {
                    FleetOutcome::Reserved(_) => {
                        receiver
                            && matches!(kind, MovementAction::Prepare | MovementAction::Inspect)
                    }
                    FleetOutcome::Released(p) => {
                        source
                            && matches!(
                                kind,
                                MovementAction::Release
                                    | MovementAction::ReleaseMaintenance
                                    | MovementAction::Inspect
                            )
                            && p.incarnation == attempt.spec.incarnation
                            && p.epoch == attempt.spec.source_epoch
                    }
                    FleetOutcome::Activated(e) => {
                        matches!(kind, MovementAction::Activate | MovementAction::Inspect)
                            && e.position.incarnation == attempt.spec.incarnation
                            && e.position.epoch > attempt.spec.source_epoch
                            && e.session != attempt.spec.source
                            && (e.node != attempt.spec.destination_node || receiver)
                            && (action.receiver_route.is_none() || receiver)
                    }
                    FleetOutcome::Recovered(e) => {
                        receiver
                            && matches!(kind, MovementAction::Recover | MovementAction::Inspect)
                            && e.recovery.basis().spec() == attempt.spec()
                            && e.recovery.basis().scope == action.scope
                    }
                    FleetOutcome::ReceiverCleaned => {
                        receiver && matches!(kind, MovementAction::Cancel | MovementAction::Inspect)
                    }
                    FleetOutcome::Rejected(_)
                    | FleetOutcome::Blocked(_)
                    | FleetOutcome::Unknown => match kind {
                        MovementAction::Release | MovementAction::ReleaseMaintenance => source,
                        MovementAction::Prepare
                        | MovementAction::Activate
                        | MovementAction::Cancel
                        | MovementAction::Recover => receiver,
                        MovementAction::Inspect => source || receiver,
                        MovementAction::Retire => false,
                    },
                    _ => false,
                }
            }
            FleetActionKind::Maintenance {
                action: kind,
                operation,
            } => {
                self.node == operation.node
                    && self.session == operation.session
                    && match self.outcome {
                        FleetOutcome::Cordoned => {
                            matches!(kind, MaintenanceAction::Cordon | MaintenanceAction::Inspect)
                        }
                        FleetOutcome::RolesSettled { .. } | FleetOutcome::RolesSettledAt { .. } => {
                            matches!(
                                kind,
                                MaintenanceAction::SettleRoles | MaintenanceAction::Inspect
                            )
                        }
                        FleetOutcome::Stopped(evidence) => {
                            matches!(
                                kind,
                                MaintenanceAction::Finalize | MaintenanceAction::Inspect
                            ) && operation.drain_evidence().is_some_and(|mut expected| {
                                expected.facilities_closed = true;
                                expected.stopped = true;
                                expected.withdrawn = true;
                                evidence == expected
                            })
                        }
                        FleetOutcome::Rejected(_)
                        | FleetOutcome::Blocked(_)
                        | FleetOutcome::Unknown => true,
                        _ => false,
                    }
            }
        };
        if !permitted {
            return Err(OperationError::Invalid(
                "fleet reply effect or target mismatch",
            ));
        }
        Ok(())
    }
}

// Preserve the existing action index bytes for all prior movement effects.
// Recovery basis decoders use this same function, not an independent key format.
pub(super) fn movement_key(
    scope: FleetScope,
    action: MovementAction,
    spec: &super::MoveAttemptSpec,
) -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-action-key.v1\0");
    hash.update(scope.fleet.as_bytes());
    hash.update(scope.application.as_bytes());
    hash.update(&[1, action as u8]);
    hash.update(spec.id.operation.as_bytes());
    hash.update(&spec.id.sequence.to_be_bytes());
    hash.update(spec.target.cell_id().as_bytes());
    hash.update(spec.incarnation.as_bytes());
    hash.update(spec.source.as_bytes());
    hash.update(&spec.generation.to_be_bytes());
    hash.update(spec.destination.as_bytes());
    Digest::from_bytes(*hash.finalize().as_bytes())
}
