use crate::identity::{CellTarget, Digest, NodeId, SessionId};
use crate::node::NodeMode;

use super::{
    FleetScope, MaintenanceOperation, MaintenancePhase, NodeIntent, OperationError,
    PublishedPosition, Result, nonzero,
};

/// Exact physical node and boot checked before an enrollment effect starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentEndpoint {
    /// Physical identity used for retained cordon lookup.
    pub node: NodeId,
    /// Boot that will participate in the ordinary enrollment protocol.
    pub session: SessionId,
    /// Committed intent revision checked in the acceptance transaction.
    pub intent_revision: u64,
}

impl EnrollmentEndpoint {
    pub(super) fn validate(self) -> Result<()> {
        if !nonzero(self.node.as_bytes())
            || !nonzero(self.session.as_bytes())
            || self.intent_revision == 0
        {
            return Err(OperationError::Invalid("invalid enrollment endpoint"));
        }
        Ok(())
    }

    fn check(self, scope: FleetScope, intent: &NodeIntent, receives_role: bool) -> Result<()> {
        intent.validate()?;
        if intent.scope() != scope
            || intent.node() != self.node
            || intent.session() != self.session
            || intent.revision() != self.intent_revision
            || (receives_role && intent.mode() != NodeMode::Active)
        {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }
}

/// Responsibility that ordinary authority or local lifecycle establishes.
/// Follower responsibility belongs to a node-log epoch, not an invented Cell lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnrollmentRole {
    /// Enroll the target boot in its retained mode. A reboot under maintenance
    /// may renew/withdraw its lease with acquisition closed; this is not readiness.
    Node {
        /// Exact retained mode the ordinary boot enrollment must honor.
        mode: NodeMode,
    },
    /// Open a reader at one checked Cell incarnation and published position.
    Reader {
        /// Catalog-validated Cell target.
        target: CellTarget,
        /// Position used by the existing reader opening protocol.
        position: PublishedPosition,
    },
    /// Enroll the target as follower of the source boot's exact log epoch.
    Follower {
        /// Nonzero node-log epoch; source boot is carried separately.
        log_epoch: u64,
    },
}

/// Immutable request identity and participating sessions for one responsibility.
/// The request digest is an idempotency index; duplicates compare this entire spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentSpec {
    /// Shared journal and application namespace.
    pub scope: FleetScope,
    /// Application-assigned stable request identity.
    pub request: Digest,
    /// Ordinary protocol whose side effect follows pending acceptance.
    pub role: EnrollmentRole,
    /// Existing owner or log leader. Absent only for boot enrollment.
    pub source: Option<EnrollmentEndpoint>,
    /// Boot receiving the new responsibility.
    pub target: EnrollmentEndpoint,
}

impl EnrollmentSpec {
    /// Returns a stable index without conflating it with a full input comparison.
    pub fn key(&self) -> Result<Digest> {
        self.validate()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-enrollment-key.v1\0");
        hash.update(self.scope.fleet.as_bytes());
        hash.update(self.scope.application.as_bytes());
        hash.update(self.request.as_bytes());
        Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.target.validate()?;
        if !nonzero(self.request.as_bytes()) {
            return Err(OperationError::Invalid("zero enrollment request"));
        }
        if let Some(source) = self.source {
            source.validate()?;
            if source.node == self.target.node || source.session == self.target.session {
                return Err(OperationError::Invalid("enrollment endpoints coincide"));
            }
        }
        match &self.role {
            EnrollmentRole::Node { .. } if self.source.is_none() => {}
            EnrollmentRole::Reader { target, position } if self.source.is_some() => {
                position.validate()?;
                if target.application() != self.scope.application {
                    return Err(OperationError::Invalid(
                        "reader enrollment application differs",
                    ));
                }
            }
            EnrollmentRole::Follower { log_epoch } if self.source.is_some() && *log_epoch != 0 => {}
            _ => return Err(OperationError::Invalid("invalid enrollment role or source")),
        }
        Ok(())
    }
}

/// Retained enrollment progress. Expiry never settles pending responsibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EnrollmentStatus {
    /// Accepted before the ordinary side effect; lost replies remain here.
    Pending = 1,
    /// Ordinary enrollment completion has been checked and retained.
    Established = 2,
    /// A checked definite refusal proves no responsibility was created.
    Refused = 3,
    /// Canonical closure or retirement proves the responsibility is settled.
    Retired = 4,
}

/// Checked ordinary-protocol result supplied by the trusted enrollment adapter.
/// Timeout, lease expiry and transport failure are not settlement events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentEvent {
    /// Canonical enrollment completion evidence.
    Established(Digest),
    /// Canonical definite refusal evidence.
    Refused(Digest),
    /// Canonical role closure or retirement evidence.
    Retired(Digest),
}

/// Durable advisory obligation, including failed boots and unknown outcomes.
/// Evidence digests identify application-retained canonical evidence. They do
/// not authenticate it or replace ordinary node-log/reader authority checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentRecord {
    pub(super) spec: EnrollmentSpec,
    pub(super) accepted_at_ms: i64,
    pub(super) updated_at_ms: i64,
    pub(super) status: EnrollmentStatus,
    pub(super) established: Option<Digest>,
    pub(super) settlement: Option<Digest>,
}

impl EnrollmentRecord {
    /// Creates an exclusion tombstone for joined work whose native enrollment
    /// never started. Publish only in an atomic absent-or-Pending transaction
    /// after checking the exact original request and nonexecution evidence.
    /// This does not admit a role or require current receiver intent; delayed
    /// acceptance must replay this terminal row before checking intent.
    pub fn unexecuted_refusal(spec: EnrollmentSpec, evidence: Digest, now_ms: i64) -> Result<Self> {
        let record = Self {
            spec,
            accepted_at_ms: now_ms,
            updated_at_ms: now_ms,
            status: EnrollmentStatus::Refused,
            established: None,
            settlement: Some(evidence),
        };
        record.validate()?;
        Ok(record)
    }

    /// Applies one checked protocol event without changing immutable inputs.
    pub fn apply(&self, event: EnrollmentEvent, now_ms: i64) -> Result<Self> {
        match event {
            EnrollmentEvent::Established(evidence) => self.establish(evidence, now_ms),
            EnrollmentEvent::Refused(evidence) => self.refuse(evidence, now_ms),
            EnrollmentEvent::Retired(evidence) => self.retire(evidence, now_ms),
        }
    }
    /// Calculates first acceptance; publish atomically with both intent checks
    /// and the source intent's retained maintenance operation. A draining source
    /// may enroll a replacement on an Active receiver before Closing. Closing
    /// and Completed fence new source requests even if the intent is unchanged.
    /// An Active source requires no maintenance input. The receiver cannot accept
    /// a new reader/follower role under a cordon. Boot enrollment honors its exact
    /// retained mode without granting readiness. Replay original rows first.
    /// No I/O is performed.
    pub fn pending(
        spec: EnrollmentSpec,
        source_intent: Option<&NodeIntent>,
        source_maintenance: Option<&MaintenanceOperation>,
        target_intent: &NodeIntent,
        now_ms: i64,
    ) -> Result<Self> {
        spec.validate()?;
        match (spec.source, source_intent, source_maintenance) {
            (Some(source), Some(intent), maintenance) => {
                source.check(spec.scope, intent, false)?;
                match (intent.operation(), maintenance) {
                    (None, None) => {}
                    (Some(id), Some(operation)) => {
                        operation.validate()?;
                        // Phase changes intentionally retain the intent revision.
                        // Checking only that revision admits delayed work after
                        // the final evacuation barrier has committed.
                        if operation.id() != id
                            || operation.node() != intent.node()
                            || operation.session() != intent.session()
                            || operation.intent_revision() != intent.revision()
                            || matches!(
                                operation.phase(),
                                MaintenancePhase::Closing | MaintenancePhase::Completed
                            )
                        {
                            return Err(OperationError::Conflict);
                        }
                    }
                    _ => return Err(OperationError::Conflict),
                }
            }
            (None, None, None) => {}
            _ => {
                return Err(OperationError::Invalid("enrollment source inputs differ"));
            }
        }
        let receives_role = match spec.role {
            EnrollmentRole::Node { mode } => {
                if mode != target_intent.mode() {
                    return Err(OperationError::Conflict);
                }
                mode == NodeMode::Active
            }
            _ => true,
        };
        spec.target
            .check(spec.scope, target_intent, receives_role)?;
        let record = Self {
            spec,
            accepted_at_ms: now_ms,
            updated_at_ms: now_ms,
            status: EnrollmentStatus::Pending,
            established: None,
            settlement: None,
        };
        record.validate()?;
        Ok(record)
    }

    /// Compares complete original inputs before returning an existing acceptance.
    /// Do this before rechecking current intents: accepted work may finish after
    /// cordon, and a changed boot or payload cannot reuse its request identity.
    pub fn validate_replay(&self, spec: &EnrollmentSpec) -> Result<()> {
        self.validate()?;
        spec.validate()?;
        if self.spec != *spec {
            return Err(OperationError::Conflict);
        }
        Ok(())
    }

    /// Confirms ordinary enrollment; duplicate replies retain original time.
    pub fn establish(&self, evidence: Digest, now_ms: i64) -> Result<Self> {
        self.change(EnrollmentStatus::Established, evidence, now_ms)
    }

    /// Records checked definite refusal, never a timeout or ambiguous reply.
    pub fn refuse(&self, evidence: Digest, now_ms: i64) -> Result<Self> {
        self.change(EnrollmentStatus::Refused, evidence, now_ms)
    }

    /// Records canonical closure, including a pending enrollment whose reply
    /// was lost. The adapter verifies that its evidence covers this exact spec.
    pub fn retire(&self, evidence: Digest, now_ms: i64) -> Result<Self> {
        self.change(EnrollmentStatus::Retired, evidence, now_ms)
    }

    fn change(&self, status: EnrollmentStatus, evidence: Digest, now_ms: i64) -> Result<Self> {
        self.validate()?;
        if !nonzero(evidence.as_bytes()) || now_ms < self.updated_at_ms {
            return Err(OperationError::Invalid(
                "invalid enrollment evidence or time",
            ));
        }
        if self.status == status {
            let original = if status == EnrollmentStatus::Established {
                self.established
            } else {
                self.settlement
            };
            if original != Some(evidence) {
                return Err(OperationError::Conflict);
            }
            return Ok(self.clone());
        }
        if self.status != EnrollmentStatus::Pending
            && !(self.status == EnrollmentStatus::Established
                && status == EnrollmentStatus::Retired)
        {
            return Err(OperationError::Conflict);
        }
        let mut next = self.clone();
        next.status = status;
        next.updated_at_ms = now_ms;
        if status == EnrollmentStatus::Established {
            next.established = Some(evidence);
        } else {
            next.settlement = Some(evidence);
        }
        next.validate()?;
        Ok(next)
    }

    /// Returns immutable accepted inputs.
    #[must_use]
    pub const fn spec(&self) -> &EnrollmentSpec {
        &self.spec
    }
    /// Returns retained progress, independent of membership expiry.
    #[must_use]
    pub const fn status(&self) -> EnrollmentStatus {
        self.status
    }
    /// Returns first acceptance time, or creation time for an exclusion tombstone.
    #[must_use]
    pub const fn accepted_at_ms(&self) -> i64 {
        self.accepted_at_ms
    }
    /// Returns the last actual transition time; retries do not refresh it.
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
    /// Returns checked original completion evidence, retained through retirement.
    #[must_use]
    pub const fn established_evidence(&self) -> Option<Digest> {
        self.established
    }
    /// Returns checked definite refusal or canonical closure evidence.
    #[must_use]
    pub const fn settlement_evidence(&self) -> Option<Digest> {
        self.settlement
    }
    /// Includes pending and failed-session work until canonical settlement.
    #[must_use]
    pub const fn unresolved(&self) -> bool {
        matches!(
            self.status,
            EnrollmentStatus::Pending | EnrollmentStatus::Established
        )
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        let proof = |value: Option<Digest>| value.is_none_or(|d| nonzero(d.as_bytes()));
        let shape = match self.status {
            EnrollmentStatus::Pending => {
                self.established.is_none()
                    && self.settlement.is_none()
                    && self.updated_at_ms == self.accepted_at_ms
            }
            EnrollmentStatus::Established => {
                self.established.is_some() && self.settlement.is_none()
            }
            EnrollmentStatus::Refused => self.established.is_none() && self.settlement.is_some(),
            EnrollmentStatus::Retired => self.settlement.is_some(),
        };
        if self.accepted_at_ms < 0
            || self.updated_at_ms < self.accepted_at_ms
            || !proof(self.established)
            || !proof(self.settlement)
            || !shape
        {
            return Err(OperationError::Invalid("invalid retained enrollment"));
        }
        Ok(())
    }
}
