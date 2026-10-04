use super::*;
use cellule_runtime::fleet::operations::{
    FleetAction, FleetActionKind, MaintenanceAction, MaintenanceOperation, MaintenancePhase,
};

#[cfg(test)]
mod tests;

/// Opaque proof that complete role coverage and replacement policy matched one
/// fresh full journal barrier before a SettleRoles action was issued.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetRoleSettlement {
    scope: FleetScope,
    action_key: Digest,
    operation: MaintenanceOperation,
    snapshot: crate::fleet::FleetJournalSnapshot,
    inventory: Digest,
    failed_boot_closure: Option<Digest>,
    capture_interval: (i64, i64),
}

impl FleetRoleSettlement {
    /// Exact inventory digest retained by the durable SettleRoles receipt.
    #[must_use]
    pub const fn inventory(&self) -> Digest {
        self.inventory
    }

    /// Exact process-closure proof for a maintenance node that has already
    /// stopped. `None` means settlement must be performed by its live session.
    #[must_use]
    pub const fn failed_boot_closure(&self) -> Option<Digest> {
        self.failed_boot_closure
    }

    /// Exact current journal head revision covered by this proof.
    #[must_use]
    pub fn head_revision(&self) -> u64 {
        self.snapshot.head().revision()
    }

    /// Exact current registry version covered by this proof.
    #[must_use]
    pub const fn registry(&self) -> RegistryVersion {
        self.snapshot.registry()
    }

    /// Original full native/foreign collection interval.
    #[must_use]
    pub const fn capture_interval(&self) -> (i64, i64) {
        self.capture_interval
    }

    /// Checks that the certificate belongs to this exact SettleRoles envelope.
    pub fn validate_for(&self, action: &FleetAction) -> Result<()> {
        let operation = match action.kind() {
            FleetActionKind::Maintenance {
                action: MaintenanceAction::SettleRoles,
                operation,
            } => operation,
            _ => return Err(Error::Fenced),
        };
        if action.scope() != self.scope
            || action.key().map_err(crate::fleet::operation)? != self.action_key
            || action.journal_revision() != self.snapshot.head().revision()
            || self.snapshot.head().scope() != self.scope
            || self.snapshot.head().maintenance() != Some(operation.as_ref())
            || &self.operation != operation.as_ref()
            || operation.phase() != MaintenancePhase::Evacuating
            || self.inventory.as_bytes().iter().all(|byte| *byte == 0)
            || self
                .failed_boot_closure
                .is_some_and(|digest| digest.as_bytes().iter().all(|byte| *byte == 0))
            || self.capture_interval.0 < 0
            || self.capture_interval.1 < self.capture_interval.0
            || action.issued_at_ms() < self.capture_interval.1
            || action.issued_at_ms() - self.capture_interval.0 > 30_000
        {
            return Err(Error::Fenced);
        }
        Ok(())
    }

    /// Checks that this settlement and a fresh closed-boot proof cover the same
    /// exact node, session, full journal barrier, and process-closure digest.
    pub fn validate_failed_boot_closure(
        &self,
        action: &FleetAction,
        closure: &FleetFailedBootClosure,
    ) -> Result<()> {
        self.validate_for(action)?;
        let operation = &self.operation;
        let target = closure.boot().spec().target;
        if self.failed_boot_closure != Some(closure.digest())
            || closure.snapshot() != &self.snapshot
            || closure.boot().status()
                != cellule_runtime::fleet::operations::EnrollmentStatus::Retired
            || target.node != operation.node()
            || target.session != operation.session()
            || closure.canonical().node() != operation.node()
            || closure.canonical().session() != operation.session()
        {
            return Err(Error::Fenced);
        }
        Ok(())
    }
}

impl FleetObservation {
    /// Creates the exact host proof accepted by SettleRoles only when the
    /// complete inventory, original obligations, native role graph, and fresh
    /// reader/follower replacement policies all cover the same snapshot.
    pub fn role_settlement(
        &self,
        action: &FleetAction,
        now_ms: i64,
    ) -> Result<FleetRoleSettlement> {
        let operation = match action.kind() {
            FleetActionKind::Maintenance {
                action: MaintenanceAction::SettleRoles,
                operation,
            } => operation,
            _ => return Err(Error::Fenced),
        };
        let roster = self.roster.as_ref().ok_or(Error::Control(
            "role settlement requires the complete roster",
        ))?;
        let role_coverage = self.role_coverage.as_ref().ok_or(Error::Control(
            "role settlement requires full role coverage",
        ))?;
        let original = self.maintenance_enrollments.as_ref().ok_or(Error::Control(
            "role settlement requires original maintenance enrollments",
        ))?;
        let policies = self.maintenance_policies.as_ref().ok_or(Error::Control(
            "role settlement requires complete replacement policy coverage",
        ))?;
        let placements = self.placements(now_ms)?;
        let snapshot = roster.snapshot();
        let roster_digest = roster.digest()?;
        let original_operation = original.original().operation();
        let covered_interval = |interval: (i64, i64)| {
            interval.0 >= self.capture_started_at_ms
                && interval.1 <= self.capture_finished_at_ms
                && interval.1 >= interval.0
        };
        if !self.complete
            || !self.counts_match(&placements)
            || !roster.covers_advertisements(&self.nodes, now_ms)?
            || roster.enrollments().iter().any(|row| {
                row.status() == cellule_runtime::fleet::operations::EnrollmentStatus::Pending
            })
            || self
                .cells
                .iter()
                .any(|owned| owned.node == operation.node())
            || role_coverage.snapshot() != snapshot
            || role_coverage.roster_digest() != roster_digest
            || role_coverage.pending_enrollments() != 0
            || !covered_interval(role_coverage.interval())
            || original.snapshot() != snapshot
            || original.roster_digest() != roster_digest
            || !covered_interval(original.interval())
            || original_operation.id() != operation.id()
            || original_operation.request_digest() != operation.request_digest()
            || original_operation.node() != operation.node()
            || original_operation.created_at_ms() != operation.created_at_ms()
            || policies.snapshot() != snapshot
            || policies.roster_digest() != roster_digest
            || !policies.is_complete()
            || !covered_interval(policies.interval())
            || operation.phase() != MaintenancePhase::Evacuating
            || action.scope() != self.scope
            || action.journal_revision() != snapshot.head().revision()
            || self.registry != snapshot.registry()
        {
            return Err(Error::Control(
                "role settlement evidence does not cover the current maintenance barrier",
            ));
        }
        action
            .authorize_against(snapshot.head(), now_ms)
            .map_err(crate::fleet::operation)?;
        let inventory = self.digest(now_ms)?;
        let failed_boot_closure = self.failed_boot_closures.as_ref().and_then(|closures| {
            closures.iter().find(|closure| {
                let target = closure.boot().spec().target;
                target.node == operation.node() && target.session == operation.session()
            })
        });
        let target_is_live = self
            .nodes
            .iter()
            .any(|node| node.node() == operation.node() && node.session() == operation.session());
        if target_is_live == failed_boot_closure.is_some() {
            return Err(Error::Control(
                "role settlement target must have exactly one live boot or process closure",
            ));
        }
        if let Some(closure) = failed_boot_closure
            && (closure.snapshot() != snapshot
                || closure.boot().status()
                    != cellule_runtime::fleet::operations::EnrollmentStatus::Retired)
        {
            return Err(Error::Fenced);
        }
        let proof = FleetRoleSettlement {
            scope: self.scope,
            action_key: action.key().map_err(crate::fleet::operation)?,
            operation: operation.as_ref().clone(),
            snapshot: snapshot.clone(),
            inventory,
            failed_boot_closure: failed_boot_closure.map(FleetFailedBootClosure::digest),
            capture_interval: (self.capture_started_at_ms, self.capture_finished_at_ms),
        };
        proof.validate_for(action)?;
        Ok(proof)
    }
}
