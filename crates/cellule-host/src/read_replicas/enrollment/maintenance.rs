//! Original reader/intent barriers for replacement-checked local evacuation.
use super::*;
use crate::fleet::FleetRoster;
use cellule_runtime::fleet::operations::{MaintenanceOperation, MaintenancePhase};
use cellule_runtime::node::NodeMode;

impl ReaderEnrollment {
    pub(in crate::read_replicas) async fn maintenance_roster(
        &self,
        original: &EnrollmentRecord,
        maintenance: &MaintenanceOperation,
        deadline: tokio::time::Instant,
        runtime: &CellRuntime,
    ) -> Result<FleetRoster> {
        if original.spec().scope != self.scope
            || original.spec().target.node != self.node
            || maintenance.node() != self.node
            || original.spec().target.session != maintenance.session()
        {
            return Err(Error::Fenced);
        }
        let snapshot = self
            .journal
            .load_snapshot(self.scope)
            .await
            .map_err(journal)?;
        if now_ms()? >= maintenance.deadline_ms() {
            return Err(Error::Node("reader maintenance operation deadline elapsed"));
        }
        if snapshot.head().maintenance() != Some(maintenance)
            || maintenance.phase() != MaintenancePhase::Evacuating
            || snapshot.registry().bootstrap_revision().is_none()
        {
            return Err(Error::Fenced);
        }
        let roster =
            FleetRoster::collect_admitted(self.journal.as_ref(), &snapshot, deadline, runtime)
                .await?;
        let intent = roster
            .intents()
            .iter()
            .find(|intent| intent.node() == self.node)
            .ok_or(Error::Fenced)?;
        if intent.session() != maintenance.session()
            || intent.revision() != maintenance.intent_revision()
            || intent.mode() != NodeMode::Draining
        {
            return Err(Error::Fenced);
        }
        let current = roster
            .enrollments()
            .iter()
            .find(|row| row.spec() == original.spec())
            .ok_or(Error::Control("original reader enrollment is absent"))?;
        current
            .validate_replay(original.spec())
            .map_err(operation)?;
        if current.accepted_at_ms() != original.accepted_at_ms()
            || current.established_evidence() != original.established_evidence()
            || !matches!(
                current.status(),
                EnrollmentStatus::Established | EnrollmentStatus::Retired
            )
        {
            return Err(Error::Fenced);
        }
        Ok(roster)
    }

    pub(in crate::read_replicas) async fn confirm_maintenance_roster(
        &self,
        roster: &FleetRoster,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        roster.confirm(self.journal.as_ref(), deadline).await
    }
}
