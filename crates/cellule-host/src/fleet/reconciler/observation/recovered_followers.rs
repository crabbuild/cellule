//! Canonical recovered follower retirement retained at the maintenance barrier.
use super::*;
use crate::fleet::{FleetFollowerEvacuationCheck, FleetReaderEvacuationCheck};
use cellule_runtime::fleet::operations::{EnrollmentRole, EnrollmentStatus};
use cellule_runtime::node::log_state::NodeLogPhase;

impl FleetObservation {
    /// Retains exact canonical recovered follower closures for maintenance
    /// matching. A failed source additionally needs its matching failed-boot
    /// process closure before the source-side request is considered checked.
    /// This does not upgrade `complete` or grant finalization rights.
    pub fn with_recovered_follower_closures(
        mut self,
        closures: Vec<FleetRecoveredFollowerClosure>,
    ) -> Result<Self> {
        if self.recovered_follower_closures.is_some() {
            return Err(Error::Control(
                "recovered follower closures already retained",
            ));
        }
        self.recovered_follower_closures = Some(closures);
        self.validate_role_coverage()?;
        Ok(self)
    }

    /// Canonically retired recovered epochs at this observation barrier.
    #[must_use]
    pub fn recovered_follower_closures(&self) -> Option<&[FleetRecoveredFollowerClosure]> {
        self.recovered_follower_closures.as_deref()
    }

    pub(super) fn validate_recovered_follower_closures(&self) -> Result<()> {
        let Some(closures) = &self.recovered_follower_closures else {
            return Ok(());
        };
        if closures.len() > 10_000 {
            return Err(Error::Capacity("recovered follower closure bound exceeded"));
        }
        let first = closures.first();
        let expected = self
            .roster
            .as_ref()
            .map(FleetRoster::snapshot)
            .or_else(|| self.role_coverage.as_ref().map(FleetRoleCoverage::snapshot))
            .or_else(|| {
                self.original_writer_successors
                    .as_ref()
                    .map(|proof| proof.original().snapshot())
            })
            .or_else(|| {
                self.failed_boot_closures
                    .as_ref()
                    .and_then(|proofs| proofs.first())
                    .map(FleetFailedBootClosure::snapshot)
            })
            .or_else(|| {
                self.maintenance_enrollments
                    .as_ref()
                    .map(FleetMaintenanceEnrollments::snapshot)
            })
            .or_else(|| {
                self.maintenance_nonexecution
                    .as_ref()
                    .map(FleetMaintenanceNonexecution::snapshot)
            })
            .or_else(|| {
                self.source_reader_policies
                    .as_ref()
                    .map(FleetSourceReaderPolicies::snapshot)
            })
            .or_else(|| {
                self.reader_evacuations()
                    .and_then(|rows| rows.first())
                    .map(FleetReaderEvacuationCheck::snapshot)
                    .or_else(|| {
                        self.follower_evacuations()
                            .and_then(|rows| rows.first())
                            .map(FleetFollowerEvacuationCheck::snapshot)
                    })
            })
            .or_else(|| first.map(FleetRecoveredFollowerClosure::snapshot));
        let mut epochs = HashSet::new();
        for closure in closures {
            let sealed = closure.retired();
            let interval = closure.interval();
            if closure.snapshot().head().scope() != self.scope
                || closure.snapshot().registry() != self.registry
                || expected != Some(closure.snapshot())
                || interval.0 < self.capture_started_at_ms
                || interval.1 > self.capture_finished_at_ms
                || interval.1 < interval.0
                || sealed.log().phase() != NodeLogPhase::Retired
                || closure.members().is_empty()
                || closure.members().len() != sealed.log().members().len()
                || !epochs.insert((
                    closure.leader_node(),
                    sealed.session(),
                    sealed.log().epoch(),
                ))
            {
                return Err(Error::Node(
                    "recovered follower observation barrier differs",
                ));
            }
            let mut targets = HashSet::new();
            for row in closure.members() {
                let source = row.spec().source.ok_or(Error::Fenced)?;
                let EnrollmentRole::Follower { log_epoch } = row.spec().role else {
                    return Err(Error::Fenced);
                };
                if row.spec().scope != self.scope
                    || source.node != closure.leader_node()
                    || source.session != sealed.session()
                    || log_epoch != sealed.log().epoch()
                    || row.status() != EnrollmentStatus::Retired
                    || row.settlement_evidence().is_none()
                    || !sealed.log().members().contains(&row.spec().target.node)
                    || !targets.insert(row.spec().target.node)
                {
                    return Err(Error::Fenced);
                }
                if let Some(roster) = &self.roster {
                    let key = row.spec().key().map_err(super::super::operation)?;
                    if roster
                        .enrollments()
                        .iter()
                        .find(|current| current.spec().key().is_ok_and(|value| value == key))
                        != Some(row)
                    {
                        return Err(Error::Node(
                            "recovered follower row differs from current roster",
                        ));
                    }
                }
            }
            if sealed
                .log()
                .members()
                .iter()
                .any(|member| !targets.contains(member))
            {
                return Err(Error::Fenced);
            }
            if let Some(boot) = self.failed_boot_closures.as_ref().and_then(|boots| {
                boots.iter().find(|proof| {
                    let target = proof.boot().spec().target;
                    target.node == closure.leader_node() && target.session == sealed.session()
                })
            }) && (boot.snapshot() != closure.snapshot()
                || boot.canonical().node() != closure.leader_node()
                || boot.canonical().session() != sealed.session()
                || boot.canonical().log() != Some(sealed.log()))
            {
                return Err(Error::Node(
                    "recovered follower and failed boot evidence differ",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn hash_recovered_follower_closures(&self, hash: &mut blake3::Hasher) -> Result<()> {
        let Some(closures) = &self.recovered_follower_closures else {
            hash.update(&[0]);
            return Ok(());
        };
        hash.update(&[1]);
        let mut ordered = closures.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|closure| {
            (
                *closure.leader_node().as_bytes(),
                *closure.retired().session().as_bytes(),
                closure.retired().log().epoch(),
            )
        });
        hash.update(&(ordered.len() as u64).to_be_bytes());
        for closure in ordered {
            hash.update(closure.digest().as_bytes());
            for bytes in [
                closure
                    .snapshot()
                    .head()
                    .to_bytes()
                    .map_err(super::super::operation)?,
                closure
                    .snapshot()
                    .registry()
                    .to_bytes()
                    .map_err(super::super::operation)?,
            ] {
                hash.update(&(bytes.len() as u64).to_be_bytes());
                hash.update(&bytes);
            }
            for time in [closure.interval().0, closure.interval().1] {
                hash.update(&time.to_be_bytes());
            }
        }
        Ok(())
    }
}
