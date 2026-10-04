//! Complete lookup through the current canonical follower policy verifier.
use super::verification::{adapter, bounded, interval};
use super::*;
use crate::fleet::maintenance_policies::requests;
use cellule_runtime::{Error, Result, fleet::operations::EnrollmentRole};
use std::collections::HashSet;
use tokio::time::Instant;

impl FleetFollowerEvacuationVerifier {
    /// Looks up every eligible original/current retired follower donor at the
    /// exact full roster barrier and rechecks native source/member policy.
    /// Missing history remains absent for the maintenance matcher to report as
    /// MissingPolicy. Source/failed-owner and unknown requests stay blocking.
    /// Applications account the bounded retained native checks. No effects start.
    pub async fn collect_maintenance(
        &self,
        journal: &dyn FleetFollowerEvacuationJournal,
        original: &FleetMaintenanceEnrollments,
        roster: &FleetRoster,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Vec<FleetFollowerEvacuationCheck>> {
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let now = clock()?;
            interval(started, last, now)?;
            last = now;
            Ok(now)
        };
        bounded(deadline, async {
            let required = requests::required(original, roster)?;
            roster.confirm(journal, deadline).await?;
            let maintenance = original.original().operation();
            let mut checks = Vec::new();
            let mut witnesses = HashSet::new();
            for request in required {
                if !request.retired_donor(maintenance.node())
                    || !matches!(request.current.spec().role, EnrollmentRole::Follower { .. })
                {
                    continue;
                }
                clock()?;
                let key = request.current.spec().key().map_err(operation)?;
                let Some(record) = journal
                    .latest_follower_evacuation(roster.snapshot(), maintenance.id(), key)
                    .await
                    .map_err(adapter)?
                else {
                    continue;
                };
                if record.operation().id() != maintenance.id()
                    || record.original_key() != key
                    || !record.retired().iter().any(|row| row == request.current)
                {
                    return Err(Error::Fenced);
                }
                requests::validate_original(request.original, Some(record.original_digest()))?;
                // Bound all retained ensemble witnesses before native graph copies.
                // Overlapping ensembles cannot substitute for distinct original work.
                for row in record.retired() {
                    if witnesses.len() >= 10_000 {
                        return Err(Error::Capacity("maintenance policy witness bound exceeded"));
                    }
                    if !witnesses.insert(row.spec().key().map_err(operation)?) {
                        return Err(Error::Node("maintenance policy obligation is duplicated"));
                    }
                }
                let check = self.recheck(journal, &record, deadline, &mut clock).await?;
                if check.snapshot() != roster.snapshot()
                    || check.roster_digest() != roster.digest()?
                {
                    return Err(Error::Fenced);
                }
                checks.push(check);
            }
            roster.confirm(journal, deadline).await?;
            clock()?;
            Ok(checks)
        })
        .await
    }
}
