//! Complete lookup through the current canonical reader policy verifier.
use super::verification::{adapter, bounded};
use super::*;
use crate::fleet::maintenance_policies::requests;
use cellule_runtime::{Error, Result, fleet::operations::EnrollmentRole};
use tokio::time::Instant;

impl FleetReaderEvacuationVerifier {
    /// Looks up every eligible original/current retired reader donor at the exact
    /// full roster barrier and rechecks current policy and native ready prefixes.
    /// Missing history remains absent for the maintenance matcher to report as
    /// MissingPolicy. All original/source/unknown requests remain in that matcher.
    /// This starts no effects and grants no settlement/finalization rights.
    pub async fn collect_maintenance(
        &self,
        journal: &dyn FleetReaderEvacuationJournal,
        original: &FleetMaintenanceEnrollments,
        roster: &FleetRoster,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Vec<FleetReaderEvacuationCheck>> {
        let started = clock()?;
        let mut last = started;
        let mut clock = || {
            let now = clock()?;
            if started < 0 || now < last || now - started > 30_000 {
                return Err(Error::Deadline);
            }
            last = now;
            Ok(now)
        };
        bounded(deadline, async {
            let required = requests::required(original, roster)?;
            roster.confirm(journal, deadline).await?;
            let maintenance = original.original().operation();
            let mut checks = Vec::new();
            for request in required {
                if !request.retired_donor(maintenance.node())
                    || !matches!(request.current.spec().role, EnrollmentRole::Reader { .. })
                {
                    continue;
                }
                clock()?;
                let Some(record) = journal
                    .latest_reader_evacuation(
                        roster.snapshot(),
                        maintenance.id(),
                        request.current.spec().key().map_err(operation)?,
                    )
                    .await
                    .map_err(adapter)?
                else {
                    continue;
                };
                if record.operation().id() != maintenance.id()
                    || record.retired() != request.current
                {
                    return Err(Error::Fenced);
                }
                requests::validate_original(request.original, Some(record.original_digest()))?;
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
