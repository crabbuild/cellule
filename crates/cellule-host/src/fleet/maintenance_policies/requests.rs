//! One canonical original/current request set for lookup and final matching.
use super::{FleetMaintenanceEnrollments, FleetRoster, enrollment_digest, operation};
use cellule_runtime::{
    Error, Result,
    fleet::operations::{EnrollmentRecord, EnrollmentStatus, MaintenanceEnrollmentInventory},
    identity::{Digest, NodeId},
};
use std::collections::BTreeMap;

pub(in crate::fleet) struct RequiredRequest<'a> {
    pub(in crate::fleet) original: Option<&'a EnrollmentRecord>,
    pub(in crate::fleet) current: &'a EnrollmentRecord,
}
impl RequiredRequest<'_> {
    pub(in crate::fleet) fn retired_donor(&self, node: NodeId) -> bool {
        self.current.spec().target.node == node
            && self.current.status() == EnrollmentStatus::Retired
            && self.current.established_evidence().is_some()
    }
}

pub(in crate::fleet) fn required<'a>(
    original: &'a FleetMaintenanceEnrollments,
    roster: &'a FleetRoster,
) -> Result<Vec<RequiredRequest<'a>>> {
    if original.snapshot() != roster.snapshot() || original.roster_digest() != roster.digest()? {
        return Err(Error::Fenced);
    }
    let mut requests = BTreeMap::new();
    for row in original.entries() {
        requests.insert(*row.spec().key().map_err(operation)?.as_bytes(), Some(row));
    }
    for row in roster.enrollments() {
        if MaintenanceEnrollmentInventory::includes(original.original().operation().node(), row) {
            requests
                .entry(*row.spec().key().map_err(operation)?.as_bytes())
                .or_insert(None);
        }
    }
    let current = roster
        .enrollments()
        .iter()
        .map(|row| Ok((*row.spec().key().map_err(operation)?.as_bytes(), row)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    requests
        .into_iter()
        .map(|(key, accepted)| {
            let row = *current.get(&key).ok_or(Error::Fenced)?;
            if let Some(accepted) = accepted {
                accepted.validate_replay(row.spec()).map_err(operation)?;
                if accepted.accepted_at_ms() != row.accepted_at_ms() {
                    return Err(Error::Fenced);
                }
            }
            Ok(RequiredRequest {
                original: accepted,
                current: row,
            })
        })
        .collect()
}

pub(in crate::fleet) fn validate_original(
    accepted: Option<&EnrollmentRecord>,
    digest: Option<Digest>,
) -> Result<()> {
    if let Some(accepted) = accepted
        && accepted.status() == EnrollmentStatus::Established
        && Some(enrollment_digest(accepted)?) != digest
    {
        return Err(Error::Fenced);
    }
    Ok(())
}
