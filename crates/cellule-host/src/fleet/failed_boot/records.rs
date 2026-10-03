use super::*;
use crate::fleet::FleetFollowerReferences;
use cellule_runtime::{identity::NodeId, node::log_state::NodeLogPhase};

pub(super) fn boot(roster: &FleetRoster, original: &EnrollmentRecord) -> Result<EnrollmentRecord> {
    original.to_bytes().map_err(operation)?;
    let spec = original.spec();
    if !matches!(spec.role, EnrollmentRole::Node { .. })
        || spec.source.is_some()
        || spec.scope != roster.snapshot().head().scope()
        || original.established_evidence().is_none()
        || !matches!(
            original.status(),
            EnrollmentStatus::Established | EnrollmentStatus::Retired
        )
    {
        return Err(Error::Fenced);
    }
    let mut boots = roster.enrollments().iter().filter(|row| {
        matches!(row.spec().role, EnrollmentRole::Node { .. })
            && row.spec().target.node == spec.target.node
            && row.spec().target.session == spec.target.session
            && row.status() != EnrollmentStatus::Refused
    });
    let current = boots.next().ok_or(Error::Fenced)?;
    current.validate_replay(spec).map_err(operation)?;
    if boots.next().is_some()
        || current.accepted_at_ms() != original.accepted_at_ms()
        || current.established_evidence() != original.established_evidence()
        || current.updated_at_ms() < original.updated_at_ms()
        || !matches!(
            current.status(),
            EnrollmentStatus::Established | EnrollmentStatus::Retired
        )
    {
        return Err(Error::Fenced);
    }
    Ok(current.clone())
}

pub(super) fn select(
    roster: &FleetRoster,
    original: &EnrollmentRecord,
) -> Result<EnrollmentRecord> {
    let current = boot(roster, original)?;
    let spec = current.spec();
    for row in roster.enrollments() {
        if row.spec() == spec || !row.unresolved() {
            continue;
        }
        if row
            .spec()
            .source
            .into_iter()
            .chain(std::iter::once(row.spec().target))
            .any(|endpoint| {
                endpoint.node == spec.target.node && endpoint.session == spec.target.session
            })
        {
            return Err(Error::Control(
                "failed boot has unresolved enrollment responsibilities",
            ));
        }
    }
    Ok(current)
}

pub(super) async fn references(
    directory: &NodeDirectory,
    roster: &FleetRoster,
    node: NodeId,
    session: SessionId,
    deadline: Instant,
    clock: &mut impl FnMut() -> Result<i64>,
) -> Result<()> {
    let references =
        FleetFollowerReferences::collect(directory, roster, node, 128, deadline, clock).await?;
    references.validate_enrollments(roster)?;
    for reference in references.entries() {
        if reference.log.phase() == NodeLogPhase::Retired {
            continue;
        }
        let matching = |row: &&EnrollmentRecord| {
            row.spec().target.node == node
                && row.spec().source.is_some_and(|source| {
                    source.node == reference.leader_node && source.session == reference.leader
                })
                && matches!(row.spec().role, EnrollmentRole::Follower { log_epoch } if log_epoch == reference.log.epoch())
        };
        let mut original = false;
        let mut replacement = false;
        for row in roster.enrollments().iter().filter(matching) {
            original |=
                row.spec().target.session == session && row.status() != EnrollmentStatus::Refused;
            replacement |= row.spec().target.session != session && row.unresolved();
        }
        // A successor may carry other epochs on the same physical node. It
        // cannot substitute for this original boot's unresolved native lane or
        // make a contradictory Retired registry row agree with live authority.
        if original || !replacement {
            return Err(Error::Control(
                "failed boot retains a foreign follower responsibility",
            ));
        }
    }
    Ok(())
}

pub(super) fn request_digest(
    boot: &EnrollmentRecord,
    canonical: &NodeSessionClosure,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-failed-boot-process-request.v1\0");
    hash.update(&boot.spec().to_bytes().map_err(operation)?);
    hash.update(&boot.accepted_at_ms().to_be_bytes());
    hash.update(boot.established_evidence().ok_or(Error::Fenced)?.as_bytes());
    hash.update(canonical.node().as_bytes());
    hash.update(canonical.session().as_bytes());
    hash.update(&canonical.expires_at_ms().to_be_bytes());
    hash.update(&canonical.retired_at_ms().to_be_bytes());
    hash.update(&[u8::from(canonical.log().is_some())]);
    if let Some(log) = canonical.log() {
        hash.update(&log.epoch().to_be_bytes());
        hash.update(b"retired\0");
        hash.update(&[u8::from(log.active())]);
        hash.update(&log.tiered_through().to_be_bytes());
        hash.update(&(log.members().len() as u64).to_be_bytes());
        for member in log.members() {
            hash.update(member.as_bytes());
        }
        hash.update(&[u8::from(log.recovery_manifest().is_some())]);
        if let Some(manifest) = log.recovery_manifest() {
            hash.update(manifest.as_bytes());
        }
    }
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
pub(super) fn retirement(process: &FleetFailedBootProcessEvidence) -> Digest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-failed-boot-retirement.v1\0");
    hash.update(process.request.as_bytes());
    hash.update(process.witness.as_bytes());
    Digest::from_bytes(*hash.finalize().as_bytes())
}
pub(super) fn result(
    original: &EnrollmentRecord,
    returned: &EnrollmentRecord,
    evidence: Digest,
) -> Result<()> {
    returned
        .validate_replay(original.spec())
        .map_err(operation)?;
    if returned.status() != EnrollmentStatus::Retired
        || returned.accepted_at_ms() != original.accepted_at_ms()
        || returned.established_evidence() != original.established_evidence()
        || returned.updated_at_ms() < original.updated_at_ms()
        || returned.settlement_evidence() != Some(evidence)
    {
        return Err(Error::Fenced);
    }
    Ok(())
}
pub(super) fn closure_digest(
    boot: &EnrollmentRecord,
    process: &FleetFailedBootProcessEvidence,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-failed-boot-closure.v1\0");
    hash.update(&boot.to_bytes().map_err(operation)?);
    hash.update(retirement(process).as_bytes());
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
