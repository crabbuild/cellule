use super::*;

pub(super) fn select(
    roster: &FleetRoster,
    request: &FleetFailedBootProcessRequest,
    original: &EnrollmentRecord,
) -> Result<EnrollmentRecord> {
    original.to_bytes().map_err(operation)?;
    let spec = original.spec();
    if !matches!(spec.role, EnrollmentRole::Reader { .. })
        || spec.scope != roster.snapshot().head().scope()
        || spec.target.node != request.boot.spec().target.node
        || spec.target.session != request.boot.spec().target.session
        || spec.target.intent_revision < request.boot.spec().target.intent_revision
        || original.status() == EnrollmentStatus::Refused
    {
        return Err(Error::Fenced);
    }
    let key = spec.key().map_err(operation)?;
    let current = roster
        .enrollments()
        .iter()
        .find(|row| row.spec().key().is_ok_and(|candidate| candidate == key))
        .ok_or(Error::Fenced)?;
    history(original, current)?;
    if original.status() == EnrollmentStatus::Retired && original != current {
        return Err(Error::Fenced);
    }
    Ok(current.clone())
}

fn history(original: &EnrollmentRecord, returned: &EnrollmentRecord) -> Result<()> {
    returned
        .validate_replay(original.spec())
        .map_err(operation)?;
    if returned.accepted_at_ms() != original.accepted_at_ms()
        || returned.updated_at_ms() < original.updated_at_ms()
        || returned.status() == EnrollmentStatus::Refused
        || original
            .established_evidence()
            .is_some_and(|evidence| returned.established_evidence() != Some(evidence))
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

pub(super) fn retirement(
    original: &EnrollmentRecord,
    process: &FleetFailedBootProcessEvidence,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-failed-reader-retirement.v1\0");
    hash.update(process.request.as_bytes());
    hash.update(process.witness.as_bytes());
    hash.update(&original.spec().to_bytes().map_err(operation)?);
    hash.update(&original.accepted_at_ms().to_be_bytes());
    // Establishment may complete after the original capture. The journal's
    // canonical transition preserves that history; it cannot change this event.
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}

pub(super) fn result(
    original: &EnrollmentRecord,
    returned: &EnrollmentRecord,
    evidence: Digest,
) -> Result<()> {
    history(original, returned)?;
    if returned.status() != EnrollmentStatus::Retired
        || returned.settlement_evidence() != Some(evidence)
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

pub(super) fn closure_digest(
    reader: &EnrollmentRecord,
    process: &FleetFailedBootProcessEvidence,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-failed-reader-closure.v1\0");
    hash.update(&reader.to_bytes().map_err(operation)?);
    hash.update(retirement(reader, process)?.as_bytes());
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
