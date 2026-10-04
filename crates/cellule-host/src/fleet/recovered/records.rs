use super::*;
use cellule_runtime::fleet::operations::{EnrollmentRole, EnrollmentStatus};

pub(super) fn select(
    roster: &FleetRoster,
    leader_node: NodeId,
    retired: &SealedNodeLog,
) -> Result<Vec<EnrollmentRecord>> {
    let rows = roster
        .enrollments()
        .iter()
        .filter(|row| {
            row.spec().source.is_some_and(|source| source.session == retired.session())
                && matches!(row.spec().role, EnrollmentRole::Follower { log_epoch } if log_epoch == retired.log().epoch())
                && row.status() != EnrollmentStatus::Refused
        })
        .collect::<Vec<_>>();
    if rows.len() != retired.log().members().len() || rows.len() > 16 {
        return Err(Error::Control(
            "recovered follower enrollment set is incomplete",
        ));
    }
    let mut members = Vec::with_capacity(rows.len());
    let mut source = None;
    for member in retired.log().members() {
        let mut matches = rows.iter().filter(|row| row.spec().target.node == *member);
        let row = matches.next().ok_or(Error::Fenced)?;
        let endpoint = row.spec().source.ok_or(Error::Fenced)?;
        if matches.next().is_some()
            || endpoint.node != leader_node
            || source.is_some_and(|original| original != endpoint)
            || row.spec().scope != roster.snapshot().head().scope()
        {
            return Err(Error::Fenced);
        }
        source = Some(endpoint);
        row.to_bytes().map_err(operation)?;
        members.push((*row).clone());
    }
    Ok(members)
}

fn authority(hash: &mut blake3::Hasher, leader_node: NodeId, retired: &SealedNodeLog) {
    hash.update(leader_node.as_bytes());
    hash.update(retired.session().as_bytes());
    hash.update(&retired.log().epoch().to_be_bytes());
    hash.update(&[u8::from(retired.log().active())]);
    hash.update(&retired.log().tiered_through().to_be_bytes());
    hash.update(&(retired.log().members().len() as u64).to_be_bytes());
    for member in retired.log().members() {
        hash.update(member.as_bytes());
    }
    hash.update(&[u8::from(retired.log().recovery_manifest().is_some())]);
    if let Some(manifest) = retired.log().recovery_manifest() {
        hash.update(manifest.as_bytes());
    }
}

pub(super) fn evidence(
    leader_node: NodeId,
    retired: &SealedNodeLog,
    row: &EnrollmentRecord,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-recovered-follower-retirement.v1\0");
    authority(&mut hash, leader_node, retired);
    let spec = row.spec().to_bytes().map_err(operation)?;
    hash.update(&(spec.len() as u64).to_be_bytes());
    hash.update(&spec);
    hash.update(&row.accepted_at_ms().to_be_bytes());
    // Establishment can race the closing transaction. Its mutable evidence and
    // current timestamps cannot change this original request's retirement event.
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}

pub(super) fn validate_terminal(row: &EnrollmentRecord, evidence: Digest) -> Result<()> {
    if row.status() == EnrollmentStatus::Retired && row.settlement_evidence() != Some(evidence) {
        return Err(Error::Control(
            "recovered follower settlement evidence differs",
        ));
    }
    Ok(())
}

pub(super) fn validate_result(
    original: &EnrollmentRecord,
    result: &EnrollmentRecord,
    evidence: Digest,
) -> Result<()> {
    result.validate_replay(original.spec()).map_err(operation)?;
    result.to_bytes().map_err(operation)?;
    if result.status() != EnrollmentStatus::Retired
        || result.accepted_at_ms() != original.accepted_at_ms()
        || result.updated_at_ms() < original.updated_at_ms()
        || result.settlement_evidence() != Some(evidence)
        || original
            .established_evidence()
            .is_some_and(|old| result.established_evidence() != Some(old))
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

pub(super) fn closure_digest(
    leader_node: NodeId,
    retired: &SealedNodeLog,
    rows: &[EnrollmentRecord],
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-recovered-follower-closure.v1\0");
    authority(&mut hash, leader_node, retired);
    for row in rows {
        let encoded = row.to_bytes().map_err(operation)?;
        hash.update(&(encoded.len() as u64).to_be_bytes());
        hash.update(&encoded);
    }
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
