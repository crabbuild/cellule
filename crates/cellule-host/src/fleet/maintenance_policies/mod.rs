//! Complete original/current request matching for maintenance replacement policy.
use super::{
    FleetFailedBootClosure, FleetFollowerEvacuationCheck, FleetJournalSnapshot,
    FleetMaintenanceEnrollments, FleetReaderEvacuationCheck, FleetRecoveredFollowerClosure,
    FleetRoster, FleetSourceReaderPolicies, operation,
};
use cellule_runtime::fleet::operations::{EnrollmentRecord, EnrollmentStatus, RegistryVersion};
use cellule_runtime::identity::Digest;
use cellule_runtime::{Error, Result};
use std::collections::BTreeMap;

mod nonexecution;
pub(in crate::fleet) mod requests;
pub use nonexecution::{
    FleetEnrollmentNonexecution, FleetEnrollmentNonexecutionEvidence,
    FleetEnrollmentNonexecutionRequest, FleetMaintenanceNonexecution,
};

/// Policy result for one exact retained request. Unchecked states remain blockers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetMaintenancePolicyStatus {
    /// Current ready readers and policy checked from this immutable history.
    Reader(Digest),
    /// Current native replacement ensemble and policy checked from this history.
    Follower(Digest),
    /// Independently joined original work whose role effect never committed.
    Nonexecution(Digest),
    /// Exact native source reader joined with current successor and reader policy.
    SourceReader(Digest),
    /// Original follower owner failed, its canonical log was recovered and
    /// retired, and the exact failed process boot was joined.
    RecoveredFollower(Digest),
    /// Original acceptance still has an unknown native outcome.
    Pending,
    /// An installed original role has not been retired with checked replacement policy.
    Established,
    /// Closed donor request has no freshly checked replacement policy.
    MissingPolicy,
    /// Source-side or failed-owner succession requires separate checked policy.
    SourceSuccessor,
    /// An original unknown acceptance has no checked nonexecution/policy proof.
    UnprovenNonexecution,
}
impl FleetMaintenancePolicyStatus {
    /// Whether current policy or independently joined original nonexecution is checked.
    #[must_use]
    pub const fn is_checked(self) -> bool {
        matches!(
            self,
            Self::Reader(_)
                | Self::Follower(_)
                | Self::Nonexecution(_)
                | Self::SourceReader(_)
                | Self::RecoveredFollower(_)
        )
    }
}

/// Full original acceptance and current progress, without timestamp substitution.
pub struct FleetMaintenancePolicyObligation {
    original: Option<EnrollmentRecord>,
    current: EnrollmentRecord,
    status: FleetMaintenancePolicyStatus,
}
impl FleetMaintenancePolicyObligation {
    /// Immutable first-evacuation acceptance, including original Pending history.
    #[must_use]
    pub fn original(&self) -> Option<&EnrollmentRecord> {
        self.original.as_ref()
    }
    /// Complete current row at the exact checked roster barrier.
    #[must_use]
    pub fn current(&self) -> &EnrollmentRecord {
        &self.current
    }
    /// Checked policy identity or the explicit remaining blocker.
    #[must_use]
    pub const fn status(&self) -> FleetMaintenancePolicyStatus {
        self.status
    }
}

/// Bounded advisory counts at their original full journal/capture barrier.
/// These values are progress reporting, never a settlement/finalization proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetMaintenancePolicyProgress {
    /// Full head revision against which request matching was checked.
    pub head_revision: u64,
    /// Complete registry barrier, including original terminal rows.
    pub registry: RegistryVersion,
    /// Original union of fresh capture intervals.
    pub interval: (i64, i64),
    /// Every original/current required request.
    pub required: usize,
    /// Requests with current policy or independently checked original nonexecution.
    pub checked: usize,
    /// Requests independently confirmed never to have installed a native role.
    pub nonexecution: usize,
    /// Failed-owner follower requests closed by canonical recovery and process proof.
    pub recovered_followers: usize,
    /// Unknown accepted native outcomes.
    pub pending: usize,
    /// Installed roles awaiting checked retirement/replacement.
    pub established: usize,
    /// Closed donor roles without current policy evidence.
    pub missing_policy: usize,
    /// Original source-side/failed-owner succession still needing policy evidence.
    pub source_successors: usize,
    /// Original unknown requests with no checked nonexecution/policy proof.
    pub unproven_nonexecution: usize,
}

/// Every original acceptance plus every current unresolved related role.
///
/// This local value proves that a supplied subset cannot masquerade as complete
/// request-policy coverage. It does not discover unjournaled native roles, join
/// installed-role accepted work, prove failed-owner succession or grant settlement/finalization.
/// Applications account bounded copies of at most 10,000 retained requests.
pub struct FleetMaintenancePolicyCoverage {
    snapshot: FleetJournalSnapshot,
    roster: Digest,
    inputs: Digest,
    interval: (i64, i64),
    obligations: Vec<FleetMaintenancePolicyObligation>,
    digest: Digest,
}

/// Checked supplemental evidence retained by one maintenance-policy matcher.
pub(super) struct FleetMaintenancePolicyEvidence<'a> {
    pub(super) nonexecution: Option<&'a FleetMaintenanceNonexecution>,
    pub(super) source_readers: Option<&'a FleetSourceReaderPolicies>,
    pub(super) failed_boots: &'a [FleetFailedBootClosure],
    pub(super) recovered_followers: &'a [FleetRecoveredFollowerClosure],
}

impl FleetMaintenancePolicyCoverage {
    pub(in crate::fleet) fn check(
        original: &FleetMaintenanceEnrollments,
        roster: &FleetRoster,
        readers: &[FleetReaderEvacuationCheck],
        followers: &[FleetFollowerEvacuationCheck],
        evidence: FleetMaintenancePolicyEvidence<'_>,
        inputs: Digest,
    ) -> Result<Self> {
        let FleetMaintenancePolicyEvidence {
            nonexecution,
            source_readers,
            failed_boots,
            recovered_followers,
        } = evidence;
        let requests = requests::required(original, roster)?;
        let node = original.original().operation().node();
        let mut witnesses = BTreeMap::new();
        if let Some(checked) = nonexecution {
            if checked.snapshot() != roster.snapshot()
                || checked.roster_digest() != roster.digest()?
                || checked.original_digest() != original.digest()?
            {
                return Err(Error::Fenced);
            }
            for (request, evidence) in checked.checks() {
                witness_optional(
                    &mut witnesses,
                    request.terminal(),
                    None,
                    FleetMaintenancePolicyStatus::Nonexecution(evidence.request_digest()),
                )?;
            }
        }
        if let Some(checked) = source_readers {
            if checked.snapshot() != roster.snapshot()
                || checked.roster_digest() != roster.digest()?
                || checked.original_digest() != original.digest()?
            {
                return Err(Error::Fenced);
            }
            let proof = checked.digest()?;
            for check in checked.checks() {
                witness_optional(
                    &mut witnesses,
                    check.retirement().retired(),
                    Some(enrollment_digest(check.retirement().original())?),
                    FleetMaintenancePolicyStatus::SourceReader(proof),
                )?;
            }
        }
        for check in readers {
            witness_optional(
                &mut witnesses,
                check.record().retired(),
                Some(check.record().original_digest()),
                FleetMaintenancePolicyStatus::Reader(check.record_digest()),
            )?;
        }
        for check in followers {
            for row in check.record().retired() {
                // Only the donor's digest is the original-key witness. Sibling
                // ensemble rows remain checked but cannot substitute that donor.
                let digest = (row.spec().key().map_err(operation)?
                    == check.record().original_key())
                .then_some(check.record().original_digest());
                witness_optional(
                    &mut witnesses,
                    row,
                    digest,
                    FleetMaintenancePolicyStatus::Follower(check.record_digest()),
                )?;
            }
        }
        for closure in recovered_followers {
            if closure.leader_node() != node {
                continue;
            }
            if closure.snapshot() != roster.snapshot() {
                return Err(Error::Fenced);
            }
            let leader_session = closure.retired().session();
            let Some(boot) = failed_boots.iter().find(|proof| {
                let target = proof.boot().spec().target;
                target.node == node && target.session == leader_session
            }) else {
                // Retired log authority does not prove the old process stopped.
                // Keep its source-side requests in SourceSuccessor until joined.
                continue;
            };
            if boot.snapshot() != roster.snapshot()
                || boot.boot().status() != EnrollmentStatus::Retired
                || boot.canonical().node() != node
                || boot.canonical().session() != leader_session
                || boot.canonical().log() != Some(closure.retired().log())
            {
                return Err(Error::Fenced);
            }
            let mut proof_hash = blake3::Hasher::new();
            proof_hash.update(b"cellule.fleet-maintenance-recovered-follower.v1\0");
            proof_hash.update(closure.digest().as_bytes());
            proof_hash.update(boot.digest().as_bytes());
            let proof = Digest::from_bytes(*proof_hash.finalize().as_bytes());
            for row in closure.members() {
                let source = row.spec().source.ok_or(Error::Fenced)?;
                if source.node != node || source.session != leader_session {
                    return Err(Error::Fenced);
                }
                let key = row.spec().key().map_err(operation)?;
                if !roster.enrollments().iter().any(|current| {
                    current
                        .spec()
                        .key()
                        .is_ok_and(|current_key| current_key == key)
                        && current == row
                }) {
                    return Err(Error::Fenced);
                }
                let accepted = original.entries().find(|accepted| {
                    accepted
                        .spec()
                        .key()
                        .is_ok_and(|accepted_key| accepted_key == key)
                });
                let accepted_digest = accepted
                    .filter(|accepted| accepted.status() == EnrollmentStatus::Established)
                    .map(enrollment_digest)
                    .transpose()?;
                requests::validate_original(accepted, accepted_digest)?;
                witness_optional(
                    &mut witnesses,
                    row,
                    accepted_digest,
                    FleetMaintenancePolicyStatus::RecoveredFollower(proof),
                )?;
            }
        }
        let mut obligations = Vec::with_capacity(requests.len());
        for request in requests {
            let accepted = request.original;
            let row = request.current;
            let key = *row.spec().key().map_err(operation)?.as_bytes();
            let status = match row.status() {
                EnrollmentStatus::Pending => FleetMaintenancePolicyStatus::Pending,
                EnrollmentStatus::Established => FleetMaintenancePolicyStatus::Established,
                _ if let Some((terminal, _, FleetMaintenancePolicyStatus::Nonexecution(proof))) =
                    witnesses.get(&key) =>
                {
                    if row != *terminal
                        || row.established_evidence().is_some()
                        || accepted.is_some_and(|row| row.established_evidence().is_some())
                    {
                        return Err(Error::Fenced);
                    }
                    FleetMaintenancePolicyStatus::Nonexecution(*proof)
                }
                _ if row.spec().target.node != node => match witnesses.get(&key) {
                    Some((retired, digest, FleetMaintenancePolicyStatus::SourceReader(proof))) => {
                        if row != *retired {
                            return Err(Error::Fenced);
                        }
                        requests::validate_original(accepted, *digest)?;
                        FleetMaintenancePolicyStatus::SourceReader(*proof)
                    }
                    Some((
                        retired,
                        digest,
                        status @ FleetMaintenancePolicyStatus::RecoveredFollower(_),
                    )) => {
                        if row != *retired {
                            return Err(Error::Fenced);
                        }
                        requests::validate_original(accepted, *digest)?;
                        *status
                    }
                    _ => FleetMaintenancePolicyStatus::SourceSuccessor,
                },
                _ if row.established_evidence().is_none() => {
                    FleetMaintenancePolicyStatus::UnprovenNonexecution
                }
                _ => match witnesses.get(&key) {
                    Some((retired, digest, status)) => {
                        if row != *retired {
                            return Err(Error::Fenced);
                        }
                        requests::validate_original(accepted, *digest)?;
                        *status
                    }
                    None => FleetMaintenancePolicyStatus::MissingPolicy,
                },
            };
            obligations.push(FleetMaintenancePolicyObligation {
                original: accepted.cloned(),
                current: row.clone(),
                status,
            });
        }
        let interval = readers
            .iter()
            .map(FleetReaderEvacuationCheck::interval)
            .chain(followers.iter().map(FleetFollowerEvacuationCheck::interval))
            .chain(nonexecution.map(FleetMaintenanceNonexecution::interval))
            .chain(source_readers.map(FleetSourceReaderPolicies::interval))
            .fold(original.interval(), |interval, next| {
                (interval.0.min(next.0), interval.1.max(next.1))
            });
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-maintenance-policy-coverage.v3\0");
        hash.update(inputs.as_bytes());
        hash.update(&(obligations.len() as u64).to_be_bytes());
        for obligation in &obligations {
            hash.update(&[u8::from(obligation.original.is_some())]);
            if let Some(row) = &obligation.original {
                hash.update(enrollment_digest(row)?.as_bytes());
            }
            hash.update(enrollment_digest(&obligation.current)?.as_bytes());
            let (tag, proof) = match obligation.status {
                FleetMaintenancePolicyStatus::Reader(proof) => (1, Some(proof)),
                FleetMaintenancePolicyStatus::Follower(proof) => (2, Some(proof)),
                FleetMaintenancePolicyStatus::Pending => (3, None),
                FleetMaintenancePolicyStatus::Established => (4, None),
                FleetMaintenancePolicyStatus::MissingPolicy => (5, None),
                FleetMaintenancePolicyStatus::SourceSuccessor => (6, None),
                FleetMaintenancePolicyStatus::UnprovenNonexecution => (7, None),
                FleetMaintenancePolicyStatus::Nonexecution(proof) => (8, Some(proof)),
                FleetMaintenancePolicyStatus::SourceReader(proof) => (9, Some(proof)),
                FleetMaintenancePolicyStatus::RecoveredFollower(proof) => (10, Some(proof)),
            };
            hash.update(&[tag]);
            if let Some(proof) = proof {
                hash.update(proof.as_bytes());
            }
        }
        Ok(Self {
            snapshot: roster.snapshot().clone(),
            roster: roster.digest()?,
            inputs,
            interval,
            obligations,
            digest: Digest::from_bytes(*hash.finalize().as_bytes()),
        })
    }
    /// Exact full snapshot whose current rows were matched.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Every required original/current request, in canonical request-key order.
    #[must_use]
    pub fn obligations(&self) -> &[FleetMaintenancePolicyObligation] {
        &self.obligations
    }
    /// All requests have checked policy or nonexecution; other node barriers remain.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.obligations.iter().all(|row| row.status.is_checked())
    }
    /// Original union of fresh capture intervals, never restamped.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        self.interval
    }
    /// Complete original roster identity used for request matching.
    #[must_use]
    pub const fn roster_digest(&self) -> Digest {
        self.roster
    }
    /// Operator progress retains its own barrier even if a later pass allocates work.
    #[must_use]
    pub fn progress(&self) -> FleetMaintenancePolicyProgress {
        let mut progress = FleetMaintenancePolicyProgress {
            head_revision: self.snapshot.head().revision(),
            registry: self.snapshot.registry(),
            interval: self.interval,
            required: self.obligations.len(),
            checked: 0,
            nonexecution: 0,
            recovered_followers: 0,
            pending: 0,
            established: 0,
            missing_policy: 0,
            source_successors: 0,
            unproven_nonexecution: 0,
        };
        for obligation in &self.obligations {
            match obligation.status {
                FleetMaintenancePolicyStatus::Reader(_)
                | FleetMaintenancePolicyStatus::Follower(_)
                | FleetMaintenancePolicyStatus::SourceReader(_) => progress.checked += 1,
                FleetMaintenancePolicyStatus::RecoveredFollower(_) => {
                    progress.checked += 1;
                    progress.recovered_followers += 1;
                }
                FleetMaintenancePolicyStatus::Nonexecution(_) => {
                    progress.checked += 1;
                    progress.nonexecution += 1;
                }
                FleetMaintenancePolicyStatus::Pending => progress.pending += 1,
                FleetMaintenancePolicyStatus::Established => progress.established += 1,
                FleetMaintenancePolicyStatus::MissingPolicy => progress.missing_policy += 1,
                FleetMaintenancePolicyStatus::SourceSuccessor => progress.source_successors += 1,
                FleetMaintenancePolicyStatus::UnprovenNonexecution => {
                    progress.unproven_nonexecution += 1
                }
            }
        }
        progress
    }
    /// Canonical required rows and explicit checked/blocking statuses.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    pub(in crate::fleet) const fn inputs(&self) -> Digest {
        self.inputs
    }
}

type Witnesses<'a> = BTreeMap<
    [u8; 32],
    (
        &'a EnrollmentRecord,
        Option<Digest>,
        FleetMaintenancePolicyStatus,
    ),
>;
fn witness_optional<'a>(
    rows: &mut Witnesses<'a>,
    row: &'a EnrollmentRecord,
    digest: Option<Digest>,
    status: FleetMaintenancePolicyStatus,
) -> Result<()> {
    if rows.len() >= 10_000 {
        return Err(Error::Capacity("maintenance policy witness bound exceeded"));
    }
    if rows
        .insert(
            *row.spec().key().map_err(operation)?.as_bytes(),
            (row, digest, status),
        )
        .is_some()
    {
        return Err(Error::Node("maintenance policy obligation is duplicated"));
    }
    Ok(())
}

fn enrollment_digest(row: &EnrollmentRecord) -> Result<Digest> {
    Ok(Digest::from_bytes(
        *blake3::hash(&row.to_bytes().map_err(operation)?).as_bytes(),
    ))
}
