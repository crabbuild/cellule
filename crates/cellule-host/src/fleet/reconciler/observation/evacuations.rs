//! Fresh replacement policy checks retained through the complete planner barrier.
use super::*;
use crate::durability::enrollment::maintenance::boot_identity as follower_boot_identity;
use crate::fleet::{FleetFollowerEvacuationCheck, FleetReaderEvacuationCheck};
use crate::read_replicas::maintenance::boot_identity as reader_boot_identity;
use cellule_runtime::fleet::operations::EnrollmentRecord;
use cellule_runtime::node::log_state::NodeLogPhase;

pub(super) struct RoleEvacuations {
    readers: Vec<FleetReaderEvacuationCheck>,
    followers: Vec<FleetFollowerEvacuationCheck>,
}

impl FleetObservation {
    /// Retains fresh confirmations of durable reader/follower replacement history.
    /// Every check must share the exact full head and roster with the other
    /// retained evidence, with its fresh interval inside this observation.
    /// Signed replacement boots must
    /// match their native checks. Duplicate original obligations refuse.
    ///
    /// This starts no effects and never upgrades `complete`. A supplied subset
    /// does not prove complete role settlement, original accepted-work joining,
    /// failed-owner recovery or permission to finalize maintenance.
    pub fn with_role_evacuations(
        mut self,
        readers: Vec<FleetReaderEvacuationCheck>,
        followers: Vec<FleetFollowerEvacuationCheck>,
    ) -> Result<Self> {
        if self.role_evacuations.is_some() {
            return Err(Error::Control("fleet role evacuations already retained"));
        }
        self.role_evacuations = Some(RoleEvacuations { readers, followers });
        self.validate_role_coverage()?;
        Ok(self)
    }

    /// Original reader confirmations, including current ready replacement prefixes.
    #[must_use]
    pub fn reader_evacuations(&self) -> Option<&[FleetReaderEvacuationCheck]> {
        self.role_evacuations
            .as_ref()
            .map(|roles| roles.readers.as_slice())
    }

    /// Original live-owner follower confirmations and their retained native graph.
    #[must_use]
    pub fn follower_evacuations(&self) -> Option<&[FleetFollowerEvacuationCheck]> {
        self.role_evacuations
            .as_ref()
            .map(|roles| roles.followers.as_slice())
    }

    pub(super) fn validate_role_evacuations(&self) -> Result<()> {
        let Some(roles) = &self.role_evacuations else {
            return Ok(());
        };
        if roles.readers.len().saturating_add(roles.followers.len()) > 10_000 {
            return Err(Error::Capacity("fleet role evacuation bound exceeded"));
        }
        let first = roles
            .readers
            .first()
            .map(|check| (check.snapshot(), check.roster_digest()))
            .or_else(|| {
                roles
                    .followers
                    .first()
                    .map(|check| (check.snapshot(), check.roster_digest()))
            });
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
                self.recovered_follower_closures
                    .as_ref()
                    .and_then(|proofs| proofs.first())
                    .map(FleetRecoveredFollowerClosure::snapshot)
            })
            .or_else(|| first.map(|(snapshot, _)| snapshot));
        let roster = match &self.roster {
            Some(roster) => Some(roster.digest()?),
            None => self
                .role_coverage
                .as_ref()
                .map(FleetRoleCoverage::roster_digest)
                .or_else(|| first.map(|(_, digest)| digest)),
        };
        let mut obligations = HashSet::new();
        for check in &roles.readers {
            self.role_barrier(
                check.snapshot(),
                check.roster_digest(),
                check.interval(),
                expected,
                roster,
            )?;
            self.role_row(check.record().retired(), &mut obligations)?;
            for replacement in check.replacements() {
                self.role_boot(
                    replacement.node,
                    replacement.session,
                    replacement.boot_identity,
                    reader_boot_identity,
                )?;
            }
            if let Some(owned) = self
                .cells
                .iter()
                .find(|owned| owned.observation.target.cell_id() == check.authority().cell)
            {
                let row = &owned.observation;
                let authority = check.authority();
                if row.incarnation != authority.incarnation
                    || row.code != authority.code
                    || row.schema != authority.schema
                    || authority
                        .owner
                        .as_ref()
                        .is_none_or(|owner| owner.session != owned.session)
                    || row.position.as_ref().is_none_or(|position| {
                        position.epoch != authority.epoch
                            || Some(&position.root) != authority.root.as_ref()
                    })
                {
                    return Err(Error::Node("reader evacuation current writer differs"));
                }
            }
        }
        for check in &roles.followers {
            self.role_barrier(
                check.snapshot(),
                check.roster_digest(),
                check.interval(),
                expected,
                roster,
            )?;
            for retired in check.record().retired() {
                self.role_row(retired, &mut obligations)?;
            }
            let source = check.record().source().map_err(super::super::operation)?;
            let leader = self.role_boot(
                source.node,
                source.session,
                check.record().source_boot(),
                follower_boot_identity,
            )?;
            let original = check.authority();
            if leader.generation() < original.generation()
                || leader.issued_at_ms() < original.issued_at_ms()
                || leader
                    .log()
                    .zip(original.log())
                    .is_none_or(|(current, checked)| {
                        current.phase() != NodeLogPhase::Open
                            || current.epoch() != checked.epoch()
                            || current.members() != checked.members()
                            || current.tiered_through() < checked.tiered_through()
                    })
            {
                return Err(Error::Node("follower evacuation current authority differs"));
            }
            for replacement in check.record().replacements() {
                let endpoint = replacement.enrollment.spec().target;
                self.role_boot(
                    endpoint.node,
                    endpoint.session,
                    replacement.boot_identity,
                    follower_boot_identity,
                )?;
            }
        }
        Ok(())
    }

    fn role_barrier(
        &self,
        snapshot: &crate::fleet::FleetJournalSnapshot,
        digest: Digest,
        interval: (i64, i64),
        expected: Option<&crate::fleet::FleetJournalSnapshot>,
        roster: Option<Digest>,
    ) -> Result<()> {
        if snapshot.head().scope() != self.scope
            || snapshot.registry() != self.registry
            || expected != Some(snapshot)
            || roster != Some(digest)
            || interval.0 < self.capture_started_at_ms
            || interval.1 > self.capture_finished_at_ms
        {
            return Err(Error::Node("fleet role evacuation barrier differs"));
        }
        Ok(())
    }

    fn role_row(&self, row: &EnrollmentRecord, obligations: &mut HashSet<Digest>) -> Result<()> {
        if !obligations.insert(row.spec().key().map_err(super::super::operation)?) {
            return Err(Error::Node(
                "fleet role evacuation obligation is duplicated",
            ));
        }
        if self
            .roster
            .as_ref()
            .is_some_and(|roster| !roster.enrollments().iter().any(|current| current == row))
        {
            return Err(Error::Node("fleet role evacuation roster differs"));
        }
        Ok(())
    }

    pub(super) fn role_boot(
        &self,
        node: NodeId,
        session: SessionId,
        identity: Digest,
        digest: fn(&NodeAdvertisement) -> Result<Digest>,
    ) -> Result<&NodeAdvertisement> {
        let signed = self
            .nodes
            .iter()
            .find(|signed| signed.node() == node && signed.session() == session)
            .ok_or(Error::Node("fleet role evacuation boot is missing"))?;
        if digest(signed)? != identity || !signed.accepts_new_roles(self.capture_finished_at_ms) {
            return Err(Error::Node("fleet role evacuation boot differs"));
        }
        Ok(signed)
    }

    pub(super) fn hash_role_evacuations(&self, hash: &mut blake3::Hasher) -> Result<()> {
        hash.update(&[u8::from(self.role_evacuations.is_some())]);
        let Some(roles) = &self.role_evacuations else {
            return Ok(());
        };
        let mut readers = roles.readers.iter().collect::<Vec<_>>();
        readers.sort_by_key(|check| *check.record_digest().as_bytes());
        hash.update(&(readers.len() as u64).to_be_bytes());
        for check in readers {
            hash_check(
                hash,
                check.snapshot(),
                check.record_digest(),
                check.roster_digest(),
                check.interval(),
            )?;
            hash_bytes(hash, &check.authority().encode()?);
            let mut replacements = check.replacements().iter().collect::<Vec<_>>();
            replacements.sort_by_key(|replacement| {
                (
                    *replacement.node.as_bytes(),
                    *replacement.session.as_bytes(),
                )
            });
            hash.update(&(replacements.len() as u64).to_be_bytes());
            for replacement in replacements {
                hash.update(replacement.node.as_bytes());
                hash.update(replacement.session.as_bytes());
                for digest in [
                    replacement.boot_identity,
                    replacement.enrollment_key,
                    replacement.enrollment_digest,
                ] {
                    hash.update(digest.as_bytes());
                }
                hash.update(replacement.receipt.cell.as_bytes());
                hash.update(replacement.receipt.incarnation.as_bytes());
                hash.update(&replacement.receipt.commit_sequence.to_be_bytes());
            }
        }
        let mut followers = roles.followers.iter().collect::<Vec<_>>();
        followers.sort_by_key(|check| *check.record_digest().as_bytes());
        hash.update(&(followers.len() as u64).to_be_bytes());
        for check in followers {
            hash_check(
                hash,
                check.snapshot(),
                check.record_digest(),
                check.roster_digest(),
                check.interval(),
            )?;
            let authority = check.authority();
            hash.update(follower_boot_identity(authority)?.as_bytes());
            for value in [
                authority.generation(),
                authority.issued_at_ms() as u64,
                authority.progress(),
            ] {
                hash.update(&value.to_be_bytes());
            }
            let log = authority.log().ok_or(Error::Fenced)?;
            hash.update(&log.tiered_through().to_be_bytes());
            hash.update(&[u8::from(log.active())]);
            let mut native = check.native().iter().collect::<Vec<_>>();
            native.sort_by_key(|inventory| {
                (
                    *inventory.node().as_bytes(),
                    *inventory.session().as_bytes(),
                )
            });
            hash.update(&(native.len() as u64).to_be_bytes());
            for inventory in native {
                hash.update(inventory.coverage_digest()?.as_bytes());
                let (collected, checked) = inventory.coverage_checkpoint();
                let (start, finish) = checked.ok_or(Error::Fenced)?;
                for value in [inventory.interval().0, collected, start, finish] {
                    hash.update(&value.to_be_bytes());
                }
            }
        }
        Ok(())
    }
}

fn hash_check(
    hash: &mut blake3::Hasher,
    snapshot: &crate::fleet::FleetJournalSnapshot,
    record: Digest,
    roster: Digest,
    interval: (i64, i64),
) -> Result<()> {
    for digest in [record, roster] {
        hash.update(digest.as_bytes());
    }
    hash_bytes(
        hash,
        &snapshot
            .head()
            .to_bytes()
            .map_err(super::super::operation)?,
    );
    hash_bytes(
        hash,
        &snapshot
            .registry()
            .to_bytes()
            .map_err(super::super::operation)?,
    );
    for value in [interval.0, interval.1] {
        hash.update(&value.to_be_bytes());
    }
    Ok(())
}
fn hash_bytes(hash: &mut blake3::Hasher, bytes: &[u8]) {
    hash.update(&(bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}
