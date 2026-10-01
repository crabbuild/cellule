//! Application-owned boot producer: Pending precedes canonical advertisement.

use super::*;
use cellule_host::fleet::FleetEnrollmentAcceptance;
use cellule_runtime::fleet::operations::{
    EnrollmentEndpoint, EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentSpec,
    EnrollmentStatus,
};
use cellule_runtime::node::{NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain};
use ed25519_dalek::SigningKey;

pub(super) fn spec(intent: &NodeIntent) -> JournalResult<EnrollmentSpec> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.example-fleet-boot-request.v1\0");
    hash.update(intent.scope().fleet.as_bytes());
    hash.update(intent.scope().application.as_bytes());
    hash.update(intent.node().as_bytes());
    hash.update(intent.session().as_bytes());
    Ok(EnrollmentSpec {
        scope: intent.scope(),
        request: Digest::from_bytes(*hash.finalize().as_bytes()),
        role: EnrollmentRole::Node {
            mode: intent.mode(),
        },
        source: None,
        target: EnrollmentEndpoint {
            node: intent.node(),
            session: intent.session(),
            intent_revision: intent.revision(),
        },
    })
}

pub(super) async fn advertisement(
    index: usize,
    node: &CellNode,
    intent: &NodeIntent,
) -> JournalResult<NodeAdvertisement> {
    // Boot discovery publishes no receive capacity before readiness. The
    // observer separately captures real signed capacity after startup.
    let sample = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(sample) = node.runtime().operational_sample()? {
                return Ok::<_, cellule_runtime::Error>(sample);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let now = clock()?;
    let stats = node.stats();
    let key = SigningKey::from_bytes(&[index as u8 + 1; 32]);
    Ok(NodeAdvertisement::sign(
        intent.node(),
        intent.session(),
        owner(index).endpoint,
        intent.scope().fleet,
        Digest::from_bytes([30; 32]),
        Digest::from_bytes([31; 32]),
        node.application().registry().release_digest(),
        &key,
        1,
        now,
        now + 30_000,
        node.application().registry().module_digests(),
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            log_protocol: 1,
            ..NodeCapacity::default()
        },
    )?
    .with_operational_placement(
        cellule_runtime::node::NodePlacementCapacity {
            memory_capacity_bytes: u64::try_from(
                stats.resident_capacity_bytes() + stats.retained_capacity_bytes(),
            )?,
            disk_capacity_bytes: stats.local_disk_capacity_bytes(),
            active_cells: stats.placement_active_cells(),
            max_active_cells: stats.placement_active_cell_capacity(),
            running_jobs: stats.placement_running_jobs(),
            job_capacity: stats.placement_job_capacity(),
            ..Default::default()
        },
        sample,
        &key,
    )?)
}

fn evidence(spec: &EnrollmentSpec, ad: &NodeAdvertisement) -> JournalResult<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.example-fleet-boot-evidence.v1\0");
    hash.update(&spec.to_bytes()?);
    hash.update(ad.node().as_bytes());
    hash.update(ad.session().as_bytes());
    hash.update(ad.fleet().as_bytes());
    hash.update(ad.certificate().as_bytes());
    hash.update(ad.image().as_bytes());
    hash.update(ad.release().as_bytes());
    hash.update(&ad.verifying_key()?.to_bytes());
    hash.update(&(ad.endpoint().len() as u64).to_be_bytes());
    hash.update(ad.endpoint().as_bytes());
    hash.update(&ad.generation().to_be_bytes());
    hash.update(&ad.issued_at_ms().to_be_bytes());
    hash.update(&ad.expires_at_ms().to_be_bytes());
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}

pub(super) async fn enroll(
    journal: &SqliteJournal,
    directory: &NodeDirectory,
    spec: &EnrollmentSpec,
    ad: NodeAdvertisement,
    now: i64,
) -> JournalResult<EnrollmentRecord> {
    if ad.node() != spec.target.node
        || ad.session() != spec.target.session
        || ad.fleet() != spec.scope.fleet
        || !matches!(spec.role, EnrollmentRole::Node { .. })
        || spec.source.is_some()
    {
        return Err(invalid("example boot advertisement binding differs"));
    }
    let (record, observed) = match journal.accept_enrollment(spec, now).await? {
        FleetEnrollmentAcceptance::New(record) => {
            (record, directory.create(ad.clone(), now).await?)
        }
        FleetEnrollmentAcceptance::Existing(record) => {
            if !matches!(
                record.status(),
                EnrollmentStatus::Pending | EnrollmentStatus::Established
            ) {
                return Err(invalid("example boot enrollment is already settled"));
            }
            // Existing Pending cannot authorize another unobserved side effect.
            // Inspect the canonical advertisement; absence retains the blocker.
            let observed = directory
                .load(ad.session(), now)
                .await?
                .ok_or_else(|| invalid("example pending boot has no proven advertisement"))?;
            (record, observed)
        }
    };
    if observed.advertisement() != &ad {
        return Err(invalid("example original boot advertisement changed"));
    }
    journal
        .publish_enrollment_result(
            &record,
            EnrollmentEvent::Established(evidence(spec, observed.advertisement())?),
            now,
        )
        .await
}

#[cfg(test)]
mod tests;

/// Application retains the boot scope through joined runtime shutdown and
/// canonical withdrawal. Pending or failed boots stay in the journal unless
/// that exact directory obligation can be closed.
pub(super) struct BootOwner {
    pub(super) node: Arc<CellNode>,
    pub(super) directory: NodeDirectory,
    pub(super) spec: EnrollmentSpec,
    pub(super) advertisement: NodeAdvertisement,
    pub(super) guard: Option<NodeLeaseGuard>,
}

impl BootOwner {
    pub(super) async fn withdraw(&self, journal: &SqliteJournal) -> JournalResult<()> {
        if self.node.state() != NodeState::Stopped {
            return Err(invalid("example boot runtime has not joined shutdown"));
        }
        if let Some(guard) = &self.guard {
            guard.fence();
        }
        let Some(record) = journal
            .load_enrollment(self.spec.scope, self.spec.key()?)
            .await?
        else {
            return Ok(());
        };
        record.validate_replay(&self.spec)?;
        if matches!(
            record.status(),
            EnrollmentStatus::Retired | EnrollmentStatus::Refused
        ) {
            return Ok(());
        }
        let original_evidence = evidence(&self.spec, &self.advertisement)?;
        if record
            .established_evidence()
            .is_some_and(|evidence| evidence != original_evidence)
        {
            return Err(invalid("example boot withdrawal original evidence differs"));
        }
        // A previous withdrawal can commit before its reply or journal result
        // is observed. The permanent exact-session tombstone closes that boot;
        // an absent live advertisement by itself never supplies closure.
        if !self.directory.is_retired(self.spec.target.session).await? {
            let now = clock()?;
            let observed = self
                .directory
                .load(self.spec.target.session, now)
                .await?
                .ok_or_else(|| invalid("example boot withdrawal remains unresolved"))?;
            if observed.advertisement() != &self.advertisement {
                return Err(invalid("example boot withdrawal identity differs"));
            }
            self.directory.withdraw_after_drain(&observed, now).await?;
        }
        if !self.directory.is_retired(self.spec.target.session).await? {
            return Err(invalid("example boot withdrawal lacks its tombstone"));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.example-fleet-boot-retirement.v1\0");
        hash.update(original_evidence.as_bytes());
        journal
            .publish_enrollment_result(
                &record,
                EnrollmentEvent::Retired(Digest::from_bytes(*hash.finalize().as_bytes())),
                clock()?,
            )
            .await?;
        Ok(())
    }
}
