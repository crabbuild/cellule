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
    advertisement_with_receive_capacity(index, node, intent, false).await
}

#[cfg(test)]
pub(super) async fn advertisement_with_read_capacity(
    index: usize,
    node: &CellNode,
    intent: &NodeIntent,
) -> JournalResult<NodeAdvertisement> {
    advertisement_with_receive_capacity(index, node, intent, true).await
}

async fn advertisement_with_receive_capacity(
    index: usize,
    node: &CellNode,
    intent: &NodeIntent,
    receive_capacity: bool,
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
    let memory_capacity =
        u64::try_from(stats.resident_capacity_bytes() + stats.retained_capacity_bytes())?;
    let memory_used = u64::try_from(stats.resident_bytes() + stats.retained_bytes())?;
    let disk_free = stats
        .local_disk_capacity_bytes()
        .saturating_sub(stats.local_disk_reserved_bytes());
    let job_capacity = stats.placement_job_capacity();
    let jobs_running = stats.placement_running_jobs();
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
            free_memory_bytes: if receive_capacity {
                memory_capacity.saturating_sub(memory_used)
            } else {
                0
            },
            free_disk_bytes: if receive_capacity { disk_free } else { 0 },
            job_credits: if receive_capacity {
                job_capacity.saturating_sub(jobs_running)
            } else {
                0
            },
            log_protocol: 1,
            ..Default::default()
        },
    )?
    .with_operational_placement(
        cellule_runtime::node::NodePlacementCapacity {
            memory_capacity_bytes: memory_capacity,
            disk_capacity_bytes: stats.local_disk_capacity_bytes(),
            active_cells: stats.placement_active_cells(),
            max_active_cells: stats.placement_active_cell_capacity(),
            running_jobs: jobs_running,
            job_capacity,
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
#[derive(Clone)]
pub(super) struct BootOwner {
    pub(super) node: Arc<CellNode>,
    pub(super) directory: NodeDirectory,
    pub(super) spec: EnrollmentSpec,
    pub(super) advertisement: NodeAdvertisement,
    pub(super) guard: Option<NodeLeaseGuard>,
}

impl BootOwner {
    /// Refresh only the retained canonical boot. No missing/expired record can
    /// authorize recreation, and local lease credit advances only after CAS.
    pub(super) async fn refresh_capacity(
        &self,
        index: usize,
        journal: &dyn FleetEnrollmentJournal,
        deadline: Instant,
    ) -> JournalResult<NodeAdvertisement> {
        if !self.node.is_management_ready() {
            return Err(invalid("example capacity node is not management ready"));
        }
        let guard = self
            .guard
            .as_ref()
            .ok_or_else(|| invalid("example boot lease guard is unbound"))?;
        guard.check()?;
        // A live boot must consume retained maintenance intent before another
        // membership/lease renewal. Lost Cordon RPCs cannot keep its gate open.
        self.node
            .refresh_fleet_intent(journal, deadline.into_std())
            .await?;
        let now = clock()?;
        let observed = self
            .directory
            .load(self.spec.target.session, now)
            .await?
            .ok_or_else(|| invalid("example capacity boot is missing or expired"))?;
        self.validate_successor(observed.advertisement())?;
        let previous = observed.advertisement().operational_sample();
        // A new capacity block cannot reuse an earlier classifier sequence.
        // Wait for its real sample; neither sequence nor time is fabricated.
        let sample = tokio::time::timeout_at(deadline, async {
            loop {
                if let Some(sample) = self.node.runtime().operational_sample()?
                    && previous.is_none_or(|old| sample.sequence > old.sequence)
                {
                    return Ok::<_, cellule_runtime::Error>(sample);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let stats = self.node.stats();
        let memory =
            u64::try_from(stats.resident_capacity_bytes() + stats.retained_capacity_bytes())?;
        let used = u64::try_from(stats.resident_bytes() + stats.retained_bytes())?;
        let follower = self
            .node
            .try_owned_component::<cellule_runtime::follower::FollowerStore>(
                cellule_host::FOLLOWER_STORE_COMPONENT,
            )?;
        let original = &self.advertisement;
        let key = SigningKey::from_bytes(&[index as u8 + 1; 32]);
        let now = clock()?;
        let next = NodeAdvertisement::sign(
            original.node(),
            original.session(),
            original.endpoint().to_owned(),
            original.fleet(),
            original.certificate(),
            original.image(),
            original.release(),
            &key,
            1,
            now,
            now.checked_add(30_000)
                .ok_or_else(|| invalid("example heartbeat deadline overflow"))?,
            original.module_digests().to_vec(),
            original.peer_versions().to_vec(),
            original.failure_domain().clone(),
            NodeCapacity {
                follower_free_bytes: follower
                    .as_ref()
                    .map_or(0, |store| store.available_bytes())
                    .min(
                        stats
                            .local_disk_capacity_bytes()
                            .saturating_sub(stats.local_disk_reserved_bytes()),
                    ),
                follower_retained_bytes: follower
                    .as_ref()
                    .map_or(0, |store| store.retained_bytes()),
                free_memory_bytes: memory.saturating_sub(used),
                free_disk_bytes: stats
                    .local_disk_capacity_bytes()
                    .saturating_sub(stats.local_disk_reserved_bytes()),
                job_credits: stats
                    .placement_job_capacity()
                    .saturating_sub(stats.placement_running_jobs()),
                log_protocol: 1,
            },
        )?
        .with_operational_placement(
            cellule_runtime::node::NodePlacementCapacity {
                memory_capacity_bytes: memory,
                disk_capacity_bytes: stats.local_disk_capacity_bytes(),
                active_cells: stats.placement_active_cells(),
                max_active_cells: stats.placement_active_cell_capacity(),
                running_jobs: stats.placement_running_jobs(),
                job_capacity: stats.placement_job_capacity(),
                ..Default::default()
            },
            sample,
            &key,
        )?;
        guard.check()?;
        let refreshed = self.directory.refresh(&observed, next, now).await?;
        self.validate_successor(refreshed.advertisement())?;
        guard.renew(clock()?, refreshed.advertisement().expires_at_ms())?;
        Ok(refreshed.advertisement().clone())
    }

    fn validate_successor(&self, ad: &NodeAdvertisement) -> JournalResult<()> {
        let original = &self.advertisement;
        // Pin the original enrolled signing key and executable identity; a
        // self-valid signature on an unrelated advertisement is insufficient.
        if ad.node() != original.node()
            || ad.session() != original.session()
            || ad.endpoint() != original.endpoint()
            || ad.fleet() != original.fleet()
            || ad.certificate() != original.certificate()
            || ad.image() != original.image()
            || ad.release() != original.release()
            || ad.verifying_key()? != original.verifying_key()?
            || ad.module_digests() != original.module_digests()
            || ad.peer_versions() != original.peer_versions()
            || ad.failure_domain() != original.failure_domain()
            || ad.generation() < original.generation()
            || ad.issued_at_ms() < original.issued_at_ms()
            || ad.expires_at_ms() < original.expires_at_ms()
        {
            return Err(invalid("example retained boot identity differs"));
        }
        Ok(())
    }

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
        if !self
            .directory
            .is_withdrawn(self.spec.target.session)
            .await?
        {
            let now = clock()?;
            let observed = self
                .directory
                .load(self.spec.target.session, now)
                .await?
                .ok_or_else(|| invalid("example boot withdrawal remains unresolved"))?;
            self.validate_successor(observed.advertisement())?;
            self.directory.withdraw_after_drain(&observed, now).await?;
        }
        if !self
            .directory
            .is_withdrawn(self.spec.target.session)
            .await?
        {
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
