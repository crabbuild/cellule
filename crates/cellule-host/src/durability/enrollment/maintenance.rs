//! Current managed replacement evidence after the original supervisor rotation.
use super::*;
use crate::fleet::{FleetJournalSnapshot, FleetRoster};
use cellule_runtime::fleet::operations::{MaintenanceOperation, MaintenancePhase};
use cellule_runtime::node::log_state::NodeLogPhase;
use cellule_runtime::node::{MAX_NODE_BYTES, NodeAdvertisement, NodeMode};

/// Checked evacuation of one live owner's original follower responsibility.
///
/// This is interval evidence, not permission to stop a physical node. Persist
/// and revalidate this evidence together with complete native/foreign inventory,
/// Pending producers, failed-owner recovery and all other role obligations.
/// The original rotation retains its errors independently of a successful retry.
pub struct FollowerEvacuation {
    original: EnrollmentRecord,
    retired: EnrollmentRecord,
    retired_members: Vec<EnrollmentRecord>,
    snapshot: FleetJournalSnapshot,
    rotation: Arc<NodeLogRotationCompletion>,
    replacement: NodeLogEnrollmentProof,
    replacements: Vec<EnrollmentRecord>,
    authority: NodeAdvertisement,
    minimum_members: usize,
    started_at_ms: i64,
    finished_at_ms: i64,
    _memory: NodeByteReservation,
}

impl FollowerEvacuation {
    /// Complete original Retired ensemble, preserving every member's history.
    #[must_use]
    pub fn retired_members(&self) -> &[EnrollmentRecord] {
        &self.retired_members
    }

    /// Builds immutable durable metadata under the current application policy.
    /// Copied buffers remain the embedding application's accounting obligation.
    pub fn durable_record(
        &self,
        policy: cellule_runtime::fleet::operations::FollowerReplacementPolicy,
    ) -> cellule_runtime::Result<cellule_runtime::fleet::operations::FollowerEvacuationRecord> {
        use cellule_runtime::fleet::operations::{
            FollowerEvacuationRecord, FollowerReplacementWitness,
        };
        if usize::from(policy.minimum_members()) != self.minimum_members {
            return Err(Error::Fenced);
        }
        FollowerEvacuationRecord::new(
            self.snapshot
                .head()
                .maintenance()
                .ok_or(Error::Fenced)?
                .clone(),
            (
                Digest::from_bytes(
                    *blake3::hash(&self.snapshot.head().to_bytes().map_err(super::operation)?)
                        .as_bytes(),
                ),
                self.snapshot.registry(),
            ),
            policy,
            (
                self.original.spec().key().map_err(super::operation)?,
                Digest::from_bytes(
                    *blake3::hash(&self.original.to_bytes().map_err(super::operation)?).as_bytes(),
                ),
            ),
            self.retired_members.clone(),
            self.rotation.retirement().barrier().covered_through(),
            boot_identity(&self.authority)?,
            (
                self.rotation.replacement_epoch(),
                self.replacement.evidence_digest()?,
            ),
            self.replacements
                .iter()
                .zip(self.replacement.prepared().followers())
                .map(|(enrollment, boot)| {
                    Ok(FollowerReplacementWitness {
                        enrollment: enrollment.clone(),
                        boot_identity: boot_identity(boot)?,
                    })
                })
                .collect::<cellule_runtime::Result<Vec<_>>>()?,
            self.interval(),
        )
        .map_err(super::operation)
    }
    /// Immutable Established request named by the caller, without restamping.
    #[must_use]
    pub fn original(&self) -> &EnrollmentRecord {
        &self.original
    }
    /// Committed retirement of that exact request after confirmed native closure.
    #[must_use]
    pub fn retired(&self) -> &EnrollmentRecord {
        &self.retired
    }
    /// Complete journal head and registry version rechecked after collection.
    #[must_use]
    pub fn snapshot(&self) -> &FleetJournalSnapshot {
        &self.snapshot
    }
    /// Original confirmed retirement and installed replacement, retaining its Arc.
    #[must_use]
    pub fn rotation(&self) -> &Arc<NodeLogRotationCompletion> {
        &self.rotation
    }
    /// Checked canonical replacement with its original signed source/member boots.
    /// Retain these identities when persisting and revalidating the evidence.
    #[must_use]
    pub fn replacement(&self) -> &NodeLogEnrollmentProof {
        &self.replacement
    }
    /// Every Established member of the complete replacement ensemble.
    #[must_use]
    pub fn replacements(&self) -> &[EnrollmentRecord] {
        &self.replacements
    }
    /// Current signed leader authority at the end of the checked interval.
    #[must_use]
    pub fn authority(&self) -> &NodeAdvertisement {
        &self.authority
    }
    /// Explicit application redundancy requirement checked by this capture.
    #[must_use]
    pub const fn minimum_members(&self) -> usize {
        self.minimum_members
    }
    /// Original capture interval; durable records keep their original timestamps.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        (self.started_at_ms, self.finished_at_ms)
    }
}

impl CellNode {
    /// Checks one foreign follower evacuation after an accepted epoch rotation.
    ///
    /// Authenticate the caller and request rotation through
    /// [`Self::request_node_log_rotation`] on the live owner. This method starts
    /// no effects and does not wait for recruitment: incomplete rotation returns
    /// a blocker, leaving the existing supervisor and its original errors owned.
    /// Supply the original Established follower row and current Evacuating
    /// operation. A replacement must exclude the donor, meet the explicit
    /// nonzero member minimum and retain every original managed boot/request.
    /// Dead-owner recovery and complete fleet settlement remain separate paths.
    pub async fn follower_evacuation(
        &self,
        original: &EnrollmentRecord,
        operation: &MaintenanceOperation,
        minimum_members: usize,
        deadline: tokio::time::Instant,
    ) -> cellule_runtime::Result<FollowerEvacuation> {
        let captured = tokio::time::Instant::now();
        let remaining = operation
            .deadline_ms()
            .checked_sub(now_ms()?)
            .filter(|remaining| *remaining > 0)
            .ok_or(Error::Deadline)?;
        if captured >= deadline {
            return Err(Error::Deadline);
        }
        let limit = captured
            .checked_add(Duration::from_millis(remaining.min(30_000) as u64))
            .ok_or(Error::Deadline)?;
        let deadline = deadline.min(limit);
        let producer = self
            .try_owned_component::<FleetFollowerEnrollment>(NODE_DURABILITY_PROVIDER_COMPONENT)?
            .ok_or(Error::Control(
                "follower evacuation requires managed enrollment",
            ))?;
        tokio::time::timeout_at(
            deadline,
            producer.evacuated(self, original, operation, minimum_members, deadline),
        )
        .await
        .map_err(|source| Error::Facility {
            name: "follower-evacuation-deadline",
            source: Box::new(source),
        })?
    }
}

impl FleetFollowerEnrollment {
    async fn evacuated(
        &self,
        node: &CellNode,
        original: &EnrollmentRecord,
        operation: &MaintenanceOperation,
        minimum_members: usize,
        deadline: tokio::time::Instant,
    ) -> cellule_runtime::Result<FollowerEvacuation> {
        // Reserve before encoding/cloning records or opaque prepared advertisements.
        // A canonical epoch has at most two members; copied records and transient
        // directory bodies remain bounded even when the full roster is much larger.
        let memory = self.runtime.try_reserve_node_metadata_bytes(
            10 * MAX_RECORD_BYTES as usize + 8 * MAX_NODE_BYTES as usize + 8192,
        )?;
        original.to_bytes().map_err(super::operation)?;
        let EnrollmentRole::Follower { log_epoch } = original.spec().role else {
            return Err(Error::Fenced);
        };
        if minimum_members == 0
            || minimum_members > 2
            || original.status() != EnrollmentStatus::Established
            || original.established_evidence().is_none()
            || original.spec().scope != self.scope
            || original
                .spec()
                .source
                .is_none_or(|source| source.node != self.node || source.session != self.session)
            || original.spec().target.node != operation.node()
            || original.spec().target.session != operation.session()
            || operation.node() == self.node
            || !matches!(
                operation.phase(),
                MaintenancePhase::Evacuating | MaintenancePhase::Closing
            )
        {
            return Err(Error::Fenced);
        }
        let request = node
            .node_log_rotation_request(log_epoch)?
            .ok_or(Error::Control(
                "follower evacuation lacks original rotation",
            ))?;
        let observation = request.observe()?;
        let rotation = observation.completion().cloned().ok_or(Error::Control(
            "follower evacuation replacement is incomplete",
        ))?;
        let barrier = rotation.retirement().barrier();
        if barrier.leader_session() != self.session
            || barrier.log_epoch() != log_epoch
            || !barrier.members().contains(&operation.node())
            || rotation.replacement_epoch() <= log_epoch
        {
            return Err(Error::Fenced);
        }
        let replacement = self
            .completion(rotation.replacement_epoch())?
            .ok_or(Error::Control(
                "follower evacuation lacks retained replacement producer",
            ))?;
        let prepared = replacement.attempt.prepared();
        if !replacement.native_started
            || replacement.native_closed
            || replacement.refusal.is_some()
            || replacement.enrollment.is_none()
            || prepared.log().epoch() != rotation.replacement_epoch()
            || prepared.log().members().len() < minimum_members
            || prepared.log().members().contains(&operation.node())
            || prepared.source().node() != self.node
            || prepared.source().session() != self.session
        {
            return Err(Error::Capacity(
                "follower maintenance replacements unavailable",
            ));
        }
        let (directory, lease) = {
            let bank = self
                .bank
                .lock()
                .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?;
            let inputs = &bank
                .epochs
                .get(&rotation.replacement_epoch())
                .ok_or(Error::Fenced)?
                .inputs;
            (inputs.directory.clone(), inputs.lease.clone())
        };
        lease.check()?;
        let started_at_ms = now_ms()?;
        let snapshot = self
            .journal
            .load_snapshot(self.scope)
            .await
            .map_err(journal_error)?;
        if snapshot.head().maintenance() != Some(operation)
            || snapshot.registry().bootstrap_revision().is_none()
        {
            return Err(Error::Fenced);
        }
        let roster = FleetRoster::collect_admitted(
            self.journal.as_ref(),
            &snapshot,
            deadline,
            &self.runtime,
        )
        .await?;
        let donor = roster.boot(operation.node(), operation.session())?;
        if donor.intent().mode() != NodeMode::Draining
            || donor.intent().revision() != operation.intent_revision()
        {
            return Err(Error::Fenced);
        }
        let retired = roster
            .enrollments()
            .iter()
            .find(|row| row.spec() == original.spec())
            .filter(|row| {
                row.status() == EnrollmentStatus::Retired
                    && row.accepted_at_ms() == original.accepted_at_ms()
                    && row.established_evidence() == original.established_evidence()
            })
            .cloned()
            .ok_or(Error::Control(
                "follower evacuation retirement is unconfirmed",
            ))?;
        // Every old member is settled, not only the donor's row. A changed or
        // delayed original producer must not be hidden by a newer ensemble.
        let mut retired_members = Vec::with_capacity(barrier.members().len());
        for member in barrier.members() {
            let mut rows = roster.enrollments().iter().filter(|row| {
                row.spec().source.is_some_and(|source| source.node == self.node && source.session == self.session)
                    && row.spec().target.node == *member
                    && matches!(row.spec().role, EnrollmentRole::Follower { log_epoch: epoch } if epoch == log_epoch)
            });
            let row = rows.next().ok_or(Error::Fenced)?;
            if row.status() != EnrollmentStatus::Retired || rows.next().is_some() {
                return Err(Error::Fenced);
            }
            retired_members.push(row.clone());
        }
        let mut replacements = Vec::new();
        if roster
            .enrollments()
            .iter()
            .filter(|row| {
                row.spec().source.is_some_and(|source| {
                    source.node == self.node && source.session == self.session
                }) && row.spec().role == (EnrollmentRole::Follower { log_epoch })
            })
            .count()
            != retired_members.len()
        {
            return Err(Error::Fenced);
        }
        if replacement.members.len() != prepared.followers().len() {
            return Err(Error::Fenced);
        }
        for (member, signed) in replacement.members.iter().zip(prepared.followers()) {
            let row = roster
                .enrollments()
                .iter()
                .find(|row| row.spec() == &member.spec)
                .ok_or(Error::Fenced)?;
            if !member.published
                || member.accepted.as_ref().is_none_or(|accepted| {
                    accepted.accepted_at_ms() != row.accepted_at_ms()
                        || accepted.spec() != row.spec()
                })
                || !matches!(member.event, Some(EnrollmentEvent::Established(evidence)) if Some(evidence) == row.established_evidence())
                || row.status() != EnrollmentStatus::Established
                || member.spec.target.node != signed.node()
                || member.spec.target.session != signed.session()
                || !matches!(member.spec.role, EnrollmentRole::Follower { log_epoch: epoch } if epoch == rotation.replacement_epoch())
            {
                return Err(Error::Fenced);
            }
            let boot = roster.boot(signed.node(), signed.session())?;
            if boot.intent().mode() != NodeMode::Active
                || boot.intent().revision() != member.spec.target.intent_revision
            {
                return Err(Error::Fenced);
            }
            replacements.push(row.clone());
        }
        let enrollment = directory
            .inspect_log_enrollment(&replacement.attempt, now_ms()?)
            .await?
            .ok_or(Error::Fenced)?;
        let authority = enrollment.enrollment().advertisement().clone();
        let source = roster.boot(self.node, self.session)?;
        if source.intent().mode() != NodeMode::Active
            || replacement.members.iter().any(|member| {
                member
                    .spec
                    .source
                    .is_none_or(|endpoint| endpoint.intent_revision != source.intent().revision())
            })
        {
            return Err(Error::Fenced);
        }
        // Probe exact signed boots twice around the full journal confirmation.
        // This also checks the donor's actual signed admission gate is closed.
        for _ in 0..2 {
            for signed in prepared.followers() {
                let current = directory
                    .load_if_live(signed.session(), now_ms()?)
                    .await?
                    .ok_or(Error::Fenced)?;
                if !same_boot(signed, current.advertisement())?
                    || !current.advertisement().accepts_new_roles(now_ms()?)
                {
                    return Err(Error::Fenced);
                }
            }
            let donor = directory
                .load_if_live(operation.session(), now_ms()?)
                .await?
                .ok_or(Error::Fenced)?;
            if donor.advertisement().node() != operation.node()
                || donor
                    .advertisement()
                    .operational_sample()
                    .is_none_or(|sample| sample.mode != NodeMode::Draining)
            {
                return Err(Error::Fenced);
            }
            let current = directory
                .inspect_log_enrollment(&replacement.attempt, now_ms()?)
                .await?
                .ok_or(Error::Fenced)?;
            if current.enrollment().advertisement() != &authority
                || authority
                    .log()
                    .is_none_or(|log| log.phase() != NodeLogPhase::Open)
                || authority
                    .operational_sample()
                    .is_none_or(|sample| sample.mode != NodeMode::Active)
            {
                return Err(Error::Fenced);
            }
            let (application, durability) = self.runtime.node_durability().ok_or(Error::Fenced)?;
            if application != self.scope.application
                || !node.is_ready()
                || self.runtime.is_shutting_down()
                || self.cancellation.is_cancelled()
                || self.runtime.node_admission().mode()? != NodeMode::Active
                || durability.identity()? != (self.session, self.node, rotation.replacement_epoch())
                || !request
                    .observe()?
                    .completion()
                    .is_some_and(|current| Arc::ptr_eq(current, &rotation))
            {
                return Err(Error::Fenced);
            }
            lease.check()?;
            roster.confirm(self.journal.as_ref(), deadline).await?;
        }
        let finished_at_ms = now_ms()?;
        if finished_at_ms < started_at_ms
            || finished_at_ms - started_at_ms > 30_000
            || finished_at_ms >= operation.deadline_ms()
        {
            return Err(Error::Deadline);
        }
        Ok(FollowerEvacuation {
            original: original.clone(),
            retired,
            retired_members,
            snapshot,
            rotation,
            replacement: enrollment,
            replacements,
            authority,
            minimum_members,
            started_at_ms,
            finished_at_ms,
            _memory: memory,
        })
    }
}

fn same_boot(
    original: &NodeAdvertisement,
    current: &NodeAdvertisement,
) -> cellule_runtime::Result<bool> {
    Ok(original.node() == current.node()
        && original.session() == current.session()
        && original.fleet() == current.fleet()
        && original.endpoint() == current.endpoint()
        && original.certificate() == current.certificate()
        && original.image() == current.image()
        && original.release() == current.release()
        && original.verifying_key()? == current.verifying_key()?
        && original.module_digests() == current.module_digests()
        && original.peer_versions() == current.peer_versions()
        && original.failure_domain() == current.failure_domain()
        && current.generation() >= original.generation()
        && current.issued_at_ms() >= original.issued_at_ms())
}

/// Immutable identity shared by native capture and durable revalidation.
pub(crate) fn boot_identity(node: &NodeAdvertisement) -> cellule_runtime::Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.follower-maintenance-boot.v1\0");
    for value in [
        node.node().as_bytes().as_slice(),
        node.session().as_bytes().as_slice(),
        node.fleet().as_bytes().as_slice(),
        node.certificate().as_bytes().as_slice(),
        node.image().as_bytes().as_slice(),
        node.release().as_bytes().as_slice(),
        node.endpoint().as_bytes(),
    ] {
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(value);
    }
    hash.update(&node.verifying_key()?.to_bytes());
    hash.update(&(node.module_digests().len() as u64).to_le_bytes());
    for digest in node.module_digests() {
        hash.update(digest.as_bytes());
    }
    hash.update(&(node.peer_versions().len() as u64).to_le_bytes());
    for version in node.peer_versions() {
        hash.update(&version.to_le_bytes());
    }
    for label in [node.failure_domain().zone(), node.failure_domain().host()] {
        match label {
            Some(label) => {
                hash.update(&[1]);
                hash.update(&(label.len() as u64).to_le_bytes());
                hash.update(label.as_bytes());
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
