//! Read-only follower selection and exact CAS inputs for durable enrollment owners.
use super::*;

/// A complete selected ensemble and its original signed physical boot identities.
/// Preparation sends no frames and changes no authority. Metadata grants no
/// admission: an embedding fleet producer must journal every member Pending
/// before committing the retained attempt.
#[derive(Clone)]
pub struct PreparedNodeLogEnrollment {
    directory_scope: [u8; 16],
    source: NodeAdvertisement,
    followers: Vec<NodeAdvertisement>,
    log: NodeLogStatus,
    required_follower_bytes: u64,
}

impl PreparedNodeLogEnrollment {
    /// Returns the original signed leader boot, including its physical identity.
    #[must_use]
    pub const fn source(&self) -> &NodeAdvertisement {
        &self.source
    }

    /// Returns every selected signed follower boot in canonical physical-node order.
    #[must_use]
    pub fn followers(&self) -> &[NodeAdvertisement] {
        &self.followers
    }

    /// Returns the fixed epoch and complete sorted member set to be enrolled.
    #[must_use]
    pub const fn log(&self) -> &NodeLogStatus {
        &self.log
    }
}

/// One immutable conditional enrollment write, prepared without side effects.
/// Retain this exact attempt before awaiting the CAS. Retry/inspection never
/// chooses another ensemble or boot. A missing log is not a refusal proof.
#[derive(Clone)]
pub struct NodeLogEnrollmentAttempt {
    prepared: PreparedNodeLogEnrollment,
    observed: VersionedNodeAdvertisement,
    next: NodeAdvertisement,
}

impl NodeLogEnrollmentAttempt {
    /// Digests the original signed source version, selected boot snapshots and
    /// epoch for a durable producer's request-bound evidence. Grants no admission.
    pub fn evidence_digest(&self) -> Result<Digest> {
        enrollment_digest(
            &self.prepared,
            &self.observed,
            b"cellule.node-log.attempt.v1\0",
        )
    }
    /// Returns the complete original selection retained by this attempt.
    #[must_use]
    pub const fn prepared(&self) -> &PreparedNodeLogEnrollment {
        &self.prepared
    }

    /// Returns the fresh signed source and CAS version captured before dispatch.
    #[must_use]
    pub const fn observed(&self) -> &VersionedNodeAdvertisement {
        &self.observed
    }
}

/// Canonical observation of this exact attempt's leader boot, epoch and ensemble.
/// It proves enrollment, not follower fsync, fleet registry publication, current
/// redundancy policy, or retirement. The original selected follower boots remain
/// pinned even if they subsequently expire or are replaced.
#[derive(Clone)]
pub struct NodeLogEnrollmentProof {
    prepared: PreparedNodeLogEnrollment,
    enrollment: VersionedNodeAdvertisement,
}

impl NodeLogEnrollmentProof {
    /// Digests the original selected scope and its checked canonical observation.
    pub fn evidence_digest(&self) -> Result<Digest> {
        enrollment_digest(
            &self.prepared,
            &self.enrollment,
            b"cellule.node-log.enrolled.v1\0",
        )
    }
    /// Returns the original scope and every selected follower boot.
    #[must_use]
    pub const fn prepared(&self) -> &PreparedNodeLogEnrollment {
        &self.prepared
    }

    /// Returns the exact canonical leader version establishing the enrollment.
    #[must_use]
    pub const fn enrollment(&self) -> &VersionedNodeAdvertisement {
        &self.enrollment
    }
}

/// Proof that a conditional fence won against this exact enrollment attempt.
/// The fence and enrollment use the same original source version, so this
/// attempt's delayed CAS can no longer enroll followers. This proves neither
/// the absence of other enrollments nor retirement of any existing epoch.
#[derive(Clone)]
pub struct NodeLogEnrollmentRefusalProof {
    prepared: PreparedNodeLogEnrollment,
    refusal: VersionedNodeAdvertisement,
}

impl NodeLogEnrollmentRefusalProof {
    /// Digests the selected scope and original-token conditional refusal receipt.
    pub fn evidence_digest(&self) -> Result<Digest> {
        enrollment_digest(
            &self.prepared,
            &self.refusal,
            b"cellule.node-log.refused.v1\0",
        )
    }
    /// Returns the original selected scope whose one attempt is now fenced.
    #[must_use]
    pub const fn prepared(&self) -> &PreparedNodeLogEnrollment {
        &self.prepared
    }

    /// Returns the canonical source version produced by the conditional fence.
    #[must_use]
    pub const fn refusal(&self) -> &VersionedNodeAdvertisement {
        &self.refusal
    }
}

fn enrollment_digest(
    prepared: &PreparedNodeLogEnrollment,
    source: &VersionedNodeAdvertisement,
    domain: &[u8],
) -> Result<Digest> {
    let mut digest = blake3::Hasher::new();
    digest.update(domain);
    // These private, immutable proof inputs were authenticated by preparation,
    // canonical inspection, or the checked conditional write. Historical
    // fingerprints serialize those exact bytes; every fresh provider read still
    // authenticates independently. Re-verifying here repeated all boot signatures
    // for each producer page without observing any new authority.
    digest.update(&prepared.source.canonical_bytes()?);
    digest.update(&source.advertisement.canonical_bytes()?);
    // Preserve the immutable conditional-write token as well as signed bytes.
    // Lengths and option markers keep arbitrary provider tokens unambiguous.
    for token in [&source.token.e_tag, &source.token.version] {
        match token {
            Some(token) => {
                digest.update(&[1]);
                digest.update(&(token.len() as u64).to_le_bytes());
                digest.update(token.as_bytes());
            }
            None => {
                digest.update(&[0]);
            }
        }
    }
    digest.update(&prepared.log.epoch().to_le_bytes());
    for follower in &prepared.followers {
        digest.update(&follower.canonical_bytes()?);
    }
    Ok(Digest::from_bytes(*digest.finalize().as_bytes()))
}

impl NodeDirectory {
    /// Selects one complete ensemble without executing its authority CAS.
    /// A producer can now persist every selected follower obligation before the
    /// first native effect, rather than registering only after recruitment.
    /// Providers must assign a never-reused, monotonically advancing epoch for
    /// this signed leader boot, including after closure and ambiguous results.
    pub async fn prepare_log_enrollment(
        &self,
        observed: &VersionedNodeAdvertisement,
        log_epoch: u64,
        required_follower_bytes: u64,
        live_node_limit: usize,
        now_ms: i64,
    ) -> Result<Option<PreparedNodeLogEnrollment>> {
        self.validate(&observed.advertisement, now_ms)?;
        if observed.advertisement.log.is_some() {
            return Err(Error::Node("node session already has an enrolled log"));
        }
        let followers = self
            .select_log_advertisements(
                observed.advertisement.session,
                required_follower_bytes,
                now_ms,
                live_node_limit,
            )
            .await?;
        if followers.is_empty() {
            return Ok(None);
        }
        let log = NodeLogStatus::open(
            observed.advertisement.node,
            log_epoch,
            followers.iter().map(|member| member.node).collect(),
        )?;
        Ok(Some(PreparedNodeLogEnrollment {
            directory_scope: self.inventory_scope,
            source: observed.advertisement.clone(),
            followers,
            log,
            required_follower_bytes,
        }))
    }

    /// Revalidates the original boots and captures a fresh source CAS version.
    /// Heartbeats may change mutable measurements; replacement boots and an
    /// already-enrolled source are rejected. Members are never reselected.
    /// Prepare this before journal acceptance, then retain it through settlement.
    pub async fn prepare_log_enrollment_attempt(
        &self,
        prepared: &PreparedNodeLogEnrollment,
        now_ms: i64,
    ) -> Result<NodeLogEnrollmentAttempt> {
        self.validate_enrollment_scope(prepared)?;
        let observed = self
            .load_if_live(prepared.source.session, now_ms)
            .await?
            .ok_or(Error::Node("node-log leader is not live"))?;
        if !same_boot_identity(&prepared.source, &observed.advertisement) {
            return Err(Error::Node("node-log leader boot differs from preparation"));
        }
        if observed.advertisement.log.is_some() {
            return Err(Error::Node("node session already has an enrolled log"));
        }
        for original in &prepared.followers {
            let current = self
                .load_if_live(original.session, now_ms)
                .await?
                .ok_or(Error::Node("prepared node-log follower is not live"))?;
            let current = &current.advertisement;
            if !same_boot_identity(original, current) {
                return Err(Error::Node(
                    "node-log follower boot differs from preparation",
                ));
            }
            if !advertisement::accepts_log_enrollment(
                current,
                prepared.required_follower_bytes,
                now_ms,
            ) {
                return Err(Error::Node(
                    "prepared node-log follower cannot receive enrollment",
                ));
            }
        }
        self.enrollment_attempt(prepared, observed)
    }

    pub(super) fn enrollment_attempt(
        &self,
        prepared: &PreparedNodeLogEnrollment,
        observed: VersionedNodeAdvertisement,
    ) -> Result<NodeLogEnrollmentAttempt> {
        let mut next = observed.advertisement.clone();
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(Error::Node("node session generation overflow"))?;
        next.log = Some(prepared.log.clone());
        Ok(NodeLogEnrollmentAttempt {
            prepared: prepared.clone(),
            observed,
            next,
        })
    }

    /// Executes only the retained attempt's conditional write and fixed ensemble.
    /// Fleet producers must first accept Pending for every original participant.
    /// An error, timeout or dropped waiter is ambiguous; retain the same attempt
    /// and inspect it. Starting a different enrollment cannot settle this one.
    pub async fn commit_log_enrollment(
        &self,
        attempt: &NodeLogEnrollmentAttempt,
        now_ms: i64,
    ) -> Result<NodeLogEnrollmentProof> {
        self.validate_enrollment_scope(&attempt.prepared)?;
        self.validate(&attempt.observed.advertisement, now_ms)?;
        let enrollment = self
            .update_advertisement(&attempt.observed, attempt.next.clone(), now_ms)
            .await?;
        self.enrollment_proof(&attempt.prepared, enrollment)?
            .ok_or(Error::Node(
                "node-log enrollment CAS returned a different ensemble",
            ))
    }

    /// Freshly reconciles the exact original attempt across activation/heartbeats.
    /// `None` leaves the outcome unknown. Absence, another epoch, expiry or a
    /// tombstone cannot prove nonexecution or settle any registry obligation.
    pub async fn inspect_log_enrollment(
        &self,
        attempt: &NodeLogEnrollmentAttempt,
        now_ms: i64,
    ) -> Result<Option<NodeLogEnrollmentProof>> {
        self.validate_enrollment_scope(&attempt.prepared)?;
        let Some(current) = self
            .load_if_live(attempt.prepared.source.session, now_ms)
            .await?
        else {
            return Ok(None);
        };
        self.enrollment_proof(&attempt.prepared, current)
    }

    /// Conditionally fences only the original attempt using its exact CAS token.
    /// The no-log successor competes with the enrollment write on the same
    /// version; at most one can succeed. Never rebase a refusal fence. A failure,
    /// lost reply, newer empty record or missing source leaves the outcome unknown.
    pub async fn fence_log_enrollment(
        &self,
        attempt: &NodeLogEnrollmentAttempt,
        now_ms: i64,
    ) -> Result<NodeLogEnrollmentRefusalProof> {
        self.validate_enrollment_scope(&attempt.prepared)?;
        self.validate(&attempt.observed.advertisement, now_ms)?;
        let mut next = attempt.observed.advertisement.clone();
        next.generation = attempt.next.generation;
        // The original observed source is unenrolled. Publishing its unchanged
        // body with the next generation invalidates the delayed enrollment CAS.
        let refusal = self
            .update_advertisement(&attempt.observed, next, now_ms)
            .await?;
        Ok(NodeLogEnrollmentRefusalProof {
            prepared: attempt.prepared.clone(),
            refusal,
        })
    }

    fn validate_enrollment_scope(&self, prepared: &PreparedNodeLogEnrollment) -> Result<()> {
        if prepared.directory_scope != self.inventory_scope {
            return Err(Error::Node(
                "node-log enrollment belongs to another directory",
            ));
        }
        Ok(())
    }

    fn enrollment_proof(
        &self,
        prepared: &PreparedNodeLogEnrollment,
        current: VersionedNodeAdvertisement,
    ) -> Result<Option<NodeLogEnrollmentProof>> {
        if !same_boot_identity(&prepared.source, &current.advertisement) {
            return Err(Error::Node("node-log leader boot differs from preparation"));
        }
        let Some(log) = &current.advertisement.log else {
            return Ok(None);
        };
        if log.epoch() != prepared.log.epoch() || log.members() != prepared.log.members() {
            return Ok(None);
        }
        Ok(Some(NodeLogEnrollmentProof {
            prepared: prepared.clone(),
            enrollment: current,
        }))
    }
}
