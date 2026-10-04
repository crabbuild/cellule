//! Exact boot withdrawal after the canonical host resource sequence joins.

use std::sync::Arc;

use cellule_runtime::fleet::operations::{
    EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentStatus,
};
use cellule_runtime::identity::Digest;
use cellule_runtime::node::{NodeDirectory, VersionedNodeAdvertisement};
use cellule_runtime::{Error, Result};

use super::{FleetEnrollmentJournal, actions::wall_time_ms, operation};

/// One immutable boot binding retained by the original host drain owner.
pub(crate) struct FleetBootWithdrawal {
    directory: NodeDirectory,
    observed: VersionedNodeAdvertisement,
    original: EnrollmentRecord,
    journal: Arc<dyn FleetEnrollmentJournal>,
    evidence: Digest,
}

impl FleetBootWithdrawal {
    pub(crate) fn new(
        directory: NodeDirectory,
        observed: VersionedNodeAdvertisement,
        original: EnrollmentRecord,
        journal: Arc<dyn FleetEnrollmentJournal>,
    ) -> Result<Self> {
        original.to_bytes().map_err(operation)?;
        let spec = original.spec();
        let ad = observed.advertisement();
        // VersionedNodeAdvertisement is constructed only by the canonical
        // directory's authenticated read/CAS paths; it cannot be fabricated by
        // a remote action. Bind that original observation to the durable boot.
        if !matches!(spec.role, EnrollmentRole::Node { .. })
            || spec.source.is_some()
            || original.status() != EnrollmentStatus::Established
            || spec.target.node != ad.node()
            || spec.target.session != ad.session()
            || spec.scope.fleet != ad.fleet()
        {
            return Err(Error::Fenced);
        }
        let established = original
            .established_evidence()
            .ok_or(Error::Control("fleet boot lacks establishment evidence"))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"cellule.fleet-boot-withdrawal.v1\0");
        hash.update(&spec.to_bytes().map_err(operation)?);
        hash.update(established.as_bytes());
        hash.update(ad.certificate().as_bytes());
        hash.update(ad.image().as_bytes());
        hash.update(ad.release().as_bytes());
        hash.update(&ad.verifying_key()?.to_bytes());
        hash.update(&(ad.endpoint().len() as u64).to_be_bytes());
        hash.update(ad.endpoint().as_bytes());
        Ok(Self {
            directory,
            observed,
            original,
            journal,
            evidence: Digest::from_bytes(*hash.finalize().as_bytes()),
        })
    }

    pub(crate) async fn withdraw(&self) -> Result<()> {
        let spec = self.original.spec();
        let current = self
            .journal
            .load_enrollment(spec.scope, spec.key().map_err(operation)?)
            .await
            .map_err(journal_error)?
            .ok_or(Error::Control("fleet boot withdrawal record is absent"))?;
        current.validate_replay(spec).map_err(operation)?;
        if current.accepted_at_ms() != self.original.accepted_at_ms()
            || current.established_evidence() != self.original.established_evidence()
            || !matches!(
                current.status(),
                EnrollmentStatus::Established | EnrollmentStatus::Retired
            )
        {
            return Err(Error::Fenced);
        }
        // Always cross canonical conditional withdrawal, including replay. A
        // missing record or a recovery claimant cannot stand in for this boot's
        // checked shutdown. The original token reconciles a late heartbeat CAS.
        self.directory
            .withdraw_after_drain(&self.observed, wall_time_ms()?)
            .await?;
        if !self.directory.is_withdrawn(spec.target.session).await? {
            return Err(Error::Control("fleet boot withdrawal lacks its tombstone"));
        }
        let retired = self
            .journal
            .publish_enrollment_result(
                &self.original,
                EnrollmentEvent::Retired(self.evidence),
                wall_time_ms()?,
            )
            .await
            .map_err(journal_error)?;
        retired.validate_replay(spec).map_err(operation)?;
        if retired.accepted_at_ms() != self.original.accepted_at_ms()
            || retired.established_evidence() != self.original.established_evidence()
            || retired.status() != EnrollmentStatus::Retired
            || retired.settlement_evidence() != Some(self.evidence)
        {
            return Err(Error::Control("fleet boot retirement result differs"));
        }
        Ok(())
    }
}

fn journal_error(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Error::Facility {
        name: "fleet-boot-journal",
        source,
    }
}
