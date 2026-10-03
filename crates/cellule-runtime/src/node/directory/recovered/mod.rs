//! Canonical authorization and complete member closure after recovered tail pinning.
use super::*;
use crate::node::log_recovery::retirement::RecoveredNodeLogRetirementProof;

/// Fresh authorization of one original receiver's canonically recovered lane.
/// Applications authenticate the live claimant and bind `member` to the local
/// receiver before requesting this proof. It supplies no lane deletion permission.
pub struct RecoveredLogRetirementAuthorization {
    member: NodeId,
    sealed: SealedNodeLog,
}
impl RecoveredLogRetirementAuthorization {
    /// Exact physical receiver authorized by the original ensemble.
    #[must_use]
    pub const fn member(&self) -> NodeId {
        self.member
    }
    /// Canonical sealed/retired epoch and pinned recovery manifest identity.
    #[must_use]
    pub const fn sealed(&self) -> &SealedNodeLog {
        &self.sealed
    }
}

impl NodeDirectory {
    /// Adopts an exact committed recovered retirement after a lost result or
    /// controller restart, including after local grace collection. `None` means
    /// the matching canonical epoch is still Sealed and its members must be
    /// confirmed. Missing, changed or unrecovered authority is an error.
    pub async fn retired_recovered_log(
        &self,
        sealed: &SealedNodeLog,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<Option<SealedNodeLog>> {
        if now_ms < 0 || claimant == sealed.session() {
            return Err(Error::Fenced);
        }
        self.load(claimant, now_ms).await?.ok_or(Error::Fenced)?;
        let path = self.layout.node_path(sealed.session().as_bytes());
        let Some((NodeRecord::Tombstone(current), _)) = self.load_record_at(&path).await? else {
            return Err(Error::Fenced);
        };
        let log = current.log.as_ref().ok_or(Error::Fenced)?;
        if current.session != sealed.session()
            || log.retire_recovered(current.node)? != sealed.log().retire_recovered(current.node)?
        {
            return Err(Error::Fenced);
        }
        Ok(
            (log.phase() == NodeLogPhase::Retired).then(|| SealedNodeLog {
                session: current.session,
                log: log.clone(),
            }),
        )
    }

    /// Rechecks the receiver's exact recovery retirement request against authority.
    /// Authenticate `claimant` using application enrollment; never trust a claimed
    /// sender ID. Expiry, a recovery claim or a native seal alone cannot authorize
    /// retirement. Only canonical Sealed/Retired authority after pinning does so.
    pub async fn authorize_recovered_log_retire(
        &self,
        claimant: SessionId,
        member: NodeId,
        leader: SessionId,
        epoch: u64,
        manifest: Option<Digest>,
        now_ms: i64,
    ) -> Result<RecoveredLogRetirementAuthorization> {
        if now_ms < 0 || epoch == 0 || claimant == leader {
            return Err(Error::Fenced);
        }
        self.load(claimant, now_ms)
            .await?
            .ok_or(Error::PeerAuthorization(
                "recovered log retirement requester is not live",
            ))?;
        let path = self.layout.node_path(leader.as_bytes());
        let Some((NodeRecord::Tombstone(current), _)) = self.load_record_at(&path).await? else {
            return Err(Error::PeerAuthorization(
                "recovered log leader is not fenced",
            ));
        };
        let log = current.log.as_ref().ok_or(Error::Fenced)?;
        if current.session != leader
            || log.epoch() != epoch
            || log.recovery_manifest() != manifest
            || !log.members().contains(&member)
            || !matches!(log.phase(), NodeLogPhase::Sealed | NodeLogPhase::Retired)
        {
            return Err(Error::Fenced);
        }
        Ok(RecoveredLogRetirementAuthorization {
            member,
            sealed: SealedNodeLog {
                session: leader,
                log: log.clone(),
            },
        })
    }

    /// CAS-closes a recovered epoch only after every original member confirms its fence.
    /// Lost replies adopt the exact Retired record. The tombstone and manifest
    /// remain permanent fencing/history; grace-aged local collection is separate.
    pub async fn retire_recovered_log(
        &self,
        proof: &RecoveredNodeLogRetirementProof,
        claimant: SessionId,
        now_ms: i64,
    ) -> Result<SealedNodeLog> {
        if now_ms < 0 || claimant == proof.sealed().session() {
            return Err(Error::Fenced);
        }
        self.load(claimant, now_ms).await?.ok_or(Error::Fenced)?;
        let path = self.layout.node_path(proof.sealed().session().as_bytes());
        let Some((NodeRecord::Tombstone(mut current), token)) = self.load_record_at(&path).await?
        else {
            return Err(Error::Fenced);
        };
        if current.session != proof.sealed().session() {
            return Err(Error::Fenced);
        }
        let log = current.log.as_ref().ok_or(Error::Fenced)?;
        let retired = log.retire_recovered(current.node)?;
        if retired != proof.sealed().log().retire_recovered(current.node)? {
            return Err(Error::Fenced);
        }
        if log.phase() == NodeLogPhase::Retired {
            return Ok(SealedNodeLog {
                session: current.session,
                log: retired,
            });
        }
        current.log = Some(retired.clone());
        current.validate()?;
        let expected = SealedNodeLog {
            session: current.session,
            log: retired,
        };
        match self
            .layout
            .store()
            .update(&path, Bytes::from(current.encode()?), token)
            .await
        {
            Ok(_) => Ok(expected),
            Err(source) => match self.load_record_at(&path).await? {
                Some((NodeRecord::Tombstone(current), _))
                    if current.session == expected.session
                        && current.log.as_ref() == Some(&expected.log) =>
                {
                    Ok(expected)
                }
                _ => Err(source.into()),
            },
        }
    }
}
