//! One cancellation-scoped renewal loop for a managed recovery attempt.
use super::*;
use crate::node::directory::{NodeRecord, NodeTombstone};
use tokio::sync::{Mutex, MutexGuard};

pub(crate) struct RecoveryClaimLease<'a> {
    directory: &'a NodeDirectory,
    current: Mutex<FencedNodeSession>,
    now_ms: i64,
    started: std::time::Instant,
}

impl<'a> RecoveryClaimLease<'a> {
    pub(crate) async fn new(
        directory: &'a NodeDirectory,
        fenced: FencedNodeSession,
        now_ms: i64,
    ) -> Result<Self> {
        let lease = Self {
            directory,
            current: Mutex::new(fenced),
            now_ms,
            started: std::time::Instant::now(),
        };
        lease.verify().await?;
        Ok(lease)
    }

    pub(crate) fn now(&self) -> Result<i64> {
        crate::node::bundle::recovery::logical_now(self.now_ms, self.started)
    }

    pub(in crate::node) async fn lock_verified(
        &self,
    ) -> Result<(
        MutexGuard<'_, FencedNodeSession>,
        NodeTombstone,
        cellule_store::ETag,
    )> {
        let current = self.current.lock().await;
        let (record, token) = self.check(&current).await?;
        Ok((current, record, token))
    }

    pub(crate) async fn verify(&self) -> Result<FencedNodeSession> {
        let (current, _, _) = self.lock_verified().await?;
        Ok(current.clone())
    }

    async fn check(
        &self,
        fenced: &FencedNodeSession,
    ) -> Result<(NodeTombstone, cellule_store::ETag)> {
        self.directory
            .load(fenced.claimant(), self.now()?)
            .await?
            .ok_or(Error::Fenced)?;
        let path = self.directory.layout.node_path(fenced.session().as_bytes());
        let Some((NodeRecord::Tombstone(current), token)) =
            self.directory.load_record_at(&path).await?
        else {
            return Err(Error::Fenced);
        };
        if current.fenced()? != *fenced || self.now()? >= fenced.claim_expires_at_ms() {
            return Err(Error::Fenced);
        }
        Ok((*current, token))
    }

    async fn keep_alive(&self) -> Result<()> {
        loop {
            let expiry = self.current.lock().await.claim_expires_at_ms();
            let lifetime = crate::node::log_state::RECOVERY_CLAIM_LIFETIME_MS;
            let delay = expiry
                .saturating_sub(self.now()?)
                .saturating_sub(lifetime * 2 / 3);
            tokio::time::sleep(std::time::Duration::from_millis(
                u64::try_from(delay.max(1))
                    .map_err(|_| Error::Capacity("recovery renewal delay"))?,
            ))
            .await;
            // Renewal and terminal node CAS share this lock. Immutable data
            // verification and Cell materialization never hold it.
            let (mut current, _, _) = self.lock_verified().await?;
            *current = self
                .directory
                .refresh_recovery_claim(&current, self.now()?)
                .await?;
            self.check(&current).await?;
        }
    }

    pub(crate) async fn run<T>(
        &self,
        work: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        // No detached heartbeat survives success, failure or cancellation.
        tokio::select! {
            biased;
            result = work => result,
            result = self.keep_alive() => {
                result?;
                Err(Error::Node("recovery renewal loop stopped"))
            }
        }
    }

    pub(crate) async fn seal(&self, manifest: Option<Digest>) -> Result<CompletedNodeRecoverySeal> {
        let snapshot = self.verify().await?;
        let prepared = self.directory.prepare_recovery_seal(&snapshot).await?;
        // The immutable terminal inventory was checked outside the renewal
        // lock. Its exact head is checked again by the final authority CAS.
        let (current, _, _) = self.lock_verified().await?;
        let sealed = self
            .directory
            .seal_recovery_prepared(&current, manifest, self.now()?, &prepared)
            .await?;
        let takeover = NodeTakeoverProof::after_recovery(&current, &sealed)?;
        Ok(CompletedNodeRecoverySeal { sealed, takeover })
    }
}

pub(crate) struct CompletedNodeRecoverySeal {
    pub(crate) sealed: SealedNodeLog,
    pub(crate) takeover: NodeTakeoverProof,
}
