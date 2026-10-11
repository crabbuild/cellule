//! Fixed placement embedding using celld's production CAS transport and wire formats.
//! This is not celld's complete dynamic fleet/decision-core executor.
use anyhow::{bail, ensure, Context, Result};
use celld::{bucket::Bucket, ownership_store::BucketOwnership};
use celld_logic::{CasGuard, CasOutcome, LeaseCasOutcome, NodeLeaseRecord};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub fn now() -> Result<u64> {
    Ok(u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

pub fn random_id() -> Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("OS random: {error}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub struct Authority {
    pub store: BucketOwnership,
    pub deadline: AtomicU64,
    born: std::time::Instant,
    monotonic_deadline: AtomicU64,
}

impl Authority {
    pub async fn start(
        address: &str,
    ) -> Result<(
        Arc<Self>,
        tokio::task::JoinHandle<Result<()>>,
        tokio::sync::oneshot::Sender<()>,
    )> {
        let spec = format!(
            "s3://{}/{}",
            std::env::var("CELLULE_TEST_BUCKET")?,
            std::env::var("CELLULE_TEST_PREFIX")?
        );
        let endpoint = std::env::var("CELLULE_TEST_ENDPOINT")?;
        let bucket = || Bucket::open(&spec, Some(&endpoint), "us-east-1", None, None);
        let node = random_id()?;
        let key = celld::peer_probe::install_signer()?;
        let authority = Arc::new(Self {
            store: BucketOwnership::new(bucket()?, bucket()?, node.clone(), key.clone())
                .with_lease_ttl_ms(30_000),
            deadline: AtomicU64::new(0),
            born: std::time::Instant::now(),
            monotonic_deadline: AtomicU64::new(0),
        });
        let mut record = NodeLeaseRecord {
            node,
            addr: address.into(),
            expires_ms: 0,
            peer_protocol: 1,
            generation: key,
            log_state: None,
            etag: String::new(),
        };
        authority.renew(&mut record, CasGuard::Absent).await?;
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let renewing = authority.clone();
        let task = tokio::spawn(async move {
            loop {
                celld::asyncrt::select! {
                    _ = &mut stopped => return Ok(()),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}
                }
                let guard = CasGuard::Match(record.etag.clone());
                if let Err(error) = renewing.renew(&mut record, guard).await {
                    renewing.deadline.store(0, Ordering::SeqCst);
                    return Err(error);
                }
            }
        });
        Ok((authority, task, stop))
    }

    async fn renew(&self, record: &mut NodeLeaseRecord, guard: CasGuard) -> Result<()> {
        let renewing = record.expires_ms != 0;
        if renewing {
            self.check()?;
        }
        let monotonic_deadline = u64::try_from(self.born.elapsed().as_millis())?
            .checked_add(30_000)
            .context("monotonic lease overflow")?;
        record.expires_ms = now()?
            .checked_add(30_000)
            .context("lease deadline overflow")?;
        let result = self
            .store
            .cas_node_lease(guard, record, &mut None)
            .await
            .map_err(|error| error.into_error())
            .context("lease CAS")?;
        if renewing {
            // A slow CAS cannot resurrect service after the prior authority
            // expired, even if the new lease object happened to land.
            self.check()?;
        }
        match result {
            LeaseCasOutcome::Applied { etag } => record.etag = etag,
            LeaseCasOutcome::Rejected => bail!("lease CAS rejected"),
        }
        ensure!(
            now()? < record.expires_ms,
            "lease expired during publication"
        );
        self.deadline.store(record.expires_ms, Ordering::SeqCst);
        self.monotonic_deadline
            .store(monotonic_deadline, Ordering::SeqCst);
        Ok(())
    }

    pub fn check(&self) -> Result<()> {
        ensure!(
            now()? < self.deadline.load(Ordering::SeqCst)
                && self.born.elapsed().as_millis()
                    < u128::from(self.monotonic_deadline.load(Ordering::SeqCst)),
            "node lease expired"
        );
        Ok(())
    }

    pub async fn acquire(&self, cell: &str) -> Result<(u64, bool, bool)> {
        let old = self.store.read_owner(cell).await?;
        let taken_over = old.as_ref().is_some_and(|owner| owner.node.is_some());
        if let Some(prior) = old.as_ref().and_then(|owner| owner.node.as_deref()) {
            let started = std::time::Instant::now();
            loop {
                self.check()?;
                let lease = self.store.read_node_lease(prior).await?;
                if let Some(lease) = lease {
                    // A raw SQLite/LTX embedding never ACKs a follower log.
                    ensure!(
                        lease.log_state.is_none(),
                        "takeover requires node-log recovery"
                    );
                    if lease.expires_ms > now()? {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                }
                break;
            }
            super::log(format_args!(
                "Node takeover: {}",
                serde_json::json!({"previous":prior,"wait_ms":started.elapsed().as_millis()})
            ))?;
        }
        let epoch = old.as_ref().map_or(Ok(1), |owner| {
            owner.epoch.checked_add(1).context("epoch overflow")
        })?;
        let guard = old.as_ref().map_or(CasGuard::Absent, |owner| {
            CasGuard::Match(owner.etag.clone())
        });
        self.check()?;
        ensure!(
            matches!(
                self.store.cas_owner(cell, guard, epoch).await?,
                CasOutcome::Applied
            ),
            "owner CAS rejected"
        );
        Ok((epoch, old.is_none(), taken_over))
    }
}
