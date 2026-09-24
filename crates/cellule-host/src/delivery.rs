//! Host-owned delivery for the Cells one node serves.
//!
//! The embedding service keeps providers, authentication, and peer transport.
//! This module owns the loop that turns a due Cell into a maintenance tick and,
//! for the namespaces that registered runners, one bounded activity, queue
//! consumer, and effect pass. Cells this node does not serve are skipped: the
//! node that owns them runs its own pass.

use std::{
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_runtime::{
    ApplicationId, BlockingActivityPool, CellAuthority, CellCatalog, CellClient, CellRuntime,
    CellTarget, DueCell, DueCellScan, EffectPeerClient, Error, InvocationError,
    MaintenanceTickRequest, MutationIdentity, Registry, RequestId, Result, TenantId,
};
use rand::RngCore;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::FacilityResult;

const DEFAULT_POLL_INTERVAL_MS: u64 = 5_000;
const DEFAULT_MAX_CELLS_PER_PASS: usize = 32;
const DEFAULT_LEASE_MS: u32 = 30_000;
const MIN_LEASE_MS: u32 = 5_000;
const MAX_LEASE_MS: u32 = 300_000;
const MAX_CELLS_PER_PASS: usize = 128;
const DEFAULT_MAX_CONCURRENT_CELLS: usize = 4;
const MAX_CONCURRENT_CELLS: usize = 64;
const DEFAULT_MAX_SHARDS_PER_PASS: usize = 64;
const MAX_SHARDS: usize = 256;
const MIN_POLL_INTERVAL_MS: u64 = 10;
const MAX_POLL_INTERVAL_MS: u64 = 10 * 60 * 1_000;
const IDENTITY_LIFETIME_MS: i64 = 60_000;

/// Bounds and cadence for host-owned delivery passes.
#[derive(Clone, Debug)]
pub struct CellDeliveryConfig {
    tenant: TenantId,
    application: ApplicationId,
    catalog_shards: Vec<u8>,
    poll_interval: Duration,
    max_cells_per_pass: usize,
    max_concurrent_cells: usize,
    max_shards_per_pass: usize,
    lease_ms: u32,
}

impl CellDeliveryConfig {
    /// Creates a config that scans every catalog shard on the default cadence.
    #[must_use]
    pub fn new(tenant: TenantId, application: ApplicationId) -> Self {
        Self {
            tenant,
            application,
            catalog_shards: (0..=u8::MAX).collect(),
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            max_cells_per_pass: DEFAULT_MAX_CELLS_PER_PASS,
            max_concurrent_cells: DEFAULT_MAX_CONCURRENT_CELLS,
            max_shards_per_pass: DEFAULT_MAX_SHARDS_PER_PASS,
            lease_ms: DEFAULT_LEASE_MS,
        }
    }

    /// Scans only the catalog shards this node is responsible for.
    #[must_use]
    pub fn with_catalog_shards(mut self, shards: impl IntoIterator<Item = u8>) -> Self {
        let mut unique: Vec<u8> = shards.into_iter().collect();
        unique.sort_unstable();
        unique.dedup();
        self.catalog_shards = unique;
        self
    }

    /// Sets the delay between delivery passes.
    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets how many due Cells one catalog batch returns.
    #[must_use]
    pub fn with_max_cells_per_pass(mut self, limit: usize) -> Self {
        self.max_cells_per_pass = limit;
        self
    }

    /// Sets how many due Cells one pass delivers at the same time.
    #[must_use]
    pub fn with_max_concurrent_cells(mut self, limit: usize) -> Self {
        self.max_concurrent_cells = limit;
        self
    }

    /// Sets how many catalog shards one pass reads, rotating across passes.
    ///
    /// A node that owns every shard still reads a bounded window per pass, so
    /// the discovery cost of a large catalog does not scale with the number of
    /// Cells it holds.
    #[must_use]
    pub fn with_max_shards_per_pass(mut self, limit: usize) -> Self {
        self.max_shards_per_pass = limit;
        self
    }

    /// Sets the lease used by activity, queue consumer, and effect passes.
    #[must_use]
    pub fn with_lease_ms(mut self, lease_ms: u32) -> Self {
        self.lease_ms = lease_ms;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.catalog_shards.is_empty() {
            return Err(Error::Control("Cell delivery requires a catalog shard"));
        }
        let poll_interval_ms = u64::try_from(self.poll_interval.as_millis()).unwrap_or(u64::MAX);
        if !(MIN_POLL_INTERVAL_MS..=MAX_POLL_INTERVAL_MS).contains(&poll_interval_ms) {
            return Err(Error::Control(
                "Cell delivery poll interval must be in 10ms..=10m",
            ));
        }
        if !(1..=MAX_CELLS_PER_PASS).contains(&self.max_cells_per_pass) {
            return Err(Error::Control(
                "Cell delivery batch must be in 1..=128 Cells",
            ));
        }
        if !(1..=MAX_CONCURRENT_CELLS).contains(&self.max_concurrent_cells) {
            return Err(Error::Control(
                "Cell delivery concurrency must be in 1..=64 Cells",
            ));
        }
        if !(1..=MAX_SHARDS).contains(&self.max_shards_per_pass) {
            return Err(Error::Control(
                "Cell delivery shard window must be in 1..=256 shards",
            ));
        }
        if !(MIN_LEASE_MS..=MAX_LEASE_MS).contains(&self.lease_ms) {
            return Err(Error::Control(
                "Cell delivery lease must be in 5..=300 seconds",
            ));
        }
        Ok(())
    }
}

/// One host-owned delivery loop.
pub struct CellDelivery {
    config: CellDeliveryConfig,
    catalog: CellCatalog,
    authority: CellAuthority,
    client: CellClient,
    runtime: CellRuntime,
    registry: Arc<Registry>,
    peer: EffectPeerClient,
    blocking: Arc<BlockingActivityPool>,
    counters: Arc<CellDeliveryCounters>,
    shard_cursor: AtomicU64,
}

/// Shared delivery counters for one node.
#[derive(Debug, Default)]
pub struct CellDeliveryCounters {
    passes: AtomicU64,
    due_cells: AtomicU64,
    delivered: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
    in_flight: AtomicU64,
}

impl CellDeliveryCounters {
    /// Samples the counters without waiting for an in-flight delivery.
    #[must_use]
    pub fn snapshot(&self) -> CellDeliveryStats {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        CellDeliveryStats {
            passes: read(&self.passes),
            due_cells: read(&self.due_cells),
            delivered: read(&self.delivered),
            skipped: read(&self.skipped),
            failed: read(&self.failed),
            in_flight: read(&self.in_flight),
        }
    }
}

/// Point-in-time delivery activity for one node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellDeliveryStats {
    passes: u64,
    due_cells: u64,
    delivered: u64,
    skipped: u64,
    failed: u64,
    in_flight: u64,
}

impl CellDeliveryStats {
    /// Returns how many passes the loop completed or started.
    #[must_use]
    pub const fn passes(self) -> u64 {
        self.passes
    }

    /// Returns how many due Cells the scans observed.
    #[must_use]
    pub const fn due_cells(self) -> u64 {
        self.due_cells
    }

    /// Returns how many due Cells this node delivered for.
    #[must_use]
    pub const fn delivered(self) -> u64 {
        self.delivered
    }

    /// Returns how many due Cells this node did not serve or found stale.
    #[must_use]
    pub const fn skipped(self) -> u64 {
        self.skipped
    }

    /// Returns how many deliveries failed and ended their pass early.
    #[must_use]
    pub const fn failed(self) -> u64 {
        self.failed
    }

    /// Returns how many deliveries are running right now.
    #[must_use]
    pub const fn in_flight(self) -> u64 {
        self.in_flight
    }
}

/// What one delivery pass did for one due Cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryOutcome {
    Delivered,
    Skipped,
}

impl CellDelivery {
    /// Creates a delivery loop from the node's runtime, registry, and adapters.
    pub fn new(
        config: CellDeliveryConfig,
        catalog: CellCatalog,
        authority: CellAuthority,
        client: CellClient,
        runtime: CellRuntime,
        registry: Arc<Registry>,
        peer: EffectPeerClient,
        blocking: Arc<BlockingActivityPool>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            catalog,
            authority,
            client,
            runtime,
            registry,
            peer,
            blocking,
            counters: Arc::new(CellDeliveryCounters::default()),
            shard_cursor: AtomicU64::new(0),
        })
    }

    /// Returns the shared counters this loop updates.
    #[must_use]
    pub fn counters(&self) -> Arc<CellDeliveryCounters> {
        Arc::clone(&self.counters)
    }

    /// Runs delivery passes until the cancellation token fires.
    pub async fn run(self, cancellation: CancellationToken) -> FacilityResult {
        let delivery = Arc::new(self);
        loop {
            let deadline = Instant::now() + delivery.config.poll_interval;
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = sleep_until(deadline) => {}
            }
            match delivery.pass().await {
                Ok(()) => {}
                Err(Error::RuntimeClosed | Error::CellDraining) => return Ok(()),
                Err(error) => {
                    tracing::warn!(error = %error, "Cell delivery pass was not resolved");
                }
            }
        }
    }

    async fn pass(self: &Arc<Self>) -> Result<()> {
        let logical_time_ms = system_time_ms()?;
        self.counters.passes.fetch_add(1, Ordering::Relaxed);
        let mut deliveries = JoinSet::new();
        for shard in self.shards_for_pass() {
            let mut scan = DueCellScan::new(&self.catalog, self.authority.clone(), shard).await?;
            while let Some(due) = scan
                .next_batch_bounded(logical_time_ms, self.config.max_cells_per_pass)
                .await?
            {
                for cell in due {
                    self.counters.due_cells.fetch_add(1, Ordering::Relaxed);
                    while deliveries.len() >= self.config.max_concurrent_cells {
                        join_delivery(&mut deliveries).await?;
                    }
                    let delivery = Arc::clone(self);
                    deliveries.spawn(async move {
                        let counters = Arc::clone(&delivery.counters);
                        counters.in_flight.fetch_add(1, Ordering::Relaxed);
                        let result = delivery.deliver(&cell).await;
                        counters.in_flight.fetch_sub(1, Ordering::Relaxed);
                        match &result {
                            Ok(DeliveryOutcome::Delivered) => {
                                counters.delivered.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(DeliveryOutcome::Skipped) => {
                                counters.skipped.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                counters.failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        result.map(|_| ())
                    });
                }
            }
        }
        while !deliveries.is_empty() {
            join_delivery(&mut deliveries).await?;
        }
        Ok(())
    }

    /// Returns the next bounded shard window, rotating across passes so every
    /// configured shard is scanned within a bounded number of passes.
    fn shards_for_pass(&self) -> Vec<u8> {
        let shards = &self.config.catalog_shards;
        let window = self.config.max_shards_per_pass.min(shards.len());
        let start = self
            .shard_cursor
            .fetch_add(window as u64, Ordering::Relaxed) as usize
            % shards.len();
        (0..window)
            .map(|offset| shards[(start + offset) % shards.len()])
            .collect()
    }

    async fn deliver(&self, due: &DueCell) -> Result<DeliveryOutcome> {
        let entry = due.catalog().entry();
        let target = CellTarget::new(
            self.config.tenant,
            self.config.application,
            entry.namespace(),
            entry.partition(),
        )?;
        let Some(_job) = self.runtime.try_reserve_worker_job()? else {
            return Ok(DeliveryOutcome::Skipped);
        };
        if self
            .runtime
            .resident_handle(&target, entry.role())
            .await?
            .is_none()
        {
            return Ok(DeliveryOutcome::Skipped);
        }
        let Some(root) = due.control().value().root.as_ref() else {
            return Ok(DeliveryOutcome::Skipped);
        };
        let tick = self
            .registry
            .run_maintenance_once(
                self.client.clone(),
                target.clone(),
                delivery_identity()?,
                MaintenanceTickRequest {
                    expected_commit_sequence: root.commit_sequence,
                },
            )
            .await;
        match tick {
            Ok(_) => {}
            // The Cell advanced between the scan and the dispatch; the next
            // pass rescans it instead of forcing a second attempt now.
            Err(InvocationError::Rejected(_) | InvocationError::Pending(_)) => {
                return Ok(DeliveryOutcome::Skipped);
            }
            Err(InvocationError::NotStarted(error)) => return Err(error),
            Err(InvocationError::InvalidPublishedResult { source, .. }) => return Err(*source),
        }

        let namespace = target.namespace();
        if self.registry.has_activity_runner(namespace) {
            let blocking = if self.registry.requires_blocking_activity(namespace) {
                self.blocking.try_reserve()?
            } else {
                None
            };
            if let Err(error) = self
                .registry
                .run_activity_once(self.client.clone(), &target, self.config.lease_ms, blocking)
                .await
            {
                tracing::warn!(error = %error, "Cell activity pass was not resolved");
            }
        }
        if self.registry.has_queue_consumer(namespace)
            && let Err(error) = self
                .registry
                .run_queue_consumer_once(self.client.clone(), &target, self.config.lease_ms)
                .await
        {
            tracing::warn!(error = %error, "Cell queue consumer pass was not resolved");
        }
        if self.registry.has_effect_runner(namespace)
            && let Err(error) = self
                .registry
                .run_effect_once(
                    self.client.clone(),
                    target.clone(),
                    self.peer.clone(),
                    self.config.lease_ms,
                )
                .await
        {
            tracing::warn!(error = %error, "Cell effect pass was not resolved");
        }
        Ok(DeliveryOutcome::Delivered)
    }
}

async fn join_delivery(deliveries: &mut JoinSet<Result<()>>) -> Result<()> {
    match deliveries.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(_)) => Err(Error::Control("Cell delivery task did not finish")),
        None => Ok(()),
    }
}

fn system_time_ms() -> Result<i64> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Command("system clock is before Unix epoch"))?
            .as_millis(),
    )
    .map_err(|_| Error::Command("system clock exceeds i64 milliseconds"))
}

fn delivery_identity() -> Result<MutationIdentity> {
    let issued_at_ms = system_time_ms()?;
    let expires_at_ms = issued_at_ms
        .checked_add(IDENTITY_LIFETIME_MS)
        .ok_or(Error::Command("Cell delivery identity expiry overflow"))?;
    let mut request_id = [0; 16];
    rand::rng().fill_bytes(&mut request_id);
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(request_id),
        issued_at_ms,
        expires_at_ms,
    })
}
