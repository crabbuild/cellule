use super::*;
use cellule_host::fleet::*;
use cellule_runtime::cell::actor::CellInventoryEntry;
use cellule_runtime::fleet::operations::*;
use cellule_runtime::node::{
    NodeAdvertisement, NodeCapacity, NodeFailureDomain, NodePlacementCapacity,
};
use ed25519_dalek::SigningKey;

pub(super) struct Cells {
    pub records: Arc<HashMap<CellId, Record>>,
    pub local: usize,
    pub root: PathBuf,
}
impl FleetCellProvider for Cells {
    fn cell_inputs<'a>(
        &'a self,
        spec: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetCellInputs> {
        Box::pin(async move {
            let record = self
                .records
                .get(&spec.target.cell_id())
                .ok_or_else(|| invalid("unknown example Cell"))?;
            if record.target != spec.target || record.incarnation != spec.incarnation {
                return Err(invalid("example Cell binding mismatch"));
            }
            Ok(FleetCellInputs {
                catalog: record.catalog.clone(),
                replica: record.replica.clone(),
                authority: record.authority.clone(),
                destination: self.root.join(format!(
                    "node-{}-{:?}.sqlite",
                    self.local,
                    spec.target.cell_id()
                )),
                owner: owner(self.local),
            })
        })
    }
    fn recovery_inputs<'a>(
        &'a self,
        _: &'a MoveAttemptSpec,
    ) -> FleetAdapterFuture<'a, FleetRecoveryInputs> {
        Box::pin(async {
            Err(invalid(
                "this clean-movement scenario has no failed-session recovery proof",
            ))
        })
    }
}

/// Fixed three-boot in-process transport. Trusted composition pins identities;
/// production endpoints must provide equivalent authentication independently.
pub(super) struct LocalFleet {
    pub nodes: Vec<Arc<CellNode>>,
    pub journal: Arc<SqliteJournal>,
    pub lose_release_replies: bool,
    pub lost_release_replies: std::sync::atomic::AtomicUsize,
    pub expired_receiver_cleanups: std::sync::atomic::AtomicUsize,
}
impl LocalFleet {
    fn endpoint(&self, physical: NodeId, boot: SessionId) -> JournalResult<&Arc<CellNode>> {
        let index = (0..3)
            .find(|n| node_id(*n) == physical && session(*n) == boot)
            .ok_or_else(|| invalid("unrecognized example boot endpoint"))?;
        self.nodes
            .get(index)
            .ok_or_else(|| invalid("missing example node"))
    }
}
impl FleetTransport for LocalFleet {
    fn dispatch<'a>(
        &'a self,
        action: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            let FleetActionKind::Movement {
                action: effect,
                attempt,
            } = action.kind()
            else {
                return Err(invalid("non-movement example action"));
            };
            let spec = attempt.spec();
            let (node, boot) = if *effect == MovementAction::Release {
                (spec.source_node, spec.source)
            } else {
                (spec.destination_node, spec.destination)
            };
            let completion = self
                .endpoint(node, boot)?
                .apply_fleet_action(action.clone(), clock()?)
                .await
                .map_err(|error| Box::new(error) as super::JournalError)?;
            let now = clock()?;
            if *effect == MovementAction::Cancel
                && attempt.phase() == AttemptPhase::CleaningReceiver
                && attempt
                    .reservation()
                    .is_some_and(|r| r.expires_at_ms <= now)
                && completion.committed
                && matches!(completion.outcome.outcome, FleetOutcome::ReceiverCleaned)
            {
                self.expired_receiver_cleanups
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if self.lose_release_replies && *effect == MovementAction::Release {
                if !completion.committed
                    || !matches!(completion.outcome.outcome, FleetOutcome::Released(_))
                {
                    return Err(invalid("release failed before injected reply loss"));
                }
                self.lost_release_replies
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(invalid("injected loss after committed source release"));
            }
            Ok(completion)
        })
    }
    fn inspect<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        Box::pin(async move {
            self.endpoint(request.node(), request.session())?
                .inspect_fleet_action(request.clone())
                .await
                .map_err(|error| Box::new(error) as super::JournalError)
        })
    }
}
impl FleetObserver for LocalFleet {
    fn observe<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            if self.journal.load_snapshot(scope()).await? != *expected {
                return Err(OperationError::Conflict.into());
            }
            let started = clock()?;
            let mut nodes = Vec::new();
            let mut cells = Vec::new();
            for (index, node) in self.nodes.iter().enumerate() {
                let page = node.runtime().fleet_cells_page(None, 128).await?;
                if page.session() != session(index) || page.next().is_some() {
                    return Err(invalid(
                        "example inventory exceeded its fixed fixture bound",
                    ));
                }
                for entry in page.entries() {
                    if let CellInventoryEntry::Owned(row) = entry {
                        cells.push(FleetOwnedCell {
                            node: node_id(index),
                            session: session(index),
                            observation: (**row).clone(),
                        });
                    }
                }
                let stats = node.stats();
                let memory = u64::try_from(
                    stats.resident_capacity_bytes() + stats.retained_capacity_bytes(),
                )?;
                let used = u64::try_from(stats.resident_bytes() + stats.retained_bytes())?;
                let sample = node
                    .runtime()
                    .operational_sample()?
                    .ok_or_else(|| invalid("local classifier has not sampled yet"))?;
                let key = SigningKey::from_bytes(&[index as u8 + 1; 32]);
                let now = clock()?;
                let ad = NodeAdvertisement::sign(
                    node_id(index),
                    session(index),
                    owner(index).endpoint,
                    scope().fleet,
                    Digest::from_bytes([30; 32]),
                    Digest::from_bytes([31; 32]),
                    Digest::from_bytes([32; 32]),
                    &key,
                    1,
                    now,
                    now + 30_000,
                    node.application().registry().module_digests(),
                    vec![1],
                    NodeFailureDomain::default(),
                    NodeCapacity {
                        free_memory_bytes: memory.saturating_sub(used),
                        free_disk_bytes: stats
                            .local_disk_capacity_bytes()
                            .saturating_sub(stats.local_disk_reserved_bytes()),
                        job_credits: stats
                            .placement_job_capacity()
                            .saturating_sub(stats.placement_running_jobs()),
                        log_protocol: 1,
                        ..NodeCapacity::default()
                    },
                )?
                .with_operational_placement(
                    NodePlacementCapacity {
                        memory_capacity_bytes: memory,
                        disk_capacity_bytes: stats.local_disk_capacity_bytes(),
                        active_cells: stats.placement_active_cells(),
                        max_active_cells: stats.placement_active_cell_capacity(),
                        running_jobs: stats.placement_running_jobs(),
                        job_capacity: stats.placement_job_capacity(),
                        ..NodePlacementCapacity::default()
                    },
                    sample,
                    &key,
                )?;
                nodes.push(ad);
            }
            if self.journal.load_snapshot(scope()).await? != *expected {
                return Err(OperationError::Conflict.into());
            }
            // This finite fixture authenticates all three boots, but does not
            // implement production role/enrollment coverage. Count balancing
            // remains disabled; measured pressure can authorize relief.
            Ok(FleetObservation::new(
                scope(),
                expected.registry(),
                1,
                started,
                clock()?,
                false,
                nodes,
                cells,
            )?)
        })
    }
}
