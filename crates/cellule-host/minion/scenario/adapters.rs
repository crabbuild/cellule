use super::*;
use cellule_host::fleet::*;
use cellule_runtime::fleet::operations::*;

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
    pub boots: Vec<startup::BootOwner>,
    pub records: Arc<HashMap<CellId, Record>>,
    pub capture_sequence: std::sync::atomic::AtomicU64,
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
            let (node, boot) = if effect.is_source_release() {
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
            if self.lose_release_replies && effect.is_source_release() {
                if !completion.committed
                    || !matches!(completion.outcome.outcome, FleetOutcome::Released(_))
                {
                    return Err(std::io::Error::other(format!(
                        "release failed before injected reply loss: {completion:?}"
                    ))
                    .into());
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
        roster: &'a FleetRoster,
        _: i64,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(super::observation::observe(self, roster, deadline))
    }
}
