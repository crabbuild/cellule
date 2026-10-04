use super::*;
use cellule_host::fleet::FleetReaderEvacuationVerifier;
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
    pub reader_verifier: Option<FleetReaderEvacuationVerifier>,
    pub capture_sequence: std::sync::atomic::AtomicU64,
    pub lose_release_replies: bool,
    pub lost_release_replies: std::sync::atomic::AtomicUsize,
    pub drop_closed_finalize_replies: std::sync::atomic::AtomicUsize,
    pub expired_receiver_cleanups: std::sync::atomic::AtomicUsize,
}

pub(super) struct LocalSnapshots {
    nodes: Vec<Arc<CellNode>>,
}
impl LocalSnapshots {
    pub(super) fn new(nodes: Vec<Arc<CellNode>>) -> Self {
        Self { nodes }
    }
}
impl FleetSnapshotTransport for LocalSnapshots {
    fn capture<'a>(
        &'a self,
        request: &'a FleetSnapshotRequest,
        deadline: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetNodeSnapshot>> {
        Box::pin(async move {
            if Instant::now() >= deadline {
                return Err(Box::new(cellule_runtime::Error::Deadline) as JournalError);
            }
            let index = (0..self.nodes.len())
                .find(|index| {
                    node_id(*index) == request.node() && session(*index) == request.session()
                })
                .ok_or_else(|| Box::new(cellule_runtime::Error::Fenced) as JournalError)?;
            self.nodes[index]
                .fleet_snapshot(request.clone())
                .await
                .map_err(|error| Box::new(error) as JournalError)
        })
    }
}

impl LocalFleet {
    fn endpoint(&self, physical: NodeId, boot: SessionId) -> JournalResult<&Arc<CellNode>> {
        let index = (0..self.nodes.len())
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
            let (node, boot, source_release) = match action.kind() {
                FleetActionKind::Movement {
                    action: effect,
                    attempt,
                } => {
                    let spec = attempt.spec();
                    let (node, boot) = if effect.is_source_release() {
                        (spec.source_node, spec.source)
                    } else {
                        action
                            .receiver_endpoint()
                            .ok_or_else(|| invalid("receiver action has no endpoint"))?
                    };
                    (node, boot, effect.is_source_release())
                }
                FleetActionKind::Maintenance { operation, .. } => {
                    (operation.node(), operation.session(), false)
                }
            };
            let completion = self
                .endpoint(node, boot)?
                .apply_fleet_action(action.clone(), clock()?)
                .await
                .map_err(|error| Box::new(error) as super::JournalError)?;
            let now = clock()?;
            let expired_receiver_cleanup = match action.kind() {
                FleetActionKind::Movement {
                    action: MovementAction::Cancel,
                    attempt,
                } => {
                    attempt.phase() == AttemptPhase::CleaningReceiver
                        && attempt
                            .reservation()
                            .is_some_and(|reservation| reservation.expires_at_ms <= now)
                }
                _ => false,
            };
            if expired_receiver_cleanup
                && completion.committed
                && matches!(completion.outcome.outcome, FleetOutcome::ReceiverCleaned)
            {
                self.expired_receiver_cleanups
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if self.lose_release_replies && source_release {
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
    fn settle_roles<'a>(
        &'a self,
        action: &'a FleetAction,
        settlement: &'a FleetRoleSettlement,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            let FleetActionKind::Maintenance { operation, .. } = action.kind() else {
                return Err(invalid("role settlement requires a maintenance action"));
            };
            self.endpoint(operation.node(), operation.session())?
                .apply_fleet_role_settlement(action.clone(), settlement.clone(), clock()?)
                .await
                .map_err(|error| Box::new(error) as super::JournalError)
        })
    }
    fn settle_roles_after_process_closure<'a>(
        &'a self,
        action: &'a FleetAction,
        settlement: &'a FleetRoleSettlement,
        closure: &'a FleetFailedBootClosure,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            settlement.validate_failed_boot_closure(action, closure)?;
            let (node, session) = match action.kind() {
                FleetActionKind::Maintenance { operation, .. } => {
                    (operation.node(), operation.session())
                }
                FleetActionKind::Movement { .. } => {
                    return Err(invalid("closed-boot settlement requires maintenance"));
                }
            };
            let now = clock()?;
            if now < action.issued_at_ms() {
                return Err(invalid("closed-boot settlement clock regressed"));
            }
            let (accepted, previous) = match self
                .journal
                .accept_action(action, node, session, now)
                .await?
            {
                FleetActionAcceptance::New(accepted) => (accepted, None),
                FleetActionAcceptance::Existing { accepted, result } => (accepted, result),
            };
            accepted.validate_replay(action, node, session)?;
            let outcome = FleetActionOutcome {
                scope: action.scope(),
                action_key: action.key()?,
                node,
                session,
                observed_at_ms: now,
                outcome: FleetOutcome::RolesSettledAt {
                    inventory: settlement.inventory(),
                    head_revision: settlement.head_revision(),
                    registry: settlement.registry(),
                },
            };
            accepted.validate_result(&outcome)?;
            if let Some(previous) = previous {
                let previous = *previous;
                accepted.validate_result(&previous)?;
                match &previous.outcome {
                    FleetOutcome::Unknown => {}
                    FleetOutcome::RolesSettledAt {
                        inventory,
                        head_revision,
                        registry,
                    } if *inventory == settlement.inventory()
                        && *head_revision == settlement.head_revision()
                        && *registry == settlement.registry() =>
                    {
                        return Ok(Arc::new(FleetActionCompletion {
                            accepted,
                            outcome: previous,
                            committed: true,
                            execution_error: None,
                            journal_error: None,
                        }));
                    }
                    _ => return Err(invalid("closed-boot role-settlement result differs")),
                }
            }
            self.journal
                .publish_action_result(&accepted, &outcome)
                .await?;
            Ok(Arc::new(FleetActionCompletion {
                accepted,
                outcome,
                committed: true,
                execution_error: None,
                journal_error: None,
            }))
        })
    }
    fn finalize_after_process_closure<'a>(
        &'a self,
        action: &'a FleetAction,
        closure: &'a FleetFailedBootClosure,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async move {
            let operation = match action.kind() {
                FleetActionKind::Maintenance {
                    action: MaintenanceAction::Finalize,
                    operation,
                } => operation,
                _ => return Err(invalid("closed-boot finalization requires Finalize")),
            };
            let target = closure.boot().spec().target;
            let mut evidence = operation
                .drain_evidence()
                .ok_or_else(|| invalid("closed-boot finalization lacks drain evidence"))?;
            if operation.phase() != MaintenancePhase::Closing
                || evidence.remaining_cells != 0
                || evidence.unresolved_attempts != 0
                || !evidence.relocated
                || !evidence.readers_settled
                || !evidence.followers_settled
                || evidence.facilities_closed
                || evidence.stopped
                || evidence.withdrawn
                || closure.snapshot().head().scope() != action.scope()
                || closure.snapshot().head().maintenance() != Some(operation.as_ref())
                || closure.snapshot().head().revision() != action.journal_revision()
                || closure.boot().status() != EnrollmentStatus::Retired
                || !matches!(closure.boot().spec().role, EnrollmentRole::Node { .. })
                || target.node != operation.node()
                || target.session != operation.session()
                || closure.canonical().node() != operation.node()
                || closure.canonical().session() != operation.session()
                || closure.interval().1 > action.issued_at_ms()
            {
                return Err(invalid("closed-boot finalization evidence differs"));
            }
            let now = clock()?;
            action.authorize_against(closure.snapshot().head(), now)?;
            evidence.facilities_closed = true;
            evidence.stopped = true;
            evidence.withdrawn = true;
            let (node, session) = (operation.node(), operation.session());
            let (accepted, previous) = match self
                .journal
                .accept_action(action, node, session, now)
                .await?
            {
                FleetActionAcceptance::New(accepted) => (accepted, None),
                FleetActionAcceptance::Existing { accepted, result } => (accepted, result),
            };
            accepted.validate_replay(action, node, session)?;
            let outcome = FleetActionOutcome {
                scope: action.scope(),
                action_key: action.key()?,
                node,
                session,
                observed_at_ms: now,
                outcome: FleetOutcome::Stopped(evidence),
            };
            accepted.validate_result(&outcome)?;
            if let Some(previous) = previous {
                let previous = *previous;
                accepted.validate_result(&previous)?;
                match &previous.outcome {
                    FleetOutcome::Unknown => {}
                    FleetOutcome::Stopped(previous_evidence) if previous_evidence == &evidence => {
                        return Ok(Arc::new(FleetActionCompletion {
                            accepted,
                            outcome: previous,
                            committed: true,
                            execution_error: None,
                            journal_error: None,
                        }));
                    }
                    _ => return Err(invalid("closed-boot finalization result differs")),
                }
            }
            self.journal
                .publish_action_result(&accepted, &outcome)
                .await?;
            if self
                .drop_closed_finalize_replies
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(invalid(
                    "injected loss after committed closed-boot finalization",
                ));
            }
            Ok(Arc::new(FleetActionCompletion {
                accepted,
                outcome,
                committed: true,
                execution_error: None,
                journal_error: None,
            }))
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
