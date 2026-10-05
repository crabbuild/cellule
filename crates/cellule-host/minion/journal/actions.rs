use super::*;

#[derive(Clone)]
struct ClosedReceiverProof {
    snapshot: FleetJournalSnapshot,
    node: NodeId,
    session: SessionId,
    digest: Digest,
    retired: bool,
}

impl Db<'_> {
    pub(super) fn accepted(
        &self,
        key: Digest,
        node: NodeId,
        session: SessionId,
    ) -> JournalResult<Option<(AcceptedFleetAction, Option<FleetActionOutcome>)>> {
        let row = self
            .tx
            .query_row(
                "SELECT accepted,result FROM actions WHERE key=?1 AND node=?2 AND session=?3",
                params![
                    key.as_bytes().as_slice(),
                    node.as_bytes().as_slice(),
                    session.as_bytes().as_slice()
                ],
                |row| {
                    let result = if matches!(row.get_ref(1)?, rusqlite::types::ValueRef::Null) {
                        None
                    } else {
                        Some(blob(row, 1, MAX_RECORD_BYTES)?)
                    };
                    Ok((blob(row, 0, MAX_RECORD_BYTES)?, result))
                },
            )
            .optional()?;
        row.map(|(accepted, result)| {
            let accepted = AcceptedFleetAction::from_bytes(&accepted)?;
            self.check_scope(accepted.action().scope())?;
            if accepted.action().key()? != key
                || accepted.node() != node
                || accepted.session() != session
            {
                return Err(OperationError::Conflict.into());
            }
            let result = result
                .map(|bytes| FleetActionOutcome::from_bytes(&bytes))
                .transpose()?;
            if let Some(result) = &result {
                accepted.validate_result(result)?;
            }
            Ok((accepted, result))
        })
        .transpose()
    }

    fn check_receiver_route(&self, action: &FleetAction) -> JournalResult<()> {
        let Some(route) = action.receiver_route() else {
            return Ok(());
        };
        let FleetActionKind::Movement { attempt, .. } = action.kind() else {
            return Err(OperationError::Conflict.into());
        };
        let spec = attempt.spec();
        let rows = {
            let mut statement = self.tx.prepare(
                "SELECT key,node,session FROM actions WHERE operation=?1 AND sequence=?2 AND length(sequence)=8 LIMIT 65",
            )?;
            statement
                .query_map(
                    params![
                        spec.id.operation.as_bytes().as_slice(),
                        spec.id.sequence.to_be_bytes().as_slice()
                    ],
                    |row| Ok((blob(row, 0, 32)?, blob(row, 1, 16)?, blob(row, 2, 16)?)),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        if rows.len() > 64 {
            return Err(OperationError::Conflict.into());
        }
        let mut latest: Option<ReceiverRoute> = None;
        for (key, node, session) in rows {
            let key = Digest::from_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| OperationError::Invalid("action index width"))?,
            );
            let node = NodeId::from_bytes(
                node.as_slice()
                    .try_into()
                    .map_err(|_| OperationError::Invalid("node identity width"))?,
            );
            let session = SessionId::from_bytes(
                session
                    .as_slice()
                    .try_into()
                    .map_err(|_| OperationError::Invalid("session identity width"))?,
            );
            let (accepted, _) = self
                .accepted(key, node, session)?
                .ok_or(OperationError::NotFound)?;
            let FleetActionKind::Movement {
                attempt: recorded, ..
            } = accepted.action().kind()
            else {
                return Err(OperationError::Conflict.into());
            };
            if accepted.action().scope() != action.scope()
                || recorded.spec() != spec
                || accepted.node() != node
                || accepted.session() != session
            {
                return Err(OperationError::Conflict.into());
            }
            let Some(recorded_route) = accepted.action().receiver_route() else {
                continue;
            };
            if let Some(previous) = &latest {
                if recorded_route.hop_count() == previous.hop_count() {
                    if recorded_route != previous {
                        return Err(OperationError::Conflict.into());
                    }
                } else if recorded_route.hop_count() > previous.hop_count() {
                    if !recorded_route.follows(previous) {
                        return Err(OperationError::Conflict.into());
                    }
                    latest = Some(recorded_route.clone());
                }
            } else {
                latest = Some(recorded_route.clone());
            }
        }
        match latest {
            Some(previous) if route.follows(&previous) => Ok(()),
            None if route.hop_count() == 1 => Ok(()),
            _ => Err(OperationError::Conflict.into()),
        }
    }

    fn accept_action(
        &self,
        action: FleetAction,
        node: NodeId,
        session: SessionId,
        now_ms: i64,
        closed: Option<ClosedReceiverProof>,
    ) -> JournalResult<FleetActionAcceptance> {
        self.check_scope(action.scope())?;
        let key = action.key()?;
        if let Some((accepted, result)) = self.accepted(key, node, session)? {
            accepted.validate_replay(&action, node, session)?;
            return Ok(FleetActionAcceptance::Existing {
                accepted,
                result: result.map(Box::new),
            });
        }
        let snapshot = self.snapshot()?;
        match (action.receiver_route(), closed) {
            (Some(route), Some(closed)) => {
                let hop = route
                    .latest_handoff()
                    .ok_or(OperationError::Invalid("receiver route lacks final hop"))?;
                if !closed.retired
                    || hop.previous() != (closed.node, closed.session)
                    || hop.process_closure() != closed.digest
                    || hop.registry() != snapshot.registry()
                    || closed.snapshot.head() != snapshot.head()
                    || closed.snapshot.registry() != snapshot.registry()
                    || action.receiver_endpoint() != Some((node, session))
                {
                    return Err(OperationError::Conflict.into());
                }
            }
            (Some(_), None) => {
                return Err(OperationError::Fenced.into());
            }
            (None, Some(_)) => {
                return Err(OperationError::Invalid(
                    "closed receiver proof supplied for an ordinary action",
                )
                .into());
            }
            (None, None) => {}
        }
        let accepted = AcceptedFleetAction::new_with_registry(
            action.clone(),
            snapshot.head(),
            snapshot.registry(),
            node,
            session,
            now_ms,
        )?;
        self.check_intents(&action, node, session)?;
        self.check_receiver_route(&action)?;
        let (operation, sequence, effect) = match action.kind() {
            FleetActionKind::Movement { action, attempt } => (
                attempt.spec().id.operation,
                attempt.spec().id.sequence.to_be_bytes().to_vec(),
                *action as u8,
            ),
            FleetActionKind::Maintenance { action, operation } => {
                (operation.id(), vec![], *action as u8)
            }
        };
        self.tx.execute(
            "INSERT INTO actions(key,node,session,operation,sequence,effect,accepted) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                key.as_bytes().as_slice(),
                node.as_bytes().as_slice(),
                session.as_bytes().as_slice(),
                operation.as_bytes().as_slice(),
                sequence,
                effect,
                accepted.to_bytes()?
            ],
        )?;
        Ok(FleetActionAcceptance::New(accepted))
    }

    fn original(&self, accepted: &AcceptedFleetAction) -> JournalResult<()> {
        let (original, _) = self
            .accepted(
                accepted.action().key()?,
                accepted.node(),
                accepted.session(),
            )?
            .ok_or(OperationError::NotFound)?;
        if original != *accepted {
            return Err(OperationError::Conflict.into());
        }
        Ok(())
    }
    pub(super) fn basis(
        &self,
        accepted: &AcceptedFleetAction,
        kind: u8,
    ) -> JournalResult<Option<Vec<u8>>> {
        self.original(accepted)?;
        Ok(self
            .tx
            .query_row(
                "SELECT body FROM bases WHERE key=?1 AND node=?2 AND session=?3 AND kind=?4",
                params![
                    accepted.action().key()?.as_bytes().as_slice(),
                    accepted.node().as_bytes().as_slice(),
                    accepted.session().as_bytes().as_slice(),
                    kind
                ],
                |row| blob(row, 0, MAX_RECORD_BYTES),
            )
            .optional()?)
    }
    fn write_basis(
        &self,
        accepted: &AcceptedFleetAction,
        kind: u8,
        bytes: Vec<u8>,
    ) -> JournalResult<()> {
        self.original(accepted)?;
        self.tx.execute(
            "INSERT INTO bases(key,node,session,kind,body) VALUES (?1,?2,?3,?4,?5)",
            params![
                accepted.action().key()?.as_bytes().as_slice(),
                accepted.node().as_bytes().as_slice(),
                accepted.session().as_bytes().as_slice(),
                kind,
                bytes
            ],
        )?;
        Ok(())
    }
    fn check_intents(
        &self,
        action: &FleetAction,
        node: NodeId,
        session: SessionId,
    ) -> JournalResult<()> {
        let local = self.required_intent(node)?;
        if local.session() != session {
            return Err(OperationError::Conflict.into());
        }
        match action.kind() {
            FleetActionKind::Maintenance { operation, .. } => {
                if local.operation() != Some(operation.id())
                    || local.revision() != operation.intent_revision()
                    || local.mode() == NodeMode::Active
                {
                    return Err(OperationError::Conflict.into());
                }
            }
            FleetActionKind::Movement {
                action: effect,
                attempt,
            } => {
                let spec = attempt.spec();
                if matches!(
                    effect,
                    MovementAction::Prepare
                        | MovementAction::Release
                        | MovementAction::ReleaseMaintenance
                        | MovementAction::Activate
                        | MovementAction::Recover
                ) {
                    let (receiver_node, receiver_session) =
                        action.receiver_endpoint().ok_or(OperationError::Conflict)?;
                    let receiver = self.required_intent(receiver_node)?;
                    if receiver.session() != receiver_session || receiver.mode() != NodeMode::Active
                    {
                        return Err(OperationError::Conflict.into());
                    }
                }
                if matches!(
                    effect,
                    MovementAction::Prepare
                        | MovementAction::Release
                        | MovementAction::ReleaseMaintenance
                ) {
                    let source = self.required_intent(spec.source_node)?;
                    if source.session() != spec.source {
                        return Err(OperationError::Conflict.into());
                    }
                    if source.mode() != NodeMode::Active
                        && self
                            .snapshot()?
                            .head()
                            .maintenance()
                            .is_none_or(|operation| {
                                source.operation() != Some(operation.id())
                                    || source.revision() != operation.intent_revision()
                                    || source.session() != operation.session()
                                    || spec.id.operation != operation.id()
                                    || operation.phase() != MaintenancePhase::Evacuating
                            })
                    {
                        return Err(OperationError::Conflict.into());
                    }
                }
            }
        }
        Ok(())
    }
}

impl FleetActionJournal for SqliteJournal {
    fn authorize_snapshot<'a>(
        &'a self,
        request: &'a FleetSnapshotRequest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ()> {
        let request = request.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(request.expected().head().scope())?;
            let snapshot = db.snapshot()?;
            let intent = db.required_intent(request.node())?;
            let authorization = request.authorize_against(&snapshot, &intent, now_ms);
            #[cfg(test)]
            if matches!(authorization, Err(OperationError::Conflict)) {
                eprintln!(
                    "[DEBUG-fleet-57] snapshot conflict node={:?} subject={:?} head_equal={} registry_equal={} expected_head={} current_head={} expected_registry={} current_registry={} intent_scope_equal={} intent_node_equal={} intent_session_equal={}",
                    request.node(),
                    request.subject(),
                    snapshot.head() == request.expected().head(),
                    snapshot.registry() == request.expected().registry(),
                    request.expected().head().revision(),
                    snapshot.head().revision(),
                    request.expected().registry().revision(),
                    snapshot.registry().revision(),
                    intent.scope() == snapshot.head().scope(),
                    intent.node() == request.node(),
                    intent.session() == request.session(),
                );
            }
            authorization?;
            Ok(())
        }))
    }

    fn authorize_inspection<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ()> {
        let request = request.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(request.action().scope())?;
            let snapshot = db.snapshot()?;
            request.authorize_against(snapshot.head(), snapshot.registry(), now_ms)?;
            db.check_intents(request.action(), request.node(), request.session())
        }))
    }

    fn accept_action<'a>(
        &'a self,
        action: &'a FleetAction,
        node: NodeId,
        session: SessionId,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FleetActionAcceptance> {
        let action = action.clone();
        Box::pin(self.run(move |db| db.accept_action(action, node, session, now_ms, None)))
    }

    fn accept_closed_receiver_action<'a>(
        &'a self,
        action: &'a FleetAction,
        node: NodeId,
        session: SessionId,
        now_ms: i64,
        closure: &'a FleetFailedBootClosure,
    ) -> FleetAdapterFuture<'a, FleetActionAcceptance> {
        let action = action.clone();
        let closed = ClosedReceiverProof {
            snapshot: closure.snapshot().clone(),
            node: closure.canonical().node(),
            session: closure.canonical().session(),
            digest: closure.digest(),
            retired: closure.boot().status() == EnrollmentStatus::Retired,
        };
        Box::pin(self.run(move |db| db.accept_action(action, node, session, now_ms, Some(closed))))
    }
    fn publish_action_result<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        result: &'a FleetActionOutcome,
    ) -> FleetAdapterFuture<'a, ()> {
        let accepted = accepted.clone();
        let result = result.clone();
        Box::pin(self.run(move |db| {
            db.original(&accepted)?;
            accepted.validate_result(&result)?;
            if let FleetOutcome::RolesSettledAt {
                registry,
                head_revision,
                ..
            } = &result.outcome
            {
                let current = db.snapshot()?;
                if current.registry() != *registry || current.head().revision() != *head_revision {
                    return Err(OperationError::Conflict.into());
                }
            }
            if let FleetActionKind::Movement {
                action: MovementAction::Activate,
                ..
            } = accepted.action().kind()
                && matches!(result.outcome, FleetOutcome::Activated(_))
            {
                if let Some(bytes) = db.basis(&accepted, 1)? {
                    if db.receiver_basis(&accepted, 1)?.is_some() {
                        return Err(OperationError::Conflict.into());
                    }
                    AcquisitionBasis::from_bytes(&bytes)?.validate_result(&result)?;
                } else {
                    let evidence = ReceiverRecoveryEvidence::from_bytes(
                        &db.receiver_basis(&accepted, 2)?
                            .ok_or(OperationError::NotFound)?,
                    )?;
                    let basis = ReceiverRecoveryBasis::from_bytes(
                        &db.receiver_basis(&accepted, 1)?
                            .ok_or(OperationError::NotFound)?,
                    )?;
                    if basis.accepted() != &accepted || evidence.basis() != &basis {
                        return Err(OperationError::Conflict.into());
                    }
                    evidence.validate_result(&result)?;
                }
            }
            if let FleetOutcome::Recovered(recovered) = &result.outcome
                && let FleetActionKind::Movement {
                    action: MovementAction::Recover,
                    ..
                } = accepted.action().kind()
            {
                let evidence = RecoveryEvidence::from_bytes(
                    &db.basis(&accepted, 3)?.ok_or(OperationError::NotFound)?,
                )?;
                if evidence != recovered.recovery {
                    return Err(OperationError::Conflict.into());
                }
            }
            let (_, previous) = db
                .accepted(
                    accepted.action().key()?,
                    accepted.node(),
                    accepted.session(),
                )?
                .ok_or(OperationError::NotFound)?;
            if let Some(previous) = previous {
                if previous == result {
                    return Ok(());
                }
                let refreshable_roles = matches!(
                    accepted.action().kind(),
                    FleetActionKind::Maintenance {
                        action: MaintenanceAction::SettleRoles,
                        ..
                    }
                ) && matches!(
                    (&previous.outcome, &result.outcome),
                    (
                        FleetOutcome::RolesSettled { .. } | FleetOutcome::RolesSettledAt { .. },
                        FleetOutcome::RolesSettledAt { .. }
                    )
                ) && result.observed_at_ms > previous.observed_at_ms
                    && match (&previous.outcome, &result.outcome) {
                        (
                            FleetOutcome::RolesSettledAt {
                                registry: previous,
                                head_revision: previous_head,
                                ..
                            },
                            FleetOutcome::RolesSettledAt {
                                registry: current,
                                head_revision: current_head,
                                ..
                            },
                        ) => {
                            current.revision() >= previous.revision()
                                && current_head >= previous_head
                        }
                        (
                            FleetOutcome::RolesSettled { .. },
                            FleetOutcome::RolesSettledAt { .. },
                        ) => true,
                        _ => false,
                    };
                if !matches!(previous.outcome, FleetOutcome::Unknown) && !refreshable_roles {
                    return Err(OperationError::Conflict.into());
                }
                if matches!(result.outcome, FleetOutcome::Unknown) {
                    return Ok(());
                }
            }
            db.tx.execute(
                "UPDATE actions SET result=?1 WHERE key=?2 AND node=?3 AND session=?4",
                params![
                    result.to_bytes()?,
                    accepted.action().key()?.as_bytes().as_slice(),
                    accepted.node().as_bytes().as_slice(),
                    accepted.session().as_bytes().as_slice()
                ],
            )?;
            Ok(())
        }))
    }
    fn load_movement_action<'a>(
        &'a self,
        scope: FleetScope,
        attempt: AttemptId,
        effect: MovementAction,
        node: NodeId,
        session: SessionId,
    ) -> FleetAdapterFuture<'a, Option<FleetActionAcceptance>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            let key = db.tx.query_row("SELECT key FROM actions WHERE operation=?1 AND sequence=?2 AND effect=?3 AND node=?4 AND session=?5", params![attempt.operation.as_bytes().as_slice(), attempt.sequence.to_be_bytes().as_slice(), effect as u8, node.as_bytes().as_slice(), session.as_bytes().as_slice()], |row| blob(row, 0, 32)).optional()?;
            key.map(|bytes| {
                let key = Digest::from_bytes(bytes.as_slice().try_into().map_err(|_| OperationError::Invalid("action index width"))?);
                let (accepted, result) = db.accepted(key, node, session)?.ok_or(OperationError::NotFound)?;
                match accepted.action().kind() {
                    FleetActionKind::Movement { action, attempt: original } if *action == effect && original.spec().id == attempt => {}
                    _ => return Err(OperationError::Conflict.into()),
                }
                Ok(FleetActionAcceptance::Existing { accepted, result: result.map(Box::new) })
            }).transpose()
        }))
    }

    fn load_movement_actions<'a>(
        &'a self,
        scope: FleetScope,
        attempt: &'a MoveAttempt,
        effect: MovementAction,
    ) -> FleetAdapterFuture<'a, Vec<FleetActionAcceptance>> {
        let attempt = attempt.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            let rows = {
                let mut statement = db.tx.prepare(
                    "SELECT key,node,session FROM actions WHERE operation=?1 AND sequence=?2 AND effect=?3 ORDER BY node,session LIMIT 4",
                )?;
                statement
                    .query_map(
                        params![
                            attempt.spec().id.operation.as_bytes().as_slice(),
                            attempt.spec().id.sequence.to_be_bytes().as_slice(),
                            effect as u8
                        ],
                        |row| {
                            Ok((
                                blob(row, 0, 32)?,
                                blob(row, 1, 16)?,
                                blob(row, 2, 16)?,
                            ))
                        },
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            if rows.len() > MAX_RECEIVER_HANDOFFS + 1 {
                return Err(OperationError::Conflict.into());
            }
            rows.into_iter()
                .map(|(key, node, session)| {
                    let key = Digest::from_bytes(
                        key.as_slice()
                            .try_into()
                            .map_err(|_| OperationError::Invalid("action index width"))?,
                    );
                    let node = NodeId::from_bytes(
                        node.as_slice()
                            .try_into()
                            .map_err(|_| OperationError::Invalid("node identity width"))?,
                    );
                    let session = SessionId::from_bytes(
                        session
                            .as_slice()
                            .try_into()
                            .map_err(|_| OperationError::Invalid("session identity width"))?,
                    );
                    let (accepted, result) = db
                        .accepted(key, node, session)?
                        .ok_or(OperationError::NotFound)?;
                    let FleetActionKind::Movement {
                        action,
                        attempt: original,
                    } = accepted.action().kind()
                    else {
                        return Err(OperationError::Conflict.into());
                    };
                    if accepted.action().scope() != scope
                        || accepted.action().key()? != key
                        || *action != effect
                        || original.spec() != attempt.spec()
                        || accepted.node() != node
                        || accepted.session() != session
                    {
                        return Err(OperationError::Conflict.into());
                    }
                    accepted.validate_replay(accepted.action(), node, session)?;
                    Ok(FleetActionAcceptance::Existing {
                        accepted,
                        result: result.map(Box::new),
                    })
                })
                .collect()
        }))
    }

    fn record_acquisition_basis<'a>(
        &'a self,
        basis: &'a AcquisitionBasis,
    ) -> FleetAdapterFuture<'a, AcquisitionBasis> {
        let basis = basis.clone();
        Box::pin(self.run(move |db| {
            let bytes = basis.to_bytes()?;
            if db.receiver_basis(basis.accepted(), 1)?.is_some() {
                return Err(OperationError::Conflict.into());
            }
            if let Some(bytes) = db.basis(basis.accepted(), 1)? {
                let original = AcquisitionBasis::from_bytes(&bytes)?;
                if original.accepted() != basis.accepted() || original.control() != basis.control()
                {
                    return Err(OperationError::Conflict.into());
                }
                return Ok(original);
            }
            db.write_basis(basis.accepted(), 1, bytes)?;
            Ok(basis)
        }))
    }
    fn load_acquisition_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<AcquisitionBasis>> {
        let accepted = accepted.clone();
        Box::pin(self.run(move |db| {
            db.basis(&accepted, 1)?
                .map(|bytes| {
                    let basis = AcquisitionBasis::from_bytes(&bytes)?;
                    if basis.accepted() != &accepted {
                        return Err(OperationError::Conflict.into());
                    }
                    Ok(basis)
                })
                .transpose()
        }))
    }
    fn record_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        basis: &'a RecoveryBasis,
    ) -> FleetAdapterFuture<'a, RecoveryBasis> {
        let accepted = accepted.clone();
        let basis = basis.clone();
        Box::pin(self.run(move |db| {
            basis.validate_acceptance(&accepted)?;
            let bytes = basis.to_bytes()?;
            if let Some(bytes) = db.basis(&accepted, 2)? {
                let original = RecoveryBasis::from_bytes(&bytes)?;
                original.validate_acceptance(&accepted)?;
                if original.control() != basis.control() {
                    return Err(OperationError::Conflict.into());
                }
                return Ok(original);
            }
            db.write_basis(&accepted, 2, bytes)?;
            Ok(basis)
        }))
    }
    fn load_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<RecoveryBasis>> {
        let accepted = accepted.clone();
        Box::pin(self.run(move |db| {
            db.basis(&accepted, 2)?
                .map(|bytes| {
                    let basis = RecoveryBasis::from_bytes(&bytes)?;
                    basis.validate_acceptance(&accepted)?;
                    Ok(basis)
                })
                .transpose()
        }))
    }
    fn record_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        evidence: &'a RecoveryEvidence,
    ) -> FleetAdapterFuture<'a, RecoveryEvidence> {
        let accepted = accepted.clone();
        let evidence = evidence.clone();
        Box::pin(self.run(move |db| {
            evidence.basis().validate_acceptance(&accepted)?;
            let basis = RecoveryBasis::from_bytes(
                &db.basis(&accepted, 2)?.ok_or(OperationError::NotFound)?,
            )?;
            if &basis != evidence.basis() {
                return Err(OperationError::Conflict.into());
            }
            let bytes = evidence.to_bytes()?;
            if let Some(bytes) = db.basis(&accepted, 3)? {
                let original = RecoveryEvidence::from_bytes(&bytes)?;
                if original.basis() != evidence.basis()
                    || original.restored() != evidence.restored()
                {
                    return Err(OperationError::Conflict.into());
                }
                return Ok(original);
            }
            db.write_basis(&accepted, 3, bytes)?;
            Ok(evidence)
        }))
    }
    fn load_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<RecoveryEvidence>> {
        let accepted = accepted.clone();
        Box::pin(self.run(move |db| {
            db.basis(&accepted, 3)?
                .map(|bytes| {
                    let evidence = RecoveryEvidence::from_bytes(&bytes)?;
                    evidence.basis().validate_acceptance(&accepted)?;
                    Ok(evidence)
                })
                .transpose()
        }))
    }

    fn record_receiver_recovery_basis<'a>(
        &'a self,
        basis: &'a ReceiverRecoveryBasis,
    ) -> FleetAdapterFuture<'a, ReceiverRecoveryBasis> {
        self.receiver_recovery_basis(basis)
    }
    fn load_receiver_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<ReceiverRecoveryBasis>> {
        self.receiver_recovery_input(accepted)
    }
    fn record_receiver_recovery_evidence<'a>(
        &'a self,
        evidence: &'a ReceiverRecoveryEvidence,
    ) -> FleetAdapterFuture<'a, ReceiverRecoveryEvidence> {
        self.receiver_recovery_evidence(evidence)
    }
    fn load_receiver_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<ReceiverRecoveryEvidence>> {
        self.receiver_recovery_result(accepted)
    }
}
