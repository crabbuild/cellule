use super::*;

impl Db<'_> {
    fn accepted(
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
    fn basis(&self, accepted: &AcceptedFleetAction, kind: u8) -> JournalResult<Option<Vec<u8>>> {
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
                    let receiver = self.required_intent(spec.destination_node)?;
                    if receiver.session() != spec.destination || receiver.mode() != NodeMode::Active
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
        Box::pin(self.run(move |db| {
            db.check_scope(action.scope())?;
            let key = action.key()?;
            if let Some((accepted, result)) = db.accepted(key, node, session)? {
                accepted.validate_replay(&action, node, session)?;
                return Ok(FleetActionAcceptance::Existing { accepted, result: result.map(Box::new) });
            }
            let accepted = AcceptedFleetAction::new(action.clone(), db.snapshot()?.head(), node, session, now_ms)?;
            db.check_intents(&action, node, session)?;
            let (operation, sequence, effect) = match action.kind() {
                FleetActionKind::Movement { action, attempt } => (attempt.spec().id.operation, attempt.spec().id.sequence.to_be_bytes().to_vec(), *action as u8),
                FleetActionKind::Maintenance { action, operation } => (operation.id(), vec![], *action as u8),
            };
            db.tx.execute("INSERT INTO actions(key,node,session,operation,sequence,effect,accepted) VALUES (?1,?2,?3,?4,?5,?6,?7)", params![key.as_bytes().as_slice(), node.as_bytes().as_slice(), session.as_bytes().as_slice(), operation.as_bytes().as_slice(), sequence, effect, accepted.to_bytes()?])?;
            Ok(FleetActionAcceptance::New(accepted))
        }))
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
            if let FleetActionKind::Movement {
                action: MovementAction::Activate,
                ..
            } = accepted.action().kind()
                && matches!(result.outcome, FleetOutcome::Activated(_))
            {
                let basis = AcquisitionBasis::from_bytes(
                    &db.basis(&accepted, 1)?.ok_or(OperationError::NotFound)?,
                )?;
                basis.validate_result(&result)?;
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
                if !matches!(previous.outcome, FleetOutcome::Unknown) {
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
    fn record_acquisition_basis<'a>(
        &'a self,
        basis: &'a AcquisitionBasis,
    ) -> FleetAdapterFuture<'a, AcquisitionBasis> {
        let basis = basis.clone();
        Box::pin(self.run(move |db| {
            let bytes = basis.to_bytes()?;
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
}
