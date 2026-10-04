//! Receiver recovery has its own table; existing source basis kinds are fixed.
use super::*;

impl Db<'_> {
    pub(super) fn receiver_basis(
        &self,
        accepted: &AcceptedFleetAction,
        kind: u8,
    ) -> JournalResult<Option<Vec<u8>>> {
        // Exact original acceptance is required in the same transaction.
        let (original, _) = self
            .accepted(
                accepted.action().key()?,
                accepted.node(),
                accepted.session(),
            )?
            .ok_or(OperationError::NotFound)?;
        if &original != accepted {
            return Err(OperationError::Conflict.into());
        }
        Ok(self.tx.query_row("SELECT body FROM receiver_recoveries WHERE key=?1 AND node=?2 AND session=?3 AND kind=?4", params![accepted.action().key()?.as_bytes().as_slice(), accepted.node().as_bytes().as_slice(), accepted.session().as_bytes().as_slice(), kind], |row| blob(row, 0, MAX_RECORD_BYTES)).optional()?)
    }
    fn write_receiver_basis(
        &self,
        accepted: &AcceptedFleetAction,
        kind: u8,
        bytes: Vec<u8>,
    ) -> JournalResult<()> {
        self.receiver_basis(accepted, kind)?;
        self.tx.execute(
            "INSERT INTO receiver_recoveries(key,node,session,kind,body) VALUES (?1,?2,?3,?4,?5)",
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
}

impl SqliteJournal {
    pub(super) fn receiver_recovery_basis<'a>(
        &'a self,
        basis: &'a ReceiverRecoveryBasis,
    ) -> FleetAdapterFuture<'a, ReceiverRecoveryBasis> {
        let basis = basis.clone();
        Box::pin(async move {
            #[cfg(test)]
            self.receiver_recovery_write_boundary(
                RecoveryWrite::Basis,
                RecoveryWriteBoundary::BeforeCommit,
            )
            .await?;
            let retained = self
                .run(move |db| {
                    let bytes = basis.to_bytes()?;
                    if db.basis(basis.accepted(), 1)?.is_some() {
                        return Err(OperationError::Conflict.into());
                    }
                    if let Some(bytes) = db.receiver_basis(basis.accepted(), 1)? {
                        let original = ReceiverRecoveryBasis::from_bytes(&bytes)?;
                        if original.accepted() != basis.accepted()
                            || original.control() != basis.control()
                        {
                            return Err(OperationError::Conflict.into());
                        }
                        return Ok(original);
                    }
                    db.write_receiver_basis(basis.accepted(), 1, bytes)?;
                    Ok(basis)
                })
                .await?;
            #[cfg(test)]
            self.receiver_recovery_write_boundary(
                RecoveryWrite::Basis,
                RecoveryWriteBoundary::AfterCommit,
            )
            .await?;
            Ok(retained)
        })
    }
    pub(super) fn receiver_recovery_input<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<ReceiverRecoveryBasis>> {
        let accepted = accepted.clone();
        Box::pin(self.run(move |db| {
            db.receiver_basis(&accepted, 1)?
                .map(|bytes| {
                    let basis = ReceiverRecoveryBasis::from_bytes(&bytes)?;
                    if basis.accepted() != &accepted {
                        return Err(OperationError::Conflict.into());
                    }
                    Ok(basis)
                })
                .transpose()
        }))
    }
    pub(super) fn receiver_recovery_evidence<'a>(
        &'a self,
        evidence: &'a ReceiverRecoveryEvidence,
    ) -> FleetAdapterFuture<'a, ReceiverRecoveryEvidence> {
        let evidence = evidence.clone();
        Box::pin(async move {
            #[cfg(test)]
            self.receiver_recovery_write_boundary(
                RecoveryWrite::Evidence,
                RecoveryWriteBoundary::BeforeCommit,
            )
            .await?;
            let retained = self
                .run(move |db| {
                    let accepted = evidence.basis().accepted();
                    let basis = ReceiverRecoveryBasis::from_bytes(
                        &db.receiver_basis(accepted, 1)?
                            .ok_or(OperationError::NotFound)?,
                    )?;
                    if &basis != evidence.basis() {
                        return Err(OperationError::Conflict.into());
                    }
                    let bytes = evidence.to_bytes()?;
                    if let Some(bytes) = db.receiver_basis(accepted, 2)? {
                        let original = ReceiverRecoveryEvidence::from_bytes(&bytes)?;
                        if original.basis() != evidence.basis()
                            || original.restored() != evidence.restored()
                        {
                            return Err(OperationError::Conflict.into());
                        }
                        return Ok(original);
                    }
                    db.write_receiver_basis(accepted, 2, bytes)?;
                    Ok(evidence)
                })
                .await?;
            #[cfg(test)]
            self.receiver_recovery_write_boundary(
                RecoveryWrite::Evidence,
                RecoveryWriteBoundary::AfterCommit,
            )
            .await?;
            Ok(retained)
        })
    }
    pub(super) fn receiver_recovery_result<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<ReceiverRecoveryEvidence>> {
        let accepted = accepted.clone();
        Box::pin(self.run(move |db| {
            db.receiver_basis(&accepted, 2)?
                .map(|bytes| {
                    let evidence = ReceiverRecoveryEvidence::from_bytes(&bytes)?;
                    if evidence.basis().accepted() != &accepted {
                        return Err(OperationError::Conflict.into());
                    }
                    let basis = ReceiverRecoveryBasis::from_bytes(
                        &db.receiver_basis(&accepted, 1)?
                            .ok_or(OperationError::NotFound)?,
                    )?;
                    if &basis != evidence.basis() {
                        return Err(OperationError::Conflict.into());
                    }
                    Ok(evidence)
                })
                .transpose()
        }))
    }
}
