use super::*;

pub(super) fn scope_bytes(scope: FleetScope) -> Vec<u8> {
    [
        scope.fleet.as_bytes().as_slice(),
        scope.application.as_bytes().as_slice(),
    ]
    .concat()
}
pub(super) fn profile_bytes(profile: FleetProfile) -> JournalResult<Vec<u8>> {
    let count = u64::try_from(profile.max_inflight)?;
    Ok([
        count.to_be_bytes(),
        profile.max_restore_bytes.to_be_bytes(),
        profile.controller_lease_ms.to_be_bytes(),
        profile.reconcile_interval_ms.to_be_bytes(),
    ]
    .concat())
}

pub(super) fn blob(row: &rusqlite::Row<'_>, column: usize, max: u32) -> rusqlite::Result<Vec<u8>> {
    let bytes = row.get_ref(column)?.as_blob()?;
    if bytes.len() > max as usize {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Blob,
            Box::new(cellule_runtime::Error::Capacity("reference journal record")),
        ));
    }
    Ok(bytes.to_vec())
}

pub(super) struct Db<'a> {
    pub tx: &'a Transaction<'a>,
    pub scope: FleetScope,
    pub profile: FleetProfile,
}

impl Db<'_> {
    pub fn check_scope(&self, scope: FleetScope) -> JournalResult<()> {
        if scope != self.scope {
            return Err(OperationError::Conflict.into());
        }
        Ok(())
    }
    pub fn movement_time(
        &self,
        expected: &FleetJournalSnapshot,
        target: Option<(
            cellule_runtime::identity::CellId,
            cellule_runtime::identity::IncarnationId,
        )>,
    ) -> JournalResult<Option<i64>> {
        self.check_scope(expected.head().scope())?;
        if self.snapshot()? != *expected {
            return Err(OperationError::Conflict.into());
        }
        // Constant resident history memory; only the CAS-reachable chain counts.
        // An indexed production backend can satisfy the same atomic contract.
        let mut cursor = expected.head().progress();
        let mut latest = None;
        while let Some(head) = cursor {
            let page = self
                .progress(head.digest)?
                .ok_or(OperationError::NotFound)?;
            if page.sequence() != head.sequence {
                return Err(OperationError::Conflict.into());
            }
            for entry in page.entries() {
                if target.is_none_or(|(cell, incarnation)| {
                    entry.spec().target.cell_id() == cell && entry.spec().incarnation == incarnation
                }) && matches!(
                    entry.phase(),
                    AttemptPhase::Activated | AttemptPhase::Recovered
                ) {
                    let at = entry.completed_at_ms().ok_or(OperationError::Conflict)?;
                    latest = Some(latest.map_or(at, |old: i64| old.max(at)));
                }
            }
            cursor = page.previous().map(|digest| ProgressHead {
                digest,
                sequence: head.sequence - 1,
            });
        }
        Ok(latest)
    }
    pub fn snapshot(&self) -> JournalResult<FleetJournalSnapshot> {
        let (head, registry) = self.tx.query_row(
            "SELECT head, registry FROM state WHERE singleton=1",
            [],
            |row| {
                Ok((
                    blob(row, 0, MAX_RECORD_BYTES)?,
                    blob(row, 1, MAX_RECORD_BYTES)?,
                ))
            },
        )?;
        let head = FleetHead::from_bytes(&head)?;
        let registry = RegistryVersion::from_bytes(&registry)?;
        self.check_scope(head.scope())?;
        if let Some(operation) = head.maintenance()
            && self.operation(operation.id())?.as_ref() != Some(operation)
        {
            return Err(OperationError::Invalid(
                "maintenance head lacks its exact retained operation",
            )
            .into());
        }
        if let Some(progress) = head.progress() {
            let page = self
                .progress(progress.digest)?
                .ok_or(OperationError::NotFound)?;
            if page.sequence() != progress.sequence {
                return Err(OperationError::Conflict.into());
            }
        }
        Ok(FleetJournalSnapshot::new(head, registry)?)
    }
    pub fn set_head(&self, head: &FleetHead) -> JournalResult<()> {
        self.check_scope(head.scope())?;
        self.tx.execute(
            "UPDATE state SET head=?1 WHERE singleton=1",
            [head.to_bytes()?],
        )?;
        Ok(())
    }
    pub fn set_registry(&self, version: RegistryVersion) -> JournalResult<()> {
        self.check_scope(version.scope())?;
        self.tx.execute(
            "UPDATE state SET registry=?1 WHERE singleton=1",
            [version.to_bytes()?],
        )?;
        Ok(())
    }
    pub fn advance_registry(&self) -> JournalResult<()> {
        let version = self.snapshot()?.registry();
        self.set_registry(version.advance(version.revision())?)
    }
    pub fn intent(&self, node: NodeId) -> JournalResult<Option<NodeIntent>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM intents WHERE key=?1",
                [node.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_RECORD_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| {
                let intent = NodeIntent::from_bytes(&bytes)?;
                self.check_scope(intent.scope())?;
                if intent.node() != node {
                    return Err(OperationError::Conflict.into());
                }
                Ok(intent)
            })
            .transpose()
    }
    pub fn required_intent(&self, node: NodeId) -> JournalResult<NodeIntent> {
        self.intent(node)?
            .ok_or_else(|| OperationError::NotFound.into())
    }
    pub fn write_intent(&self, intent: &NodeIntent) -> JournalResult<()> {
        self.check_scope(intent.scope())?;
        self.tx.execute("INSERT INTO intents(key,body) VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET body=excluded.body", params![intent.node().as_bytes().as_slice(), intent.to_bytes()?])?;
        self.advance_registry()
    }
    pub fn operation(&self, id: OperationId) -> JournalResult<Option<MaintenanceOperation>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM operations WHERE key=?1",
                [id.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_RECORD_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| {
                let operation = MaintenanceOperation::from_bytes(&bytes)?;
                if operation.id() != id {
                    return Err(OperationError::Conflict.into());
                }
                Ok(operation)
            })
            .transpose()
    }

    pub fn progress(&self, digest: Digest) -> JournalResult<Option<ProgressPage>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM progress WHERE key=?1",
                [digest.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_PAGE_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| {
                let page = ProgressPage::from_bytes(&bytes)?;
                self.check_scope(page.scope())?;
                if page.digest()? != digest {
                    return Err(OperationError::Conflict.into());
                }
                Ok(page)
            })
            .transpose()
    }
    pub fn enrollment(&self, key: Digest) -> JournalResult<Option<EnrollmentRecord>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM enrollments WHERE key=?1",
                [key.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_RECORD_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| {
                let record = EnrollmentRecord::from_bytes(&bytes)?;
                self.check_scope(record.spec().scope)?;
                if record.spec().key()? != key {
                    return Err(OperationError::Conflict.into());
                }
                Ok(record)
            })
            .transpose()
    }
    pub fn write_enrollment(&self, record: &EnrollmentRecord) -> JournalResult<()> {
        self.check_scope(record.spec().scope)?;
        self.tx.execute("INSERT INTO enrollments(key,body) VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET body=excluded.body", params![record.spec().key()?.as_bytes().as_slice(), record.to_bytes()?])?;
        self.advance_registry()
    }
    pub fn check_version(&self, expected: RegistryVersion) -> JournalResult<()> {
        self.check_scope(expected.scope())?;
        if self.snapshot()?.registry() != expected {
            return Err(OperationError::Conflict.into());
        }
        Ok(())
    }
}
