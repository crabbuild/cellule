use super::*;

impl Db<'_> {
    pub(super) fn freeze_maintenance_enrollments(
        &self,
        current: &FleetJournalSnapshot,
        now_ms: i64,
    ) -> JournalResult<()> {
        let operation = current
            .head()
            .maintenance()
            .ok_or(OperationError::NotFound)?;
        if operation.phase() != MaintenancePhase::Cordoned {
            return Err(OperationError::Conflict.into());
        }
        // The operation's one-way capture anchor survives boot adoption. A
        // missing manifest after capture is unknown, never a new first capture.
        if let Some(anchor) = self.maintenance_enrollment_anchor(operation.id())? {
            let original = self
                .maintenance_enrollments(operation.id())?
                .ok_or(OperationError::NotFound)?;
            if original.digest()? != anchor {
                return Err(OperationError::Conflict.into());
            }
            if original.operation().node() != operation.node()
                || original.operation().request_digest() != operation.request_digest()
                || original.operation().created_at_ms() != operation.created_at_ms()
            {
                return Err(OperationError::Conflict.into());
            }
            let pages = original
                .pages()
                .iter()
                .map(|digest| {
                    self.maintenance_enrollment_page(*digest)?
                        .ok_or_else(|| OperationError::NotFound.into())
                })
                .collect::<JournalResult<Vec<_>>>()?;
            original.validate_pages(&pages)?;
            return Ok(());
        }
        let mut entries = Vec::new();
        let mut statement = self
            .tx
            .prepare("SELECT key,body FROM enrollments ORDER BY key LIMIT ?1")?;
        let mut rows = statement.query([(MAX_MAINTENANCE_ENROLLMENTS + 1) as i64])?;
        let mut scanned = 0;
        while let Some(row) = rows.next()? {
            if scanned >= MAX_MAINTENANCE_ENROLLMENTS {
                return Err(OperationError::Budget.into());
            }
            scanned += 1;
            let key = blob(row, 0, 32)?;
            let enrollment = EnrollmentRecord::from_bytes(&blob(row, 1, MAX_RECORD_BYTES)?)?;
            self.check_scope(enrollment.spec().scope)?;
            if enrollment.spec().key()?.as_bytes().as_slice() != key {
                return Err(OperationError::Conflict.into());
            }
            if MaintenanceEnrollmentInventory::includes(operation.node(), &enrollment) {
                entries.push(enrollment);
            }
        }
        let (inventory, pages) = MaintenanceEnrollmentInventory::new(
            operation.clone(),
            (
                Digest::from_bytes(*blake3::hash(&current.head().to_bytes()?).as_bytes()),
                current.registry(),
            ),
            now_ms,
            entries,
        )?;
        for page in pages {
            let digest = page.digest()?;
            if let Some(original) = self.maintenance_enrollment_page(digest)? {
                if original != page {
                    return Err(OperationError::Conflict.into());
                }
            } else {
                self.tx.execute(
                    "INSERT INTO maintenance_enrollment_pages(key,body) VALUES(?1,?2)",
                    params![digest.as_bytes().as_slice(), page.to_bytes()?],
                )?;
            }
        }
        self.tx.execute(
            "INSERT INTO maintenance_enrollments(operation,key,body) VALUES(?1,?2,?3)",
            params![
                operation.id().as_bytes().as_slice(),
                inventory.digest()?.as_bytes().as_slice(),
                inventory.to_bytes()?
            ],
        )?;
        if self.tx.execute(
            "UPDATE maintenance_enrollment_anchors SET key=?1 WHERE operation=?2 AND key IS NULL",
            params![
                inventory.digest()?.as_bytes().as_slice(),
                operation.id().as_bytes().as_slice()
            ],
        )? != 1
        {
            return Err(OperationError::Conflict.into());
        }
        Ok(())
    }
    fn maintenance_enrollment_anchor(
        &self,
        operation: OperationId,
    ) -> JournalResult<Option<Digest>> {
        let key = self
            .tx
            .query_row(
                "SELECT key FROM maintenance_enrollment_anchors WHERE operation=?1",
                [operation.as_bytes().as_slice()],
                |row| row.get::<_, Option<Vec<u8>>>(0),
            )
            .optional()?
            .ok_or(OperationError::NotFound)?;
        key.map(|bytes| {
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| OperationError::Conflict)?;
            Ok(Digest::from_bytes(bytes))
        })
        .transpose()
    }
    pub(super) fn maintenance_enrollments(
        &self,
        operation: OperationId,
    ) -> JournalResult<Option<MaintenanceEnrollmentInventory>> {
        let anchor = self.maintenance_enrollment_anchor(operation)?;
        let row = self
            .tx
            .query_row(
                "SELECT key,body FROM maintenance_enrollments WHERE operation=?1",
                [operation.as_bytes().as_slice()],
                |row| Ok((blob(row, 0, 32)?, blob(row, 1, MAX_RECORD_BYTES)?)),
            )
            .optional()?;
        row.map(|(key, bytes)| -> JournalResult<_> {
            let inventory = MaintenanceEnrollmentInventory::from_bytes(&bytes)?;
            self.check_scope(inventory.registry().scope())?;
            if inventory.operation().id() != operation
                || inventory.digest()?.as_bytes().as_slice() != key
                || anchor != Some(inventory.digest()?)
            {
                return Err(OperationError::Conflict.into());
            }
            Ok(inventory)
        })
        .transpose()
    }
    pub(super) fn maintenance_enrollment_page(
        &self,
        digest: Digest,
    ) -> JournalResult<Option<MaintenanceEnrollmentPage>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM maintenance_enrollment_pages WHERE key=?1",
                [digest.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_PAGE_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| -> JournalResult<_> {
                let page = MaintenanceEnrollmentPage::from_bytes(&bytes)?;
                if page.digest()? != digest {
                    return Err(OperationError::Conflict.into());
                }
                for row in page.entries() {
                    self.check_scope(row.spec().scope)?;
                }
                Ok(page)
            })
            .transpose()
    }
}
