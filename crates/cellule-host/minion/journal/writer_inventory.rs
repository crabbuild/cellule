use super::*;

impl FleetOriginalWriterJournal for SqliteJournal {
    fn persist_original_writers<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        record: &'a OriginalWriterInventoryRecord,
        pages: &'a [OriginalWriterInventoryPage],
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, OriginalWriterInventoryRecord> {
        Box::pin(async move {
            record.validate_pages(pages)?;
            let expected = expected.clone();
            let record = record.clone();
            let pages = pages.to_vec();
            let stored = self
                .run(move |db| {
                    let basis = record.basis();
                    db.check_scope(basis.registry.scope())?;
                    let digest = record.digest()?;
                    if let Some(original) =
                        db.original_writers(basis.operation.id(), basis.process_request)?
                    {
                        if original != record {
                            return Err(OperationError::Conflict.into());
                        }
                        for page in &pages {
                            if db.original_writer_page(page.digest()?)?.as_ref() != Some(page) {
                                return Err(OperationError::Conflict.into());
                            }
                        }
                        // Immutable original set: no superseding pointer, timestamp
                        // refresh, provider retry or registry change on exact replay.
                        return Ok(original);
                    }
                    let current = db.snapshot()?;
                    let lease = current.head().controller().ok_or(OperationError::Fenced)?;
                    let intent = db.required_intent(basis.operation.node())?;
                    if current != expected
                        || basis.registry != expected.registry()
                        || basis.head_digest
                            != Digest::from_bytes(
                                *blake3::hash(&expected.head().to_bytes()?).as_bytes(),
                            )
                        || current.head().maintenance() != Some(&basis.operation)
                        || current.registry().bootstrap_revision().is_none()
                        || basis.operation.phase() == MaintenancePhase::Completed
                        || intent.mode() != NodeMode::Draining
                        || intent.session() != basis.operation.session()
                        || intent.revision() != basis.operation.intent_revision()
                        || now_ms >= lease.expires_at_ms
                        || now_ms >= basis.operation.deadline_ms()
                        || now_ms < basis.interval.1
                        || now_ms - basis.interval.0 > 30_000
                        || db.enrollment(basis.boot.spec().key()?)?.as_ref() != Some(&basis.boot)
                    {
                        return Err(OperationError::Conflict.into());
                    }
                    for page in &pages {
                        let key = page.digest()?;
                        if let Some(original) = db.original_writer_page(key)? {
                            if original != *page {
                                return Err(OperationError::Conflict.into());
                            }
                        } else {
                            db.tx.execute(
                                "INSERT INTO original_writer_pages(key,body) VALUES(?1,?2)",
                                params![key.as_bytes().as_slice(), page.to_bytes()?],
                            )?;
                        }
                    }
                    db.tx.execute(
                    "INSERT INTO original_writers(operation,process,key,body) VALUES(?1,?2,?3,?4)",
                    params![
                        basis.operation.id().as_bytes().as_slice(),
                        basis.process_request.as_bytes().as_slice(),
                        digest.as_bytes().as_slice(),
                        record.to_bytes()?
                    ],
                )?;
                    db.advance_registry()?;
                    Ok(record)
                })
                .await?;
            #[cfg(test)]
            self.original_writer_reply().await;
            Ok(stored)
        })
    }
    fn original_writers<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
        process_request: Digest,
    ) -> FleetAdapterFuture<'a, Option<OriginalWriterInventoryRecord>> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(expected.head().scope())?;
            if db.snapshot()? != expected {
                return Err(OperationError::Conflict.into());
            }
            db.original_writers(operation, process_request)
        }))
    }
    fn original_writer_page(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<OriginalWriterInventoryPage>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.original_writer_page(digest)
        }))
    }
}
impl Db<'_> {
    fn original_writers(
        &self,
        operation: OperationId,
        process_request: Digest,
    ) -> JournalResult<Option<OriginalWriterInventoryRecord>> {
        let row = self
            .tx
            .query_row(
                "SELECT key,body FROM original_writers WHERE operation=?1 AND process=?2",
                params![
                    operation.as_bytes().as_slice(),
                    process_request.as_bytes().as_slice()
                ],
                |row| Ok((blob(row, 0, 32)?, blob(row, 1, MAX_RECORD_BYTES)?)),
            )
            .optional()?;
        row.map(|(key, body)| -> JournalResult<_> {
            let record = OriginalWriterInventoryRecord::from_bytes(&body)?;
            self.check_scope(record.basis().registry.scope())?;
            if record.basis().operation.id() != operation
                || record.basis().process_request != process_request
                || record.digest()?.as_bytes().as_slice() != key.as_slice()
            {
                return Err(OperationError::Conflict.into());
            }
            Ok(record)
        })
        .transpose()
    }
    fn original_writer_page(
        &self,
        digest: Digest,
    ) -> JournalResult<Option<OriginalWriterInventoryPage>> {
        let body = self
            .tx
            .query_row(
                "SELECT body FROM original_writer_pages WHERE key=?1",
                [digest.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_PAGE_BYTES),
            )
            .optional()?;
        body.map(|body| -> JournalResult<_> {
            let page = OriginalWriterInventoryPage::from_bytes(&body)?;
            if page.digest()? != digest {
                return Err(OperationError::Conflict.into());
            }
            Ok(page)
        })
        .transpose()
    }
}
