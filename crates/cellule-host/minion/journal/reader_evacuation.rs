use super::*;

impl FleetReaderEvacuationJournal for SqliteJournal {
    fn persist_reader_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        record: &'a ReaderEvacuationRecord,
        pages: &'a [ReaderEvacuationPage],
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ReaderEvacuationRecord> {
        Box::pin(async move {
            record.validate_pages(pages)?;
            let expected = expected.clone();
            let record = record.clone();
            let pages = pages.to_vec();
            let result=self.run(move|db| {
                db.check_scope(record.retired().spec().scope)?;
                let digest=record.digest()?;
                if let Some(original)=db.reader_evacuation(digest)? {
                    if original!=record {return Err(OperationError::Conflict.into());}
                    for page in &pages {
                        if db.reader_evacuation_page(page.digest()?)?.as_ref()!=Some(page) {return Err(OperationError::Conflict.into());}
                    }
                    // Replay returns history only. It never restores a pointer
                    // superseded by a fresh capture for the same responsibility.
                    return Ok(original);
                }
                let current=db.snapshot()?;
                let operation=record.operation();
                let lease=current.head().controller().ok_or(OperationError::Fenced)?;
                let (started,finished)=record.interval();
                if current!=expected || record.registry()!=expected.registry()
                    || record.head_digest()!=Digest::from_bytes(*blake3::hash(&expected.head().to_bytes()?).as_bytes())
                    || current.head().maintenance()!=Some(operation)
                    || current.registry().bootstrap_revision().is_none()
                    || !matches!(operation.phase(),MaintenancePhase::Evacuating|MaintenancePhase::Closing)
                    || now_ms>=lease.expires_at_ms || now_ms>=operation.deadline_ms()
                    || now_ms<finished || now_ms-started>30_000 {
                    return Err(OperationError::Conflict.into());
                }
                let donor=db.required_intent(operation.node())?;
                if donor.session()!=operation.session() || donor.revision()!=operation.intent_revision() || donor.mode()!=NodeMode::Draining
                    || db.enrollment(record.retired().spec().key()?)?.as_ref()!=Some(record.retired()) {
                    return Err(OperationError::Conflict.into());
                }
                for page in &pages {
                    for entry in page.entries() {
                        let row=db.enrollment(entry.enrollment_key)?.ok_or(OperationError::NotFound)?;
                        let intent=db.required_intent(entry.node)?;
                        if row.status()!=EnrollmentStatus::Established || row.spec().target.node!=entry.node || row.spec().target.session!=entry.session
                            || intent.session()!=entry.session || intent.mode()!=NodeMode::Active
                            || Digest::from_bytes(*blake3::hash(&row.to_bytes()?).as_bytes())!=entry.enrollment_digest
                            || !matches!(&row.spec().role,EnrollmentRole::Reader {target,position}
                                if target.cell_id()==record.authority().cell && position.incarnation==record.authority().incarnation
                                    && entry.commit_sequence>=position.root.commit_sequence) {
                            return Err(OperationError::Conflict.into());
                        }
                    }
                    let key=page.digest()?;
                    if let Some(original)=db.reader_evacuation_page(key)? {
                        if original!=*page {return Err(OperationError::Conflict.into());}
                    } else {
                        db.tx.execute("INSERT INTO reader_evacuation_pages(key,body) VALUES(?1,?2)",params![key.as_bytes().as_slice(),page.to_bytes()?])?;
                    }
                }
                db.tx.execute("INSERT INTO reader_evacuations(key,body) VALUES(?1,?2)",params![digest.as_bytes().as_slice(),record.to_bytes()?])?;
                db.tx.execute("INSERT INTO latest_reader_evacuations(operation,original,witness) VALUES(?1,?2,?3) ON CONFLICT(operation,original) DO UPDATE SET witness=excluded.witness",
                    params![operation.id().as_bytes().as_slice(),record.retired().spec().key()?.as_bytes().as_slice(),digest.as_bytes().as_slice()])?;
                db.advance_registry()?;
                Ok(record)
            }).await?;
            #[cfg(test)]
            self.reader_evacuation_reply().await?;
            Ok(result)
        })
    }
    fn load_reader_evacuation(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<ReaderEvacuationRecord>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.reader_evacuation(digest)
        }))
    }
    fn load_reader_evacuation_page(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<ReaderEvacuationPage>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.reader_evacuation_page(digest)
        }))
    }
    fn latest_reader_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
        original: Digest,
    ) -> FleetAdapterFuture<'a, Option<ReaderEvacuationRecord>> {
        let expected = expected.clone();
        Box::pin(self.run(move|db|{
            db.check_scope(expected.head().scope())?;
            if db.snapshot()?!=expected {return Err(OperationError::Conflict.into());}
            let key=db.tx.query_row("SELECT witness FROM latest_reader_evacuations WHERE operation=?1 AND original=?2",params![operation.as_bytes().as_slice(),original.as_bytes().as_slice()],|row|blob(row,0,32)).optional()?;
            key.map(|bytes| ->JournalResult<_> {
                let digest=Digest::from_bytes(bytes.as_slice().try_into()?);
                let record=db.reader_evacuation(digest)?.ok_or(OperationError::NotFound)?;
                if record.operation().id()!=operation || record.retired().spec().key()?!=original {return Err(OperationError::Conflict.into());}
                Ok(record)
            }).transpose()
        }))
    }
}
impl Db<'_> {
    fn reader_evacuation(&self, digest: Digest) -> JournalResult<Option<ReaderEvacuationRecord>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM reader_evacuations WHERE key=?1",
                [digest.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_PAGE_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| -> JournalResult<_> {
                let record = ReaderEvacuationRecord::from_bytes(&bytes)?;
                self.check_scope(record.retired().spec().scope)?;
                if record.digest()? != digest {
                    return Err(OperationError::Conflict.into());
                }
                Ok(record)
            })
            .transpose()
    }
    fn reader_evacuation_page(
        &self,
        digest: Digest,
    ) -> JournalResult<Option<ReaderEvacuationPage>> {
        let bytes = self
            .tx
            .query_row(
                "SELECT body FROM reader_evacuation_pages WHERE key=?1",
                [digest.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_RECORD_BYTES),
            )
            .optional()?;
        bytes
            .map(|bytes| -> JournalResult<_> {
                let page = ReaderEvacuationPage::from_bytes(&bytes)?;
                if page.digest()? != digest {
                    return Err(OperationError::Conflict.into());
                }
                Ok(page)
            })
            .transpose()
    }
}
