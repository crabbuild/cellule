use super::*;

impl FleetFollowerEvacuationJournal for SqliteJournal {
    fn follower_replacement_policy<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<FollowerReplacementPolicy>> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(expected.head().scope())?;
            if db.snapshot()? != expected {
                return Err(OperationError::Conflict.into());
            }
            db.follower_policy()
        }))
    }
    fn set_follower_replacement_policy<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        policy: FollowerReplacementPolicy,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FollowerReplacementPolicy> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(policy.scope())?;policy.to_bytes()?;
            let snapshot=db.snapshot()?;
            if snapshot!=expected || snapshot.registry().bootstrap_revision().is_none()
                || snapshot.head().controller().is_none_or(|lease| now_ms>=lease.expires_at_ms)
                || now_ms<0 {
                return Err(OperationError::Conflict.into());
            }
            let expected_revision=db.follower_policy()?.map_or(Some(1),|old| old.revision().checked_add(1)).ok_or(OperationError::Conflict)?;
            if policy.revision()!=expected_revision {return Err(OperationError::Conflict.into());}
            db.tx.execute("INSERT INTO follower_policy(singleton,body) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET body=excluded.body",[policy.to_bytes()?])?;
            db.advance_registry()?;
            Ok(policy)
        }))
    }
    fn persist_follower_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        record: &'a FollowerEvacuationRecord,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FollowerEvacuationRecord> {
        Box::pin(async move {
            record.to_bytes()?;
            let expected = expected.clone();
            let record = record.clone();
            let result=self.run(move |db| {
                db.check_scope(record.policy().scope())?;
                let digest=record.digest()?;
                if let Some(original)=db.follower_evacuation(digest)? {
                    if original!=record {return Err(OperationError::Conflict.into());}
                    // Historical replay cannot restore a superseded latest pointer.
                    return Ok(original);
                }
                let snapshot=db.snapshot()?;let operation=record.operation();let (started,finished)=record.interval();
                if snapshot!=expected || record.registry()!=expected.registry()
                    || record.head_digest()!=Digest::from_bytes(*blake3::hash(&expected.head().to_bytes()?).as_bytes())
                    || snapshot.registry().bootstrap_revision().is_none()
                    || snapshot.head().maintenance()!=Some(operation)
                    || !matches!(operation.phase(),MaintenancePhase::Evacuating | MaintenancePhase::Closing)
                    || snapshot.head().controller().is_none_or(|lease| now_ms>=lease.expires_at_ms)
                    || now_ms>=operation.deadline_ms() || now_ms<finished || now_ms-started>30_000
                    || db.follower_policy()?!=Some(record.policy()) {
                    return Err(OperationError::Conflict.into());
                }
                let donor=db.required_intent(operation.node())?;
                if donor.session()!=operation.session() || donor.revision()!=operation.intent_revision() || donor.mode()!=NodeMode::Draining {
                    return Err(OperationError::Conflict.into());
                }
                for row in record.retired() {
                    if db.enrollment(row.spec().key()?)?.as_ref()!=Some(row) {return Err(OperationError::Conflict.into());}
                }
                let source=record.source()?;let source_intent=db.required_intent(source.node)?;
                if db.follower_rows(source.node,source.session,record.original_epoch()?)?!=record.retired()
                    || db.follower_rows(source.node,source.session,record.replacement_epoch())?
                        !=record.replacements().iter().map(|entry| entry.enrollment.clone()).collect::<Vec<_>>() {
                    return Err(OperationError::Conflict.into());
                }
                if source_intent.session()!=source.session || source_intent.mode()!=NodeMode::Active {return Err(OperationError::Conflict.into());}
                for entry in record.replacements() {
                    let row=&entry.enrollment;let target=row.spec().target;let intent=db.required_intent(target.node)?;
                    if db.enrollment(row.spec().key()?)?.as_ref()!=Some(row)
                        || intent.session()!=target.session || intent.revision()!=target.intent_revision || intent.mode()!=NodeMode::Active
                        || row.spec().source.is_none_or(|endpoint| endpoint.intent_revision!=source_intent.revision()) {
                        return Err(OperationError::Conflict.into());
                    }
                }
                db.tx.execute("INSERT INTO follower_evacuations(key,body) VALUES(?1,?2)",params![digest.as_bytes().as_slice(),record.to_bytes()?])?;
                db.tx.execute("INSERT INTO latest_follower_evacuations(operation,original,witness) VALUES(?1,?2,?3) ON CONFLICT(operation,original) DO UPDATE SET witness=excluded.witness",
                    params![operation.id().as_bytes().as_slice(),record.original_key().as_bytes().as_slice(),digest.as_bytes().as_slice()])?;
                db.advance_registry()?;
                Ok(record)
            }).await?;
            #[cfg(test)]
            self.follower_evacuation_reply().await?;
            Ok(result)
        })
    }
    fn load_follower_evacuation(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<FollowerEvacuationRecord>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.follower_evacuation(digest)
        }))
    }
    fn latest_follower_evacuation<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
        original: Digest,
    ) -> FleetAdapterFuture<'a, Option<FollowerEvacuationRecord>> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(expected.head().scope())?;
            if db.snapshot()?!=expected {return Err(OperationError::Conflict.into());}
            let key=db.tx.query_row("SELECT witness FROM latest_follower_evacuations WHERE operation=?1 AND original=?2",
                params![operation.as_bytes().as_slice(),original.as_bytes().as_slice()],|row| blob(row,0,32)).optional()?;
            key.map(|key| -> JournalResult<_> {
                let record=db.follower_evacuation(Digest::from_bytes(key.as_slice().try_into()?))?.ok_or(OperationError::NotFound)?;
                if record.operation().id()!=operation || record.original_key()!=original {return Err(OperationError::Conflict.into());}
                Ok(record)
            }).transpose()
        }))
    }
}
impl Db<'_> {
    fn follower_rows(
        &self,
        node: NodeId,
        session: SessionId,
        epoch: u64,
    ) -> JournalResult<Vec<EnrollmentRecord>> {
        let mut statement = self
            .tx
            .prepare("SELECT key,body FROM enrollments ORDER BY key")?;
        let rows = statement.query_map([], |row| {
            Ok((blob(row, 0, 32)?, blob(row, 1, MAX_RECORD_BYTES)?))
        })?;
        let mut matches = Vec::new();
        for row in rows {
            let (key, body) = row?;
            let record = EnrollmentRecord::from_bytes(&body)?;
            self.check_scope(record.spec().scope)?;
            if record.spec().key()?.as_bytes().as_slice() != key {
                return Err(OperationError::Conflict.into());
            }
            if record
                .spec()
                .source
                .is_some_and(|source| source.node == node && source.session == session)
                && record.spec().role == (EnrollmentRole::Follower { log_epoch: epoch })
            {
                if matches.len() == 2 {
                    return Err(OperationError::Conflict.into());
                }
                matches.push(record);
            }
        }
        matches.sort_by_key(|row| *row.spec().target.node.as_bytes());
        Ok(matches)
    }
    fn follower_policy(&self) -> JournalResult<Option<FollowerReplacementPolicy>> {
        self.tx
            .query_row(
                "SELECT body FROM follower_policy WHERE singleton=1",
                [],
                |row| blob(row, 0, MAX_RECORD_BYTES),
            )
            .optional()?
            .map(|bytes| -> JournalResult<_> {
                let policy = FollowerReplacementPolicy::from_bytes(&bytes)?;
                self.check_scope(policy.scope())?;
                Ok(policy)
            })
            .transpose()
    }
    fn follower_evacuation(
        &self,
        digest: Digest,
    ) -> JournalResult<Option<FollowerEvacuationRecord>> {
        self.tx
            .query_row(
                "SELECT body FROM follower_evacuations WHERE key=?1",
                [digest.as_bytes().as_slice()],
                |row| blob(row, 0, MAX_PAGE_BYTES),
            )
            .optional()?
            .map(|bytes| -> JournalResult<_> {
                let record = FollowerEvacuationRecord::from_bytes(&bytes)?;
                self.check_scope(record.policy().scope())?;
                if record.digest()? != digest {
                    return Err(OperationError::Conflict.into());
                }
                Ok(record)
            })
            .transpose()
    }
}
