use super::*;

impl FleetJournal for SqliteJournal {
    fn load_snapshot(&self, scope: FleetScope) -> FleetAdapterFuture<'_, FleetJournalSnapshot> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.snapshot()
        }))
    }
    fn claim_controller(
        &self,
        scope: FleetScope,
        expected_revision: u64,
        claimant: SessionId,
        now_ms: i64,
    ) -> FleetAdapterFuture<'_, FleetJournalSnapshot> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            let current = db.snapshot()?;
            let head = current
                .head()
                .claim(db.profile, expected_revision, claimant, now_ms)?;
            db.set_head(&head)?;
            db.snapshot()
        }))
    }
    fn compare_exchange<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        epoch: u64,
        now_ms: i64,
        transition: &'a JournalTransition,
    ) -> FleetAdapterFuture<'a, FleetJournalSnapshot> {
        let expected = expected.clone();
        let transition = transition.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(expected.head().scope())?;
            let current = db.snapshot()?;
            if current != expected { return Err(OperationError::Conflict.into()); }
            if let JournalTransition::ResolveUnaccepted { id, effect } = &transition {
                let attempt = current.head().attempts().iter().find(|attempt| attempt.spec().id == *id).ok_or(OperationError::NotFound)?;
                let spec = attempt.spec();
                let (node, session) = if effect.is_source_release() { (spec.source_node, spec.source) } else { (spec.destination_node, spec.destination) };
                let accepted = db.tx.query_row("SELECT EXISTS(SELECT 1 FROM actions WHERE operation=?1 AND sequence=?2 AND effect=?3 AND node=?4 AND session=?5)",
                    params![id.operation.as_bytes().as_slice(), id.sequence.to_be_bytes().as_slice(), *effect as u8, node.as_bytes().as_slice(), session.as_bytes().as_slice()], |row| row.get::<_, bool>(0))?;
                if accepted { return Err(OperationError::Busy.into()); }
            }
            if let JournalTransition::Allocate(spec) = &transition {
                current.registry().authorize_allocation(current.head(), spec, &db.required_intent(spec.source_node)?, &db.required_intent(spec.destination_node)?)?;
            }
            if let JournalTransition::BeginMaintenance(request) = &transition
                && let Some(original) = db.tx.query_row("SELECT request FROM operations WHERE key=?1", [request.id().as_bytes().as_slice()], |row| blob(row, 0, MAX_RECORD_BYTES)).optional()? {
                    let original = MaintenanceOperation::from_bytes(&original)?;
                    if original != *request { return Err(OperationError::Conflict.into()); }
                    let lease = current.head().controller().ok_or(OperationError::Fenced)?;
                    if epoch != lease.epoch || now_ms >= lease.expires_at_ms { return Err(OperationError::Fenced.into()); }
                    // The canonical calculation checks revision and monotonic
                    // time; its successor is not published for an idempotent request.
                    current.head().claim(db.profile, current.head().revision(), lease.claimant, now_ms)?;
                    // The original request is retained independently of mutable
                    // deadline/session progress. Replays cannot start it again.
                    return Ok(current);
            }
            let head = current.head().transition(db.profile, current.head().revision(), epoch, now_ms, transition.clone())?;
            if head == *current.head() { return Ok(current); }
            if matches!(transition, JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation)) {
                db.freeze_maintenance_enrollments(&current, now_ms)?;
            }
            if head.maintenance() != current.head().maintenance() && let Some(operation) = head.maintenance() {
                let old = db.required_intent(operation.node())?;
                let next = old.advance_maintenance(operation)?;
                if next != old { db.write_intent(&next)?; }
                let request = if let JournalTransition::BeginMaintenance(request) = &transition { request.to_bytes()? }
                    else { db.tx.query_row("SELECT request FROM operations WHERE key=?1", [operation.id().as_bytes().as_slice()], |row| blob(row, 0, MAX_RECORD_BYTES))? };
                db.tx.execute("INSERT INTO operations(key,request,body) VALUES (?1,?2,?3) ON CONFLICT(key) DO UPDATE SET body=excluded.body", params![operation.id().as_bytes().as_slice(), request, operation.to_bytes()?])?;
                if matches!(transition, JournalTransition::BeginMaintenance(_)) {
                    db.tx.execute("INSERT INTO maintenance_enrollment_anchors(operation,key) VALUES (?1,NULL)", [operation.id().as_bytes().as_slice()])?;
                }
            }
            if let JournalTransition::Retire { progress } = &transition {
                db.tx.execute("INSERT INTO progress(key,body) VALUES (?1,?2)", params![progress.digest()?.as_bytes().as_slice(), progress.to_bytes()?])?;
            }
            db.set_head(&head)?;
            db.snapshot()
        }))
    }
    fn load_operation(
        &self,
        scope: FleetScope,
        operation: OperationId,
    ) -> FleetAdapterFuture<'_, Option<MaintenanceOperation>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.operation(operation)
        }))
    }
    fn maintenance_enrollments<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        operation: OperationId,
    ) -> FleetAdapterFuture<'a, Option<MaintenanceEnrollmentInventory>> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(expected.head().scope())?;
            if db.snapshot()? != expected {
                return Err(OperationError::Conflict.into());
            }
            db.maintenance_enrollments(operation)
        }))
    }
    fn maintenance_enrollment_page(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<MaintenanceEnrollmentPage>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.maintenance_enrollment_page(digest)
        }))
    }
    fn load_progress(
        &self,
        scope: FleetScope,
        digest: Digest,
    ) -> FleetAdapterFuture<'_, Option<ProgressPage>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.progress(digest)
        }))
    }
    fn last_moved_at<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
        cell: cellule_runtime::identity::CellId,
        incarnation: cellule_runtime::identity::IncarnationId,
    ) -> FleetAdapterFuture<'a, Option<i64>> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| db.movement_time(&expected, Some((cell, incarnation)))))
    }
    fn last_movement_at<'a>(
        &'a self,
        expected: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, Option<i64>> {
        let expected = expected.clone();
        Box::pin(self.run(move |db| db.movement_time(&expected, None)))
    }
    fn intents_page(
        &self,
        version: RegistryVersion,
        after: Option<NodeId>,
        limit: usize,
    ) -> FleetAdapterFuture<'_, IntentPage> {
        Box::pin(self.run(move |db| {
            page_limit(limit)?;
            db.check_version(version)?;
            if let Some(after) = after {
                db.required_intent(after)?;
            }
            let mut statement = db.tx.prepare(
                "SELECT key,body FROM intents WHERE (?1 IS NULL OR key>?1) ORDER BY key LIMIT ?2",
            )?;
            let mut rows = statement.query(params![
                after.map(|key| key.as_bytes().to_vec()),
                i64::try_from(limit + 1)?
            ])?;
            let mut entries = Vec::with_capacity(limit);
            while entries.len() < limit {
                let Some(row) = rows.next()? else {
                    break;
                };
                let key = blob(row, 0, 16)?;
                let intent = NodeIntent::from_bytes(&blob(row, 1, MAX_RECORD_BYTES)?)?;
                if intent.node().as_bytes().as_slice() != key {
                    return Err(OperationError::Conflict.into());
                }
                entries.push(intent);
            }
            let next = if rows.next()?.is_some() {
                entries.last().map(NodeIntent::node)
            } else {
                None
            };
            Ok(IntentPage::new(version, after, entries, next)?)
        }))
    }
    fn enrollments_page(
        &self,
        version: RegistryVersion,
        after: Option<Digest>,
        limit: usize,
    ) -> FleetAdapterFuture<'_, EnrollmentPage> {
        Box::pin(self.run(move |db| {
            page_limit(limit)?;
            db.check_version(version)?;
            if let Some(after) = after && db.enrollment(after)?.is_none() { return Err(OperationError::NotFound.into()); }
            let mut statement = db.tx.prepare("SELECT key,body FROM enrollments WHERE (?1 IS NULL OR key>?1) ORDER BY key LIMIT ?2")?;
            let mut rows = statement.query(params![after.map(|key| key.as_bytes().to_vec()), i64::try_from(limit + 1)?])?;
            let mut entries = Vec::with_capacity(limit);
            while entries.len() < limit {
                let Some(row) = rows.next()? else { break; };
                let key = blob(row, 0, 32)?;
                let record = EnrollmentRecord::from_bytes(&blob(row, 1, MAX_RECORD_BYTES)?)?;
                if record.spec().key()?.as_bytes().as_slice() != key { return Err(OperationError::Conflict.into()); }
                entries.push(record);
            }
            let next = if rows.next()?.is_some() { entries.last().map(|record| record.spec().key()).transpose()? } else { None };
            Ok(EnrollmentPage::new(version, after, entries, next)?)
        }))
    }
    fn set_scheduling(
        &self,
        expected: RegistryVersion,
        enabled: bool,
    ) -> FleetAdapterFuture<'_, RegistryVersion> {
        Box::pin(self.run(move |db| {
            db.check_version(expected)?;
            let next = expected.set_scheduling(expected.revision(), enabled)?;
            db.set_registry(next)?;
            Ok(next)
        }))
    }
}

fn page_limit(limit: usize) -> JournalResult<()> {
    if limit == 0 || limit > MAX_PAGE_ENTRIES {
        return Err(OperationError::Invalid("invalid registry page limit").into());
    }
    Ok(())
}
