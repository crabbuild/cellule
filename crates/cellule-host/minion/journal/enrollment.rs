use super::*;

impl FleetEnrollmentJournal for SqliteJournal {
    fn refuse_unexecuted_enrollment<'a>(
        &'a self,
        spec: &'a EnrollmentSpec,
        evidence: Digest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, EnrollmentRecord> {
        let spec = spec.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(spec.scope)?;
            let original = db.enrollment(spec.key()?)?;
            let next = match &original {
                Some(original) => {
                    original.validate_replay(&spec)?;
                    original.refuse(evidence, now_ms)?
                }
                None => EnrollmentRecord::unexecuted_refusal(spec, evidence, now_ms)?,
            };
            if original.as_ref() != Some(&next) {
                db.write_enrollment(&next)?;
            }
            Ok(next)
        }))
    }

    fn load_boot(
        &self,
        scope: FleetScope,
        node: NodeId,
        key: Digest,
    ) -> FleetAdapterFuture<'_, Option<cellule_host::fleet::FleetBootObservation>> {
        Box::pin(async move {
            let observed = self
                .run(move |db| {
                    db.check_scope(scope)?;
                    let (Some(intent), Some(enrollment)) = (db.intent(node)?, db.enrollment(key)?)
                    else {
                        return Ok(None);
                    };
                    Ok(Some(cellule_host::fleet::FleetBootObservation::new(
                        intent, enrollment,
                    )?))
                })
                .await?;
            #[cfg(test)]
            {
                // Hold only the reply after the read transaction has joined.
                // Another client can advance intent without a locked database.
                let pause = self.inner.boot_reply.lock().unwrap().take();
                if let Some(pause) = pause {
                    let _ = pause.captured.send(());
                    let _ = pause.resume.await;
                }
            }
            Ok(observed)
        })
    }
    fn register_initial_intent<'a>(
        &'a self,
        intent: &'a NodeIntent,
    ) -> FleetAdapterFuture<'a, NodeIntent> {
        let intent = intent.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(intent.scope())?;
            if NodeIntent::initial(intent.scope(), intent.node(), intent.session())? != intent {
                return Err(OperationError::Conflict.into());
            }
            if let Some(existing) = db.intent(intent.node())? {
                if existing != intent {
                    return Err(OperationError::Conflict.into());
                }
                return Ok(existing);
            }
            db.write_intent(&intent)?;
            Ok(intent)
        }))
    }
    fn rebind_active_intent<'a>(
        &'a self,
        original: &'a NodeIntent,
        session: SessionId,
        revision: u64,
    ) -> FleetAdapterFuture<'a, NodeIntent> {
        let original = original.clone();
        Box::pin(self.run(move |db| {
            db.check_scope(original.scope())?;
            let next = original.rebind_active(session, revision)?;
            let current = db.required_intent(original.node())?;
            if current == next {
                return Ok(current);
            }
            if current != original {
                return Err(OperationError::Conflict.into());
            }
            db.write_intent(&next)?;
            Ok(next)
        }))
    }
    fn return_to_service(
        &self,
        scope: FleetScope,
        node: NodeId,
        operation: OperationId,
        session: SessionId,
        revision: u64,
    ) -> FleetAdapterFuture<'_, NodeIntent> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            let completed = db.operation(operation)?.ok_or(OperationError::NotFound)?;
            let old = db.required_intent(node)?;
            let predecessor = NodeIntent::maintenance(scope, &completed)?;
            let next = predecessor.return_to_service(&completed, session, revision)?;
            if old == next {
                return Ok(old);
            }
            if old != predecessor {
                return Err(OperationError::Conflict.into());
            }
            db.write_intent(&next)?;
            Ok(next)
        }))
    }
    fn bootstrap_registry(
        &self,
        expected: RegistryVersion,
    ) -> FleetAdapterFuture<'_, RegistryVersion> {
        Box::pin(self.run(move |db| {
            db.check_version(expected)?;
            let next = expected.bootstrap(expected.revision())?;
            db.set_registry(next)?;
            Ok(next)
        }))
    }
    fn accept_enrollment<'a>(
        &'a self,
        spec: &'a EnrollmentSpec,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FleetEnrollmentAcceptance> {
        let spec = spec.clone();
        Box::pin(async move {
            #[cfg(test)]
            self.enrollment_reply(false, true).await?;
            let result = self
                .run(move |db| {
                    db.check_scope(spec.scope)?;
                    if let Some(original) = db.enrollment(spec.key()?)? {
                        original.validate_replay(&spec)?;
                        return Ok(FleetEnrollmentAcceptance::Existing(original));
                    }
                    let source = spec
                        .source
                        .map(|endpoint| db.required_intent(endpoint.node))
                        .transpose()?;
                    let source_maintenance = source
                        .as_ref()
                        .and_then(NodeIntent::operation)
                        .map(|id| -> JournalResult<MaintenanceOperation> {
                            db.operation(id)?
                                .ok_or_else(|| OperationError::NotFound.into())
                        })
                        .transpose()?;
                    let target = db.required_intent(spec.target.node)?;
                    let pending = EnrollmentRecord::pending(
                        spec,
                        source.as_ref(),
                        source_maintenance.as_ref(),
                        &target,
                        now_ms,
                    )?;
                    db.write_enrollment(&pending)?;
                    Ok(FleetEnrollmentAcceptance::New(pending))
                })
                .await?;
            #[cfg(test)]
            self.enrollment_reply(false, false).await?;
            Ok(result)
        })
    }
    fn publish_enrollment_result<'a>(
        &'a self,
        original: &'a EnrollmentRecord,
        event: EnrollmentEvent,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, EnrollmentRecord> {
        let original = original.clone();
        Box::pin(async move {
            let result = self
                .run(move |db| {
                    db.check_scope(original.spec().scope)?;
                    let current = db
                        .enrollment(original.spec().key()?)?
                        .ok_or(OperationError::NotFound)?;
                    current.validate_replay(original.spec())?;
                    if current.accepted_at_ms() != original.accepted_at_ms() {
                        return Err(OperationError::Conflict.into());
                    }
                    let next = current.apply(event, now_ms)?;
                    if next != current {
                        db.write_enrollment(&next)?;
                    }
                    Ok(next)
                })
                .await?;
            #[cfg(test)]
            self.enrollment_reply(true, false).await?;
            Ok(result)
        })
    }
    fn load_enrollment(
        &self,
        scope: FleetScope,
        key: Digest,
    ) -> FleetAdapterFuture<'_, Option<EnrollmentRecord>> {
        Box::pin(self.run(move |db| {
            db.check_scope(scope)?;
            db.enrollment(key)
        }))
    }
}
