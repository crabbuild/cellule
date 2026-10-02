//! One retained enrollment attempt, with no native replay after ambiguous dispatch.
use super::*;

impl FleetFollowerEnrollment {
    async fn specs(
        &self,
        inputs: &FleetNodeLogRecruitment,
    ) -> cellule_runtime::Result<Vec<EnrollmentSpec>> {
        let prepared = inputs.attempt.prepared();
        if prepared.source().fleet() != self.scope.fleet
            || prepared.source().node() != self.node
            || prepared.source().session() != self.session
        {
            return Err(Error::Fenced);
        }
        let version = self
            .journal
            .load_snapshot(self.scope)
            .await
            .map_err(journal_error)?
            .registry();
        if version.scope() != self.scope {
            return Err(Error::Fenced);
        }
        let mut endpoints = std::collections::HashMap::new();
        let mut after = None;
        let mut count = 0;
        loop {
            let page = self
                .journal
                .intents_page(version, after, 128)
                .await
                .map_err(journal_error)?;
            if page.version() != version || page.after() != after {
                return Err(Error::Fenced);
            }
            count += page.entries().len();
            if count > 10_000 {
                return Err(Error::Capacity("follower intent inventory bound"));
            }
            for intent in page.entries() {
                if (intent.node() == self.node || prepared.log().members().contains(&intent.node()))
                    && endpoints
                        .insert(
                            intent.node(),
                            EnrollmentEndpoint {
                                node: intent.node(),
                                session: intent.session(),
                                intent_revision: intent.revision(),
                            },
                        )
                        .is_some()
                {
                    return Err(Error::Fenced);
                }
            }
            if endpoints.len() == prepared.followers().len() + 1 || page.next().is_none() {
                break;
            }
            if page.next().map(|node| *node.as_bytes()) <= after.map(|node| *node.as_bytes()) {
                return Err(Error::Fenced);
            }
            after = page.next();
        }
        let source = endpoints.get(&self.node).copied().ok_or(Error::Fenced)?;
        if source.session != self.session {
            return Err(Error::Fenced);
        }
        prepared
            .followers()
            .iter()
            .map(|member| {
                let target = endpoints
                    .get(&member.node())
                    .copied()
                    .ok_or(Error::Fenced)?;
                if target.session != member.session() {
                    return Err(Error::Fenced);
                }
                Ok(EnrollmentSpec {
                    scope: self.scope,
                    source: Some(source),
                    target,
                    role: EnrollmentRole::Follower {
                        log_epoch: prepared.log().epoch(),
                    },
                    request: Digest::from_bytes(
                        *blake3::hash(uuid::Uuid::now_v7().as_bytes()).as_bytes(),
                    ),
                })
            })
            .collect()
    }

    pub(super) async fn recruit_owned(
        &self,
        limits: ReplicaLimits,
        required_follower_bytes: u64,
        live_node_limit: usize,
    ) -> cellule_runtime::Result<Option<NodeDurabilityConfig>> {
        let _protocol = self.protocol.lock().await;
        let pending = self
            .bank
            .lock()
            .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?
            .pending
            .clone();
        let record = match pending {
            Some(record) => record,
            None => {
                if self.cancellation.is_cancelled()
                    || self.runtime.node_admission().startup_held()?
                {
                    return Ok(None);
                }
                let Some(inputs) = self
                    .provider
                    .clone()
                    .prepare(limits, required_follower_bytes, live_node_limit)
                    .await
                    .map_err(|source| Error::Facility {
                        name: "fleet-follower-inputs",
                        source,
                    })?
                else {
                    return Ok(None);
                };
                inputs
                    .config(limits, inputs.authority.clone())?
                    .validate()?;
                let specs = self.specs(&inputs).await?;
                let reservation = self
                    .runtime
                    .try_reserve_node_bytes(16 * MAX_RECORD_BYTES as usize)?;
                let epoch = inputs.attempt.prepared().log().epoch();
                let mut bank = self
                    .bank
                    .lock()
                    .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?;
                if bank.draining || self.cancellation.is_cancelled() {
                    return Ok(None);
                }
                if epoch <= bank.last_epoch || bank.epochs.len() >= 32 {
                    return Err(Error::Fenced);
                }
                let record = Arc::new(Responsibility {
                    inputs,
                    limits,
                    closing: AsyncMutex::new(()),
                    bank: Arc::downgrade(&self.bank),
                    data: StdMutex::new(Progress {
                        members: specs
                            .into_iter()
                            .map(|spec| FollowerEnrollmentMember {
                                spec,
                                accepted: None,
                                event: None,
                                published: false,
                            })
                            .collect(),
                        native_started: false,
                        no_effect: false,
                        delivered: false,
                        enrollment: None,
                        refusal: None,
                        retirement: None,
                        native_closed: false,
                        execution_error: None,
                        journal_error: None,
                        reservation: Some(reservation),
                        cleanup: None,
                    }),
                });
                bank.last_epoch = epoch;
                bank.epochs.insert(epoch, record.clone());
                bank.pending = Some(record.clone());
                record
            }
        };
        if let Err(error) = self.step(&record).await {
            return Err(retained(record.remember_unrecorded(error)?));
        }
        if record.progress()?.no_effect {
            self.clear_pending(&record)?;
            return Ok(None);
        }
        let config = self.configuration(&record, limits)?;
        if self.cancellation.is_cancelled() {
            drop(config);
            self.close_unused(&record).await?;
            self.clear_pending(&record)?;
            return Ok(None);
        }
        record.progress()?.delivered = true;
        self.clear_pending(&record)?;
        Ok(Some(config))
    }

    fn configuration(
        &self,
        record: &Arc<Responsibility>,
        limits: ReplicaLimits,
    ) -> cellule_runtime::Result<NodeDurabilityConfig> {
        record.inputs.config(
            limits,
            Arc::new(authority::EnrollmentAuthority {
                record: Arc::downgrade(record),
                journal: self.journal.clone(),
                interval: self.interval,
            }),
        )
    }

    async fn close_unused(&self, record: &Arc<Responsibility>) -> cellule_runtime::Result<()> {
        if record.progress()?.cleanup.is_none() {
            let cleanup = self.configuration(record, record.limits)?.build()?;
            record.progress()?.cleanup = Some(cleanup);
        }
        let cleanup = record
            .progress()?
            .cleanup
            .clone()
            .ok_or(Error::Node("follower cleanup owner missing"))?;
        if let Err(error) = cleanup.shutdown_for_maintenance().await {
            return Err(retained(record.remember(error, false)?));
        }
        record.progress()?.cleanup.take();
        Ok(())
    }

    fn clear_pending(&self, record: &Arc<Responsibility>) -> cellule_runtime::Result<()> {
        let mut bank = self
            .bank
            .lock()
            .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?;
        if !bank
            .pending
            .as_ref()
            .is_some_and(|pending| Arc::ptr_eq(pending, record))
        {
            return Err(Error::Fenced);
        }
        bank.pending = None;
        Ok(())
    }

    async fn step(&self, record: &Arc<Responsibility>) -> cellule_runtime::Result<()> {
        if !record.progress()?.native_started && self.cancellation.is_cancelled() {
            record.progress()?.no_effect = true;
        }
        if record.progress()?.no_effect {
            return self.refuse_unexecuted(record).await;
        }
        if !record.progress()?.native_started {
            let members = record.progress()?.members.clone();
            for (index, member) in members.iter().enumerate() {
                let result = self
                    .journal
                    .accept_enrollment(&member.spec, now_ms()?)
                    .await;
                // Until every first-acceptance reply validates, this owner has
                // no native effect. Keep that fact even on malformed replies.
                record.progress()?.no_effect = true;
                match result {
                    Ok(FleetEnrollmentAcceptance::New(original)) => {
                        original.validate_replay(&member.spec).map_err(operation)?;
                        original.to_bytes().map_err(operation)?;
                        if original.status() != EnrollmentStatus::Pending {
                            return Err(Error::Fenced);
                        }
                        let mut progress = record.progress()?;
                        progress.members[index].accepted = Some(original);
                        progress.no_effect = false;
                    }
                    Ok(FleetEnrollmentAcceptance::Existing(original)) => {
                        original.validate_replay(&member.spec).map_err(operation)?;
                        record.progress()?.members[index].accepted = Some(original);
                        record.progress()?.no_effect = true;
                        return self.refuse_unexecuted(record).await;
                    }
                    Err(source) => {
                        record.remember(journal_error(source), true)?;
                        // The finite acceptance call has joined. No native CAS
                        // began; atomically fence any delayed acceptance itself.
                        record.progress()?.no_effect = true;
                        return self.refuse_unexecuted(record).await;
                    }
                }
            }
            if self.cancellation.is_cancelled() {
                record.progress()?.no_effect = true;
                return self.refuse_unexecuted(record).await;
            }
            record.progress()?.native_started = true;
            match record
                .inputs
                .directory
                .commit_log_enrollment(&record.inputs.attempt, now_ms()?)
                .await
            {
                Ok(proof) => record.progress()?.enrollment = Some(proof),
                Err(error) => {
                    record.remember(error, false)?;
                }
            }
        }
        if record.progress()?.enrollment.is_none() {
            match record
                .inputs
                .directory
                .inspect_log_enrollment(&record.inputs.attempt, now_ms()?)
                .await
            {
                Ok(Some(proof)) => record.progress()?.enrollment = Some(proof),
                Ok(None) => {
                    match record
                        .inputs
                        .directory
                        .fence_log_enrollment(&record.inputs.attempt, now_ms()?)
                        .await
                    {
                        Ok(refusal) => {
                            let mut progress = record.progress()?;
                            progress.refusal = Some(refusal);
                            progress.no_effect = true;
                        }
                        Err(error) => return Err(retained(record.remember(error, false)?)),
                    }
                    return self.refuse_unexecuted(record).await;
                }
                Err(error) => return Err(retained(record.remember(error, false)?)),
            }
        }
        let members = record.progress()?.members.clone();
        for (index, member) in members.iter().enumerate() {
            if member.event.is_none() {
                let event = EnrollmentEvent::Established(evidence(record, member, b"enrolled")?);
                record.progress()?.members[index].event = Some(event);
            }
        }
        publish(&self.journal, record).await
    }

    async fn refuse_unexecuted(&self, record: &Arc<Responsibility>) -> cellule_runtime::Result<()> {
        let members = record.progress()?.members.clone();
        for (index, member) in members.iter().enumerate() {
            if member.published {
                continue;
            }
            let digest = match member.event {
                Some(EnrollmentEvent::Refused(digest)) => digest,
                None => evidence(record, member, b"joined-unexecuted")?,
                _ => return Err(Error::Fenced),
            };
            record.progress()?.members[index].event = Some(EnrollmentEvent::Refused(digest));
            let result = match self
                .journal
                .refuse_unexecuted_enrollment(&member.spec, digest, now_ms()?)
                .await
            {
                Ok(result) => result,
                Err(source) => return Err(retained(record.remember(journal_error(source), true)?)),
            };
            result.validate_replay(&member.spec).map_err(operation)?;
            if result.status() != EnrollmentStatus::Refused
                || result.settlement_evidence() != Some(digest)
            {
                return Err(Error::Fenced);
            }
            let mut progress = record.progress()?;
            progress.members[index].accepted = Some(result);
            progress.members[index].published = true;
        }
        record.finished()
    }

    pub(super) async fn drain_owned(&self) -> cellule_runtime::Result<()> {
        let _protocol = self.protocol.lock().await;
        self.bank
            .lock()
            .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?
            .draining = true;
        loop {
            let record = self
                .bank
                .lock()
                .map_err(|_| Error::Node("follower enrollment bank lock poisoned"))?
                .pending
                .clone();
            let Some(record) = record else {
                return Ok(());
            };
            // The supervisor is joined. Only its original undelivered attempt
            // can remain; delivered epochs close later through runtime drain.
            if !record.progress()?.native_started {
                record.progress()?.no_effect = true;
            }
            let result = async {
                self.step(&record).await?;
                if !record.progress()?.no_effect {
                    // No Cell ever used this undelivered configuration. Its
                    // canonical gate starts at zero; retire every cold member.
                    self.close_unused(&record).await?;
                }
                self.clear_pending(&record)
            }
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    record.remember_unrecorded(error)?;
                }
            }
            tokio::time::sleep(self.interval).await;
        }
    }
}

pub(super) async fn publish(
    journal: &Arc<dyn FleetJournal>,
    record: &Responsibility,
) -> cellule_runtime::Result<()> {
    let members = record.progress()?.members.clone();
    for (index, member) in members.iter().enumerate() {
        if member.published {
            continue;
        }
        let original = member
            .accepted
            .as_ref()
            .ok_or(Error::Node("follower acceptance remains unknown"))?;
        let event = member
            .event
            .ok_or(Error::Node("follower event remains unknown"))?;
        let result = match journal
            .publish_enrollment_result(original, event, now_ms()?)
            .await
        {
            Ok(result) => result,
            Err(source) => return Err(retained(record.remember(journal_error(source), true)?)),
        };
        result.validate_replay(&member.spec).map_err(operation)?;
        result.to_bytes().map_err(operation)?;
        let agrees = match event {
            EnrollmentEvent::Established(digest) => {
                result.status() == EnrollmentStatus::Established
                    && result.established_evidence() == Some(digest)
            }
            EnrollmentEvent::Retired(digest) => {
                result.status() == EnrollmentStatus::Retired
                    && result.settlement_evidence() == Some(digest)
            }
            EnrollmentEvent::Refused(_) => false,
        };
        if result.accepted_at_ms() != original.accepted_at_ms() || !agrees {
            return Err(Error::Fenced);
        }
        record.progress()?.members[index].published = true;
    }
    Ok(())
}
