//! Canonical activation-lane protocol with observable progress across every await.
use super::*;

impl ReaderEnrollment {
    pub(in crate::read_replicas) async fn open(
        &self,
        manager: &ReadReplicaManager,
        source: ReadReplicaSource,
        path: PathBuf,
    ) -> Result<Receipt> {
        let cell = source.description().cell;
        let existing = self.records()?.get(&cell).cloned();
        if let Some(record) = existing {
            self.publish(&record).await?;
            return Err(Error::Control(
                "reader enrollment remains unresolved; close before a new opening",
            ));
        }
        if self.records()?.len() >= MAX_READ_VIEWS {
            return Err(Error::Capacity("reader enrollment inventory bound"));
        }
        let spec = self.spec(manager, &source).await?;
        let reservation = manager
            .runtime
            .try_reserve_node_bytes(3 * MAX_RECORD_BYTES as usize)?;
        let record = Arc::new(StdMutex::new(Responsibility {
            spec,
            source: source.clone(),
            original: None,
            event: None,
            published: false,
            opening_started: false,
            opening_joined: false,
            execution_error: None,
            journal_error: None,
            _retained: reservation,
        }));
        // All protocol mutations already serialize on the manager's activation
        // lane. The index/progress locks serve short reads and writes only, so
        // inventory can see the original request during lost/paused RPC replies.
        self.records()?.insert(cell, record.clone());
        self.accept(&record).await?;
        data(&record)?.opening_started = true;
        let result = manager
            .open_source_locked(source, path)
            .await
            .map_err(Arc::new);
        {
            let mut progress = data(&record)?;
            progress.opening_joined = true;
            if let Err(error) = &result {
                progress
                    .execution_error
                    .get_or_insert_with(|| error.clone());
            }
            progress.event = Some(match result.as_ref().ok().copied() {
                Some(receipt) => {
                    EnrollmentEvent::Established(evidence(&progress, b"opened", Some(receipt))?)
                }
                None => {
                    EnrollmentEvent::Retired(evidence(&progress, b"joined-opening-refusal", None)?)
                }
            });
            progress.published = false;
        }
        let publication = self.publish(&record).await;
        if result.is_err() && publication.is_ok() {
            self.records()?.remove(&cell);
        }
        match result {
            Err(error) => Err(retained(error)),
            Ok(receipt) => {
                publication?;
                Ok(receipt)
            }
        }
    }

    async fn accept(&self, record: &Record) -> Result<()> {
        let spec = {
            let progress = data(record)?;
            if progress.original.is_some() {
                return Ok(());
            }
            progress.spec.clone()
        };
        let acceptance = self.journal.accept_enrollment(&spec, now_ms()?).await;
        let acceptance = match acceptance {
            Ok(acceptance) => acceptance,
            Err(source) => return Err(retained(data(record)?.remember_journal(source))),
        };
        match acceptance {
            FleetEnrollmentAcceptance::New(original) => {
                if original.spec() != &spec || original.status() != EnrollmentStatus::Pending {
                    return Err(Error::Fenced);
                }
                original.to_bytes().map_err(operation)?;
                data(record)?.original = Some(original);
                Ok(())
            }
            FleetEnrollmentAcceptance::Existing(original) => {
                if original.spec() != &spec {
                    return Err(Error::Fenced);
                }
                data(record)?.original = Some(original);
                Err(Error::Control(
                    "reader enrollment acceptance reply is unresolved",
                ))
            }
        }
    }

    async fn publish(&self, record: &Record) -> Result<Option<EnrollmentRecord>> {
        let (original, event) = {
            let progress = data(record)?;
            if progress.published {
                return Ok(None);
            }
            let Some(event) = progress.event else {
                return Ok(None);
            };
            let original = progress
                .original
                .clone()
                .ok_or(Error::Control("reader enrollment acceptance is unknown"))?;
            (original, event)
        };
        let result = self
            .journal
            .publish_enrollment_result(&original, event, now_ms()?)
            .await;
        let result = match result {
            Ok(result) => result,
            Err(source) => return Err(retained(data(record)?.remember_journal(source))),
        };
        result.to_bytes().map_err(operation)?;
        let agrees = match event {
            EnrollmentEvent::Established(evidence) => {
                result.status() == EnrollmentStatus::Established
                    && result.established_evidence() == Some(evidence)
            }
            EnrollmentEvent::Retired(evidence) => {
                result.status() == EnrollmentStatus::Retired
                    && result.settlement_evidence() == Some(evidence)
            }
            EnrollmentEvent::Refused(evidence) => {
                result.status() == EnrollmentStatus::Refused
                    && result.settlement_evidence() == Some(evidence)
            }
        };
        let mut progress = data(record)?;
        if !agrees
            || result.spec() != &progress.spec
            || result.accepted_at_ms() != original.accepted_at_ms()
            || progress.event != Some(event)
        {
            return Err(Error::Fenced);
        }
        progress.published = true;
        Ok(Some(result))
    }

    pub(in crate::read_replicas) async fn established(&self, cell: CellId) -> Result<()> {
        let record = self
            .records()?
            .get(&cell)
            .cloned()
            .ok_or(Error::Control("reader enrollment owner is absent"))?;
        if !matches!(data(&record)?.event, Some(EnrollmentEvent::Established(_))) {
            return Err(Error::Control("reader enrollment remains unresolved"));
        }
        self.publish(&record).await.map(|_| ())
    }

    pub(in crate::read_replicas) async fn retire(
        &self,
        cell: CellId,
        receipt: Option<Receipt>,
    ) -> Result<()> {
        self.retire_record(cell, receipt).await.map(|_| ())
    }

    pub(in crate::read_replicas) async fn retire_record(
        &self,
        cell: CellId,
        receipt: Option<Receipt>,
    ) -> Result<Option<EnrollmentRecord>> {
        let record = self.records()?.get(&cell).cloned();
        let Some(record) = record else {
            return Ok(None);
        };
        let never_started = {
            let progress = data(&record)?;
            if receipt.is_none() && progress.opening_started && !progress.opening_joined {
                return Err(Error::Control(
                    "reader native opening remains unproven after task failure",
                ));
            }
            !progress.opening_started
        };
        if never_started {
            if receipt.is_some() {
                return Err(Error::Fenced);
            }
            let (spec, accepted_at, digest) = {
                let mut progress = data(&record)?;
                let digest = match progress.event {
                    Some(EnrollmentEvent::Refused(digest)) => digest,
                    None => {
                        let mut hash = blake3::Hasher::new();
                        hash.update(b"cellule.fleet-reader-unexecuted.v1\0");
                        hash.update(&progress.spec.to_bytes().map_err(operation)?);
                        Digest::from_bytes(*hash.finalize().as_bytes())
                    }
                    _ => return Err(Error::Fenced),
                };
                progress.event = Some(EnrollmentEvent::Refused(digest));
                (
                    progress.spec.clone(),
                    progress
                        .original
                        .as_ref()
                        .map(EnrollmentRecord::accepted_at_ms),
                    digest,
                )
            };
            let original = match self
                .journal
                .refuse_unexecuted_enrollment(&spec, digest, now_ms()?)
                .await
            {
                Ok(original) => original,
                Err(source) => return Err(retained(data(&record)?.remember_journal(source))),
            };
            original.validate_replay(&spec).map_err(operation)?;
            original.to_bytes().map_err(operation)?;
            if original.status() != EnrollmentStatus::Refused
                || original.settlement_evidence() != Some(digest)
                || accepted_at.is_some_and(|time| time != original.accepted_at_ms())
            {
                return Err(Error::Fenced);
            }
            self.records()?.remove(&cell);
            return Ok(Some(original));
        }
        {
            let mut progress = data(&record)?;
            if !matches!(progress.event, Some(EnrollmentEvent::Retired(_))) {
                progress.event = Some(EnrollmentEvent::Retired(evidence(
                    &progress,
                    b"joined-closure",
                    receipt,
                )?));
                progress.published = false;
            }
        }
        let returned = match self.publish(&record).await? {
            Some(returned) => returned,
            None => {
                // Cancellation may occur after the confirmed reply but before
                // index removal. Reload the exact retained event; never reopen.
                let (original, event) = {
                    let progress = data(&record)?;
                    (
                        progress.original.clone().ok_or(Error::Fenced)?,
                        progress.event,
                    )
                };
                let returned = self
                    .journal
                    .load_enrollment(self.scope, original.spec().key().map_err(operation)?)
                    .await
                    .map_err(journal)?
                    .ok_or(Error::Fenced)?;
                if returned.spec() != original.spec()
                    || returned.accepted_at_ms() != original.accepted_at_ms()
                    || returned.status() != EnrollmentStatus::Retired
                    || event != returned.settlement_evidence().map(EnrollmentEvent::Retired)
                {
                    return Err(Error::Fenced);
                }
                returned
            }
        };
        self.records()?.remove(&cell);
        Ok(Some(returned))
    }
}

fn evidence(record: &Responsibility, phase: &[u8], receipt: Option<Receipt>) -> Result<Digest> {
    let original = record
        .original
        .as_ref()
        .ok_or(Error::Control("reader acceptance is unknown"))?;
    evidence_input(original, &record.source, phase, receipt)
}

pub(super) fn evidence_input(
    original: &EnrollmentRecord,
    source: &ReadReplicaSource,
    phase: &[u8],
    receipt: Option<Receipt>,
) -> Result<Digest> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule.fleet-reader-evidence.v1\0");
    hash.update(&original.to_bytes().map_err(operation)?);
    hash.update(phase);
    let description = source.description();
    hash.update(description.code.as_bytes());
    hash.update(&description.schema.to_be_bytes());
    hash.update(&(source.owner().endpoint.len() as u64).to_be_bytes());
    hash.update(source.owner().endpoint.as_bytes());
    if let Some(receipt) = receipt {
        let EnrollmentRole::Reader { target, position } = &original.spec().role else {
            return Err(Error::Fenced);
        };
        if receipt.cell != target.cell_id()
            || receipt.incarnation != position.incarnation
            || receipt.commit_sequence < position.root.commit_sequence
        {
            return Err(Error::Fenced);
        }
        if phase == b"opened" && receipt.commit_sequence != position.root.commit_sequence {
            return Err(Error::Fenced);
        }
        hash.update(receipt.cell.as_bytes());
        hash.update(receipt.incarnation.as_bytes());
        hash.update(&receipt.commit_sequence.to_be_bytes());
    }
    Ok(Digest::from_bytes(*hash.finalize().as_bytes()))
}
