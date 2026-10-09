use super::*;
use cellule_runtime::{
    fleet::operations::{
        MAX_ORIGINAL_WRITERS, MaintenancePhase, OriginalCatalogWitness,
        OriginalWriterInventoryBasis, OriginalWriterObservation,
    },
    identity::CellTarget,
    node::NodeMode,
};

impl FleetOriginalWriterCapture {
    /// Joins the original process through its existing provider, traverses every
    /// authenticated catalog, retains all original ownership epochs, then rechecks
    /// source configuration, process/fence, catalogs and the complete journal.
    /// Missing legacy/restore owner history is an error, never an empty set.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture(
        journal: &dyn FleetOriginalWriterJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        catalogs: &dyn FleetOriginalCatalogs,
        request: &FleetFailedBootProcessRequest,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let started = clock()?;
        let mut last = started;
        let mut now = || {
            let next = clock()?;
            interval(started, next)?;
            if next < last {
                return Err(Error::Deadline);
            }
            last = next;
            Ok(next)
        };
        bounded(deadline, async {
            let joined = request
                .confirm(journal, directory, processes, claimant, deadline, &mut now)
                .await?;
            let snapshot = joined.snapshot().clone();
            let operation = snapshot.head().maintenance().ok_or(Error::Fenced)?.clone();
            let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
            check_operation(&snapshot, &roster, request, now()?)?;
            let boot = records::boot(&roster, request.boot())?;
            let sources = catalogs
                .catalogs(request, &operation, &snapshot)
                .await
                .map_err(adapter_error)?;
            if sources.request != request.digest() || sources.operation != operation {
                return Err(Error::Fenced);
            }
            let mut owners = Vec::new();
            let mut witnesses = Vec::new();
            let mut receipts = Vec::new();
            let mut cell_count = 0;
            let mut history_count = 0;
            for source in &sources.sources {
                let catalog = CellCatalog::new(source.layout.clone(), source.tenant);
                let authority = CellAuthority::new(source.layout.clone());
                let mut scan = catalog
                    .scan_all((MAX_ORIGINAL_WRITERS - cell_count).max(1))
                    .await?;
                let before = owners.len();
                let mut histories = blake3::Hasher::new();
                histories.update(b"cellule.original-writer-catalog-history.v1\0");
                while let Some(page) = scan.next_page().await? {
                    if page.entries().len() > MAX_ORIGINAL_WRITERS - cell_count {
                        return Err(Error::Capacity(
                            "complete original catalogs exceed Cell bound",
                        ));
                    }
                    for proof in page.entries() {
                        now()?;
                        cell_count += 1;
                        let entry = proof.entry();
                        let target = CellTarget::new(
                            source.tenant,
                            source.application(),
                            entry.namespace(),
                            entry.partition(),
                        )?;
                        histories.update(target.cell_id().as_bytes());
                        if authority.load(entry.cell()).await?.is_none() {
                            // Provisioning can commit a catalog entry before its
                            // first Control. Original accepted metadata work is
                            // already joined; absence is retained explicitly.
                            histories.update(&[0]);
                            continue;
                        }
                        histories.update(&[1]);
                        let history = authority
                            .owner_history(
                                entry.cell(),
                                (MAX_ORIGINAL_WRITERS - history_count).max(1),
                            )
                            .await?;
                        if history.owners().len() > MAX_ORIGINAL_WRITERS - history_count {
                            return Err(Error::Capacity(
                                "complete original owner histories exceed bound",
                            ));
                        }
                        history_count += history.owners().len();
                        hash_control(&mut histories, history.current())?;
                        histories.update(&(history.owners().len() as u64).to_be_bytes());
                        for control in history.owners() {
                            hash_control(&mut histories, control)?;
                            if control
                                .owner
                                .as_ref()
                                .is_some_and(|owner| owner.session == request.fence().session())
                            {
                                owners.push(OriginalWriterObservation {
                                    target: target.clone(),
                                    control: control.clone(),
                                });
                            }
                        }
                    }
                }
                let receipt = scan.finish().await?;
                let mut heads = blake3::Hasher::new();
                heads.update(b"cellule.original-writer-catalog-heads.v1\0");
                heads.update(source.application().as_bytes());
                heads.update(source.tenant.as_bytes());
                for shard in 0..=u8::MAX {
                    heads.update(&[shard]);
                    heads.update(&receipt.revision(shard).to_be_bytes());
                    let pages = receipt.page_digests(shard);
                    heads.update(&(pages.len() as u64).to_be_bytes());
                    for page in pages {
                        heads.update(page.as_bytes());
                    }
                }
                witnesses.push(OriginalCatalogWitness {
                    application: source.application(),
                    tenant: source.tenant,
                    source: source.identity,
                    heads: Digest::from_bytes(*heads.finalize().as_bytes()),
                    histories: Digest::from_bytes(*histories.finalize().as_bytes()),
                    cells: receipt.entry_count() as u64,
                    owners: (owners.len() - before) as u64,
                });
                receipts.push(receipt);
            }
            if !sources.matches(
                &catalogs
                    .catalogs(request, &operation, &snapshot)
                    .await
                    .map_err(adapter_error)?,
            ) {
                return Err(Error::Control("original catalog configuration changed"));
            }
            for receipt in &receipts {
                now()?;
                receipt.revalidate().await?;
            }
            let final_join = request
                .confirm(journal, directory, processes, claimant, deadline, &mut now)
                .await?;
            if final_join.snapshot() != &snapshot || final_join.process() != joined.process() {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
            let finished = now()?;
            let (record, pages) = OriginalWriterInventoryRecord::new(
                OriginalWriterInventoryBasis {
                    operation,
                    head_digest: Digest::from_bytes(
                        *blake3::hash(&snapshot.head().to_bytes().map_err(operation_error)?)
                            .as_bytes(),
                    ),
                    registry: snapshot.registry(),
                    boot,
                    process_request: request.digest(),
                    process_witness: joined.process().witness(),
                    catalog_witness: sources.witness,
                    interval: (started, finished),
                },
                witnesses,
                owners,
            )
            .map_err(operation_error)?;
            Ok(Self {
                snapshot,
                request: request.clone(),
                process: joined.process().clone(),
                sources,
                catalogs: receipts,
                record,
                pages,
            })
        })
        .await
    }

    /// Confirms the original basis before first publication in the existing
    /// accepted journal owner. A committed exact replay returns original history
    /// without repeating provider reads or refreshing times. Success proves
    /// retention only; every current successor still requires separate evidence.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        &self,
        journal: &dyn FleetOriginalWriterJournal,
        directory: &NodeDirectory,
        processes: &dyn FleetFailedBootProcesses,
        catalogs: &dyn FleetOriginalCatalogs,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<OriginalWriterInventoryRecord> {
        bounded(deadline, async {
            let current = journal
                .load_snapshot(self.snapshot.head().scope())
                .await
                .map_err(adapter_error)?;
            if let Some(original) = FleetOriginalWriterInventory::load(
                journal,
                &current,
                self.record.basis().operation.id(),
                self.request.digest(),
                deadline,
            )
            .await?
            {
                if original.record() != &self.record || original.pages() != self.pages.as_slice() {
                    return Err(Error::Fenced);
                }
                return Ok(original.record().clone());
            }
            if current != self.snapshot {
                return Err(Error::Fenced);
            }
            let start = self.record.basis().interval.0;
            let mut last = self.record.basis().interval.1;
            let mut now = || {
                let next = clock()?;
                interval(start, next)?;
                if next < last {
                    return Err(Error::Deadline);
                }
                last = next;
                Ok(next)
            };
            let joined = self
                .request
                .confirm(journal, directory, processes, claimant, deadline, &mut now)
                .await?;
            if joined.snapshot() != &self.snapshot || joined.process() != &self.process {
                return Err(Error::Fenced);
            }
            let source_set = catalogs
                .catalogs(
                    &self.request,
                    &self.record.basis().operation,
                    &self.snapshot,
                )
                .await
                .map_err(adapter_error)?;
            if !self.sources.matches(&source_set) {
                return Err(Error::Fenced);
            }
            for receipt in &self.catalogs {
                now()?;
                receipt.revalidate().await?;
            }
            // Provider/configuration I/O cannot hide a registry or claimant change.
            let roster = FleetRoster::collect(journal, &self.snapshot, deadline).await?;
            check_operation(&self.snapshot, &roster, &self.request, now()?)?;
            self.request
                .confirm_canonical(directory, claimant, now()?, deadline)
                .await?;
            if self.request.confirm_process(processes, deadline).await? != self.process {
                return Err(Error::Fenced);
            }
            roster.confirm(journal, deadline).await?;
            self.request
                .confirm_canonical(directory, claimant, now()?, deadline)
                .await?;
            journal
                .persist_original_writers(&self.snapshot, &self.record, &self.pages, now()?)
                .await
                .map_err(adapter_error)
        })
        .await
    }
}
fn hash_control(
    hash: &mut blake3::Hasher,
    control: &cellule_runtime::control::Control,
) -> Result<()> {
    let bytes = control.encode()?;
    hash.update(&(bytes.len() as u64).to_be_bytes());
    hash.update(&bytes);
    Ok(())
}
pub(super) fn check_operation(
    snapshot: &FleetJournalSnapshot,
    roster: &FleetRoster,
    request: &FleetFailedBootProcessRequest,
    now: i64,
) -> Result<()> {
    let current = snapshot.head().maintenance().ok_or(Error::Fenced)?;
    let lease = snapshot.head().controller().ok_or(Error::Fenced)?;
    let intent = roster
        .intents()
        .iter()
        .find(|row| row.node() == current.node())
        .ok_or(Error::Fenced)?;
    records::boot(roster, request.boot())?;
    if current.phase() == MaintenancePhase::Completed
        || current.node() != request.fence().node()
        || current.session() != request.fence().session()
        || intent.session() != current.session()
        || intent.revision() != current.intent_revision()
        || intent.mode() != NodeMode::Draining
        || now >= lease.expires_at_ms
        || now >= current.deadline_ms()
    {
        return Err(Error::Fenced);
    }
    Ok(())
}
