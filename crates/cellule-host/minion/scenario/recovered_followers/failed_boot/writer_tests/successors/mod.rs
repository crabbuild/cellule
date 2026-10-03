//! Real current native successors; process joining remains a lifetime stand-in.
use super::*;
use cellule_host::fleet::{
    FleetOriginalWriterSuccessorInputs, FleetOriginalWriterSuccessorInventory,
    FleetOriginalWriterSuccessors,
};
use cellule_runtime::{
    cell::actor::CellHandle, fleet::operations::OriginalWriterObservation, ltx::CellObjectKind,
};
use std::collections::HashMap;

mod inherited;
mod observation;

struct Successors {
    cells: HashMap<CellId, FleetOriginalWriterSuccessorInputs>,
    handles: HashMap<CellId, CellHandle>,
    reads: AtomicUsize,
    first: Mutex<Option<CellId>>,
    change_first: bool,
    wrong_node: bool,
    refuse: bool,
}
impl FleetOriginalWriterSuccessors for Successors {
    fn successor<'a>(
        &'a self,
        original: &'a OriginalWriterObservation,
        _: &'a FleetJournalSnapshot,
    ) -> FleetAdapterFuture<'a, FleetOriginalWriterSuccessorInputs> {
        Box::pin(async move {
            let read = self.reads.fetch_add(1, Ordering::AcqRel);
            if self.refuse {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "successor backend authentication failed",
                )
                .into());
            }
            if read == 0 {
                *self.first.lock().unwrap() = Some(original.target.cell_id());
            }
            if read == 1 && self.change_first {
                let cell = self.first.lock().unwrap().unwrap();
                let now = crate::scenario::clock().unwrap();
                self.handles[&cell]
                    .execute(
                        MutationIdentity {
                            request_id: RequestId::from_bytes([91; 16]),
                            issued_at_ms: now,
                            expires_at_ms: now + 60_000,
                        },
                        Digest::from_bytes([92; 32]),
                        now,
                        64,
                        64,
                        |tx| {
                            tx.execute("UPDATE counter SET value = value + 1", [])?;
                            Ok(HandlerOutcome::Success(Vec::new()))
                        },
                    )
                    .await?;
            }
            let mut inputs = self
                .cells
                .get(&original.target.cell_id())
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "original successor is missing",
                    )
                })?
                .clone();
            if self.wrong_node {
                inputs.node = node_id(2);
            }
            Ok(inputs)
        })
    }
}

struct SuccessorFixture {
    original: WriterFixture,
    node: Arc<CellNode>,
    provider: Successors,
    slots: Arc<tokio::sync::Semaphore>,
}
impl SuccessorFixture {
    async fn new() -> Self {
        Self::new_at(NOW).await
    }
    async fn new_at(now: i64) -> Self {
        let original = WriterFixture::with_suffixes_and_takeover_at(2, true, false, now).await;
        Self::from_original(original).await
    }
    async fn from_original(original: WriterFixture) -> Self {
        original.retain().await;
        let roster = original.base.roster().await;
        let intent = roster
            .intents()
            .iter()
            .find(|row| row.node() == node_id(1))
            .unwrap()
            .clone();
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let node = Arc::new(
            CellNodeBuilder::new(crate::scenario::application::compile().unwrap())
                .with_runtime(SqlWorkerPool::new(2, 8).unwrap(), 128 << 20)
                .with_replica_host(Host::default().with_io_slots(slots.clone()))
                .with_session(session(1))
                .with_fleet_startup_intent(intent.clone())
                .build()
                .unwrap(),
        );
        node.install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        let now = crate::scenario::clock().unwrap();
        node.install_node_lease_for_startup(NodeLeaseGuard::new(now, now + 60_000).unwrap())
            .unwrap();
        node.confirm_fleet_startup(
            original.base.journal.as_ref(),
            startup::spec(&intent).unwrap().key().unwrap(),
        )
        .await
        .unwrap();
        node.start().unwrap();
        let limits = Limits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..Limits::default()
        };
        let mut cells = HashMap::new();
        let mut handles = HashMap::new();
        let takeover = original
            .base
            .directory
            .takeover_proof(session(0), session(1), original.check)
            .await
            .unwrap()
            .unwrap();
        let row = original.capture().await.unwrap();
        for control in &original.expected {
            let historical = row
                .pages()
                .iter()
                .flat_map(|page| page.entries())
                .find(|row| row.control.cell == control.cell)
                .unwrap();
            let layout = original
                .layouts
                .iter()
                .find(|layout| {
                    layout.application_id() == historical.target.application().as_bytes()
                })
                .unwrap();
            let catalog = CellCatalog::new(layout.clone(), historical.target.tenant())
                .lookup(control.cell)
                .await
                .unwrap()
                .unwrap();
            let authority = CellAuthority::new(layout.clone());
            let replica = CellReplica::new(
                layout.clone(),
                *control.cell.as_bytes(),
                *control.incarnation.as_bytes(),
                limits,
            )
            .unwrap();
            let manifests = RecoveryManifestStore::new(
                CellStorageLayout::new(
                    original.base.recovery_layout.store().clone(),
                    ObjectPath::from("recovered-enrollment"),
                    *historical.target.application().as_bytes(),
                ),
                limits,
            );
            let current = authority.load(control.cell).await.unwrap().unwrap();
            let path = original
                .base
                ._root
                .path()
                .join(format!("successor-{}.sqlite", handles.len()));
            let handle = if current.value().root.is_some() {
                node.runtime()
                    .takeover_restored(
                        catalog.clone(),
                        replica.clone(),
                        authority.clone(),
                        current,
                        takeover.clone(),
                        manifests.clone(),
                        path,
                        owner(1),
                    )
                    .await
                    .unwrap()
            } else {
                node.runtime().takeover_unpublished(catalog.clone(), replica.clone(), authority.clone(), current, takeover.clone(), path, owner(1),
                    |tx| { tx.execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)")?; Ok(()) }).await.unwrap()
            };
            handles.insert(control.cell, handle);
            cells.insert(
                control.cell,
                FleetOriginalWriterSuccessorInputs {
                    node: node_id(1),
                    host: node.clone(),
                    catalog,
                    authority,
                    replica,
                    manifests: manifests.clone(),
                },
            );
        }
        Self {
            original,
            node,
            slots,
            provider: Successors {
                cells,
                handles,
                reads: AtomicUsize::new(0),
                first: Mutex::new(None),
                change_first: false,
                wrong_node: false,
                refuse: false,
            },
        }
    }
    async fn collect(
        &self,
        deadline: Instant,
    ) -> cellule_runtime::Result<FleetOriginalWriterSuccessorInventory> {
        FleetOriginalWriterSuccessorInventory::collect(
            self.original.base.journal.as_ref(),
            &self.original.base.directory,
            &Processes::new(self.original.base.process_path()),
            &self.original.base.manifests,
            &self.provider,
            &self.original.request,
            session(1),
            deadline,
            || Ok(self.original.check),
        )
        .await
    }
    async fn close(self) {
        self.node.shutdown().await.unwrap();
        assert_eq!(self.node.stats().retained_bytes(), 0);
        assert_eq!(self.node.stats().active_cells(), 0);
        self.original.base.journal.close().await.unwrap();
    }
}

#[tokio::test]
async fn complete_original_successors_verify_every_rootless_owner_and_exact_sealed_suffix() {
    let fixture = SuccessorFixture::new().await;
    let before = fixture
        .original
        .base
        .journal
        .load_snapshot(scope())
        .await
        .unwrap();
    let inventory = fixture.collect(deadline()).await.unwrap();
    assert_eq!(inventory.proofs().len(), 4);
    assert_eq!(
        inventory
            .proofs()
            .iter()
            .map(|proof| proof.suffixes().len())
            .sum::<usize>(),
        2
    );
    assert_eq!(
        inventory
            .proofs()
            .iter()
            .filter(|proof| proof.original().control.root.is_none())
            .count(),
        2
    );
    let applications = inventory
        .proofs()
        .iter()
        .map(|proof| *proof.original().target.application().as_bytes())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(applications.len(), 2);
    for proof in inventory.proofs() {
        assert_eq!(proof.node(), node_id(1));
        assert_eq!(proof.serving().owner().session, session(1));
        assert!(proof.serving().position().epoch > proof.original().control.epoch);
        assert!(proof.origin().dependency_count() > 0);
        assert_eq!(
            proof.origin().root(),
            proof.serving().position().root.to_ltx(
                proof.original().control.cell,
                proof.original().control.incarnation
            )
        );
        let value = fixture.provider.handles[&proof.original().control.cell]
            .query(8, 8, |db| {
                Ok(db
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            })
            .await
            .unwrap();
        assert_eq!(
            value,
            if proof.suffixes().is_empty() {
                0_i64
            } else {
                2_i64
            }
            .to_be_bytes()
        );
        for suffix in proof.suffixes() {
            assert_eq!(suffix.required().cell_epoch, proof.original().control.epoch);
            assert_eq!(suffix.required().recovery.final_commit_sequence, 2);
        }
    }
    assert_eq!(
        fixture
            .original
            .base
            .journal
            .load_snapshot(scope())
            .await
            .unwrap(),
        before
    );
    await_native_release(&fixture.node).await;
    drop(inventory);
    fixture.close().await;
}

#[tokio::test]
async fn complete_original_successors_refuse_missing_or_unauthenticated_provider_rows() {
    for authenticate in [false, true] {
        let mut fixture = SuccessorFixture::new().await;
        if authenticate {
            fixture.provider.refuse = true;
        } else {
            fixture
                .provider
                .cells
                .remove(&fixture.original.expected[0].cell);
        }
        let error = fixture.collect(deadline()).await.err().unwrap();
        let Error::Facility { source, .. } = error else {
            panic!("original provider error was lost");
        };
        let original = source.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(
            original.kind(),
            if authenticate {
                std::io::ErrorKind::PermissionDenied
            } else {
                std::io::ErrorKind::NotFound
            }
        );
        fixture.close().await;
    }
}

#[tokio::test]
async fn complete_original_successors_refuse_changed_writer_during_other_prefix_collection() {
    let mut fixture = SuccessorFixture::new().await;
    fixture.provider.change_first = true;
    assert!(matches!(
        fixture.collect(deadline()).await,
        Err(Error::Fenced)
    ));
    assert!(fixture.provider.reads.load(Ordering::Acquire) >= 2);
    await_native_release(&fixture.node).await;
    fixture.close().await;
}

#[tokio::test]
async fn complete_original_successors_refuse_wrong_physical_destination() {
    let mut fixture = SuccessorFixture::new().await;
    fixture.provider.wrong_node = true;
    assert!(matches!(
        fixture.collect(deadline()).await,
        Err(Error::Fenced)
    ));
    fixture.close().await;
}

#[tokio::test]
async fn complete_original_successors_read_actual_origin_despite_live_native_sqlite() {
    let fixture = SuccessorFixture::new().await;
    let inputs = fixture.provider.cells.values().next().unwrap();
    let current = inputs
        .authority
        .load(inputs.catalog.entry().cell())
        .await
        .unwrap()
        .unwrap();
    let root = current.value().ltx_root().unwrap();
    let objects = inputs.replica.reachable_objects(&root).await.unwrap();
    let missing = objects
        .iter()
        .find(|row| row.kind == CellObjectKind::Ltx)
        .unwrap();
    let captured = fixture.original.capture().await.unwrap();
    let target = captured
        .pages()
        .iter()
        .flat_map(|page| page.entries())
        .find(|row| row.control.cell == inputs.catalog.entry().cell())
        .unwrap()
        .target
        .clone();
    let layout = fixture
        .original
        .layouts
        .iter()
        .find(|layout| layout.application_id() == target.application().as_bytes())
        .unwrap();
    layout
        .store()
        .delete(&layout.incarnation_object_path(
            &root.cell,
            &root.incarnation,
            &missing.digest,
            missing.kind,
        ))
        .await
        .unwrap();
    assert!(matches!(
        fixture.collect(deadline()).await,
        Err(Error::Ltx(_))
    ));
    // Native databases can still query; their cache is not origin evidence.
    assert!(
        fixture.provider.handles[&inputs.catalog.entry().cell()]
            .query(1, 1, |_| Ok(Vec::new()))
            .await
            .is_ok()
    );
    fixture.close().await;
}

#[tokio::test]
async fn complete_original_successors_cancellation_releases_shared_origin_admission() {
    let fixture = SuccessorFixture::new().await;
    let before = fixture
        .original
        .base
        .journal
        .load_snapshot(scope())
        .await
        .unwrap();
    let permit = fixture.slots.acquire().await.unwrap();
    let mut collection = Box::pin(fixture.collect(Instant::now() + Duration::from_secs(3)));
    // Cancellation must reach actual origin admission. A fixed sleep can
    // expire during the preceding journal/actor reads on a loaded CI worker.
    // Poll both the original bounded collection and its real reservation;
    // an early return (including its unchanged deadline) fails this case.
    tokio::select! {
        outcome = &mut collection => panic!("collection completed before origin admission: {:?}", outcome.err()),
        () = async {
            while fixture.node.stats().retained_bytes() <= 16 << 20 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } => {}
    }
    assert!(fixture.node.stats().retained_bytes() > 16 << 20);
    drop(collection);
    // The large caller-owned origin metadata reservation is gone. Already
    // accepted native I/O can retain its small charge behind this test gate.
    let released = fixture
        .node
        .runtime()
        .try_reserve_node_bytes(112 << 20)
        .unwrap();
    drop(released);
    drop(permit);
    await_native_release(&fixture.node).await;
    assert_eq!(
        fixture
            .original
            .base
            .journal
            .load_snapshot(scope())
            .await
            .unwrap(),
        before
    );
    fixture.collect(deadline()).await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn complete_original_successors_expired_deadline_starts_no_provider_work() {
    let fixture = SuccessorFixture::new().await;
    assert!(matches!(
        fixture.collect(Instant::now()).await,
        Err(Error::Deadline)
    ));
    assert_eq!(fixture.provider.reads.load(Ordering::Acquire), 0);
    fixture.close().await;
}

async fn await_native_release(node: &CellNode) {
    // Dropping an observer releases origin admission immediately. Accepted
    // native worker replies still use their original finite owner; join that
    // cleanup before asserting the shared ledger is empty.
    tokio::time::timeout(Duration::from_secs(3), async {
        while node.stats().retained_bytes() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(node.stats().retained_bytes(), 0);
}
