//! Real canonical reader production against the durable reference transaction domain.
use super::*;
use cellule_host::read_replicas::ReadReplicaManager;
use cellule_runtime::{
    CellRuntime, Error,
    fleet::operations::{EnrollmentRecord, EnrollmentStatus},
    node::NodeDirectory,
    peer::PeerReplicaResolver,
};

mod inventory;
mod leases;

struct ReaderFixture {
    root: tempfile::TempDir,
    node: Arc<CellNode>,
    manager: ReadReplicaManager,
    source: CellRuntime,
    handle: CellHandle,
    target: CellTarget,
    journal: Arc<SqliteJournal>,
    layout: CellStorageLayout,
    limits: Limits,
    leases: leases::ReaderLeases,
}
impl ReaderFixture {
    async fn new() -> Self {
        Self::with_store(Store::new(Arc::new(InMemory::new()))).await
    }
    async fn with_store(store: Store) -> Self {
        Self::with_capacity(store, 8, 128 << 20).await
    }
    async fn with_capacity(store: Store, active_cells: usize, native_memory_bytes: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = application::compile().unwrap();
        let code = app.registry().module_digests()[0];
        let layout = CellStorageLayout::new(store, ObjectPath::from("enrolled-readers"), [3; 16]);
        let directory = NodeDirectory::new(
            layout.clone(),
            scope().fleet,
            Digest::from_bytes([31; 32]),
            app.registry().release_digest(),
        );
        let now = clock().unwrap();
        let leases = leases::ReaderLeases::create(
            directory.clone(),
            code,
            app.registry().release_digest(),
            30_000,
        )
        .await;
        let journal = Arc::new(
            SqliteJournal::open(
                root.path().join("journal.sqlite"),
                scope(),
                FleetProfile::default(),
                now,
            )
            .await
            .unwrap(),
        );
        for index in [0, 1] {
            journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node_id(index), session(index)).unwrap(),
                )
                .await
                .unwrap();
        }
        let limits = Limits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..Limits::default()
        };
        let node = Arc::new(
            CellNodeBuilder::new(app)
                .with_runtime(
                    SqlWorkerPool::new(2, active_cells)
                        .unwrap()
                        .with_native_memory_limit(native_memory_bytes)
                        .unwrap(),
                    active_cells << 21,
                )
                .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
                .with_session(session(1))
                .build()
                .unwrap(),
        );
        node.install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        let manager = node
            .install_read_replicas(
                layout.clone(),
                directory,
                root.path().join("readers"),
                limits,
            )
            .unwrap();
        node.install_fleet_reader_enrollment(scope(), node_id(1), journal.clone())
            .unwrap();
        assert!(
            node.install_fleet_reader_enrollment(scope(), node_id(1), journal.clone())
                .is_err()
        );
        node.install_node_lease(leases.receiver_guard().clone())
            .unwrap();
        let source_workers = SqlWorkerPool::new(2, active_cells).unwrap();
        let source_workers = if active_cells == 8 {
            source_workers
        } else {
            source_workers
                .with_native_memory_limit(native_memory_bytes)
                .unwrap()
        };
        let source = CellRuntime::new_with_replica_host(
            source_workers,
            16 << 20,
            session(0),
            Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
        )
        .unwrap();
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            scope().application,
            application::NAMESPACE,
            &[1],
        )
        .unwrap();
        let incarnation = IncarnationId::from_bytes([1; 16]);
        let catalog = CellCatalog::new(layout.clone(), target.tenant());
        let proof = catalog
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
            .await
            .unwrap();
        let authority = CellAuthority::new(layout.clone());
        let observed = authority
            .create_initial(&proof, incarnation, owner(0))
            .await
            .unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            limits,
        )
        .unwrap();
        let handle = source
            .bootstrap(
                proof,
                replica,
                authority,
                observed,
                root.path().join("source.sqlite"),
                |tx| {
                    tx.execute_batch(
                        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (17)",
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        manager.set_target(&target, 0, 1).await.unwrap().unwrap();
        Self {
            root,
            node,
            manager,
            source,
            handle,
            target,
            journal,
            layout,
            limits,
            leases,
        }
    }
    async fn additional_cell(&self, partition: u8) -> (CellTarget, CellHandle) {
        self.additional_partition(partition, 1).await
    }
    async fn additional_partition(&self, partition: u8, width: usize) -> (CellTarget, CellHandle) {
        let target = CellTarget::new(
            self.target.tenant(),
            scope().application,
            application::NAMESPACE,
            &vec![partition; width],
        )
        .unwrap();
        let code = application::compile().unwrap().registry().module_digests()[0];
        let proof = CellCatalog::new(self.layout.clone(), target.tenant())
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
            .await
            .unwrap();
        let incarnation = IncarnationId::from_bytes([partition; 16]);
        let authority = CellAuthority::new(self.layout.clone());
        let observed = authority
            .create_initial(&proof, incarnation, owner(0))
            .await
            .unwrap();
        let replica = CellReplica::new(
            self.layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            self.limits,
        )
        .unwrap();
        let handle = self
            .source
            .bootstrap(
                proof,
                replica,
                authority,
                observed,
                self.root.path().join(format!("source-{partition}.sqlite")),
                |tx| {
                    tx.execute_batch(
                        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (17)",
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        self.manager
            .set_target(&target, 0, 1)
            .await
            .unwrap()
            .unwrap();
        (target, handle)
    }
    async fn rows(&self) -> Vec<EnrollmentRecord> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let version = self
                    .journal
                    .load_snapshot(scope())
                    .await
                    .unwrap()
                    .registry();
                match self.journal.enrollments_page(version, None, 128).await {
                    Ok(page) => return page.entries().to_vec(),
                    Err(error)
                        if matches!(
                            error
                                .downcast_ref::<cellule_runtime::fleet::operations::OperationError>(
                                ),
                            Some(cellule_runtime::fleet::operations::OperationError::Conflict)
                        ) =>
                    {
                        // The owned producer may publish between the header and
                        // exact-version page. Retry a fresh consistent scan only.
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("reader registry scan failed: {error}"),
                }
            }
        })
        .await
        .unwrap()
    }
    async fn read(&self) {
        let reader = self.manager.resolve(self.target.clone()).await.unwrap();
        let observed = reader
            .query::<application::ReadValue>(Some(reader.receipt().await), 0)
            .await
            .unwrap();
        assert_eq!(observed.output, 17);
    }
    async fn finish(self) {
        self.finish_status(EnrollmentStatus::Retired).await;
    }
    async fn finish_status(self, expected: EnrollmentStatus) {
        self.node.shutdown().await.unwrap();
        assert!(self.rows().await.iter().all(|row| row.status() == expected));
        assert_eq!(self.node.stats().retained_bytes(), 0);
        assert_eq!(self.node.stats().local_disk_reserved_bytes(), 0);
        self.handle.drain().await.unwrap();
        self.source.shutdown().await.unwrap();
        self.leases.fence();
        assert_eq!(self.source.stats().resident_bytes(), 0);
        assert_eq!(self.source.stats().retained_bytes(), 0);
        assert_eq!(self.source.stats().worker_jobs(), 0);
        assert_eq!(self.source.stats().local_disk_reserved_bytes(), 0);
        self.journal.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_journals_before_open_and_owns_a_cancelled_activation() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let activation = tokio::spawn(async move { manager.activate(target, session(0)).await });
    tokio::time::timeout(Duration::from_secs(3), captured)
        .await
        .unwrap()
        .unwrap();
    let pending = fixture.rows().await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status(), EnrollmentStatus::Pending);
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert!(
        fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .is_err()
    );
    activation.abort();
    assert!(activation.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if fixture.rows().await[0].status() == EnrollmentStatus::Established {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.read().await;
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let established = fixture.rows().await;
    assert_eq!(established.len(), 1);
    assert_eq!(established[0].spec(), pending[0].spec());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_lost_acceptance_cannot_repeat_open_and_is_refused_by_joined_removal() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(false, true);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let activation = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    resume.send(()).unwrap();
    assert!(activation.await.unwrap().is_err());
    let pending = fixture.rows().await;
    assert_eq!(pending[0].status(), EnrollmentStatus::Pending);
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .accepted
            .is_none()
    );
    assert!(
        fixture
            .manager
            .activate(fixture.target.clone(), session(0))
            .await
            .is_err()
    );
    assert_eq!(fixture.rows().await, pending);
    assert!(
        fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .is_err()
    );
    fixture
        .manager
        .remove(fixture.target.cell_id())
        .await
        .unwrap();
    assert_eq!(fixture.rows().await[0].status(), EnrollmentStatus::Refused);
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .is_none()
    );
    fixture.finish_status(EnrollmentStatus::Refused).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_replays_original_establishment_and_retirement_after_lost_replies() {
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, true);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let activation = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    assert_eq!(original.status(), EnrollmentStatus::Established);
    resume.send(()).unwrap();
    assert!(activation.await.unwrap().is_err());
    let completion = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(!completion.published);
    assert!(completion.journal_error.is_some());
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    assert_eq!(fixture.rows().await, vec![original.clone()]);
    fixture.read().await;
    let peer = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    fixture.journal.lose_next_commit_reply();
    assert!(
        fixture
            .manager
            .remove(fixture.target.cell_id())
            .await
            .is_err()
    );
    let retired = fixture.rows().await[0].clone();
    assert_eq!(retired.status(), EnrollmentStatus::Retired);
    assert_eq!(
        retired.established_evidence(),
        original.established_evidence()
    );
    assert!(matches!(
        peer.query::<application::ReadValue>(None, 0).await,
        Err(Error::Fenced)
    ));
    assert_eq!(
        fixture
            .manager
            .fleet_readers_page(None, 128, clock().unwrap())
            .await
            .unwrap()
            .entries()
            .len(),
        1
    );
    fixture
        .manager
        .remove(fixture.target.cell_id())
        .await
        .unwrap();
    assert_eq!(fixture.rows().await, vec![retired.clone()]);
    let restarted = SqliteJournal::open(
        fixture.root.path().join("journal.sqlite"),
        scope(),
        FleetProfile::default(),
        clock().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        restarted
            .load_enrollment(scope(), retired.spec().key().unwrap())
            .await
            .unwrap(),
        Some(retired)
    );
    restarted.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_owns_native_open_through_cancelled_waiter_and_shutdown_deadline() {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    let runtime = Arc::new(Mutex::new(None::<CellRuntime>));
    let observed = runtime.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let armed = Arc::new(AtomicBool::new(false));
    let once = armed.clone();
    let (release, receive) = std::sync::mpsc::channel();
    let gate = Mutex::new(Some(receive));
    let fixture = ReaderFixture::with_store(
        Store::new(Arc::new(InMemory::new())).with_read_request_observer(Arc::new(move |kind| {
            if kind == cellule_store::StorageReadKind::Range
                && observed
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|runtime| runtime.stats().worker_jobs() == 1)
                && !once.swap(true, Ordering::AcqRel)
            {
                let receiver = gate.lock().unwrap().take().unwrap();
                signal.notify_one();
                let _ = receiver.recv();
            }
        })),
    )
    .await;
    *runtime.lock().unwrap() = Some(fixture.node.runtime().clone());
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let activation = tokio::spawn(async move { manager.activate(target, session(0)).await });
    let captured = tokio::time::timeout(Duration::from_secs(3), entered.notified()).await;
    if captured.is_err() {
        let _ = release.send(());
        let result = activation.await.unwrap();
        panic!("reader native VFS pause was not reached: {result:?}");
    }
    let pending = fixture.rows().await;
    assert_eq!(pending[0].status(), EnrollmentStatus::Pending);
    assert_eq!(fixture.node.stats().worker_jobs(), 1);
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(page.entries()[0].spec, *pending[0].spec());
    assert_eq!(page.entries()[0].accepted.as_ref().unwrap(), &pending[0]);
    assert!(page.entries()[0].opening_started);
    assert!(!page.entries()[0].opening_joined);
    assert!(page.entries()[0].event.is_none());
    assert_eq!(page.jobs().running(), 1);
    drop(page);
    activation.abort();
    assert!(activation.await.unwrap_err().is_cancelled());
    let drained = fixture
        .node
        .shutdown_until(std::time::Instant::now() + Duration::from_millis(30))
        .await;
    let before = fixture.node.stats();
    let state = fixture.node.state();
    let _ = release.send(());
    fixture.node.shutdown().await.unwrap();
    runtime.lock().unwrap().take();
    assert!(drained.is_err());
    assert_eq!(state, NodeState::Draining);
    assert_eq!(before.worker_jobs(), 1);
    assert_eq!(fixture.rows().await[0].status(), EnrollmentStatus::Retired);
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .is_none()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_cancelled_removal_retains_fenced_inventory_and_original_retirement() {
    let fixture = ReaderFixture::new().await;
    fixture
        .manager
        .activate(fixture.target.clone(), session(0))
        .await
        .unwrap();
    let peer = fixture
        .manager
        .resolve(fixture.target.clone())
        .await
        .unwrap();
    let (captured, resume) = fixture.journal.pause_next_enrollment_reply(true, false);
    let manager = fixture.manager.clone();
    let cell = fixture.target.cell_id();
    let removal = tokio::spawn(async move { manager.remove(cell).await });
    captured.await.unwrap();
    let original = fixture.rows().await[0].clone();
    assert_eq!(original.status(), EnrollmentStatus::Retired);
    removal.abort();
    assert!(removal.await.unwrap_err().is_cancelled());
    let _ = resume.send(());
    assert!(matches!(
        peer.query::<application::ReadValue>(None, 0).await,
        Err(Error::Fenced)
    ));
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert_eq!(fixture.node.stats().local_disk_reserved_bytes(), 0);
    assert_eq!(
        fixture
            .manager
            .fleet_readers_page(None, 128, clock().unwrap())
            .await
            .unwrap()
            .total_views(),
        1
    );
    let completion = fixture
        .manager
        .enrollment_completion(cell)
        .await
        .unwrap()
        .unwrap();
    assert!(!completion.published);
    fixture.manager.remove(cell).await.unwrap();
    assert_eq!(fixture.rows().await, vec![original]);
    assert_eq!(
        fixture
            .manager
            .fleet_readers_page(None, 128, clock().unwrap())
            .await
            .unwrap()
            .total_views(),
        0
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_cordon_refusal_preserves_native_and_lost_retirement_errors_separately() {
    let fixture = ReaderFixture::new().await;
    let (accepted, resume_acceptance) = fixture.journal.pause_next_enrollment_reply(false, false);
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let activation = tokio::spawn(async move { manager.activate(target, session(0)).await });
    accepted.await.unwrap();
    assert_eq!(fixture.rows().await[0].status(), EnrollmentStatus::Pending);
    let (published, resume_publication) = fixture.journal.pause_next_enrollment_reply(true, true);
    fixture.node.runtime().node_admission().cordon().unwrap();
    resume_acceptance.send(()).unwrap();
    published.await.unwrap();
    assert_eq!(fixture.rows().await[0].status(), EnrollmentStatus::Retired);
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert!(page.entries()[0].opening_started && page.entries()[0].opening_joined);
    assert!(matches!(
        page.entries()[0].execution_error.as_deref(),
        Some(Error::CellDraining)
    ));
    assert!(matches!(
        page.entries()[0].event,
        Some(cellule_runtime::fleet::operations::EnrollmentEvent::Retired(_))
    ));
    assert!(!page.entries()[0].published);
    assert_eq!(page.jobs().running(), 1);
    drop(page);
    resume_publication.send(()).unwrap();
    assert!(activation.await.unwrap().is_err());
    let completion = fixture
        .manager
        .enrollment_completion(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        completion.execution_error.as_deref(),
        Some(Error::CellDraining)
    ));
    assert!(completion.journal_error.is_some());
    assert!(!completion.published);
    assert_eq!(fixture.node.stats().worker_jobs(), 0);
    assert!(
        fixture
            .manager
            .resolve(fixture.target.clone())
            .await
            .is_err()
    );
    fixture
        .manager
        .remove(fixture.target.cell_id())
        .await
        .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_producer_atomic_cordon_refusal_closes_without_creating_a_retirement_request() {
    use cellule_runtime::fleet::operations::{
        JournalTransition, MaintenanceOperation, OperationId,
    };
    let fixture = ReaderFixture::new().await;
    let (captured, resume) = fixture.journal.pause_before_enrollment_acceptance();
    let manager = fixture.manager.clone();
    let target = fixture.target.clone();
    let activation = tokio::spawn(async move { manager.activate(target, session(0)).await });
    captured.await.unwrap();
    let page = fixture
        .manager
        .fleet_reader_enrollments_page(None, 128, clock().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(page.total_enrollments(), 1);
    assert!(page.entries()[0].accepted.is_none());
    assert!(!page.entries()[0].opening_started);
    assert_eq!(page.jobs().running(), 1);
    assert!(fixture.rows().await.is_empty());
    drop(page);
    let old = fixture.journal.load_snapshot(scope()).await.unwrap();
    let now = clock().unwrap();
    let controller = fixture
        .journal
        .claim_controller(
            scope(),
            old.head().revision(),
            SessionId::from_bytes([202; 16]),
            now,
        )
        .await
        .unwrap();
    fixture
        .journal
        .compare_exchange(
            &controller,
            controller.head().controller().unwrap().epoch,
            now,
            &JournalTransition::BeginMaintenance(
                MaintenanceOperation::new(
                    OperationId::from_bytes([203; 16]).unwrap(),
                    Digest::from_bytes([204; 32]),
                    node_id(1),
                    session(1),
                    2,
                    now,
                    now + 60_000,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    fixture.node.runtime().node_admission().cordon().unwrap();
    resume.send(()).unwrap();
    assert!(activation.await.unwrap().is_err());
    assert!(fixture.rows().await.is_empty());
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .accepted
            .is_none()
    );
    fixture
        .manager
        .remove(fixture.target.cell_id())
        .await
        .unwrap();
    let exclusion = fixture.rows().await;
    assert_eq!(exclusion.len(), 1);
    assert_eq!(exclusion[0].status(), EnrollmentStatus::Refused);
    let delayed = fixture
        .journal
        .accept_enrollment(exclusion[0].spec(), clock().unwrap())
        .await
        .unwrap();
    assert!(
        matches!(delayed, cellule_host::fleet::FleetEnrollmentAcceptance::Existing(row)
        if row == exclusion[0])
    );
    assert!(
        fixture
            .manager
            .enrollment_completion(fixture.target.cell_id())
            .await
            .unwrap()
            .is_none()
    );
    fixture.finish_status(EnrollmentStatus::Refused).await;
}
