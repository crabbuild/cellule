//! Real native prefixes in a partial observation; no capacity/maintenance claim.
use super::*;
use cellule_host::fleet::{
    FleetActionCompletion, FleetObservation, FleetObserver, FleetOwnedCell, FleetReconciler,
    FleetRoster, FleetTransport,
};
use cellule_runtime::fleet::operations::{
    DrainBlocker, FleetAction, FleetInspectionObservation, FleetInspectionRequest,
};
use cellule_runtime::node::{NodeMode, NodeOperationalSample, NodePlacementCapacity, NodePressure};

impl SuccessorFixture {
    async fn collect_current(
        &self,
    ) -> cellule_runtime::Result<FleetOriginalWriterSuccessorInventory> {
        FleetOriginalWriterSuccessorInventory::collect(
            self.original.base.journal.as_ref(),
            &self.original.base.directory,
            &Processes::new(self.original.base.process_path()),
            &self.original.base.manifests,
            &self.provider,
            &self.original.request,
            session(1),
            deadline(),
            crate::scenario::clock,
        )
        .await
    }

    async fn observation_parts(
        &self,
        inventory: &FleetOriginalWriterSuccessorInventory,
    ) -> (Vec<NodeAdvertisement>, Vec<FleetOwnedCell>) {
        let mut nodes = Vec::new();
        for index in 1..3 {
            let mut now = crate::scenario::clock().unwrap();
            let current = self
                .original
                .base
                .directory
                .load_if_live(session(index), now)
                .await
                .unwrap()
                .unwrap();
            let original = current.advertisement();
            while now <= original.issued_at_ms() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                now = crate::scenario::clock().unwrap();
            }
            let key = SigningKey::from_bytes(&[index as u8 + 1; 32]);
            // This partial fixture declares the existing boot's placement
            // envelope; it cannot claim a measured pressure or complete graph.
            let ad = NodeAdvertisement::sign(
                original.node(),
                original.session(),
                original.endpoint().to_owned(),
                original.fleet(),
                original.certificate(),
                original.image(),
                original.release(),
                &key,
                original.progress(),
                now,
                now + 10_000,
                original.module_digests().to_vec(),
                original.peer_versions().to_vec(),
                original.failure_domain().clone(),
                original.capacity(),
            )
            .unwrap()
            .with_operational_placement(
                NodePlacementCapacity {
                    memory_capacity_bytes: 1 << 20,
                    disk_capacity_bytes: 1 << 20,
                    active_cells: if index == 1 {
                        self.node.stats().active_cells() as u32
                    } else {
                        0
                    },
                    max_active_cells: 128,
                    job_capacity: 4,
                    ..Default::default()
                },
                NodeOperationalSample {
                    sequence: original
                        .operational_sample()
                        .map_or(1, |sample| sample.sequence + 1),
                    observed_at_ms: now,
                    mode: NodeMode::Active,
                    pressure: NodePressure::Normal,
                },
                &key,
            )
            .unwrap();
            let refreshed = self
                .original
                .base
                .directory
                .refresh(&current, ad, now)
                .await
                .unwrap();
            nodes.push(refreshed.advertisement().clone());
        }
        let cells = inventory
            .proofs()
            .iter()
            .filter(|proof| proof.original().target.application() == scope().application)
            .map(|proof| FleetOwnedCell {
                node: proof.node(),
                session: proof.serving().owner().session,
                observation: proof.serving().native().clone(),
            })
            .collect();
        (nodes, cells)
    }
    fn observation(
        &self,
        inventory: &FleetOriginalWriterSuccessorInventory,
        nodes: Vec<NodeAdvertisement>,
        cells: Vec<FleetOwnedCell>,
        complete: bool,
    ) -> FleetObservation {
        FleetObservation::new(
            scope(),
            inventory.original().snapshot().registry(),
            inventory.original().snapshot().registry().revision(),
            inventory.interval().0,
            crate::scenario::clock().unwrap(),
            complete,
            nodes,
            cells,
        )
        .unwrap()
    }
}

async fn fixture() -> SuccessorFixture {
    let fixture = SuccessorFixture::new_at(crate::scenario::clock().unwrap() - 10_005).await;
    let snapshot = fixture
        .original
        .base
        .journal
        .load_snapshot(scope())
        .await
        .unwrap();
    fixture
        .original
        .base
        .journal
        .set_scheduling(snapshot.registry(), true)
        .await
        .unwrap();
    fixture
}

#[tokio::test]
async fn original_writer_observation_retains_all_applications_and_refuses_duplicate_attachment() {
    let fixture = fixture().await;
    let inventory = fixture.collect_current().await.unwrap();
    let second = fixture.collect_current().await.unwrap();
    let (nodes, cells) = fixture.observation_parts(&inventory).await;
    assert_eq!(cells.len(), 2);
    let observation = fixture
        .observation(&inventory, nodes, cells, false)
        .with_original_writer_successors(inventory)
        .unwrap();
    assert_eq!(
        observation
            .original_writer_successors()
            .unwrap()
            .proofs()
            .len(),
        4
    );
    assert_eq!(
        observation
            .original_writer_successors()
            .unwrap()
            .original()
            .writers()
            .record()
            .catalogs()
            .len(),
        2
    );
    assert!(matches!(
        observation.with_original_writer_successors(second),
        Err(Error::Control(
            "original writer successors already retained"
        ))
    ));
    fixture.close().await;
}

#[tokio::test]
async fn original_writer_observation_refuses_changed_native_rows_and_complete_omission() {
    let fixture = fixture().await;
    for field in 0..8 {
        let inventory = fixture.collect_current().await.unwrap();
        let (nodes, mut cells) = fixture.observation_parts(&inventory).await;
        let row = &mut cells[0];
        match field {
            0 => {
                row.node = node_id(2);
                row.session = session(2);
            }
            1 => row.observation.generation += 1,
            2 => row.observation.position = None,
            3 => {
                row.observation.position.as_mut().unwrap().root.digest =
                    Digest::from_bytes([99; 32])
            }
            4 => row.observation.incarnation = IncarnationId::from_bytes([99; 16]),
            5 => row.observation.code = Digest::from_bytes([99; 32]),
            6 => row.observation.schema += 1,
            _ => row.observation.role = CatalogRole::Blob,
        }
        assert!(
            matches!(
                fixture
                    .observation(&inventory, nodes, cells, false)
                    .with_original_writer_successors(inventory),
                Err(Error::Node("original writer successor row differs"))
            ),
            "field {field}"
        );
    }
    let inventory = fixture.collect_current().await.unwrap();
    let (nodes, mut cells) = fixture.observation_parts(&inventory).await;
    cells.pop();
    assert!(matches!(
        fixture
            .observation(&inventory, nodes, cells, true)
            .with_original_writer_successors(inventory),
        Err(Error::Node("original writer successor row is missing"))
    ));
    fixture.close().await;
}

#[tokio::test]
async fn original_writer_observation_refuses_missing_physical_boot_and_interval_restamping() {
    let fixture = fixture().await;
    let inventory = fixture.collect_current().await.unwrap();
    let (mut nodes, _) = fixture.observation_parts(&inventory).await;
    nodes.retain(|ad| ad.node() != node_id(1));
    assert!(matches!(
        fixture
            .observation(&inventory, nodes, Vec::new(), false)
            .with_original_writer_successors(inventory),
        Err(Error::Node("original writer successor boot differs"))
    ));
    for later_start in [false, true] {
        let inventory = fixture.collect_current().await.unwrap();
        let (start, end) = inventory.interval();
        let observation = FleetObservation::new(
            scope(),
            inventory.original().snapshot().registry(),
            1,
            if later_start { start + 1 } else { start - 1 },
            if later_start {
                crate::scenario::clock().unwrap().max(start + 1)
            } else {
                end - 1
            },
            false,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert!(matches!(
            observation.with_original_writer_successors(inventory),
            Err(Error::Node("original writer observation barrier differs"))
        ));
    }
    fixture.close().await;
}

struct Observer {
    fixture: Arc<SuccessorFixture>,
    stale: Mutex<Option<FleetObservation>>,
    reads: AtomicUsize,
}
impl FleetObserver for Observer {
    fn observe<'a>(
        &'a self,
        _: &'a FleetRoster,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::AcqRel);
            if let Some(stale) = self.stale.lock().unwrap().take() {
                return Ok(stale);
            }
            let inventory = self.fixture.collect_current().await?;
            let (nodes, cells) = self.fixture.observation_parts(&inventory).await;
            Ok(self
                .fixture
                .observation(&inventory, nodes, cells, false)
                .with_original_writer_successors(inventory)?)
        })
    }
}
struct FailedSource;
impl FleetTransport for FailedSource {
    fn dispatch<'a>(
        &'a self,
        _: &'a FleetAction,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetActionCompletion>> {
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "original physical source has stopped",
            )
            .into())
        })
    }
    fn inspect<'a>(
        &'a self,
        _: &'a FleetInspectionRequest,
        _: Instant,
    ) -> FleetAdapterFuture<'a, Arc<FleetInspectionObservation>> {
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "original physical source has stopped",
            )
            .into())
        })
    }
}

#[tokio::test]
async fn original_writer_observation_reconciler_consumes_fresh_proofs_without_granting_settlement()
{
    let fixture = Arc::new(fixture().await);
    let observer = Arc::new(Observer {
        fixture: fixture.clone(),
        stale: Mutex::new(None),
        reads: AtomicUsize::new(0),
    });
    let driver = FleetReconciler::new(
        scope(),
        session(1),
        FleetProfile::default(),
        fixture.original.base.journal.clone(),
        observer.clone(),
        Arc::new(FailedSource),
    )
    .unwrap();
    let report = driver
        .reconcile_once(crate::scenario::clock, deadline())
        .await
        .unwrap();
    assert_eq!(observer.reads.load(Ordering::Acquire), 1);
    assert_eq!(report.allocated, 0);
    assert!(
        report
            .blockers
            .contains(&DrainBlocker::IncompleteObservation)
    );
    assert!(report.maintenance_failure.is_some());
    assert_ne!(
        report.snapshot.head().maintenance().unwrap().phase(),
        cellule_runtime::fleet::operations::MaintenancePhase::Completed
    );
    drop(driver);
    drop(observer);
    Arc::try_unwrap(fixture).ok().unwrap().close().await;
}

#[tokio::test]
async fn original_writer_observation_reconciler_refuses_stale_full_head_with_unchanged_registry() {
    let fixture = Arc::new(fixture().await);
    let inventory = fixture.collect_current().await.unwrap();
    let old = inventory.original().snapshot().clone();
    let (nodes, cells) = fixture.observation_parts(&inventory).await;
    let stale = fixture
        .observation(&inventory, nodes, cells, false)
        .with_original_writer_successors(inventory)
        .unwrap();
    let observer = Arc::new(Observer {
        fixture: fixture.clone(),
        stale: Mutex::new(Some(stale)),
        reads: AtomicUsize::new(0),
    });
    let driver = FleetReconciler::new(
        scope(),
        session(1),
        FleetProfile::default(),
        fixture.original.base.journal.clone(),
        observer.clone(),
        Arc::new(FailedSource),
    )
    .unwrap();
    assert!(matches!(
        driver
            .reconcile_once(crate::scenario::clock, deadline())
            .await,
        Err(Error::Node("original writer observation barrier differs"))
    ));
    let now = fixture
        .original
        .base
        .journal
        .load_snapshot(scope())
        .await
        .unwrap();
    assert_eq!(now.registry(), old.registry());
    assert_ne!(now.head(), old.head());
    assert!(now.head().attempts().is_empty());
    drop(driver);
    drop(observer);
    Arc::try_unwrap(fixture).ok().unwrap().close().await;
}

#[tokio::test]
async fn original_writer_observation_digest_changes_with_actual_native_publication() {
    let fixture = SuccessorFixture::new().await;
    let before = fixture.collect(deadline()).await.unwrap().digest().unwrap();
    assert_eq!(
        before,
        fixture.collect(deadline()).await.unwrap().digest().unwrap()
    );
    let handle = fixture.provider.handles.values().next().unwrap();
    let now = crate::scenario::clock().unwrap();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([93; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([94; 32]),
            now,
            64,
            64,
            |tx| {
                tx.execute("UPDATE counter SET value=value+1", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
    assert_ne!(
        before,
        fixture.collect(deadline()).await.unwrap().digest().unwrap()
    );
    fixture.close().await;
}
