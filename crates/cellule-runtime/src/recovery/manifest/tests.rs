use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

use super::*;
use crate::node::log::{RecoveryBase, build_recovery_overlays};

struct RecoveryFixture {
    inner: Arc<InMemory>,
    layout: CellStorageLayout,
    replica: cellule_ltx::CellReplica,
    manifests: RecoveryManifestStore,
    pinned: PinnedRecoveryCell,
    publication: RecoveryPublicationSummary,
    base: cellule_ltx::RootRef,
    final_position: cellule_ltx::Position,
}

async fn recovery_fixture() -> RecoveryFixture {
    recovery_fixture_with_store(None).await
}

struct PausedArtifactStore {
    started: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl RecoveryArtifactStore for PausedArtifactStore {
    fn retain(&self, _key: RecoveryArtifactKey, bundle: cellule_ltx::bundle::Bundle) -> Result<()> {
        // Ownership stays unique so the ordinary file cache can take it without
        // copying or dropping source admission before its own work has joined.
        let path = bundle.detach_file()?;
        self.started.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        std::fs::remove_file(path)?;
        Ok(())
    }
    fn load(&self, _key: &RecoveryArtifactKey) -> Result<Option<RecoveryArtifact>> {
        Ok(None)
    }
}

#[tokio::test]
async fn cancelled_pin_retains_original_scratch_admission_through_cache_job() {
    let fixture = recovery_fixture().await;
    let disk = cellule_ltx::DiskBudget::new(32 << 20);
    let loader = fixture.manifests.clone().with_recovery_disk(disk.clone());
    let pin = fixture.pinned;
    let overlay = loader
        .load_overlay(pin.cell, pin.incarnation, &pin.recovery)
        .await
        .unwrap();
    let bytes = overlay.bundle().len();
    assert_eq!(disk.used(), bytes);
    let (release, receiver) = std::sync::mpsc::channel();
    let cache = Arc::new(PausedArtifactStore {
        started: tokio::sync::Notify::new(),
        release: Mutex::new(receiver),
    });
    let manifests = fixture.manifests.with_recovery_artifacts(cache.clone());
    let leader = pin.recovery.leader_session;
    let epoch = pin.recovery.log_epoch;
    let task = tokio::spawn(async move {
        manifests
            .pin(
                leader,
                epoch,
                vec![crate::node::log::RecoveredCellTail {
                    application: *pin.application.as_bytes(),
                    cell_epoch: pin.cell_epoch,
                    first_node_sequence: pin.recovery.first_node_sequence,
                    last_node_sequence: pin.recovery.last_node_sequence,
                    overlay,
                }],
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), cache.started.notified())
        .await
        .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(
        disk.used(),
        bytes,
        "cancelled waiter cannot release accepted cache work"
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while disk.used() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn manifest_codec_covers_2000_cells_with_the_existing_byte_bound() {
    let fixture = recovery_fixture().await;
    let recovery = fixture.pinned.recovery;
    let path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let (body, _) = fixture
        .layout
        .store()
        .get_with_etag_bounded(&path, MAX_MANIFEST_BYTES)
        .await
        .unwrap();
    let original = RecoveryManifest::decode(&body).unwrap();
    let row = &original.cells[0];
    let cells = (1_u64..=2_000)
        .map(|i| {
            let mut cell = [0_u8; 32];
            cell[24..].copy_from_slice(&i.to_be_bytes());
            let mut predecessor = row.predecessor;
            predecessor.cell = cell;
            ManifestCell {
                application: row.application,
                cell,
                incarnation: row.incarnation,
                cell_epoch: row.cell_epoch,
                first_node_sequence: i,
                last_node_sequence: i,
                predecessor,
                final_position: row.final_position,
                final_commit_sequence: row.final_commit_sequence,
                bundle_digest: row.bundle_digest,
            }
        })
        .collect();
    let manifest = RecoveryManifest {
        leader_session: original.leader_session,
        log_epoch: original.log_epoch,
        cells,
    };
    let body = manifest.encode().unwrap();
    assert!((body.len() as u64) < MAX_MANIFEST_BYTES);
    assert_eq!(RecoveryManifest::decode(&body).unwrap().cells.len(), 2_000);
    let mut raw = RawManifest::from(&manifest);
    let duplicate = serde_json::to_vec(&raw.cells[0]).unwrap();
    raw.cells.resize_with(MAX_MANIFEST_CELLS + 1, || {
        serde_json::from_slice(&duplicate).unwrap()
    });
    assert!(matches!(
        RecoveryManifest::try_from(raw),
        Err(Error::Node("invalid recovery manifest shape"))
    ));
}

struct MemoryArtifactStore {
    limits: cellule_ltx::Limits,
    reject_retain: bool,
    bundles: Mutex<BTreeMap<RecoveryArtifactKey, Vec<u8>>>,
    loads: AtomicU64,
}

struct MemoryArtifactLease;

impl cellule_ltx::bundle::BundleLease for MemoryArtifactLease {}

impl RecoveryArtifactStore for MemoryArtifactStore {
    fn retain(&self, key: RecoveryArtifactKey, bundle: cellule_ltx::bundle::Bundle) -> Result<()> {
        if self.reject_retain {
            return Err(Error::Capacity("artifact test store"));
        }
        let bytes = bundle.read_all()?;
        self.bundles
            .lock()
            .map_err(|_| Error::Node("artifact test store lock poisoned"))?
            .insert(key, bytes.to_vec());
        Ok(())
    }

    fn load(&self, key: &RecoveryArtifactKey) -> Result<Option<RecoveryArtifact>> {
        let bytes = self
            .bundles
            .lock()
            .map_err(|_| Error::Node("artifact test store lock poisoned"))?
            .get(key)
            .cloned();
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        self.loads.fetch_add(1, Ordering::Relaxed);
        let bundle = cellule_ltx::bundle::Bundle::decode(bytes, self.limits)?;
        Ok(Some(RecoveryArtifact::new(
            bundle,
            Arc::new(MemoryArtifactLease),
        )))
    }
}

async fn recovery_fixture_with_store(
    artifacts: Option<Arc<dyn RecoveryArtifactStore>>,
) -> RecoveryFixture {
    recovery_fixture_with_scopes(artifacts, false).await
}

async fn recovery_fixture_with_scopes(
    artifacts: Option<Arc<dyn RecoveryArtifactStore>>,
    multiple: bool,
) -> RecoveryFixture {
    let limits = cellule_ltx::Limits::default();
    let directory = tempfile::TempDir::new().unwrap();
    let mut database =
        cellule_ltx::Db::open(&directory.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let first = database.capture().unwrap();
    let inner = Arc::new(InMemory::new());
    let store = cellule_store::Store::new(inner.clone());
    let application = [3; 16];
    let cell = [4; 32];
    let incarnation = [5; 16];
    let layout = CellStorageLayout::new(store, Path::from("root"), application);
    let replica = cellule_ltx::CellReplica::new(layout.clone(), cell, incarnation, limits).unwrap();
    let base = replica.prepare(None, &first, 1, 1).await.unwrap().root();
    database
        .transaction(|transaction| {
            transaction.execute("INSERT INTO values_ VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
    let tail = database.capture().unwrap();
    let segment = tail.segments.first().unwrap();
    let frame = cellule_ltx::encode_node_frame(
        cellule_ltx::NodeFrameScope {
            leader_session: [1; 16],
            log_epoch: 2,
            node_sequence: 1,
            application,
            cell,
            incarnation,
            cell_epoch: 6,
            commit_sequence: 2,
        },
        segment.info().clone(),
        Bytes::from(std::fs::read(segment.path()).unwrap()),
        limits,
    )
    .unwrap();
    let mut recovered = build_recovery_overlays(
        vec![frame],
        &[RecoveryBase {
            application,
            cell_epoch: 6,
            root: base,
        }],
        limits,
    )
    .unwrap();
    if multiple {
        for (application, cell, incarnation, sequence) in [
            ([3; 16], [7; 32], [8; 16], 2),
            ([9; 16], [10; 32], [11; 16], 3),
        ] {
            let layout =
                CellStorageLayout::new(layout.store().clone(), Path::from("root"), application);
            let replica = cellule_ltx::CellReplica::new(layout, cell, incarnation, limits).unwrap();
            let root = replica.prepare(None, &first, 1, 1).await.unwrap().root();
            let frame = cellule_ltx::encode_node_frame(
                cellule_ltx::NodeFrameScope {
                    leader_session: [1; 16],
                    log_epoch: 2,
                    node_sequence: sequence,
                    application,
                    cell,
                    incarnation,
                    cell_epoch: 6,
                    commit_sequence: 2,
                },
                segment.info().clone(),
                Bytes::from(std::fs::read(segment.path()).unwrap()),
                limits,
            )
            .unwrap();
            recovered.extend(
                build_recovery_overlays(
                    vec![frame],
                    &[RecoveryBase {
                        application,
                        cell_epoch: 6,
                        root,
                    }],
                    limits,
                )
                .unwrap(),
            );
        }
    }
    let manifests = RecoveryManifestStore::new(layout.clone(), limits);
    let manifests = artifacts.map_or(manifests.clone(), |store| {
        manifests.with_recovery_artifacts(store)
    });
    let pinned = manifests
        .pin_with_summary(SessionId::from_bytes([1; 16]), 2, recovered)
        .await
        .unwrap();
    let publication = pinned.summary;
    let mut pinned = pinned.cells;
    database.close().unwrap();
    RecoveryFixture {
        inner,
        layout,
        replica,
        manifests,
        pinned: pinned.remove(0),
        publication,
        base,
        final_position: tail.position,
    }
}

async fn load_error(fixture: &RecoveryFixture, recovery: &RecoveryOverlayRef) -> Error {
    match fixture
        .manifests
        .load_overlay(fixture.pinned.cell, fixture.pinned.incarnation, recovery)
        .await
    {
        Ok(_) => panic!("corrupt recovery input must not load"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn pinned_manifest_reopens_exact_overlay_and_prepares_successor() {
    let fixture = recovery_fixture().await;
    assert!(fixture.publication.bundle_bytes > 0);
    assert!(fixture.publication.object_reads > 0);
    assert!(fixture.publication.object_writes > 0);
    let overlay = fixture
        .manifests
        .load_overlay(
            fixture.pinned.cell,
            fixture.pinned.incarnation,
            &fixture.pinned.recovery,
        )
        .await
        .unwrap();
    let prepared = fixture
        .replica
        .prepare_recovered_overlay(&overlay, 1)
        .await
        .unwrap();
    assert_eq!(prepared.predecessor(), Some(fixture.base));
    assert_eq!(prepared.root().position, fixture.final_position);
    let mut control = crate::control::Control::initial(
        fixture.pinned.cell,
        fixture.pinned.incarnation,
        crate::control::Owner {
            session: SessionId::from_bytes([1; 16]),
            endpoint: "https://dead.internal:8081".into(),
        },
        Digest::from_bytes([12; 32]),
        1,
    )
    .unwrap();
    control.state = crate::control::ControlState::Serving;
    control.root = Some(runtime_root(fixture.base));
    let attached = control
        .attach_recovery(fixture.pinned.recovery.clone())
        .unwrap();
    let takeover = attached
        .takeover(crate::control::Owner {
            session: SessionId::from_bytes([13; 16]),
            endpoint: "https://successor.internal:8081".into(),
        })
        .unwrap();
    let published = takeover.publish_recovery(&prepared, None).unwrap();
    assert_eq!(published.state, crate::control::ControlState::Recovering);
    assert!(published.recovery.is_none());
    assert_eq!(published.root.unwrap().txid, fixture.final_position.txid);
}

#[tokio::test]
async fn verified_artifact_store_hit_reuses_the_pinned_bundle() {
    let limits = cellule_ltx::Limits::default();
    let artifacts = Arc::new(MemoryArtifactStore {
        limits,
        reject_retain: false,
        bundles: Mutex::new(BTreeMap::new()),
        loads: AtomicU64::new(0),
    });
    let fixture = recovery_fixture_with_store(Some(
        Arc::clone(&artifacts) as Arc<dyn RecoveryArtifactStore>
    ))
    .await;
    let overlay = fixture
        .manifests
        .load_overlay(
            fixture.pinned.cell,
            fixture.pinned.incarnation,
            &fixture.pinned.recovery,
        )
        .await
        .unwrap();
    assert_eq!(artifacts.loads.load(Ordering::Relaxed), 1);
    let prepared = fixture
        .replica
        .prepare_recovered_overlay(&overlay, 1)
        .await
        .unwrap();
    assert_eq!(prepared.root().position, fixture.final_position);
}

#[tokio::test]
async fn artifact_cache_failure_keeps_object_store_recovery_available() {
    let artifacts = Arc::new(MemoryArtifactStore {
        limits: cellule_ltx::Limits::default(),
        reject_retain: true,
        bundles: Mutex::new(BTreeMap::new()),
        loads: AtomicU64::new(0),
    });
    let fixture = recovery_fixture_with_store(Some(
        Arc::clone(&artifacts) as Arc<dyn RecoveryArtifactStore>
    ))
    .await;
    let overlay = fixture
        .manifests
        .load_overlay(
            fixture.pinned.cell,
            fixture.pinned.incarnation,
            &fixture.pinned.recovery,
        )
        .await
        .unwrap();
    assert_eq!(artifacts.loads.load(Ordering::Relaxed), 0);
    assert_eq!(overlay.final_position(), fixture.final_position);
}

#[tokio::test]
async fn loaded_overlay_holds_bundle_disk_reservation_until_drop() {
    let fixture = recovery_fixture().await;
    let scratch = tempfile::TempDir::new().unwrap();
    let recovery = &fixture.pinned.recovery;
    let manifest_path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let (body, _) = fixture
        .layout
        .store()
        .get_with_etag_bounded(&manifest_path, MAX_MANIFEST_BYTES)
        .await
        .unwrap();
    let manifest = RecoveryManifest::decode(&body).unwrap();
    let bundle_path = fixture.layout.node_log_bundle_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        &manifest.cells[0].bundle_digest,
    );
    let size = fixture
        .layout
        .store()
        .head(&bundle_path)
        .await
        .unwrap()
        .size;
    let budget = cellule_ltx::DiskBudget::new(size);
    let manifests =
        RecoveryManifestStore::new(fixture.layout.clone(), cellule_ltx::Limits::default())
            .with_recovery_disk(budget.clone())
            .with_recovery_scratch(scratch.path().to_owned());
    let overlay = manifests
        .load_overlay(fixture.pinned.cell, fixture.pinned.incarnation, recovery)
        .await
        .unwrap();
    assert_eq!(overlay.bundle().len(), size);
    assert_eq!(budget.used(), size);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 1);
    drop(overlay);
    assert_eq!(budget.used(), 0);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn load_overlay_rejects_corrupt_manifest_bytes() {
    let fixture = recovery_fixture().await;
    let recovery = &fixture.pinned.recovery;
    let path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    fixture
        .inner
        .put(&path, Bytes::from_static(b"corrupt manifest").into())
        .await
        .unwrap();

    let error = load_error(&fixture, recovery).await;

    assert!(matches!(
        error,
        Error::Node("recovery manifest digest differs")
    ));
}

#[tokio::test]
async fn load_overlay_rejects_self_consistent_manifest_metadata_change() {
    let fixture = recovery_fixture().await;
    let mut recovery = fixture.pinned.recovery.clone();
    let original_path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let (body, _) = fixture
        .layout
        .store()
        .get_with_etag_bounded(&original_path, MAX_MANIFEST_BYTES)
        .await
        .unwrap();
    let mut raw: RawManifest = serde_json::from_slice(&body).unwrap();
    raw.cells[0].final_commit_sequence = "3".into();
    let changed = serde_json::to_vec(&raw).unwrap();
    let digest = *blake3::hash(&changed).as_bytes();
    let changed_path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        &digest,
    );
    fixture
        .layout
        .store()
        .put(&changed_path, Bytes::from(changed))
        .await
        .unwrap();
    recovery.manifest_digest = Digest::from_bytes(digest);

    let error = load_error(&fixture, &recovery).await;

    assert!(matches!(
        error,
        Error::Node("recovery control pointer differs from manifest")
    ));
}

#[tokio::test]
async fn load_overlay_rejects_corrupt_bundle_bytes() {
    let fixture = recovery_fixture().await;
    let recovery = &fixture.pinned.recovery;
    let manifest_path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let (body, _) = fixture
        .layout
        .store()
        .get_with_etag_bounded(&manifest_path, MAX_MANIFEST_BYTES)
        .await
        .unwrap();
    let manifest = RecoveryManifest::decode(&body).unwrap();
    let bundle_path = fixture.layout.node_log_bundle_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        &manifest.cells[0].bundle_digest,
    );
    fixture
        .inner
        .put(&bundle_path, Bytes::from_static(b"corrupt bundle").into())
        .await
        .unwrap();

    let error = load_error(&fixture, recovery).await;

    assert!(matches!(
        error,
        Error::Node("recovery bundle digest differs")
    ));
}

async fn raw_manifest(fixture: &RecoveryFixture) -> RawManifest {
    let recovery = &fixture.pinned.recovery;
    let path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let (body, _) = fixture
        .layout
        .store()
        .get_with_etag_bounded(&path, MAX_MANIFEST_BYTES)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn install_manifest_bytes(fixture: &RecoveryFixture, body: Vec<u8>) -> Digest {
    let digest = Digest::from_bytes(*blake3::hash(&body).as_bytes());
    let recovery = &fixture.pinned.recovery;
    let path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        digest.as_bytes(),
    );
    fixture
        .layout
        .store()
        .put(&path, Bytes::from(body))
        .await
        .unwrap();
    digest
}

#[tokio::test]
async fn manifest_inventory_reconstructs_every_scope_across_applications() {
    let fixture = recovery_fixture_with_scopes(None, true).await;
    // A newly constructed adapter has no local pinning result or artifact cache.
    let manifests =
        RecoveryManifestStore::new(fixture.layout.clone(), cellule_ltx::Limits::default());
    let original = &fixture.pinned.recovery;
    let inventory = manifests
        .load_manifest(
            original.leader_session,
            original.log_epoch,
            original.manifest_digest,
        )
        .await
        .unwrap();
    assert_eq!(inventory.leader_session(), original.leader_session);
    assert_eq!(inventory.log_epoch(), original.log_epoch);
    assert_eq!(inventory.manifest_digest(), original.manifest_digest);
    assert_eq!(inventory.cells().len(), 3);
    assert_eq!(inventory.cells()[0].cell, fixture.pinned.cell);
    assert_eq!(inventory.cells()[0].recovery, fixture.pinned.recovery);
    assert_eq!(inventory.cells()[1].cell, CellId::from_bytes([7; 32]));
    assert_eq!(
        inventory.cells()[2].application,
        ApplicationId::from_bytes([9; 16])
    );
    for row in inventory.cells() {
        let layout = CellStorageLayout::new(
            fixture.layout.store().clone(),
            Path::from("root"),
            *row.application.as_bytes(),
        );
        let store = RecoveryManifestStore::new(layout, cellule_ltx::Limits::default());
        let overlay = store
            .load_overlay(row.cell, row.incarnation, &row.recovery)
            .await
            .unwrap();
        assert_eq!(overlay.final_position(), fixture.final_position);
        assert_eq!(row.cell_epoch, 6);
        assert_eq!(row.recovery.manifest_digest, inventory.manifest_digest());
    }
}

#[tokio::test]
async fn manifest_inventory_is_metadata_and_does_not_certify_bundle_availability() {
    let fixture = recovery_fixture().await;
    let recovery = &fixture.pinned.recovery;
    let raw = raw_manifest(&fixture).await;
    let bundle_digest: [u8; 32] = unhex(&raw.cells[0].bundle_digest).unwrap();
    let path = fixture.layout.node_log_bundle_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        &bundle_digest,
    );
    fixture.inner.delete(&path).await.unwrap();
    let inventory = fixture
        .manifests
        .load_manifest(
            recovery.leader_session,
            recovery.log_epoch,
            recovery.manifest_digest,
        )
        .await
        .unwrap();
    assert_eq!(inventory.cells().len(), 1);
    assert!(matches!(
        load_error(&fixture, recovery).await,
        Error::Storage(StorageError::NotFound { .. })
    ));
}

#[tokio::test]
async fn manifest_inventory_rejects_invalid_scope_and_preserves_missing_object_error() {
    let fixture = recovery_fixture().await;
    let recovery = &fixture.pinned.recovery;
    for (leader, epoch) in [
        (SessionId::from_bytes([0; 16]), 2),
        (recovery.leader_session, 0),
    ] {
        assert!(matches!(
            fixture
                .manifests
                .load_manifest(leader, epoch, recovery.manifest_digest)
                .await,
            Err(Error::Node("invalid recovery manifest scope"))
        ));
    }
    assert!(matches!(
        fixture
            .manifests
            .load_manifest(
                recovery.leader_session,
                recovery.log_epoch,
                Digest::from_bytes([99; 32])
            )
            .await,
        Err(Error::Storage(StorageError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn manifest_inventory_rejects_corrupt_digest_and_wrong_path_scope() {
    let fixture = recovery_fixture().await;
    let recovery = &fixture.pinned.recovery;
    let path = fixture.layout.node_log_recovery_path(
        recovery.leader_session.as_bytes(),
        recovery.log_epoch,
        recovery.manifest_digest.as_bytes(),
    );
    let raw = raw_manifest(&fixture).await;
    fixture
        .inner
        .put(&path, Bytes::from_static(b"corrupt").into())
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .manifests
            .load_manifest(
                recovery.leader_session,
                recovery.log_epoch,
                recovery.manifest_digest
            )
            .await,
        Err(Error::Node("recovery manifest digest differs"))
    ));
    for (leader, epoch) in [([88; 16], "2"), ([1; 16], "3")] {
        let mut changed: RawManifest =
            serde_json::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        changed.leader_session = encode_hex(&leader);
        changed.log_epoch = epoch.into();
        let digest = install_manifest_bytes(&fixture, serde_json::to_vec(&changed).unwrap()).await;
        assert!(matches!(
            fixture
                .manifests
                .load_manifest(recovery.leader_session, recovery.log_epoch, digest)
                .await,
            Err(Error::Node("recovery manifest path scope differs"))
        ));
    }
}

#[tokio::test]
async fn manifest_inventory_and_overlay_reject_duplicate_and_reordered_scopes() {
    let fixture = recovery_fixture_with_scopes(None, true).await;
    for duplicate in [true, false] {
        let mut raw = raw_manifest(&fixture).await;
        if duplicate {
            let copy = serde_json::from_slice(&serde_json::to_vec(&raw.cells[0]).unwrap()).unwrap();
            raw.cells.insert(1, copy);
        } else {
            raw.cells.swap(0, 1);
        }
        let digest = install_manifest_bytes(&fixture, serde_json::to_vec(&raw).unwrap()).await;
        let mut recovery = fixture.pinned.recovery.clone();
        recovery.manifest_digest = digest;
        assert!(matches!(
            fixture
                .manifests
                .load_manifest(recovery.leader_session, recovery.log_epoch, digest)
                .await,
            Err(Error::Node(
                "recovery manifest Cell scopes are not strictly ordered"
            ))
        ));
        assert!(matches!(
            load_error(&fixture, &recovery).await,
            Error::Node("recovery manifest Cell scopes are not strictly ordered")
        ));
    }
}

#[tokio::test]
async fn manifest_inventory_rejects_self_consistent_noncanonical_or_invalid_shapes() {
    let fixture = recovery_fixture().await;
    let raw = raw_manifest(&fixture).await;
    let valid = serde_json::to_value(&raw).unwrap();
    let mut bodies = vec![serde_json::to_vec_pretty(&raw).unwrap()];
    for variant in 0..7 {
        let mut changed = valid.clone();
        match variant {
            0 => {
                changed["unknown"] = serde_json::json!(true);
            }
            1 => {
                changed["version"] = serde_json::json!(2);
            }
            2 => {
                changed["cells"] = serde_json::json!([]);
            }
            3 => {
                changed["cells"] = serde_json::json!(vec![valid["cells"][0].clone(); 1_025]);
            }
            4 => {
                changed["cells"][0]["first_node_sequence"] = serde_json::json!("0");
            }
            5 => {
                changed["cells"][0]["cell_epoch"] = serde_json::json!("06");
            }
            _ => {
                changed["cells"][0]["final_txid"] = changed["cells"][0]["predecessor_txid"].clone();
            }
        }
        bodies.push(serde_json::to_vec(&changed).unwrap());
    }
    for body in bodies {
        let digest = install_manifest_bytes(&fixture, body).await;
        let recovery = &fixture.pinned.recovery;
        assert!(
            fixture
                .manifests
                .load_manifest(recovery.leader_session, recovery.log_epoch, digest)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn manifest_inventory_rejects_oversized_object_before_decode() {
    let fixture = recovery_fixture().await;
    let digest =
        install_manifest_bytes(&fixture, vec![b' '; MAX_MANIFEST_BYTES as usize + 1]).await;
    let recovery = &fixture.pinned.recovery;
    assert!(matches!(
        fixture
            .manifests
            .load_manifest(recovery.leader_session, recovery.log_epoch, digest)
            .await,
        Err(Error::Storage(_))
    ));
}
