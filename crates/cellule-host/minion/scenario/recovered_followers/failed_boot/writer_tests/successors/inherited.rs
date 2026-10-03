//! Earlier sealed input survives a real failed intermediate native acquisition.
//! Process evidence remains a lifetime stand-in; no role/finalization claim.
use super::*;
use cellule_runtime::control::ControlState;

impl WriterFixture {
    async fn inherited() -> Self {
        let mut inputs = WriterInputs::new(1, true, false, 2).await;
        let base = Fixture::with_recovered_boot_at(
            false,
            std::mem::take(&mut inputs.bases),
            std::mem::take(&mut inputs.frames),
            NOW,
            2,
            0,
        )
        .await;
        let intent = base
            .roster()
            .await
            .intents()
            .iter()
            .find(|row| row.node() == node_id(0))
            .unwrap()
            .clone();
        let node = CellNodeBuilder::new(crate::scenario::application::compile().unwrap())
            .with_runtime(SqlWorkerPool::new(2, 8).unwrap(), 128 << 20)
            .with_replica_host(Host::default())
            .with_session(session(0))
            .with_fleet_startup_intent(intent.clone())
            .build()
            .unwrap();
        node.install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        let real_now = crate::scenario::clock().unwrap();
        let guard = NodeLeaseGuard::new(real_now, real_now + 60_000).unwrap();
        node.install_node_lease_for_startup(guard.clone()).unwrap();
        node.confirm_fleet_startup(
            base.journal.as_ref(),
            startup::spec(&intent).unwrap().key().unwrap(),
        )
        .await
        .unwrap();
        node.start().unwrap();
        let takeover = base
            .directory
            .takeover_proof(session(2), session(0), CHECK)
            .await
            .unwrap()
            .unwrap();
        let digest = base.sealed.log().recovery_manifest().unwrap();
        let path = base.recovery_layout.node_log_recovery_path(
            session(2).as_bytes(),
            4,
            digest.as_bytes(),
        );
        let body = base
            .recovery_layout
            .store()
            .get_with_etag(&path)
            .await
            .unwrap()
            .0;
        // Lose the actual canonical manifest after sealing. Native takeover
        // commits its ownership CAS, then refuses unavailable recovery input.
        // Rollback must preserve the attached suffix; no fake Control or
        // successful acquisition/materialization record is constructed here.
        base.recovery_layout.store().delete(&path).await.unwrap();
        let limits = Limits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..Limits::default()
        };
        for (index, expected) in inputs.expected.iter_mut().enumerate() {
            let layout = &inputs.layouts[index];
            let authority = CellAuthority::new(layout.clone());
            let catalog = CellCatalog::new(layout.clone(), inputs.sources[index].tenant())
                .lookup(expected.cell)
                .await
                .unwrap()
                .unwrap();
            let before = authority.load(expected.cell).await.unwrap().unwrap();
            assert_eq!(before.value().epoch, 1);
            assert_eq!(before.value().owner.as_ref().unwrap().session, session(2));
            assert_eq!(
                before.value().recovery.as_ref().unwrap().leader_session,
                session(2)
            );
            let replica = CellReplica::new(
                layout.clone(),
                *expected.cell.as_bytes(),
                *expected.incarnation.as_bytes(),
                limits,
            )
            .unwrap();
            let manifests = RecoveryManifestStore::new(
                CellStorageLayout::new(
                    base.recovery_layout.store().clone(),
                    ObjectPath::from("recovered-enrollment"),
                    *layout.application_id(),
                ),
                limits,
            );
            let error = node
                .runtime()
                .takeover_restored(
                    catalog,
                    replica,
                    authority.clone(),
                    before.clone(),
                    takeover.clone(),
                    manifests,
                    base._root
                        .path()
                        .join(format!("interrupted-{index}.sqlite")),
                    owner(0),
                )
                .await
                .err()
                .unwrap();
            assert!(matches!(
                error,
                Error::Storage(cellule_store::StorageError::NotFound { .. })
            ));
            let claimed = authority.load(expected.cell).await.unwrap().unwrap();
            assert_eq!(claimed.value().state, ControlState::Recovering);
            assert_eq!(claimed.value().epoch, 2);
            assert_eq!(claimed.value().owner.as_ref().unwrap().session, session(0));
            assert_eq!(claimed.value().recovery, before.value().recovery);
            assert_eq!(claimed.value().root, before.value().root);
            assert!(
                authority
                    .acquisition_record(expected.cell, expected.incarnation, 2)
                    .await
                    .unwrap()
                    .is_none()
            );
            *expected = claimed.value().clone();
        }
        assert_eq!(node.stats().active_cells(), 0);
        guard.fence();
        node.shutdown().await.unwrap();
        assert_eq!(node.stats().retained_bytes(), 0);
        assert_eq!(node.stats().active_cells(), 0);
        base.recovery_layout.store().put(&path, body).await.unwrap();

        // Keep the later successor live with an ordinary canonical heartbeat
        // before the intermediate boot's original lease expires. Existing
        // fixtures and qualification profiles retain their original timings.
        let heartbeat_at = NOW + 18_000;
        let observed = base
            .directory
            .load_if_live(session(1), heartbeat_at)
            .await
            .unwrap()
            .unwrap();
        let ad = observed.advertisement();
        let next = NodeAdvertisement::sign(
            ad.node(),
            ad.session(),
            ad.endpoint().to_owned(),
            ad.fleet(),
            ad.certificate(),
            ad.image(),
            ad.release(),
            &SigningKey::from_bytes(&[2; 32]),
            ad.progress(),
            heartbeat_at,
            heartbeat_at + 10_000,
            ad.module_digests().to_vec(),
            ad.peer_versions().to_vec(),
            ad.failure_domain().clone(),
            ad.capacity(),
        )
        .unwrap();
        base.directory
            .refresh(&observed, next, heartbeat_at)
            .await
            .unwrap();
        base.directory
            .claim_expired(session(0), session(1), NOW + 19_001)
            .await
            .unwrap();
        Self::from_inputs(base, inputs, NOW + 19_005).await
    }
}

async fn fixture() -> SuccessorFixture {
    SuccessorFixture::from_original(WriterFixture::inherited().await).await
}

#[tokio::test]
async fn inherited_original_successors_preserve_earlier_epoch_and_join_failed_native_claims() {
    let fixture = fixture().await;
    let inventory = fixture.collect(deadline()).await.unwrap();
    assert!(inventory.original().manifest().is_none());
    assert!(inventory.original().recovered().log().is_none());
    assert_eq!(inventory.proofs().len(), 2);
    assert_eq!(inventory.original().writers().record().catalogs().len(), 2);
    let digest = fixture
        .original
        .base
        .sealed
        .log()
        .recovery_manifest()
        .unwrap();
    let earlier = fixture
        .original
        .base
        .manifests
        .load_manifest(session(2), 4, digest)
        .await
        .unwrap();
    assert_eq!(earlier.cells().len(), 2);
    for proof in inventory.proofs() {
        assert_eq!(proof.original().control.state, ControlState::Recovering);
        assert_eq!(proof.original().control.epoch, 2);
        assert_eq!(
            proof.original().control.owner.as_ref().unwrap().session,
            session(0)
        );
        assert_eq!(proof.serving().owner().session, session(1));
        assert_eq!(proof.serving().position().epoch, 3);
        assert_eq!(proof.suffixes().len(), 1);
        let suffix = &proof.suffixes()[0];
        let required = earlier
            .cells()
            .iter()
            .find(|row| row.cell == proof.original().control.cell)
            .unwrap();
        assert_eq!(suffix.required().cell_epoch, 1);
        assert_eq!(suffix.required().recovery, required.recovery);
        assert_eq!(suffix.required().recovery.leader_session, session(2));
        assert_eq!(suffix.acquisition_epoch(), 3);
        assert_eq!(suffix.proof().prefix().commit_sequence, 2);
        assert_eq!(suffix.proof().root().commit_sequence, 2);
        assert!(proof.origin().dependency_count() > 0);
        let inputs = &fixture.provider.cells[&required.cell];
        assert!(
            inputs
                .authority
                .acquisition_record(required.cell, required.incarnation, 2)
                .await
                .unwrap()
                .is_none()
        );
        let acquisition = inputs
            .authority
            .acquisition_record(required.cell, required.incarnation, 3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(acquisition.input(), &proof.original().control);
        assert_eq!(
            fixture.provider.handles[&required.cell]
                .query(8, 8, |db| Ok(db
                    .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec()))
                .await
                .unwrap(),
            2_i64.to_be_bytes()
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn inherited_original_successors_refuse_missing_or_corrupt_historical_manifest() {
    for corrupt in [false, true] {
        let fixture = fixture().await;
        let before = fixture
            .original
            .base
            .journal
            .load_snapshot(scope())
            .await
            .unwrap();
        let digest = fixture
            .original
            .base
            .sealed
            .log()
            .recovery_manifest()
            .unwrap();
        let layout = &fixture.original.base.recovery_layout;
        let path = layout.node_log_recovery_path(session(2).as_bytes(), 4, digest.as_bytes());
        if corrupt {
            layout
                .store()
                .put_overwrite(&path, Bytes::from_static(b"corrupt inherited manifest"))
                .await
                .unwrap();
        } else {
            layout.store().delete(&path).await.unwrap();
        }
        let error = fixture.collect(deadline()).await.err().unwrap();
        if corrupt {
            assert!(matches!(
                error,
                Error::Node("recovery manifest digest differs")
            ));
        } else {
            assert!(matches!(
                error,
                Error::Storage(cellule_store::StorageError::NotFound { .. })
            ));
        }
        for handle in fixture.provider.handles.values() {
            handle.query(1, 1, |_| Ok(Vec::new())).await.unwrap();
        }
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
        fixture.close().await;
    }
}

#[tokio::test]
async fn inherited_original_successors_refuse_substituted_historical_backend() {
    let mut fixture = fixture().await;
    let inputs = fixture.provider.cells.values_mut().next().unwrap();
    inputs.manifests = RecoveryManifestStore::new(
        CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("recovered-enrollment"),
            *scope().application.as_bytes(),
        ),
        Limits::default(),
    );
    assert!(matches!(
        fixture.collect(deadline()).await,
        Err(Error::Storage(cellule_store::StorageError::NotFound { .. }))
    ));
    await_native_release(&fixture.node).await;
    fixture.close().await;
}

#[tokio::test]
async fn inherited_original_successors_refuse_missing_canonical_materialization() {
    let fixture = fixture().await;
    let inputs = fixture.provider.cells.values().next().unwrap();
    let cell = inputs.catalog.entry().cell();
    let original = fixture
        .original
        .expected
        .iter()
        .find(|row| row.cell == cell)
        .unwrap();
    let layout = fixture
        .original
        .layouts
        .iter()
        .find(|layout| layout.application_id() == inputs.authority.layout().application_id())
        .unwrap();
    let path = layout.acquisition_record_path(cell.as_bytes(), original.incarnation.as_bytes(), 3);
    layout.store().delete(&path).await.unwrap();
    assert!(
        matches!(fixture.collect(deadline()).await, Err(Error::AcquisitionHistoryIncomplete { cell: observed, .. }) if observed == cell)
    );
    fixture.provider.handles[&cell]
        .query(1, 1, |_| Ok(Vec::new()))
        .await
        .unwrap();
    await_native_release(&fixture.node).await;
    fixture.close().await;
}

#[tokio::test]
async fn inherited_original_successors_refuse_missing_earlier_owner_history() {
    let fixture = fixture().await;
    let inputs = fixture.provider.cells.values().next().unwrap();
    let cell = inputs.catalog.entry().cell();
    let original = fixture
        .original
        .expected
        .iter()
        .find(|row| row.cell == cell)
        .unwrap();
    let layout = fixture
        .original
        .layouts
        .iter()
        .find(|layout| layout.application_id() == inputs.authority.layout().application_id())
        .unwrap();
    layout
        .store()
        .delete(&layout.owner_observation_path(cell.as_bytes(), original.incarnation.as_bytes(), 1))
        .await
        .unwrap();
    assert!(
        matches!(fixture.collect(deadline()).await, Err(Error::OwnerHistoryIncomplete { cell: observed, epoch: 1, .. }) if observed == cell)
    );
    fixture.provider.handles[&cell]
        .query(1, 1, |_| Ok(Vec::new()))
        .await
        .unwrap();
    await_native_release(&fixture.node).await;
    fixture.close().await;
}

#[tokio::test]
async fn inherited_original_successors_keep_exact_materialized_prefix_after_publication() {
    let fixture = fixture().await;
    let (cell, handle) = fixture.provider.handles.iter().next().unwrap();
    let now = crate::scenario::clock().unwrap();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([95; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 60_000,
            },
            Digest::from_bytes([96; 32]),
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
    let inventory = fixture.collect(deadline()).await.unwrap();
    let proof = inventory
        .proofs()
        .iter()
        .find(|proof| proof.original().control.cell == *cell)
        .unwrap();
    assert_eq!(proof.original().control.epoch, 2);
    assert_eq!(proof.suffixes()[0].required().cell_epoch, 1);
    assert_eq!(proof.suffixes()[0].acquisition_epoch(), 3);
    assert_eq!(proof.suffixes()[0].proof().prefix().commit_sequence, 2);
    assert_eq!(proof.suffixes()[0].proof().root().commit_sequence, 3);
    assert_eq!(proof.serving().position().root.commit_sequence, 3);
    assert_eq!(
        handle
            .query(8, 8, |db| Ok(db
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .to_be_bytes()
                .to_vec()))
            .await
            .unwrap(),
        3_i64.to_be_bytes()
    );
    fixture.close().await;
}
