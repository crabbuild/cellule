use super::*;

impl Inherited {
    pub(super) async fn new() -> Self {
        let native = super::super::super::fixture::Fixture::released_with_recovery(true).await;
        let record = &native.records[&native.spec.target.cell_id()];
        let early_session = session(9);
        let early_owner = owner(9);
        let early = CellNodeBuilder::new(application::compile().unwrap())
            .with_session(early_session)
            .with_runtime(SqlWorkerPool::new(2, 8).unwrap(), 128 << 20)
            .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
            .build()
            .unwrap();
        early
            .install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        let guard = NodeLeaseGuard::new(clock().unwrap(), clock().unwrap() + 60_000).unwrap();
        early.install_node_lease(guard.clone()).unwrap();
        let idle = record
            .authority
            .load(record.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let handle = early
            .runtime()
            .acquire_idle_restored(
                record.catalog.clone(),
                record.replica.clone(),
                record.authority.clone(),
                idle,
                native.scratch("earlier-owner.sqlite"),
                early_owner,
            )
            .await
            .unwrap();
        let observed = record
            .authority
            .load(record.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.value().epoch, native.released.epoch + 1);
        let predecessor = observed.value().ltx_root().unwrap();
        let frames = frames(&native, &observed).await;
        let directory = native.directory();
        native
            .journal
            .register_initial_intent(
                &NodeIntent::initial(scope(), node_id(9), early_session).unwrap(),
            )
            .await
            .unwrap();
        // Original member requests are journaled before canonical enrollment.
        // These native in-process stores and their complete retired enrollment
        // set are joined before the later receiver's process closure.
        for index in [1, 2] {
            let current = directory
                .load_if_live(session(index), clock().unwrap())
                .await
                .unwrap()
                .unwrap();
            let ad = current.advertisement();
            let previous = ad.operational_sample().unwrap();
            let sample = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(sample) =
                        native.nodes[index].runtime().operational_sample().unwrap()
                        && sample.sequence > previous.sequence
                    {
                        break sample;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let mut capacity = ad.capacity();
            assert!(
                capacity.free_memory_bytes != 0
                    && capacity.free_disk_bytes != 0
                    && capacity.job_credits != 0,
                "inherited follower node={index} capacity={capacity:?}"
            );
            capacity.follower_free_bytes = capacity.free_disk_bytes.min(64 << 20);
            capacity.log_protocol = cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION;
            let next = NodeAdvertisement::sign(
                ad.node(),
                ad.session(),
                ad.endpoint().into(),
                ad.fleet(),
                ad.certificate(),
                ad.image(),
                ad.release(),
                &ed25519_dalek::SigningKey::from_bytes(&[index as u8 + 1; 32]),
                ad.progress(),
                clock().unwrap(),
                clock().unwrap() + 30_000,
                ad.module_digests().to_vec(),
                ad.peer_versions().to_vec(),
                ad.failure_domain().clone(),
                capacity,
            )
            .unwrap()
            .with_operational_placement(
                ad.placement_capacity().unwrap(),
                sample,
                &ed25519_dalek::SigningKey::from_bytes(&[index as u8 + 1; 32]),
            )
            .unwrap();
            directory
                .refresh(&current, next, clock().unwrap())
                .await
                .unwrap();
        }
        let reference = directory
            .load_if_live(session(0), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        let ad = reference.advertisement();
        let now = clock().unwrap();
        let expires = now + 1_000;
        let leader = directory
            .create(
                NodeAdvertisement::sign(
                    node_id(9),
                    early_session,
                    owner(9).endpoint,
                    ad.fleet(),
                    ad.certificate(),
                    ad.image(),
                    ad.release(),
                    &ed25519_dalek::SigningKey::from_bytes(&[10; 32]),
                    1,
                    now,
                    expires,
                    ad.module_digests().to_vec(),
                    ad.peer_versions().to_vec(),
                    ad.failure_domain().clone(),
                    ad.capacity(),
                )
                .unwrap(),
                now,
            )
            .await
            .unwrap();
        let prepared = directory
            .prepare_log_enrollment(&leader, 7, 1, 128, clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        let enrollment = directory
            .prepare_log_enrollment_attempt(&prepared, clock().unwrap())
            .await
            .unwrap();
        let snapshot = native.journal.load_snapshot(scope()).await.unwrap();
        let roster = FleetRoster::collect(
            native.journal.as_ref(),
            &snapshot,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        for (index, member) in prepared.followers().iter().enumerate() {
            let intent = roster
                .intents()
                .iter()
                .find(|intent| intent.node() == member.node())
                .unwrap();
            let accepted = native
                .journal
                .accept_enrollment(
                    &EnrollmentSpec {
                        scope: scope(),
                        request: Digest::from_bytes([index as u8 + 248; 32]),
                        source: Some(EnrollmentEndpoint {
                            node: node_id(9),
                            session: early_session,
                            intent_revision: 1,
                        }),
                        target: EnrollmentEndpoint {
                            node: member.node(),
                            session: member.session(),
                            intent_revision: intent.revision(),
                        },
                        role: EnrollmentRole::Follower { log_epoch: 7 },
                    },
                    clock().unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(accepted, FleetEnrollmentAcceptance::New(_)));
        }
        let enrolled = directory
            .commit_log_enrollment(&enrollment, clock().unwrap())
            .await
            .unwrap()
            .enrollment()
            .clone();
        let mut peers = Vec::new();
        for member in enrolled.advertisement().log().unwrap().members() {
            let store = FollowerStore::open(
                native.scratch(&format!("inherited-follower-{member:?}")),
                record.replica.limits(),
                DiskBudget::new(8 << 30),
            )
            .unwrap();
            let local = LocalFollowerTransport::new(*member, store);
            local
                .append(
                    *member,
                    AppendRequest {
                        leader_session: early_session,
                        log_epoch: 7,
                        frames: frames.clone(),
                        covered_through: 0,
                    },
                )
                .await
                .unwrap();
            peers.push((
                *member,
                LocalRecoveredFollowerTransport::new(local, directory.clone(), session(1), clock)
                    .unwrap(),
            ));
        }
        let transport = Arc::new(Members::new(peers));
        // Activate only after every original member's first append has fsynced.
        directory
            .activate_log(&enrolled, clock().unwrap())
            .await
            .unwrap();
        guard.fence();
        assert!(matches!(
            handle.query(1, 1, |_| Ok(Vec::new())).await,
            Err(Error::Fenced) | Err(Error::CellDraining)
        ));
        let remaining = (expires.saturating_sub(clock().unwrap()) + 1).max(0);
        tokio::time::sleep(Duration::from_millis(u64::try_from(remaining).unwrap())).await;
        let fenced = directory
            .claim_expired(early_session, session(1), clock().unwrap())
            .await
            .unwrap();
        let manifests =
            RecoveryManifestStore::new(record.authority.layout().clone(), record.replica.limits());
        let completed = RecoveryCoordinator::new(
            NodeLogRecovery::from_fenced(transport.clone(), &fenced, record.replica.limits())
                .unwrap(),
            manifests.clone(),
        )
        .recover_and_seal(
            &directory,
            fenced,
            vec![RecoveryCell {
                application: scope().application,
                authority: record.authority.clone(),
                observed,
            }],
            clock().unwrap(),
        )
        .await
        .unwrap();
        let retirement = retire_recovered_members(transport, &completed.sealed)
            .await
            .unwrap()
            .confirmed()
            .unwrap();
        directory
            .retire_recovered_log(&retirement, session(1), clock().unwrap())
            .await
            .unwrap();
        let snapshot = native.journal.load_snapshot(scope()).await.unwrap();
        let roster = FleetRoster::collect(
            native.journal.as_ref(),
            &snapshot,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        FleetRecoveredFollowerRetirement::capture(
            native.journal.as_ref(),
            &directory,
            &roster,
            &completed.sealed,
            session(1),
            Instant::now() + Duration::from_secs(5),
            clock,
        )
        .await
        .unwrap()
        .publish(
            native.journal.as_ref(),
            &directory,
            session(1),
            Instant::now() + Duration::from_secs(5),
            clock,
        )
        .await
        .unwrap()
        .confirmed()
        .unwrap();
        let original = completed.controls[0].value().clone();
        let overlay = original.recovery.as_ref().unwrap();
        assert_eq!(overlay.predecessor.digest.as_bytes(), &predecessor.digest);
        let path = record.authority.layout().node_log_recovery_path(
            early_session.as_bytes(),
            7,
            overlay.manifest_digest.as_bytes(),
        );
        let manifest = record
            .authority
            .layout()
            .store()
            .get_with_etag(&path)
            .await
            .unwrap()
            .0;
        record
            .authority
            .layout()
            .store()
            .delete(&path)
            .await
            .unwrap();
        let failure = native.nodes[1]
            .runtime()
            .takeover_restored(
                record.catalog.clone(),
                record.replica.clone(),
                record.authority.clone(),
                completed.controls[0].clone(),
                completed.takeover,
                manifests,
                native.scratch("interrupted-preferred-receiver.sqlite"),
                owner(1),
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(failure, Error::Storage(_)), "{failure:?}");
        let claimed = record
            .authority
            .load(record.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.value().epoch, original.epoch + 1);
        assert_eq!(claimed.value().state, ControlState::Recovering);
        assert_eq!(claimed.value().recovery, original.recovery);
        assert_eq!(claimed.value().owner.as_ref().unwrap().session, session(1));
        assert!(
            record
                .authority
                .acquisition_record(original.cell, original.incarnation, claimed.value().epoch)
                .await
                .unwrap()
                .is_none()
        );
        // The original prepared receiver still owns its one admitted slot;
        // the interrupted ordinary claim admitted no writer alongside it.
        assert_eq!(native.nodes[1].stats().active_cells(), 1);
        let inventory = native.nodes[1]
            .runtime()
            .fleet_cells_page(None, 128)
            .await
            .unwrap();
        assert!(
            inventory
                .entries()
                .iter()
                .all(|row| !matches!(row, CellInventoryEntry::Owned(_)))
        );
        drop(inventory);
        early.shutdown().await.unwrap();
        assert_eq!(early.stats().active_cells(), 0);
        assert_eq!(early.stats().worker_jobs(), 0);
        assert_eq!(early.stats().resident_bytes(), 0);
        assert_eq!(early.stats().retained_bytes(), 0);
        assert_eq!(early.stats().file_descriptors(), 0);
        assert_eq!(early.stats().blocking_jobs(), 0);
        assert_eq!(early.stats().recovery_jobs(), 0);
        assert_eq!(early.stats().io_slots(), 0);
        assert_eq!(early.stats().local_disk_reserved_bytes(), 0);
        record
            .authority
            .layout()
            .store()
            .create_strict(&path, manifest.clone())
            .await
            .unwrap();
        let value = native.acknowledged[&record.target.cell_id()].value + 1;
        Self {
            native,
            path,
            manifest,
            original,
            value,
        }
    }
}

async fn frames(
    native: &super::super::super::fixture::Fixture,
    observed: &cellule_runtime::control::authority::VersionedControl,
) -> Vec<Bytes> {
    let record = &native.records[&native.spec.target.cell_id()];
    let predecessor = observed.value().ltx_root().unwrap();
    let path = native.scratch("inherited-actual-tail.sqlite");
    let writable = record
        .replica
        .open_root(&predecessor)
        .await
        .unwrap()
        .paged()
        .prepare_writable(&path)
        .await
        .unwrap();
    let mut writer = writable.open_writable(&path).unwrap();
    writer.transaction(|tx| {
        tx.execute("UPDATE counter SET value = value + 1", [])?;
        tx.execute("UPDATE sys_meta SET commit_sequence = commit_sequence + 1, logical_time_ms = logical_time_ms + 1 WHERE singleton = 1", [])?;
        Ok(())
    }).unwrap();
    let capture = writer.capture().unwrap();
    let frames = capture
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| {
            cellule_runtime::ltx::encode_node_frame(
                cellule_runtime::ltx::NodeFrameScope {
                    leader_session: *session(9).as_bytes(),
                    log_epoch: 7,
                    node_sequence: index as u64 + 1,
                    application: *scope().application.as_bytes(),
                    cell: *record.target.cell_id().as_bytes(),
                    incarnation: *record.incarnation.as_bytes(),
                    cell_epoch: observed.value().epoch,
                    commit_sequence: predecessor.commit_sequence + 1,
                },
                segment.info().clone(),
                Bytes::from(std::fs::read(segment.path()).unwrap()),
                record.replica.limits(),
            )
            .unwrap()
            .encoded()
            .clone()
        })
        .collect();
    writer.close().unwrap();
    frames
}
