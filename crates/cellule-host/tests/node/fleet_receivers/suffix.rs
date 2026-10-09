//! Fresh serving proof against a genuinely sealed native follower suffix.
use super::*;
use bytes::Bytes;
use cellule_runtime::identity::NodeId;
use cellule_runtime::node::log_recovery::{NodeLogRecovery, RecoveryCell, RecoveryCoordinator};
use cellule_runtime::node::log_transport::{
    AppendRequest, LocalFollowerTransport, NodeLogTransport,
};
use cellule_runtime::recovery::manifest::RecoveryManifestStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_suffix_inspection_requires_original_manifest_after_advancing() {
    inspect_suffix(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_suffix_inspection_refuses_corrupt_original_manifest() {
    inspect_suffix(true).await;
}

async fn inspect_suffix(corrupt: bool) {
    let movement = Movement::new(128 << 20).await;
    start_suffix_recovery(&movement).await;
    let result = apply(&movement.receiver, movement.action(MovementAction::Recover)).await;
    assert!(
        result.committed && result.execution_error.is_none(),
        "{result:?}"
    );
    let FleetOutcome::Recovered(recovered) = &result.outcome.outcome else {
        panic!("not recovered: {result:?}")
    };
    assert!(recovered.recovery.basis().control().recovery.is_some());
    movement.event(AttemptEvent::Recovered(recovered.clone()));
    let cleaned = apply(&movement.receiver, movement.action(MovementAction::Cancel)).await;
    assert!(matches!(
        cleaned.outcome.outcome,
        FleetOutcome::ReceiverCleaned
    ));
    movement.event(AttemptEvent::ReceiverCleaned);
    let current = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &current)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, 43);
    let now = clock();
    handle
        .execute(
            MutationIdentity {
                request_id: RequestId::from_bytes([231; 16]),
                issued_at_ms: now,
                expires_at_ms: now + 10_000,
            },
            Digest::from_bytes([231; 32]),
            now,
            64,
            64,
            |transaction| {
                transaction.execute("UPDATE counter SET value = 99", [])?;
                Ok(HandlerOutcome::Success(vec![99]))
            },
        )
        .await
        .unwrap();
    movement.inspect(232).await;
    let overlay = recovered
        .recovery
        .basis()
        .control()
        .recovery
        .as_ref()
        .unwrap();
    let layout = movement.inputs.authority.layout();
    let path = layout.node_log_recovery_path(
        overlay.leader_session.as_bytes(),
        overlay.log_epoch,
        overlay.manifest_digest.as_bytes(),
    );
    let (original, _) = layout.store().get_with_etag(&path).await.unwrap();
    layout.store().delete(&path).await.unwrap();
    if corrupt {
        layout
            .store()
            .create_strict(&path, Bytes::from_static(b"corrupt-manifest"))
            .await
            .unwrap();
    }
    assert_eq!(counter(&handle).await, 99);
    let error = movement
        .receiver
        .inspect_fleet_action(movement.inspection(233))
        .await
        .unwrap_err();
    if corrupt {
        assert!(matches!(error.as_ref(), Error::Node(_)), "{error:?}");
        layout.store().delete(&path).await.unwrap();
    } else {
        assert!(matches!(error.as_ref(), Error::Storage(_)), "{error:?}");
    }
    layout.store().create_strict(&path, original).await.unwrap();
    assert!(matches!(
        movement.inspect(234).await.outcome().outcome,
        FleetOutcome::Recovered(_)
    ));
    movement.shutdown().await;
}

pub(super) async fn start_suffix_recovery(movement: &Movement) {
    let prepared = movement.prepare().await;
    let FleetOutcome::Reserved(reservation) = prepared.outcome.outcome else {
        panic!("not prepared")
    };
    movement.event(AttemptEvent::Reserved(reservation));
    movement.event(AttemptEvent::BeginRelease);
    let observed = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let predecessor = observed.value().ltx_root().unwrap();
    let tail = movement.source._root.path().join("follower-tail.sqlite");
    let writable = movement
        .inputs
        .replica
        .open_root(&predecessor)
        .await
        .unwrap()
        .paged()
        .prepare_writable(&tail)
        .await
        .unwrap();
    let mut writer = writable.open_writable(&tail).unwrap();
    writer.transaction(|transaction| {
        transaction.execute("UPDATE counter SET value = value + 1", [])?;
        transaction.execute("UPDATE sys_meta SET commit_sequence = commit_sequence + 1, logical_time_ms = logical_time_ms + 1 WHERE singleton = 1", [])?;
        Ok(())
    }).unwrap();
    let capture = writer.capture().unwrap();
    let mut frames = Vec::new();
    for (index, segment) in capture.segments.iter().enumerate() {
        frames.push(
            cellule_runtime::ltx::encode_node_frame(
                cellule_runtime::ltx::NodeFrameScope {
                    leader_session: *movement.spec.source.as_bytes(),
                    log_epoch: 1,
                    node_sequence: index as u64 + 1,
                    application: *movement.spec.target.application().as_bytes(),
                    cell: *movement.spec.target.cell_id().as_bytes(),
                    incarnation: *movement.spec.incarnation.as_bytes(),
                    cell_epoch: observed.value().epoch,
                    commit_sequence: predecessor.commit_sequence + 1,
                },
                segment.info().clone(),
                Bytes::from(std::fs::read(segment.path()).unwrap()),
                movement.inputs.replica.limits(),
            )
            .unwrap()
            .encoded()
            .clone(),
        );
    }
    writer.close().unwrap();
    let member = SessionId::from_bytes([230; 16]);
    let member_node = NodeId::from_bytes(*member.as_bytes());
    let store = cellule_runtime::FollowerStore::open(
        movement.source._root.path().join("follower"),
        movement.inputs.replica.limits(),
        DiskBudget::new(8 << 30),
    )
    .unwrap();
    let transport: Arc<dyn NodeLogTransport> =
        Arc::new(LocalFollowerTransport::new(member_node, store));
    transport
        .append(
            member_node,
            AppendRequest {
                leader_session: movement.spec.source,
                log_epoch: 1,
                frames,
                covered_through: 0,
            },
        )
        .await
        .unwrap();
    movement.source.lease.fence();
    assert!(matches!(
        movement.source.handle.query(1, 1, |_| Ok(Vec::new())).await,
        Err(Error::Fenced) | Err(Error::CellDraining)
    ));
    let now = clock();
    let image = Digest::from_bytes([221; 32]);
    let release = Digest::from_bytes([222; 32]);
    let directory = cellule_runtime::node::NodeDirectory::new(
        movement.inputs.authority.layout().clone(),
        scope().fleet,
        image,
        release,
    );
    let key = ed25519_dalek::SigningKey::from_bytes(&[223; 32]);
    let signed = |node, session, issued, expires| {
        cellule_runtime::node::NodeAdvertisement::sign(
            node,
            session,
            "https://suffix.internal:8789".into(),
            scope().fleet,
            Digest::from_bytes([224; 32]),
            image,
            release,
            &key,
            1,
            issued,
            expires,
            vec![
                movement
                    .source
                    .node
                    .application()
                    .registry()
                    .module_digests()[0],
            ],
            vec![1],
            cellule_runtime::node::NodeFailureDomain::default(),
            cellule_runtime::node::NodeCapacity {
                free_memory_bytes: 128 << 20,
                free_disk_bytes: 8 << 30,
                follower_free_bytes: 8 << 30,
                follower_retained_bytes: 0,
                job_credits: 1,
                log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
            },
        )
        .unwrap()
    };
    let leader = directory
        .create(
            signed(
                movement.spec.source_node,
                movement.spec.source,
                now - 10_000,
                now - 1,
            ),
            now - 10_000,
        )
        .await
        .unwrap();
    directory
        .create(
            signed(member_node, member, now - 10_000, now + 10_000),
            now - 9_999,
        )
        .await
        .unwrap();
    let enrolled = directory
        .recruit_log(&leader, 1, 1, 2, now - 9_998)
        .await
        .unwrap();
    directory
        .activate_log(&enrolled, now - 9_997)
        .await
        .unwrap();
    directory
        .create(
            signed(
                movement.spec.destination_node,
                movement.spec.destination,
                now,
                now + 10_000,
            ),
            now,
        )
        .await
        .unwrap();
    let fenced = directory
        .claim_expired(movement.spec.source, movement.spec.destination, now)
        .await
        .unwrap();
    let manifests = RecoveryManifestStore::new(
        movement.inputs.authority.layout().clone(),
        movement.inputs.replica.limits(),
    );
    let recovery =
        NodeLogRecovery::from_fenced(transport, &fenced, movement.inputs.replica.limits()).unwrap();
    let completed = RecoveryCoordinator::new(recovery, manifests.clone())
        .recover_and_seal(
            &directory,
            fenced,
            vec![RecoveryCell {
                application: movement.spec.target.application(),
                authority: movement.inputs.authority.clone(),
                observed,
            }],
            now,
        )
        .await
        .unwrap();
    assert_eq!(completed.controls.len(), 1);
    assert!(completed.controls[0].value().recovery.is_some());
    *movement.recovery_inputs.lock().unwrap() = Some(FleetRecoveryInputs {
        takeover: completed.takeover,
        manifests,
    });
    movement.event(AttemptEvent::OutcomeUnknown);
    movement.event(AttemptEvent::BeginRecover);
}
