use super::*;

impl Fixture {
    pub(super) async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            SqliteJournal::open(
                root.path().join("evacuation.sqlite"),
                scope(),
                FleetProfile::default(),
                clock().unwrap(),
            )
            .await
            .unwrap(),
        );
        let app = application::compile().unwrap();
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("reader-evacuation"),
            [3; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            scope().fleet,
            Digest::from_bytes([31; 32]),
            app.registry().release_digest(),
        );
        let limits = Limits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..Limits::default()
        };
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            scope().application,
            application::NAMESPACE,
            &[1],
        )
        .unwrap();
        let code = app.registry().module_digests()[0];
        let proof = CellCatalog::new(layout.clone(), target.tenant())
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
            .await
            .unwrap();
        let incarnation = IncarnationId::from_bytes([1; 16]);
        let authority = CellAuthority::new(layout.clone());
        let initial = authority
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
        let records = Arc::new(HashMap::from([(
            target.cell_id(),
            Record {
                target: target.clone(),
                incarnation,
                catalog: proof.clone(),
                replica: replica.clone(),
                authority: authority.clone(),
            },
        )]));
        let mut nodes = Vec::new();
        let mut managers = Vec::new();
        let mut boots = Vec::new();
        for index in 0..3 {
            let intent = journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node_id(index), session(index)).unwrap(),
                )
                .await
                .unwrap();
            let node = Arc::new(
                CellNodeBuilder::new(app.clone())
                    .with_runtime(
                        SqlWorkerPool::new(2, 8)
                            .unwrap()
                            .with_native_memory_limit(128 << 20)
                            .unwrap(),
                        16 << 20,
                    )
                    .with_replica_host(
                        Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
                    )
                    .with_session(session(index))
                    .with_fleet_startup_intent(intent.clone())
                    .build()
                    .unwrap(),
            );
            node.install_task_group(CancellationToken::new(), CancellationToken::new())
                .unwrap();
            node.install_fleet_actions(
                scope(),
                node_id(index),
                journal.clone(),
                Arc::new(super::super::super::adapters::Cells {
                    records: records.clone(),
                    local: index,
                    root: root.path().into(),
                }),
            )
            .unwrap();
            let manager = node
                .install_read_replicas(
                    layout.clone(),
                    directory.clone(),
                    root.path().join(format!("readers-{index}")),
                    limits,
                )
                .unwrap();
            node.install_fleet_reader_enrollment(scope(), node_id(index), journal.clone())
                .unwrap();
            let ad = startup::advertisement(index, &node, &intent).await.unwrap();
            let spec = startup::spec(&intent).unwrap();
            let original =
                startup::enroll(&journal, &directory, &spec, ad.clone(), clock().unwrap())
                    .await
                    .unwrap();
            let guard = NodeLeaseGuard::new(clock().unwrap(), ad.expires_at_ms()).unwrap();
            node.install_node_lease_for_startup(guard.clone()).unwrap();
            node.confirm_fleet_startup(journal.as_ref(), spec.key().unwrap())
                .await
                .unwrap();
            let observed = directory
                .load(session(index), clock().unwrap())
                .await
                .unwrap()
                .unwrap();
            node.install_fleet_boot_withdrawal(
                directory.clone(),
                observed,
                original,
                journal.clone(),
            )
            .unwrap();
            node.start().unwrap();
            boots.push(startup::BootOwner {
                node: node.clone(),
                directory: directory.clone(),
                spec,
                advertisement: ad,
                guard: Some(guard),
            });
            nodes.push(node);
            managers.push(manager);
        }
        let handle = nodes[0]
            .runtime()
            .bootstrap(
                proof,
                replica,
                authority,
                initial,
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
        managers[1]
            .set_target(&target, 0, 1)
            .await
            .unwrap()
            .unwrap();
        boots[1]
            .refresh_capacity(1, journal.as_ref(), Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        let transport = Arc::new(transport::NativePeers::new(
            &nodes,
            &managers,
            &layout,
            directory.clone(),
        ));
        let peer = ReplicaPeerClient::new(
            app.registry(),
            Arc::new(PeerSigner::new(
                session(0),
                app.registry().release_digest(),
                SigningKey::from_bytes(&[1; 32]),
            )),
            PeerPrincipal {
                issuer: "managed-owner".into(),
                subject: "live-owner".into(),
                actions: vec!["replica-maintenance".into()],
            },
            transport.clone(),
        );
        let ad = directory
            .load(session(1), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        let description = CellDescription {
            cell: target.cell_id(),
            incarnation,
            code,
            schema: 1,
        };
        peer.activate(&target, &directory, ad.advertisement().clone(), description)
            .await
            .unwrap();
        let reader = managers[1].resolve(target.clone()).await.unwrap();
        let completion = managers[1]
            .enrollment_completion(target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let original = journal
            .load_enrollment(scope(), completion.spec.key().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.status(), EnrollmentStatus::Established);
        let snapshot = journal.load_snapshot(scope()).await.unwrap();
        journal
            .bootstrap_registry(snapshot.registry())
            .await
            .unwrap();
        let now = clock().unwrap();
        let snapshot = journal.load_snapshot(scope()).await.unwrap();
        let mut snapshot = journal
            .claim_controller(
                scope(),
                snapshot.head().revision(),
                SessionId::from_bytes([206; 16]),
                now,
            )
            .await
            .unwrap();
        for transition in [
            JournalTransition::BeginMaintenance(
                MaintenanceOperation::new(
                    OperationId::from_bytes([80; 16]).unwrap(),
                    Digest::from_bytes([81; 32]),
                    node_id(1),
                    session(1),
                    2,
                    now,
                    now + 60_000,
                )
                .unwrap(),
            ),
            JournalTransition::Maintenance(MaintenanceEvent::Cordoned),
            JournalTransition::Maintenance(MaintenanceEvent::BeginEvacuation),
        ] {
            snapshot = journal
                .compare_exchange(
                    &snapshot,
                    snapshot.head().controller().unwrap().epoch,
                    clock().unwrap(),
                    &transition,
                )
                .await
                .unwrap();
        }
        let operation = snapshot.head().maintenance().unwrap().clone();
        boots[1]
            .refresh_capacity(1, journal.as_ref(), Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(
            nodes[1].runtime().node_admission().mode().unwrap(),
            cellule_runtime::node::NodeMode::Draining
        );
        assert!(nodes[1].is_management_ready());
        Self {
            root,
            journal,
            description,
            directory,
            nodes,
            managers,
            boots,
            handle,
            target,
            original,
            reader,
            operation,
            peer,
            transport,
        }
    }
}
