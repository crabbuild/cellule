//! Actual managed boots, node-owned follower stores and an acknowledged Cell.
use super::super::super::{adapters, startup};
use super::*;
use cellule_runtime::fleet::operations::EnrollmentRole;

pub(crate) struct ManagedFixture {
    pub native: Fixture,
    pub nodes: Vec<Arc<CellNode>>,
    pub boots: Vec<startup::BootOwner>,
    pub handle: CellHandle,
    pub records: Arc<HashMap<CellId, Record>>,
}

impl ManagedFixture {
    pub async fn new() -> Self {
        Self::with_members(3, 1).await
    }

    pub async fn with_members(count: usize, max_epochs: usize) -> Self {
        assert!((3..=4).contains(&count));
        assert!((1..=3).contains(&max_epochs));
        let root = tempfile::tempdir().unwrap();
        let app = application::compile().unwrap();
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("managed-followers"),
            [3; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            scope().fleet,
            Digest::from_bytes([31; 32]),
            app.registry().release_digest(),
        );
        let journal = Arc::new(
            SqliteJournal::open(
                root.path().join("journal.sqlite"),
                scope(),
                FleetProfile::default(),
                clock().unwrap(),
            )
            .await
            .unwrap(),
        );
        let target = CellTarget::new(
            TenantId::from_bytes([1; 16]),
            scope().application,
            application::NAMESPACE,
            &[1],
        )
        .unwrap();
        let proof = CellCatalog::new(layout.clone(), target.tenant())
            .provision(
                CatalogEntry::new(
                    &target,
                    CatalogRole::Sql,
                    app.registry().module_digests()[0],
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let incarnation = IncarnationId::from_bytes([1; 16]);
        let cell_authority = CellAuthority::new(layout.clone());
        let initial = cell_authority
            .create_initial(&proof, incarnation, owner(0))
            .await
            .unwrap();
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            limits(),
        )
        .unwrap();
        let records = Arc::new(HashMap::from([(
            target.cell_id(),
            Record {
                target,
                incarnation,
                catalog: proof.clone(),
                replica: replica.clone(),
                authority: cell_authority.clone(),
            },
        )]));
        let mut nodes = Vec::new();
        let mut boots = Vec::new();
        for index in 0..count {
            let intent = journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node_id(index), session(index)).unwrap(),
                )
                .await
                .unwrap();
            let mut builder = CellNodeBuilder::new(app.clone())
                .with_runtime(
                    SqlWorkerPool::new(2, 8)
                        .unwrap()
                        .with_native_memory_limit(128 << 20)
                        .unwrap(),
                    64 << 20,
                )
                .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
                .with_session(session(index))
                .with_fleet_startup_intent(intent.clone());
            if index != 0 {
                builder = builder.with_follower_store(
                    root.path().join(format!("follower-{index}")),
                    limits(),
                    DiskBudget::new(1 << 30),
                );
            }
            let node = Arc::new(builder.build().unwrap());
            node.install_task_group(CancellationToken::new(), CancellationToken::new())
                .unwrap();
            node.install_fleet_actions(
                scope(),
                node_id(index),
                journal.clone(),
                Arc::new(adapters::Cells {
                    records: records.clone(),
                    local: index,
                    root: root.path().into(),
                }),
            )
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
            boots.push(startup::BootOwner {
                node: node.clone(),
                directory: directory.clone(),
                spec,
                advertisement: ad,
                guard: Some(guard),
            });
            nodes.push(node);
        }
        let locals = (1..count)
            .map(|index| {
                let store = nodes[index]
                    .try_owned_component::<FollowerStore>(cellule_host::FOLLOWER_STORE_COMPONENT)
                    .unwrap()
                    .unwrap();
                (
                    node_id(index),
                    LocalFollowerTransport::new(node_id(index), (*store).clone()),
                )
            })
            .collect();
        let transport = Arc::new(Transport {
            directory: directory.clone(),
            locals,
            requests: Mutex::new(Vec::new()),
            lose_retire: AtomicBool::new(false),
            appends: AtomicUsize::new(0),
            retirement_retry: Mutex::new(None),
        });
        let authority = Arc::new(Authority {
            directory: directory.clone(),
            serial: tokio::sync::Mutex::new(()),
            closed: Mutex::new(HashMap::new()),
            members: Mutex::new(HashMap::new()),
            coverage_gate: Mutex::new(None),
            attempts: AtomicUsize::new(0),
            lose_reply: AtomicBool::new(false),
        });
        let provider = Arc::new(Provider {
            directory: directory.clone(),
            transport: transport.clone(),
            authority: authority.clone(),
            lease: boots[0].guard.as_ref().unwrap().clone(),
            prepared: AtomicUsize::new(0),
            max_epochs,
            events: Mutex::new(Vec::new()),
            preparation: Mutex::new(None),
        });
        nodes[0]
            .install_fleet_node_durability_provider(
                scope(),
                node_id(0),
                journal.clone(),
                provider.clone(),
                NodeDurabilitySupervisorConfig::new(
                    scope().application,
                    limits(),
                    1,
                    count,
                    Duration::from_millis(10),
                    Duration::from_secs(60),
                    u64::MAX,
                )
                .unwrap(),
            )
            .unwrap();
        // No enrollment producer runs until all original boots are retained.
        let snapshot = journal.load_snapshot(scope()).await.unwrap();
        journal
            .bootstrap_registry(snapshot.registry())
            .await
            .unwrap();
        for index in 1..count {
            nodes[index].start().unwrap();
            if index >= 3 {
                continue;
            }
            let ad = boots[index]
                .refresh_capacity(
                    index,
                    journal.as_ref(),
                    Instant::now() + Duration::from_secs(3),
                )
                .await
                .unwrap();
            assert!(ad.capacity().follower_free_bytes > 0);
        }
        nodes[0].start().unwrap();
        until(|| nodes[0].runtime().node_durability().is_some()).await;
        // Recruitment can use the source's closed startup advertisement for
        // outbound work. A live replacement proof requires its real Ready-mode
        // heartbeat, never an invented admission sample.
        boots[0]
            .refresh_capacity(0, journal.as_ref(), Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        let native = Fixture {
            root,
            layout,
            node: nodes[0].clone(),
            directory,
            journal,
            provider,
            transport,
            authority,
        };
        let handle = nodes[0]
            .runtime()
            .bootstrap(
                proof,
                replica,
                cell_authority,
                initial,
                native.root.path().join("source.sqlite"),
                |tx| {
                    tx.execute_batch(
                        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (17)",
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        let now = clock().unwrap();
        let (entered, resume, completed) = native.authority.pause_coverage();
        let outcome = handle
            .execute(
                MutationIdentity {
                    request_id: RequestId::from_bytes([1; 16]),
                    issued_at_ms: now,
                    expires_at_ms: now + 60_000,
                },
                Digest::from_bytes([1; 32]),
                now,
                64,
                64,
                |tx| {
                    tx.execute_batch("UPDATE counter SET value = 29")?;
                    Ok(HandlerOutcome::Success(29i64.to_be_bytes().to_vec()))
                },
            )
            .await
            .unwrap();
        assert!(matches!(outcome, StoredOutcome::Success { .. }));
        super::super::captured(entered).await;
        assert!(
            native
                .directory
                .load(session(0), clock().unwrap())
                .await
                .unwrap()
                .unwrap()
                .advertisement()
                .log()
                .unwrap()
                .active()
        );
        resume.send(()).unwrap();
        super::super::captured(completed).await;
        until(|| native.transport.appends.load(Ordering::Acquire) >= 2).await;
        let snapshot = native.journal.load_snapshot(scope()).await.unwrap();
        native
            .journal
            .claim_controller(
                scope(),
                snapshot.head().revision(),
                session(9),
                clock().unwrap(),
            )
            .await
            .unwrap();
        Self {
            native,
            nodes,
            boots,
            handle,
            records,
        }
    }

    pub async fn finish(self) {
        // Keep the enrolled receiving boots live through the canonical owner drain.
        for node in &self.nodes {
            node.shutdown().await.unwrap();
        }
        for boot in &self.boots {
            boot.withdraw(self.native.journal.as_ref()).await.unwrap();
        }
        for node in &self.nodes {
            assert_eq!(node.state(), NodeState::Stopped);
            assert_eq!(node.stats().retained_bytes(), 0);
            assert_eq!(node.stats().local_disk_reserved_bytes(), 0);
        }
        let rows = self.native.rows().await;
        let followers = rows
            .iter()
            .filter(|row| matches!(row.spec().role, EnrollmentRole::Follower { .. }))
            .count();
        let expected_followers = if self.nodes.len() == 4 && self.native.provider.max_epochs >= 2 {
            2 * self.native.provider.max_epochs
        } else {
            2
        };
        assert_eq!(rows.len(), self.nodes.len() + expected_followers);
        assert!(
            rows.iter()
                .all(|row| row.status() == EnrollmentStatus::Retired)
        );
        assert_eq!(followers, expected_followers);
        self.native.journal.close().await.unwrap();
    }
}
