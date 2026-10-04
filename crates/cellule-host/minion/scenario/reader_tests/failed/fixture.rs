use super::*;

impl Fixture {
    pub(super) async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
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
        let app = application::compile().unwrap();
        let code = app.registry().module_digests()[0];
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("failed-readers"),
            [3; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            scope().fleet,
            Digest::from_bytes([31; 32]),
            app.registry().release_digest(),
        );
        let now = clock().unwrap();
        let mut boots = Vec::new();
        for index in [0, 1] {
            let intent = journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node_id(index), session(index)).unwrap(),
                )
                .await
                .unwrap();
            let ad = NodeAdvertisement::sign(
                node_id(index),
                session(index),
                owner(index).endpoint,
                scope().fleet,
                Digest::from_bytes([30; 32]),
                Digest::from_bytes([31; 32]),
                app.registry().release_digest(),
                &SigningKey::from_bytes(&[index as u8 + 1; 32]),
                1,
                now,
                now + 30_000,
                vec![code],
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity {
                    free_memory_bytes: 1 << 30,
                    free_disk_bytes: 1 << 30,
                    job_credits: 4,
                    log_protocol: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            boots.push(
                startup::enroll(
                    journal.as_ref(),
                    &directory,
                    &startup::spec(&intent).unwrap(),
                    ad,
                    now,
                )
                .await
                .unwrap(),
            );
        }
        let limits = Limits {
            max_database_bytes: 64 << 20,
            max_capture_bytes: 16 << 20,
            ..Default::default()
        };
        let node = Arc::new(
            CellNodeBuilder::new(app.clone())
                .with_runtime(
                    SqlWorkerPool::new(2, 8)
                        .unwrap()
                        .with_native_memory_limit(128 << 20)
                        .unwrap(),
                    16 << 20,
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
                directory.clone(),
                root.path().join("readers"),
                limits,
            )
            .unwrap();
        node.install_node_lease(NodeLeaseGuard::new(clock().unwrap(), now + 30_000).unwrap())
            .unwrap();
        node.start().unwrap();
        let source = CellRuntime::new_with_replica_host(
            SqlWorkerPool::new(2, 8).unwrap(),
            16 << 20,
            session(0),
            Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)),
        )
        .unwrap();
        let mut readers = Vec::new();
        let mut views = Vec::new();
        let mut handles = Vec::new();
        for partition in [1, 2] {
            let target = CellTarget::new(
                TenantId::from_bytes([1; 16]),
                scope().application,
                application::NAMESPACE,
                &[partition],
            )
            .unwrap();
            let incarnation = IncarnationId::from_bytes([partition; 16]);
            let proof = CellCatalog::new(layout.clone(), target.tenant())
                .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
                .await
                .unwrap();
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
            let handle = source
                .bootstrap(
                    proof,
                    replica,
                    authority,
                    initial,
                    root.path().join(format!("source-{partition}.sqlite")),
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
            let prepared = manager
                .prepare_source(target.clone(), session(0))
                .await
                .unwrap();
            let spec = EnrollmentSpec {
                scope: scope(),
                request: Digest::from_bytes([partition + 90; 32]),
                source: Some(EnrollmentEndpoint {
                    node: node_id(0),
                    session: session(0),
                    intent_revision: 1,
                }),
                target: EnrollmentEndpoint {
                    node: node_id(1),
                    session: session(1),
                    intent_revision: 1,
                },
                role: EnrollmentRole::Reader {
                    target: target.clone(),
                    position: PublishedPosition {
                        incarnation,
                        epoch: prepared.epoch(),
                        root: prepared.root().clone(),
                    },
                },
            };
            // This fixture's application owns and joins native opening. Pending
            // precedes the ordinary exact-source activation; no host producer
            // is installed that could later publish a different retirement.
            let FleetEnrollmentAcceptance::New(mut original) = journal
                .accept_enrollment(&spec, clock().unwrap())
                .await
                .unwrap()
            else {
                panic!("new reader expected")
            };
            let receipt = manager.activate_source(prepared).await.unwrap();
            let reader = manager.resolve(target).await.unwrap();
            assert_eq!(
                reader
                    .query::<application::ReadValue>(Some(receipt), 0)
                    .await
                    .unwrap()
                    .output,
                17
            );
            if partition == 1 {
                original = journal
                    .publish_enrollment_result(
                        &original,
                        EnrollmentEvent::Established(Digest::from_bytes([71; 32])),
                        clock().unwrap(),
                    )
                    .await
                    .unwrap();
            }
            readers.push(original);
            views.push(reader);
            handles.push(handle);
        }
        let snapshot = journal.load_snapshot(scope()).await.unwrap();
        journal
            .bootstrap_registry(snapshot.registry())
            .await
            .unwrap();
        // All source handles refer to runtime-owned actors. Keep one for exact
        // receipt readback; runtime shutdown joins both original source actors.
        Self {
            root,
            journal,
            node,
            directory,
            source,
            handle: handles.remove(0),
            manager,
            boot: boots.remove(1),
            readers,
            views,
        }
    }
    pub(super) async fn roster(&self) -> FleetRoster {
        let snapshot = self.journal.load_snapshot(scope()).await.unwrap();
        FleetRoster::collect(self.journal.as_ref(), &snapshot, deadline())
            .await
            .unwrap()
    }
    pub(super) async fn fence(&self) {
        let original = self
            .directory
            .load(session(1), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        self.directory
            .withdraw(&original, clock().unwrap())
            .await
            .unwrap();
    }
    pub(super) async fn capture(&self, index: usize) -> FleetFailedReaderRetirement {
        FleetFailedReaderRetirement::capture(
            self.journal.as_ref(),
            &self.directory,
            &self.roster().await,
            &self.boot,
            &self.readers[index],
            session(0),
            deadline(),
            clock,
        )
        .await
        .unwrap()
    }
    pub(super) fn process_path(&self) -> PathBuf {
        self.root.path().join("joined-original-lifetime")
    }
    pub(super) async fn join_and_retain(&self, request: &FleetFailedBootProcessRequest) {
        assert_eq!(request.boot().spec(), self.boot.spec());
        self.node.shutdown().await.unwrap();
        assert_eq!(self.node.state(), NodeState::Stopped);
        for view in &self.views {
            assert!(view.lifecycle_observation().await.locally_joined());
            assert!(matches!(
                view.query::<application::ReadValue>(None, 0).await,
                Err(Error::RuntimeClosed)
            ));
        }
        let EnrollmentRole::Reader { target, .. } = &self.readers[0].spec().role else {
            panic!("reader expected")
        };
        assert!(
            self.manager
                .activate(target.clone(), session(0))
                .await
                .is_err()
        );
        let stats = self.node.stats();
        assert_eq!(stats.resident_bytes(), 0);
        assert_eq!(stats.retained_bytes(), 0);
        assert_eq!(stats.worker_jobs(), 0);
        assert_eq!(stats.local_disk_reserved_bytes(), 0);
        // There are no application external jobs in this fixture. All native
        // accepted work is joined and the closed manager cannot restart it.
        let mut hash = blake3::Hasher::new();
        hash.update(b"joined-original-native-lifetime\0");
        hash.update(request.digest().as_bytes());
        let mut bytes = request.digest().as_bytes().to_vec();
        bytes.extend_from_slice(hash.finalize().as_bytes());
        use std::io::Write;
        let mut file = std::fs::File::create(self.process_path()).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    }
    pub(super) async fn row(&self, index: usize) -> EnrollmentRecord {
        self.journal
            .load_enrollment(scope(), self.readers[index].spec().key().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
    pub(super) async fn reconstruct(&self) -> SqliteJournal {
        SqliteJournal::open(
            self.root.path().join("journal.sqlite"),
            scope(),
            FleetProfile::default(),
            clock().unwrap(),
        )
        .await
        .unwrap()
    }
    pub(super) async fn finish(self) {
        self.node.shutdown().await.unwrap();
        let bytes = self
            .handle
            .query(64, 8, |connection| {
                let value: i64 =
                    connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap();
        assert_eq!(bytes, 17_i64.to_be_bytes());
        self.source.shutdown().await.unwrap();
        assert_eq!(self.source.stats().resident_bytes(), 0);
        assert_eq!(self.source.stats().retained_bytes(), 0);
        assert_eq!(self.source.stats().worker_jobs(), 0);
        assert_eq!(self.source.stats().local_disk_reserved_bytes(), 0);
        self.journal.close().await.unwrap();
    }
}
