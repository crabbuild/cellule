use super::*;

pub(super) struct Authority {
    directory: NodeDirectory,
    serial: tokio::sync::Mutex<()>,
    closed: Mutex<Option<NodeLogRotationBarrier>>,
    pub attempts: AtomicUsize,
    pub lose_reply: AtomicBool,
}
impl Authority {
    async fn current(
        &self,
        epoch: u64,
    ) -> cellule_runtime::Result<cellule_runtime::node::VersionedNodeAdvertisement> {
        let observed = self
            .directory
            .load_if_live(session(0), clock()?)
            .await?
            .ok_or(Error::Fenced)?;
        if observed.advertisement().node() != node_id(0)
            || observed
                .advertisement()
                .log()
                .is_none_or(|log| log.epoch() != epoch || log.members() != [node_id(1), node_id(2)])
        {
            return Err(Error::Fenced);
        }
        Ok(observed)
    }
}
impl NodeLogAuthority for Authority {
    fn activate<'a>(&'a self, epoch: u64) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let _serial = self.serial.lock().await;
            let observed = self.current(epoch).await?;
            self.directory.activate_log(&observed, clock()?).await?;
            Ok(())
        })
    }
    fn advance_coverage<'a>(
        &'a self,
        epoch: u64,
        through: u64,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let _serial = self.serial.lock().await;
            let observed = self.current(epoch).await?;
            self.directory
                .advance_log_coverage(&observed, through, clock()?)
                .await?;
            Ok(())
        })
    }
    fn close<'a>(
        &'a self,
        retirement: &'a NodeLogRetirementObservation,
    ) -> BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            retirement.confirmed()?;
            let _serial = self.serial.lock().await;
            self.attempts.fetch_add(1, Ordering::AcqRel);
            if let Some(original) = self.closed.lock().unwrap().as_ref() {
                assert_eq!(original, retirement.barrier());
                return Ok(());
            }
            let observed = self.current(retirement.barrier().log_epoch()).await?;
            let closed = self
                .directory
                .close_log(&observed, retirement.barrier(), clock()?)
                .await?;
            assert!(closed.advertisement().log().is_none());
            // Preserve the exact checked close receipt before losing its reply.
            *self.closed.lock().unwrap() = Some(retirement.barrier().clone());
            if self.lose_reply.swap(false, Ordering::AcqRel) {
                return Err(Error::Node("original canonical close reply lost"));
            }
            Ok(())
        })
    }
}
pub(super) struct Transport {
    directory: NodeDirectory,
    locals: Vec<(NodeId, LocalFollowerTransport)>,
    pub requests: Mutex<Vec<(NodeId, RetireRequest)>>,
    pub lose_retire: AtomicBool,
    retirement_retry: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}
impl Transport {
    pub fn pause_retirement_retry(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, captured) = tokio::sync::oneshot::channel();
        let (resume, release) = tokio::sync::oneshot::channel();
        *self.retirement_retry.lock().unwrap() = Some((entered, release));
        (captured, resume)
    }
    fn local(&self, node: NodeId) -> &LocalFollowerTransport {
        &self
            .locals
            .iter()
            .find(|(member, _)| *member == node)
            .unwrap()
            .1
    }
}
impl NodeLogTransport for Transport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async move {
            self.directory
                .authorize_log_append(
                    request.leader_session,
                    member,
                    request.log_epoch,
                    request.covered_through,
                    clock()?,
                )
                .await?;
            self.local(member).append(member, request).await
        })
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.local(member).seal(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<Vec<Bytes>>> {
        self.local(member).tail(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        Box::pin(async move {
            let retry = {
                let mut requests = self.requests.lock().unwrap();
                let retry = requests.iter().any(|(peer, original)| {
                    *peer == member
                        && original.leader_session == request.leader_session
                        && original.log_epoch == request.log_epoch
                });
                requests.push((member, request));
                retry
            };
            let gate = if retry {
                self.retirement_retry.lock().unwrap().take()
            } else {
                None
            };
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.await.unwrap();
            }
            self.directory
                .authorize_log_retire(
                    request.leader_session,
                    member,
                    request.log_epoch,
                    request.covered_through,
                    clock()?,
                )
                .await?;
            let receipt = self.local(member).retire(member, request).await?;
            if member == node_id(1) && self.lose_retire.load(Ordering::Acquire) {
                return Err(Error::Node("original member retirement reply lost"));
            }
            Ok(receipt)
        })
    }
}
pub(super) struct Provider {
    directory: NodeDirectory,
    transport: Arc<Transport>,
    authority: Arc<Authority>,
    lease: NodeLeaseGuard,
    pub prepared: AtomicUsize,
    pub events: Mutex<Vec<NodeDurabilityRotation>>,
    preparation: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}
impl Provider {
    pub fn pause_preparation(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, captured) = tokio::sync::oneshot::channel();
        let (resume, release) = tokio::sync::oneshot::channel();
        *self.preparation.lock().unwrap() = Some((entered, release));
        (captured, resume)
    }
}
impl FleetNodeDurabilityProvider for Provider {
    fn prepare(
        self: Arc<Self>,
        _limits: Limits,
        bytes: u64,
        live: usize,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = FacilityResult<Option<FleetNodeLogRecruitment>>>
                + Send,
        >,
    > {
        Box::pin(async move {
            // One unique epoch for this fixture. No second selection can hide
            // a refused, ambiguous, or incompletely retired original attempt.
            if self.prepared.fetch_add(1, Ordering::AcqRel) != 0 {
                return Ok(None);
            }
            let gate = self.preparation.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.await.unwrap();
            }
            let now = clock()?;
            let source = self
                .directory
                .load_if_live(session(0), now)
                .await?
                .ok_or(Error::Fenced)?;
            let prepared = self
                .directory
                .prepare_log_enrollment(&source, 1, bytes, live, now)
                .await?
                .ok_or(Error::Fenced)?;
            let attempt = self
                .directory
                .prepare_log_enrollment_attempt(&prepared, now)
                .await?;
            Ok(Some(FleetNodeLogRecruitment::new(
                self.directory.clone(),
                attempt,
                self.transport.clone(),
                self.authority.clone(),
                self.lease.clone(),
                Default::default(),
            )?))
        })
    }
    fn rotation_event(&self, event: NodeDurabilityRotation) {
        self.events.lock().unwrap().push(event);
    }
}
pub(super) struct Fixture {
    pub root: tempfile::TempDir,
    pub layout: CellStorageLayout,
    pub node: Arc<CellNode>,
    pub directory: NodeDirectory,
    pub journal: Arc<SqliteJournal>,
    pub provider: Arc<Provider>,
    pub transport: Arc<Transport>,
    pub authority: Arc<Authority>,
}
impl Fixture {
    pub async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = application::compile().unwrap();
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("enrolled-followers"),
            [3; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            scope().fleet,
            Digest::from_bytes([31; 32]),
            app.registry().release_digest(),
        );
        let now = clock().unwrap();
        for index in 0..3 {
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
                app.registry().module_digests(),
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity {
                    follower_free_bytes: 1 << 30,
                    free_memory_bytes: 1 << 30,
                    free_disk_bytes: 1 << 30,
                    job_credits: 4,
                    log_protocol: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            directory.create(ad, now).await.unwrap();
        }
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
        for index in 0..3 {
            journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node_id(index), session(index)).unwrap(),
                )
                .await
                .unwrap();
        }
        let lease = NodeLeaseGuard::new(now, now + 60_000).unwrap();
        let node = Arc::new(
            CellNodeBuilder::new(app)
                .with_runtime(
                    SqlWorkerPool::new(2, 8)
                        .unwrap()
                        .with_native_memory_limit(128 << 20)
                        .unwrap(),
                    16 << 20,
                )
                .with_session(session(0))
                .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
                .build()
                .unwrap(),
        );
        node.install_task_group(CancellationToken::new(), CancellationToken::new())
            .unwrap();
        node.install_node_lease_for_startup(lease.clone()).unwrap();
        let locals = (1..3)
            .map(|index| {
                let store = FollowerStore::open(
                    root.path().join(format!("follower-{index}")),
                    Limits::default(),
                    DiskBudget::new(1 << 30),
                )
                .unwrap();
                (
                    node_id(index),
                    LocalFollowerTransport::new(node_id(index), store),
                )
            })
            .collect();
        let transport = Arc::new(Transport {
            directory: directory.clone(),
            locals,
            requests: Mutex::new(Vec::new()),
            lose_retire: AtomicBool::new(false),
            retirement_retry: Mutex::new(None),
        });
        let authority = Arc::new(Authority {
            directory: directory.clone(),
            serial: tokio::sync::Mutex::new(()),
            closed: Mutex::new(None),
            attempts: AtomicUsize::new(0),
            lose_reply: AtomicBool::new(false),
        });
        let provider = Arc::new(Provider {
            directory: directory.clone(),
            transport: transport.clone(),
            authority: authority.clone(),
            lease,
            prepared: AtomicUsize::new(0),
            events: Mutex::new(Vec::new()),
            preparation: Mutex::new(None),
        });
        Self {
            root,
            layout,
            node,
            directory,
            journal,
            provider,
            transport,
            authority,
        }
    }
    pub fn install(&self) {
        self.install_limits(limits());
    }
    pub fn install_limits(&self, limits: Limits) {
        self.node
            .install_fleet_node_durability_provider(
                scope(),
                node_id(0),
                self.journal.clone(),
                self.provider.clone(),
                NodeDurabilitySupervisorConfig::new(
                    scope().application,
                    limits,
                    1,
                    3,
                    Duration::from_millis(10),
                    Duration::from_secs(60),
                    u64::MAX,
                )
                .unwrap(),
            )
            .unwrap();
        self.node.start().unwrap();
    }
    pub async fn rows(&self) -> Vec<EnrollmentRecord> {
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
                        tokio::task::yield_now().await
                    }
                    Err(error) => panic!("follower registry scan: {error}"),
                }
            }
        })
        .await
        .unwrap()
    }
    pub async fn installed(&self) {
        until(|| self.node.runtime().node_durability().is_some()).await;
        let rows = self.rows().await;
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.status() == EnrollmentStatus::Established)
        );
    }
    pub async fn finish(self) {
        self.node.shutdown().await.unwrap();
        assert_eq!(self.node.state(), NodeState::Stopped);
        assert!(self.rows().await.iter().all(|row| matches!(
            row.status(),
            EnrollmentStatus::Retired | EnrollmentStatus::Refused
        )));
        assert_eq!(self.node.stats().retained_bytes(), 0);
        assert_eq!(self.node.stats().local_disk_reserved_bytes(), 0);
        assert!(
            self.node
                .follower_enrollment_completion(1)
                .unwrap()
                .is_none()
        );
        self.journal.close().await.unwrap();
        assert!(self.root.path().exists());
    }
}
pub(super) async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

pub(super) fn limits() -> Limits {
    Limits {
        max_database_bytes: 64 << 20,
        max_capture_bytes: 16 << 20,
        ..Limits::default()
    }
}
