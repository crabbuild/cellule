//! Device-backed process loss of a two-follower ACK before root publication.
//! Native followers share the killed process; this is storage/recovery
//! qualification, not authentication or remote-peer transport qualification.
use super::*;
use cellule_runtime::node::log_transport::LocalFollowerTransport;
use cellule_runtime::node::{NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain};
use std::path::{Path as FsPath, PathBuf};
use std::process::{Child, Command, Stdio};

const CHILD: &str = "runtime::lifecycle::ownership::follower_process::rustfs_follower_tail_child";
const CHILD_ROOT: &str = "CELLULE_FOLLOWER_TAIL_CHILD_ROOT";
const PARTITION: &str = "CELLULE_FOLLOWER_TAIL_PARTITION";
const LEADER: SessionId = SessionId::from_bytes([151; 16]);
const SUCCESSOR: SessionId = SessionId::from_bytes([154; 16]);
const MEMBERS: [NodeId; 2] = [NodeId::from_bytes([152; 16]), NodeId::from_bytes([153; 16])];

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn fixture(
    store: Store,
    partition: bool,
) -> (CellTarget, CellStorageLayout, CellReplica, CellAuthority) {
    let target = CellTarget::new(
        TenantId::from_bytes([61; 16]),
        ApplicationId::from_bytes([62; 16]),
        NamespaceId::from_bytes([63; 16]),
        b"follower-process-tail",
    )
    .unwrap();
    let prefix = std::env::var("CELLULE_TEST_PREFIX").unwrap();
    let layout = CellStorageLayout::new(
        store,
        Path::from(format!(
            "{prefix}/follower-process-tail/{}",
            if partition { "partition" } else { "kill" }
        )),
        [62; 16],
    );
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        [64; 16],
        Limits::default(),
    )
    .unwrap();
    let authority = CellAuthority::new(layout.clone());
    (target, layout, replica, authority)
}

fn directory(layout: &CellStorageLayout) -> NodeDirectory {
    NodeDirectory::new(
        layout.clone(),
        Digest::from_bytes([90; 32]),
        Digest::from_bytes([91; 32]),
        Digest::from_bytes([92; 32]),
    )
}

async fn enroll(directory: &NodeDirectory) {
    let mut leader = None;
    for session in [
        LEADER,
        SessionId::from_bytes([152; 16]),
        SessionId::from_bytes([153; 16]),
        SUCCESSOR,
    ] {
        let expires = if session == LEADER { 10_001 } else { 20_000 };
        let ad = NodeAdvertisement::sign(
            NodeId::from_bytes(*session.as_bytes()),
            session,
            "https://process-fixture.internal:8081".into(),
            Digest::from_bytes([90; 32]),
            Digest::from_bytes([94; 32]),
            Digest::from_bytes([91; 32]),
            Digest::from_bytes([92; 32]),
            &ed25519_dalek::SigningKey::from_bytes(
                session.as_bytes().repeat(2).as_slice().try_into().unwrap(),
            ),
            1,
            1,
            expires,
            vec![Digest::from_bytes([95; 32])],
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity {
                free_memory_bytes: 1 << 20,
                free_disk_bytes: 1 << 30,
                follower_free_bytes: if MEMBERS.contains(&NodeId::from_bytes(*session.as_bytes())) {
                    1 << 30
                } else {
                    0
                },
                follower_retained_bytes: 0,
                job_credits: 1,
                log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
            },
        )
        .unwrap();
        let observed = directory.create(ad, 1).await.unwrap();
        if session == LEADER {
            leader = Some(observed);
        }
    }
    let enrolled = directory
        .recruit_log(&leader.unwrap(), 1, 2, 4, 2)
        .await
        .unwrap();
    assert_eq!(enrolled.advertisement().log().unwrap().members(), MEMBERS);
    directory.activate_log(&enrolled, 3).await.unwrap();
}

struct NativeAuthority(NodeDirectory);
impl NodeLogAuthority for NativeAuthority {
    fn activate<'a>(
        &'a self,
        epoch: u64,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async move {
            let current = self
                .0
                .load_if_live(LEADER, 4)
                .await?
                .ok_or(cellule_runtime::Error::Fenced)?;
            let log = current
                .advertisement()
                .log()
                .ok_or(cellule_runtime::Error::Fenced)?;
            assert_eq!(log.epoch(), epoch);
            assert!(log.active());
            assert_eq!(log.members(), MEMBERS);
            Ok(())
        })
    }
    fn advance_coverage<'a>(
        &'a self,
        _epoch: u64,
        _through: u64,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<()>> {
        // This cut must retain an unpublished tail. Any credit here fails the test.
        Box::pin(async {
            Err(cellule_runtime::Error::Node(
                "unexpected bucket credit before killed owner",
            ))
        })
    }
    fn close<'a>(
        &'a self,
        _retirement: &'a cellule_runtime::node::log::NodeLogRetirementObservation,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<()>> {
        Box::pin(async {
            Err(cellule_runtime::Error::Node(
                "killed owner cannot close its log",
            ))
        })
    }
}

struct NativeTransport(Vec<(NodeId, LocalFollowerTransport)>);
impl NativeTransport {
    fn open(root: &FsPath) -> Self {
        Self(
            MEMBERS
                .into_iter()
                .enumerate()
                .map(|(index, member)| {
                    let store = cellule_runtime::FollowerStore::open(
                        root.join(format!("follower-{index}")),
                        Limits::default(),
                        DiskBudget::new(1 << 30),
                    )
                    .unwrap();
                    (member, LocalFollowerTransport::new(member, store))
                })
                .collect(),
        )
    }
    fn peer(&self, member: NodeId) -> &LocalFollowerTransport {
        &self.0.iter().find(|(id, _)| *id == member).unwrap().1
    }
}
impl NodeLogTransport for NativeTransport {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.peer(member).append(member, request)
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.peer(member).seal(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<FollowerReceipt>> {
        self.peer(member).retire(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> futures_util::future::BoxFuture<'a, cellule_runtime::Result<Vec<Bytes>>> {
        self.peer(member).tail(member, request)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_follower_tail_child() {
    let Ok(root) = std::env::var(CHILD_ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    let partition = std::env::var_os(PARTITION).is_some();
    let remote = sparse_process::rustfs_store();
    let pausing = Arc::new(PausingStore::new(remote.inner().clone()));
    let (target, layout, replica, authority) = fixture(Store::new(pausing.clone()), partition);
    let catalog = cellule_runtime::cell::catalog::CellCatalog::new(layout.clone(), target.tenant());
    let proof = catalog.lookup(target.cell_id()).await.unwrap().unwrap();
    let observed = authority.load(target.cell_id()).await.unwrap().unwrap();
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 10).unwrap(),
        64 << 20,
        LEADER,
        ReplicaHost::default(),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let handle = runtime
        .acquire_idle_restored(
            proof,
            replica,
            authority.clone(),
            observed,
            root.join("owner/active.sqlite"),
            Owner {
                session: LEADER,
                endpoint: "https://process-fixture.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    if partition {
        let first = handle
            .execute(
                mutation_identity_window(155, 10, 10_000),
                Digest::from_bytes([156; 32]),
                20,
                1_024,
                1_024,
                |tx| {
                    tx.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(
                        b"published-before-partition".to_vec(),
                    ))
                },
            )
            .await
            .unwrap();
        assert_eq!(first.commit_sequence(), 1);
        assert_eq!(
            authority
                .load(target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .value()
                .ltx_root()
                .unwrap()
                .commit_sequence,
            1
        );
    }
    let transport: Arc<dyn NodeLogTransport> = Arc::new(NativeTransport::open(&root));
    let gate =
        DurabilityGate::new(LEADER, NodeId::from_bytes(*LEADER.as_bytes()), 1, MEMBERS).unwrap();
    let shipper = NodeLogShipper::new(gate.clone(), transport.clone(), Limits::default()).unwrap();
    runtime
        .install_node_durability(
            target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(NativeAuthority(directory(&layout))),
                transport,
                lease,
            )),
        )
        .unwrap();
    let responses = Arc::new(super::super::durability::RecordingResponses::default());
    runtime.install_telemetry(responses.clone()).unwrap();
    runtime.active_catalog_entries().await.unwrap();
    if partition {
        std::fs::write(
            root.join("ready-to-partition"),
            b"published-before-partition:1",
        )
        .unwrap();
        while !root.join("partition-started").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let uncertain = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.execute(
                mutation_identity_window(157, 10, 10_000),
                Digest::from_bytes([158; 32]),
                21,
                1_024,
                1_024,
                |tx| {
                    tx.execute("UPDATE counter SET value = value + 1", [])?;
                    Ok(HandlerOutcome::Success(b"uncertain-partition".to_vec()))
                },
            ),
        )
        .await;
        assert!(
            !matches!(uncertain, Ok(Ok(_))),
            "partition exposed unproved success: {uncertain:?}"
        );
        assert!(responses.0.lock().unwrap().is_empty());
        // Timeout is uncertain: it does not certify that SQLite never committed.
        std::fs::write(root.join("partition-result"), format!("{uncertain:?}")).unwrap();
        std::fs::write(root.join("acknowledged"), b"published-before-partition:1").unwrap();
        std::future::pending::<()>().await;
    }
    // Hold the first data PUT, before either bytes or root credit are published.
    pausing.arm();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        handle.execute(
            mutation_identity_window(155, 10, 10_000),
            Digest::from_bytes([156; 32]),
            20,
            1_024,
            1_024,
            |tx| {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"fleet-tail".to_vec()))
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        outcome,
        StoredOutcome::Success {
            result: b"fleet-tail".to_vec(),
            commit_sequence: 1
        }
    );
    assert_eq!(
        responses.0.lock().unwrap().as_slice(),
        &[cellule_runtime::fleet::telemetry::CommandResponseSource::Fleet]
    );
    pausing.wait_until_blocked().await;
    assert_eq!(
        authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root()
            .unwrap()
            .commit_sequence,
        0
    );
    std::fs::write(root.join("acknowledged"), b"fleet-tail:1").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated RustFS, explicit credentials and CELLULE_TEST_DEVICE_PATH on a device filesystem"]
async fn rustfs_killed_follower_tail_owner_recovers_every_ack_and_exact_retry() {
    run_cut(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated RustFS, explicit credentials and CELLULE_TEST_DEVICE_PATH on a device filesystem"]
async fn rustfs_partitioned_owner_exposes_no_unproved_success_and_recovers_earlier_ack() {
    run_cut(true).await;
}

async fn run_cut(partition: bool) {
    let device = PathBuf::from(std::env::var("CELLULE_TEST_DEVICE_PATH").unwrap());
    let scratch = tempfile::tempdir_in(device).unwrap();
    let root = scratch.path();
    std::fs::create_dir(root.join("owner")).unwrap();
    let (target, layout, replica, authority) = fixture(sparse_process::rustfs_store(), partition);
    let catalog = cellule_runtime::cell::catalog::CellCatalog::new(layout.clone(), target.tenant());
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Application,
                Digest::from_bytes([65; 32]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let initial = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([64; 16]),
            Owner {
                session: LEADER,
                endpoint: "https://process-fixture.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let bootstrap_runtime =
        CellRuntime::new(SqlWorkerPool::new(1, 10).unwrap(), 64 << 20, LEADER).unwrap();
    let bootstrap = bootstrap_runtime
        .bootstrap(
            proof.clone(),
            replica.clone(),
            authority.clone(),
            initial,
            root.join("bootstrap.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    bootstrap.drain().await.unwrap();
    bootstrap_runtime.shutdown().await.unwrap();
    let directory = directory(&layout);
    // The exact ensemble is authoritative before the child can acknowledge.
    enroll(&directory).await;
    let proxy = if partition {
        Some(super::partition_proxy::PartitionProxy::start().await)
    } else {
        None
    };
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CHILD, "--nocapture"])
        .env(CHILD_ROOT, root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(proxy) = &proxy {
        command
            .env(PARTITION, "1")
            .env("CELLULE_TEST_ENDPOINT", &proxy.endpoint);
    }
    let mut child = KillOnDrop(command.spawn().unwrap());
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before its ACK cut"
            );
            if root.join("acknowledged").exists() {
                break;
            }
            if partition
                && root.join("ready-to-partition").exists()
                && !root.join("partition-started").exists()
            {
                assert_eq!(
                    std::fs::read(root.join("ready-to-partition")).unwrap(),
                    b"published-before-partition:1"
                );
                proxy.as_ref().unwrap().partition();
                std::fs::write(root.join("partition-started"), b"network-cut").unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(root.join("acknowledged")).unwrap(),
        if partition {
            b"published-before-partition:1".as_slice()
        } else {
            b"fleet-tail:1".as_slice()
        }
    );
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    if let Some(proxy) = proxy {
        assert!(
            proxy.rejected() > 0,
            "network partition was never exercised"
        );
        eprintln!(
            "partition={} rejected_connections={} response={}",
            proxy.endpoint,
            proxy.rejected(),
            std::fs::read_to_string(root.join("partition-result")).unwrap()
        );
        proxy.shutdown().await;
    }
    std::fs::remove_dir_all(root.join("owner")).unwrap();
    assert_eq!(
        authority
            .load(target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root()
            .unwrap()
            .commit_sequence,
        u64::from(partition)
    );
    let transport: Arc<dyn NodeLogTransport> = Arc::new(NativeTransport::open(root));
    let fenced = directory
        .claim_expired(LEADER, SUCCESSOR, 10_001)
        .await
        .unwrap();
    for member in MEMBERS {
        assert_eq!(
            transport
                .seal(
                    member,
                    SealRequest {
                        leader_session: LEADER,
                        log_epoch: 1
                    }
                )
                .await
                .unwrap(),
            FollowerReceipt {
                base_sequence: 1,
                durable_through: 1
            }
        );
        let frames = transport
            .tail(
                member,
                TailRequest {
                    leader_session: LEADER,
                    log_epoch: 1,
                    first_sequence: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(frames.len(), 1);
        let frame = cellule_ltx::inspect_node_frame(frames[0].clone(), Limits::default()).unwrap();
        assert_eq!(frame.scope().leader_session, *LEADER.as_bytes());
        assert_eq!(frame.scope().log_epoch, 1);
        assert_eq!(frame.scope().node_sequence, 1);
        assert_eq!(frame.scope().commit_sequence, if partition { 2 } else { 1 });
        assert_eq!(frame.scope().cell, *target.cell_id().as_bytes());
    }
    let recovery = cellule_runtime::node::log_recovery::NodeLogRecovery::from_fenced(
        transport,
        &fenced,
        Limits::default(),
    )
    .unwrap();
    let manifests = cellule_runtime::recovery::manifest::RecoveryManifestStore::new(
        layout.clone(),
        Limits::default(),
    );
    let coordinator =
        cellule_runtime::node::log_recovery::RecoveryCoordinator::new(recovery, manifests.clone());
    let inventory =
        cellule_runtime::node::log_recovery::recoverable_cells(&catalog, &authority, LEADER, 10)
            .await
            .unwrap();
    let completed = coordinator
        .recover_and_seal(&directory, fenced, inventory, 10_002)
        .await
        .unwrap();
    assert_eq!(completed.controls.len(), 1);
    let successor =
        CellRuntime::new(SqlWorkerPool::new(1, 10).unwrap(), 64 << 20, SUCCESSOR).unwrap();
    let restored = successor
        .takeover_restored(
            proof,
            replica,
            authority.clone(),
            completed.controls.into_iter().next().unwrap(),
            completed.takeover,
            manifests,
            root.join("successor.sqlite"),
            Owner {
                session: SUCCESSOR,
                endpoint: "https://successor.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let retry = restored
        .execute(
            mutation_identity_window(155, 10, 10_000),
            Digest::from_bytes([156; 32]),
            30,
            1_024,
            1_024,
            |_| {
                Err(cellule_runtime::Error::Control(
                    "acknowledged follower-tail retry executed twice",
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(
        retry,
        StoredOutcome::Success {
            result: if partition {
                b"published-before-partition".to_vec()
            } else {
                b"fleet-tail".to_vec()
            },
            commit_sequence: 1
        }
    );
    let bytes = restored
        .query(64, 64, |connection| {
            let value: i64 =
                connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await
        .unwrap();
    assert_eq!(
        i64::from_be_bytes(bytes.try_into().unwrap()),
        if partition { 2 } else { 1 }
    );
    if partition {
        let resolved = restored
            .execute(
                mutation_identity_window(157, 10, 10_000),
                Digest::from_bytes([158; 32]),
                31,
                1_024,
                1_024,
                |_| {
                    Err(cellule_runtime::Error::Control(
                        "recovered uncertain partition retry executed twice",
                    ))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            resolved,
            StoredOutcome::Success {
                result: b"uncertain-partition".to_vec(),
                commit_sequence: 2
            }
        );
        let bytes = restored
            .query(64, 64, |connection| {
                let value: i64 =
                    connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap();
        assert_eq!(i64::from_be_bytes(bytes.try_into().unwrap()), 2);
    }
    restored.drain().await.unwrap();
    successor.shutdown().await.unwrap();
    scratch.close().unwrap();
}
