//! Completed SQL hands unused result admission to its exact publication cut.

use super::*;
use std::time::Duration;

struct SqlCompletionGate {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    resume: Mutex<mpsc::Receiver<()>>,
    responses: Mutex<Vec<CommandResponseSource>>,
}

impl CellTelemetry for SqlCompletionGate {
    fn command_execution(&self, _queue: Duration, _worker: Duration, succeeded: bool) {
        assert!(succeeded);
        if let Some(entered) = self.entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            // Pause after the original worker has returned its physical cut,
            // before the actor reserves publication memory. Only this test's
            // callback blocks; production telemetry must remain nonblocking.
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
    }

    fn command_response(&self, source: CommandResponseSource, _: Duration, _: Duration) {
        self.responses.lock().unwrap().push(source);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_result_capacity_funds_publication_after_sql_under_full_memory_pressure() {
    let fixture = fixture_for(b"publication-admission-handoff");
    let session = SessionId::from_bytes([144; 16]);
    let member = NodeId::from_bytes([145; 16]);
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1).unwrap(),
        2 * 1024 * 1024,
        session,
        ReplicaHost::default().with_dirty_slots(dirty.clone()),
    )
    .unwrap();
    let lease = NodeLeaseGuard::new(0, 60_000).unwrap();
    runtime.install_node_lease(lease.clone()).unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let follower = cellule_runtime::FollowerStore::open(
        directory.path().to_owned(),
        Limits::default(),
        DiskBudget::new(1 << 30),
    )
    .unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(
        cellule_runtime::node::log_transport::LocalFollowerTransport::new(member, follower),
    );
    let gate = DurabilityGate::new(
        session,
        NodeId::from_bytes(*session.as_bytes()),
        1,
        [member],
    )
    .unwrap();
    let shipper = NodeLogShipper::new(gate.clone(), transport.clone(), Limits::default()).unwrap();
    runtime
        .install_node_durability(
            fixture.target.application(),
            Arc::new(NodeDurability::new(
                gate,
                shipper,
                Arc::new(TestNodeAuthority::default()),
                transport,
                lease,
            )),
        )
        .unwrap();
    let handle = bootstrap_on(&runtime, &fixture, session).await;
    let occupied = dirty.clone().acquire_owned().await.unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = mpsc::channel();
    let observations = Arc::new(SqlCompletionGate {
        entered: Mutex::new(Some(entered)),
        resume: Mutex::new(resume_rx),
        responses: Mutex::new(Vec::new()),
    });
    runtime.install_telemetry(observations.clone()).unwrap();
    let identity = mutation_identity_window(146, 10, 10_000);
    let digest = Digest::from_bytes([146; 32]);
    let first_handle = handle.clone();
    let first = tokio::spawn(async move {
        first_handle
            .execute(identity, digest, 20, 1_024, 1 << 20, |tx| {
                tx.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(b"admitted".to_vec()))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let stats = runtime.stats();
    let pressure = runtime
        .try_reserve_node_bytes(stats.retained_capacity_bytes() - stats.retained_bytes())
        .unwrap();
    assert_eq!(
        runtime.stats().retained_bytes(),
        stats.retained_capacity_bytes()
    );
    resume.send(()).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), first).await;
    let progress = runtime.publication_progress().await;
    // Release every injected resource before checking results, including on
    // the old post-commit admission failure, so shutdown joins original work.
    drop(pressure);
    drop(occupied);
    let query = handle
        .query(16, 16, |db| {
            let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })
        .await;
    let retry = handle
        .execute(identity, digest, 21, 1_024, 1 << 20, |_| {
            panic!("a durable retry must never execute the SQL callback")
        })
        .await;
    let shutdown = runtime.shutdown().await;

    let outcome = outcome.unwrap().unwrap().unwrap();
    assert_eq!(outcome.commit_sequence(), 1);
    assert_eq!(outcome.result(), b"admitted");
    let progress = progress.unwrap();
    assert_eq!(progress.pending_publications, 1);
    assert!(progress.retained_capture_bytes > 0);
    assert_eq!(query.unwrap(), 1_i64.to_be_bytes());
    assert_eq!(retry.unwrap(), outcome);
    assert_eq!(
        observations.responses.lock().unwrap().as_slice(),
        &[
            CommandResponseSource::Fleet,
            CommandResponseSource::Recorded
        ]
    );
    shutdown.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
    assert_eq!(dirty.available_permits(), 1);

    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().state, ControlState::Idle);
    let root = control.value().ltx_root().unwrap();
    assert_eq!(root.commit_sequence, 1);
    let restored = fixture._directory.path().join("handoff-restored.sqlite");
    let verified = fixture.replica.open_root(&root).await.unwrap();
    assert_eq!(verified.restore(&restored).await.unwrap(), root.position);
    let mut recovered = cellule_runtime::cell::executor::CellExecutor::new(
        cellule_ltx::Db::open(&restored, Limits::default()).unwrap(),
        fixture.target.cell_id(),
        IncarnationId::from_bytes([2; 16]),
        1,
    );
    assert_eq!(
        recovered.resolve(identity, digest, 22, 1 << 20).unwrap(),
        Resolution::Committed(outcome)
    );
    recovered.close().unwrap();
}
