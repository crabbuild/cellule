//! Quiet Cells must retain compaction progress while independent writers queue.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_and_shutdown_cancel_unadmitted_quiet_compactions() {
    let dirty = Arc::new(tokio::sync::Semaphore::new(8));
    let recovery = Arc::new(tokio::sync::Semaphore::new(2));
    let session = SessionId::from_bytes([62; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 4).unwrap(),
        16 * 1024 * 1024,
        session,
        ReplicaHost::default()
            .with_dirty_slots(dirty.clone())
            .with_io_slots(Arc::new(tokio::sync::Semaphore::new(32)))
            .with_job_slots(Arc::new(tokio::sync::Semaphore::new(8)))
            .with_recovery_slots(recovery.clone()),
    )
    .unwrap();
    let fixtures: Vec<_> = (0..4_u8).map(|index| fixture_for(&[62, index])).collect();
    let mut handles = Vec::new();
    for fixture in &fixtures {
        handles.push(bootstrap_on(&runtime, fixture, session).await);
    }
    let occupied = recovery.clone().acquire_many_owned(2).await.unwrap();
    for handle in &handles {
        for sequence in 1..=8 {
            write(handle, sequence).await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    // Recovery stays externally occupied through both operations. Neither may
    // need a compaction slot or abandon accepted work to close the Cell.
    let drained = tokio::time::timeout(std::time::Duration::from_secs(1), handles[0].drain()).await;
    let shutdown =
        tokio::time::timeout(std::time::Duration::from_secs(1), runtime.shutdown()).await;
    drop(occupied);
    if shutdown.is_err() {
        runtime.shutdown().await.unwrap();
    }
    drained
        .expect("drain waited for optional compaction admission")
        .unwrap();
    shutdown
        .expect("shutdown waited for optional compaction admission")
        .unwrap();
    assert_eq!(runtime.stats().active_cells(), 0);
    assert_eq!(dirty.available_permits(), 8);
    assert_eq!(recovery.available_permits(), 2);
    for fixture in &fixtures {
        check_restored(fixture, 8).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiet_compaction_progresses_under_continuous_independent_writes() {
    let dirty = Arc::new(tokio::sync::Semaphore::new(8));
    let recovery = Arc::new(tokio::sync::Semaphore::new(2));
    let session = SessionId::from_bytes([61; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 64).unwrap(),
        16 * 1024 * 1024,
        session,
        ReplicaHost::default()
            .with_dirty_slots(dirty.clone())
            .with_io_slots(Arc::new(tokio::sync::Semaphore::new(32)))
            .with_job_slots(Arc::new(tokio::sync::Semaphore::new(8)))
            .with_recovery_slots(recovery.clone()),
    )
    .unwrap();
    let quiet = fixture_for(b"quiet-under-independent-writes");
    let quiet_handle = bootstrap_on(&runtime, &quiet, session).await;
    let slow = Arc::new(PausingStore::new(Arc::new(InMemory::new())));
    let fixtures: Vec<_> = (0..32_u8)
        .map(|index| {
            fixture_with_limits_and_store(&[61, index], Limits::default(), Store::new(slow.clone()))
        })
        .collect();
    let mut handles = Vec::new();
    for fixture in &fixtures {
        handles.push(bootstrap_on(&runtime, fixture, session).await);
    }
    let occupied = recovery.clone().acquire_many_owned(2).await.unwrap();
    for sequence in 1..=8_u8 {
        write(&quiet_handle, sequence).await;
    }
    let authority = CellAuthority::new(quiet.layout.clone());
    let before = authority
        .load(quiet.target.cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .ltx_root()
        .unwrap();
    assert!(
        quiet
            .replica
            .open_root(&before)
            .await
            .unwrap()
            .segment_count()
            >= 8
    );

    // More active writers than dirty slots keep real root preparation queued.
    // Only independent foreground stores are delayed; the quiet Cell's native
    // compaction and object path have their ordinary resources and latency.
    slow.delay_puts(50);
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = handles
        .into_iter()
        .map(|handle| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut sequence = 0_u8;
                while !stop.load(Ordering::Acquire) {
                    sequence = sequence.checked_add(1).unwrap();
                    write(&handle, sequence).await;
                }
                sequence
            })
        })
        .collect();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while dirty.available_permits() > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    drop(occupied);
    let promoted = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let root = authority
                .load(quiet.target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .value()
                .ltx_root()
                .unwrap();
            if root != before {
                break root;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    // Stop the load and join accepted work even when the progress assertion
    // fails. A cancelled client is not permission to abandon durable writes.
    stop.store(true, Ordering::Release);
    let sequences = futures_util::future::join_all(writers).await;
    runtime.shutdown().await.unwrap();
    assert_eq!(dirty.available_permits(), 8);
    assert_eq!(recovery.available_permits(), 2);
    let promoted = promoted.expect("continuous independent writes starved quiet compaction");
    assert_eq!(promoted.position, before.position);
    assert_eq!(promoted.commit_sequence, before.commit_sequence);
    assert!(
        quiet
            .replica
            .open_root(&promoted)
            .await
            .unwrap()
            .segment_count()
            < 8
    );
    check_restored(&quiet, 8).await;
    for (fixture, sequence) in fixtures.iter().zip(sequences) {
        check_restored(fixture, sequence.unwrap()).await;
    }
}

async fn write(handle: &cellule_runtime::cell::actor::CellHandle, sequence: u8) {
    let outcome = handle
        .execute(
            mutation_identity_window(sequence, 10, 10_000),
            Digest::from_bytes([sequence; 32]),
            20,
            16,
            16,
            |transaction| {
                transaction.execute("UPDATE counter SET value = value + 1", [])?;
                Ok(HandlerOutcome::Success(Vec::new()))
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome.commit_sequence(), u64::from(sequence));
}

async fn check_restored(fixture: &Fixture, value: u8) {
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.value().state, ControlState::Idle);
    let root = control.value().ltx_root().unwrap();
    assert_eq!(root.commit_sequence, u64::from(value));
    let restored = fixture._directory.path().join("fairness-restored.sqlite");
    fixture
        .replica
        .open_root(&root)
        .await
        .unwrap()
        .restore(&restored)
        .await
        .unwrap();
    let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, u8>(0))
            .unwrap(),
        value
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_waiting_for_compaction_recovery_leaves_independent_dirty_capacity_available() {
    struct Admissions {
        count: AtomicUsize,
        changed: tokio::sync::Notify,
    }
    impl cellule_runtime::fleet::telemetry::CellTelemetry for Admissions {
        fn ltx_phase(
            &self,
            phase: cellule_ltx::LtxPhase,
            _elapsed: std::time::Duration,
            succeeded: bool,
        ) {
            if phase == cellule_ltx::LtxPhase::DirtyAdmission && succeeded {
                self.count.fetch_add(1, Ordering::AcqRel);
                self.changed.notify_one();
            }
        }
    }
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let recovery = Arc::new(tokio::sync::Semaphore::new(1));
    let observed = Arc::new(Admissions {
        count: AtomicUsize::new(0),
        changed: tokio::sync::Notify::new(),
    });
    let session = SessionId::from_bytes([63; 16]);
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(2, 4).unwrap(),
        16 * 1024 * 1024,
        session,
        ReplicaHost::default()
            .with_dirty_slots(dirty.clone())
            .with_recovery_slots(recovery.clone()),
    )
    .unwrap();
    runtime.install_telemetry(observed.clone()).unwrap();
    let pressure = fixture_with_limits(
        b"admission-compaction-pressure",
        Limits {
            max_segments: 4,
            ..Limits::default()
        },
    );
    let independent = fixture_for(b"admission-independent-writer");
    let pressure_handle = bootstrap_on(&runtime, &pressure, session).await;
    let independent_handle = bootstrap_on(&runtime, &independent, session).await;
    write(&pressure_handle, 1).await;
    write(&pressure_handle, 2).await;
    let baseline = observed.count.load(Ordering::Acquire);
    let occupied = recovery.clone().acquire_owned().await.unwrap();
    let pressure_writer = tokio::spawn(async move {
        write(&pressure_handle, 3).await;
    });
    let admission_observed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while observed.count.load(Ordering::Acquire) == baseline {
            observed.changed.notified().await;
        }
    })
    .await
    .is_ok();
    // The selected append now needs compaction. It must leave dirty available
    // while recovery remains externally occupied, so an independent normal
    // root can publish without waiting for that recovery permit.
    let mut independent_writer = tokio::spawn(async move {
        write(&independent_handle, 1).await;
    });
    let progress =
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut independent_writer).await;
    let progressed = matches!(&progress, Ok(Ok(())));
    drop(occupied);
    let pressure_result = pressure_writer.await;
    let independent_result = if progress.is_err() {
        Some(independent_writer.await)
    } else {
        None
    };
    let shutdown = runtime.shutdown().await;
    assert!(
        admission_observed,
        "pressure append never reached dirty admission"
    );
    pressure_result.unwrap();
    if let Some(result) = independent_result {
        result.unwrap();
    }
    shutdown.unwrap();
    assert!(
        progressed,
        "recovery wait retained an unnecessary dirty cohort: {progress:?}"
    );
    assert_eq!(dirty.available_permits(), 1);
    assert_eq!(recovery.available_permits(), 1);
    check_restored(&pressure, 3).await;
    check_restored(&independent, 1).await;
}
