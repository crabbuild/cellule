//! Real-file append diagnostics; batch throughput is not application TPS.

use super::*;
use crate::fleet::telemetry::{CellTelemetry, CellTelemetryHandle, FollowerAppendTiming};
use std::time::Instant;

#[derive(Default)]
struct Recorder(Mutex<Vec<(SessionId, FollowerAppendTiming)>>);

impl CellTelemetry for Recorder {
    fn follower_append(&self, leader: SessionId, _: u64, timing: FollowerAppendTiming) {
        self.0.lock().unwrap().push((leader, timing));
    }
}

fn encoded(leader: SessionId, sequence: u64, segment: &cellule_ltx::LocalSegment) -> Bytes {
    encode_node_frame(
        NodeFrameScope {
            leader_session: *leader.as_bytes(),
            log_epoch: 2,
            node_sequence: sequence,
            application: [3; 16],
            cell: [4; 32],
            incarnation: [5; 16],
            cell_epoch: 6,
            commit_sequence: sequence,
        },
        segment.info().clone(),
        Bytes::from(std::fs::read(segment.path()).unwrap()),
        cellule_ltx::Limits::default(),
    )
    .unwrap()
    .encoded()
    .clone()
}

fn parameter(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled disk and fresh CELLULE_FOLLOWER_BENCH_EVIDENCE required"]
async fn warm_append_diagnostic() {
    let lanes = parameter("CELLULE_FOLLOWER_BENCH_LANES", 1);
    let frames = parameter("CELLULE_FOLLOWER_BENCH_FRAMES", 1);
    let rounds = parameter("CELLULE_FOLLOWER_BENCH_ROUNDS", 512);
    assert!(matches!(lanes, 1 | 8 | 32));
    assert!(matches!(frames, 1 | 16 | 64));
    assert!((1..=1_024).contains(&rounds));
    // Bound the retained frame population and diagnostic recorder together.
    assert!(lanes * frames * (rounds + 1) <= 65_536);
    let advancing = match std::env::var("CELLULE_FOLLOWER_BENCH_COVERAGE").as_deref() {
        Ok("advancing") => true,
        Ok("zero") | Err(_) => false,
        other => panic!("undeclared coverage: {other:?}"),
    };
    let evidence = PathBuf::from(std::env::var("CELLULE_FOLLOWER_BENCH_EVIDENCE").unwrap());
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(evidence)
        .unwrap();
    let mut output = std::io::BufWriter::new(output);
    writeln!(output, "leader\tframes\tencoded_bytes\tblocking_queue_us\taccounting_wait_us\taccounting_hold_us\tlane_wait_us\tappend_us\tprune_us\tdata_sync_us\tdirectory_sync_us\trecount_us\ttotal_us\tdata_sync_calls\tdirectory_sync_calls\trecounts\tsucceeded").unwrap();
    output.flush().unwrap();
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("source.sqlite"), limits).unwrap();
    database
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE values_(payload BLOB NOT NULL)")?;
            tx.execute("INSERT INTO values_ VALUES (?1)", [vec![117_u8; 1_024]])?;
            Ok(())
        })
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    // Encode before timing. The microbenchmark measures the follower path,
    // including dispatch/resumption, without a source-file read per arrival.
    let inputs = (1..=lanes)
        .map(|lane| {
            let leader = SessionId::from_bytes([lane as u8; 16]);
            let batches = (0..=rounds)
                .map(|round| {
                    (1..=frames)
                        .map(|index| encoded(leader, (round * frames + index) as u64, segment))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            (leader, batches)
        })
        .collect::<Vec<_>>();
    let root = tempfile::TempDir::new().unwrap();
    let sink = Arc::new(Recorder::default());
    let telemetry = CellTelemetryHandle::default();
    telemetry.install(sink.clone()).unwrap();
    let store = FollowerStore::open_with_telemetry(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(2 << 30),
        telemetry,
    )
    .unwrap();
    for (leader, batches) in &inputs {
        store
            .append(*leader, 2, batches[0].clone(), 0)
            .await
            .unwrap();
    }
    sink.0.lock().unwrap().clear();
    let started = Instant::now();
    let mut jobs = tokio::task::JoinSet::new();
    for (leader, batches) in inputs {
        let store = store.clone();
        jobs.spawn(async move {
            for (round, batch) in batches.into_iter().enumerate().skip(1) {
                let covered = if advancing {
                    (round * frames) as u64
                } else {
                    0
                };
                let receipt = store.append(leader, 2, batch, covered).await.unwrap();
                assert_eq!(receipt.durable_through, ((round + 1) * frames) as u64);
            }
        });
    }
    while let Some(result) = jobs.join_next().await {
        result.unwrap();
    }
    let elapsed = started.elapsed();
    {
        let measurements = sink.0.lock().unwrap();
        assert_eq!(measurements.len(), lanes * rounds);
        for (leader, timing) in measurements.iter() {
            assert!(timing.succeeded);
            writeln!(
                output,
                "{leader:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                timing.frames,
                timing.encoded_bytes,
                timing.blocking_queue.as_micros(),
                timing.accounting_wait.as_micros(),
                timing.accounting_hold.as_micros(),
                timing.lane_wait.as_micros(),
                timing.append.as_micros(),
                timing.prune.as_micros(),
                timing.data_sync.as_micros(),
                timing.directory_sync.as_micros(),
                timing.recount.as_micros(),
                timing.total.as_micros(),
                timing.data_sync_calls,
                timing.directory_sync_calls,
                timing.recounts,
                timing.succeeded
            )
            .unwrap();
        }
    }
    output.flush().unwrap();
    let last = ((rounds + 1) * frames) as u64;
    for lane in 1..=lanes {
        let leader = SessionId::from_bytes([lane as u8; 16]);
        let sealed = store.seal(leader, 2).await.unwrap();
        assert_eq!(sealed.durable_through, last);
        assert_eq!(
            sealed.base_sequence,
            if advancing {
                last - frames as u64 + 1
            } else {
                1
            }
        );
    }
    assert_eq!(store.retained_bytes(), follower_bytes(root.path()).unwrap());
    drop(store);
    // Cold startup and paged reads validate every retained frame after timing.
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(2 << 30),
    )
    .unwrap();
    for lane in 1..=lanes {
        let leader = SessionId::from_bytes([lane as u8; 16]);
        let mut next = Some(if advancing {
            last - frames as u64 + 1
        } else {
            1
        });
        let mut count = 0;
        while let Some(first) = next {
            let page = store.read_tail_page(leader, 2, first).await.unwrap();
            for (offset, frame) in page.frames.iter().enumerate() {
                assert_eq!(*frame, encoded(leader, first + offset as u64, segment));
            }
            count += page.frames.len();
            next = page.next_sequence;
        }
        assert_eq!(
            count,
            if advancing {
                frames
            } else {
                (rounds + 1) * frames
            }
        );
    }
    database.close().unwrap();
    println!(
        "FOLLOWER_APPEND_DIAGNOSTIC lanes={lanes} frames={frames} rounds={rounds} coverage={} batches={} elapsed_us={} batches_per_second={:.3}",
        if advancing { "advancing" } else { "zero" },
        lanes * rounds,
        elapsed.as_micros(),
        (lanes * rounds) as f64 / elapsed.as_secs_f64()
    );
}
