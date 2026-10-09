use super::*;

fn sample(arrival: usize, outcome: &'static str) -> Sample {
    Sample {
        arrival,
        scheduled_us: 1_000,
        started_us: 2_000,
        generator_started_us: 2_500,
        elapsed_us: 3_000,
        entity: 4,
        write: true,
        outcome,
        sequence: 5,
        read_sequence: 6,
        count: 7,
    }
}

#[test]
fn deferred_evidence_retains_every_outcome_and_original_timing() {
    let outcomes = [
        "ok",
        "resolved",
        "scheduler_late",
        "client_full",
        "not_started",
    ];
    let samples: Vec<_> = outcomes
        .iter()
        .enumerate()
        .map(|(arrival, outcome)| sample(arrival, outcome))
        .collect();
    let mut output = Vec::new();
    write_samples(&mut output, &samples).unwrap();
    let output = String::from_utf8(output).unwrap();
    let expected = outcomes
        .iter()
        .enumerate()
        .map(|(arrival, outcome)| {
            format!("{arrival}\t1000\t2000\t2500\t3000\t4\twrite\t{outcome}\t5\t6\t7\n")
        })
        .collect::<String>();
    assert_eq!(output, expected);
}

struct FailedEvidence;

impl Write for FailedEvidence {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("evidence storage unavailable"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn deferred_evidence_failure_is_reported() {
    let error = write_samples(&mut FailedEvidence, &[sample(0, "ok")]).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(error.to_string(), "evidence storage unavailable");
}

#[test]
fn capacity_still_rejects_missed_arrivals_and_slow_drain() {
    let limit = WINDOW_SECONDS as u64 * 1_000_000 + DRAIN_GRACE_US;
    let samples = [sample(0, "ok"), sample(1, "resolved")];
    assert!(fully_served(&samples, limit, true));
    assert!(!fully_served(&samples, limit + 1, true));
    for outcome in ["scheduler_late", "client_full", "not_started", "write_only"] {
        assert!(!fully_served(&[sample(0, outcome)], 1, true));
    }
}

#[tokio::test]
async fn slow_evidence_flush_does_not_drop_scheduled_arrivals() {
    struct SlowFlush(Vec<u8>);
    impl Write for SlowFlush {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            std::thread::sleep(Duration::from_millis(750));
            Ok(())
        }
    }
    let window = Window {
        id: 0,
        prefix: "capacity",
        nodes: 1,
        shape: "uniform",
        rate_per_node: 2,
        concurrency: 8,
    };
    let mut output = SlowFlush(Vec::new());
    let samples = collect_arrivals(
        &window,
        3,
        Instant::now(),
        12,
        |mut sample, _, _| async move {
            sample.outcome = "ok";
            sample
        },
    )
    .await;
    write_samples(&mut output, &samples).unwrap();
    assert_eq!(samples.len(), 3);
    assert!(
        samples.iter().all(|sample| sample.outcome == "ok"),
        "evidence I/O under-offered the workload: {:?}",
        samples
            .iter()
            .map(|s| (s.arrival, s.started_us, s.outcome))
            .collect::<Vec<_>>()
    );
    assert_eq!(String::from_utf8(output.0).unwrap().lines().count(), 3);
}

#[tokio::test]
async fn missed_arrivals_remain_visible_without_dispatch() {
    let window = Window {
        id: 0,
        prefix: "capacity",
        nodes: 1,
        shape: "uniform",
        rate_per_node: 2,
        concurrency: 8,
    };
    let mut output = Vec::new();
    let samples = collect_arrivals(
        &window,
        2,
        Instant::now() - Duration::from_secs(2),
        12,
        |_, _, _| async { panic!("missed arrival was dispatched") },
    )
    .await;
    write_samples(&mut output, &samples).unwrap();
    assert!(
        samples
            .iter()
            .all(|s| s.outcome == "scheduler_late" && s.elapsed_us == 0)
    );
    assert_eq!(String::from_utf8(output).unwrap().lines().count(), 2);
}
