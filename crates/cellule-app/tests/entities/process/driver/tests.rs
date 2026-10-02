use super::*;

fn sample(arrival: usize, outcome: &'static str) -> Sample {
    Sample {
        arrival,
        scheduled_us: 1_000,
        started_us: 2_000,
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
            format!("{arrival}\t1000\t2000\t3000\t4\twrite\t{outcome}\t5\t6\t7\n")
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
