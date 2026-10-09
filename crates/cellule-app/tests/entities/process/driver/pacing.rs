//! Absolute pacing and standalone generator evidence.

use super::*;

// Tokio timers are millisecond-granularity. At sub-millisecond arrival spacing
// they can invalidate the generator before the server reaches saturation.
// Linux Docker on the diagnostic VZ VM also oversleeps by about 10 ms. A
// 12-ms guard leaves deadline precision to this thread, charged to the same
// cgroup CPU budget. A dedicated thread uses absolute deadlines and a bounded handoff. A full
// handoff delays dispatch; every intended arrival still produces a row and
// remains subject to the same scheduler-late gate.
pub(in crate::entities::process) fn paced_arrivals(
    started: Instant,
    planned: usize,
    rate: usize,
    capacity: usize,
) -> (
    tokio::sync::mpsc::Receiver<(usize, u64)>,
    tokio::task::JoinHandle<()>,
) {
    arrivals_with_guard(started, planned, rate, capacity, Duration::from_millis(12))
}

fn arrivals_with_guard(
    started: Instant,
    planned: usize,
    rate: usize,
    capacity: usize,
    spin_budget: Duration,
) -> (
    tokio::sync::mpsc::Receiver<(usize, u64)>,
    tokio::task::JoinHandle<()>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    let pacer = tokio::task::spawn_blocking(move || {
        for arrival in 0..planned {
            let scheduled_us = (arrival as u64 * 1_000_000) / rate as u64;
            let deadline = started + Duration::from_micros(scheduled_us);
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                if remaining > spin_budget {
                    std::thread::sleep(remaining - spin_budget);
                } else {
                    std::hint::spin_loop();
                }
            }
            let generated_us = started.elapsed().as_micros() as u64;
            if sender.blocking_send((arrival, generated_us)).is_err() {
                break;
            }
        }
    });
    (receiver, pacer)
}

#[tokio::test]
async fn bounded_pacer_retains_all_intended_arrivals() {
    let started = Instant::now() + Duration::from_millis(20);
    let (mut arrivals, pacer) = paced_arrivals(started, 8, 500, 2);
    for expected in 0..8 {
        let (arrival, generated_us) = arrivals.recv().await.unwrap();
        assert_eq!(arrival, expected);
        assert!(generated_us >= arrival as u64 * 2_000);
    }
    assert!(arrivals.recv().await.is_none());
    pacer.await.unwrap();
}

fn cpu_us() -> u64 {
    std::fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .unwrap()
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == "usage_usec").then(|| value.parse().unwrap())
        })
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "frozen Linux Docker generator calibration; no server-capacity claim"]
async fn pacer_timing_diagnostic() {
    const SECONDS: usize = 10;
    const GUARDS_US: [u64; 4] = [50, 2000, 500, 12_000];
    let root = env::var("CELLULE_PACER_BENCH_EVIDENCE").unwrap();
    let root = Path::new(&root);
    let mut summary = BufWriter::new(File::create(root.join("windows.tsv")).unwrap());
    writeln!(
        summary,
        "round\tguard_us\trate\tseconds\tcount\telapsed_us\tcpu_us"
    )
    .unwrap();
    for round in 0..5 {
        for rate in [30, 60_000] {
            for offset in 0..GUARDS_US.len() {
                let guard_us = GUARDS_US[(round + offset) % GUARDS_US.len()];
                let planned = rate * SECONDS;
                let start = Instant::now() + Duration::from_millis(20);
                let before_cpu = cpu_us();
                let (mut receiver, pacer) = arrivals_with_guard(
                    start,
                    planned,
                    rate,
                    1024,
                    Duration::from_micros(guard_us),
                );
                let mut samples = Vec::with_capacity(planned);
                while let Some((arrival, generated_us)) = receiver.recv().await {
                    let received_us = start.elapsed().as_micros() as u64;
                    assert_eq!(arrival, samples.len());
                    assert!(generated_us >= arrival as u64 * 1_000_000 / rate as u64);
                    assert!(received_us >= generated_us);
                    samples.push((arrival as u64, generated_us, received_us));
                }
                pacer.await.unwrap();
                tokio::time::sleep_until((start + Duration::from_secs(SECONDS as u64)).into())
                    .await;
                let elapsed_us = start.elapsed().as_micros() as u64;
                let used_cpu = cpu_us() - before_cpu;
                assert_eq!(samples.len(), planned);
                // Export after timing. Fixed-width, big-endian rows retain
                // every generated and received timestamp without timed I/O.
                let name = format!("p{round}-r{rate}-g{guard_us}.bin");
                let mut output = BufWriter::new(File::create(root.join(name)).unwrap());
                for (arrival, generated, received) in samples {
                    output.write_all(&arrival.to_be_bytes()).unwrap();
                    output.write_all(&generated.to_be_bytes()).unwrap();
                    output.write_all(&received.to_be_bytes()).unwrap();
                }
                output.flush().unwrap();
                writeln!(
                    summary,
                    "{round}\t{guard_us}\t{rate}\t{SECONDS}\t{planned}\t{elapsed_us}\t{used_cpu}"
                )
                .unwrap();
                summary.flush().unwrap();
                println!(
                    "PACER_DIAGNOSTIC round={round} guard_us={guard_us} rate={rate} count={planned} elapsed_us={elapsed_us}"
                );
            }
        }
    }
}
