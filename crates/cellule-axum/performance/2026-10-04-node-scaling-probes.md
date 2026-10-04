# Node scaling diagnostic probes

Target: 2,000 independent resident SQLite Cells, 10,000 durable writes/sec,
and 50,000 owner-ordered reads/sec on an 8-vCPU, 16-GiB node. **Not established.**
These probes isolate provider capacity and activation limits; they are not
framework optimization results or maximum-throughput measurements.

Framework revision: `80c4fd99c10fc7e91ab34c628ea61fcd488e0cea`. The fixture
adds bounded offered load, exact receipt validation, streamed journals,
resource sampling and full acknowledged-write cold replay. Frozen source and
binary hashes, configurations, critical phases and journal hashes are in the
[dataset](2026-10-04-node-scaling-probes.json).

## Provider CPU probe

One 120-second run per allocation, 5-second warmup, 16 Cells, 8 SQL workers,
64 clients, queue capacity 256, offered 100 writes/sec and 50 reads/sec.
Only RustFS's CPU allowance changes, from two to four cores. Both runs use
the same server/driver binaries and fresh object prefixes. The historical
50-read/sec workload is a diagnostic load, not the updated target.

| Metric | RustFS 2 cores | RustFS 4 cores |
| --- | ---: | ---: |
| Write successes/sec within window | 96.800 | 99.983 |
| Read successes/sec within window | 47.925 | 50.000 |
| Write / read queue drops | 383 / 249 | 0 / 0 |
| Issued request errors | 0 | 0 |
| Write HTTP p50 / p99, ms | 67.7 / 1552.6 | 15.9 / 1078.0 |
| Write scheduled p99 including queue, ms | 3961.7 | 1394.6 |
| Read HTTP p50 / p99, ms | 8.9 / 770.1 | 0.5 / 522.7 |
| Owner mean CPU cores including seed/warmup/drain | 0.415 | 0.471 |
| Owner peak RSS, MiB | 102.91 | 104.80 |
| Owner peak descriptors | 265 | 266 |
| Root preparation work mean, ms | 47.261 | 23.868 |
| Authority phase mean, ms | 28.655 | 13.633 |
| SQL worker phase mean, ms | 5.563 | 5.526 |
| Cold verified writes / reads including setup and warmup | 12083 / 5966 | 12516 / 6250 |

Provider throttled-time deltas across each whole point, including recovery,
are 100.016 seconds and 0.022 seconds. Their populations differ from the HTTP
measurement windows. Phase means include setup and warmup and have different
counts; do not add them into a synthetic request latency.

**Finding:** provider CPU throttling contributes substantially to queueing and
publication latency. Increasing provider resources removes drops at this
offered rate. Persistent tail latency remains. One pair has no replication or
confidence interval; it does not establish the remaining throughput ceiling.

## Density admission

The first 2,000-Cell probe fails before HTTP startup with `Too many open files`;
the container's OS soft limit is 1,024. Raising only that limit to 32,768 passes
that obstruction, then fails with `Capacity("node pressure")` after 94.535
seconds. Neither failed run establishes steady-state capacity or activation
latency for 2,000 Cells.

The second obstruction is fixture sizing: the worker pool's default native
ceiling equals its configured writer reservations. Dense residency approaches
the pressure threshold even with ample physical memory. The retry uses
the existing `with_native_memory_limit` API with a 512-MiB ceiling. Pressure
thresholds, retained-cut limits, and disk limits stay unchanged. An admission
ceiling is not allocated memory or measured RSS.

The retry activates all 2,000 Cells in 251,102 ms in the serial activation loop.
Eight SQL workers serve them. The load window reaches 602.16 MiB peak owner RSS
and 16,226 descriptors. At the same offered 100 writes/sec and 50 reads/sec,
the 120-second measurement records:

| Metric | 2,000 Cells |
| --- | ---: |
| Write / read successes/sec within window | 42.825 / 10.225 |
| Write / read queue drops | 6597 / 4720 |
| Write / read issued errors | 1 / 0 |
| Write HTTP p50 / p99, ms | 1082.4 / 7832.8 |
| Write scheduled p99 | Beyond the histogram; null, maximum 13,796.4 ms |
| Read HTTP p50 / p99, ms | 0.7 / 751.0 |
| Root admission mean, ms | 671.221 |
| Root preparation work mean, ms | 120.443 |
| Authority phase mean, ms | 85.220 |
| SQL worker phase mean, ms | 21.754 |

Phase populations include bootstrap, seed writes, warmup and measurement; this
is a bottleneck indicator, not an additive latency decomposition. The one
failure is HTTP 503 `unavailable` for the retained original request to Cell
shard 1950. Its internal cause is not established by the HTTP response. All
2,000 Cells release to Idle on failure shutdown, but the coordinator stops at
the failed load gate and **does not run cold audit**. No durable recovery or
qualified throughput claim is made for this failed run.

**Finding:** independent SQLite residency is inexpensive for this tiny fixture,
but object publication admission and provider work dominate the measured write
path at density. More cells do not remove those shared limits. The serial bulk
activation average is 125.6 ms/Cell; it is not an individual creation-latency
measurement. Constant small RSS cannot be extrapolated to large working sets.

## Read-only offered-rate smoke

A separate 10-second diagnostic offers 50,000 reads/sec, no measured writes,
16 seeded Cells, 8 SQL workers, 256 clients, queue capacity 1,024 and a 2-second
warmup. This validates the read-only driver path and supplies a profiling
workload; it is not a steady-state capacity test.

It records 20,304.2 successful reads/sec within the window, 295,527 queue drops,
14 unissued offers and 139 HTTP 503 responses. HTTP p50/p95/p99 are
8.6/36.7/61.9 ms; scheduled p99 including driver queue is 154.8 ms. Successful
response-body throughput within the window is 3.724 MiB/sec, excluding HTTP
headers, request bytes, warmup and drained completions.

Owner CPU averages 1.426 cores; owner plus driver average 2.506 cores across the
sampled setup/warmup/load/drain population, with no container CPU throttling.
The runtime observes 9.014 ms mean actor queue wait, 0.573 ms mean SQL worker
round trip, and 0.010 ms mean primitive query time. These populations include
warmup and seed verification. The data points to queueing/dispatch overhead;
profiling and larger Cell counts must distinguish per-Cell serialization,
shared actor work and worker admission before changing the framework.

The load gate fails, so cold audit is not run. Every Cell releases to Idle on
failure shutdown. The JSON retains the failed metrics and raw journal hashes;
no supported read-throughput or latency claim follows from this smoke.

## Environment and next probes

Linux ARM64, Rust 1.97.1, shared Docker VM with 8 vCPUs and 16,732,606,464
bytes RAM. The owner and load driver share an 8-core, 12-GiB container; RustFS
shares the VM and has a separate 2-GiB limit. This is not isolated target
hardware. SQL HTTP writes also perform a receipt-bound read before responding.
The fixture uses object durability proofs.

Next gates are successful 2,000-Cell load and recovery, separate read
capacity, a real follower-backed HTTP assembly, profiling SQL capture versus
publication, and stable object/log backlogs under the combined target. Follow
the sustained and failure qualification in [node capacity](node-capacity.md).
Cells remain independently ordered and recoverable; transport batching must
preserve exact Cell identity, sequence, fencing and durable-response proofs.
