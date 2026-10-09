# PR 67: bounded closure admission and current performance

**Joined drain and cold recovery now complete, but throughput parity remains
unmet.** Revision `d12eac881523cf00fcfdee9662e82e10c9627991` completes
339.30 Fleet writes/s and 16,410.03 read-only queries/s in separate short
diagnostics. Both points are lower than the previous observations; this change
delivers a closure scheduling fix, not a demonstrated throughput improvement.
The first read setup fails with HTTP 503 and remains an availability failure.
PR #67 remains a draft.

## Reproduced shutdown failure and fix

The previous [2,000-Cell comparison](pr67-base-cohort-measurement.md) fails
the unchanged 120-second joined-drain deadline, including in read-only load.
Sampled mutex timing in a separate instrumented read-only reproduction at
`60c30936096f5e0e1b93dba11a2d6fe842ccc9bb` establishes the scheduling failure:

| Observation | Result |
| --- | ---: |
| Heartbeat wait on the example provider's shared FIFO mutex | 20,001 ms; lease expired before renewal |
| Largest sampled closure queue wait | 27,349 ms |
| Sampled completed closure mutex holds | 9–44 ms |
| Instrumented read-only drain | Fails the original 120-second deadline; cold audit not reached |

Population-wide closure places too many callbacks ahead of heartbeat renewal.
The samples do not rule out an unsampled slow individual operation. Runtime
shutdown already joins before the example stops heartbeat renewal.

`NodeDurability::close_bundle_cell` now admits at most eight provider closure
callbacks **after freezing issuance and joining the complete issued producer
prefix**. The lease is checked again before and after the original callback.
Fencing wakes callers waiting outside the callback cohort; cancellation returns
admission without reopening frozen issuance. No lease, drain deadline, capacity,
durability or persisted format is widened.

The regression uses the real closure entry point for 2,000 bound controls and
a FIFO provider mutex shared with heartbeat. Two checks fail in each of three
before runs; all three pass in each of three after runs. They verify heartbeat
gets a turn, every exact scope closes, fenced queued callbacks do not enter,
and cancelled admission can be retried. Separate bundle tests retain real
selected-prefix, prior-Fleet-ACK, checkpoint and cold-recovery coverage. The
scheduling test's synthetic roots establish no physical durability claim.

## Workload and identities

| Dimension | This slice |
| --- | --- |
| Measured production revision | `d12eac881523cf00fcfdee9662e82e10c9627991`; diagnostic timing instrumentation removed |
| Population | 2,000 uniformly active Cells; all successful timed cases independently reconcile the full population |
| Command | SQL INSERT/SELECT, 96-byte value, two-hour durable request/result ledger |
| Durability | Fleet, one owner and two followers; local state and follower logs on tmpfs |
| Client and timing | 128 clients/queue slots; 30-second warmup, 60-second window; write-only and read-only separately |
| Offered load | 2,000 writes/s or 20,000 reads/s |
| Cellule limits | Original 20-MiB producer admission, 64-MiB retained work and 1-GiB managed disk |
| Previous celld reference | `f2bf648663a610eefde71f3547ad61e9b896b1f0`; no fresh celld point in this slice |

Every attempt uses a fresh namespace and provider volume. Candidate cases use
the same immutable corrected runner, clients, auditor, fixture settings and
images as the previous comparison. Builds and contributor suites finish before
candidate timed windows. Both managed write paths already use SQLite WAL NORMAL.

The shared ARM64 Docker VM has **8 CPUs and 8 GiB total RAM**. Serving containers
have 8-CPU/16-GiB ceilings, the provider 2-CPU/8-GiB and the client 4-CPU/4-GiB;
their ceilings exceed aggregate capacity. Internal resource policies remain
asymmetric. These are workload diagnostics, not dedicated standard-node capacity,
KV-overwrite or physical-media durability qualification.

## Actual write and read results

Latency covers successful responses, including trailing completions. Errors
and dropped offers remain failures. Previous points below are historical
references, not a newly paired control for closure admission.

| Workload / revision | In-window successes/s | Scheduled p99 ms | Request p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet write, previous Cellule | 372.87 | 932.77 | 531.98 | 49,986 | 47,642 |
| Fleet write, current Cellule | 339.30 | 1,824.77 | 657.85 | 52,299 | 47,216 |
| Fleet write, previous celld | 1,999.80 | 13.38 | 12.22 | 0 | 0 |
| Read-only, previous Cellule | 17,456.38 | 28.79 | 14.67 | 0 | 152,566 |
| Read-only, current Cellule retry | 16,410.03 | 30.95 | 15.42 | 0 | 215,374 |
| Read-only, previous celld | 19,973.25 | 3.43 | 2.19 | 0 | 1,573 |

Current write completes 20,358 responses in-window and 127 afterward. Warmup
has 15,750 errors and 30,444 dropped offers. Successful scheduled p50/p95 are
452.24/676.88 ms; request p50/p95 are 231.77/367.11 ms. The observed rate is
9.00% lower than the previous point; this unpaired short observation establishes
neither an improvement nor a repeatable regression caused by the closure change.

The read retry completes 984,602 responses in-window and 24 afterward. Warmup
has zero read errors and 417,074 drops. Successful scheduled p50/p95 are
2.14/22.34 ms; request p50/p95 are 0.64/11.28 ms. Its rate is 5.99% below the
previous point. No repeatable read gain or regression guardrail is established.

| Current case | Complete ACK cohort | Warm reads / original retries | Joined drain | Cold reads / original retries |
| --- | ---: | --- | ---: | --- |
| Fleet write | 36,292 | All pass | 52.63 s | All pass |
| Read-only retry | 2,001 | All pass | 41.72 s | All pass |

Cohorts include seeds, contract checks, warmup and trailing successful writes.
Every ACK mutation and original retry result passes both audits, with zero
errors or changed incarnations. All 2,000 Cells have timed successes. Journal
counts, receipt scope and expected outputs reconcile independently. These two
cases complete lifecycle checks; their performance reports still fail.

The first read-only attempt aborts **during initialization**, after 1,783
successful seed responses, on HTTP 503 (`Cell is temporarily unavailable`).
It produces no timed TPS and reaches no cold audit. The owner exits normally
without OOM; the cause remains unproven. The unchanged fresh-namespace retry
does not erase this availability failure. The runner previously masked the
driver's 503 with a missing-summary error; it now retains the original exit
and source output plus copied partial journals. A replay at the actual driver
call seam fails before and passes after, including preservation of nonzero
drivers that have a real measurement summary. Measured cases retain the earlier
immutable runner; this diagnostic fix changes none of their results.

## Why write throughput is still low

In the current write window, all eight submission phase counts match 20,422
successful assignments and their time partition residual is zero:

| Submission phase | Mean ms |
| --- | ---: |
| Local load and validation | 0.11691 |
| Global ordered-lock wait | 181.20062 |
| Publication capacity, with lock held | 1.68201 |
| Ticket assignment and enqueue | 0.00700 |
| Complete submission | 183.00707 |

Ordered-lock waiting is **99.01% of submission time**. The canonical
[`assign_capture`](../crates/cellule-runtime/src/node/log_shipper/mod.rs)
still reserves publication capacity under the node-wide ordering mutex before
frames enter follower shipping. Background selection and materialization can
therefore throttle otherwise independent Fleet writers. Mean SQL worker,
capture and follower-proof timers are 0.20, 0.15 and 11.09 ms in their respective
overlapping cohorts; they are not additive to this submission partition.

Windowed GET/range attempts are 7.09 per completed write; successful PUTs are
0.495 and materialized commands per root are 8.43. These include background work
and exclude SDK retries, so they are not exact per-command costs. Between window
boundary samples, pending publications grow 1,444→2,493 and retained bytes
50,290,456→64,769,848 against a 67,108,864-byte limit. Unpublished node-log bytes
grow 10,650,379→29,629,212. Two endpoints do not establish a sustained debt slope.

Celld's [Fleet loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4514)
separates follower progress from bucket publication and pipelines ordered
rounds. Its [follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160)
groups delivered append frames before durable append. Cellule awaits each
shipping batch. The measured dominant gap remains publication-coupled admission,
followed by publication cost and the separate shipping pipeline gap.

Next work is bounded overlap of authenticated catalog/history reads, lower
root/checkpoint cost, recoverably bounded native/publication separation and
ordered shipping. Initial availability, mixed load, Bucket, sustained debt,
safe collection, full fault lifecycle and three paired five-minute repetitions
remain required. Larger queues and a passing drain alone cannot qualify parity.

## Verification and evidence

All contributor routes pass in a frozen source snapshot: 1,978 workspace tests,
60 local LTX tests, Rust 1.97/1.99 Clippy with warnings denied, format, all-feature
checks, docs and boundary/layout/document/SQL-peer gates. The 38 ignored tests
retain their documented environment requirements. All 1,311 Rust/Cargo files
match that verified snapshot and the measured Linux build. The later changes
are this report, status text and the source-preserving Python diagnostic.

Raw source, build identity, binaries, timing samples, failed setup, regression
replays, journals, metrics, audits and the rehash inventory remain outside Git
under `/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/closure-admission-20261009-*`.
The external inventory rehashes the preceding frozen comparison as well.

[Implementation](bundle-coverage-implementation.md),
[capacity contract](../crates/cellule-runtime/docs/write-performance-design.md).
