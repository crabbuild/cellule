# PR67 Fleet root-delay measurement

**The root-delay experiment was rejected and reverted.** One matched
five-minute window completed 350.62 writes/s versus 403.73 for the retained
implementation, a 13.2% observed regression. Successful scheduled p99 improved,
but request p99 worsened, root density did not improve, and both Cellule runs
failed audit and drain. This is not a performance-parity delivery or a
sustainable-capacity result.

## Measured change and rollback

Experiment `bb2f08986cb16ff0c0d477226767670f3fcd0a4f` deferred an already
follower-proven Cell root outside a saturated shared preparation lane. It
requested preparation after 32 physical captures, five seconds from the oldest
capture, or a fallback/backlog/drain/fence signal. The existing proof gate,
capture limits and publication retry deadline remained unchanged. Ordinary
bundle-based application ACKs remained disabled.

Revert `ddb030c776e5d61a75b76d54e8610120be781df0` restores exactly the complete
`e900bfae21fba2d073316707b18f4a80d292d7d2` tree. That delivery's production source
matches measured baseline `b1856728984781cee5398f8d7185fb88fde23993`; their
differences are documentation only. The additional report in this delivery does
not change the measured application behavior. The rejected experiment and its
failed evidence remain available in history and outside Git.

One overloaded pair cannot establish repeatable causal attribution. Removing
an unverified delay avoids adding reader lag and lifecycle complexity without
measured root-density or throughput benefit.

## Matched workload

Runs completed on 2026-10-08 UTC, in baseline/candidate/celld order. The baseline
reuses the verified `b185672` release binary; the experiment is a fresh release
build. celld v0.6.1 is pinned to
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. Source, image, fixture and binary
identities are retained; clients and auditors are byte-identical.

Each arm used 1,000 uniform Cells, 96-byte values, SQL INSERT plus SELECT and a
two-hour durable request/result ledger, 128 clients, a 128-offer queue,
30-second warmup and a 300-second window offering 15,000 writes/s. There was
**one repetition** and no separate read load. Bucket, read-only and mixed
performance were not rerun. This workload does not validate the separate
laptop's reported 15K write TPS or the 2,000-Cell serving-node target.

All roles shared one ARM64 Linux VM with 8 CPUs and nominal 16 GiB RAM. Nodes
retained 4 GiB tmpfs ceilings and Cellule's 64 MiB retained-memory/1 GiB
managed-disk budgets. RustFS retained 2 CPUs and an 8 GiB memory ceiling in the
separate provider-headroom diagnostic. The original qualification profiles and
deadlines were unchanged; their prior failed 2 GiB provider run remains in the
[earlier report](pr67-write-measurement-4957985.md). Another VM was running on the
physical host. Dedicated serving-node capacity and physical-device durability
are unqualified.

## Actual completed windows

TPS counts successful logical writes completed inside the 300-second window.
Successful percentiles use exact nearest-rank client-journal values, including
late successful responses. Scheduled latency includes waiting from the offered
time; request latency starts when the client sends the request. Fast errors and
queue drops are excluded from these successful percentiles.

| Metric | Retained Cellule baseline | Rejected experiment | celld |
| --- | ---: | ---: | ---: |
| Completed writes/s | 403.73 | 350.62 | 1,107.42 |
| Successful writes inside window | 121,120 | 105,187 | 332,226 |
| Successful scheduled p50 ms | 107.15 | 111.80 | 67.01 |
| Successful scheduled p95 ms | 427.97 | 359.75 | 153.97 |
| Successful scheduled p99 ms | 1,030.44 | 767.49 | 562.32 |
| Successful request p99 ms | 499.85 | 600.02 | 255.69 |
| Window errors | 3,104,546 | 3,300,026 | 1,354,855 |
| Queue drops | 1,274,320 | 1,094,700 | 2,812,919 |
| Late successful responses | 14 | 87 | 0 |

The experiment's scheduled p99 improved 25.5%, while request p99 worsened
20.0% and median scheduled latency worsened 4.3%. It achieved 31.7% of celld's
observed completion rate. **All three rates are failing-run observations.**
The previous window of the unchanged baseline measured 378.88 TPS versus 403.73
in this window;
the [previous comparison](pr67-sparse-root-coverage-measurement.md) remains
separate. No repeatable gain, healthy celld capacity or parity claim follows.

## Cost and publication pressure

| Window metric | Baseline | Experiment |
| --- | ---: | ---: |
| Selected Cell roots | 54,930 | 48,922 |
| Materialized commits | 111,265 | 96,322 |
| Materialized commits per root | 2.026 | 1.969 |
| All-provider successful PUTs per completed command | 1.971 | 1.965 |
| Node-authority-family successful PUTs | 796 | 910 |
| End oldest unpublished age ms | 189,537 | 203,144 |
| Peak sampled retained-capture bytes | 69,813,120 | 71,136,510 |

These storage API ratios include all serving nodes, exclude trailing
publication and SDK-internal retries, and have outstanding work at the window
boundaries. They are not total lifecycle cost. Node-authority-family counts
also include coordination. The delay did not deliver denser materialized
roots or a substantial reduction in PUTs per command.

Both Cellule runs remained Fleet-active and nonrotating in all six samples.
Both fail the late-window publication-age slope gate; the experiment also
fails the debt and pending-publication slopes. Its final issued/follower-proven
frontiers were 129,234/129,174 versus tiered 67,430. This gap includes
already-rooted sparse completions and is **not a count of missing or lost
writes**.

## Audit, drain and local capacity failures

| Case | Warm ACK read/exact-retry audit | Drain and cold recovery |
| --- | --- | --- |
| Baseline | 143,024 ACKs checked; 141,913 retries passed; 1,111 errors | Owner missed 120-second deadline; forced cleanup; cold not reached |
| Experiment | 124,293 ACKs checked; 123,502 retries passed; 791 errors | Owner missed 120-second deadline; forced cleanup; cold not reached |
| celld | 476,269 ACKs checked; no retries passed; 476,269 errors | Cleanup exited zero; cold not reached after audit failure |

Cellule's retained audit examples are HTTP 503; celld's are HTTP 500. They do
not classify every failure or establish data loss. Unlike its previous run,
celld's three nodes exited zero during cleanup and had false OOM flags.
Its owner log repeatedly reports WAL capture failures with `database or disk
is full` and `No space left on device`. Local state used the configured 4 GiB
tmpfs. A direct post-window filesystem read failed because the owner had
already stopped; no exact occupancy snapshot was captured. This invalidates a
healthy-capacity interpretation without establishing the complete failure
chain. Live provider samples had byte/inode headroom; required post-audit and
cold-lifecycle snapshots remain missing because audits failed.

## Verification and remaining architecture

The rejected frozen source passed all 11 contributor checks, including 1,942
top-level workspace cases and doctests, one nested subprocess, 38 ignored and
60 local LTX cases. Its actor regression verified Fleet ACKs, owner reads,
exact retries, drain wakeup and all 12 outcomes after canonical cold restore;
it failed against the prior source as expected. That fixture simulates occupied
preparation permits and does not qualify distributed overload or hardware
durability. The revert restores the previously verified production tree.

Raw evidence stays outside Git under `cellule-write-perf-8ad1`: the plan,
immutable builds, every client/ACK journal, failed audits, logs and retained
Docker volumes. Normalized results are
`fleet-root-batch-bb2f089-results.json`; independent streaming verification is
`fleet-root-batch-bb2f089-evidence-index.json`. Verification reconciles offered,
attempted, failed and successful writes, exact successful percentiles,
acknowledgement manifests, measured binaries and 535 artifact hashes. The
dedicated benchmark VM is stopped; the user's default VM and Docker context
remain unchanged.

The remaining work is verified shared selection in actor command/read/retry
paths with exact capture release, admitted asynchronous root materialization,
complete issued-range drain/recovery and safe cross-Cell collection. Per-Cell
delay is not a substitute for that architecture. The
[design's exit gates](../crates/cellule-runtime/docs/write-performance-design.md)
still require three matched repetitions, zero errors/drops, stable publication,
all-ACK cold recovery, complete drain and read guardrails.
