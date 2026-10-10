# PR67 sparse-root coverage measurement

For the subsequent root-delay experiment and its rollback, see the
[new matched measurement](pr67-fleet-root-delay-measurement.md).

**Performance parity is not delivered.** The live-path change observed 378.88
successful writes/s versus 282.43 before it in one matched five-minute run.
Successful-write p99 improved, but median latency worsened, overload errors
increased, publication debt grew, and audit/drain failed. These rates are
overloaded completion observations, not sustainable capacities.

## Change and safety evidence

Production source `b1856728984781cee5398f8d7185fb88fde23993` avoids a node
authority GET/CAS when already-selected exact Cell roots prove sparse native
sequences without advancing the contiguous reclamation frontier. Each actual
frontier advance still persists authority coverage before local confirmation
and follower reclamation. Sparse proof cannot bridge an unpublished gap or
permit rotation; the original node lease must remain fresh. Failed advancing
updates retain their original staged tickets for retry and joined shutdown.

The regression failed before the change: out-of-order roots called authority at
frontiers 0, 2 and 6 instead of only 2 and 6. A second test proves 63 sparse
roots without node I/O, rejects rotation across the missing first sequence, and
retains the gap-closing batch through authority failure before its successful
retry. All 16 adjacent durability tests pass. Ordinary actor bundle ACKs remain
disabled; this measures the existing per-Cell root path.

## Matched workload

Measurements ran on 2026-10-08 UTC. The baseline is the original verified
release binary of `49579857ac4e5b9015ecb88b0a1a78e283154460`; the candidate is a
fresh release build of `b185672`; celld v0.6.1 is pinned to
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. Clients/auditors are byte-identical;
fixture, image, binary and source hashes are retained. The comparison verified
one Docker host and loaded runner.

Each arm used 1,000 uniformly selected Cells, 96-byte values, SQL INSERT plus
SELECT with a two-hour durable request/result ledger, 128 clients, a 128-offer
queue, 30-second warmup and a 300-second window offering 15,000 writes/s. There
was **one repetition**, with no separate read load. Bucket, read-only and mixed
performance were not rerun for this change.

Owner, two followers, client and provider shared one ARM64 Linux VM with 8 CPUs
and nominal 16 GiB RAM. Nodes retained 4 GiB tmpfs ceilings and Cellule's
64 MiB retained-memory/1 GiB managed-disk budgets. This is the separate
provider-headroom diagnostic: RustFS has 2 CPUs and an 8 GiB memory ceiling in
every arm. The prior original 2 GiB provider OOM failure remains in the
[earlier evidence](pr67-write-measurement-4957985.md). Qualification profiles and
deadlines remain unchanged. Another VM was running on the physical host;
serving-node isolation and physical-device durability are unqualified.

## Actual completed windows

TPS counts successful logical writes completed inside the 300-second window.
Percentiles below use exact nearest-rank journal values for successful measured
offers, including late responses. Fast error responses are excluded from these
percentiles; queue drops are excluded from both TPS and latency samples.

| Metric | Baseline Cellule | Candidate Cellule | celld |
| --- | ---: | ---: | ---: |
| Completed writes/s | 282.43 | 378.88 | 1,258.30 |
| Successful writes inside window | 84,730 | 113,665 | 377,489 |
| Successful-write scheduled p50 ms | 89.40 | 111.77 | 85.32 |
| Successful-write scheduled p99 ms | 6,857.58 | 489.90 | 636.43 |
| Window errors | 1,913,111 | 3,240,146 | 1,085,585 |
| Queue drops | 2,501,905 | 1,146,156 | 3,036,926 |
| Late successful responses | 254 | 33 | 0 |

Observed candidate throughput increased 34.1%; median latency worsened 25.0%.
Successful-write p99 still misses the 50 ms Fleet goal by about 9.8 times.
The unchanged baseline previously measured 344.19 TPS and now measures 282.43;
one pair cannot establish a repeatable causal improvement. celld's owner and
one follower exited with code 3 **during the window**, with Docker OOM flags
false. Its TPS is a failing-run observation, not a healthy capacity reference.
The exit cause is unresolved. Candidate TPS is 30.1% of that observed celld rate.

## Audit, drain and publication failures

| Case | Warm ACK read/exact-retry audit | Drain and cold recovery |
| --- | --- | --- |
| Baseline | 104,129 ACKs checked; 103,674 retries passed; 455 errors | Owner missed 120-second deadline; forced cleanup; cold not reached |
| Candidate | 126,856 ACKs checked; 126,327 retries passed; 529 errors | Owner missed 120-second deadline; forced cleanup; cold not reached |
| celld | 460,849 ACKs checked; no retries passed; 460,849 errors | Owner/follower exited during traffic; cold not reached |

Cellule's four retained audit examples per case are HTTP 503; celld's are
transport failures. They do not classify every error or establish data loss.
The baseline rotated and disabled Fleet shipping during the window, so its
configured-Fleet result includes fallback. The candidate remained active in
all six samples, but its tiered frontier stalled: issued/follower-proven
131,956 versus tiered 61,296 at the end. This difference includes already-rooted
sparse completions; it is **not a count of missing or lost writes**.

Candidate retained captures peaked at 69,107,248 bytes and ended at 40,185,871;
oldest publication age ended at 156,737 ms. The late-segment debt/age slopes
fail stability. Required post-audit provider filesystem and cold-lifecycle
snapshots are absent because audits failed; live samples do not substitute for
that evidence. All three cases fail qualification.

## Measured I/O and next gap

Node-authority-family window GET attempts fell from 5,795 to 975 and successful
PUTs from 5,885 to 1,065 (about 82% fewer PUTs). These totals include coordination
and all serving nodes. All-provider successful PUTs per completed command fell
from 2.088 to 1.920. Window ratios exclude trailing drain and SDK-internal
retries and are affected by outstanding work; they do not establish lifecycle
cost. Materialized commands per selected Cell root remain about 2.10, far from
the conditional 215-command checkpoint spacing.

The next architectural work remains shared verified selection in actor
command/read/retry paths with exact capture release, admitted asynchronous root
materialization, complete issued-range drain/recovery and safe cross-Cell
collection. The [design and exit gates](../crates/cellule-runtime/docs/write-performance-design.md)
still require three matched repetitions, zero errors/drops, stable debt,
all-ACK cold recovery, complete drain and read guardrails before a parity claim.

## Verification and retained evidence

The frozen measured source passed all 11 contributor verification routes:
format, all-feature/all-target check, workspace tests, local LTX, warnings-denied
Clippy/API docs, boundaries/layout, document fences/links and SQL/peer contracts.
There were 1,941 top-level workspace cases including doctests, 38 ignored, plus
one nested subprocess success; 60 local LTX cases passed. Environment-dependent
ignored tests remain outside this result.

Raw evidence stays outside Git under `cellule-write-perf-8ad1`. Normalized
results are `sparse-root-b185672-results.json`; the paired report is
`sparse-root-b185672-fleet15000-comparison.json`. Independent streaming
verification reconciled offer/attempt/error/success counts and exact successful
p99 against raw journals and verified 476 artifact hashes in
`sparse-root-b185672-evidence-index.json`. Build identities, failed-before logs,
isolated checks and retained Docker volumes remain available. The standard
[performance runbook](../scripts/perf/README.md) describes the build/run/report
workflow; the diagnostic runner and plan are retained with this evidence.
