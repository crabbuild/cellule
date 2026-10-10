# PR67 ordered publication feed measurement

**No write-throughput improvement was demonstrated.** The current source,
`d4bec27351310d1a96ca0579d2115edfa15969af`, completed 369.85 writes/s against
443.41 for the retained `b1856728984781cee5398f8d7185fb88fde23993` baseline.
That is a 16.6% observed decrease in one matched five-minute Fleet pair.
The candidate also failed its warm ACK audit and owner drain. These are
overloaded-run observations, not sustainable capacity or performance parity.
One pair cannot establish a repeatable causal regression.

## Exact measured implementation

The candidate adds an ordered, bounded publication feed to the original native
issuance lane. It retains a complete verified capture and assignment under the
existing byte admission, reserves queue space before issuing a sequence, and
wakes blocked producers on shutdown. A 65-frame capture remains one complete
witness when follower transport splits it into 64 and one frames.

The example application **does not install this feed**. Ordinary actor bundle
ACKs remain disabled; both measured windows recorded zero bundle proofs.
This is a correctness prerequisite for the producer, not an activated shared
selection optimization. It has not delivered a TPS gain.

## Workload and environment

The windows ran on 2026-10-08 UTC in baseline/candidate order. Both offered
15,000 writes/s for 300 seconds after 30 seconds of warmup, with 1,000 uniformly
active Cells, 96-byte values, 128 clients and a 128-offer queue. Each command
performs SQL INSERT plus SELECT and retains a two-hour durable request/result
ledger. There was one repetition and no separate read load. This is not the
bounded KV workload used for the separate laptop result.

All roles shared one ARM64 Linux VM with 8 CPUs and nominal 16 GiB RAM:
owner, two followers, client and RustFS. Node ceilings remained 8 CPUs/16 GiB
with 4 GiB tmpfs each; Cellule retained its 64 MiB memory and 1 GiB managed-disk
budgets. RustFS retained the separate diagnostic's 2-CPU/8-GiB ceiling. Another
VM remained running on the physical host. These resources do not establish
dedicated serving-node capacity or physical-device durability.

Clients, auditors, workload fixtures, pinned images, loaded runner, Docker host
and point contracts matched. The candidate was a fresh release build without
overlays. Its actual adapted-source cache key and frozen verification source
were independently reconciled with the build manifest and measured binaries.

## Actual completed windows

TPS counts successful logical writes completed inside the 300-second window.
Successful latency percentiles are exact nearest-rank values from client
journals. Scheduled latency starts at the offered time; request latency starts
when the request is sent. Errors and queue drops are excluded from successful
percentiles and are reported separately. Neither window had late successes.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Completed writes/s | 443.41 | 369.85 |
| Successful writes inside window | 133,023 | 110,956 |
| Successful scheduled p50 ms | 100.62 | 104.68 |
| Successful scheduled p95 ms | 496.47 | 370.11 |
| Successful scheduled p99 ms | 1,121.46 | 1,029.88 |
| Successful request p99 ms | 509.98 | 444.17 |
| All-attempt scheduled p99 ms | 251.5 | 166.3 |
| Request errors | 2,722,188 | 3,079,381 |
| Queue drops | 1,644,789 | 1,309,663 |

Both generated all 4,500,000 scheduled offers. Attempted successes, errors and
queue drops reconcile exactly. Lower successful tail latency alongside fewer
successful writes and more errors does not establish a performance improvement.
Both fail the unchanged delivery, latency and publication-age stability gates.

## Publication and lifecycle

| Window observation | Baseline | Candidate |
| --- | ---: | ---: |
| Selected Cell roots | 60,029 | 49,998 |
| Materialized commits per root | 2.207 | 2.165 |
| All-provider successful PUTs per completed command | 2.016 | 1.922 |
| End oldest unpublished age ms | 153,876 | 176,495 |
| Mean owner capture ms | 0.54 | 0.84 |
| Mean owner dirty admission ms | 208.98 | 222.96 |
| Mean owner publication ms | 2,561.30 | 3,013.88 |

Phase means describe different overlapping cohorts; they cannot be added into a
command critical path. Storage ratios include all serving nodes but exclude
trailing publication and SDK-internal retries. They are not total lifecycle
cost. Publication remains sparse and age grows despite sampled local tmpfs
headroom. Both retain active, nonrotating Fleet in the original epoch.

The baseline checked all 158,567 ACKs, including seed and warmup, through warm
reads/exact retries and bucket-only cold recovery with zero audit errors. Its
fleet drain took 9.01 seconds. It still fails performance qualification.

The candidate checked 128,406 ACKs in the warm audit: 128,160 retries passed
and 246 checks failed with HTTP 503 examples. Its owner missed the 120-second
drain deadline and required forced cleanup. Cold recovery was not reached.
These failures do not by themselves establish data loss; they prevent the
candidate from passing the durability/availability qualification.

## Aborted comparison and verification

The six-case plan also included pinned celld v0.6.1
(`f2bf648663a610eefde71f3547ad61e9b896b1f0`) and Bucket at 2,000 offered writes/s.
During the celld Fleet arm, macOS reported only 104 MiB available, temporary-file
creation failed with ENOSPC, and Docker reported a storage I/O error. That
window was interrupted and excluded. None of the Bucket arms was attempted.
There is **no fresh complete celld comparison, Bucket result, read-only result
or mixed result** in this delivery. Earlier results remain separate in the
[previous measurement](pr67-fleet-root-delay-measurement.md).

The benchmark VM is stopped; the default VM and Docker context remain
unchanged. Raw evidence stays outside Git under `cellule-write-perf-8ad1`:
`ordered-feed-d4bec27-plan.json`, `ordered-feed-d4bec27-plan-status.json`,
`ordered-feed-d4bec27-results.json`, `ordered-feed-d4bec27-cellule-pair.json`
and `ordered-feed-d4bec27-evidence-index.json`. The independent verifier
reconciles the two completed windows' counts, exact successful percentiles,
ACK manifests and source/binary identities. Its overall status intentionally
fails because the plan is incomplete and the celld window is missing.

The frozen candidate passed all 11 contributor verification routes, including
the publication-feed cancellation, shutdown and complete-witness regressions.
Passing source checks does not qualify performance. Shared selection in actor
ACK/read/retry paths, exact capture release, admitted asynchronous
materialization, complete issued-range drain/recovery and safe cross-Cell
collection remain necessary. The
[design exit gates](../crates/cellule-runtime/docs/write-performance-design.md)
remain unmet.
