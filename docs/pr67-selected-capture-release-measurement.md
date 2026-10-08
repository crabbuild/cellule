# Selected bundle capture release: measured writes

**Write parity is not achieved.** Current code completed
**461.53 Fleet writes/s** and **168.92 Bucket writes/s**.
Fleet was 5.9% higher than baseline; Bucket was 18.2% lower.
This report measures committed runtime code
`3e436013a7e7500694bfdfe5600fe4a69adb1988`, including exact selected-capture
release through the actor-owned publisher. One short overloaded repetition
cannot establish a repeatable improvement or regression. The embedding SQL
application still does not install a node bundle producer, so the new shared
selection response/capture-release lane was not exercised by these TPS cases.

## Matched diagnostic

The six cases ran serially on the same ARM64 Docker VM with **8 vCPUs and
8 GiB total RAM**, shared by the serving nodes, RustFS and client. Fleet used
one owner and two followers. Each case used a fresh object-store volume.
Node container ceilings were 8 CPUs/16 GiB with 4-GiB tmpfs state; RustFS had
a 2-CPU/8-GiB ceiling and the client 4 CPUs/4 GiB. These ceilings exceed the
VM's available memory. Cellule retained its 64-MiB retained-memory and 1-GiB
managed-disk budgets. Another workstation VM remained running.

All arms used identical load/audit binaries, fixture bytes, pinned images and
runner code: 1,000 uniform Cells, 96-byte values, 128 clients and 128 queue
slots, SQL INSERT plus SELECT and a two-hour request/result ledger. Each point
had 30 seconds of warmup and a 60-second measured window. Fleet offered 15,000
writes/s; Bucket offered 2,000/s. This differs from the laptop's bounded KV
workload and the dedicated 8-vCPU/16-GiB serving-node qualification. There
were no separate read-only or mixed measurements.

Baseline: `b1856728984781cee5398f8d7185fb88fde23993`.
Celld: `f2bf648663a610eefde71f3547ad61e9b896b1f0`.

## Independently reconciled windows

TPS counts successful logical writes completed inside the window. Successful
latency is the exact nearest-rank percentile replayed from request journals,
including trailing successful completions. Scheduled latency starts at the
offered arrival; request latency starts at send. Errors and drops are excluded
from successful percentiles and remain separate delivery failures.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / baseline | 435.62 | 302.84 | 149.11 | 680,024 | 193,839 |
| Fleet / current code | 461.53 | 266.49 | 192.42 | 685,202 | 187,106 |
| Fleet / celld | 3,525.58 | 204.00 | 113.85 | 0 | 688,209 |
| Bucket / baseline | 206.50 | 4,979.39 | 4,381.86 | 0 | 107,354 |
| Bucket / current code | 168.92 | 10,784.35 | 8,705.00 | 0 | 109,609 |
| Bucket / celld | 486.93 | 1,754.17 | 961.24 | 0 | 90,528 |

All generated offers reconcile to successful, errored, dropped or unissued
offers. None passes the unchanged Fleet 15K TPS/p99 50-ms or Bucket 2K TPS/
p99 200-ms delivery gates with zero errors and drops. These are observed
overloaded window rates, not sustainable maximum throughput.

| Mode | Current-code successful writes inside / after window | Current / baseline observed TPS |
| --- | ---: | ---: |
| Fleet | 27,692 / 0 | 1.059× |
| Bucket | 10,135 / 256 | 0.818× |

The ratios describe this single pair only. Inactive bundle counters prevent
attribution to the new capture-release lane. Workstation conditions and run
order remain confounders; older snapshots' measurements remain separate.

## Recovery and publication observations

| Mode / system | ACK cohort | Warm / cold checks | Warm / cold | Owner drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / baseline | 45,266 | 45,266 / 45,266 | pass / pass | 11.06 |
| Fleet / current code | 47,369 | 47,369 / 47,369 | pass / pass | 5.85 |
| Fleet / celld | 356,306 | 356,306 / 356,306 | pass / pass | 25.39 |
| Bucket / baseline | 19,657 | 19,657 / 19,657 | pass / pass | 10.39 |
| Bucket / current code | 20,391 | 20,391 / 20,391 | pass / pass | 28.04 |
| Bucket / celld | 50,585 | 50,585 / 50,585 | pass / pass | 3.24 |

ACK cohorts include setup, warmup, steady and trailing successful commands.
Cold recovery reconstructs from the bucket with the old local state and
follower logs unavailable; retries check original outcomes. These successful
audits do not qualify power-loss durability on physical storage.

| Current-code mode | Bundle proof / response count | Selected Cell roots | Commits / root | Successful provider PUTs / completed command |
| --- | ---: | ---: | ---: | ---: |
| Fleet | 0 / 0 | 12,174 | 2.31 | 1.75 |
| Bucket | 0 / 0 | 10,241 | 1.01 | 3.64 |

Provider totals are window-only storage API observations, excluding trailing
publication and SDK-internal retries. Per-Cell roots remain sparse. Fleet
retained memory was near its 64-MiB ceiling at the initial sample and all eight
dirty publication slots were occupied at both boundaries. This is evidence
of backlog/admission pressure, not an isolated causal profile. Shared capture
upload batching does not remove the remaining per-Cell authority work.

## Delivered code and remaining gates

The actor can now release an exact selected capture prefix before its Cell-root
CAS and rebuild that root from the authenticated selected origin through its
existing serialized publisher. Original assignment fingerprints cover every
ordered segment descriptor and body digest. The executor retains outcomes and
root obligations until joined materialization; an unproved newer suffix remains
hidden. A regression test pauses root selection, verifies capture files and
metadata were released, adds a newer command, then checks drain, retry, cold
restore and complete resource release. No wire or persisted format changed.

A canonical bounded node bundle producer and fair admitted materializer
scheduling still need framework implementation and host integration before
improving the measured application path. Dense checkpoints, stable bounded debt, complete
failed-owner issued-suffix orchestration and safe cross-Cell collection remain
open. Three paired five-minute runs, the 16-GiB serving-node profile, 2,000-Cell
10K-write/50K-read qualification and read guardrails remain outstanding.

## Verification and external evidence

The measured source passed all 11 contributor verification routes in an
isolated snapshot: **1,951 workspace tests passed, zero failed, 38 documented
ignored**. An independent journal replay verified all six window counts,
successful percentiles, ACK hashes/cohorts, source and adapted build manifests,
matched binaries and provider lifecycle/capacity evidence. Measurement integrity
passed; performance qualification remains false.

The complete source-manifest SHA-256 is
`e4ad9aea226ede251ae88ffa5cfcde086aacda80823d151c26df19736ec41548`.
The measured Linux SQL release binary SHA-256 is
`96546154c9d2b327d6ad9d0a0cd1ea02e3a73d6613e07a4ff529c5158a20edda`.

Raw journals, manifests, release binaries and logs remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`, evidence label
`selected-capture-release-3e43601-vm8-20261008`. The evidence-index SHA-256 is
`c6435776c737465303a2e7a31547ed6a9136048e7d399da2f7eea214af219ecc`.
The earlier [receipt measurement](pr67-bundle-receipt-measurement.md) and its
interrupted attempts remain historical evidence. See the
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md)
for the unchanged exit gates.
