# PR 67 write performance reevaluation

Measured 2026-10-07. **Write parity remains unqualified.** The latest candidate
improves low-load Fleet latency and modestly improves overloaded bucket results
against main, but completes fewer commands at the Fleet target. These are fresh
measurements of the enabled application path, not a benchmark of the experimental
bundle ACK APIs.

## Sources and experiment

| Arm | Pinned revision |
| --- | --- |
| Latest PR 67 candidate | `7fc079345b5214a18a001833732a84039e4485db` |
| Main | `831877cf5af864b11b7bda79b594639fbd233e94` |
| celld v0.6.1 | `f2bf648663a610eefde71f3547ad61e9b896b1f0` |

Nine serial cases used the same SQL-ledger application: 1,000 uniform Cells,
96-byte values, INSERT plus SELECT and a durable request/result ledger,
128 clients and 128 queued offers. Each case measured 300 seconds after
30 seconds warmup, with fresh provider data. This is one matched repetition per
point, not the proposal's three-repetition qualification or a bounded-KV test.

The owner, two Fleet followers, client and RustFS shared an ARM64 Linux Docker VM
with 8 CPUs and 16 GiB RAM. Node state used 4-GiB tmpfs mounts. RustFS retained its
2-CPU/2-GiB quota; the client retained its 4-CPU/4-GiB quota. Cellule retained
64 MiB capture admission and a 1-GiB managed disk budget. The Docker data disk
was expanded from 120 to 200 GiB before every arm to preserve inode headroom;
older results are not a controlled before/after pair for that change.

The latest source was exported into a fresh external directory and compiled in
release mode. Main reused its verified immutable release binary because remote
main was unchanged. Neither arm used a measurement overlay. Client and ACK
auditor binaries, fixture sources, loaded runner and pinned container images
matched byte-for-byte. Latency starts at scheduled arrival; errors, drops,
unissued offers and late completions remain failures.

## Measured requests

| Mode / offered writes per second | Main completed TPS / p99 ms | Latest candidate completed TPS / p99 ms | celld completed TPS / p99 ms |
| --- | ---: | ---: | ---: |
| Fleet / 100 | 100.000 / 34.7 | 100.000 / 19.9 | 100.000 / 16.0 |
| Fleet / 15,000 | 545.647 / 2,823.8 | 319.063 / 146.6 | 1,039.493 / 102.0 |
| Bucket / 2,000 | 154.840 / 9,677.6 | 167.233 / 6,299.8 | 635.750 / 1,553.6 |

The two target rows are **overloaded completion rates, not sustainable capacity**.
Their all-attempt p99 values include failures. In particular, the candidate's
lower Fleet stress p99 cannot establish a latency win with millions of errors.

| Target case | Measurement request errors | Dropped offers |
| --- | ---: | ---: |
| Main Fleet | 14 | 4,336,039 |
| Candidate Fleet | 3,629,118 | 775,163 |
| celld Fleet | 2,120,374 | 2,067,778 |
| Main bucket | 0 | 553,292 |
| Candidate bucket | 0 | 549,574 |
| celld bucket | 0 | 409,019 |

All nine cases generated every planned measurement offer. At 100 Fleet writes/s,
the candidate's p99 was 42.7% below main and 24.4% above celld. Each arm's 34,001
seed/warmup/window ACKs passed every warm and cold GET and exact original retry.
Original nodes were removed before bucket-only restore.

At the Fleet target, the candidate completed 41.5% fewer commands than main.
At the bucket target, it completed 8.0% more than main with 34.9% lower p99.
Neither observation establishes repeatability, qualified capacity or parity.
Main's fresh low-load latency differs substantially from its historical samples;
changes between sessions cannot be attributed solely to the latest source.

## Recovery and failure evidence

| Case | Warm / cold ACK and exact-retry audit | Original fleet drain |
| --- | --- | ---: |
| Main Fleet 100 | All 34,001 pass both | 4.69 s |
| Candidate Fleet 100 | All 34,001 pass both | 4.17 s |
| celld Fleet 100 | All 34,001 pass both | 4.17 s |
| Main Fleet target | All 203,350 pass both | 20.47 s |
| Candidate Fleet target | Warm: 107,065 HTTP 503 errors; cold not reached | No successful drain result |
| celld Fleet target | Warm: 487,194 HTTP 500 errors; cold not reached | No successful drain result |
| Main bucket target | All 53,387 pass both | 5.35 s |
| Candidate bucket target | All 57,416 pass both | 4.16 s |
| celld bucket target | Warm: all 218,423 pass; cold: 111 HTTP 500 errors | 9.79 s |

The candidate Fleet provider was OOM-killed about four seconds after the final
measurement metric sample. The warm audit therefore ran with an unavailable
provider. Preserve this infrastructure failure separately from Cellule's
publication pressure; the HTTP 503s alone do not identify a framework availability
bug or acknowledged-state loss. The complete case fails infrastructure and
recovery qualification even though the measurement window produced a TPS value.

The celld Fleet owner reported `database or disk is full` during local WAL
capture. Its warm HTTP 500 failures and missing cold audit leave durability
unverified. The stopped tmpfs could not be inspected afterward; the log is not
a verified byte/inode measurement or evidence of object-provider exhaustion.

The celld bucket provider remained healthy through cold recovery, but the cold
audit had 111 HTTP 500 errors and only 218,312 of 218,423 exact retries were
checked. Its cold log records a 30-second S3 LIST timeout during restore. This
does not isolate the cause of every HTTP error or demonstrate missing durable
bytes; the unchanged complete audit fails.

## Publication and architecture findings

Cost counters measure storage API operations; SDK-internal retry attempts are
not individually counted.

The candidate Fleet 100 window cost **5.468 successful provider PUTs/command**,
versus **5.493** for main. It selected about one root per command, and 99.1% of
captures used singleton uploads. Enrollment cost was 0.0138 GETs/command across
owner and receivers. The strict debt trend check failed for both Cellule arms;
the candidate's sampled byte slope was +18.55 bytes/s. A small observed backlog
does not override the unchanged qualification gate.

At the Fleet target, main materialized 4.55 logical commands/selected root,
versus 2.12 for the candidate. Candidate publication averaged 3,620.7 ms and
worker round trip 23.16 ms, versus main's 2,858.8 ms and 10.60 ms. These are
overlapping event populations, not additive command service times. The candidate
charged about 61 MiB of its 64-MiB retained budget by minute two; its minute-four
object frontier stopped advancing while Fleet proofs continued. Complete issued
range tracking does not itself resolve publication or admission pressure.

Candidate follower durable append averaged 21.82–21.89 ms per batched operation
at the Fleet target. Data sync averaged about 0.001 ms on tmpfs. This does not
establish physical-device fsync performance or a RocksDB comparison. Window PUT
ratios at the Fleet target exclude substantial unpaid publication and trailing
work and must not be credited as a steady-state cost reduction.

The healthy candidate bucket case cost **3.795 successful PUTs/command**, with
1.024 materialized commands/selected root. Its sampled age trend passed, but
delivery and latency failed. Per-Cell root/lineage/authority selection remains
the cost floor of the enabled path. The experimental shared node selection,
authenticated locators and independent materialization APIs are still absent
from application response/read/retry and background scheduling. See the
[implementation status](bundle-coverage-implementation.md).

Read-only/mixed capacity, A/A variance, physical-device durability, owner-loss,
grace-qualified collection and complete M0–M5 qualification remain unverified
by this write-only matrix.
The [proposal](write-performance-proposal.md) retains all original acceptance
gates. Source manifests, binaries, request journals, failed cases and machine
reports are retained outside the repository under the `pr67-7fc0793` evidence
tag; only this concise result is committed. The external evidence index is
`pr67-7fc0793-reevaluation-evidence-index.json`, SHA-256
`1ebb4d3e19f1265afa69047c0237612dea288bee1f8a5789d457413b4c960855`.
