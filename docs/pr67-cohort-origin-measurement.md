# Fresh bundle origin reads: write parity still fails

**Measured code: 95.35 Fleet writes/s and 274.53 Bucket writes/s.** In one
fresh paired diagnostic, Fleet completed 8.0% more writes and successful
scheduled p99 fell 46.1%. The Bucket fixture bypasses this optimization yet
completed 19.1% more writes; its request p99 worsened 2.9%. These short runs
establish neither a repeatable, attributable throughput gain nor parity.
**Every point fails qualification. PR #67 remains a draft.**

## Verified change

Candidate: `9d4e6328bceb2f88087698809c3b4167f94959ef`.
Immediate predecessor: `e40ecd6b6b2972ee91a52846737cbc12bd48f406`.
Celld: `f2bf648663a610eefde71f3547ad61e9b896b1f0`, using the unchanged pinned
image. Both Cellule release binaries were exported from committed source.

Selection previously fetched a fresh cohort's header, shards, histories and
native frames separately. The real 64-Cell regression observed **187 reads of
the same newly uploaded object** before this change and **one full origin read**
after it. Selection now reads that object once, bounded to 4 MiB, compares every
byte with the proposal, checks its digest, and uses shared byte slices through
the same canonical index/frame verifiers. It drops checked frame bodies
promptly. Historical extents and each Cell's base dependencies still require
origin verification. The operation retains no availability cache across calls;
the regression also blocks the origin object and verifies selection retry fails.

The fresh origin body's separate **4-MiB buffer is charged before installing the producer**.
The producer reservation increases **16 → 20 MiB** within the unchanged
64-MiB runtime retention ledger. The protocol object ceiling remains 4 MiB.
This admission cost reduces headroom for other retained work. No persisted
format, lease check, exact-range check or recovery requirement changes.

## Workload and environment

All six cases ran sequentially with fresh prefixes and fresh Linux provider
volumes. Comparison verified byte-identical client/auditor binaries, fixtures,
pinned images, loaded runner and Docker resource contract. Each used 1,000
uniform Cells, 96-byte values, SQL INSERT plus SELECT with a two-hour result
ledger, 128 clients and 128 queue slots. Warmup was 30 seconds, followed by
one 60-second measured window. Fleet offered 15,000 writes/s with two followers;
Bucket offered 2,000/s without followers. These are SQL application diagnostics;
they do not reproduce the reported bounded-KV laptop workload.

The ARM64 Docker VM has **8 CPUs and 8 GiB total shared RAM**. Serving containers
have an 8-CPU/16-GiB ceiling and 4-GiB tmpfs; the client ceiling is 4 CPUs/4 GiB.
RustFS has 2 CPUs and an 8-GiB memory/swap ceiling, using the same external
diagnostic adaptation for every arm. Container ceilings exceed available VM
resources; another workstation VM was present. The dedicated 8-CPU/16-GiB node,
2,000 Cells, three paired five-minute repetitions, physical-media durability,
and separate read/mixed qualification remain unverified. Acceptance gates and
workload budgets were preserved.

## Reconciled write windows

TPS counts successful logical writes completed inside the measured window.
Successful p99 is independent nearest-rank journal replay including trailing
successes. Scheduled latency starts at offered arrival; request latency starts
at issuance. Successful percentiles exclude errors and drops; those failures
remain explicit. The unchanged delivery gate also checks all-attempt latency.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / predecessor | 88.28 | 8,188.28 | 4,911.10 | 0 | 894,447 |
| Fleet / candidate | 95.35 | 4,414.52 | 2,800.05 | 0 | 894,023 |
| Fleet / celld | 4,076.40 | 252.99 | 71.66 | 48,999 | 606,417 |
| Bucket / predecessor | 230.58 | 5,783.21 | 3,713.34 | 0 | 105,909 |
| Bucket / candidate | 274.53 | 4,367.63 | 3,820.18 | 0 | 103,272 |
| Bucket / celld | 1,310.63 | 1,152.28 | 509.51 | 0 | 41,106 |

Replay reconciled offers to successes, errors or drops, including trailing
completions, and reconciled every ACK cohort and its hash. **All six points fail
delivery qualification. These are overloaded completion rates, not sustainable
capacities.** The predecessor previously completed 100.20 Fleet/s and 271.63
Bucket/s in the [coverage-race measurement](pr67-coverage-race-measurement.md).
Different runs of unchanged code vary substantially. The positive paired
differences above do not reverse the producer's earlier regression or establish
an attributable improvement.

## Availability, drain and recovery

| Mode / system | ACK cohort | Warm read/retry | Bucket-only cold read/retry | Drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / predecessor | 7,371 | pass | pass | 27.98 |
| Fleet / candidate | 10,979 | pass | pass | 26.01 |
| Fleet / celld | 467,118 | fail: 463,546 HTTP errors | not reached | failed |
| Bucket / predecessor | 22,837 | pass | pass | 15.15 |
| Bucket / candidate | 28,125 | pass | pass | 21.26 |
| Bucket / celld | 118,680 | pass | pass | 1.75 |

ACK cohorts include setup, warmup, steady and trailing successes. Passing audits
read and retry every original ACK and verify its stored result and incarnation;
these cases also pass the cold contract retry. Cold recovery starts with empty
local state after joined graceful drain. It does not qualify failed-owner
recovery before materialization or recovery of an owner-lost Fleet suffix.

Celld Fleet's retained Docker state reports **OOMKilled: true, exit 137**. Its
warm audit failed and cold audit was not reached. That is an availability and
recovery-evidence failure in this shared-memory fixture, not established data
loss. Missing drain/cold evidence stays failed rather than being inferred.

## Remaining bottleneck

| Fleet window metric | Predecessor | Candidate |
| --- | ---: | ---: |
| Native-authority GET/range attempts per completed write | 6.32 | 3.04 |
| Total GET/range attempts per completed write | 20.47 | 20.40 |
| Successful PUTs per completed write | 3.30 | 3.68 |
| Materialized commands per selected root | 1.09 | 1.08 |
| Steady Bundle ACKs | 0 | 0 |
| Mean capture ms | 0.30 | 0.27 |
| Mean follower proof ms | 5.93 | 5.88 |
| Mean publication ms | 9,121.96 | 5,811.08 |
| Mean Fleet response ms | 1,850.17 | 1,282.76 |

Native range reads fall **6.26 → 2.97 per completed write**, but immutable GETs
rise **12.24 → 15.44**. Operation counts include background work; phase samples
cover different cohorts and cannot be summed into one request's critical path.
Both windows still select almost one Cell root per command. The candidate
releases no additional commands through steady Bundle ACKs. Reducing one
verification object's fanout therefore does not remove ordinary per-Cell
publication or its upstream admission pressure.

Candidate retained bytes sampled **22.98 → 62.34 MiB** against the unchanged
64-MiB ledger, including the 20-MiB producer reservation. Pending native object
sequences grow **435 → 470**; pending publications are **596 → 599**, with oldest
unpublished work **5,639 → 6,164 ms**. Two boundary samples do not establish
stable bounded debt. Activation took **70.80 seconds versus 45.06** for the
predecessor, excluded from write TPS but retained as a separate result.

The next implementation must separate exact selected-capture cleanup from
immediate Cell-root materialization, retain admitted authenticated root debt,
and schedule fair dense checkpoints with joined shutdown and valid lease
renewal. The conditional **215 commands/checkpoint** and **0.05 publication
PUTs/command** targets remain unchanged. Bucket producer integration,
complete failed-owner suffix recovery including prior Fleet ACKs, safe
cross-Cell collection, and full read/write/mixed qualification remain open.

## Verification and evidence

The exact frozen functional candidate passed all eleven contributor routes on
Rust 1.97: format, targets/features, workspace tests (**1,956 passed, zero failed,
38 documented ignored**), local LTX, Clippy with `-D warnings`, API docs,
boundaries, layout, Rust fences, links and SQL/peer contracts. Linux releases
used the unchanged pinned Rust 1.98.1 image. The failing before-change I/O
regression is retained alongside passing after-change verification.

Raw material stays outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`.

| Evidence | Identity |
| --- | --- |
| Frozen verification source manifest | `9a734c09d69a304204bc785bafc6b079e0c6c87cccc8aad87b34c4a03352e543` |
| Candidate release source manifest | `350915bcff5cbb06cac4e954a443be2fac81a39d8ae1877fac854c538693e5c6` |
| Candidate SQL binary | `1660e2f686a3f4bcb4fe22dea0068045bead27a2c9147999710ac92967faf386` |
| Identical client | `417f07b0424d27df75b1dca22666a7adb621db3cd11fdb1048eb9247a664e60b` |
| Identical auditor | `6595c24b0be217e181e8a9aa7de5cf478669ac341b37b76754fdcf0b3865b7b2` |
| Verified evidence index: 1,941 files | `60e1b32284fdce2cb7e67e22564ddf2f8a318f47e6b13cb288df660350d2393a` |

The index is `cohort-origin-20261008-evidence-index.json`; its 1,203,520,400 bytes
were rehashed without mismatch. Replay, telemetry and paired comparisons are
`cohort-origin-20261008-{reconciled,telemetry,fleet-comparison,bucket-comparison}.json`.
Both comparison files verify fixture/host/runner provenance and explicitly
report `qualification_pass: false`. Linux provider volumes remain retained via
their recorded volume metadata.
