# Managed producer: measured write regression

Historical measurement. The subsequent
[coverage-race fix and fresh comparison](pr67-coverage-race-measurement.md)
restore Fleet warm/cold availability and drain, but demonstrate no TPS gain.
The latest [cohort-origin comparison](pr67-cohort-origin-measurement.md)
measures the next I/O reduction; write parity remains unqualified.

**The new Fleet connection regressed. Parity is not achieved.** It completed
93.57 successful writes/s versus 525.67 before the producer: **82.2% lower in
this pair**. Successful scheduled p99 grew from 155.13 to 3,501.22 ms. Warm
availability and joined drain failed. This experimental implementation remains
in draft PR #67; functional tests do not establish production readiness.

Bucket current code completed 227.78/s versus 246.20/s: 7.5% lower in this
single pair. The Bucket-only fixture bypasses node durability and its producer,
so this difference cannot be attributed to the new shared producer.

## Workload and identity

Measured runtime commit: `2dc19172ce71d1f7b051705d70f005a70a47e554`.
Before-producer baseline: `3e436013a7e7500694bfdfe5600fe4a69adb1988`.
Celld: `f2bf648663a610eefde71f3547ad61e9b896b1f0` (pinned image).
Every exported candidate file was compared with the committed Git tree;
contents match, resolving original symlinks as the build export does. The build
manifest recorded the preceding HEAD because export preceded the code commit.
Its immutable source and binary hashes were preserved rather than rewritten.

Each point used 1,000 uniform Cells, 96-byte SQL INSERT plus SELECT, a two-hour
request/result ledger, 128 clients and 128 queue slots. Warmup was 30 seconds;
the measured window was 60 seconds, one repetition. Fleet offered 15,000 writes/s
with two followers; Bucket offered 2,000/s. Read-only and mixed capacity were
not measured. This is SQL application parity, not the laptop's bounded KV load.

All arms used the same Docker VM with 8 CPUs and **8 GiB total shared RAM**.
Owner/follower container ceilings were 8 CPUs/16 GiB with 4-GiB tmpfs; the client
ceiling was 4 CPUs/4 GiB. RustFS had 2 CPUs and an 8-GiB memory/swap ceiling,
changed from the canonical diagnostic runner's 2 GiB identically for every arm.
The loaded runner and this adaptation are hashed. No delivery, recovery or
qualification gate changed. Each case retained a fresh Linux Docker volume.
These ceilings exceed the shared VM's actual resources; another workstation VM
was present. This does not qualify a dedicated 8-CPU/16-GiB serving node.

The first celld Fleet attempt was interrupted when its processes and VM
stopped, with no completed case summary. Its files and container states are
retained separately and excluded below. Both celld cases were rerun with fresh
prefixes. The comparator confirmed identical host/resource contracts, client
and auditor binaries, fixtures, pinned images and loaded runner across arms.
One serial overloaded pair cannot establish repeatability or isolated causality.

## Reconciled windows

TPS counts successful logical commands completed inside the window. Successful
p99 is nearest-rank replay of request journals, including trailing successful
completions; scheduled latency begins at offered arrival. Errors and drops are
excluded from these successful percentiles and remain explicit failures.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: |
| Fleet / before producer | 525.67 | 155.13 | 736,684 | 131,776 |
| Fleet / new producer | 93.57 | 3,501.22 | 76 | 894,057 |
| Fleet / celld | 4,238.78 | 66.73 | 9,334 | 636,199 |
| Bucket / before producer | 246.20 | 4,721.84 | 0 | 104,972 |
| Bucket / current code | 227.78 | 4,775.42 | 0 | 106,077 |
| Bucket / celld | 1,368.50 | 724.42 | 0 | 37,638 |

Independent streaming replay reconciled every generated offer to a successful,
errored or dropped request, including trailing completions. ACK counts and
journal hashes also reconcile. **Every point fails delivery qualification**;
overloaded completion rates are not sustainable capacities. Celld Fleet also
changed posture and failed availability/recovery evidence; its high window rate
is not a qualified durable-throughput reference.

## Recovery and failure evidence

| Mode / system | ACK cohort | Warm | Bucket-only cold | Owner/fleet drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / before producer | 44,760 | pass | pass | 7.76 |
| Fleet / new producer | 11,036 | fail: 173 errors | not completed | failed cleanup |
| Fleet / celld | 465,015 | fail: 427,303 errors | not completed | failed cleanup |
| Bucket / before producer | 24,924 | pass | pass | 7.27 |
| Bucket / current code | 23,925 | pass | pass | 10.00 |
| Bucket / celld | 129,955 | pass | pass | 1.78 |

ACK cohorts include setup, warmup, steady and trailing successes. Audits GET
and retry every original acknowledged command, verifying stored output and
sequence. Current Fleet's warm errors were HTTP 503. Its owner failed the
120-second cleanup drain while reporting `PendingPublication`, then required
forced container stop. Cold audit was not reached. This establishes failure
of availability/drain, not a proved data-loss result.

Celld Fleet's warm audit returned HTTP 500 errors; Docker recorded owner
`OOMKilled=true`, exit 137. Cold audit and successful drain were not reached.
Required after/cold provider observations are missing in both aborted cases;
missing evidence cannot pass. The interrupted attempt and failed rerun remain
retained. Successful tmpfs audits do not qualify physical power-loss durability.

## Publication cost and next action

| Fleet metric | Before producer | New producer |
| --- | ---: | ---: |
| Successful provider PUTs / completed command | 1.68 | 3.87 |
| GET and range calls / completed command | 0.48 | 24.30 |
| Materialized commands / selected Cell root | 2.35 | 1.09 |

These are window-only storage API observations, excluding trailing work and
SDK-internal retries. The new producer was installed, but measured Fleet Bundle
response/proof counters did not advance: both remained four from setup/warmup.
Follower proof continued to win responses. The selector still performed work;
its 512-entry feed reserves space before native issuance, so slow publication
also backpressures Fleet commands. Memory was about 20 MiB at the initial sample
and 19 MiB at the final sample, including the 16-MiB producer reservation, below
the unchanged 64-MiB ledger ceiling at those sampled boundaries.

The connected code retains and joins one bounded producer, fairly serves native
selection and exact checkpoint requests, narrows later-cohort proofs using
original complete assignments, joins checkpoint callbacks before Cell closure,
and waits for the complete issued prefix including prior Fleet ACKs. Three
managed-producer regressions test actor visibility/retry/cold results, rejected
startup admission, and preservation of the original producer failure cause.

The next implementation must reduce repeated origin verification and sparse
materialization work, establish admitted dense materializer scheduling, and fix
failed-actor/overload closure. Increasing queues or dropping issued obligations
would not satisfy the contract. Bucket producer integration, large-capture
handling, retryable producer errors, complete failed-owner orchestration, safe
cross-Cell collection, and 2,000-Cell qualification remain open. The conditional
215-command checkpoint target is a component result, not the density above.

## Verification and retained evidence

The exact measured snapshot passed workspace checks and **1,954 tests**, with
zero failures and 38 documented ignored tests, local LTX tests, API docs,
boundary/layout, documentation and SQL/peer checks. Full workspace Clippy with
`-D warnings` passed on the declared Rust 1.97 minimum. Additional Rust 1.99
Clippy failed on the pre-existing `AtomicU64::fetch_update` deprecation; that
failure is retained. No warning or performance gate was suppressed. Functional
verification does not erase the failed end-to-end result.

Candidate source-manifest SHA-256:
`8c13c9ec32ba2dad6ca0857a6165ad661fdac49af3e4e2b9ccfd38a8f41498f1`.
Linux SQL release binary SHA-256:
`51d12763a1d81886d171b4c1592a86ec296f1eb50e57504d54206f8f933697b8`.
Raw journals, binaries, source snapshots, hashes, reports, failures and volumes
stay outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`, label
`managed-producer-20261008`. Three paired five-minute repetitions, A/A variance,
stable debt, overload recovery, read guardrails and the dedicated serving-node
profile remain unqualified. See the unchanged
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md).

The external evidence index covers 1,854 retained files. Its SHA-256 is
`0475fdff305c8214de9b4f309a4dd74c4d152b9dc49384e1afd018633c726a4b`.
