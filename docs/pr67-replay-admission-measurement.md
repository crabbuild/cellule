# PR 67: durable replays and publication admission

**Performance parity remains unmet.** Candidate
`c45693aa6c79cb8c83a0dedbd2db0ad9977934ac` completes 158.15 Fleet writes/s
versus 152.92 before and 4,473.32 for celld in fresh overloaded diagnostics.
The candidate passes complete warm/cold ACK read and retry audits; the baseline
fails its warm audit. A separate 1-GiB budget control reaches only 191.08
writes/s. Raising retained credit alone does not close the throughput gap.

## Reproduced failure and delivered change

Previously, node publication pressure refused a command before checking whether
its original outcome was already durable. A full Cell publication queue also
left the same retry waiting for object selection. Two real actor/Fleet tests
reproduce these failures in three baseline executions: capacity refusal at the
node limit and timeout at the Cell limit.

The candidate uses the existing bounded mailbox, worker and node byte admission
for a read-only lookup of that original request. The normal reply gate still
checks the original owner and durability visibility. Digest conflicts, result
limits and identity expiry remain enforced. An absent node-pressure request
retains its original refusal and never invokes its handler. An already admitted
fresh request at a full Cell queue is probed once, then retains its handler and
original FIFO position until capacity returns. Probes cannot join mutating native
groups or invalidate persisted-work inventory.

The initial implementation rejected that already admitted fresh request. The
existing full-suite FIFO contract caught the regression. The final implementation
preserves the contract without changing its test or any qualification gate.
The two new regressions pass three times after correction, including exact
original outcomes, cold counter/ledger state, joined closure and zero retained
credit. Inbox-effect delivery is not changed by this command-replay fix.

All contributor checks and Rust 1.99 Clippy pass in an immutable final snapshot:
1,965 workspace tests and 60 local LTX tests pass; 38 environment-dependent tests
remain ignored. All 1,300 Rust/Cargo files match the verified snapshot and pinned
production source used for the Linux build. Raw failed builds, the intermediate
FIFO failure and final verification are retained outside Git.

## Fresh original-budget comparison

Baseline: `addc1cecefb42a094bc6a1c42289d43334c0e645`.
Celld: `f2bf648663a610eefde71f3547ad61e9b896b1f0`.
The three sequential cases use identical client/auditor binaries, fixtures and
images: 1,000 uniform Cells, 96-byte SQL values, INSERT plus SELECT and a two-hour
outcome ledger, 128 clients/queue slots, 30-second warmup, 60-second window and
15K offered writes/s. No build or contributor suite overlaps a timed window.

| System | Completed writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cellule baseline | 152.92 | 2,595.27 | 2,211.48 | 609,543 | 281,244 |
| Cellule candidate | 158.15 | 1,761.20 | 1,097.45 | 367,845 | 522,434 |
| celld | 4,473.32 | 95.22 | 51.03 | 5,447 | 626,152 |

TPS counts successes inside the 60-second window. Successful p99 also includes
trailing measured successes and excludes errors/drops; scheduled latency begins
at offered arrival. Qualification retains all-attempt latency and zero-drop
requirements. Candidate has 9,489 successes inside and 232 after the window.
The 3.42% completion difference in one overloaded pair is not a repeatable gain.
Candidate also records 7,385 warmup request errors versus zero in the baseline;
warmup drops and attempt populations differ.

| System | Complete ACK cohort | Warm errors / completed retries | Cold audit | Controlled drain |
| --- | ---: | ---: | --- | --- |
| Cellule baseline | 18,647 | 4,305 / 14,342 | not reached | unverified |
| Cellule candidate | 19,542 | 0 / 19,542 | 19,542 reads and retries; zero errors | 30.03 s |
| celld | 465,076 | 458,655 / 6,421 | not reached | unverified |

Candidate provider health/lifecycle observations also pass. The baseline's
sampled warm failures are retry POSTs returning 503. Celld's sampled failures
are GETs returning 500, and its owner is OOM-killed, exit 137. Failed warm audits
prevent controlled drain/cold qualification and required after/cold observations;
missing gates do not themselves prove mutation loss. Every offer, attempt,
completion and complete ACK cohort reconciles independently.

The shared ARM64 Docker VM has 8 CPUs and 8 GiB total RAM. Node container ceilings
are 8 CPUs/16 GiB with 4-GiB tmpfs; client/provider ceilings also exceed the VM.
Only Cellule receives the explicit 64-MiB retained-work and 1-GiB managed-disk
limits. Celld metadata records these values without applying equivalent limits.
These are workload-matched diagnostics with asymmetric resource policies,
not dedicated standard-node or physical-device qualification. All runs fail
performance qualification; the completed candidate lifecycle is not a passing
capacity profile.

## Budget control

Two additional sequential Cellule cases change only retained-work credit from
64 MiB to 1 GiB, using the same before/after binaries, workload, VM, client,
auditor and 1-GiB managed-disk limit. This is a separately labelled diagnostic,
not a replacement for the original profile or a matched celld qualification.

| Code | Completed writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops | Warm/cold ACK cohort | Drain s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 170.42 | 2,475.05 | 1,472.67 | 0 | 889,519 | 20,855; zero errors | 26.39 |
| Candidate | 191.08 | 2,613.81 | 1,782.00 | 0 | 888,279 | 21,871; zero errors | 23.02 |

The candidate's observed completion difference is 12.13%, but scheduled and
request p99 worsen. Neither row meets delivery or latency requirements. The
larger-budget baseline ends with only 27.34 MiB retained and 209.28 MiB of managed
disk reserved, while median request latency is 692.2 ms. Ample credit does not
remove the slow path. The three-arm comparator correctly rejects this two-arm
control; that rejected invocation is retained. Both rows independently reconcile
and their before/after workload and execution identities match.

## Architectural gap and next measurable work

Shared SQLite, LTX, node bundles and follower logs are components, not an equal
critical path. Managed Cellule already uses WAL NORMAL. In the larger-budget
baseline, mean native capture is 0.196 ms, worker execution 1.091 ms, actor queue
29.039 ms, Fleet proof 7.330 ms and Fleet response 732.140 ms. These are different
overlapping cohorts, not an additive per-request latency decomposition.

One important wait is outside the worker/proof timers:
[`execute_command`](../crates/cellule-runtime/src/cell/actor/requests.rs) records
worker execution, then awaits durability submission.
[`submit_capture`](../crates/cellule-runtime/src/node/log_shipper/mod.rs) reserves
native bytes and a shipping slot, then holds the ordered issuance lock while
waiting for publication queue capacity **before** committing the ticket.
The [publication feed](../crates/cellule-runtime/src/node/log_shipper/publication/mod.rs)
has 512 submissions and retains shared outstanding-byte admission through joined
selection. Consequently, slow object selection can throttle Fleet admission
before the follower proof timer starts. A larger node RAM ledger does not change
this separate bounded lane. Per-stage wait measurements are still needed to
quantify its share of the total gap.

Cellule [selection](../crates/cellule-runtime/src/node/bundle/selection.rs)
awaits each affected binding's verification in turn. The
[verifier](../crates/cellule-runtime/src/node/bundle/proof.rs) reopens the base and
historical ranges, even though the new cohort is read once. Native shipping also
awaits each complete append batch before beginning the next. Celld
[pipelines ordered shipping rounds and bundles dirty Cell tails](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4530),
and [groups already delivered follower appends before fsync](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160).

Original-budget candidate window observations are 13.64 GET/range attempts and
0.6106 successful PUTs per completed write, with 11.24 commands per materialized
root. This is still expensive background work. Endpoint API counters include
background cohorts; the denominator includes only successful in-window writes.
SDK retries and trailing publication are excluded. Fleet wins every observed
response race; zero Bundle responses do not mean the shared producer is disabled.
Candidate worker timings include 377,611 executions, mostly read-only pressure
probes, versus 9,617 captures. Their smaller average is not faster mutation SQL.

| Next slice | Required evidence before accepting it |
| --- | --- |
| Instrument post-SQL submission | Same-request timestamps for native-byte wait, shipping slot, ordered lock, publication slot, load/encode, ticket issuance, proof and final reply; reconcile bounded occupancy |
| Reduce repeated selection work | Original-authority retention and exact range witnesses; verify new bytes once without skipping cold dependencies; lower total GET/PUT cost in paired application runs |
| Separate native progress from publication debt | Recoverable bounded backlog before issuance; all prior Fleet ACKs survive complete-range recovery and drain; queue enlargement alone is not sustainable throughput |
| Pipeline follower rounds | Ordered confirmation and durable grouped appends; no release past an unresolved earlier range; fault and complete-suffix recovery tests |
| Qualify the node | Three matched five-minute repetitions, zero errors/drops, bounded debt, all-ACK warm/cold recovery, safe collection and read/mixed guardrails at the unchanged targets |

No new read-only or Bucket capacity result is claimed. The 2,000-Cell /
10K-write / 50K-read target and conditional 215-command checkpoint cost target
remain unqualified. PR #67 remains a draft.

## Evidence

Raw sources, binaries, fixtures, failed attempts, verification, journals,
provider observations and independent replay remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/replay-admission-20261008-*`.
The external evidence index records hashes and sizes; its independent verification
certificate covers every indexed file. No raw performance journal is committed.

[Previous receipt-pressure diagnostic](pr67-receipt-admission-measurement.md),
[bundle implementation](bundle-coverage-implementation.md),
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md).
