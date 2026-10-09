# PR 67: bounded base verification and the 2,000-Cell comparison

**Publication still throttles native issuance.** One matched Fleet write pair
completes **372.87 writes/s after versus 271.00 before**; celld completes
**1,999.80/s at 2,000 offered/s**. The change overlaps fresh small-base
verification within the original memory admission. It improves this observation
by 37.59%, but Cellule still fails throughput, latency, availability and drain.
PR #67 remains a draft; performance parity is not delivered.

## Workload and identities

| Dimension | This diagnostic |
| --- | --- |
| Before / candidate framework | `90f099a295158c6effe51079d35db07eab204d17` / `29b94157c5915819c65b5ee80a345c2f32a41f3c` |
| Fixture population correction | `cf49ef5637c40cc081365706669a7e2bf83f0a00`; production Rust is identical to the measured candidate |
| celld | `f2bf648663a610eefde71f3547ad61e9b896b1f0` |
| Population | 2,000 uniformly active Cells, independently verified in configuration, placement and seed receipts |
| Command | SQL INSERT and SELECT, 96-byte value, two-hour durable request/result ledger |
| Durability | Fleet, one owner and two followers; local state and follower logs on tmpfs |
| Client | 128 clients and 128 queue slots; writes and reads measured separately |
| Timing | 30-second warmup, 60-second window; one before/after write pair and one celld write point |
| Offered load | 2,000 writes/s or 20,000 reads/s |
| Cellule limits | Original 20-MiB producer admission, 64-MiB retained work, 1-GiB managed disk |

Each case uses a fresh namespace and object-store volume. The client, auditor,
fixtures, images and request settings match across the corrected cases. No
build or contributor suite overlaps their timed windows. Both managed write
paths use SQLite WAL NORMAL.

The shared ARM64 Docker VM has **8 CPUs and 8 GiB total RAM**. Each serving-node
container has an 8-CPU/16-GiB ceiling, the provider a 2-CPU/8-GiB ceiling and the
client a 4-CPU/4-GiB ceiling. These ceilings exceed aggregate VM capacity. Only
Cellule has explicit internal retained/disk limits. This is a matched workload
diagnostic, not dedicated 8-vCPU/16-GiB capacity qualification, a KV overwrite
benchmark or physical-media durability evidence.

The runner now accepts `--cells` and deploys that population in the celld
application configuration as well as the Cellule environment and client. The
first setup exposed the old celld template's fixed 1,000-Cell population before
any celld measurement began. That controller was stopped after its baseline
child completed; all five cases below use the same corrected runner. The
excluded initial baseline reached 301.22/s and also failed drain. Its evidence
is preserved; it is not substituted for the corrected pair's 271.00/s baseline.

## Write results

Latency columns contain **successful responses only**, including trailing
completions. Errors and dropped offers remain separate failures.

| System | In-window writes/s | Scheduled p99 ms | Request p99 ms | Measured errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before | 271.00 | 2,184.86 | 874.13 | 55,715 | 47,769 |
| Candidate | 372.87 | 932.77 | 531.98 | 49,986 | 47,642 |
| celld | 1,999.80 | 13.38 | 12.22 | 0 | 0 |

Before completes 16,260 writes in-window and 256 afterward; candidate completes
22,372 in-window and none afterward; celld completes 119,988 in-window and 12
afterward. Successful scheduled p99 improves 57.31% and request p99 improves
39.14% in this single pair. These are observations, not repeatable capacity.
Candidate warmup errors worsen from 14,450 to 16,496; warmup drops change from
33,450 to 30,692. Celld has zero warmup errors or drops. Its offered rate limits
this point, so 1,999.80/s is not a maximum-throughput measurement.

| System | Complete ACK cohort | Warm reads and original retries | Joined drain | Cold reads and original retries |
| --- | ---: | --- | --- | --- |
| Before | 30,617 | All pass | Fails 120-second deadline | Not reached |
| Candidate | 37,185 | All pass | Fails 120-second deadline | Not reached |
| celld | 182,001 | All pass | 16.63 s | All pass |

ACK cohorts include seed, contract, warmup and trailing successful writes.
Cellule owner logs report fenced heartbeat/drain retries. The cause of that
shutdown failure is not yet proven; missing cold evidence does not establish
mutation loss. The complete issued range must still join before departure.

## Read-only results

There is no before read-only case, so this comparison establishes neither a
read improvement nor a read-regression guardrail for the change.

| System | In-window reads/s | Scheduled p50 / p95 / p99 ms | Request p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Candidate | 17,456.38 | 2.13 / 20.30 / 28.79 | 14.67 | 0 | 152,566 |
| celld | 19,973.25 | 1.48 / 2.66 / 3.43 | 2.19 | 0 | 1,573 |

Candidate request p50/p95 are 0.70/10.73 ms; celld's are 0.73/1.42 ms.
Warmup drops are 413,815 and 786 respectively, with zero read errors. All
successful reads match the expected seed output and Cell/incarnation, with a
commit sequence at least the seed's. Both warm audits pass all 2,001 ACKs.
Cellule again fails the 120-second drain and does not reach cold audit; celld
drains in 3.54 seconds and passes all cold reads/retries. Failure even in this
read-only case warrants investigating closure scheduling and lease renewal
alongside dirty publication debt.

All five reports fail qualification. Read/write load together, Bucket, three
five-minute repetitions, equal internal resource policies and the dedicated
standard-node environment remain unmeasured in this slice.

## Why similar components have different throughput

The [earlier exact submission diagnosis](pr67-submission-timing-measurement.md)
measured 727.30 ms waiting for the ordered lock and 6.14 ms waiting for a
publication slot while holding it. That serialized 6.15-ms service interval
permits about 163 submissions/s, consistent with its 158.37 HTTP writes/s.
The actual hot-path scheduling differs despite both systems using SQLite, LTX,
node logs and object storage.

The current pair retains the same coupling:

| Successful assignment phase | Before mean ms | Candidate mean ms |
| --- | ---: | ---: |
| Local load and validation | 0.10626 | 0.08800 |
| Global ordered-lock wait | 224.93167 | 166.24648 |
| Publication slot with lock held | 2.07012 | 1.55474 |
| Ticket assignment and enqueue | 0.00660 | 0.00698 |
| Complete submission | 227.11516 | 167.89669 |

All phase counts reconcile to the 16,324/22,372 successful assignment callbacks,
and their partition residual is zero. This is a submission cohort, not a sum of
HTTP, SQL, follower or publication timers. Ordered-lock waiting remains about
99% of submission time. Candidate mean SQL-worker, capture and follower-proof
timers are 0.16, 0.14 and 10.44 ms in their respective cohorts. Those cohorts
overlap and the timers are not additive to submission time.

[`assign_capture`](../crates/cellule-runtime/src/node/log_shipper/mod.rs)
holds the node-wide ordering mutex while reserving publication capacity, before
assigning and enqueueing frames. Slow verification or materialization therefore
throttles writers across otherwise independent Cells before follower proof.
The reservation prevents sequence gaps and bounds original publication debt;
moving the same wait outside the mutex alone does not speed the consumer.

Celld's [Fleet loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4514)
does not wait for bucket publication and pipelines rounds with ordered
completion. Its [follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160)
groups delivered append frames into one durable batch. Cellule currently awaits
each `append_batch` before starting the next shipping round. That is a separate
pipeline gap; publication-coupled admission is the measured dominant gap.

Windowed storage GET/range attempts per completed write rise **6.73 to 7.14**;
successful PUTs fall **0.486 to 0.476**, and materialized commands/root rise
**6.97 to 9.37**. These counters include background work and exclude SDK retries.
Overlapping fresh reads reduces waiting; it does not eliminate repeated base,
historical, index and root I/O. The checkpoint cost target remains unmet.

## Delivered change and verification

`RootOriginVerification` is a one-use fresh origin plan. Small packed leaf graphs
with at most 32 inline descriptors charge 512 KiB per operation. Up to eight
operations overlap within 4 MiB. Fresh base verification and historical
scratch/facts use disjoint phases under the original **20-MiB** reservation.
Large graphs and oversized historical extents retain canonical serial fallback.
There is no cross-operation availability cache or new durability authority.
Missing/corrupt dependencies still prevent selection; ordinary canonical
scope, digest, graph, body, inventory and exact-range checks remain in force.
The bound is a structural working-set charge, not allocator-profile evidence.

Two real selector regressions hold root or pack I/O, observe exactly eight
operations and no ninth, and verify cancellation selects no authority. Both
fail three times before and pass three times after. Fresh retry verifies all
ten proofs and exact cold SQLite outcomes. LTX cases cover corruption/deletion
after planning, zero objects and a large-graph serial fallback with exact restore.

All contributor routes pass across immutable snapshots with identical production
Rust: **1,975 workspace tests pass, zero fail, 38 ignored; 60 local LTX tests
pass**. Format, all-target/all-feature check, Rust 1.97/1.99 Clippy, API docs,
boundaries/layout, Rust fences, document links, SQL/peer contracts and 32 harness
tests pass. The first compilation error and subsequent missing API inventory
entry are preserved alongside their successful repairs. All 1,310 Rust/Cargo
files match the verified snapshot and pinned Linux build. Independent journal
reconciliation confirms every offer, attempt, success, error, per-Cell count,
successful latency and complete ACK cohort for all five corrected cases.

Next: diagnose and fix 2,000-Cell joined closure/lease progress; lower fresh
verification and checkpoint I/O; separate native issuance from bounded
recoverable publication debt; then add ordered follower pipelining. Preserve
exact suffix recovery and safe collection contracts throughout. Full read,
mixed, Bucket and lifecycle/performance qualification remains required.

Raw sources, binaries, fixtures, failed attempts, journals and reconciliations
remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/base-cohort-20261009-*`.
The two new baseline cases reuse the immutable `bounded-history-20261008-candidate`
build; the external evidence inventory records those new files separately and
rehashes the earlier frozen inventory.

[Implementation](bundle-coverage-implementation.md) ·
[Capacity contract](../crates/cellule-runtime/docs/write-performance-design.md) ·
[Previous comparison](pr67-bounded-history-measurement.md).
