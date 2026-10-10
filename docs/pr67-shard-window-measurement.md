# PR 67: rejected sparse shard batching diagnostic

**The wide-read prototype is reverted. PR #67 remains a draft; performance parity
is unmet.** A fresh pair completes 510.13 Fleet writes/s for the retained runtime,
300.63/s for the prototype and 1,999.82/s for celld. Successful scheduled p99 is
1,314.72, 2,554.88 and 14.06 ms respectively. Fewer metadata requests did not
demonstrate an application performance benefit. One short pair establishes no
repeatable causal attribution.

## Experiment and disposition

The retained [native pipeline](pr67-native-pipeline-measurement.md) moves
publication admission before global ordering, pipelines eight original follower
rounds and uses follower group commit. It reuses private encoder-checked metadata
only after a complete fresh origin match. The [command-credit handoff](pr67-publication-admission-measurement.md)
also remains. Repeated base/history verification and checkpoint work are still
expensive; these existing changes do not establish sustainable publication.

Prototype `21029b5801a161d193c820714f6069c0820efee8` lets requested catalog shards
share sparse object ranges using the unused 4-MiB shard-phase raw-body allowance.
It authenticates only requested extents; padding grants no authority. The
combined selected encoded metadata preflight remains 4 MiB, history gaps retain
their 32-KiB limit and shard bodies join/drop before history I/O. Working memory,
concurrency, persisted formats and fresh dependency checks are unchanged.

The controlled 2,000-binding/64-Cell fixture falls from 49 reads to two in three
repetitions, with exact cold mutation and request-result recovery for all 64
Cells. Later unavailable original metadata still rejects preparation without a
new PUT. An initial 32-KiB-gap trial takes 11 reads and fails the original
at-most-five-read assertion; that assertion is never loosened. All 101 bundle
tests pass on the final prototype, but its application performance fails below.

Revert `d70530c9a004d0b24199c52db5253908033be14f` restores the production source
byte-identically to the preceding PR head `1f3d78d`. The new fixture, private
planner test and prototype source remain in external evidence; the rejected
optimization is absent from the delivered implementation. This is a measured
rejection, not a new performance improvement.

## Fresh application results

Same SQL ledger workload: 2,000 uniformly active Cells, 96-byte values, one owner
and two followers, WAL NORMAL/tmpfs, 128 clients and queue slots, 2,000 offered
writes/s, 30-second warmup and 60-second measured window. Baseline `cf4785c`
and prototype `21029b5` have byte-identical driver/auditor binaries, fixtures,
workload and pinned images, with recorded distinct serving binaries. Fresh
celld is `f2bf648663a610eefde71f3547ad61e9b896b1f0`. Builds, contributor checks
and independent journal replay do not overlap the timed windows.

| Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: |
| Retained Cellule, `cf4785c` | 510.13 | 1,314.72 | 61,426 | 27,966 |
| Rejected prototype, `21029b5` | 300.63 | 2,554.88 | 15,172 | 86,607 |
| Fresh celld, `f2bf648` | 1,999.82 | 14.06 | 0 | 0 |

TPS counts successful completions inside the measured window. Scheduled p99
includes measured successful offers through client drain and excludes failures;
it is not the all-attempt histogram. Successful request p99 is 532.86, 1,418.80
and 12.92 ms respectively. Prototype TPS falls 41.07% and scheduled p99 rises
94.33% in this pair. Its 183 trailing successes do not count toward in-window
TPS. Celld has 11 trailing successes. Every arm has measured successes on all
2,000 Cells.

Independent replay reconciles original successful outputs, payload bytes,
per-Cell counts, every planned/generated offer and attempt, trailing completions
and the complete ACK cohort. All measured and warmup Cellule errors are
`unavailable`, with zero `outcome_unknown`. Baseline warmup has 32,968 errors
and 2,622 drops; prototype warmup has 16,814 errors and 19,094 drops. Celld has
zero warmup errors or drops.

| Arm | Complete ACKs | Warm mutation / original retry audit | Cold audit / fleet drain |
| --- | ---: | --- | --- |
| Retained Cellule | 57,019 | 110 HTTP 503s; 56,909 retries checked | Cold not reached; timed drain result absent |
| Rejected prototype | 44,314 | All pass | All pass; 46.48 s |
| Fresh celld | 182,001 | All pass | All pass; 14.24 s |

Both Cellule owner logs contain zero fencing warnings and all 2,000 Cells report
Idle during original cleanup/drain. All original owner/follower containers exit
successfully without OOM. Baseline audit unavailability does not establish lost
data; it is still a failed availability/recovery gate. The prototype's passed
cohort audit does not qualify every failed-owner, transfer or collection schedule.

## Publication cost and next work

| Measured-window metric | Retained Cellule | Rejected prototype |
| --- | ---: | ---: |
| Node-authority range starts | 89,386 | 75,710 |
| Node-authority range bytes | 985,951,049 | 1,589,052,493 |
| Range bytes / successful in-window write | 32,212 | 88,095 |
| Immutable GET starts / bytes | 105,383 / 386,976,745 | 59,337 / 206,666,996 |
| Mean ordered-lock wait ms | 0.00068 | 0.00068 |
| Mean native submission ms | 0.14479 | 0.64885 |
| Separate mean Fleet-proof wait ms | 45.96 | 121.57 |
| Follower frames / sync, two members | 12.12 / 12.11 | 8.94 / 8.96 |

Native submissions have identical counts across all seven phases and their
nanosecond totals reconcile exactly in each arm. The Fleet-proof cohort is
separate and caller waits overlap; these means are not a serial TPS partition.
Range starts fall 15.30% but transferred range bytes rise 61.17%, even while
successful output falls. This supports the byte-amplification concern without
proving that one factor causes the entire regression.

Baseline unpublished native bytes grow 28.17→45.86 MB and retained runtime
memory 54.12→64.10 MB. Prototype native debt grows 33.86→42.57 MB and oldest
unpublished age 53.22→97.47 seconds. Its pending entries fall 5,632→2,135 and
memory 62.90→37.93 MB, which does not establish stable publication: native debt
and age still grow. Active Cell count remains 2,000 in both arms.

Prioritize a bounded authenticated append/lookup and checkpoint representation
that reduces total requests **and bytes per verified issued range**. Preserve
fresh origin/dependency checks and complete suffix/cold-recovery proofs; measure
checkpoint work separately from shared selection. Diagnose warm audit admission
under backlog with original errors and deadlines. Keep the existing ordering
and follower pipeline bounds until new evidence justifies changing them. Each
next candidate needs an unchanged corruption/recovery regression and a fresh
paired application measurement, not only a component read-count reduction.

## Verification and limits

The prototype passes all 13 contributor routes in an isolated snapshot: 1,995
reported workspace test/doctest executions (including child-process reporting),
38 ignored environment tests, 60 local LTX tests, Rust 1.97/1.99 Clippy,
targets, rustdoc and boundary/layout/document/contract/script gates. Its three
sparse fixture repetitions and 101 bundle tests also pass. The invalid initial
cache-overlap compilation attempt, controlled 49-read baseline failure and
11-read prototype failure are preserved, not counted as passing evidence.
The restored production source is identical to the preceding verified head;
final document gates cover only the report/design edits.

All canonical qualification reports remain false. The shared Docker VM has
eight CPUs and 8,306,286,592 total memory bytes across all roles; it does not
qualify a dedicated 8-vCPU/16-GiB owner. tmpfs does not qualify physical-media
durability. Cellule uses pinned mTLS/signed protobuf with a reused client per
member; celld's native fixture uses internal HTTP on loopback. These protocol
differences remain, and no transport-cost attribution is claimed. No new Bucket,
read-only or mixed result is claimed. Three matched repetitions of at least
five minutes and unchanged zero-error/drop, latency, recovery, read and debt
gates remain required.

Raw sources, builds, complete journals, audits, telemetry and passing/failed
attempts stay outside Git at
`/Volumes/Workspace/crabbuild-target/native-shard-window-20261009`.
