# PR 67: rejected checkpoint verification cohort

**The checkpoint prototype is reverted. PR #67 remains a draft; performance
parity is unmet.** A fresh Docker comparison completes 583.20 Fleet writes/s
for retained Cellule, 537.28/s for the prototype and 1,999.90/s for celld.
Successful scheduled p99 is 368.89, 367.03 and 14.48 ms respectively. The
component change establishes no acceptable application performance gain.

## Experiment and disposition

Prototype `634bd90e0a3415301f02b922d65e8e4c52a6db55` routes changed checkpoint
participants through the existing bounded base verifier used by bundle
selection. Up to eight fresh small-root operations overlap inside its original
4-MiB phase allowance and 20-MiB producer working budget. Larger roots keep
the serial fallback. Only the original changed participants are verified;
every required root and dependency must finish before catalog upload or CAS.
There is no across-call availability cache or persisted-format change.

The unchanged five-second controlled assertion fails on the baseline with
only one original dependency started. Both fixtures pass three repetitions
on the prototype with exactly eight overlapping dependencies, no ninth read,
no PUT after cancellation or missing required root bodies, and exact cold
mutation/retry recovery for all ten Cells per fixture. All 101 bundle tests pass.

Revert `41bc99e` restores production byte-identically to the preceding PR head
`6450734`. The original admission-before-ordering, eight-round follower pipeline,
follower group commit, fresh-origin encoder reuse and command-credit handoff
remain. The rejected source, fixtures and measurements stay in external evidence.

## Fresh application results

Same SQL ledger workload: 2,000 uniformly active Cells, 96-byte values, one
owner and two followers, WAL NORMAL/tmpfs, 128 clients and queue slots,
2,000 offered writes/s, 30-second warmup and 60-second measured window.
Retained runtime `cf4785c` and prototype `634bd90` have identical driver/auditor
binaries, fixtures, workload and pinned images. Fresh celld is
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. Builds, tests and independent replay
do not overlap timed windows; all original controllers join before analysis.

| Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: |
| Retained Cellule | 583.20 | 368.89 | 49,119 | 35,889 |
| Rejected checkpoint prototype | 537.28 | 367.03 | 52,127 | 35,522 |
| Fresh celld | 1,999.90 | 14.48 | 0 | 0 |

TPS counts completions inside the measured window. Scheduled p99 includes
measured successful offers through client drain and excludes failures. Successful
request p99 is 190.05, 217.50 and 13.36 ms respectively. Prototype TPS falls
7.87%; request p99 rises 14.44%. Its 114 trailing successes do not count toward
TPS; celld has six. Every arm has measured successes on all 2,000 Cells.
One short sequential pair establishes no repeatable causal attribution.

Independent journal replay reconciles all original outputs, payload bytes,
per-Cell counts, offers, attempts, trailing completions and complete ACK provenance.
Retained Cellule has 49,101 `unavailable` and 18 `outcome_unknown` measured errors;
all prototype errors are `unavailable`. Warmup errors/drops are 24,425/7,963,
24,447/7,894 and 0/269 respectively. Celld's warmup drops remain a failed gate.

| Arm | Complete ACKs | Warm mutation / original retry audit | Cold audit / fleet drain |
| --- | ---: | --- | --- |
| Retained Cellule | 64,605 | 1,652 HTTP 503s; 62,953 retries checked | Cold not reached; owner cleanup exceeds 120 s |
| Rejected prototype | 62,011 | All pass | All pass; 46.73 s |
| Fresh celld | 181,732 | All pass | All pass; 13.31 s |

Retained Cellule logs 18 owner fences with `Capacity("pending publication bytes")`;
active Cells fall 2,000→1,982. The prototype logs no owner fences and drains all
2,000 Cells to Idle. Audit unavailability and unknown outcomes do not establish
lost acknowledged data, but the baseline fails availability and draining gates.
The prototype's passed ACK cohort does not qualify the complete failure matrix.

## Remaining publication work

| Measured-window metric | Retained Cellule | Rejected prototype |
| --- | ---: | ---: |
| Node-authority range starts / bytes | 121,161 / 1,389,532,747 | 111,387 / 1,271,899,521 |
| Range bytes / successful in-window write | 39,710 | 39,455 |
| Immutable GET starts / bytes | 113,879 / 466,391,362 | 104,518 / 406,223,379 |
| Mean ordered-lock wait ms | 0.00034 | 0.00074 |
| Mean native submission ms | 0.13844 | 0.17528 |
| Separate mean Fleet-proof wait ms | 59.97 | 58.96 |
| Follower frames / sync, two members | 10.95 / 10.94 | 11.18 / 11.18 |
| Unpublished native bytes, start→end | 38,463,948→54,826,539 | 37,329,092→47,068,226 |

All seven native phase counts and nanosecond totals reconcile exactly. Fleet
proof waits are a separate overlapping cohort, not a serial throughput partition.
Publication bytes per successful write barely change. Retained runtime memory
grows 49,544,295→57,478,675 bytes; prototype memory grows 49,494,363→62,988,087.
Neither establishes stable debt at the offered load.

Both arms record zero shared root-packing cohorts. `materialize_bundle_with_due`
calls `prepare_recovered_overlay` directly, bypassing the existing bounded shared
publication producer. The next experiment should connect eligible exact recovered
overlays to that canonical producer, preserving lineage, proofs, byte/descriptor
admission, lifetime ownership, collection and joined shutdown. Measure total
publication requests and bytes per successful write, not only read concurrency.
Also reproduce and fix post-SQL publication capacity fencing without raising
limits; the fresh baseline shows the prior handoff has not eliminated that failure.

## Verification and limits

The prototype passes all 13 contributor routes in an isolated snapshot: 1,995
reported workspace test/doctest executions including child-process reporting,
38 ignored environment tests, 60 local LTX tests, Rust 1.97/1.99 Clippy, targets,
rustdoc and boundary/layout/document/contract/script gates. The invalid initial
cache-overlap compilation is preserved and is not passing evidence; the separate
controlled baseline reaches and fails both original assertions. Final document
gates cover the report/design edits after the byte-identical production restore.

All canonical qualification reports remain false. The shared Docker VM has eight
CPUs and 8,306,286,592 memory bytes across all roles; it does not qualify a
dedicated 8-vCPU/16-GiB owner. tmpfs does not qualify physical-media durability.
Cellule uses pinned mTLS/signed protobuf and celld internal HTTP on loopback;
no transport-cost attribution is claimed. No new Bucket, read-only or mixed
result is claimed. Three matched repetitions of at least five minutes and the
unchanged latency, zero-error/drop, complete recovery, read and debt gates remain.

Raw evidence stays outside Git at
`/Volumes/Workspace/crabbuild-target/native-checkpoint-cohort-20261009`.
