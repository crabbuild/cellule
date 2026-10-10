# PR 67: recovered-overlay shared publication experiment

The shared-producer integration is **rejected and reverted**. Fresh matched Fleet
throughput falls **537.38→501.40 writes/s**; successful scheduled p99 rises
**371.96→383.70 ms**. Shared publication activates, but only 58 of 1,450 admitted
Cells share an object and 1,005 preparations fall back under memory pressure.
Total observed object-store bytes per successful write rise **74,318→78,018**.
Both Cellule arms fail warm availability checks. This establishes no performance
parity; PR #67 remains a draft.

## Source and implementation

| Role | Revision |
| --- | --- |
| Fresh retained Cellule binary | `cf4785c675698afe786f566242d6d3dace32e775` |
| Current baseline | `9f7738a94770910f896d1d61bd3bed2abcda6372`, identical Rust/Cargo production source |
| Rejected prototype | `c2c0ef10b08b9844401ca9e11397e9321a744f91` |
| Reversion | `ce3e8d6`, restores the complete baseline crate/Cargo tree |
| Fresh celld | `f2bf648663a610eefde71f3547ad61e9b896b1f0`, pinned image |

The prototype extracts the canonical recovered-bundle input reader, routes
eligible materialization through the existing shared producer, retains original
chain facts before coalescing and preserves lineage and exact endpoint checks
before fenced Cell CAS. A scoped host token retains pre-admitted working memory
through cancelled native input preparation. Large or memory-constrained inputs retain
direct recovery preparation. Original object, row, memory and scheduling bounds
remain unchanged. No copied celld code, persisted format change or weaker proof
is delivered. Rejected source remains in Git history and external evidence.

The comparison remains celld's [ordered shipping](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4530),
[follower group commit](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L179)
and [new-entry publication](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L6628).
Cellule retains the preceding admission-before-ordering, eight ordered follower
rounds, fresh-origin encoder reuse and completed-command credit transfer.

## Fresh application measurement

One owner, two followers, 2,000 uniform active Cells, 96-byte SQL-ledger values,
WAL NORMAL/tmpfs, 128 clients and queue slots, 2,000 offered writes/s, 30-second
warmup and 60-second measured window. Driver and auditor binaries are byte
identical; workload, fixtures and pinned images match. The arms run sequentially.
Builds, tests and replay do not overlap timed windows; original controllers join
before independent journal analysis.

| Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: |
| Retained Cellule | 537.38 | 371.96 | 61,556 | 26,201 |
| Rejected prototype | 501.40 | 383.70 | 51,127 | 38,789 |
| Fresh celld | 1,999.78 | 14.52 | 0 | 0 |

TPS counts completions inside the measured window. P99 covers successful
measured offers through client drain and excludes failed requests. Successful
request p99 is 201.93/214.82/13.36 ms; all-attempt scheduled p99 is
328.47/389.81/14.52 ms. Cellule has no trailing successful completions; celld has
13, excluded from TPS. All 2,000 Cells have measured successes in every arm.
Warmup errors/drops are 28,113/6,132, 26,104/7,602 and 0/1,602 respectively.
One short sequential pair establishes no repeatable causal attribution.

Independent full-journal replay reconciles every offer, counter, original output,
payload, per-Cell count and ACK provenance. Retained Cellule's 59,999-ACK warm
audit fails 1,152 HTTP 503 checks; the prototype's 58,379-ACK warm audit fails 45.
Neither reaches cold audit. Both log all 2,000 Cells drained to Idle and all three
processes exit zero during cleanup; neither logs an owner fence or an unknown
write outcome in this run. Cleanup success does not replace a passing audit.
Celld passes all 180,399 warm/cold mutations and original retries and drains in
12.29 seconds. These facts do not establish lost acknowledged data or a
qualified failure/transfer/collection matrix.

## Observed work and remaining architecture gap

| Measured-window metric | Retained | Prototype |
| --- | ---: | ---: |
| Shared root cohorts / Cells | 0 / 0 | 29 / 58 |
| Shared singletons / pressure fallbacks | 0 / 0 | 1,392 / 1,005 |
| Immutable GET bytes | 433,418,483 | 385,574,161 |
| Immutable PUT bytes | 27,035,528 | 25,253,095 |
| Node-authority range bytes | 1,113,485,391 | 1,160,104,166 |
| Node-authority PUT bytes | 428,672,754 | 405,013,158 |
| Node-authority total bytes/success | 59,837 | 64,163 |
| All store read+write bytes/success | 74,318 | 78,018 |
| Native publication debt bytes, start→end | 30,614,543→46,599,220 | 35,265,911→43,058,711 |
| Runtime retained bytes, start→end | 49,099,899→64,629,624 | 66,781,638→64,956,731 |

Storage totals include every observed operation in each family. These are window
cost ratios, not isolated operation costs. Node-authority traffic accounts for
about four-fifths of total observed store bytes. Only 4% of admitted shared Cells
actually share an object; reducing the small immutable-upload component does
not remove catalog/history and root-checkpoint work. Debt grows in both arms.

Mean ordered-lock waits are 0.00085/0.00056 ms. Follower batches average
11.80–11.82/10.70–10.71 input frames per sync, with zero follower append failures.
Fleet proof waits average 52.84/62.28 ms in separate populations. The retained
submission phase populations differ by one at the sample boundary, so no exact
closed partition is claimed for that arm; the prototype's counts and nanoseconds
reconcile. The failed first analysis and corrected interpretation are preserved.

Next work must reduce authenticated catalog and checkpoint work per command:
compose ready root checkpoints with native selection under one bounded fresh
catalog verification and fenced CAS, preserve exact original range/lineage
proofs, and improve materializer admission and cohort density using existing
credits. Avoid repeated historical reads within the same verified operation;
retain fresh dependency verification and complete issued-suffix recovery. Fix
warm read/retry availability under backlog and measure each candidate before
retaining it. Shared packing alone has not delivered the required architecture
or sustainable throughput.

## Verification and limits

The baseline regression reproduces zero shared-producer entries after exact cold
recovery. Three candidate repetitions pass all eight Cells and memory-pressure
fallback; public LTX tests enforce scope, endpoint, original-chain admission and
byte-identical cold restore. The host cancellation test, all 103 bundle tests,
shared-publication tests and all 13 isolated contributor routes pass. Workspace
logs report 1,998 test/doctest executions including child-process reporting,
38 environment ignores and 60 local LTX tests. Invalid fixture and compilation
attempts are preserved separately. Reversion restores previously verified
production byte-identically; final document checks cover the delivered report.

All canonical qualification reports are false. The shared 8-CPU/8.3-GB Docker VM
does not qualify a dedicated 8-vCPU/16-GiB owner; tmpfs does not qualify
physical-media durability. Cellule mTLS/signed protobuf and celld loopback HTTP
differ. New Bucket/read/mixed measurements, three paired ≥5-minute repetitions,
zero errors/drops, tail targets, complete recovery/transfer/collection evidence
and stable debt remain required. No qualification profiles, deadlines, budgets or
expected assertions are weakened. Raw evidence stays outside Git at
`/Volumes/Workspace/crabbuild-target/native-shared-overlay-20261009`.
