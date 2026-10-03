# Local RustFS performance comparison — 2026-10-02

Cellule and the pinned Celld lineage have workload-dependent costs. The controlled
operation-count finding is redundant publication of unchanged descriptor pages.
Reusing verified predecessor pages removes those writes and conflict-verification
reads. In five alternating long-history pairs, the median paired throughput gain
was **31%**, with pooled p95 command latency falling from **54.7 to 41.9 ms**.
This applies to uncompacted histories with several descriptor pages; small roots
do not receive the same savings. It is not a general service-capacity claim.

## Environment and boundaries

| Property | Recorded setup |
| --- | --- |
| Cellule source | `853b982b06f825d53855b7460ef54a98a40f26c6` plus benchmark changes; updated runs also include descriptor-page reuse. |
| Celld source | `10cb1303dac710dcb3b557e318e08c855261f68b`, the pinned import lineage, not a latest-upstream claim. |
| Host | Apple M2 Max, 12 CPUs, 32 GiB RAM, macOS 26.5.2; native ARM64 release builds, Rust 1.97.0. |
| SQLite / object_store | Cellule 3.49.1 / 0.14.2; Celld 3.45.0 / 0.12.5. |
| SQLite page size | 4 KiB. |
| Local scratch and builds | Mounted Workspace APFS volume; separate targets per runner and checkout. |
| RustFS | Dedicated container, bucket and Docker volume; loopback endpoint in a shared Colima VM with 8 CPUs and about 15.6 GiB RAM. |
| RustFS image | `ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`. |

The workstation and VM were shared. Some early local samples overlapped compilation;
broad correctness tests paused the comparison between processes. Task builds and
tests did not overlap the remote matrices or paired comparison. Timings varied
substantially between processes and between the two matrices; retain the distributions
and do not infer that payload entropy or this patch explains every timing difference.
No exclusive CPU/disk reservation or provider resource cap was applied.

| Path | Completion measured |
| --- | --- |
| Cellule local immediate | SQLite commit, LTX file sync and parent-directory sync. |
| Celld local default | SQLite commit and LTX file sync, without parent-directory sync. |
| Celld `sync-parent` | Runner adds directory barriers for a durability diagnostic. |
| Cellule batch 8 | Eight cuts share a durability barrier; throughput only, no independently durable command percentile. |
| Cellule remote | Commit, deferred capture, immutable root preparation, and cut cleanup. |
| Celld remote | Commit, immediate capture, and native L0 upload; cuts retained during the measured loop. |

Neither remote runner performs authority CAS, leases, node scheduling, HTTP, or
application acknowledgements. Cellule maintains authenticated indexes, directories
and exact roots; Celld uploads one LTX object per command. Those protocols and their
local durability work differ. Provider operation totals count storage API calls,
not hidden HTTP retries. Automatic compaction is absent from the append matrix.

## Method

130 independent processes completed and verified their restored payloads:
48 local baseline processes, 36 remote baseline processes, 36 updated remote
processes, and 10 paired long-history processes. Local processes warm one round
and measure three rounds of 128 inserts. Remote processes warm eight commands
and measure 128 more. Modes alternate, with reverse order every other process.
Both 4 KiB and 64 KiB payloads use the periodic fixture or the same reproducible
command-seeded high-entropy generator. Celld remote restore starts after deleting
its local database and capture directory; Cellule restores from roots after
closing the writer and pruning its cuts. Every run compares all restored values.
Local runners also check SQLite integrity and row count.

Command percentiles below pool nearest-rank per-command samples: 1,152 per local
row, 384 per remote row. Throughput is the median of per-process rates. Local
rates use commit plus capture time, including schema bootstrap; remote rates use
the measured serial command path. Fixture generation, bootstrap/warmup for remote
runs, post-run verification and restore are excluded from command throughput.
Percentiles are diagnostic samples, not production tails under offered load.

## Local capture and recovery

Production changes affect only replica metadata publication, so this local
matrix uses the original production implementation. Recovery includes input
verification, compaction, compacted-plan verification and restore for Cellule;
Celld includes its own compaction and restore verification. Recovery is a median
whole-round subtotal, not a single-command timer.

| Payload | Pattern | Mode | Commands/s | Command p50 / p95 ms | Recovery ms |
| --- | --- | --- | ---: | ---: | ---: |
| 4 KiB | periodic | Cellule immediate | 24.8 | 26.55 / 106.44 | 61.1 |
| 4 KiB | periodic | Celld default | 63.2 | 15.29 / 31.22 | 100.5 |
| 4 KiB | periodic | Celld sync-parent | 35.6 | 26.03 / 53.83 | 104.5 |
| 4 KiB | periodic | Cellule batch 8 | 111.6 | group barrier | 58.4 |
| 4 KiB | random | Cellule immediate | 26.4 | 29.39 / 97.80 | 84.7 |
| 4 KiB | random | Celld default | 44.1 | 16.27 / 55.28 | 164.7 |
| 4 KiB | random | Celld sync-parent | 24.7 | 29.01 / 84.19 | 350.8 |
| 4 KiB | random | Cellule batch 8 | 84.6 | group barrier | 100.6 |
| 64 KiB | periodic | Cellule immediate | 30.0 | 28.06 / 57.81 | 264.8 |
| 64 KiB | periodic | Celld default | 52.4 | 16.27 / 44.43 | 330.3 |
| 64 KiB | periodic | Celld sync-parent | 28.4 | 27.93 / 59.64 | 278.4 |
| 64 KiB | periodic | Cellule batch 8 | 94.3 | group barrier | 190.6 |
| 64 KiB | random | Cellule immediate | 31.7 | 27.48 / 49.45 | 246.9 |
| 64 KiB | random | Celld default | 49.7 | 16.08 / 33.93 | 333.2 |
| 64 KiB | random | Celld sync-parent | 28.1 | 28.46 / 72.50 | 255.5 |
| 64 KiB | random | Cellule batch 8 | 102.7 | group barrier | 280.0 |

For high-entropy Cellule immediate captures, file and directory sync phases
accounted for roughly 90–98% of capture time across the nine measured rounds.
At 4 KiB the median durable command cost is close to Celld with directory syncs;
Celld default pays fewer barriers. Grouped Cellule barriers improve throughput,
but change when individual cuts may be acknowledged. The embedding runtime
already uses deferred capture behind its stronger external publication proof.

## Updated build against Celld on RustFS

These are the fresh alternating remote runs after the production fix. The
root-preparation/upload column isolates remote work from local capture and commit.
Restore is the median of three post-run samples from a warm provider/process;
Celld uses one download slot while Cellule retains default Host admission. It is
not a matched cold-recovery or recovery-concurrency qualification.

| Payload | Pattern | Mode | Commands/s | Command p50 / p95 ms | Prepare/upload p50 ms | Restore ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 4 KiB | periodic | Cellule root proposal | 64.0 | 14.81 / 25.84 | 13.37 | 100.2 |
| 4 KiB | periodic | Celld native L0 | 46.2 | 21.09 / 33.19 | 7.18 | 436.7 |
| 4 KiB | periodic | Celld L0 + sync-parent | 29.8 | 30.17 / 55.87 | 5.51 | 500.8 |
| 4 KiB | random | Cellule root proposal | 32.2 | 22.19 / 65.60 | 20.18 | 256.5 |
| 4 KiB | random | Celld native L0 | 31.6 | 25.59 / 71.40 | 11.27 | 621.9 |
| 4 KiB | random | Celld L0 + sync-parent | 27.2 | 32.02 / 101.42 | 6.37 | 788.1 |
| 64 KiB | periodic | Cellule root proposal | 16.3 | 53.95 / 130.85 | 50.88 | 370.9 |
| 64 KiB | periodic | Celld native L0 | 23.2 | 31.32 / 82.28 | 16.31 | 1735.1 |
| 64 KiB | periodic | Celld L0 + sync-parent | 19.6 | 41.67 / 130.67 | 14.15 | 1691.1 |
| 64 KiB | random | Cellule root proposal | 18.7 | 44.83 / 116.16 | 40.89 | 551.4 |
| 64 KiB | random | Celld native L0 | 22.4 | 39.59 / 112.43 | 22.73 | 2317.6 |
| 64 KiB | random | Celld L0 + sync-parent | 16.4 | 45.56 / 132.37 | 16.80 | 2371.5 |

The original remote matrix measured Cellule at 32.3 / 53.8 commands/s for
4 / 64 KiB high-entropy writes, against Celld default at 39.7 / 42.4. The updated
matrix measured 32.2 / 18.7 versus 31.6 / 22.4. This change in provider conditions
is too large to attribute the across-matrix difference to this patch. Use the
alternating paired experiment below for the patch effect.

## Verified descriptor-page reuse

Each pair ran 384 high-entropy 4 KiB inserts and retained commands 288–383
(96 samples per process), after three complete predecessor descriptor pages.
Five alternating pairs retain 480 command samples per implementation. The
first matrix already exposed a conflict PUT followed by a full GET for every
unchanged page on every append. The fix compares the encoded page digest with
the origin-checked predecessor pages and uploads only new metadata.

| Long-history metric | Before | After |
| --- | ---: | ---: |
| Median process commands/s | 31.3 | 46.2 |
| Command p50 ms | 25.11 | 19.72 |
| Command p95 ms | 54.71 | 41.94 |
| Prepare p50 ms | 23.59 | 18.20 |
| Prepare p95 ms | 51.74 | 39.09 |
| PUT API attempts / command | 10.04 | 7.02 |
| GET API calls / command | 3.02 | 0.00 |
| GET bytes / command | 134256 | 0 |
| Predecessor HEADs / command | 5.01 | 5.01 |

The ratio of process medians is 1.47x; the median of the five paired throughput
ratios is **1.31x**. Individual pair ratios range from 1.05x to 1.82x. Pooled
p50 and p95 reductions are 21% and 23%. Provider requests are the stronger
evidence: roughly three duplicate PUTs and three GETs disappear per command,
while the same five predecessor HEAD checks remain. Byte formats, object paths,
root contents, admission limits, and authority semantics are unchanged. Normal
scheduled compaction may keep roots below this history size, so this saving
should not be applied to every production command.

The regression test failed before the patch with seven PUTs instead of five
for an append to two complete descriptor pages. After the patch it verifies
five PUTs, zero conflict-verification GETs, all three predecessor HEADs, exact
restore, and rejection before any upload if a reused page disappears at origin.
The same reuse path serves append and representation-only compaction.

## Verification and further work

Passed in an isolated source snapshot: LTX tests with all features and without
default features; LTX Clippy for all targets/features with warnings denied;
workspace all-target/all-feature locked check; LTX API docs with warnings denied;
and the existing replica-cost writer-history/restore process tests. Formatting,
boundary/layout, document fence/link, and SQL/peer contract checks passed.
Six manual/provider tests remain ignored by the standard replicated suite;
cloud/fault qualification and the full workspace test suite were not run.

| Next work | Evidence and required experiment |
| --- | --- |
| Keep root histories bounded | Existing scheduled compaction limits descriptor history. Measure the public runtime with compaction enabled, including its provider writes and latency spikes. |
| Reduce metadata publication cost | Cellule still performs multiple immutable writes versus one native Celld L0 upload. Test existing bundle/group paths at the host boundary before changing compatibility contracts. |
| Qualify sparse activation and first writes | This matrix measures a fresh writer. Use the existing sparse/hydrated/resumed churn fixtures on larger roots; include checksum-sidecar preparation and write-first origin faults. |
| Establish supported throughput and tails | Run matched fixed-resource workers and a dedicated provider with multiple Cells, sustained arrivals, authority CAS, compaction, memory/CPU counters, and unaffected resident Cells. |

## Reproduction and retained evidence

Use [the RustFS runner](../run-rustfs.sh) and [matrix driver](../compare.py),
following [the harness guide](../README.md). Build original and updated binaries
into separate targets to reproduce the paired experiment. On each binary use
`--commands 384 --warmup 288 --payload-bytes 4096 --random-payload`, the same
RustFS endpoint and bucket, and five alternating independent process pairs.

Raw JSON, per-process stderr, summaries, binary hashes, RustFS/Docker inspection,
compiler/host versions, original production hashes, the paired runner, source
patch and verification logs are retained at:
`$HOME/Workspace/crabbuild-target/cellule-ed2a-ltx/evidence/`.
`comparison/` contains the original 84-process matrix; `comparison-after/` the
updated 36-process remote matrix; `before-after/` the paired long-history data.
The original coordinator metadata explicitly distinguishes its later checkout
diff from the baseline binaries it executed. Provider test storage is removed
after measurements; raw evidence and the verification source snapshot remain.
