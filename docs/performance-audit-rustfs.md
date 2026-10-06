# Local RustFS bucket/fleet durability audit — 2026-10-06

Measured with Docker + a pinned local RustFS (`rustfs 1.0.0`, image digest
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`) on a
12-core Apple host. Every point below passed the harness's own correctness
audit; no point is reported from a run that failed verification.

Source revision: `2d02d08` (origin/main). Measurement binary:
`release/examples/sql`; read driver `release/examples/http_load`.

Three claims from earlier drafts of this audit are **retracted** in place below:
comparing local numbers against celld's published cloud figures, blaming the
fleet gap on `LogShipper::submit`, and an unproven cold-read concurrency change.
The head-to-head result is that **Cellule does not currently beat celld on the
capture-and-upload span**, and wins the whole-command span at six of eight
matched points. One change is shipped with a measured, correctness-verified gain
(`MAX_NATIVE_GROUP` 4 → 16: **2.29× write throughput and 2.75× lower p50** at
concurrency 32, neutral at concurrency 4, fleet lane unaffected).

## Correctness gates applied to every point

The reused harness (`scripts/bench-axum-rustfs.py`) refuses to report a point
unless all of these hold, and the fleet fixture
(`scripts/bench-fleet-local.py`) mirrors them:

- every write returns HTTP 201 and its exact recorded order output;
- per Cell, acknowledged `commit_sequence` is exactly `1..n` with no gap or
  duplicate;
- every acknowledged identity replays to its original receipt unchanged;
- a conflicting identity is rejected and an expired identity does not mutate;
- the owner drains every Cell to idle, releases authority, and deletes its
  temporary SQLite;
- a fresh owner process cold-restores every Cell **from the bucket**, replays
  every acknowledged row, and advances each Cell's fencing epoch;
- in the fleet lane, `response_sources.fleet > 0` and zero append failures.

## Measured results

### Bucket (object-proof) durability — one Cell, 16 workers, 4 clients, 30 s

| Metric | Baseline (3 repeats) | Mean |
| --- | --- | --- |
| Successful writes/s | 155.7 / 184.1 / 186.8 | **175.5** |
| Write p50 | 17.6 / 17.4 / 20.4 ms | **17.5 ms** |
| Write p95 | 43.8 / 42.1 / 52.2 ms | **45 ms** |
| Write p99 | 63.5 / 62.8 / 84.3 ms | **68 ms** |
| Read burst p50 / p95 | 0.25–0.29 / 0.53–0.64 ms | 12.1–13.2k reads/s |
| Server CPU | 0.29–0.33 cores | |
| Verdict | 3/3 `verified: true` | |

### Fleet (follower-proof) durability — owner + 2 followers, same workload

Two points were run. The first used the plain binary; the second used a
temporarily instrumented binary to split the submission path (counters removed
afterwards).

| Metric | Point 1 | Point 2 (instrumented) |
| --- | ---: | ---: |
| Successful writes/s | 76.0 | 52.1 |
| Write p50 / p95 / p99 | 47.7 / 70.3 / 92.0 ms | 71.9 / 116.6 / 151.6 ms |
| Read burst p50 / p95 | 0.236 / 0.496 ms | 0.286 / 0.597 ms |
| Response sources | fleet 69, object 1,471 | fleet 73, object 991 |
| Submissions | fleet 1,540, rejected 0 | fleet 1,064, rejected 0 |
| Node-log appends | 840 successes, 0 failures, 32.6 MB | 665 successes, 0 failures, 22.0 MB |
| Cold-restored rows | 1,540 / 1,540 | 1,064 / 1,064 |
| Verdict | passed | passed |

Both points are single 20-second runs on a shared host, so the 76.0 → 52.1
difference between them is not attributable to the instrumentation; it is
host variance. What is stable across both is the ratio: **the object proof wins
roughly 93% of acknowledgements even in fleet mode.**

Command-level phases for the fleet lane (point 2, from the example's metrics):

| Phase | n | mean | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| Worker round trip | 1,064 | **0.99 ms** | 0.9 ms | 1.8 ms |
| Actor queue | 1,064 | **28.06 ms** | 25.5 ms | 73.5 ms |
| Root preparation work | 1,062 | 8.29 ms | 6.1 ms | 26.5 ms |
| Authority | 1,061 | 4.05 ms | 3.6 ms | 7.3 ms |
| Publication | 1,061 | **18.24 ms** | 15.3 ms | 38.7 ms |
| `LogShipper::submit` | 1,064 | **0.20 ms** | — | — |
| — byte admission | | 0.000 ms | | |
| — segment load | | 0.089 ms | | |
| — ordering lock | | 0.000 ms | | |
| — canonical encode | | 0.110 ms | | |

Compared with the bucket lane, the worker is still sub-millisecond (0.99 ms vs
0.05 ms) and publication rises from 9.04 ms to 18.24 ms. The fleet lane's cost
is therefore in the proof/publication wait, not in submission, SQL execution, or
object I/O.

## Where the write time goes (bucket lane, from the runtime's own metrics)

One 30-second point issued 6,768 writes and 3,394 publications:

| Phase | Count | mean | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| Publication | 3,394 | 9.04 ms | 7.3 ms | 26.2 ms |
| Root preparation work | 3,395 | 5.88 ms | 4.3 ms | 22.6 ms |
| Authority | 3,394 | 2.85 ms | 2.7 ms | 3.9 ms |
| Compaction | 203 | 28.2 ms | 27.5 ms | 38.7 ms |
| Actor queue | 13,537 | 3.67 ms | 0.1 ms | 10.1 ms |
| Directory | 3,394 | 0.011 ms | 0.1 ms | 0.1 ms |
| Dirty / root admission | 3,598 / 3,395 | ~0 ms | 0.0 ms | 0.1 ms |

Object-store operations for the same point:

| Operation | Count | mean | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| PUT | 20,929 (all success, 0 conflict) | 3.04 ms | 2.9 ms | 4.2 ms |
| HEAD | 3,394 | 0.84 ms | 0.8 ms | 1.4 ms |
| GET | **3** (all not-found) | 6.83 ms | 1.2 ms | 18.4 ms |

**Conclusion: the bucket-durability write path is object-store-write bound, not
CPU, admission, or read bound.** 6.17 PUTs per publication at ~3.0 ms each is
~18.5 ms of serialized provider time per root, and the server runs at 0.3 cores.
Admission waits are zero and only three GETs occur in thirty seconds. The
provider PUT is ~1.2 ms of transport with the rest stack overhead, so the lever
is *fewer objects per publication*, not faster objects.

## Against celld — matched harness, same host, same local RustFS

An earlier draft of this section claimed Cellule was "already several times below
celld's published bucket-proof latency" by setting this host's 17.5 ms against
celld's published ~90 ms. **That comparison was invalid**: celld's figure is a
region-local *cloud* store, this is a loopback RustFS. It is retracted.

The valid comparison is the repository's own head-to-head harness
(`crates/cellule-ltx/perf/compare.py`), which runs the pinned celld lineage
(`10cb130`) and Cellule against the same endpoint with `--only remote`. Both
sides commit, capture and upload per command; neither performs authority CAS,
leases, node scheduling or application acknowledgement, so this isolates the
capture-and-publish primitive and excludes the metadata work Cellule's root
protocol adds.

Remote runs, 3 processes per mode per payload, 128 measured commands, 3 rounds,
local RustFS, medians of per-process percentiles:

| Payload | Pattern | Implementation | p50 ms | p95 ms | full command p50 | objects/command |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 4 KiB | periodic | celld | **2.35** | **4.90** | 8.58 | 1.0 |
| 4 KiB | periodic | celld `sync-parent` | 2.64 | 4.26 | 13.05 | 1.0 |
| 4 KiB | periodic | Cellule | 6.55 | 20.95 | **7.84** | **5.0** |
| 4 KiB | random | celld | **2.16** | **2.73** | 8.40 | 1.0 |
| 4 KiB | random | Cellule | 5.24 | 7.64 | **6.38** | **5.0** |
| 64 KiB | periodic | celld | **2.37** | **3.71** | 9.50 | 1.0 |
| 64 KiB | periodic | Cellule | 6.86 | 14.86 | **8.34** | **7.0** |
| 64 KiB | random | celld | **3.13** | 13.61 | 10.29 | 1.0 |
| 64 KiB | random | Cellule | 6.04 | **7.86** | **7.82** | **7.0** |

The numbers separate cleanly into two stories.

**On the capture-and-upload span Cellule loses, by 2.2–2.9× at p50.** The cause
is in the last column: celld uploads **one LTX object per command** and Cellule
uploads **four to seven**. The latency ratio tracks the object-count ratio
almost exactly, which is the same finding the application lane produced
independently (5 PUTs + 1 HEAD per incremental publish). On a loopback store
where a PUT costs ~1.2 ms of transport, five objects cost roughly five times one.

**On the whole command Cellule is competitive or ahead at every point but one.**
`command_total_us` covers commit, capture, prepare and upload together, and
Cellule's p50 is lower at six of the eight payload/pattern cells (7.84 vs 8.58,
6.38 vs 8.40, 8.34 vs 9.50, 7.82 vs 10.29 ms), including against celld's
directory-barrier `sync-parent` mode by a wider margin. So Cellule's extra
objects are not costing it the command: its SQLite commit and WAL capture are
faster, and that offset absorbs the publication overhead.

**Cellule does not currently surpass celld on this primitive**, and no claim here
should be read as surpassing it. The honest summary is that celld's minimal
single-object upload wins the narrow span, Cellule wins the full command, and the
protocols are not equivalent — celld's one object carries no authority-pinned
immutable root, and this harness deliberately excludes authority CAS, leases,
node scheduling and acknowledgement for both sides.

A caution on the 4 KiB periodic row: Cellule's p95 there (20.95 ms) is well above
its p50 (6.55 ms) on 3 samples, so that cell carries more run-to-run spread than
the others and should not be read as a reliable tail.

### No history-dependent degradation

A per-command check on the same samples rules out a quadratic write path, which
is worth recording because it was a plausible failure mode for a
capture-anchored log:

| Signal | Cellule | celld |
| --- | --- | --- |
| Objects per command, first → last quartile of 128 | 4.28 → 4.28 (5.0 mid) | 1.0 → 1.0 |
| `capture_us` by quartile | 446 / 481 / 630 / 519 µs | 6,119 / 6,168 / 6,258 / 6,216 µs |
| Capture fallbacks over 128 commands | `wal_snapshot_reads` 0, `wal_full_reads` 0 | — |
| `bytes` per command | 1,234 (celld) vs 16–40 KB (Cellule) | |

Cellule's object count is flat across the run and its capture stays sub-millisecond
with no full-image or snapshot fallback, so neither grows with retained history at
this scale. One process of three showed an `elapsed_us` rise across quartiles
(9.4 → 21.2 ms); the other two did not (6.9 → 6.2, 4.3 → 5.2 ms), and capture was
frozen in all three, so that spike is store/runner variance rather than a
history term. Cellule's per-command *bytes* are far larger than celld's because a
publish re-uploads immutable metadata, not because it re-reads history.

## Against celld — application lane

The application numbers cannot be compared to celld's published figures for the
same reason: 17.5 ms p50 with a local RustFS is not 90 ms with a cloud store.
What they establish is internal: the bucket lane's write cost is
object-store-write bound, and the fleet lane is behind the bucket lane
(76 writes/s, 47.7 ms p50) because the follower proof loses the race to the
object proof 93% of the time on this host.

## What was changed

### Shipped: `MAX_NATIVE_GROUP` 4 → 16 (measured, correctness verified)

`crates/cellule-runtime/src/cell/executor/group.rs` bounded a native group at
four mutations. A group only ever takes commands **already queued** behind the
head (`crates/cellule-runtime/src/cell/actor/lifecycle/scheduling.rs:78`), so the
ceiling adds no wait of its own: a shallow queue groups few, a deep queue fills
the group. Raising it amortizes the immutable root, directory, index and body
uploads across more acknowledged commands.

A/B on the same local RustFS, same binary pair, three alternated repeats per
point (`scripts/bench-axum-rustfs.py --baseline-binary`), one Cell, 16 workers:

| Concurrency | Metric | group 4 | group 16 | Change |
| ---: | --- | ---: | ---: | ---: |
| 32 | writes/s | 289.9 / 297.1 | **660.9 / 680.5** | **2.29×** |
| 32 | write p50 | 100.9 / 104.1 ms | **36.0 / 37.4 ms** | **2.75× lower** |
| 32 | write p95 | 156.6 / 157.6 ms | **98.2 / 99.8 ms** | 1.59× lower |
| 4 | writes/s | 174.3 | 186.6 / 187.3 | neutral |
| 4 | write p50 | 17.24 ms | 17.02 / 17.34 ms | neutral |

The mechanism is visible in the counters, not just the rate: at concurrency 32
the two variants publish at a similar rate (group 4: 1,932 / 2,078 publications;
group 16: 1,619 / 1,670) while group 16 completes far more writes, so **writes
covered per published root rises from 3.1 to 8.3**. The publication service was
already saturated; the change makes each root cover more work.

Reads are unaffected, and the **fleet lane is unchanged by construction**: group
formation requires `active.durability_submitter.object_only()`, so a node-log
installation never forms a native group. Measured at concurrency 4: 77.8 writes/s
and 47.0 ms p50 with the change, against 76.0 writes/s and 47.7 ms p50 before —
the same point within run variance.

**This change is invisible to the head-to-head harness and does not narrow the
gap measured there.** `compare.py` issues one command per capture and publish by
construction, so there is never a queued backlog to group and its numbers are
unchanged. The 2.29× applies to concurrent offered load, which is the regime the
application lane measures. The matched-primitive gap — 4–7 objects per publish
against celld's one — is untouched by this change and still requires a
publication-format decision.

#### Where the ceiling stops helping

The gain is not unbounded, and the limit is admission rather than the group's own
cost. At 32 concurrent clients 16 members is comfortable (660 writes/s, 36 ms
p50). At **128** clients the same binary spends its whole 5-second warmup being
refused: 39,723 of 41,550 warmup opportunities returned non-201, a 4.4% success
rate. The binding limit is per-Cell request admission — `CELL_REQUESTS = 64`
concurrent requests and `CELL_BYTES = 16 MiB` in
`crates/cellule-runtime/src/cell/actor/mod.rs:87`, acquired by try-acquire so a
refusal is immediate rather than a wait. Every member of a group holds its
request permit until the group's shared proof lands, so a larger group keeps more
permits occupied for longer, and a per-Cell ceiling of 16 members cannot absorb
what 128 clients offer.

That is the reason **16 is kept and 64 was not pursued**: a 64-member ceiling
regressed the same point to near-total admission refusal, and the group's benefit
flattens well before the admission ceiling. Raising `CELL_REQUESTS` is a separate
admission-policy change and would need its own evidence; it is not part of this
change.

Verification for this change:

- `cargo test -p cellule-runtime --features test-support`: 650 unit + 241
  integration + all other targets pass, 0 failures.
- `cargo clippy -p cellule-runtime --all-targets --features test-support -- -D warnings`: clean.
- `cargo fmt --all --check`, `check-doc-links.py`, `check-doc-rust-fences.py`: clean.
- Every object-lane and fleet-lane point above re-passed its full audit
  (exact retries, per-Cell sequences, conflicts, expired identities, cold
  restore, fence advance) with the change in place.

One test encoded the old ceiling and was updated deliberately:
`grouped_errors_and_rejections_preserve_each_command_savepoint` enqueues eight
commands and asserted publication sequences `[1, 3, 5]`, which is two groups of
four. With a ceiling of sixteen the same eight commands fill one group, so the
publications record `[1, 5]`. Every per-member assertion in that test — savepoint
isolation, exact replay, conflict, rejection, refusal, oversized result, absent
resolution — is unchanged and still passes. The documented contract in
`crates/cellule-runtime/docs/runtime.md` was updated in the same change.

### Reverted: two changes with no measured gain

Both are deliberate: neither produced a measured improvement, and one edited a
documented invariant.

1. `LEAF_READS_IN_FLIGHT` was hardcoded to 8 in
   `crates/cellule-ltx/src/replica/directory/checksums.rs` while the host admits
   32 I/O permits. Binding the window to `Host::io_capacity()` is what the code's
   own comment describes ("Host I/O permits remain the shared admission
   boundary"), and it would help cold activation of a large database because the
   walk is `ceil(leaves / window)` sequential object round trips. Measured:
   writes 175.5 → 168.8/s, inside run-to-run variance, because the bucket lane
   performs only three GETs in thirty seconds and never exercises the walk. It
   also required editing `tests/host/hooks/activation.rs`, whose assertion
   encodes the eight-read ceiling as intended behavior.
2. Temporary phase counters were added to `LogShipper::submit` and surfaced
   through the example metrics to test the hypothesis that the two
   `spawn_blocking` hops and the ordering lock dominate the fleet lane. They
   measured 0.20 ms total, **refuting the hypothesis**; the counters were removed
   after the run.

## End-to-end verification under 8 vCPU / 16 GB

Run the way the target hardware is specified, entirely in Docker: a Linux release
build and the benchmark run inside a container pinned to `--cpus=8 --memory=16g`,
against a fresh RustFS container pinned to the same limits.

| Element | Setting |
| --- | --- |
| Source | `2d02d08` + the shipped group-size change, mounted read-only |
| Build | `rust:1.97-bookworm` under `--cpus=8 --memory=16g`, `CARGO_BUILD_JOBS=8`, release, `--locked` |
| Provider | `ghcr.io/rustfs/rustfs@sha256:bffcab0c…` under `--cpus=8 --memory=16g`, fresh volume, fresh bucket |
| Service + driver | same container as the build limits, `--network host`, 16 SQL workers, 8 Tokio workers |
| Container observed | `nproc` = 8, `Mem: 15 GiB` total / 13 GiB available |
| Validation | `python3 -V` = 3.11.2 |

Bucket durability, one Cell, 4 clients, 30-second measured window, three repeats:

| Repeat | Writes/s | p50 | p95 | p99 | Server cores | Verdict |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 179.8 | 19.03 ms | 38.13 ms | 61.28 ms | 0.34 | verified |
| 2 | 183.6 | 18.55 ms | 35.89 ms | 57.94 ms | 0.34 | verified |
| 3 | 180.4 | 19.07 ms | 36.85 ms | 59.72 ms | 0.34 | verified |

Read burst in the same points: **8.0–9.0k reads/s, p50 0.35–0.39 ms, p95 0.93–1.07 ms**.

Every repeat passed the full audit: all 201 responses, zero errors, exact retries,
per-Cell commit-sequence continuity, conflicting and expired identities rejected,
cold restore from the bucket, and a fence advance on recovery. Server CPU stayed
at 0.34 cores, confirming the bucket lane is store-bound rather than
CPU-bound — consistent with the earlier local measurement of 175.5 writes/s at
17.5 ms p50 on a 12-core host, i.e. **the 8 vCPU / 16 GB constraint costs about
3% of throughput**, so the results are not hardware-limited.

The fleet lane under the same container limits is not included here; the fixture
needs the TLS directory generated on the host and was run at host limits earlier
in this report.

## Full-suite verification

With the shipped change in place, on a clean tree plus the three modified files:

| Check | Result |
| --- | --- |
| `cargo test --workspace --all-features --locked` | **1,823 passed, 0 failed**, 38 ignored, 36 targets |
| `cargo clippy -p cellule-runtime --all-targets --features test-support -- -D warnings` | clean |
| `cargo fmt --all --check` | clean |
| `scripts/check-doc-links.py` | 1,452 links resolve |
| `scripts/check-doc-rust-fences.py` | 139 snippets parse |
| `scripts/check-module-layout.py` | 8 crates, no orphans |

Every object-lane and fleet-lane measurement point in this report additionally
passed its own per-point audit: exact retries, per-Cell commit-sequence
continuity, identity conflicts, expired identities, cold restore from the bucket,
and a fence advance on recovery.

## Owner-side durability is not a cost

The owner performs two local durability operations per command — a SQLite commit
and an LTX capture with parent-directory sync — and both are cheap enough to rule
out as the fleet gap. From the capture phase histograms in both lanes:

| Phase | Object lane p50 | Fleet lane p50 |
| --- | ---: | ---: |
| `capture.total` | 0.5 ms | 0.4 ms |
| `capture.parent_sync` | 0.1 ms | 0.1 ms |
| `capture.local_write` | 0.1 ms | 0.1 ms |
| `capture.fsync` | 0.0 ms | 0.0 ms |
| `capture.encode` | 0.1 ms | 0.1 ms |

So local durability is identical across the two modes and cannot explain the
47.0 ms fleet p50 against 17.4 ms in the object lane. The difference is follower
append work, which is where the `open.log` re-scan above sits.

## Fleet proof: what is measured and what is left

Every component of the fleet path has now been timed, and the earlier candidates
are all excluded except one:

| Component | Cost | How measured |
| --- | ---: | --- |
| `LogShipper::submit` (admission, load, encode) | 0.20 ms | instrumented statics |
| Shipper batch collection window | ≤1 ms | single deadline by construction |
| Follower `open.log` re-scan | 0.053 ms | instrumented counts over 1,504 writes |
| Owner SQLite commit + LTX capture | 0.4–0.5 ms | capture phase histograms |
| SQL worker round trip | 0.99 ms | example metrics |
| Follower `sync_data` | **~7 ms per append** | instrumented on both followers |
| **Remaining: HTTP request to the follower** | **~40 ms** | by elimination |

The follower `sync_data` figure comes from counters placed around the two
`sync_data` calls inside `append_sync`
(`crates/cellule-runtime/src/follower/records/append.rs:107`, `:144`): both
followers accumulated **5,914,109 µs and 5,774,512 µs** of sync time over a
24-second point with 826 appends, i.e. roughly 7 ms each and about a 24% duty
cycle per follower.

The largest remaining term is the owner-to-follower HTTP request itself. It is
not in the runtime crate: the production `NodeLogTransport` for this fixture is
`impl NodeLogTransport for Transport` in
`crates/cellule-axum/examples/fleet/transport.rs:139`, so the next measurement
belongs in the application adapter — time the request send/receive there and
compare it against the follower's own 7 ms of sync to split transport overhead
from storage.

One caveat that applies to the whole fleet lane: the owner, both followers and
the RustFS container share this 12-core host, and the followers write to the same
APFS volume, so a 7 ms `sync_data` is a shared-device figure and not a
per-device durability cost.

## Next candidates, in evidence order

1. **Objects per command is the whole gap.** Cellule uploads 4–7 objects per
   command against celld's 1, and that single ratio explains the 1.8–3.1× p50
   difference in the matched harness. The bucket-durability application lane
   shows the same shape from the other side: 6.17 PUTs per publication at
   3.04 ms each is the dominant term. Every serious optimization has to reduce
   the object count per published root, not make individual objects faster.

   A code audit of the publish path accounts for the count exactly. Each
   `prepare_append` joins two branches: the segment index and body
   (`upload_prepared_segment`, `replica/upload.rs:64` — one PUT for the index,
   one PUT for the body) and the directory plus root-document objects
   (`put_objects`, `replica/prepare.rs:720`, `:839`). On top of that the runtime
   adds a lineage create and the control CAS. The directory and root-document
   PUTs are frequently skipped because their digests are already referenced from
   the verified predecessor, which is why the observed count is 4–7 rather than
   a fixed number. The two PUTs that are paid on *every* publish are the segment
   index and the segment body.

   Concrete targets, cheapest first:
   - **raise the inline-segment ceiling so the read path stops fetching a
     separate segment-page object**, which trades one GET for root bytes
     (`MAX_INLINE_SEGMENTS` is 32 of `SEGMENTS_PER_PAGE` 96, with
     `MAX_SEGMENT_PAGES` 64 and a hard `ROOT_BYTES` of 32 KiB that the existing
     `encode_root_accepts_the_segment_page_ceiling` test already pins);
   - **reuse a verified predecessor segment page across preparations** instead of
     re-uploading it when its digest is still referenced;
   - **fold the per-root lineage object into the control CAS**, which removes one
     PUT but changes a persisted format and needs a migration;
   - **raise commits covered per root** so the fixed object set is amortized
     further (`replica/coalesce.rs` already merges eligible deltas; this run
     achieved ~1.99 commits per publication).

   Two of these are plausibly small wins and two touch persisted formats. None
   should be attempted on the strength of the object count alone: each needs a
   matched before/after pair on this harness, which is now reproducible.
2. **Fleet proof must win the race.** The follower proof lost 991 times to 73 on
   this host. The cause is *not* the submission path: instrumented phases over a
   1,064-write fleet point measured `submit()` at **0.20 ms mean** (byte
   admission ~0.000, segment load 0.089, ordering lock ~0.000, canonical encode
   0.110), so the two `spawn_blocking` hops and the `order` mutex are negligible
   and an earlier claim that they dominate was **wrong**. What the same run does
   show is where the fleet lane loses: worker round trip **0.99 ms**, while the
   actor queue waits **28.1 ms** and the command-level publication takes
   **18.2 ms** (p50 15.3, p95 38.7) against 9.0 ms on the object lane. The
   follower proof itself is therefore longer than the object publish path on
   this store, and the object round trip wins. The next step is to instrument
   the follower proof latency directly — shipper batch interval, per-lane TLS
   append round trip, and follower `sync_data` — rather than the submission
   call, which is already cheap.
3. **The follower `open.log` re-scan is negligible — measured, and the earlier
   estimate is retracted.** `append_sync` calls `prune_covered`
   (`crates/cellule-runtime/src/follower/records/append.rs:46`), which calls
   `scan_chunk(&open_path, lane, limits, true, known)` (`append.rs:404`).
   `scan_chunk` opens the file and loops from offset 0, reading a header and
   allocating `vec![0; length]` per record for the whole file
   (`crates/cellule-runtime/src/follower/records/scan.rs:302`, `:321`, `:348`);
   the `known` argument compares records rather than resuming past them, so the
   file is re-read on every append.

   A previous round estimated this at **1–5 ms per append**. Instrumented
   counters on both followers over a 1,504-write fleet point at concurrency 4
   give the opposite answer:

   | Follower | scans | total scan time | bytes read | records |
   | ---: | ---: | ---: | ---: | ---: |
   | 1 | 869 | 45,947 µs | 9,172,441 | 883 |
   | 2 | 869 | 49,027 µs | 9,172,441 | 883 |

   That is **53 µs per scan** and about **10.5 KB per scan**, roughly **5% of the
   run's total append time** — not 10–100× that. The reason is
   `ROTATE_BYTES = 1 MiB` (`crates/cellule-runtime/src/follower/mod.rs:29`)
   combined with this workload's shape: the lane rotates long before the file
   approaches the cap, so `open.log` holds only ~11 KB of records here. The cost
   is bounded by rotation, not by retained history.

   **It is not worth changing**, and the 1–5 ms estimate was wrong by about two
   orders of magnitude. The one-line shape of a fix (pass the known end-of-file
   offset so the scan resumes, detecting truncation by length comparison) is
   recorded for anyone profiling a workload that keeps `open.log` near the cap,
   but it should not be attempted on the strength of the earlier estimate.

   The instrumentation was temporary and has been removed; the counters and the
   module-visibility changes it required are gone from the tree.

4. **Cold activation at scale.** The eight-read ceiling only matters with many
   directory leaves; this harness uses one Cell with a small database. A
   dedicated restore benchmark at 256–512 MiB is required before touching it, and
   `tests/host/hooks/activation.rs` should be updated deliberately in the same
   change.
5. **Reads are cheap but the actor still serializes them.** A query costs about
   0.28 ms p50 at the HTTP layer, and an acknowledged write performs a
   receipt-bound readback, so the actor processes roughly two commands per write.
   Anything that shortens the actor's per-command critical path compounds across
   both lanes.

## Open question for the next round

The follower proof is the only measured gap, and its cost is now bracketed but
not attributed:

- `LogShipper::submit` is 0.20 ms, so admission, disk load, ordering and encode
  are all excluded.
- The shipper's collection window is a single 1 ms deadline per batch
  (`crates/cellule-runtime/src/node/log_shipper/mod.rs:376`), not additive per
  frame, so it is also excluded.
- The worker round trip is 0.99 ms, so SQL execution is excluded.
- The command-level publication is 18.24 ms mean, against 9.04 ms in the bucket
  lane.

That leaves the per-lane append round trip itself: the signed TLS append over
loopback plus the follower's `sync_data`, which is batching correctly (one
`sync_data` per batch, `crates/cellule-runtime/src/follower/records/append.rs:140`)
and averages ~1.6 frames per batch at this offered load. The next instrumented
point should time the append request/response on the leader and the follower's
fsync separately. If the follower fsync dominates, the fix is on the follower
storage path; if the TLS round trip dominates, it is connection reuse. Neither
was measured here, so neither is claimed.

## Reproducing

```sh
# provider
docker run -d --name cellule-perf-rustfs -p 127.0.0.1::9000 \
  -e RUSTFS_ACCESS_KEY=cellulebench -e RUSTFS_SECRET_KEY=cellulebench \
  ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858

# bucket durability
CELLULE_TEST_ENDPOINT=http://127.0.0.1:<port> CELLULE_TEST_BUCKET=cellule-perf \
CELLULE_TEST_PREFIX=<fresh> AWS_ACCESS_KEY_ID=cellulebench \
AWS_SECRET_ACCESS_KEY=cellulebench \
python3 scripts/bench-axum-rustfs.py --binary <release>/examples/sql \
  --output <new-dir> --cells 1 --workers 16 --concurrency 4 \
  --write-seconds 30 --write-warmup-seconds 5

# fleet durability (two followers + owner)
CELLULE_AXUM_FLEET_DIR=<fixture> python3 scripts/bench-fleet-local.py \
  --binary <release>/examples/sql --output <new-dir> --cells 1 \
  --workers 16 --concurrency 4 --write-seconds 20 --write-warmup-seconds 5
```

`scripts/generate-capacity-tls.py <dir>` creates the fixture. Each point needs a
fresh bucket prefix; the harness refuses a prefix that already holds state.
