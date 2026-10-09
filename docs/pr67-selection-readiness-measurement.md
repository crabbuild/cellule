# PR 67: selection readiness and paired write measurement

**PR #67 is not ready to merge. The latest candidate completes 184.77 Fleet
writes/s and 248.68 Bucket writes/s. No throughput improvement or celld parity
is established.** Fleet still fails availability; Bucket passes ACK audits but
misses delivery and latency targets.

## Change and regression

The previous selection task owned the Cell publisher while waiting for shared
selection. Its ten-second timeout fenced a Cell after valid Fleet ACKs and
prevented older root debt from preparing. Commit
`6c909a6dec983bc222a0e1c047e878808f677cb2` observes authenticated readiness once
per Cell without owning the publisher. Only the ready oldest prefix enters
exact cleanup; the existing cleanup timeout and original proof checks remain.
Fencing cancels observation, while native publication and drain remain joined.

The real-actor regression holds the second selection for eleven seconds after
two durable filesystem followers grant its ACK. The old implementation fails
with `Fenced`; the new implementation preserves reads and exact retries,
prepares the older root before releasing the held selection, joins shutdown,
cold-restores both mutations/results and releases retained credits. This fixes
the reproduced ownership failure, not application availability under load.

## Matched diagnostic

Six fresh cases run sequentially: before, candidate and celld for each mode.
The before binary is `4a8f55cc99414e6a3907531c774a184842e1de3e`; the candidate
is `6c909a6`. Celld is pinned at
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. Identical fixture, client, auditor,
images, host and runner provenance pass comparison checks. Each case has 1,000
uniform Cells, 96-byte values, SQL INSERT plus SELECT, a two-hour retry/result
ledger, 128 clients/queue slots, a 30-second warmup and 60-second measured window.
Fleet offers 15K writes/s with two followers; Bucket offers 2K/s.

The ARM64 Docker VM has **8 CPUs and 8 GiB shared RAM**. Serving containers have
8-CPU/16-GiB ceilings and 4-GiB tmpfs; client/provider ceilings exceed VM resources.
The existing external RustFS adaptation, 64-MiB retention budget, 1-GiB managed
disk budget and gates are unchanged. No build or contributor suite overlaps
timed windows. This does not qualify a dedicated 8-vCPU/16-GiB node, physical
device durability, the laptop KV workload or the 2,000-Cell goal.

TPS counts successful writes completed inside the window. Successful scheduled
p99 includes trailing successes and excludes errors/dropped offers; request
p99 starts at issuance. Qualification also checks all-attempt latency.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Measured errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / before | 195.13 | 1,824.85 | 1,120.30 | 319,678 | 568,614 |
| Fleet / candidate | 184.77 | 1,745.68 | 1,037.21 | 328,243 | 560,671 |
| Fleet / celld | 4,717.33 | 183.65 | 90.23 | 3,583 | 613,376 |
| Bucket / before | 247.55 | 4,785.42 | 4,241.42 | 0 | 104,891 |
| Bucket / candidate | 248.68 | 4,512.72 | 4,006.82 | 0 | 104,823 |
| Bucket / celld | 1,321.83 | 1,046.07 | 561.37 | 0 | 40,437 |

Candidate Fleet completes 5.3% fewer writes than before; Bucket differs by
+0.46%, and its fixture bypasses the managed producer. One short pair does not
establish an attributable change or repeatable gain. All six points fail
qualification. These overloaded completion counts are not sustainable capacities.
Before Fleet also has 20,579 warmup request errors; the other five have zero.
All six drop warmup offers. No failed case is omitted or silently retried.

## ACK availability and drain

| Mode / system | Complete ACK cohort | Warm errors / retry checks | Cold read/retry | Successful drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / before | 21,844 | 15,692 / 6,152 | not reached | absent |
| Fleet / candidate | 21,366 | 19,797 / 1,569 | not reached | absent |
| Fleet / celld | 464,403 | 464,399 / 4 | not reached | absent |
| Bucket / before | 26,519 | 0 / 26,519 | pass: all 26,519 | 8.58 |
| Bucket / candidate | 26,423 | 0 / 26,423 | pass: all 26,423 | 25.29 |
| Bucket / celld | 124,185 | 0 / 124,185 | pass: all 124,185 | 4.96 |

Candidate Fleet's four sampled first audit errors are POST retries to `/orders`,
after the auditor's matched GET check. They suggest retry admission under
pressure needs a regression; they do not establish the cause of every failure.
The pre-SQL command gate refuses publication backlog before durable retry lookup.
Cellule owners exit zero during failed-case cleanup, which does not establish
successful aggregate drain. Celld Fleet is OOM-killed, exit 137. Missing later
provider observations fail gates; unavailable audits do not prove mutation loss.
Passing Bucket cold audits follow graceful drain, not failed-owner recovery.

Candidate Fleet materializes 11.12 commands/root, with 0.5422 PUT attempts and
13.9287 GET/range attempts per completed write. These canonical storage API
boundary deltas include work for earlier cuts; SDK internal retries are not
separately counted. Steady Bundle ACKs remain zero. Retention rises from 27.88
to 58.40 MiB of 64 MiB, and oldest debt ends at 45,452 ms. Two samples do not
prove bounded debt. The 215-command and conditional 0.05-PUT targets remain unmet.
Bucket still uses per-Cell roots, at 3.6258 PUT attempts/completed write.

## Verification and merge gates

The frozen selection-fix snapshot passes all eleven contributor routes:
1,962 tests passed, zero failed and 38 ignored. Those routes pass again on
Rust 1.97 after compatibility commit `d852525` replaces the deprecated atomic
update with an equivalent checked compare-and-swap loop. Rust 1.99 Clippy also
passes for all workspace targets/features with warnings denied. That subsequent
node-initialization change is outside the timed binaries above and is not
presented as a performance improvement. Independent journal replay
reconciles every offer, attempt, success, error, drop and trailing completion;
complete ACK cohort counts and hashes match. Raw journals, binaries, provider
data, failed snapshots and manifests remain outside Git in
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`.
All 9,530 entries in the retained measurement evidence index rehash correctly.
Index SHA-256:
`f8b13a0ca0f188212e4f8894d125fd8c4f3eab8366c3d664ec3558a6b48f29ef`.

| Required before merge | Measurable completion |
| --- | --- |
| CI and compatibility | All required checks green on the final head; retain Rust 1.97 compatibility. `d852525` addresses the previous head's deprecated `AtomicU64::fetch_update`; final-head CI remains required |
| Availability and joined drain | Reads and known retries remain available under backlog; zero ACK-audit errors; complete accepted/issued range joins before departure |
| Shared publication and bounded debt | Connect Bucket producer, demonstrate materializer progress, bounded retained/native debt and complete publication cost; qualify 215-command density and the conditional PUT target |
| Recovery and collection | Kill owner with prior Fleet ACKs, recover every issued suffix, preserve exact retries, transfer safely and collect only from complete cross-Cell references after quiescence/grace |
| Performance qualification | Three matched repetitions of at least five minutes, sustainable capacity search, zero errors/drops, Fleet 15K/s at scheduled p99 ≤50 ms, Bucket 2K/s at ≤200 ms, read-only/mixed guardrails and the 2,000-Cell / 10K-write / 50K-read standard-node goal |

The [delivery report](write-performance-delivery.md) and
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md)
retain these requirements. Keep PR #67 in draft.
