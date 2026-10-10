# PR 67: metadata windows and fresh paired measurements

**Performance parity remains unmet.** The candidate completes 502.28 Fleet
writes/s versus 455.73 before and 1,999.83 for celld at the same 2,000/s offered
load. Successful scheduled write p99 worsens 827.14→1,454.42 ms, and returned
write errors increase. Read-only throughput falls 3.12% in this pair. The
candidate passes complete ACK warm/cold read and original-retry audits and
drains in 66.52 seconds. The baseline write case fails two warm retries and
does not reach the cold audit. Every performance profile fails; PR #67 remains
a draft.

## Change and bounded verification

The preceding loader issued separate range requests for selected catalog
shards and detached histories even when they shared an immutable object.
Loading now sorts compact indices by object and offset and reads bounded
windows. Contiguous shard extents merge without fetching gaps. History windows
may bridge gaps of at most 32 KiB, charging each gap once against the unused
original 4-MiB combined metadata allowance. Exhausted credit preserves separate
valid reads. Up to eight windows join before every original extent is sliced
and canonically authenticated. Padding supplies no coverage rights.

Shard and history phases remain separate; shard bodies drop before history
reads. Plans retain at most 256 shard indices or 4,096 history indices as `u16`
values and eight window descriptors. No availability cache or new admission
policy is introduced. Persisted formats, authority pins, native verification,
the 20-MiB working reservation and maintenance inventory semantics remain
unchanged. Coalescing trades request count against returned bytes; both costs
are measured below.

The real 64-Cell preparation regression has a 2,000-binding catalog, reads one
header, one catalog window and one history window, and cold-restores all 64
Cells' exact seeds and outcomes. The unchanged loader fails its three-read
assertion in three repetitions with **123 reads**; the candidate passes all
three with **three reads**. Other catalog entries are metadata fixtures, not
2,000 active writers. Distinct-object fixtures preserve the original eight-read
limit, cancellation without publication, exact recovery and missing-origin
rejection. Those fixtures pass three repetitions before and after the change.
Three planner tests and all 99 bundle tests pass. These are component results,
not application TPS.

## Matched workload and provenance

| Dimension | Diagnostic |
| --- | --- |
| Before executable | `30cd960ff78a2be31fe84493ad0038b2ad4be7ad` |
| Candidate executable | `4a5b00148ba579c1f4e035b16e8eb783c3d6fe6d` |
| Celld | `f2bf648663a610eefde71f3547ad61e9b896b1f0` |
| Active population | 2,000 real Cells, uniformly offered; every Cell receives timed successful responses in every case |
| Command | 96-byte SQL INSERT/SELECT values and the same two-hour request/result ledger |
| Fleet | One owner, two followers; local state and follower logs on tmpfs |
| Arrival | 128 clients/queue slots, 30-second warmup, one 60-second window |
| Offered load | Separate 2,000-write/s and 20,000-read/s cases |
| Admission | Original 64-MiB retained budget, 1-GiB managed disk and 20-MiB producer reservation |

All six cases are fresh, with new namespaces and provider volumes. The before
executable is a byte-identical copy of the preceding verified executable,
measured again here. The candidate is a pinned Linux release build with no
measurement overlay. Its SQL SHA-256 is
`bc1a577f3a1851432497cc3f4a6569272a225d252e9dab72195e66674b93e43c`.
All 1,940 exported framework files, including symlink targets, match the commit;
all 1,318 Rust/TOML/lock inputs match the contributor-check snapshot. Clients,
auditor, runner, workload fixtures, images and resource settings match.
Both managed SQLite paths use WAL NORMAL. No build, broad suite or journal
audit overlaps a timed window.

The shared ARM64 Docker VM has eight CPUs and about 8 GiB total memory across
owner, followers, provider and client. Container ceilings exceed that capacity,
internal admission policies differ, and another host VM remains active. VM
settings are unchanged within the comparison. These diagnostics do not qualify
a dedicated 8-vCPU/16-GiB serving process or physical-media durability. They do
not establish maximum throughput: the write offer is capped at 2,000/s.

## Application results

TPS counts successful completions inside the timed window. Successful scheduled
p99 includes trailing successes; all-attempt p99 includes fast failures and is
reported separately. Dropped offers are separate from returned errors.

| Point | Successes/s | Successful scheduled p99 ms | All-attempt scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cellule write, before | 455.73 | 827.14 | 627.0 | 36,569 | 56,070 |
| Cellule write, candidate | 502.28 | 1,454.42 | 729.5 | 50,573 | 39,162 |
| Celld write | 1,999.83 | 13.60 | 13.7 | 0 | 0 |
| Cellule read-only, before | 17,905.02 | 26.87 | 26.9 | 0 | 125,645 |
| Cellule read-only, candidate | 17,347.03 | 31.26 | 31.3 | 0 | 159,122 |
| Celld read-only | 19,983.30 | 3.51 | 3.6 | 0 | 973 |

Write throughput is 10.21% higher in this pair, but successful write p99 is
75.84% higher. Read throughput is 3.12% lower and successful read p99 is higher.
The canonical read guardrail fails: throughput ratio 0.969, all-attempt p99
ratio 1.164, and both Cellule arms fail delivery. No acceptable, repeatable or
attributable performance gain is established. The preceding measurement of the
same baseline executable was 550.52 writes/s and 17,877.42 reads/s; that separate
observation remains evidence of run variance rather than a substitute control.

Write warmup errors/drops are 0/32,928 before, 19,799/23,579 candidate and 0/0
celld. Read warmup errors are zero; drops are 420,544/410,166/2,975. Every
Cellule write error preserves HTTP 503 and `Cell is temporarily unavailable`.
All 128 journals per write case reconcile exact attempt and error counts.
The response alone does not identify the underlying per-Cell pressure transition.

| Case | ACK cohort | Warm reads / original retries | Joined drain | Cold reads / original retries |
| --- | ---: | --- | ---: | --- |
| Cellule write, before | 56,434 | Two retries fail with HTTP 503 | Not reached | Not reached |
| Cellule write, candidate | 48,888 | All pass | 66.52 s | All pass |
| Celld write | 182,001 | All pass | 16.63 s | All pass |
| Cellule read-only, before | 2,001 | All pass | 37.15 s | All pass |
| Cellule read-only, candidate | 2,001 | All pass | 40.23 s | All pass |
| Celld read-only | 2,001 | All pass | 2.80 s | All pass |

Independent replay reconciles every timed/warm attempt, success, error, dropped
offer, per-Cell distribution, expected read output and complete ACK manifest.
Journal reconciliation is not a passing warm/cold availability audit. The
baseline's failed warm audit ends the normal drain/cold sequence; its cleanup
does not substitute for those exit gates. Every failure remains evidence.

## Storage work and remaining coupling

| Window cost per completed write | Before | Candidate |
| --- | ---: | ---: |
| GET/range attempts | 8.330 | 6.274 |
| Returned GET/range bytes | 64,987 | 52,515 |
| Successful PUTs | 0.395 | 0.374 |
| Materialized commands per root | 19.22 | 12.22 |

Observed requests fall 24.68% and returned bytes fall 19.19%. These counters
include background work; SDK retries and provider wire overhead are excluded.
Starts and completions can straddle boundaries. They are not isolated command
costs or proof that coalescing caused the application TPS change. Root density
falls and publication amplification remains well above the separate cost target.

[`assign_capture`](../crates/cellule-runtime/src/node/log_shipper/mod.rs)
still reserves publication capacity under the global ordered issuance lock
before assigning and enqueueing follower work. The publication task verifies
and selects serial cohorts before releasing capture credit. A slow publication
consumer therefore gates Fleet progress across otherwise independent Cells.
Celld's [shipping loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4514)
keeps ordered rounds in flight independently of bucket work; its
[follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160)
groups delivered frames before durable append. Cellule still awaits each append
batch. Shared storage components do not supply equivalent scheduling.

Before, all submission phases reconcile with 27,617 successes and zero residual;
ordered-lock wait averages 115.63 ms, 98.33% of submission time. Candidate
observed ordered-wait mean is 118.81 ms. Its phase counts differ, 30,137–30,158,
with a 1,990,260,492-ns residual, so they cannot support an exact partition of
successful submission time. Candidate SQL-worker and follower-proof means are
0.209 and 23.742 ms in separate cohorts; these quantities are not additive or
CPU-utilization measurements. The ordering wait remains large, while the code
shows where publication credit enters that path.

Candidate pending publications grow 2,025→2,465; unpublished node-log bytes
grow 13,300,853→34,648,517 and oldest debt grows 45.20→46.33 seconds. Retained
bytes fall 66,551,180→62,036,852 against 67,108,864. All 2,000 Cells remain
active and the node is unfenced with active Fleet at both boundaries. Issued /
follower-proven / tiered end positions are 52,662 / 52,572 / 52,036.
Two endpoints do not qualify a sustained bounded debt slope. Read-only windows
also include seed-root publication work.

## Verification and next delivery

All 13 broad contributor routes pass in the frozen source: 1,985 workspace
tests including doctests, 38 ignored; 60 local LTX tests; all-feature/target
checks; Clippy with warnings denied on Rust 1.97 and 1.99; format, API docs,
boundaries, layout, document, SQL/peer and harness gates. Focused results above
remain separate. The ignored environments remain unverified.

Finish reducing catalog/base/history verification and upload work; separate
native progress from recoverably bounded publication debt; pipeline ordered
follower rounds with safe complete-suffix drain and recovery. Diagnose overload
availability and read variance. Larger queues alone cannot increase sustainable
service capacity. Complete Bucket adapter integration, mixed load, failed-owner
orchestration and safe collection. Keep the unchanged
[node capacity contract](../crates/cellule-runtime/docs/write-performance-design.md).

Raw builds, source snapshots, invalid preparation attempts, failures, journals,
telemetry, audits and inventories stay outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/metadata-windows-20261009-*`.
An early verification preparation race and a regular-file-only source comparison
are explicitly excluded/corrected; their records remain. Neither produced a
qualified performance result. All canonical reports and the comparison fail
qualification.

[Preceding encoder measurement](pr67-encoder-cost-measurement.md),
[implementation](bundle-coverage-implementation.md),
[delivery](write-performance-delivery.md).
