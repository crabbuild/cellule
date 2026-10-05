# Publication coverage after admission: R8

The held-admission regression confirms a batching defect: selecting coverage
before waiting for shared preparation capacity publishes two roots for four
follower-proven commits. Candidate `ff19489` selects coverage after admission
and publishes one root while preserving the bounded queue, publisher exclusion,
original retry deadline, durable response gate, and exact restore.

The larger workload **did not improve write performance**. This matched local
pair completed both original audits but write TPS regressed **13.83%**. Keep the
change in draft pending causal performance work and the CI reader-scaling result.

| Critical metric | Control `ba584c3` | Candidate `ff19489` |
| --- | ---: | ---: |
| Completed write TPS | 118.17 | 101.82 |
| Write request p50 / p95 / p99, ms | 381.60 / 1,657.83 / 2,893.25 | 427.04 / 2,119.00 / 4,005.48 |
| Write scheduled p50 / p95 / p99, ms | 2,258.9 / 5,623.1 / 8,299.2 | 2,870.6 / 6,000.1 / 7,602.7 |
| Write response-body throughput, MiB/s | 0.022718 | 0.019573 |
| Mixed-read TPS | 0.920 | 0.722 |
| Read request p50 / p95 / p99, ms | 6.70 / 113.17 / 467.47 | 6.31 / 143.25 / 283.04 |
| Write queue drops / request errors | 528,786 / 0 | 538,586 / 0 |
| Peak owner RSS, MiB / descriptors | 731.88 / 16,215 | 674.04 / 16,164 |
| Owner CPU cores, setup/warmup/drain included | 0.915 | 1.020 |
| Publication count per audited write | 0.937 | 0.816 |
| Provider PUT count per audited write | 6.785 | 6.334 |
| Original writes / reads audited | 80,461 / 627 | 71,637 / 516 |

Each point used 2,000 Cells, 30-second warmup, a 600-second measured window,
1,000 offered writes/s, 10 offered reads/s, 64 clients, queue capacity 256,
16 SQL workers and eight Tokio workers. Order was candidate then control, with
a fresh RustFS store per point. Both sources include the same clock correction.
The release compiler, driver, source archives and binaries were pinned and
verified. Native capacities remain dirty 8, recovery 2, I/O 32 and jobs 8.

TPS counts completions inside the measured window. Request percentiles are raw
nearest-rank values from measured attempts including drain. Scheduled latency
also includes driver queue delay. Response-body throughput measures the small
HTTP response bodies. Sparse mixed reads do not establish read capacity.

## Remaining bottleneck

| Lifetime phase mean, ms | Control | Candidate |
| --- | ---: | ---: |
| Dirty admission | 3,784.24 | 3,265.66 |
| Total publication | 5,667.46 | 4,336.64 |
| Admitted root preparation work | 65.59 | 79.63 |
| Compaction | 24,124.55 | 34,282.08 |
| Provider PUT | 41.46 | 44.38 |
| Capture | 8.91 | 10.86 |
| SQL worker round trip | 35.91 | 41.28 |

Fewer roots and PUTs did not produce higher TPS. Publication admission,
compaction and provider I/O remain expensive while owner CPU utilization is
low. Startup, capture, worker and provider timings also vary, so this single
ordered shared-VM pair cannot attribute the full regression to the change.
The next causal experiment must distinguish native batch/compaction cost from
provider variation before claiming a gain.

These lifetime phases include setup, warmup and drain, overlap, and have different
counts. Many admission/compaction samples overflow their histograms; the JSON
retains those counts and unavailable percentiles. The candidate moves shared
admission before coverage selection. A lower `RootAdmission` value alone does
not mean its wait disappeared. Do not subtract phase means.

## Evidence and qualification

Both original follower durability gates passed, with no node-log append failures
or rejected submissions. Every original journal hash and audit count matched
independent reverification. All 2,000 Cells became idle, cold-restored, accepted
the next write at exactly the next sequence, and advanced their fencing epoch.
Both complete provider archives passed gzip/tar, inode-count and SHA-256 checks
before removing their owned fixtures. Original journals, TLS fixtures, logs,
binaries and archives remain outside Git.

The isolated validation snapshot matches all 1,229 Rust/Cargo files in the final
candidate archive. Full local Runtime/LTX tests, SQL example tests, strict
Clippy, API docs, format, boundary/layout and documentation gates passed. CI
object-proof and follower-proof passed. The first Compose reader-scaling run
returned `read replica is unavailable`; its original artifact is retained and
an unchanged retry is pending. Broad workspace CI is also pending.

[Critical metrics and evidence hashes](2026-10-05-publication-admission.json)
identify the frozen sources and external evidence under
`/Volumes/Workspace/crabbuild-target/cellule-node-scaling-54f709e0/evidence`.

This is a shared Colima diagnostic: the owner, followers and RustFS share the
8-CPU/16-GiB VM. The original target remains unestablished: three 30-minute runs
on target hardware, 10K durable write TPS, 50K owner-ordered read TPS, and the
original owner-loss/follower-only recovery qualification are still required.
