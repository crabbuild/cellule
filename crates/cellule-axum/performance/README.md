# Performance evidence

Versioned reports record the workload, compared revisions, critical metrics,
verification, and limitations. Companion JSON files retain every measured pair,
critical publication phases, receipt hashes and source/binary provenance. CI links retain
raw artifacts. Do not combine gains from separate runs.

The [node capacity target](node-capacity.md) tracks the 2,000-Cell,
10,000-write/s and 50,000-read/s goal, offered-load measurements and required
recovery/resource evidence. It is not a supported capacity claim.
The [node scaling probes](2026-10-04-node-scaling-probes.md) retain provider CPU
comparisons and failed density admission attempts with a
[dataset](2026-10-04-node-scaling-probes.json).
The [object coverage batching report](2026-10-04-object-coverage-batching.md)
tracks concurrent publication comparisons. The
[coalesced root report](2026-10-04-coalesced-root-coverage.md) retains the next
failed 64-Cell pair and identifies the conservative disk admission ceiling.
The [SQLite growth admission report](2026-10-04-sqlite-growth-admission.md)
records the concurrent admission regression and two cold-audited steady windows,
with publication and compaction remaining as throughput limits.
The [node lease report](2026-10-05-node-lease-renewals.md) records the redundant
idle renewal regression, observed 2,000-Cell residency and failed provider-bound
density runs. These failures do not establish throughput qualification.
The [shared-cache report](2026-10-05-shared-directory-cache.md) identifies duplicate
cold-start disk accounting and retains the causal regression. The fixed RustFS
replay and throughput measurements remain pending.

Reports identify frozen measured revisions. The later CI fix preserves reader
rotation across discovery changes and synchronizes a durability test with its
telemetry callback; those changes are outside the reported benchmark comparisons.

| Comparison | Clients | Cells | Measured duration | Report and dataset |
| --- | ---: | --- | --- | --- |
| Full change versus main | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-inline-root-main-writes.md) · [JSON](2026-10-04-rustfs-inline-root-main-writes.json) |
| Inline roots versus composed preparation | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-inline-root-writes.md) · [JSON](2026-10-04-rustfs-inline-root-writes.json) |
| Grouped writes | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-grouped-paired-writes.md) · [JSON](2026-10-04-rustfs-grouped-paired-writes.json) |
| Grouped writes with corrected request-slot handoff | 64 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-grouped-c64-writes.md) · [JSON](2026-10-04-rustfs-grouped-c64-writes.json) |
| Compaction transfer refill | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-compaction-paired-writes.md) · [JSON](2026-10-04-rustfs-compaction-paired-writes.json) |
| Composed compaction, before lineage integration | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-composed-original-writes.md) · [JSON](2026-10-04-rustfs-composed-original-writes.json) |
| Composed compaction, with required lineage | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-composed-lineage-writes.md) · [JSON](2026-10-04-rustfs-composed-lineage-writes.json) |

The full change improves paired median TPS, p95 and p99 at each Cell count
versus main; one 16-Cell pair loses TPS. The isolated inline-root comparison
improves TPS/p95 medians but regresses small-cell p99 and one 4-Cell pair.
[Routing](2026-10-04-rustfs-inline-root-routing.md) retains two write-p99
regressions and three read-latency alerts.
[Correctness qualification](2026-10-04-inline-root-qualification.md) verifies
the measured source, recovery, durability and resource limits.

The grouped-write comparisons improve queued workloads. The 16-client,
16-Cell profile regresses tail latency; compaction refill does not establish
a consistent multicell gain. These are single-host RustFS measurements with
fixed budgets, not evidence of horizontal scaling. Each Cell has an independent
SQLite database; SQL workers and provider capacity are shared.

The [inline-root preparation report](2026-10-04-inline-root-preparation.md)
tracks operation counts and boundary/recovery verification. Sustained comparisons
and provider qualification above now cover that candidate.

Additional cost and qualification evidence:
[root phase calibration](2026-10-04-rustfs-root-phase-calibration.md),
[compaction publication cost](2026-10-04-compaction-publication-cost.md), and
[grouped routing](2026-10-04-rustfs-grouped-routing.md).

The [composed compaction regression](2026-10-04-composed-compaction-regression.md)
records reduced preparation operations with exact-root, recovery, failure, and
cancellation checks. The integrated sustained comparison improves 16-Cell p99
but loses paired median TPS at every Cell count. The original comparison and
its gains remain separate evidence. [Composed routing](2026-10-04-rustfs-composed-routing.md)
also retains forwarded-write latency regressions. [Correctness qualification](2026-10-04-composed-compaction-qualification.md)
records independent workspace, provider, recovery, and resource-limit proofs.

Earlier measurements remain available for historical comparison:
[initial HTTP](2026-10-03-rustfs-http.md),
[multicell](2026-10-03-rustfs-multicell.md),
[steady reads](2026-10-03-rustfs-steady.md), and
[steady writes](2026-10-03-rustfs-steady-writes.md).
