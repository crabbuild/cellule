# Performance evidence

Versioned reports record the workload, compared revisions, critical metrics,
verification, and limitations. Companion JSON files retain every measured pair,
phase distribution, receipt hash, and source/binary provenance. CI links retain
raw artifacts. Do not combine gains from separate runs.

| Comparison | Clients | Cells | Measured duration | Report and dataset |
| --- | ---: | --- | --- | --- |
| Grouped writes | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-grouped-paired-writes.md) · [JSON](2026-10-04-rustfs-grouped-paired-writes.json) |
| Grouped writes with corrected request-slot handoff | 64 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-grouped-c64-writes.md) · [JSON](2026-10-04-rustfs-grouped-c64-writes.json) |
| Compaction transfer refill | 16 | 1 / 4 / 16 | Three alternating 120-second pairs per Cell count | [Report](2026-10-04-rustfs-compaction-paired-writes.md) · [JSON](2026-10-04-rustfs-compaction-paired-writes.json) |

The grouped-write comparisons improve queued workloads. The 16-client,
16-Cell profile regresses tail latency; compaction refill does not establish
a consistent multicell gain. These are single-host RustFS measurements with
fixed budgets, not evidence of horizontal scaling. Each Cell has an independent
SQLite database; SQL workers and provider capacity are shared.

Earlier measurements remain available for historical comparison:
[initial HTTP](2026-10-03-rustfs-http.md),
[multicell](2026-10-03-rustfs-multicell.md),
[steady reads](2026-10-03-rustfs-steady.md), and
[steady writes](2026-10-03-rustfs-steady-writes.md).
