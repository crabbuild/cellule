# Scoped follower validation reuse

Warm follower scans repeatedly decoded LTX bodies that had already passed full
validation. The candidate reads and hashes every current encoded byte and
reuses validation only when the complete frame digest, sequence, length,
leader/log epoch and every validation bound match its in-memory verified index.
Incoming frames and cold scans still receive full envelope/LTX validation.
No body cache, persisted shortcut, resource-cap change or authority change was
introduced.

The same review found a live-index durability bug. After deletion of the open
log, the old index could acknowledge a new append with missing original history.
The original regression fails on that deletion. Candidate rejects deleted,
shortened, missing-prefix, corrupted and valid-but-substituted original history;
restoring exact bytes permits an exact retry and byte-identical cold tail.
Observed file/record changes refresh the index, and every original uncovered
record must retain its digest and length. Object-covered prefixes remain
releasable. A second test prevents witnesses crossing lanes or bounds and
checks that changing an untrusted record digest cannot bless damaged bytes.

| Frames | Coverage | Warm full LTX redecodes, control → candidate | Median stage elapsed, ms | Median process CPU, ms |
| ---: | --- | ---: | ---: | ---: |
| 1,024 | No coverage | 7,680 → 0 | 987.12 → 278.13 | 970 → 270 |
| 1,024 | 256-frame lag | 8,064 → 0 | 1,231.66 → 361.89 | 1,070 → 340 |
| 4,096 | No coverage | 129,024 → 0 | 16,063.29 → 3,818.00 | 15,780 → 3,730 |
| 4,096 | 256-frame lag | 38,784 → 0 | 5,878.96 → 1,906.92 | 5,620 → 1,780 |

Each source ran three repetitions of all four cases: real original captured
segments, 64-frame appends and a fresh store per point. Setup, seal and cold
verification were outside timing. Every receipt, exact retained-byte count,
seal and every cold-returned original frame passed for all 24 points. Frozen
input hashes matched. Current byte traversal and coverage rewrites remain;
zero warm redecodes does not mean zero incoming or cold validation.

This is a Linux aarch64 **unoptimized debug native-stage** comparison. It does
not prove production request latency, multicell HTTP throughput, all-Cell
SQLite reconstruction or the node capacity target. Process CPU uses user and
system ticks at the platform's queried clock rate. Per-case elapsed ranges,
I/O counts, source/build fingerprints and evidence hashes are retained in JSON.
Control source was `a4840c8`; both sources used separate fresh build targets.
Test-only inspection counters were removed before broader validation.
An initial candidate fixture accessed a private field; it was corrected without
widening visibility, and its failed build log remains preserved.

The uninstrumented runtime suite passes, including 643 unit tests and all 241
public runtime scenarios, along with strict Clippy and API docs, format,
boundaries, layout, documentation and SQL/peer checks. Every Rust/Cargo source
file matches the isolated verification snapshot. The original multicell HTTP
comparison is pending. Original node target and qualification gates remain
unchanged: 2,000 resident Cells, 10K durable writes/s, 50K owner-ordered reads/s,
and required target-hardware steady windows and owner-loss/follower-only recovery.

Raw source archives, fixture bytes, per-point results and build/run logs remain
outside Git. Only critical metrics and evidence references are tracked here.

[Critical metrics and fingerprints](2026-10-05-follower-verified-reuse.json),
[prior syscall-only probe](2026-10-05-follower-history-cost.md), and
[follower contracts](../../cellule-runtime/docs/failover-and-followers.md).
