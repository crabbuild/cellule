# Shared follower history cost

Evenly offered Cell writes still enter one ordered lane per owner/log epoch.
The native append path scans `open.log` on every batch. Advancing coverage can
also rewrite retained records and rescan the lane. Independent SQLite Cell
state does not eliminate this shared work.

A frozen-input diagnostic used real captured LTX segments, every segment encoded
in order, 64-frame appends, three repetitions of four cases, and a fresh store
per point. Setup, seal and cold verification were outside the timed interval.
Both sources checked every receipt, exact retained-byte accounting, seals,
and every cold-retained frame against its original bytes. All 12 points per
source passed. This tests follower storage; it does not establish all-Cell
SQLite reconstruction, HTTP request performance or node capacity.

With no coverage, appending 4,096 frames reread **101.10 MB** for **3.54 MB**
retained. A prototype changed only `scan_chunk` to a transient 64-KiB read
buffer, retaining every frame/LTX check, logical offset, truncation and sync.
Read syscalls fell sharply, but bytes traversed and rewritten stayed effectively
unchanged. Every median elapsed time regressed; the prototype was **not adopted**.

| Frames | Coverage | Median read calls, control → prototype | Median append-stage elapsed, ms | Elapsed change |
| ---: | --- | ---: | ---: | ---: |
| 1,024 | No coverage | 15,379 → 100 | 871.94 → 1,243.81 | +42.65% |
| 1,024 | 256-frame lag | 18,475 → 2,442 | 979.82 → 1,911.68 | +95.10% |
| 4,096 | No coverage | 258,115 → 1,644 | 15,150.67 → 23,844.18 | +57.38% |
| 4,096 | 256-frame lag | 89,275 → 12,313 | 5,773.24 → 6,498.00 | +12.55% |

These are Linux aarch64 **unoptimized debug** measurements, not production
latency or TPS. The ordered shared-VM comparison had greater candidate elapsed
variation; per-case ranges are retained in JSON. It cannot isolate production
CPU cost or attribute the whole timing difference to source changes. Fewer
syscalls do not prove a throughput gain. Repeated history verification and
coverage rewrites remain candidates for the next measured optimization.

Control framework source was `a405e89`. The candidate used a separate target
directory, a freshly compiled artifact and a distinct verified binary. Exact
frozen input digests matched every point. An earlier copied snapshot reused
the control binary because of source mtimes; that comparison is explicitly
excluded, with its original log retained. The initial fixture's incorrect
one-segment assumption was corrected to encode every capture segment; the
failed fixture and log remain preserved.

Raw fixtures, all point results, build/run logs, frozen bytes, source archive
and their fingerprints remain outside Git. Only critical metrics and evidence
references are tracked here. No resource cap, response gate, persisted framing,
retry deadline or qualification profile changed. The 2,000-Cell / 10K-write /
50K-read target remains unqualified.

[Critical metrics and fingerprints](2026-10-05-follower-history-cost.json),
[append and pruning implementation](../../cellule-runtime/src/follower/records/append.rs),
and [original multicell comparison](2026-10-05-pinned-coalescing-r11.md).
