# Resumed 2,000-Cell local comparison

One fresh-provider pair compares `e5c9956` with compaction-admission candidate
`596de73` using 600-second measurement windows after 30 seconds of warmup.
The shared Colima VM has 8 vCPUs and 16 GiB RAM. Both use the same pinned Rust
image, Rust 1.97.1, SQL example identity and load generator. Each point starts
fresh RustFS state with 4 vCPUs, a 6-GiB limit and 32,768 descriptors. Candidate
runs first. Framework budgets remain 512 MiB native memory, 16 MiB retained
cuts and 1 GiB local disk; 16 SQL workers, 8 Tokio workers, 64 clients and queue
capacity 256 offer 1,000 writes/s plus 10 reads/s.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Writes completed/s inside window | 107.05 | 136.80 |
| Write HTTP p50 / p95 / p99, ms | 76.2 / 4,064.0 / 10,528.2 | 316.3 / 1,293.1 / 3,305.4 |
| Write response bodies, MiB/s inside window | 0.02059 | 0.02629 |
| Write offers dropped | 535,454 | 517,599 |
| Reads completed/s inside window | 0.760 | 1.097 |
| Read HTTP p50 / p95 / p99, ms | 1.86 / 3,486.7 / 11,709.2 | 3.79 / 63.7 / 167.9 |
| Request errors | 0 | 0 |
| Cold-audited writes / reads | 75,312 / 542 | 94,595 / 753 |
| Owner sampled peak RSS, bytes | 735,264,768 | 776,810,496 |

Candidate write TPS rises 27.8% and p99 falls 68.6%, while median latency rises
4.15 times. Both runs drop most offers and fluctuate substantially by minute.
One pair does not establish consistent gains. The small mixed-read sample is
not a standalone read-capacity measurement. Latencies use nearest-rank
percentiles from every original measured attempt, including drain completions;
the original histograms remain in the dataset. Baseline p99 exceeds its
histogram range, so the table uses verified raw journals. Throughput counts only
window completions; response-body bytes exclude requests, headers and storage I/O.

Both original audits pass: every acknowledged identity cold-replays with the
same receipt, every read verifies, both drains leave all 2,000 Cells Idle,
SQLite temporary directories disappear, and every Cell's next write advances
its epoch and commit sequence exactly once. Follower appends have zero failures
and no unsupported/unavailable/rejected submissions. This verifies the
shared-cache recovery fix at 2,000 Cells, not owner-loss/follower-only recovery.

The next measured limit is shared publication admission. Candidate root
admission averages 2,939.6 ms versus 51.9 ms admitted preparation, 28.3 ms worker
round trip and 3.1 ms actor queue. Existing dirty/recovery pools remain 8/2.
These lifetime phase populations include setup, warmup and drain and overlap;
do not add their means or quantiles. Fair background-compaction progress under
continuous foreground admission is a hypothesis requiring a causal test.
Both providers reach their memory limit and reclaim cache without OOM;
provider pressure and object amplification remain possible contributors.
The added finite native-capture collector separates WAL parsing, encoding,
local write, fsync and checkpoint work in future diagnostics. Its real
capture/capacity-refusal test and strict Axum Clippy pass; it was not present
in the measured binaries.

The initial 180-second pair preserves a passing baseline (180.59 writes/s;
43,556 writes/296 reads cold-audited). Its candidate fails after RustFS reaches
a 2-GiB cgroup limit: Docker records OOM, exit 137 at 16:41:55 UTC, and the driver
retains 317 write errors and three read errors. That point establishes no gain.
The original failed journals, follower directories and provider volume remain
available; no qualification assertion or framework ceiling was relaxed.

The [dataset](2026-10-05-local-resume.json) retains source/binary hashes,
completion intervals, critical phases, raw-journal reverification digests and
external evidence references. Original binaries and journals stay outside the
checkout. The [node capacity target](node-capacity.md) remains open: shared
owner/follower/driver/provider hardware, one ten-minute pair and drained-root
recovery do not qualify 10,000 writes/s plus 50,000 reads/s on an isolated owner.
