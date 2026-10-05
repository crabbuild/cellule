# Coalesced root coverage and the disk admission ceiling

One published Cell root now stages all its covered node-log tickets together.
Tickets retain their original durability binding; equal epoch numbers cannot
combine different bindings. The deterministic regression changes 64 authority
updates to one, retains all 64 command telemetry observations, and checks atomic
scope validation. Runtime library tests pass (616, three existing ignored), as
do strict runtime/Axum Clippy, release builds, format, boundaries and docs.

## Failed 64-Cell comparison

Baseline `764e70a` already batches concurrent independent Cell publications.
Candidate `bfccadb` adds batching within one coalesced root. Both use eight SQL
workers, 64 clients, queue capacity 256, 100 offered writes/sec and 10 reads/sec,
30-second warmup and 180-second measurement. Budgets remain 1 GiB local disk,
512 MiB native memory and 16 MiB retained cuts. Each point uses fresh object keys
and empty follower stores, RustFS, two fsynced followers and pinned mTLS. Driver
hashes match; no compilation or tests overlap measurement.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Successful writes/sec in window | 83.339 | 80.333 |
| Write errors / queue drops | 2,244 / 739 | 3,231 / 302 |
| Warmup write errors | 5 | 240 |
| Write HTTP p50 / p99, ms | 26.7 / 3,030.5 | 24.7 / 2,426.7 |
| Write scheduled p50 / p99, ms | 36.9 / 7,026.6 | 28.6 / 4,559.0 |
| Successful reads/sec in window | 8.317 | 8.039 |
| Read errors / queue drops | 215 / 86 | 321 / 32 |
| Owner peak RSS, bytes | 122,003,456 | 132,038,656 |
| Total serial activation time, ms | 1,949 | 15,971 |

Both driver gates fail and neither cold audit executes. Original journals retain
HTTP 503 unavailable replies, identities and uncertain outcomes. Latencies
include failed requests, which can respond quickly; smaller percentiles do not
establish a performance improvement. Warmup and activation differ substantially.
One failed pair on the shared eight-vCPU development VM cannot establish a
regression, improvement or supported capacity. Durability counters are not
verified because failed owner runs do not produce the required final evidence.

## Next bottleneck

[`Db::transaction_with`](../../cellule-ltx/src/db/mod.rs) reserves twice
`max_capture_bytes` before running SQL. The default is 64 MiB, so a tiny write
reserves 128 MiB until capture reconciles actual database, WAL and retained-cut
bytes. Eight simultaneous reservations consume the whole 1 GiB envelope before
existing artifacts. Earlier diagnostics reproduced local-disk capacity refusal;
the exact internal cause of every error in this pair remains unestablished.

The next change must admit file growth before bytes are written, retain capture
capacity for every committed cut, and preserve rollback, oversized full-image,
cancellation and fencing behavior. Lowering the constant alone is insufficient.
Then profile SQLite flushes, follower batching, publication and compaction
admission separately. Keep foreground progress available while maintenance
drains its retained artifacts.

The [dataset](2026-10-04-coalesced-root-coverage.json) preserves exact commands,
source/binary and journal hashes, error populations, maxima, histogram overflows,
resource peaks, per-Cell successes and completion intervals. Raw evidence stays
outside the checkout. The [2,000-Cell target](node-capacity.md) remains active and
unqualified; resident Cell count does not establish active write capacity or
individual creation latency.
