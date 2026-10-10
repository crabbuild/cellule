# PR67 WAL NORMAL verification

Runtime-managed SQLite activations opt into WAL `NORMAL` at
`075b2cd45cb2762f9795aab643e0a6ad14e4ca8d`. Standalone LTX databases keep
`FULL`. Command responses still require selected object publication or an exact
recoverable follower proof. Follower log fsync is unchanged.

This **interrupted diagnostic comparison does not qualify write parity**.
Seven of nine planned cases completed. The FULL Bucket case reached warm audit
and clean owner exit, then the Docker VM stopped before cold audit and its final
case report. NORMAL Bucket did not start. The cause of the VM stop is unknown;
the original plan and incomplete evidence remain retained outside Git.

## Matched workload and completed measurements

The controls are Cellule `7fc079345b5214a18a001833732a84039e4485db` (FULL),
Cellule `075b2cd45cb2762f9795aab643e0a6ad14e4ca8d` (NORMAL), and celld
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. Fresh pinned release builds used
identical clients and auditors. Each case used 1,000 uniformly selected Cells,
96-byte values, INSERT plus SELECT, durable request/result records, 128 clients,
a 30-second warmup and a 300-second offered-load window. This is not a bounded
KV overwrite workload.

An ARM64 Linux Docker VM shared 8 CPUs and 16 GiB across the owner, two followers,
object store and load generator. Each node had 4 GiB tmpfs scratch. Cellule's
retention budgets were 64 MiB memory and 1 GiB managed disk. This is a simulation,
not three dedicated machines, and only one repetition of each point completed.

| Durability / offered load | FULL completed TPS / p99 ms | NORMAL completed TPS / p99 ms | celld completed TPS / p99 ms |
| --- | ---: | ---: | ---: |
| Fleet / 100 per second | 98.18 / 601.7 | 99.91 / 369.7 | 100.00 / 56.8 |
| Fleet / 15,000 per second | 278.30 / 138.9 | 299.05 / 144.3 | 1,149.13 / 241.7 |
| Bucket / 2,000 per second | Interrupted: 121.43 / 9,585.6 | Not run | 499.76 / 2,234.8 |

TPS counts successful requests completed inside the measurement window. p99
covers scheduled latency of attempted requests, including errors; dropped
requests have no response latency. None of the completed cases passed the full
throughput/latency/delivery qualification. High-load completed TPS is not
sustainable capacity. The single Fleet pair's 7.5% NORMAL improvement needs
repetition; it does not establish a causal or repeatable end-to-end gain.

## Recovery, pressure and publication evidence

| Case | Verification result |
| --- | --- |
| All three Fleet 100 cases | Every ACK passed exact warm and cold retry; FULL had 384 errors and 161 drops, NORMAL 26 errors, celld zero |
| NORMAL Fleet 15K | 105,279 ACKs; warm audit had 1,349 HTTP 503s; drain exceeded 120 seconds; cold audit not reached |
| FULL Fleet 15K | 94,815 ACKs; warm audit had 59,092 HTTP 503s; provider later exited OOM 137, 43 seconds after final metrics; cold audit not reached |
| celld Fleet 15K | 481,602 ACKs; warm audit returned HTTP 500 throughout; live filesystem observation confirmed 100% tmpfs usage and logs reported no space left; cold audit not reached |
| celld Bucket 2K | 166,564 ACKs all passed warm retry; cold audit had 33 HTTP 500s with object-store LIST timeouts; provider remained healthy |
| Interrupted FULL Bucket 2K | 43,708 ACKs all passed warm retry; original owner exited zero; no completed cold audit or case report |

An HTTP audit failure does not prove acknowledged data loss. The failed cold
and drain gates remain failures, not evidence to discard or rerun invisibly.

At Fleet 15K, publication averaged 4,028 ms (FULL) and 4,088 ms (NORMAL), measured
from counter deltas. NORMAL ended with 72.0 MB of unpublished debt and an oldest
publication age of 166 seconds. At Fleet 100, successful publication PUTs per
logical command were 4.93 and 4.81 respectively. These measurements point to
publication/backlog work beyond SQLite synchronization. They exclude provider
SDK-internal retries and do not replace the full M4 cost accounting.

## Isolated SQLite mechanism probe

A separate local probe measured transaction plus commit observation only. It
excluded capture, proof, publication, HTTP and request latency. Every run then
passed exact LTX capture/restore. Three alternating 20,000-transaction pairs
produced these medians:

| Scratch | FULL TPS / transaction p99 µs | NORMAL TPS / transaction p99 µs |
| --- | ---: | ---: |
| tmpfs | 44,768 / 331.2 | 50,756 / 340.5 |
| Docker Linux filesystem volume | 1,582 / 2,513.3 | 70,683 / 62.3 |

These short probes establish the synchronization mechanism, not sustained
framework capacity or physical power-loss durability. A separate traced probe
observed 1,028 fsync calls with FULL and 25 with NORMAL across 1,000 transactions
plus setup/capture/close. Those are total calls, not exact per-command counts.

The isolated NORMAL source passed all contributor checks: 1,890 workspace tests
passed, 38 ignored, and 60 local LTX tests passed. Raw logs, ACK manifests,
source/build hashes, counters and the interrupted matrix are retained under the
external `cellule-write-perf-8ad1` evidence directory. Qualification still requires
three matched runs with all-ACK cold recovery, stable debt, drain and read gates
from the [performance proposal](write-performance-proposal.md).
