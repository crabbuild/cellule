# Multiple Cells through Axum and RustFS — 2026-10-03

All 12 runs passed, with **6,144 measured writes, 24,576 measured reads,
zero unexpected HTTP errors, and 87 individual Cell restores**. With four
SQL workers and 16 HTTP clients held constant, median write throughput rose
from 25.44 requests/s with one Cell to 114.65 with eight Cells (4.51×).
Sixteen Cells had similar median throughput, 113.23 requests/s.

## Comparison profile

| Setting | Value |
| --- | --- |
| Application | [Axum SQL orders service](../examples/sql.rs), one native process |
| Framework path | `Cellule` extractor → typed application/SQL capability → `CellClient::local_many` → runtime → LTX → real RustFS |
| Cell counts | 1, 4, 8, 16; separate database, writer, publication root, and request ledger per Cell |
| Worker/resource profile | Four SQL workers, 16 active-Cell slots, 16 MiB runtime setting and 1 GiB local disk budget, constant across all points |
| HTTP concurrency | 16 persistent HTTP/1.1 clients, closed loop, separate Python driver process |
| Work per point | 16 untimed warmup writes, 512 timed writes, then 2,048 timed reads |
| Routing | Euclidean `order_id mod active_cells`; all work counts divisible by every tested Cell count |
| Repeats | Three, each with fresh S3 prefixes and temporary SQLite directories |
| Durable POST | `INSERT`, publication, receipt-bound `SELECT`, then HTTP 201 with the write receipt |
| GET | Owner-ordered local `SELECT`, HTTP 200 with an observed receipt |
| Host | Apple M2 Max, 12 logical CPUs, 32 GiB, macOS 26.5.2 |
| Provider VM | Ubuntu 24.04.4 aarch64, 8 CPUs, 15.58 GiB, Docker 29.5.2 |
| RustFS image | `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858` |
| Build | Rust 1.97.0 release, isolated source snapshot; base `cf2bbb2003e7d3fa4a6e8472a53530b3fc4b23bb` plus these multicell changes |
| Provider/prefix | `http://127.0.0.1:19751`, bucket `cellule-cookbook`, `axum-multicell-perf-20261003-54f709e0-a/r{repeat}-n{cells}-c16` |

Only the active Cell count varies within the comparison. Each Cell receives
512 / N timed writes and 2,048 / N timed reads. Successful receipts must
identify the exact Cell selected by the service's routing policy.

Latency includes sending the HTTP request and reading the full response body.
Throughput includes the client loop; connections are established before timing.
Warmup, startup, correctness checks, and restart are untimed. Percentiles use
nearest rank over all attempts; no failed responses are discarded or retried
automatically.

## Measured results

Values are medians of three per-run statistics; rate ranges show repeat
variation. Percentiles are not pooled across runs.

Durable writes, including the receipt-bound read:

| Cells | Requests/s | Rate range | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 25.44 | 14.61–37.37 | 586.01 | 1,680.18 | 1,709.96 |
| 4 | 95.74 | 41.27–112.06 | 145.82 | 313.42 | 346.36 |
| 8 | 114.65 | 94.32–138.03 | 115.87 | 246.49 | 441.73 |
| 16 | 113.23 | 50.40–145.62 | 107.32 | 220.31 | 838.04 |

Reads of acknowledged orders:

| Cells | Requests/s | Rate range | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8,563 | 7,900–9,589 | 1.441 | 4.316 | 6.077 |
| 4 | 10,964 | 6,143–13,910 | 1.158 | 3.295 | 4.602 |
| 8 | 8,984 | 8,397–14,684 | 1.351 | 4.149 | 7.106 |
| 16 | 9,231 | 7,606–10,221 | 1.404 | 3.764 | 5.395 |

Write throughput improved when work was spread across independent writers,
then plateaued around eight to 16 Cells in this profile. Read throughput did
not increase consistently. This shared workstation/provider VM has no CPU,
storage, or network isolation; repeat ranges remain wide. The fixed total
write count also means each Cell has a shorter request ledger and LTX history
at higher Cell counts. This characterizes distributing one workload across
Cells; it does not isolate parallelism from history depth, locate a bottleneck,
or qualify production capacity. GETs access the restored owner's local SQLite.
Distributed hosts, peers, abrupt process loss, and provider faults are outside
this comparison.

## Per-Cell verification

| Gate | Evidence across all 12 runs |
| --- | --- |
| Storage semantics | Six capability probes passed on every service start |
| Distinct routing | HTTP receipts proved all requested Cells were active and distinct |
| Published sequences | Every Cell had exactly sequences 1 through 528 / N before restart; no gaps or duplicates |
| Correct data/minimum | 6,336 warmup/measured orders had correct outputs; every timed GET proved its own Cell's write receipt |
| Live retry | All 6,336 original envelopes returned their exact original output/receipt without advancing publication |
| Conflict/expiry | 87 changed inputs returned 409 `request_conflict`; 87 expired identities returned 400 `invalid_request`, without data/publication changes |
| Active owner | All 12 competing service starts refused acquisition while the original service continued |
| Drain | Every Cell released authority to Idle; processes exited successfully and removed temporary SQLite directories |
| Cold recovery | 87 Cells restored their authority-pinned roots with `acquire_idle_restored` into new local files |
| State/request ledger | All 6,336 orders read back after restart; all 6,336 envelopes replayed with their original receipts |
| Independent ledgers | A shared request ID with different SQL inputs committed and replayed independently on each of the 87 recovered Cells |
| Recovered publication | Each Cell published exactly its next sequence and drained under a higher ownership epoch |

Cold readiness included provider probing, catalog lookup, acquiring every
Cell, restoring its database, and binding HTTP. Median times for 1 / 4 / 8 /
16 Cells were 108 / 216 / 862 / 583 ms. Startup polling has 50 ms granularity;
these timings vary and are not steady request latencies.

## Reproduction and evidence

Use the [setup commands](../README.md#verify-http-against-rustfs) with
`--repeats 3 --cells 1 4 8 16 --workers 4 --concurrency 16 --warmup 16
--writes 512 --reads 2048`. Restart the service with the same Cell count and
binary, because Cell count determines order routing. Each comparison point
must use a new S3 prefix.

[Aggregate JSON](2026-10-03-rustfs-multicell.json) records all per-Cell
shutdown sequences/epochs, receipts, counters, startup times, environment,
and measured source hashes. Binary SHA-256:
`f00bff2380c3a1a6987763db12e3e8c57c1914aec4100989b8bd95b7d602fea2`.
Raw envelopes, per-request responses/timings, and logs remain at
`$HOME/Workspace/crabbuild-target/cellule-axum-multicell-54f709e0/results-20261003`;
the provider retains its S3 objects.

Local checks passed: release build; adapter integration tests and doctest;
full workspace Clippy with Rust 1.99.0 and warnings denied; architecture,
module layout, formatting, documented Rust syntax, and links. CI runs a
smaller 1/4-Cell × 1/4-client matrix with these same correctness gates and
retains its evidence. The optional error receipt is boxed internally to keep
`HttpError` compact for newer Clippy; HTTP evidence and source errors are
preserved by the existing integration tests.
