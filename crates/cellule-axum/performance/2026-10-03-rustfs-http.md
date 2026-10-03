# Axum HTTP and RustFS verification — 2026-10-03

The real `cellule-axum` orders service passed all nine runs, including cold
recovery and exact request replay. There were **zero unexpected HTTP errors**
in 4,500 measured writes and 18,000 measured reads. Median write throughput
was about 16 requests/s at each concurrency level. Higher concurrency raised
write tail latency substantially; this run does not establish production
capacity or isolate the adapter's overhead.

## Workload and environment

| Item | Setting |
| --- | --- |
| Framework base | `39a3d0b2a2ae8138a3daa55281d798d1c2c9e62c`, plus the S3/restart example changes in this PR |
| HTTP application | [SQL orders example](../examples/sql.rs), Axum 0.8.9, real TCP HTTP/1.1 |
| Framework path | `Cellule<OrdersApp>` → `ApplicationHandle` → typed SQL capability → runtime → LTX → RustFS |
| Write | One `INSERT`, durable publication, then a receipt-bound `SELECT`; HTTP 201 carries the original write receipt |
| Read | Owner-ordered local SQL `SELECT`; HTTP 200 carries an observed receipt |
| Topology | One SQL Cell, one native service process, one SQL worker; the example's unchanged queue/budget settings |
| Load | Separate Python process; persistent connection per worker, closed loop, concurrency 1 / 4 / 16 |
| Samples | Three repeats per concurrency; fresh S3 prefix per point; 20 untimed warmup writes, 500 measured writes, then 2,000 measured reads |
| Build | Release, Rust 1.97.0, `aarch64-apple-darwin`; isolated source snapshot and target directory |
| Workstation | Apple M2 Max, 12 logical CPUs, 32 GiB RAM, macOS 26.5.2 |
| Docker provider VM | Ubuntu 24.04.4, aarch64, 8 CPUs, 15.58 GiB RAM, Docker 29.5.2 |
| RustFS | `ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858` |
| Provider | `http://127.0.0.1:19751`, bucket `cellule-cookbook`, private Compose project `cellule-axum-perf-54f709e0` |
| Prefix | `axum-http-perf-20261003-54f709e0-a/r{repeat}-c{concurrency}` |

Timings include sending the HTTP request and consuming the response body.
JSON validation follows the timed request; throughput includes client loop
overhead. Connection establishment, warmup, startup, retries, and recovery
checks are excluded from the steady workload timings. Percentiles use nearest
rank over all attempts. There were no discarded errors or automatic retries.

## Measured HTTP performance

Each statistic below is the median of the three per-run statistics. The rate
range shows the minimum and maximum per-run throughput; percentiles are not
pooled across repeats.

Durable POST, including the receipt-bound read:

| Concurrent clients | Requests/s | Rate range | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 15.94 | 12.98–33.34 | 32.00 | 204.88 | 433.30 |
| 4 | 16.42 | 12.47–30.38 | 139.32 | 784.28 | 1,487.32 |
| 16 | 16.19 | 9.27–34.56 | 715.34 | 2,301.26 | 4,163.67 |

Owner-ordered GET of a previously acknowledged order:

| Concurrent clients | Requests/s | Rate range | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,527 | 1,521–3,798 | 0.162 | 0.705 | 1.664 |
| 4 | 1,544 | 1,432–5,518 | 0.630 | 5.395 | 36.858 |
| 16 | 5,940 | 5,530–7,874 | 2.001 | 6.800 | 11.078 |

This Cell has one fenced writer. Increasing HTTP concurrency queues more work
against that writer and did not consistently improve write throughput here.
The repeat ranges are wide. This was a shared workstation/provider VM: a
single resource sample saw nine other running containers, about 30% aggregate
CPU usage by those containers, and about 134% CPU usage by this RustFS
container. No native or provider CPU, storage, or network isolation was imposed.
These measurements do not locate the cost within HTTP, the runtime, SQLite,
LTX, or the provider; profiling and a controlled host are needed for that.
The Python load generator can also limit read throughput.

## Framework correctness through the adapter

The [runner](../../../scripts/bench-axum-rustfs.py) fails on a wrong status,
output, receipt, publication sequence, ownership result, or recovery result.
It exercised the production framework APIs and the same HTTP handlers used
for measurement.

| Check | Observed evidence |
| --- | --- |
| Provider semantics | Every service start passed all six storage probes: conditional create/reject, current/stale ETag CAS, ranges, read-after-write |
| One active owner | Nine competing service starts refused the already serving Cell; the original writer continued |
| Durable write and read | All 4,680 warmup/measured orders had correct outputs, one stable Cell/incarnation, and exactly contiguous commit sequences 1–520 per point |
| Read ordering | Every timed GET returned the acknowledged order with a receipt at least as new as its write |
| Exact retry | All 4,680 original POST envelopes replayed successfully with the exact original output and receipt, without advancing the published sequence |
| Conflicting identity | Nine changed inputs under the original identity returned HTTP 409 `request_conflict`; no row or publication change |
| Invalid identity | Nine expired identities returned HTTP 400 `invalid_request`; their row stayed absent |
| Graceful shutdown | Every completed service exited successfully, released authority to Idle, and removed its temporary SQLite directory |
| Cold restore | Nine fresh service processes used new temporary SQLite directories and `acquire_idle_restored`, verifying/restoring the authority-pinned RustFS root |
| Recovered state/ledger | All 4,680 orders read back correctly after restore; all 4,680 original envelopes still returned the exact original receipts |
| Recovered writer | Each restored service published order 521, read it back, then drained at sequence 521 under a higher ownership epoch |

Initial readiness took a median 170 ms (range 106–1,808 ms). Cold readiness
for the 520-order Cell took a median 223 ms (range 161–2,226 ms), including
the provider probe, catalog lookup, ownership claim, restore, and listener
setup. Readiness uses 50 ms polling, so these are coarse startup measurements.

This verifies graceful restart from durable objects. It does not test an
abrupt process loss, missing/corrupt objects, distributed peers, node lease
takeover, multitenant authorization, or network fault outcomes. GETs use the
active owner's local SQLite after restoration; they are not S3 GET benchmarks.

## Evidence and reproduction

[Aggregate JSON evidence](2026-10-03-rustfs-http.json) retains all nine points,
environment details, binary/source hashes, error counts, receipts, startup
times, and shutdown epochs. The measured binary SHA-256 is
`423146e719e4282b74520d4d7df7d20ec13c9dcd53ea398cbf3f47c96822b4d4`.
Raw envelopes, per-request responses/timings, and service logs are retained
locally at `$HOME/Workspace/crabbuild-target/cellule-axum-rustfs-54f709e0/results-20261003`.
S3 objects are retained in the private Compose volume for inspection.

Follow the [RustFS setup commands](../README.md#verify-http-against-rustfs).
The measured command used `--repeats 3 --concurrency 1 4 16 --writes 500
--reads 2000 --warmup 20`. A smaller CI smoke runs the same correctness gates
with debug binaries, two concurrency levels, and no performance thresholds;
its timings are not compared with this release measurement.

Local verification passed: release build; seven adapter integration tests and
the README doctest; all workspace targets/features check; adapter Clippy with
warnings denied; formatting, boundaries, module layout, Rust fences, and
documentation links. Full workspace tests and lints also run in PR CI.
