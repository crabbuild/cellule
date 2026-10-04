# Queued native writes with 64 HTTP clients

Bounded per-Cell grouping improves throughput and p95/p99 in every measured
pair at 1, 4, and 16 Cells. These are queued-write results on one shared
four-CPU runner, not horizontal scaling measurements.

[Completed paired run](https://github.com/crabbuild/cellule/actions/runs/37178065659)
compares baseline `068c65b1a9979aa14817754ef364a806b2fefed6` with candidate
`86145968a700cc11fa51c91304a228e2942e2103`. Both include the completed-request
slot handoff fix. The baseline retains pre-group execution; it is an explicit
corrected baseline, not an observation-only version of `ec19f8c`. The candidate
does not include the later compaction transfer-window change.

## Protocol and verification

Three alternating pairs use 64 persistent HTTP clients, 1/4/16 independent
Cell databases, four SQL workers, four HTTP Tokio workers, five seconds of
timed warmup, and 120 seconds of measured admission per point. Each POST inserts
an order and performs a receipt-bound SELECT. Both release servers are built
before load; every point has a fresh storage prefix.

All eighteen points complete with zero write errors: **343,295 measured writes**
and **360,561 acknowledged writes** including warmup. Independent raw-client
verification checks every timed receipt, request identity, and contiguous
per-Cell sequence. Harness checks cover exact and conflicting retries, expired
identities, exclusive ownership, drain, fresh-file cold recovery of every
acknowledged row, changed recovery fences, and the next recovered command.

The baseline instrumentation diff is empty. The retained observation patch
matches the fixed repository fixture. All four patch targets, the example
metrics observer, the workload driver, and `Cargo.lock` are byte-identical
between the two revisions. The [complete dataset](2026-10-04-rustfs-grouped-c64-writes.json)
retains all nine pairs, raw-client hashes, binary hashes, source provenance,
phase distributions, provider CPU, and interval stability.

## Results

Changes are medians of three paired candidate/baseline ratios. TPS columns
are independent repeat medians. Latencies are HTTP write request milliseconds;
lower is better.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 59.67 | 185.98 | +220.0% | -69.1% | -64.0% |
| 4 | 114.27 | 283.75 | +169.9% | -53.4% | -46.5% |
| 16 | 106.04 | 195.93 | +86.7% | -36.8% | -27.2% |

| Cells | Repeat | TPS change | p95 change | p99 change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | +209.2% | -70.8% | -64.0% |
| 1 | 2 | +220.0% | -66.9% | -62.3% |
| 1 | 3 | +235.0% | -69.1% | -67.2% |
| 4 | 1 | +128.7% | -46.2% | -51.7% |
| 4 | 2 | +185.6% | -53.4% | -42.5% |
| 4 | 3 | +169.9% | -55.5% | -46.5% |
| 16 | 1 | +87.2% | -37.0% | -28.9% |
| 16 | 2 | +86.7% | -36.8% | -27.2% |
| 16 | 3 | +84.0% | -35.9% | -26.8% |

Three repeats are not a statistical confidence interval. The earlier
[sixteen-client comparison](2026-10-04-rustfs-grouped-paired-writes.md) still
records sixteen-Cell p99 regressions. More concurrent work per Cell enables
grouping here; these results do not erase that lower-concurrency finding.

## Publication cost and the remaining bottleneck

| Cells | Roots per acknowledged command, baseline / candidate | Uploaded objects per command, baseline / candidate | RustFS CPU cores, baseline / candidate |
| --- | ---: | ---: | ---: |
| 1 | 1.000 / 0.262 | 5.612 / 1.945 | 2.369 / 2.361 |
| 4 | 1.000 / 0.297 | 5.316 / 1.782 | 2.712 / 2.831 |
| 16 | 1.000 / 0.521 | 5.260 / 2.743 | 3.023 / 2.955 |

Values are independent repeat medians. Phase counts include warmup; object
counts also include startup. Grouping amortizes actual publication and upload
work without increasing budgets or relaxing durable response gates. Each Cell
already has its own SQLite state; the workers and storage resources are shared.

At sixteen Cells, median root-admission mean is 82.0 ms in the baseline and
84.6 ms in the candidate; admitted root-work mean is 32.7/33.9 ms. SQL-worker
p99 is 5.7/7.6 ms. Four Cells have negligible root-admission waits and higher
aggregate TPS than sixteen Cells. These observations identify shared publication
admission and provider work as the next performance priorities; they do not
justify raising concurrency limits without measuring provider saturation and
tail latency. Phase distributions have different populations and cannot be
added to reconstruct an individual HTTP request.

The separate compaction comparison must establish whether refilling bounded
transfer windows improves end-to-end throughput and latency. Multi-node
qualification is also required before claiming horizontal scaling.
