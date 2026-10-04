# Grouped writes: 64 HTTP clients

Bounded per-Cell grouping improves TPS, p95, and p99 in all nine measured pairs.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37178065659) ·
[Full dataset](2026-10-04-rustfs-grouped-c64-writes.json).

Baseline: `068c65b1a9979aa14817754ef364a806b2fefed6`.
Candidate: `86145968a700cc11fa51c91304a228e2942e2103`.
Both include the completed-request slot handoff fix; the baseline uses pre-group
execution. This measures grouping and associated LTX preparation changes,
without the later compaction refill change.

Three alternating 120-second pairs per Cell count use 64 persistent clients,
1/4/16 independent SQLite Cells, four SQL workers, four HTTP workers, five seconds
of warmup, fresh storage prefixes, and fixed budgets on one four-CPU runner.
Each POST inserts an order and performs a receipt-bound SELECT.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 59.67 | 185.98 | +220.0% | -69.1% | -64.0% |
| 4 | 114.27 | 283.75 | +169.9% | -53.4% | -46.5% |
| 16 | 106.04 | 195.93 | +86.7% | -36.8% | -27.2% |

Changes are medians of paired ratios; TPS columns are independent repeat medians.
Three repeats are not a confidence interval. These single-host results do not
prove horizontal scaling or erase the [16-client regressions](2026-10-04-rustfs-grouped-paired-writes.md).

Verification: all 18 points pass with **343,295 measured writes**, **360,561
acknowledged writes**, and **zero measured errors**. Checks cover raw receipts,
contiguous per-Cell sequences, retry/conflict handling, exclusive ownership,
drain, fresh-file cold recovery, recovery fences, and the next recovered write.
Instrumentation is identical; source/binary hashes and every pair are in JSON.

Roots per acknowledged command fall from 1.000 to 0.262/0.297/0.521 at 1/4/16
Cells. At 16 Cells, baseline/candidate root-admission means are 82.0/84.6 ms,
root-work means 32.7/33.9 ms, and SQL-worker p99 5.7/7.6 ms. Shared publication
admission and provider work remain bottlenecks. Phase populations differ and
cannot be added to reconstruct request latency. Response throughput in JSON
counts response-body bytes, not database bytes or network-wire throughput.
