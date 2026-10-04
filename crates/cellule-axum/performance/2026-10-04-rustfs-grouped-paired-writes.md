# Grouped writes: 16 HTTP clients

Grouping improves sustained queued writes at one and four Cells; the 16-Cell
profile does not form groups and regresses tail latency.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37173160571) ·
[Full dataset](2026-10-04-rustfs-grouped-paired-writes.json).

Baseline: `ec19f8c638ffb3840c50062947a72f1af7ff3875`.
Candidate: `1e21c171ac38f1de4b180fc61c5365df4a178c26`.
Production grouping changes: `16ac5d85b72ea9b5a8970fc8eda029927679027e`.

Three alternating 120-second pairs per Cell count use 16 persistent clients,
1/4/16 independent SQLite Cells, four SQL workers, four HTTP workers, five seconds
of warmup, fresh storage prefixes, and fixed budgets on one four-CPU runner.
Each POST inserts an order and performs a receipt-bound SELECT.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 57.66 | 147.85 | +156.3% | -57.2% | -57.1% |
| 4 | 114.07 | 163.19 | +64.3% | -22.4% | -16.2% |
| 16 | 104.45 | 104.54 | -3.1% | +6.2% | +4.5% |

Changes are medians of paired ratios; TPS columns are independent repeat medians.
All 16-Cell pairs regress p99; four-Cell repeat 1 also regresses p99 (+2.7%).
Every pair remains in JSON. Three repeats are not a confidence interval, and
these single-host results do not prove horizontal scaling.

Verification: all 18 points pass with **255,428 measured writes**, **268,020
acknowledged writes**, and **zero measured errors**. Checks cover raw receipts,
contiguous sequences, retry/conflict handling, ownership, drain, fresh-file cold
recovery, recovery fences, and the next recovered write. Instrumentation is
identical; source/binary hashes are retained in JSON.

Roots per acknowledged command fall from 1.000 to 0.320/0.583/1.000 at 1/4/16
Cells. At 16 Cells, root admission averages about 83 ms, admitted root work
about 33 ms, and SQL-worker p99 remains below 7 ms. Shared publication/provider
capacity remains a bottleneck; these phases do not explain individual p99
regressions and cannot be added to reconstruct request latency.

A later [64-client attempt](https://github.com/crabbuild/cellule/actions/runs/37175252712)
failed during baseline warmup with 14 HTTP 503 responses, before measurement.
An [error-source probe](https://github.com/crabbuild/cellule/actions/runs/37176372337)
identified completed requests retaining mailbox slots after durable reply delivery.
The [completed corrected comparison](2026-10-04-rustfs-grouped-c64-writes.md)
includes the slot handoff fix in both revisions. The failed attempts remain
part of the evidence. Response throughput counts response-body bytes.
