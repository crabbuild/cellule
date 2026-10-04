# Compaction transfer refill: 16 HTTP clients

Refilling bounded transfers fixes a reproduced stall behind an earlier source,
but does not establish a consistent 16-Cell throughput or latency gain.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37179262998) ·
[Full dataset](2026-10-04-rustfs-compaction-paired-writes.json).

Baseline: `4a98b37914a742ef5568cee40fab24c9f8b3b5a6`.
Candidate: `0b4b4c28d2b47d86fde202487a25362bac1d2766`.
The production difference is body/index transfer refill in completion order,
with descriptor/error order restored and the four-transfer ceiling unchanged.

Three alternating 120-second pairs per Cell count use 16 persistent clients,
1/4/16 independent SQLite Cells, four SQL workers, four HTTP workers, five seconds
of warmup, fresh storage prefixes, and fixed budgets on one four-CPU runner.
Each POST inserts an order and performs a receipt-bound SELECT.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 215.80 | 227.61 | +3.8% | -14.2% | -16.1% |
| 4 | 244.54 | 253.49 | +1.6% | -1.5% | -2.4% |
| 16 | 166.51 | 156.98 | -1.1% | +0.4% | -2.7% |

Changes are medians of paired ratios; TPS columns are independent repeat medians.
Two 16-Cell pairs lose TPS; repeat 2 regresses p95 (+7.0%) and p99 (+3.1%).
One-Cell repeat 3 also regresses p99 (+5.0%). All nine pairs remain in JSON.
Three repeats are not a confidence interval; do not combine this result with
separate grouping runs or interpret it as horizontal scaling evidence.

Verification: all 18 points pass with **462,202 measured writes**, **485,947
acknowledged writes**, and **zero measured errors**. Checks cover raw receipts,
contiguous sequences, retry/conflict handling, ownership, drain, fresh-file cold
recovery, recovery fences, and the next recovered write. Instrumentation is
identical; source/binary hashes, fixed budgets, and phases are retained in JSON.

At 16 Cells, baseline/candidate root-admission means are 50.6/53.7 ms, root-work
means 20.5/22.0 ms, compaction means 136.9/139.3 ms, and publication p99
259.3/263.6 ms. RustFS uses 2.83/2.81 CPU cores. Shared publication admission
and ordinary root work remain priorities. Phase populations differ and cannot
be added to reconstruct request latency. Response throughput counts response-body bytes.
