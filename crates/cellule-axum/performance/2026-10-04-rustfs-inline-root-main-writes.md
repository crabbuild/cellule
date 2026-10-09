# Inline roots and composed preparation versus main

The full change improves paired median TPS, p95 and p99 at every Cell count. Eight of nine pairs improve TPS; every pair improves p95 and p99.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37193466378) ·
[Dataset](2026-10-04-rustfs-inline-root-main-writes.json).

Baseline `fa548bb2cc1993bbf77dc7f4fbef499a0de3f5a4`; candidate `9d212463d1429e77147234d31e6b1ee84abe2f55`.
Three alternating 120-second pairs at each of 1/4/16 Cells use 16 persistent
HTTP clients, four SQL workers, four HTTP workers, five seconds of warmup,
fresh storage prefixes and unchanged host budgets on a four-CPU runner.
All 18 points pass the independent evidence audit: **321,663 measured writes**,
**337,961 acknowledged writes**, and **zero measured errors**.

| Cells | TPS change | p95 change | p99 change |
| ---: | ---: | ---: | ---: |
| 1 | +6.6% | -7.9% | -12.7% |
| 4 | +21.2% | -19.8% | -16.7% |
| 16 | +12.4% | -12.9% | -31.8% |

Changes are medians of three paired candidate/baseline ratios. Absolute values
below are independent version medians; their ratios can differ.

| Cells | Baseline TPS | Candidate TPS | Baseline p95 ms | Candidate p95 ms | Baseline p99 ms | Candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 153.1 | 164.4 | 147.6 | 136.0 | 182.8 | 159.7 |
| 4 | 168.4 | 204.1 | 160.3 | 128.6 | 182.7 | 154.4 |
| 16 | 95.4 | 108.4 | 218.8 | 195.4 | 408.5 | 278.7 |

The third 16-Cell pair loses 4.1% TPS and increases p50 by 8.0%, despite lower p95/p99. The third 1-Cell pair increases p50 by 2.5%. All pairs, including regressions, are retained.

Raw request/receipt checks verify every Cell's exact live and cold replay,
ownership refusal, conflict handling, clean drain, fresh SQLite recovery,
fencing-epoch increase and next recovered write. Frozen source/binary hashes,
unchanged observation patch, exact observer transplant, fixed budgets and all
18 phase populations are checked independently and recorded in JSON.

The dataset retains every pair, response-body bytes/second, resource use and
within-window stability. Response throughput measures JSON bodies, not database
or network-wire bytes. Three repeats provide no confidence interval. This is a
small-row closed-loop workload on one host with a shared RustFS provider, not
horizontal scaling. Phase populations include warmup/verification and overlap;
do not add them or infer per-request causality. Do not compound gains from
separate comparisons. Earlier negative results remain historical evidence.
