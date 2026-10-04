# Inline roots versus composed preparation

Inline roots improve paired median TPS and p95 at every Cell count. Small-cell p99 and one 4-Cell pair regress.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37192915849) ·
[Dataset](2026-10-04-rustfs-inline-root-writes.json).

Baseline `c78ea0f48d243377ddee6308640966955de4ad35`; candidate `9d212463d1429e77147234d31e6b1ee84abe2f55`.
Three alternating 120-second pairs at each of 1/4/16 Cells use 16 persistent
HTTP clients, four SQL workers, four HTTP workers, five seconds of warmup,
fresh storage prefixes and unchanged host budgets on a four-CPU runner.
All 18 points pass the independent evidence audit: **316,933 measured writes**,
**332,754 acknowledged writes**, and **zero measured errors**.

| Cells | TPS change | p95 change | p99 change |
| ---: | ---: | ---: | ---: |
| 1 | +3.7% | -4.3% | +6.2% |
| 4 | +21.6% | -13.8% | +0.6% |
| 16 | +11.1% | -8.1% | -8.8% |

Changes are medians of three paired candidate/baseline ratios. Absolute values
below are independent version medians; their ratios can differ.

| Cells | Baseline TPS | Candidate TPS | Baseline p95 ms | Candidate p95 ms | Baseline p99 ms | Candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 157.4 | 164.3 | 140.2 | 136.1 | 170.2 | 168.2 |
| 4 | 161.5 | 182.8 | 168.5 | 145.2 | 193.5 | 181.7 |
| 16 | 97.0 | 109.1 | 211.2 | 192.1 | 308.4 | 273.7 |

All three 1-Cell pairs improve TPS/p95, but two increase p99 (one by 47.5%). The first 4-Cell pair loses 18.4% TPS and increases p95/p99; two of three 4-Cell pairs increase p99. All three 16-Cell pairs improve TPS, p95 and p99.

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
