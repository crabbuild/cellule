# Composed compaction with lineage: steady writes

The integrated change improves 16-Cell p99 latency but does not establish a
consistent throughput gain. Paired median TPS decreases at every Cell count.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37187678989) ·
[Dataset](2026-10-04-rustfs-composed-lineage-writes.json).

Baseline `fa548bb2cc1993bbf77dc7f4fbef499a0de3f5a4` is compared with
`ce93119794182c01c5d876568799cdb4dbe94628`. Both retain main’s required
root-lineage metadata through the same inline preparation owner.

Three alternating 120-second pairs at each of 1/4/16 Cells use 16 persistent
HTTP clients, four SQL workers, four HTTP workers, five seconds of warmup,
fresh storage prefixes and fixed host budgets on a four-CPU runner.
All 18 points pass: **281,659 measured writes**, **295,630 acknowledged writes**,
and **zero measured errors**. Independent checks cover raw receipts, exact
per-Cell sequences, ownership, replay/conflicts, drain, fresh-file cold recovery
and the next recovered write. The unchanged observation patch and identical
example observer are verified separately from production changes.

| Cells | TPS change | p95 change | p99 change |
| ---: | ---: | ---: | ---: |
| 1 | -3.0% | -7.4% | -7.7% |
| 4 | -2.5% | -5.4% | +0.9% |
| 16 | -3.3% | -2.9% | -23.3% |

Changes are medians of the three paired candidate/baseline ratios, not ratios
of aggregate medians. All nine pairs are retained: two of three pairs lose
TPS at every Cell count. The second 1-Cell pair increases p95 by 6.6% and p99
by 5.6%; the first two 4-Cell pairs increase p99. The third 16-Cell pair loses
14.7% TPS; all three 16-Cell pairs improve p99. Neither reduced operation counts
nor positive unpaired aggregate ratios establish a throughput improvement.

These are a small-row, closed-loop workload on one host and shared RustFS
provider. Response-body throughput is JSON bytes, not database or wire bytes.
Three repeats do not provide a confidence interval or horizontal-scaling proof.
Composed preparation includes compaction/append and admission includes recovery;
its phase populations overlap and differ from baseline standalone preparation.
Do not add phase timings or compound gains with other comparisons.

The [earlier comparison](2026-10-04-rustfs-composed-original-writes.md) predates
lineage integration and remains separate evidence. The next priority is ordinary
publication’s provider-operation count, with full verification and resource
ceilings retained.
