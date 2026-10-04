# Composed compaction: original steady-write comparison

The original implementation improves 1-Cell throughput and 16-Cell tail latency,
but does not establish consistent throughput gains across Cell counts.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37186946941) ·
[Dataset](2026-10-04-rustfs-composed-original-writes.json).

Baseline `4c1c9826322b154a4dd2af6f86b89cd586e871a7` is compared with
`ce1f918a76e1edabbdab0128e9debe28fe56ec38`. This precedes integration of main’s
required root-lineage retention; it does not qualify the later integrated source.

Three alternating 120-second pairs at each of 1/4/16 Cells use 16 persistent
HTTP clients, four SQL workers, four HTTP workers, five seconds of warmup,
fresh storage prefixes and fixed host budgets on a four-CPU runner.
All 18 points pass: **321,665 measured writes**, **337,978 acknowledged writes**,
and **zero measured errors**. Independent checks cover raw receipts, exact
per-Cell sequences, ownership, replay/conflicts, drain, fresh-file cold recovery
and the next recovered write. The unchanged observation patch and identical
example observer are verified separately from production changes.

| Cells | TPS change | p95 change | p99 change |
| ---: | ---: | ---: | ---: |
| 1 | +10.3% | -16.4% | -13.0% |
| 4 | -1.9% | -6.6% | -6.9% |
| 16 | +2.8% | -27.0% | -30.2% |

Changes are medians of the three paired candidate/baseline ratios, not ratios
of aggregate medians. All nine pairs are retained: two of three 4-Cell pairs
lose throughput, and the third repeat increases p99 by 4.7%; the first
16-Cell pair loses 0.7% throughput. All three 16-Cell pairs improve p95 and p99.

These are a small-row, closed-loop workload on one host and shared RustFS
provider. Response-body throughput is JSON bytes, not database or wire bytes.
Three repeats do not provide a confidence interval or horizontal-scaling proof.
Composed preparation includes compaction/append and admission includes recovery;
its phase populations overlap and differ from baseline standalone preparation.
Do not add phase timings or compound gains with other comparisons.
