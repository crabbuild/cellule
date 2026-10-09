# Root publication phase calibration

The longer runs identify shared publication admission and provider work as the
main write costs. Directory processing is negligible.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37182201738) ·
[Full dataset](2026-10-04-rustfs-root-phase-calibration.json).

Baseline `98ba66627c208c496cb0626437ad565d2859fe95` and observation candidate
`b1a55d8d1cb994edc6691cc09ff19051cb115b6a` have identical framework production
code. Both use the same observer, collecting existing root-open and directory
phase events. This is a cost calibration, not an optimization trial.

Three alternating 120-second pairs per Cell count use 16 persistent HTTP clients,
1/4/16 independent SQLite Cells, four SQL workers, four HTTP workers, five seconds
of warmup, fresh storage prefixes, and fixed budgets on one four-CPU runner.
All 18 points pass: **424,917 measured writes**, **447,161 acknowledged writes**,
and **zero measured errors**. Independent checks cover raw receipts, per-Cell
sequences, replay/conflict handling, exclusive ownership, drain, fresh-file cold
recovery, recovery fences, and the next recovered write. Source/observer diffs,
patch hashes, budgets, and every measured pair are retained in JSON.

| Phase | 1 Cell | 4 Cells | 16 Cells |
| --- | ---: | ---: | ---: |
| Root admission mean, ms | 0.004 | 0.004 | 57.999 |
| Root work mean, ms | 7.378 | 15.450 | 23.786 |
| Authority publication mean, ms | 3.473 | 8.682 | 15.222 |
| Provider PUT mean, ms | 4.753 | 10.225 | 16.404 |
| Root open mean, ms | 1.067 | 2.628 | 4.097 |
| Directory mean, ms | 0.015 | 0.028 | 0.023 |
| SQL worker mean, ms | 0.885 | 1.126 | 1.135 |
| SQL worker p99, ms | 5.400 | 8.650 | 9.800 |
| Compaction mean, ms | 27.339 | 49.383 | 149.524 |

Each value is the median of six point means or p99 values at that Cell count,
not a pooled request statistic. Phases have different populations, include
warmup and some verification, and overlap; do not add them to reconstruct
request latency. PUT observations include authority and immutable writes.
Three repeats are not a confidence interval or horizontal scaling evidence.

The next candidate targets [intermediate compaction metadata](2026-10-04-compaction-publication-cost.md).
It must retain origin verification, complete dependency uploads, byte-identical
final roots, exact durable replies, and the existing resource ceilings.
