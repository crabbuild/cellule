# Inline-root routing

All 32 fixed-count write pairs improve TPS and p95; 30 improve p99.
Two object-only local-write pairs at concurrency 16 regress p99.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37192886934) ·
[Dataset](2026-10-04-rustfs-inline-root-routing.json).

Baseline `fa548bb2cc1993bbf77dc7f4fbef499a0de3f5a4`; candidate
`9d212463d1429e77147234d31e6b1ee84abe2f55`. Actual CI checkout
`4a711819cd0ad85784ff7614d50256b55fd76fed` has the identical candidate tree.
Four alternating pairs per mode exercise local/forwarded writes and reads at
concurrency 1/16. The audit verifies 240 rows, 98,304 logical mutations,
304 raw sample/publication hashes, eight coordinator traces and 16 run logs.

| Mode | Write route, 16 clients | TPS change | p95 change | p99 change |
| --- | --- | ---: | ---: | ---: |
| leased | local_command | +9.4% | -14.1% | -13.7% |
| leased | forwarded_command | +11.9% | -14.2% | -15.8% |
| object_only | local_command | +9.2% | -11.2% | -1.8% |
| object_only | forwarded_command | +6.2% | -9.6% | -9.5% |

These are medians of four paired ratios. The canonical aggregate in JSON uses
ratios of independent version medians; the two statistics differ.
All unchanged coarse qualification gates pass, but three 10% read-latency alerts
remain: leased uncached forwarded reads, object-only local reads, and object-only
fresh-client local reads, all at concurrency 16. The dataset retains their
metrics, every write pair, all aggregate read comparisons and original gate limits.

Checks cover frozen source archives/build binaries, raw quantiles, exact root
coverage, recovery and every coordinated stage. Fixed-count routing is separate
from sustained HTTP evidence. Phase timings cover different populations and do
not establish the cause of individual request-tail regressions.
