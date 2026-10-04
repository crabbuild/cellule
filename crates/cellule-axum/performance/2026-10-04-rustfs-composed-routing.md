# Composed compaction routing qualification

Both routing modes pass the unchanged gates, with mixed performance and retained
forwarded-write latency alerts.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37187645453) ·
[Dataset](2026-10-04-rustfs-composed-routing.json).

Baseline `fa548bb2cc1993bbf77dc7f4fbef499a0de3f5a4` is compared with
`ce93119794182c01c5d876568799cdb4dbe94628`, built from actual merge
`1ca9759282fb5b1d5a15059b8b235bf34c819f2e` with an identical source tree.
Both frozen binaries use the same routing harness.

Four alternating pairs per mode retain all 15 lanes per point, 1/16 clients,
1,024 raw samples per command lane, and exact recovery of 6,144 mutations per
point. Independent audits verify **98,304 logical mutations**, **240 measurement
rows**, **304 raw sample/publication files**, every root range and phase summary,
binary/build/source-archive hashes and all 113 coordinated stages per pair.

| Mode | Route, 16 clients | TPS change | p95 change | p99 change |
| --- | --- | ---: | ---: | ---: |
| leased | forwarded | -2.4% | -1.3% | +20.9% |
| leased | local | +4.9% | -10.8% | -21.4% |
| object_only | forwarded | -0.1% | -2.0% | +50.5% |
| object_only | local | +10.8% | -17.9% | -31.4% |

Changes are ratios of four-run medians, following the existing routing gate.
Forwarded-write p99 increases by 20.9% in leased mode and 50.5% in object-only
mode; both remain alerts. Leased forwarded queries at one client and object-only
queries with an uncached route at 16 clients also alert. The gates allow p95
up to 1.50x, p99 up to 2.00x and throughput at least 0.90x. Passing these gates
does not prove universal latency improvement.

These are fixed-count routing lanes on a shared provider, not 120-second steady
HTTP or horizontal-scaling measurements. All lanes, regressions and provenance
are retained in JSON. Composed phase populations include compaction/append and
overlap; separate distributions cannot be added or used to infer causality.
