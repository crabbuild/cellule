# Grouped-write routing qualification

Both leased and object-only routing modes pass the unchanged gates.
[CI run](https://github.com/crabbuild/cellule/actions/runs/37181423661) ·
[Full dataset](2026-10-04-rustfs-grouped-routing.json).

Baseline `5724959a79be90df1ed8e85f6e123c109455d2f5` is compared with implementation
`98ba66627c208c496cb0626437ad565d2859fe95`, using actual merge build
`8ca9b3248e445a23f9c572bd1e5875f08bc69b5e` with an identical tree. Both binaries
use the same routing test harness. This comparison covers the grouped-write
implementation and associated preparation changes, not isolated components.

Four alternating pairs per mode retain all 15 lanes per point, 1/16 clients,
1,024 raw samples per command lane, and exact recovery of 6,144 mutations per
point. Independent audits verify **98,304 logical mutations**, **240 measurement
rows**, **304 raw sample/publication files**, all actual root ranges and phase
summaries, binary/source-archive hashes, and all 113 coordinated stages per pair.

| Mode | Route, 16 clients | TPS change | p95 change | p99 change |
| --- | --- | ---: | ---: | ---: |
| leased | forwarded | +218.1% | -63.3% | -63.0% |
| leased | local | +220.7% | -64.4% | -63.2% |
| object_only | forwarded | +222.8% | -63.1% | -65.6% |
| object_only | local | +246.7% | -67.5% | -69.7% |

Changes are ratios of four-run medians, following the existing routing gate.
Object-only single-client p99 regresses by 46.7% locally and 11.0% through the
peer; both remain latency alerts in the dataset. Higher authority-publication
tails accompany these alerts; the separate distributions do not prove causality.
The gates allow p95 up to 1.50x, p99 up to 2.00x, and throughput at least 0.90x.

These are fixed-count routing lanes on a shared provider, not 120-second steady
HTTP or horizontal scaling measurements. All lanes, regressions, and provenance
are retained in JSON; no universal latency improvement is claimed.
