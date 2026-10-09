# Bounded captured-delta publication

Small native batches previously uploaded one body and index per cut and carried each descriptor into compaction. Eligible batches now publish one verified delta. Original chain admission, source and index checks, endpoint, follower receipts and fenced publication remain required. The decoded working set is bounded; large batches retain file-backed uploads.

This fixed-latency diagnostic uses 7 ms GET and 40 ms PUT delays, with three repetitions for each point. Every final restore matches replay of every original cut. It does not qualify node capacity.

| Cuts | Compaction + append | PUTs, control → candidate | Median prepare ms, control → candidate |
| ---: | :---: | ---: | ---: |
| 1 | no | 4 → 4 | 54.3 → 53.0 |
| 1 | yes | 6 → 6 | 80.7 → 82.1 |
| 4 | no | 10 → 4 | 53.6 → 55.1 |
| 4 | yes | 12 → 6 | 80.0 → 81.7 |
| 16 | no | 34 → 4 | 180.6 → 54.5 |
| 16 | yes | 36 → 6 | 210.8 → 84.3 |

The four-cut reduction removes objects without shortening the existing parallel upload wave. Sixteen cuts formerly required several waves; their preparation now finishes in one. Single-cut behavior remains on its original path. These are stage costs, not end-to-end TPS or latency.

Isolated LTX and runtime suites, local-only LTX, strict Clippy and API docs, format, boundaries, layout, documentation gates and SQL/peer contracts pass. Tests retain the original compaction pressure and external-origin assertions, and add merged lineage, corruption, raw chain limits, retry identity and compressed large-set fallback checks.

The prior PR head failed a host substituted-history test during fenced source-node shutdown. Its original failure remains recorded; this optimization does not claim a CI fix.

The [dataset](2026-10-05-captured-coalescing.json) records frozen source identities, critical metrics and evidence hashes. Raw evidence remains outside Git. The [completed R10 comparison](2026-10-05-captured-coalescing-r10.md) uses the original 2,000-Cell development profile with fresh RustFS per point and 600-second windows, candidate before control. The [original target](node-capacity.md) and all qualification gates remain unchanged.
