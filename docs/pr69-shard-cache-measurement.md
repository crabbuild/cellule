# Catalog shard preparation cache

A bounded preparation cache improved mixed throughput in one short comparison,
but **the capacity target remains unmet**. Successful writes increased
667.90→771.42/s and reads 6,694.00→7,744.17/s. Both cases dropped offered work,
had high write latency, and accumulated root-materialization debt.

## Change and correctness boundary

The managed publisher retains up to 256 encoded catalog shards within a
separately admitted **2 MiB**, including index overhead. Its existing working
reservation remains 20 MiB. Startup reserves both before installing the feed;
shutdown and failed startup release their original reservations. The existing
32-MiB admission regression still passes with the 22-MiB total reservation.

Preparation reads the canonical header fresh, matches the exact immutable
object/range/digest under the original store, path, session and epoch, and
decodes cached bytes through the normal validator. Changed proposal shards
fill the cache after upload. This creates no authority or availability proof.
Histories, native bodies, selection, closure and cold recovery retain their
existing origin checks. The wire format, 64-frame bound and 4-MiB object bound
are unchanged.

A 2,000-binding regression verifies byte-identical cached and uncached proposals:
the warm path makes two catalog range reads instead of three. Disabling lookup
makes that assertion fail. Corrupt headers, corrupt native dependencies and
fencing still fail closed. Scope isolation, byte eviction, startup refusal and
resource release have explicit tests.

## Matched diagnostic

The baseline is `f088978cd0dd01afb5a2027b6fe30ebbe38c621f`. Both cases use 2,000
resident Cells, 96-byte SQL values, 30 seconds of warmup and 60 measured seconds
per phase. The mixed phase offers 2,000 writes/s and 20,000 reads/s simultaneously.
The owner has 8 CPUs and 16 GiB; followers, RustFS and clients use the separate
support CPUs. Both use the same 256-MiB retained-work ledger, 1-GiB disk policy,
128-request concurrency/queue bounds, RustFS image and disabled provider THP.
Load-generator and audit binaries are byte-identical. No additional Rust timing
instrumentation is installed; provider memory sampling is identical.

| Measurement | Baseline | Cache |
| --- | ---: | ---: |
| Mixed successful writes/s | 667.90 | 771.42 |
| Mixed successful reads/s | 6,694.00 | 7,744.17 |
| Write request p50 / p99, ms | 130.1 / 588.6 | 110.4 / 575.3 |
| Read request p99, ms | 100.3 | 77.0 |
| Dropped write / read offers | 79,787 / 798,243 | 73,576 / 735,233 |
| Returned write / read errors | 0 / 0 | 0 / 0 |
| Read-only successful reads/s | 19,396.62 | 18,991.32 |
| Owner range GETs in mixed metric window | 99,624 | 70,497 |
| Owner range bytes read in that window | 705,449,168 | 500,657,266 |
| Root-materialization debt, start→end bytes | 71,128,764→99,451,403 | 97,937,214→133,378,917 |
| Acknowledgements audited warm and cold | 80,277 | 96,122 |
| Joined drain, seconds | 55.99 | 58.53 |
| Cold startup, seconds | 33.86 | 38.27 |

The range counters include all owner range reads, not just catalog shards.
Every observed write-response increment in both mixed metric windows used the
Fleet proof. Independent raw-log reconciliation matches acknowledged contents,
request IDs, admitted counts, dropped offers and per-Cell counts. Every recorded
acknowledgement passes warm and cold state and original-request retry checks.

This is one exploratory pair on tmpfs, not device or sustained-capacity
qualification. Read-only throughput was lower in the candidate; the pair does
not isolate that difference. Mixed writes improved about 15.5%, but their p99
barely changed and both debt series grew. Neither case passes the unchanged
qualification gates or establishes parity with celld. Serial publication and
root materialization remain performance work.

## Evidence and validation

All 13 isolated contributor routes pass: 2,081 workspace tests pass and 43
environment-dependent tests remain ignored. The six TLS-backed authority tests
also pass. The bundle suite has 123 passing tests, including the new cache
regressions. Separate host maintenance CI diagnostics are tracked in the delivery
log; these contributor routes do not replace that executable scenario gate.

The external artifact root is `cellule-ios-parity-20261010`. The cases are
`shard-cache-baseline-trial/cellule-fleet-parity-shard-cache-baseline-r1` and
`shard-cache-candidate-trial/cellule-fleet-parity-shard-cache-candidate-r1`.
Each preserves build/source manifests, resource placement, raw client logs,
ACK manifests, metrics, provider evidence and warm/cold audits. Companion files
include `shard-cache-comparison.json`, both `shard-cache-*-independent-audit.json`
files, `shard-cache-source-delta.json`, `shard-cache-source-comparison.json` and
`shard-cache-verification-routes.json`.

| SQL executable | SHA-256 |
| --- | --- |
| Baseline | `98673fe11be8f288e3093cfadcc1d2ccbf2a06d4f930bc691f1470bcc5a3d5f7` |
| Cache | `778b4f78757772b97e2d89953132f99503c5bdade528393dda231594f94269ec` |

See [delivery status](write-performance-delivery.md) and the
[shared durability design](../crates/cellule-runtime/docs/write-performance-design.md).
