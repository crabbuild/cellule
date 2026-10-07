# Write performance implementation and verification

The implementation packs small publication dependencies and provides a pinned
Docker comparison with exact retry and cold-state audits. **Celld write parity
has not been established.** The [proposal](write-performance-proposal.md) remains
the acceptance contract; completing tests or a load run does not pass its gates.

## Delivered behavior

| Change | Measurable result | Preserved contract |
| --- | --- | --- |
| Small native LTX and index share one `.pack` | One dependency PUT instead of two; at most 256 KiB | Exact native bytes, body/index digests, complete object digest |
| Small directory leaf lives in the root | Removes its separate PUT, GET and cached-origin HEAD | Canonical leaf validation; 2 KiB leaf and 32 KiB root bounds |
| Packed compaction input supplies both scratch streams | One full GET per selected pack instead of full plus range GET | Complete verification; bounded transfer and file ownership through cancellation |
| Window telemetry separates response, proof and publication | Logical commands per selected root; capture/checkpoint, worker, peer and sync histograms | Counters and frontiers confer no authority or proof |
| Storage families distinguish owner/receiver enrollment | Summed enrollment GET cost across all three nodes | Each signed peer message still uses fresh authorization |
| Docker runner and reports preserve failures | Source/binary identities, fresh provider volumes, scheduled-arrival latency, all-ACK audits | Errors, drops, unissued offers, provider failures and failed drain cannot pass |

An ordinary small root needs two immutable PUTs plus lineage and fenced Cell
selection: **four successful PUTs instead of six**. A small scheduled
compaction composed with an append still needs two packs, the final root,
lineage and selection: **five PUTs**. Node coverage and maintenance remain in
the window numerator. M1's universal four-PUT gate has therefore not passed.

The current root development format is version 2. The
[format specification](../crates/cellule-ltx/docs/packed-root-format.md) covers
all readers, producers, sparse-read locators, recovery inventories, backup and
collection paths. There is no legacy decoding or automatic migration.

## Milestone status

| Milestone | Implementation | Exit gate |
| --- | --- | --- |
| M0 | Measurement and comparison harness delivered | Three A/A capacity pairs unverified; storage API totals reconcile, but SDK-internal HTTP retries need provider telemetry |
| M1 | Packs, inline leaves and bounded compaction spooling delivered | Ordinary append meets four PUTs; composed compaction needs five. Paired cold/sparse-read guardrail unverified |
| M2 | Existing native grouping and per-Cell coalescing preserved | Shared node publication coordinator not implemented; 0.25 publication PUTs/command not achieved |
| M3 | Fresh enrollment roles, peer phases and follower append measured | Signed append grants and durable grant fences not implemented; 0.05 enrollment GETs/command not achieved |
| M4 | Existing authority-pinned Cell roots remain the object proof | Bundle coverage proof, transfer and collection protocol not implemented |
| M5 | One matched five-minute Fleet point and all-ACK warm/cold audits completed | Three repetitions, read/failure/overload matrix and absolute/relative parity unverified |

## Evidence

The fixture is **SQL application parity**, not the user's bounded KV workload:
1,000 Cells, 96-byte values, INSERT plus in-command SELECT, and a two-hour
durable request/result ledger in both applications. Nodes have 8-CPU/16-GiB
ceilings and tmpfs state, RustFS has 2 CPUs/2 GiB, and the client has 4 CPUs/4 GiB
on one 8-CPU/16-GiB Linux VM. Summed CPU ceilings exceed VM capacity. This
profile qualifies neither device persistence nor independent-node isolation.

Latest main is `18eff0f7af47fac09b993157bb444e582072d7cf`, which merged PR 65.
Its production source matches the audited `397f500a` foundation; three additional
main files are design documents. The baseline explicitly overlays measurement
hooks. Celld is v0.6.1, `f2bf648663a610eefde71f3547ad61e9b896b1f0`, using the
pinned container digest. Both framework arms use byte-identical clients/auditors.

Each arm offered 100 Fleet writes/s for 300 seconds after 30 seconds warmup,
serially with a fresh provider volume. These are individual points, not a
capacity search or three-repetition qualification.

| Window or audit | Latest main + telemetry | Packed candidate | celld |
| --- | ---: | ---: | ---: |
| Successful commands/s inside window | 99.993 | 99.990 | 100.000 |
| Scheduled p50 / p95 / p99, ms | 66.8 / 265.3 / 868.1 | 24.0 / 200.1 / 386.5 | 5.6 / 11.8 / 44.3 |
| Errors / dropped offers | 0 / 0 | 0 / 0 | 0 / 0 |
| Commands checked by GET and exact retry, warm and cold | 34,001 each | 34,001 each | 34,001 each |
| Original fleet drain, seconds | 7.737 | 10.918 | 11.742 |
| All successful storage API PUTs/command | 6.688 | 4.992 | Not instrumented |
| All GET attempts/command, including ranges | 3.327 | 4.059 | Not instrumented |
| Fresh enrollment GETs/command, owner + receivers | 1.256 | 2.347 | Different authorization protocol |
| Logical commits per selected command root | 1.000 | 1.000 | Not instrumented |
| Final debt slope, bytes/s | +2,455.9 | +2,386.2 | Not instrumented |
| Final oldest-publication age slope, ms/s | +3.711 | +4.435 | Not instrumented |
| 50-ms delivery latency gate | Fail | Fail | Pass at this point |

All original node containers were removed before bucket-only cold audits. This
checks recovery after successful drain, not owner loss before materialization.
The candidate lowered p99 by 55.5% and PUTs/command by 25.4% in this one pair,
but its p99 remains **8.72 times celld's**. Both Cellule arms fail the proposal's
publication stability gate. No sustainable-rate improvement is established.

Compaction mean fell from 444.8 to 25.6 ms; publication mean from 1,288.7 to
489.4 ms; Fleet proof wait mean from 88.0 to 52.0 ms. Capture remained about
1.1 ms and tmpfs follower data sync about 0.0005–0.0007 ms. The faster candidate
also performed more compactions and enrollment GETs. Reduced batching is a
possible explanation for the enrollment increase; this run does not prove it.
The provider reached its two-CPU ceiling. Publication amplification and fresh
peer work remain priorities in this profile; device sync performance is untested.

The measured one-command-per-root result cannot satisfy M2's authority-write
budget by sharing data alone. With three per-Cell selection PUTs, the floor is
three PUTs/command before shared data, node coverage or compaction. M3's measured
2.347 enrollment GETs/command is 46.9 times its 0.05 budget. Meeting those gates
requires the specified coalescing/proof/grant protocols and their failure tests.

| Frozen artifact | Run/build identity | SQL binary SHA-256 prefix |
| --- | --- | --- |
| Latest main + measurement overlay | `implementation-main-logical-metrics` | `85d30c0f93df` |
| Candidate | `implementation-m1-one-fetch` | `ff4c6ede25f4` |
| celld | v0.6.1 container digest pinned in `build.json` | Container digest |

The client and auditor hashes are respectively `cc1d47522078` and
`35368139e4c5` for both builds. Full digests and every exported source hash live
in the retained manifests. All candidate production bytes match PR 66's
`a1c48fcd`; the final test expectation and documentation were edited after the
binary export. A Git base revision alone does not identify an overlaid build.

The isolated all-feature workspace suite passed **1,837 tests** with **38 ignored**
environment-dependent tests. Clippy and API documentation passed with warnings
denied; format, boundaries, layout, Rust fences, links and SQL/peer contract
checks passed. The first compaction run exposed an old range-GET expectation;
the corrected test now requires zero range GETs and two complete pack GETs.
That failure remains in the external evidence.

## Reproduce and inspect

Follow the [harness instructions](../scripts/perf/README.md). Build each revision
into a fresh external directory. Run serially and preserve failed cases.
Inspect `case.json`, `build.json`, `storage-format-smoke.json`, `summary.json`
and generated `report.json`. Content-based cache namespaces and persisted-root
checks prevent stale codec reuse. Source contains reusable drivers and compact
conclusions; caches, volumes, journals, metric windows, binaries and logs stay
outside Git. Each retained provider volume has `store-data/volume.json`.

`scripts/perf/compare.py` accepts an external JSON matrix containing `baseline`,
`candidate` and `celld` lists of case directories. It rejects different workload,
resources, images or client identities. Matching completion rates cannot establish
the full proposal from one pair. Reports include full and range GETs, copy and
multipart calls. SDK-internal retries need provider telemetry for exact HTTP counts.

## Cutover and rollback

1. Use fresh isolated prefixes for qualification and recreatable development data.
2. For a persisted format cutover, stop admission, drain accepted commands and
   preserve the bucket and recovery logs. Upgrade every root reader, producer,
   recovery, backup and collection worker together.
3. Retained version-1 data needs a separately verified logical export/rebuild
   using the old binary. No automatic conversion route is delivered here.
4. A rollback binary cannot read version-2 roots. Use a verified logical
   export/rebuild if available; otherwise preserve artifacts and roll forward.
   Bucket listing and completed uploads never select authority.

## Remaining delivery

Complete M0's reproducibility gate before attributing sustainable-rate changes
to the framework. Then implement M2's bounded node publication coordinator,
complete cross-Cell inventory and per-Cell reconciliation. Measure the selection
floor before deciding M4. M3 needs scoped signed grants and receiver fences with
seal, retire, restart, expiry and lease-loss tests; a TTL cache of consumed peer
verifiers does not implement that contract.

Finally run three paired five-minute repetitions, bounded KV, read-only and
mixed/hot-read guardrails, 1.5× overload with immediate recovery, owner loss
before materialization, and device durability. Preserve failures and report
the measured gap until every required gate passes.
