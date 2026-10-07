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
| M5 | All-ACK warm/cold audits and five-minute main characterization completed | Paired repetitions, read/failure/overload matrix and absolute/relative parity unverified |

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

Latest-main Fleet offered 100 writes/s for 300 seconds after 30 seconds warmup.
It completed **99.993 writes/s**, with **868.1 ms scheduled p99**, zero errors
and zero drops. Every one of its **34,001 acknowledged commands** passed GET
and exact-command retry audits while warm and after all original node containers
were removed. The original fleet drained in **7.737 seconds**. This point fails
the 50-ms Fleet latency gate and publication stability.

Its window measured **6.688 successful PUTs/command**, **3.327 GET attempts/command
including ranges**, and **1.256 fresh enrollment GETs/command**, summed over
owner and followers. Unpublished bytes and oldest publication age had positive
slopes over the final three minute segments. The provider was at its two-CPU
ceiling during much of the run. Fleet proof wait averaged 88.0 ms, response
confirmation 0.290 ms, capture 1.047 ms, and tmpfs follower data sync about
0.0007 ms. This points to publication and provider/peer pressure in this profile;
it does not establish the bottleneck for NVMe or managed object storage.

Candidate and matched celld results will be recorded after their complete audits.
No candidate speedup is inferred from the main result.

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
