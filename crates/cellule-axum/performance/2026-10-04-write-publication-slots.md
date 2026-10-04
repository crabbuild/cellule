# Write publication concurrency probe

**Increasing publication preparation slots did not improve aggregate write TPS
in this matched pair.** Independent SQLite state permits concurrent Cell work,
but shared admission, object publication, authority updates and compaction still
consume node and provider capacity. This is a diagnostic, not a framework
optimization or supported-capacity result.

## Matched workload

64 Cells, eight SQL workers, 128 clients, queue capacity 1,024, 30-second warmup
and 120-second measurement. Offered load is 500 writes/sec and 50 reads/sec.
Only dirty/preparation slots change, from eight to 32 through the existing
`Host::with_dirty_slots` API. Both points use the same diagnostic example binary,
fresh object prefixes, real RustFS, object durability proofs and the same
receipt-bound read after each HTTP write. All other budgets stay fixed.

The example-only selector exists in the frozen diagnostic snapshot; it is not
added to the runnable example or framework defaults. Source/binary hashes,
critical telemetry and original journal hashes are in the
[dataset](2026-10-04-write-publication-slots.json). Raw evidence is retained
outside the checkout at the dataset's external path.

| Metric | 8 slots | 32 slots |
| --- | ---: | ---: |
| Write successes/sec within window | 107.700 | 100.550 |
| Read successes/sec within window | 6.208 | 6.233 |
| Write queue drops | 45,984 | 46,823 |
| Read queue drops | 5,195 | 5,200 |
| Measured write / read errors | 0 / 0 | 15 / 0 |
| Warmup write errors | 10 | 0 |
| Write HTTP p50 / p99, ms | 369.9 / 9750.7 | 382.9 / 8166.6 |
| Write scheduled p50, ms | 7263.4 | 8723.9 |
| Write scheduled p99 | Histogram overflow; null | Histogram overflow; null |
| Write scheduled maximum, ms | 34236.6 | 31421.7 |
| Root admission mean, ms | 96.789 | 22.850 |
| Root preparation work mean, ms | 72.917 | 98.262 |
| Authority mean, ms | 48.297 | 43.681 |
| SQL worker phase mean, ms | 12.502 | 18.299 |
| Compaction attempts / mean, ms | 1178 / 3398.592 | 1292 / 3178.078 |
| Uploaded objects / bytes | 59,621 / 262,891,239 | 62,728 / 284,444,970 |
| Owner peak RSS, MiB | 110.53 | 114.58 |
| Owner peak descriptors | 738 | 755 |
| Provider average CPU cores over whole point | 2.698 | 2.776 |
| Provider throttled time over whole point, seconds | 3.763 | 3.438 |

Runtime phase, compaction and upload populations include setup, warmup and drain;
they differ from the measured HTTP window. Do not add their means into a request
latency or interpret their counts as per-write amplification. Provider counters
cover each whole point. Both points run sequentially on a shared Linux ARM64 VM
with eight vCPUs and 16,732,606,464 bytes RAM. The owner and driver share an
eight-core, 12-GiB container; RustFS shares the VM with four cores and 2 GiB.
One pair does not establish statistical significance or a throughput ceiling.

**Finding:** additional slots reduce admission wait, while preparation work and
the SQL worker phase become slower and successful TPS does not rise. The data
does not support increasing the framework default. Publication and compaction
remain expensive; separate provider I/O and native capture probes are needed to
attribute their costs precisely.

Both load gates fail. HTTP errors are 503 `unavailable`; their internal causes
are not established by this write probe. Every Cell releases to Idle on shutdown.
The coordinator stops before cold audit, so neither point qualifies recovery or
supported throughput. Read TPS here reflects the overloaded mixed workload.

## Write scaling direction

- Keep each Cell's SQLite, sequence, mutation identity and fenced writer
  independent; share a bounded worker pool rather than a thread per Cell.
- Exercise the existing [node log shipper](../../cellule-runtime/src/node/log_shipper/mod.rs)
  through a real follower-backed Axum assembly. It batches up to 64 frames
  across Cells with a one-millisecond collection interval. A response still
  requires the exact commit's recoverable proof from every selected follower.
- Use the existing [publication coalescing](../../cellule-runtime/src/cell/actor/requests.rs)
  for follower-proven commits. Object publication can leave the response path,
  while bounded retained bytes and stable tiering backlog remain required.
  Cross-Cell log batching does not combine their SQLite databases or ownership.
- Profile actor handoffs, affine-worker utilization, local commit/capture and
  compaction after selecting the durability path. Reduce measured shared work;
  retain SQLite durability, cancellation, drain and stale-owner fencing.
- Measure local initialization, durable provisioning and cold restoration
  separately. The [density probe](2026-10-04-node-scaling-probes.md) keeps 2,000
  tiny Cells resident at 602.16 MiB peak RSS, but its serial activation average
  is 125.6 ms/Cell. It establishes neither a fixed per-Cell memory cost nor
  creation within a few milliseconds.

More Cells expose parallel work until CPU, disk, network or the durability
service saturates. Additional nodes require Cell placement and ownership
transfer; adding resident Cells alone cannot increase one node's physical
capacity indefinitely. The [target and qualification gates](node-capacity.md)
remain unchanged: 2,000 Cells, 10,000 durable writes/sec and 50,000 reads/sec,
with sustained latency, exact recovery and owner-loss evidence.
