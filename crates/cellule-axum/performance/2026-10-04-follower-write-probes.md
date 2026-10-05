# Follower-backed write diagnostics

**Independent SQLite state is not sufficient for linear aggregate write scaling.**
The real three-process Axum fixture exercises the existing node log, pinned
mTLS, signed directory authorization and fsynced follower stores. A low-rate
smoke passes; higher offered load exposes disk-admission failures, fencing and
slow background publication. No framework throughput improvement or supported
capacity is established. The target remains 2,000 Cells, 10,000 durable
writes/sec and 50,000 reads/sec on isolated 8-vCPU/16-GiB owner hardware.

The [dataset](2026-10-04-follower-write-probes.json) records exact configurations,
binary hashes, frozen-source manifest hashes, phase populations, resource peaks
and original journal hashes. Raw evidence and original diagnostic sources remain
outside the checkout. Framework base is `80c4fd99c10fc7e91ab34c628ea61fcd488e0cea`.

## Real transport smoke

Two Cells, offered 10 writes/sec and 10 reads/sec, two-second warmup and ten-second
measurement: all offers complete inside the window, with no errors or drops.
Cold audit verifies 122 writes and 120 reads, including seed and warmup. Both
Cells release to Idle, restore with a fresh owner boot, replay original receipts
and accept a next write with an increased fencing epoch.

Initial runtime counters report 100 follower responses, 22 object responses,
122 follower submissions, 116 successful append batches and no append failures
or rejected/unavailable submissions. These include seed, warmup and drain;
they do not classify only the measured window. Object publication can win the
proof race. This graceful cold audit verifies object roots, not owner-crash
recovery from follower-only acknowledgments.

The final clean-source smoke repeats these command and cold-recovery checks.
Its source manifest matches the working candidate, and the dataset retains its
binary hashes and counters separately. Example regression tests also verify
that a failed post-commit read retains the original durable receipt and error;
the previous behavior fails that regression.

## Matched overload pair

Same clean server/driver binaries, 64 Cells, eight SQL workers, 128 clients,
queue capacity 1,024, 30-second warmup and 120-second measurement. Offered rates
are 500 writes/sec and 50 reads/sec. Owner local-disk admission remains 1 GiB;
retained-cut and native budgets remain 16 MiB and 512 MiB.

| Measured metric | Object proofs | Follower fixture |
| --- | ---: | ---: |
| Successful writes/sec inside window | 184.892 | 103.058 |
| Successful reads/sec inside window | 11.575 | 8.208 |
| Write errors | 106 | 5,383 |
| Read errors | 0 | 451 |
| Write queue drops | 36,625 | 41,544 |
| Read queue drops | 4,550 | 4,502 |
| Write HTTP p50 / p99, ms | 264.4 / 2,919.6 | 405.2 / 4,553.8 |
| Warmup write errors | 3 | 395 |
| Initial Cells proven Idle at exit | 64 | 0 |

Both command-validation gates fail and cold audit is not run. The follower
owner does not finish drain before the harness abort deadline and is forcibly
terminated; original follower data is retained. Final runtime counters are
unavailable for that point. The HTTP bodies alone do not establish its internal
failure causes. Neither point qualifies durability recovery or capacity.

This is one sequential pair on a shared Linux ARM64 development VM with eight
vCPUs and 16,732,606,464 bytes RAM. Owner, driver and followers share an eight-core,
12-GiB container. RustFS shares the VM with four cores and 2 GiB. Other workloads
are uncontrolled. It is not an isolated owner or independent follower failure
experiment and has no replication or confidence interval.

## Failure and admission-headroom probes

Separate tagged diagnostic snapshots retain original invocation and retirement
errors and periodic runtime counters. They do not modify response gates or
persisted frames. A 64-Cell reproduction records `Capacity("local disk bytes")`
before subsequent `NotStarted(Fenced)` refusals and uncertain mutations.
Shutdown retries `PendingPublication`. Another 30-second diagnostic shows zero
follower append failures and zero rejected/unavailable submissions while
publication mean grows from 1,108 ms to 5,046 ms across cumulative samples.
The exact first cause of every fenced Cell is not established.

A source-snapshot-only retry increases the owner disk admission budget from
1 to 2 GiB, with the same capture limits and native/retained budgets. It offers
500 writes/sec and 50 reads/sec for 30 seconds after a five-second warmup.
It records no issued-request or warmup errors. All 64 Cells drain and cold audit
verifies 4,829 acknowledged writes and 451 reads, followed by exact next-write
and fencing checks. However, only 54.633 writes/sec and 5.367 reads/sec complete
inside the measurement window; 12,294 write and 1,254 read offers are dropped.
Write HTTP p50/p99 are 1,342.4/6,210.9 ms. This is an admission-headroom diagnostic,
not a matched throughput improvement or sustained capacity result. The runnable
example's budget and framework defaults remain unchanged.

Passing command validation and cold audit does not mean the offered rate was
sustained. The driver rejects erroneous issued commands; it retains overload
loss separately. Qualification additionally requires target successes inside
the window, acceptable latency, stable backlogs and resource/failure evidence.

## Next framework optimization

The existing coalescing path publishes one Cell root for a covered range, then
calls `prove_object` for each covered ticket. Each ticket waits on the shared
node coverage mutex and its authority callback. The callback in this fixture
loads the current signed advertisement and performs a coverage CAS, including
when contiguous coverage does not advance. This remains serialized metadata
work after root coalescing; its precise share needs a dedicated phase probe.

Batch completed object coverage while preserving exact ticket scope, contiguous
watermarks, fresh authority, fencing and cancellation. Keep capture/publication
admission below pressure ceilings and retain pending proofs through drain.
Then profile native commit/capture, worker admission, compaction and metadata
renewals at increasing Cell counts. Repeat longer matched runs and the
[sustained qualification](node-capacity.md) before claiming scaling. Diagnostic
instrumentation is removed from the working sources; no production runtime
optimization is included in this benchmark change.
