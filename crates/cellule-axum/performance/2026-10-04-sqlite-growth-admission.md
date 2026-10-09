# SQLite growth admission and remaining publication limits

The managed SQLite session now admits file growth before I/O and reserves
capture/commit credit from its database image bound. It preserves a typed
resource refusal after SQLite proves rollback. The runtime keeps that owner
usable and can retry the original identity. Ambiguous commits still fence.

The [critical metrics and provenance](2026-10-04-sqlite-growth-admission.json)
identify source commit `5d00d10`, the exact source archive and release binaries.
Raw journals, logs, TLS material and source snapshots remain outside the checkout.

## Concurrent admission regression

Sixteen independent real SQLite sessions share the unchanged 1-GiB disk budget.
All accepted tiny-write callbacks wait together before committing and capturing.

| Result | Original admission | Growth admission |
| --- | ---: | ---: |
| Concurrent callbacks admitted | 7 | 16 |
| Capacity refusals | 9 | 0 |
| Peak reserved local disk bytes | 939,994,848 | 1,533,536 |
| Reserved bytes after all sessions close | 0 | 0 |

These are reservation bytes, not RSS or TPS. The new accounting also charges
32-KiB SHM per Cell. Regression coverage includes real spilled-WAL rollback,
512/4096/65536-byte pages, full-image escalation, capture with all remaining
disk capacity occupied, and pruning while newer capture credit remains pending.
At the public runtime seam, the legacy error consumer fenced a proved rollback;
the corrected consumer returns `Capacity`, serves a same-owner read and accepts
an exact-identity retry that commits once.

## Durable HTTP windows

Both fresh fixtures use 64 Cells, 16 SQL workers, 8 Tokio workers, 64 clients,
a 256-entry driver queue, 30-second warmup and 180-second measurement. Disk,
native and retained-cut limits remain 1 GiB, 512 MiB and 16 MiB. Two followers
use pinned mTLS and separate real fsynced stores. Neither build nor profiling
overlaps measurement. The ARM64 development VM has 8 vCPUs and 16 GiB shared
by the owner, followers, RustFS and other workloads; it cannot qualify an
independent owner node.

| Offered write / read TPS | Successful write / read TPS | Write / read queue drops | Write HTTP p50 / p99, ms | Read HTTP p50 / p99, ms |
| --- | --- | --- | --- | --- |
| 100 / 10 | 93.006 / 9.300 | 972 / 104 | 124.8 / 4256.3 | 1.9 / 3485.5 |
| 1000 / 10 | 154.661 / 1.394 | 151841 / 1549 | 163.8 / 2810.3 | 32.0 / 1840.3 |

Both points have zero request errors and zero unissued offers. Dropped offers
do not count as TPS. Scheduled write p99, including driver queue wait, is
7487.4 and 6388.5 ms respectively. Successful response-body throughput within
the measurement window is 18,621/1,810 bytes/sec and 31,286/272 bytes/sec for
writes/reads; this excludes HTTP/TLS overhead and request bodies.

Cold audit verifies 20,092 writes/1,996 reads and 34,886 writes/313 reads,
including seed, warmup and drain. Every original Cell drains idle, restores
from published objects, preserves original identity/receipt replay, accepts
the next sequence at a later epoch and drains idle again. These audits do not
prove follower-only recovery after an owner crash. Fleet/object responses are
15,837/4,255 and 28,417/6,469; all submissions are fleet submissions, with zero
rejected/unavailable submissions and zero failed log appends.

## Next bottleneck

Disk refusals disappear in these runs, but throughput and latency remain far
from the target. At 1000 offered write TPS, root admission averages 95.9 ms,
admitted preparation work 43.8 ms, publication 259.7 ms and compaction 1459.0 ms.
The owner uses 0.77 CPU cores over the window including setup, warmup and drain,
with 165.2 MiB peak RSS. Root preparation holds a shared dirty-memory cohort
through verified input handling and immutable uploads; the unchanged host has
eight dirty cohorts. There are 28,391 publication observations and 137,033
uploaded objects for 34,886 original writes, including warmup and drain.

This evidence points to publication admission, repeated object work and
compaction stalls before CPU saturation. It does not isolate their causal
shares: phase populations overlap and cannot be added, and the provider shares
the VM. The next probes must separate Cell density, publication work and
provider/follower costs while retaining the same proof and resource gates.
No HTTP speedup is claimed from these single fresh-fixture points. The
[2,000-Cell, 10,000-write/50,000-read target](node-capacity.md) remains unverified;
neither offered HTTP rate qualifies because both windows drop traffic.
