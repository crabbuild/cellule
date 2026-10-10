# Write, read, and activation performance plan

The original source audit below refers to `2d02d08`. Its measured follow-up
records and the current implementation section distinguish code inspection,
local characterization, and capacity qualification. Every change preserves
durability, fencing, and exact recovery.

The [write performance proposal and delivery plan](write-performance-proposal.md)
defines the next implementation milestones, proof-model decisions, operation
budgets, qualification procedure, and rollout deliverables.

Re-audit note: review the retained measurement records under
`crates/cellule-axum/performance/` before starting any row below. Two changes in
the same area are already measured — captured-delta coalescing
(`2026-10-05-captured-coalescing-r10.md`) and the bounded follower log
(`2026-10-05-follower-bounded-r14.md`) — and moving coverage selection after
admission regressed write TPS (`2026-10-05-publication-admission.md`).

## Where the time goes

### Write path: one serialized publication per published root

A Cell actor serializes accepted commands, so one command is prepared and
executed at a time. Publication is admitted off the actor loop in two phases:
`start_publication` spawns `publisher.admit_publication()` and resumes coverage
selection only after admission succeeds
(`crates/cellule-runtime/src/cell/actor/requests.rs:388`, admission at
`crates/cellule-runtime/src/publication/mod.rs:335`). The publisher token still
holds preparation and CAS exclusive per Cell.

Commit coalescing is present and is now observably active: retained 2,000-Cell
runs report 0.80–0.94 published roots per audited write. The gate still requires
a follower proof for every covered commit
(`crates/cellule-runtime/src/cell/actor/requests.rs:436`), so the object-proof
lane cannot use it:

```rust
let coalesce = active.publications.len() > 1
    && active.publications.iter().all(|queued| queued.durability.is_some());
```

Coalescing across *captures within one preparation* is separate and is
implemented in `crates/cellule-ltx/src/replica/coalesce.rs`, which merges
eligible small native deltas when the merge does not grow the total.

Structural per-root costs:

- The lineage create and the control CAS are unavoidable conditional writes
  (`crates/cellule-runtime/src/control/authority/lineage/mod.rs:112`,
  `crates/cellule-runtime/src/control/authority/mod.rs:173`).
- Concurrency inside one preparation is small: `OBJECT_UPLOAD_CONCURRENCY = 8`,
  `SEGMENT_TRANSFER_CONCURRENCY = 4`
  (`crates/cellule-ltx/src/replica/mod.rs:48` and `:52`).

A conflict on a content-addressed write is expensive rather than cheap:
`put_once` resolves it through `matches_write_target`, which streams the whole
object and runs a full blake3 over it
(`crates/cellule-store/src/store.rs:700`).

### Read path: reads wait for the write's publication, then share one thread

A read on a Cell whose write is in flight is blocked until that write has been
proven durable and published. `busy` is set by `BeginWork` and cleared only by
`finish_work`, which for a mutation runs in `handle_proven` after the durability
proof, the object upload, and the authority CAS
(`crates/cellule-runtime/src/cell/actor/tasks/publication.rs:42`). A read
admitted while a publication is still in flight is not blocked by
`publication_blocked`, because that predicate only counts
`QueuedWork::Command(_)`
(`crates/cellule-runtime/src/cell/actor/lifecycle/scheduling.rs:9`); it is
instead dispatched and rejected by `logical_head_is_durable` as
`PendingPublication` (`crates/cellule-runtime/src/cell/executor/mod.rs:573`).

Each SQL worker is then one OS thread draining one bounded channel:
`run_worker` is `while let Some(command) = receiver.blocking_recv()`
(`crates/cellule-runtime/src/cell/worker/run.rs`). Shard assignment is
`u64::from_be_bytes(cell_id[..8]) % workers`
(`crates/cellule-runtime/src/cell/worker/mod.rs:1330`), so unrelated Cells share
a shard, and reads and writes share it too. One `Semaphore::new(1)` permit per
shard is taken before enqueue, so the 256-slot queue cannot pipeline SQL work
(`crates/cellule-runtime/src/cell/worker/mod.rs:208` and `:943`).

The database budget already pays for more connections than the runtime opens:
`MANAGED_SQLITE_CONNECTIONS = 3` is charged per Cell in the resource ledger
(`crates/cellule-ltx/src/db/mod.rs:16`,
`crates/cellule-runtime/src/cell/worker/mod.rs:95`) while `Db` holds a single
writer connection, opened with `synchronous=FULL`, `wal_autocheckpoint=0`, and a
64 KiB page cache. Runtime bootstrap and verified restored activation explicitly
select `NORMAL` before accepting mutations; standalone opens retain `FULL`.
The response still waits for an exact external durability proof.

A demand fault parks that thread. `Io::page` takes a `std::sync::Mutex` gate and
then waits on a `sync_channel` with `recv_timeout`
(`crates/cellule-ltx/src/paged_io.rs:334` and `:379`), called from the SQLite
VFS read path. While it waits, every other Cell on that shard queues behind it.
The driver is process-wide with a 256-slot channel and a 32-job cap, and a full
channel returns `PagedRequestQueue` rather than applying backpressure
(`crates/cellule-ltx/src/paged_io.rs:366`).

### Activation: checksum base rebuilt from scratch, at 8-way concurrency

`load_checksums` walks every directory leaf node and rewrites the checksum base
on each activation, and it refuses to reuse an existing artifact
(`crates/cellule-ltx/src/replica/directory/checksums.rs:10`,
`:29`, `:168`). The walk runs at `LEAF_READS_IN_FLIGHT = 8`
(`checksums.rs:8`) while the host hands out `io_capacity: 32` permits
(`crates/cellule-ltx/src/environment/host/mod.rs:812`), so most permits stay
idle during the dominant phase. `OBJECT_FETCH_CONCURRENCY` has the same value
and the same mismatch (`crates/cellule-ltx/src/replica/mod.rs:47`).

Two sync costs sit on shared threads: the resume path calls a full-database
CRC64 rescan from an async context without `spawn_blocking`
(`crates/cellule-ltx/src/db/mod.rs:892` into
`crates/cellule-ltx/src/pages.rs:295`), and each directory-cache fill rewrites
the whole cache index with `sync_all` plus rename, consuming jobs from the same
`min(cpus, 16)` pool the cold read path uses
(`crates/cellule-ltx/src/environment/directory_cache.rs:439`, `:550`).

Activation readiness also waits on work unrelated to readiness: the caller's
reply is sent only after `pool.fleet_inventory(...)` completes
(`crates/cellule-runtime/src/cell/actor/task.rs:396`, reply at
`crates/cellule-runtime/src/cell/actor/tasks/activation.rs:69`).

## Measured phases at the newest retained runs

The retained 2,000-Cell records in `crates/cellule-axum/performance/` for
`2026-10-05` name the phases that dominate at that scale. Publication work is
no longer the top cost there: the phases are admission wait and compaction
lifetime, against owner CPU at roughly one core.

| Phase | Reported mean |
| --- | --- |
| Dirty (root) admission | 3,265–3,784 ms |
| Total publication | 4,337–5,667 ms |
| Admitted root preparation work | 46–80 ms |
| Compaction lifetime | 24,125–34,282 ms |
| SQL worker round trip | 36–41 ms |
| Capture | 9–11 ms |
| Provider PUT | 41–44 ms |

Read the same records for what already moved: captured-delta coalescing raised
completed write TPS from 68.81 to 136.81 in one ordered pair, the bounded
follower log cut process read bytes from 246.29 MB to 32.67 MB and append p95
from 259.00 to 49.64 ms in its largest case, and retaining one compaction leaf
removed one PUT per append for a 2.95% TPS change with p95 down 22.65%. Moving
coverage selection after admission regressed write TPS 13.83%; keep it in draft.

These are single ordered shared-VM pairs, not capacity results. They rank the
next target, which is compaction and the admission permits it holds, not the
object-proof coalescing gate.

## Change table

Each row is independently landable. "Gate" is the check that must pass.
Rows marked **hold** were re-audited at `2d02d08` and are unchanged by
`2d02d08`.

| # | Change | Site | Mechanism | Gate |
| --- | --- | --- | --- | --- |
| 1 | Drive cold-read concurrency from `io_capacity` instead of 8 (**hold**) | `crates/cellule-ltx/src/replica/directory/checksums.rs:8`, `crates/cellule-ltx/src/replica/mod.rs:48` | Sequential waves fall by the permit ratio; object-op count unchanged | Existing cold restore and checksum regressions |
| 2 | Stop rewriting the directory-cache index per fill (**hold**) | `crates/cellule-ltx/src/environment/directory_cache.rs:439` | Removes one fsync plus serialization per node and frees shared job slots | Directory-cache regression plus a cold-restore repeat |
| 3 | Reply before `fleet_inventory`; sample from the background scheduler (**hold**) | `crates/cellule-runtime/src/cell/actor/task.rs:398`, `crates/cellule-runtime/src/cell/actor/lifecycle/background.rs` | Removes the inventory statements from the activation critical path | Activation, inventory, and drain tests |
| 4 | Reuse the ETag returned by the authority CAS (**hold**) | `crates/cellule-runtime/src/control/authority/mod.rs:192`, `crates/cellule-runtime/src/cell/actor/acquire.rs:652` | One fewer control round trip per activation | Authority conflict and fencing regressions |
| 5 | Let an object-proven commit ride an unpublished root (**hold**, now needs a matched rate pair) | `crates/cellule-runtime/src/cell/actor/requests.rs:436` | Extends the existing coalescing to the object-proof lane | Exact-root recovery plus capacity A/A |
| 5b | Separate the compaction admission budget from root admission | `crates/cellule-ltx/src/replica/prepare.rs:378`, `:389`, `crates/cellule-ltx/src/environment/host/admission.rs` | Roots stop queueing behind compaction's long-held dirty and recovery permits | Compaction fairness regression |
| 6 | Cache the checksum base by root digest and walk lazily (**hold**) | `crates/cellule-ltx/src/replica/directory/checksums.rs:168` | Removes the dominant per-activation walk and fold | Byte-identical reconstruction or failure |
| 7 | Let reads overlap an in-flight publication (**hold**) | `crates/cellule-runtime/src/cell/actor/tasks/publication.rs:42`, `crates/cellule-runtime/src/cell/coordination/mod.rs`, `crates/cellule-runtime/src/cell/executor/mod.rs:573` | A read stops paying the write's proof, upload, and CAS | Read-during-publication and fence regressions |
| 7b | Open the read connection the budget already charges (**hold**) | `crates/cellule-ltx/src/db/mod.rs:16`, `:369` | Per-Cell read concurrency moves from 1 to the budgeted connection count | Duplicate-delivery and receipt regressions |
| 7c | Take the shard permit after dequeue (**hold**) | `crates/cellule-runtime/src/cell/worker/mod.rs:208`, `:943` | Removes head-of-line blocking between unrelated Cells on one shard | Worker saturation and shutdown tests |
| 7d | Release the shard across a demand fault (**hold**) | `crates/cellule-ltx/src/paged_io.rs:334`, `crates/cellule-runtime/src/cell/worker/run.rs` | A slow provider read stops stalling unrelated resident Cells | Concurrency regression on two Cells sharing a shard |
| 7e | Cache replica control and policy; call `selected` once (**hold**) | `crates/cellule-runtime/src/client/routing.rs:66`, `:146`, `:213` | Removes two to six provider round trips from every replica read | Replica freshness and expiry regressions |
| 8 | Resolve content-addressed conflicts without a full read (**hold**) | `crates/cellule-store/src/store.rs:700` | Removes a full object read plus blake3 from repeated publication | Storage conflict tests |
| 8b | Replace per-hop node-lease mutex and clock reads with one atomic check (**hold**) | `crates/cellule-runtime/src/node/lease.rs:70`, `crates/cellule-runtime/src/cell/actor/handle.rs:542`, `:588`, `:353` | Removes a node-global lock from the critical path of every read | Lease expiry and fencing regressions |
| 8c | Remove per-read allocations and the `sys_meta` select (**hold**) | `crates/cellule-runtime/src/registry/builder/mod.rs:1141`, `crates/cellule-runtime/src/client/local.rs:362` | Cycle savings per read; the sequence is already tracked | Query contract and receipt regressions |
| 9 | Defer or `spawn_blocking` the resume validation scan (**hold**) | `crates/cellule-ltx/src/db/mod.rs:892` | Stops blocking a Tokio worker and re-reading the database | Resume and occupancy tests |
| 10 | Continue on the follower path, not the retirement path | `crates/cellule-runtime/src/follower/records/scan.rs`, `crates/cellule-ltx/src/replica/coalesce.rs` | Keep the measured history-amplification and delta-coalescing work moving | Follower integrity and cold-retention regressions |

Do not re-derive rows 1–10 from scratch: every site above was re-read at
`2d02d08`. The rows that changed shape are 5 (coalescing is already active on
the follower lane, so the gain is bounded) and 10 (the follower log already
improved substantially, so the next wins there are unmeasured).

## Direction against celld

celld acknowledges a write when the owner's followers hold it on disk and keeps
the bucket upload off the acknowledgement path, grouping delivered frames into
one fsync chain. Bucket durability instead waits for synchronous
object-store coverage.

Cellule batches node-log frames across Cells and coalesces captured deltas,
but each selected Cell root still requires its immutable graph, retained
lineage, and fenced control CAS. The private HTTP follower example also reads
fresh enrollment for every append. Those costs remain materially different from
celld's paced node-wide bundle tiering.

## Current implementation and parity gate

| Change | Mechanism | Required evidence |
| --- | --- | --- |
| Fleet native groups | Up to sixteen already-queued mutations share one transaction and capture; signed version-two frames authenticate the complete logical range | Individual outcomes and retries survive adjacent groups, checkpoint cuts, object fallback, and exact restore; old followers are excluded |
| Parallel frame verification | Verify complete bodies before the global sequence lock; exclusively owned envelopes receive their sequence without another decode | Corruption consumes no ticket; concurrent shipping stays ordered; rebinding produces canonical bytes |
| Admission before SQL | Refuse new mutations at three quarters of retained RAM or managed disk while node-log coverage is pending | Refused work never executes SQL; reads remain eligible; accepted work drains and restores |
| Retained RAM accounting | Charge shared index allocations, descriptor/path copies, and compact outcomes; keep file bodies charged to disk | Allocation capacity is counted, file bytes remain bounded, and reservations return to zero |
| Shared disk checkpoint pressure | Begin 64-frame passive checkpoints at half of the node disk budget; reclaim obsolete WAL slack only after SQLite resets a generation | Repeated small writes keep WAL bounded and every checkpoint cut restores byte-identical state |
| Command response | Select the inserted row inside the SQL command and retain it in the durable outcome | The response follows the same durable gate and retries return the stored output |

The grouping change does not add a batching timer. Larger publication/I/O
concurrency and a two-second per-Cell Fleet publication delay did not improve
local throughput, so they are excluded from the implementation.

Qualification uses the same binary revisions, provider, hardware limits,
1,000 Cells, sub-100-byte values, request identities, result semantics, offered
schedule, and client concurrency for both systems. Retained-memory tuning is
reported separately from code changes. Require zero errors and dropped offers,
at least 99% of offers completed within the window, and scheduled p99 at most
50 ms for 15,000 Fleet writes/s or 200 ms for 2,000 bucket writes/s. Repeat each
point and run at least five minutes before claiming steady capacity. Warm and
cold audits must verify every acknowledged outcome and retry, after graceful
drain and destruction of all original local state. Docker tmpfs does not
qualify physical-device fsync durability or independent-machine fault isolation.

### Local Docker characterization

The following points use `origin/main` at `0813f974` and celld v0.6.1 at
`f2bf6486`. Each uses 1,000 Cells, 96-byte values, an INSERT and row SELECT inside
the durable command, 128 clients, and a 30-second window. Both Cellule variants
use a 64 MiB retained-memory budget rather than the 16 MiB example default;
this tuning is separate from the framework changes. The owner, two Fleet
followers, client, and object store share one 8-CPU, 16-GiB Docker VM. Node
state is tmpfs and the store has a 2-CPU limit. Native peer transports differ:
Cellule's example uses pinned mTLS and signed messages.

| Durability | Implementation | Completed writes/s | Request p99 ms | Scheduled p99 ms | Dropped offers |
| --- | --- | ---: | ---: | ---: | ---: |
| Fleet | Main control, same application | 979 | 527.5 | 850.0 | 420,369 |
| Fleet | Candidate | 916 | 365.0 | 797.6 | 422,248 |
| Fleet | celld | 2,572 | 372.0 | 637.1 | 372,444 |
| Bucket | Main control, same application | 55 | 7,082.0 | 9,400.4 | 58,096 |
| Bucket | Candidate | 113 | 2,001.0 | 3,337.6 | 56,347 |
| Bucket | celld | 375 | 3,902.6 | 4,616.1 | 48,508 |

All six points have zero HTTP errors and pass warm audit, graceful drain, cold
restore after destruction of every original local state, and retry checks.
The final candidate audits cover 31,516 Fleet and 6,335 bucket acknowledgements,
including setup, warmup, and low-load requests. **Every target-load capacity gate
fails.** Offered load is 15,000/s for Fleet and 2,000/s for bucket; completion
rates above are observations under overload, not qualified capacity. Runs vary
substantially, and the matched Fleet control shows no throughput improvement.
The bucket difference also needs repeated, longer paired runs before attributing
it to code. An additional celld Fleet point reached 3,561/s but failed lease
renewal and drain; it does not qualify cold recovery. Failed startup/drain and
invalid harness cases remain in the external evidence.

The next architecture work must reduce publication amplification rather than
raise concurrency alone:

1. Pack small root dependencies to reduce body/index/directory/lineage object
   operations. Update the one development representation atomically with
   explicit bounds and full producer, restore, sparse-read, compaction, and
   retention coverage. Preserve exact authority selection and uncertain-CAS
   reconciliation. Measure object operations per acknowledged write and
   dirty-admission delay, as well as TPS.
2. Aggregate verified inputs through node-wide immutable bundles. Bundle upload
   alone grants no current object-coverage credit: every covered Cell must have
   its exact selected root and dependencies before the contiguous node-log
   watermark advances. Partial CAS success, retry, cancellation, and shutdown
   must retain the remaining Cells' obligations.
3. Design epoch-bound append authorization before amortizing peer enrollment
   reads. The current verifier is deliberately consumed per request. A future
   protocol must bind both boot sessions, membership, coverage, and expiry,
   and prove safety through concurrent seal/retire, restart, and lease loss.
   Measure fresh authority reads per replicated frame and peer append latency.

These are initial design budgets, not measured results or substitutes for the
capacity gate above. Collect counters around each warmed measurement window;
report startup, compaction, and drain work separately.

| Proposal | Initial operation budget | Evidence before adoption |
| --- | --- | --- |
| Packed small roots | At most four successful provider PUTs per selected root for the small-value workload, including lineage and control | All producers and consumers change atomically under the development format policy; ambiguous publication and collection remain safe |
| Node-wide tiering | At most 0.25 publication PUTs per acknowledged Fleet command under sustained backlog, including each Cell's selection work | Partial selection cannot advance coverage past an uncovered Cell; bounded retention and complete drain pass |
| Epoch-bound peer authorization | At most 0.05 fresh enrollment GETs per acknowledged Fleet command, summed over owner and followers | Seal, retire, restart, and expiry races cannot release a new stale-owner proof |

Use the capacity and cold-recovery drivers in `crates/cellule-axum/examples/`
and retain full evidence outside the repository. A short comparison table and
these correctness gates belong with the change; raw client logs do not.
