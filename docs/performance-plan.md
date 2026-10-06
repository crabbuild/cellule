# Write, read, and activation performance plan

This page is derived from source inspection only. Line references are at
`2d02d08`. No number here is a measurement; each item names the code that
causes the cost and the check that must pass before the change lands. Nothing
here weakens a durability, fencing, or recovery contract.

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
writer connection, set to `synchronous=FULL`, `wal_autocheckpoint=0`, and a
64 KiB page cache (`crates/cellule-ltx/src/db/mod.rs:367`).

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
one fsync chain. Its single-node mode instead acknowledges on a bucket proof, one
storage round trip per write.

Cellule already has the equivalent machinery: node-log shipper batches of 64
frames over a one-millisecond interval
(`crates/cellule-runtime/src/node/log_shipper/mod.rs:14`), captured-delta
coalescing, and exact-root proofs. The measured gap between the two on the same
workload is unknown, so do not publish a ratio. Land rows 1–4, then run the
capacity workload as an alternating A/A pair on one binary before attributing
any change to rows 5–10.
