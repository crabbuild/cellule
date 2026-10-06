# Publication metadata cutover: plan

Hard cutover, no compatibility shims. Goal: reduce objects uploaded per published
root from 4–7 toward celld's 1, and re-measure the matched head-to-head in
`docs/performance-audit-rustfs.md`.

Every fact below was read from the tree at `2d02d08` plus the shipped group-size
change. No number here is a measurement except where it cites the audit.

## What is uploaded per publish today

`prepare_append` joins two branches and the runtime adds two coordination writes:

| Object | Writer | Paid per publish |
| --- | --- | --- |
| segment body | `upload_prepared_segment`, `replica/upload.rs:64` | yes |
| segment index | same function, `replica/upload.rs:91` | yes |
| directory nodes | `put_objects`, `replica/prepare.rs:720` | only when new |
| root document | `put_objects`, `replica/prepare.rs:839` | only when new |
| root lineage | `retain_root_lineage`, `publication/mod.rs:817` | **yes** |
| control CAS | `authority.transition`, `control/authority/mod.rs:173` | **yes** |

The two unconditional extras beyond the data are the lineage create and the
control CAS. The lineage one is removable without touching the LTX format.

## Step 1 — fold the root lineage into the control object

### Why it is currently one write per publish

`publish_proposal` retains lineage whenever the preparation pair is not the last
one it confirmed (`publication/mod.rs:815`):

```rust
if self.lineage_confirmed != Some(prepared.preparation()) {
    self.authority.retain_root_lineage(prepared).await?;
    self.lineage_confirmed = Some(prepared.preparation());
}
```

`RootPreparation` is `{ root, predecessor }`
(`crates/cellule-ltx/src/replica/preparation.rs:11`). `root` is the newly
published root, so the pair differs on every publish and the branch is taken
every time. `retain_verified_link` then writes a fresh object keyed by the new
root digest, so the write is unavoidable while lineage stays a per-root object.

### Target shape — corrected

An earlier draft of this plan proposed folding lineage into the **control**
object. Reading `verify_root_prefix`
(`crates/cellule-runtime/src/control/authority/lineage/verify.rs:42`) shows that
does not work, and the reason is worth recording so it is not re-proposed.

The walk is a graph traversal, not a chain: it starts at one successor root and
expands backwards, popping candidate roots and reading **each candidate's own**
record to discover its predecessors (`verify.rs:79`), bounded at
`MAX_LINEAGE_ROOTS` and 10,000 origin dependencies. Every historical root in the
walk needs its own record.

The control object cannot supply that. It is one object per Cell
(`control_path(cell)`), replaced on each CAS, so `root_lineage(candidate)` for a
historical candidate has no control object to read. Folding lineage into control
would silently break `verify_root_prefix` for any chain deeper than the live
root.

**The correct target is the root document** — the LTX root object written by
`put_objects(CellObjectKind::Root, ...)` at `replica/prepare.rs:839`. It is
already:
- immutable and content-addressed, so it does not introduce a mutable
  coordination object;
- per-root, so every candidate in the walk still has exactly one record;
- written once per publish, which is the write being amortized.

Adding a `predecessors` field to the root document therefore removes the separate
lineage object *and* keeps the traversal semantics intact. The cost moves into
`ROOT_BYTES` (32 KiB) and the root document `version`, which the LTX format
already versions.

This makes Step 1 an LTX-format change rather than a runtime-only one, so it also
needs the `ROOT_BYTES` headroom check and the format-vector review that Step 2
below describes.

### Second constraint: what the walk pays today

Reading the walk body (`verify.rs:66-108`) narrows the target further, and the
root document turns out to be a poor home as well.

For every expanded candidate the walk does exactly one
`self.root_lineage(candidate).await?` (`verify.rs:79`) — a single bounded GET of a
small record whose payload is one `RootRef` plus its predecessor list
(`MAX_LINEAGE_BYTES`). It does **not** fetch the candidate's root document. The
separate `reachable_objects_bounded(&root, ...)` call at `verify.rs:111` walks
the *successor's* dependency graph, not each candidate's.

So moving lineage into the root document would trade one small per-candidate GET
for one full root-document GET per candidate — a read-path regression in the
recovery retry loop, in exchange for one saved PUT on the publish path. That is
not obviously a good trade, and it is why this migration should not be executed
without a decision on which side to pay.

### The three shapes, with their costs

| Shape | Publish cost | Recovery-walk cost | Format change |
| --- | --- | --- | --- |
| today: standalone lineage object | 1 PUT per publish | 1 small GET per candidate | — |
| lineage in the root document | 0 extra PUT | 1 full root GET per candidate | LTX root v2 |
| lineage in the control object | 0 extra PUT | needs a retained window of ancestor links in the control, or the walk breaks | control record |

Only the first row is measured. Both alternatives move cost from publish to
recovery, and neither has been measured on the recovery side, so neither should
be chosen on the strength of the publish number alone.

### The walk is control-plane only

`verify_root_prefix` has no callers on the command path. Every call site is a
movement, evacuation or acquisition decision:

- `crates/cellule-runtime/src/cell/actor/prefix.rs:35`
- `crates/cellule-runtime/src/control/authority/acquisition/prefix.rs:190`
- `crates/cellule-host/src/fleet/movement/prefix.rs:16`, `:82`, `:183`
- `crates/cellule-host/src/fleet/failed_boot/writers/successors/prefix.rs:17`
- `crates/cellule-host/src/fleet/reader_evacuation/source/collection.rs:106`

It runs when a Cell is being moved, evacuated, booted after failure, or acquired
— none of which happen per command. A deep chain also requires a long
uncompacted history, and each candidate's root document is loaded by the same
recovery operation that calls the walk, so it should be warm rather than cold.

That makes the trade acceptable in principle: one saved PUT on **every** publish
against one extra (likely cached) GET on a **control-plane** operation. But
"should be warm" is reasoning, not a measurement, and this migration has already
been corrected twice by reading consumers. Treat the recovery side as the
remaining risk and measure it before landing.

### Third constraint: lineage accumulates, the root document cannot

This one decides whether the migration is a net win at all.

`retain_verified_link` is an **accumulating** update, not a single-parent write.
It builds `CellRootLineage { root, predecessors }`, and when the record already
exists it inserts the new parent into a sorted `predecessors` list
(`lineage/mod.rs:154-175`). The runtime passes exactly one parent per call
(`retain_root_lineage` -> `prepared.predecessor()`), but a root can be retained
more than once with different parents, so the set grows over the record's life.

The LTX layer, by contrast, only ever knows a **single** parent at document-build
time (`replica/prepare.rs:822`):

```rust
let predecessor = self.preparation_predecessor.or_else(|| base.copied());
let preparation = RootPreparation { root, predecessor };
```

So a `predecessors` field written into the immutable root document would capture
the parent that happened to be current when the document was built, and could
never be extended afterwards. Any predecessor added by a later
`retain_verified_link` — which is exactly what `prepare_after_compaction` does,
setting `preparation_predecessor = Some(predecessor)` at
`replica/prepare.rs:41` so a compacted root rebases onto the compaction's
original predecessor — would be lost. `verify_root_prefix` walks
`record.predecessors`, so a lost link is a walk that cannot reach the pinned
prefix: a recovery failure, not a performance change.

That is why the standalone mutable lineage object exists: it is the only
per-root, post-hoc extensible record in the design. The publish-time ordering
invariant reinforces it — lineage is written **before** the authority CAS so the
CAS only ever selects a root whose lineage is already durable, and a field baked
into an immutable document cannot honour that for links discovered later.

### Conclusion on Step 1

Under the three constraints found in rounds 1-3, **folding lineage into either the
control object or the root document is not a safe net win**:

- the control object is one per Cell, so it cannot answer per-candidate queries;
- the root document is immutable, so it cannot carry accumulating predecessors;
- and both would move cost onto a control-plane recovery walk to save a PUT on the
  publish path.

Removing this object requires redesigning how verified-preparation history is
recorded — for example a per-incarnation append-only index rather than a
per-root object — which is a substantially larger change than the object-count
reduction this migration was scoped as, and it must keep the pre-CAS durability
ordering and the compaction rebase path intact.

### One design that could satisfy all three constraints

The three constraints rule out *per-root mutable* records and *immutable root
fields*, but they do not rule out amortizing the record the way the node log
already amortizes frame chunks.

**Chunked lineage index.** Instead of one object per root, retain one
append-only object per bounded range of roots, keyed by incarnation and a range
ordinal rather than by root digest:

- new links are appended to the open chunk, so a publish that adds a link pays
  nothing new once the chunk is open (the node log's `open.log` is the precedent
  at `crates/cellule-runtime/src/follower/records/append.rs:105`);
- the record stays **mutable while open**, which is what the accumulating
  `predecessors` list needs, unlike the immutable root document;
- it is keyed by range, not by Cell, so per-candidate lookups still resolve —
  the walk reads the chunk covering the candidate's commit sequence;
- rotation is bounded exactly as `ROTATE_BYTES` bounds the node log
  (`crates/cellule-runtime/src/follower/mod.rs:29`), and the round-8 measurement
  showed that bounded re-scan costs ~53 µs, so the read side stays cheap.

The pre-CAS ordering invariant must be restated for it: the chunk append has to
complete before the control CAS selects the root, which is the same ordering the
standalone object has today. The compaction rebase path
(`replica/prepare.rs:41`) then simply appends a second link for the same root,
which is exactly what the current record does.

#### Torn-write proof (the prerequisite this design needed)

The objection to a mutable open chunk is that a partial write could be selected
as durable. It cannot, for three reasons that are all already load-bearing in the
current design:

1. **Publishing is serialized per Cell.** `start_publication` moves the publisher
   out of `ActiveCell` and calls it "the serialization token for root preparation
   and CAS" (`crates/cellule-runtime/src/cell/actor/requests.rs:500`). Two appends
   cannot race for the same open chunk, so the read-modify-write is not
   concurrent within a Cell.
2. **The store exposes no partial object.** Every write goes through
   `create_strict_with_etag` (create-if-absent) or `update`
   (`crates/cellule-store/src/store.rs:600`, `:748`), both conditional writes whose
   atomicity is the object store's. A chunk is therefore either the previous
   complete bytes or the next complete bytes.
3. **The write precedes the authority CAS, and a stale CAS does not retry.**
   `update` explicitly does *not* retry (`store.rs:748`), and the lineage append
   already runs before `authority.transition` today. A chunk append that fails
   leaves the CAS unpublishable, which is the existing failure mode: the
   publication retries or the Cell fences.

The residual risk is a transient network error leaving the remote ambiguous —
already true of the current lineage object, and handled by the same
conflict-adoption path (`lineage/mod.rs:154`, `:186`) which re-reads and compares.
The chunk design inherits that path unchanged; only the object's key and lifetime
change.

#### What is still unmeasured

The recovery-walk cost. The walk pays one lookup per expanded candidate, and a
chunked index makes that a lookup *within* a range rather than a single object
GET, which is cheaper per candidate only if the chunk is cached. That must be
measured against a deep chain before the format moves, exactly as the earlier
rounds measured the other candidates before trusting them.

### Recommended implementation, if a redesign is authorized

1. Add `predecessors: Vec<RootRef>` to `RootWire`
   (`crates/cellule-ltx/src/replica/root.rs:146`) and bump `version` to 2.
2. Populate it where `finish_root` already knows `preparation_predecessor`
   (`replica/prepare.rs:700` onward) — it is the same value
   `retain_root_lineage` is given today.
3. Point `load_root_lineage` (`lineage/mod.rs:88`) at the root document and keep
   the `root_lineage()` signature, so the four call sites do not change.
4. Delete `CellStorageLayout::root_lineage_path`
   (`crates/cellule-ltx/src/cell_layout.rs:148`) and the standalone record once no
   caller reads it. No dual-read.
5. Reject a v1 root document with a clear error at decode — a hard cutover, not a
   migration.

**This is the decision the migration turns on**, and it should be settled by a
measurement of the recovery walk against a deep chain before the format moves.

### Blast radius (complete, from grep)

Writers:

- `crates/cellule-runtime/src/publication/mod.rs:817`
- `crates/cellule-runtime/src/cell/actor/acquire.rs:583`

Readers:

- `crates/cellule-runtime/src/control/authority/lineage/mod.rs:81` — public
  `root_lineage()`, keep the signature
- `crates/cellule-runtime/src/control/authority/lineage/mod.rs:154`, `:186` —
  conflict-adoption paths inside `retain_verified_link`
- `crates/cellule-runtime/src/control/authority/lineage/verify.rs:79` — the
  `VerifiedRootPrefix` walk, re-exported publicly at
  `lineage/mod.rs:13`; the walk itself must keep working, only its data source
  changes

Layout: delete `CellStorageLayout::root_lineage_path`
(`crates/cellule-ltx/src/cell_layout.rs:148`).

Record: `CellRootLineage` (`lineage/mod.rs:27`) and its codec
(`lineage/codec.rs`) move into the control record's encoding.

### Ordering hazard to preserve

The lineage write currently happens **before** the control CAS, so the CAS only
ever selects a root whose lineage is already durable. Folding both into one
object preserves that: the single CAS becomes atomic for both. Do not split the
CAS into two writes.

### Hard cutover

- Keep the control object version field; reject an older version with a clear
  error rather than migrating. A bucket written by the previous revision must
  fail loudly, not silently.
- Delete the lineage path constant and the standalone record type once no caller
  reads it. No dual-read.
- Remove the now-dead `lineage_confirmed` bookkeeping if the single write makes it
  redundant; keep it if the conflict-adoption path still needs it.

## Step 2 — inline the segment index into the root document

Each `SegmentWire` already carries `index_digest` and `index_length`
(`crates/cellule-ltx/src/replica/root.rs:167`, `:292`), pointing at a separate
content-addressed object written at `replica/upload.rs:91`. Carrying the index
bytes in the descriptor removes that PUT.

Constraints to respect:

- `ROOT_BYTES` is 32 KiB and `MAX_INLINE_SEGMENTS` is 32 of
  `SEGMENTS_PER_PAGE` 96, with `encode_root_accepts_the_segment_page_ceiling`
  pinning the ceiling. An index is one entry per published database page, so this
  trades root bytes for a PUT and needs a measured size check before it lands.
- `inspect_segment_source` and `load_graph` consume the index for chain
  validation; those readers must read the inlined copy instead of fetching.
- Bump the root document `version` to 2, drop v1 decoding, and reject v1 with a
  clear error.

This step is only worth doing if Step 1 does not close the gap, because it
touches the LTX format and its vectors.

## Verification required before either step lands

1. `cargo test --workspace --all-features --locked` — currently 1,823 passing.
2. `cargo clippy -p cellule-runtime --all-targets --features test-support -- -D warnings`.
3. `scripts/check-doc-links.py`, `check-doc-rust-fences.py`, `check-module-layout.py`, `cargo fmt --all --check`.
4. Object-lane point: `scripts/bench-axum-rustfs.py` with a fresh prefix, which
   audits exact retries, per-Cell sequence continuity, conflicts, expired
   identities, cold restore from the bucket and a fence advance.
5. Fleet-lane point: `scripts/bench-fleet-local.py` with the TLS fixture, which
   additionally requires `response_sources.fleet > 0`.
6. Provider operation counts before and after, to confirm the PUT per publish
   actually fell — this is the whole point, and the counters exist in the
   example's `Query metrics`.
7. Matched head-to-head: `crates/cellule-ltx/perf/compare.py --only remote`
   against the pinned celld `10cb130` on the same endpoint, matched process
   counts, to see whether the 2.2–2.9× p50 gap moved.

Never weaken a qualification profile or an expected-evidence assertion to make a
step pass. The one test that encodes the retracted group-size sequencing is
already updated and justified in `docs/performance-audit-rustfs.md`.

## Why this is written down rather than half-applied

A format migration that is partially applied leaves the repository in a worse
state than either endpoint: v1 and v2 readers disagreeing, a bucket that neither
revision can restore, or a lineage write removed before its readers moved. The
work above is one coherent changeset and should land as one.
