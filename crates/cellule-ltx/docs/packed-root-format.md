# Bounded packed dependencies

Cell root JSON version 3 is the current development format. It replaces versions
1 and 2 atomically; there is no legacy decoder or dual-write path. Native LTX bytes,
node frame formats, Cell fencing, accumulating lineage and authority selection
remain their existing contracts.

## Small segment object

An object ending in `.pack` contains exactly one original LTX segment and its
fixed-width authenticated page index. Its total length is at most 256 KiB.
Larger segments use separate `.ltx` and `.index` objects. The complete packed
object is named by its BLAKE3 digest in the producing Cell incarnation's ordinary
immutable object directory.

| Bytes | Encoding |
| --- | --- |
| 0–7 | ASCII `CRBPACK1` |
| 8–15 | Original LTX length, unsigned big-endian u64 |
| 16–23 | Fixed-width index length, unsigned big-endian u64 |
| 24–31 | Eight zero reserved bytes |
| 32–63 | Original LTX BLAKE3 digest |
| 64 onward | Exact original LTX bytes, then exact fixed-width index bytes |

The segment descriptor has a required `packed` boolean. A packed descriptor
pins the complete object digest, a body offset of exactly 64, the original LTX
length and digest, and the index length and digest. The index starts immediately
after the body. Checked arithmetic rejects overflow, overlap, foreign offsets,
and any total exceeding the bound. Separate and bundled descriptors have
`packed: false` and preserve their ordinary extents.

Preparation verifies the source size and hash and retains its pinned handle and
shared index. Upload freezes and rechecks the complete packed bytes while holding
host I/O admission. Waiting proposals retain no additional packed body buffer.
Small compaction outputs use the same representation. Sparse reads continue
verifying individual frame hashes from the authenticated directory. Compaction
authenticates the complete packed source, including bytes outside page frames.

## Shared publication object

An application-wide `.spack` holds at most 64 rows and 256 KiB. It uses
`CRBSH001`, a big-endian u32 row count and four reserved zero bytes (16-byte
header), followed by contiguous rows. Each row is a 32-byte Cell ID and 16-byte
incarnation, then an ordinary `CRBPACK1` header, native LTX bytes and index.
Objects live beneath `shared/objects/<BLAKE3>.spack` in the application prefix.
Application-owned writer, reader, recovery and backup credentials must allow
that shared prefix as well as the existing Cell object prefixes.

A cohort with one input and one coalesced row retains its verified native
capture for the ordinary `.pack` factory. It creates no `.spack` or disposable
shared-upload file. Its root uses `shared: false`; scope, exact bytes and fenced
selection follow the same canonical path. Multi-row cohorts retain shared
publication even when the rows belong to one Cell.

Root descriptors require `shared: true` and `packed: true`, pin the complete
object digest and exact body/index extents, and retain the native body digest.
Ordinary descriptors require `shared: false`. Inventory, compaction and full
restore authenticate the complete object and the selected row's Cell scope.
Sparse reads authenticate its scoped header and individual page frame hashes;
verified header entries consume the existing bounded metadata cache. Identical
native bytes in two Cells cannot substitute for the authenticated row scope.

The runtime acquires one of 64 cohort credits before Cell dirty admission.
It probes and releases root capacity before the actor freezes its retained
range; no dirty permit waits for a shared uploader.
It charges pinned files, index and ownership tables before preparing inputs;
coalescing and the file-backed upload use the existing host and disk budgets.
The dispatcher bounds in-flight uploads to eight, under the existing host
I/O/scratch budgets. A cohort combines already-prepared rows and freezes when the queue is idle,
or at 256 KiB, 64 rows or a 1-ms assembly bound. Cells sharing a cohort
must have the same provider identity and application prefix. Large cuts,
compaction, or insufficient retained-memory admission use ordinary preparation.
Cancelled waiters leave dispatched upload and scratch cleanup owned by the lane.
The lane joins after all Cell actors and publishers during shutdown.
The worker pool reserves 4,224 node publication descriptors in addition to the
eight handles per resident Cell. Only publication pins can consume this headroom;
Cell and reader admission retains its ordinary descriptor bound. Aggregate
descriptor observations include both classes. Retained RAM and disk limits
remain separate.

Upload grants no durability authority. Each Cell independently prepares its
root, accumulates lineage and performs its existing fenced control CAS. Failed
siblings cannot select another Cell's root. Collection includes shared objects
in its complete application mark set, including dormant roots and backup pins,
and still requires exclusive maintenance and a grace boundary.

For `c` commands per cohort and `r` commands per selected Cell root, ordinary
publication still needs approximately `1/c + 3/r` PUTs per command. Sharing
payloads cannot remove the per-Cell authority floor or prove bucket parity.

## Inline directory leaf

The root has a canonical `directory_inline` field containing lowercase hex or
`null`. It may contain one directory leaf of at most 2 KiB, only at height zero,
with BLAKE3 matching `directory_digest`. All leaf checks remain mandatory:
ordered coverage, checksum, page size, frame hashes and body extent bounds.
The entire root remains bounded by 32 KiB. If inline data would exceed that
bound, the leaf uses the existing `.dir` object path.

Cached predecessor root bytes still require origin presence. Uncached inventory
authenticates the root and every packed object; an inline leaf requires no
separate directory object. Backup pinning, recovery-prefix verification and
collection consume this exact inventory. Collection recognizes `.pack` objects
under the same quiescence and grace rules as other Cell objects.

Compaction fetches each small packed input once, verifies its complete header,
body and index, then writes both admitted scratch extents from those bytes.
The transfer window and host I/O slot bound retained memory through dispatched
writes and cancellation. Large objects keep separate bounded streams.

## Evidence and cutover

`tests/cell/roots/shared.rs` covers exact independent restore, identical-byte
cross-Cell substitution, malformed rows and shared compaction. Runtime tests
cover cancelled waiters, minimum budgets and dormant-sibling collection.

`tests/cell/roots/packed.rs` validates the header and original bytes, corrupts
header/body/index bytes, truncates and deletes the object, and rejects cross-Cell
scope and invalid extents. Coalescing, compaction, sparse activation, restore,
response-loss reconciliation and missing-origin tests cover both representations.
`tests/cell/roots/prepare_cost.rs` requires two immutable PUTs for a small root.
Runtime lineage and fenced control selection add two successful PUTs.

Follow the workspace [development format policy](../../cellule-runtime/docs/storage.md#format-policy).
All producers and consumers must deploy the current format together, including
backup, recovery and collection workers. Qualification uses fresh isolated
prefixes. An older binary cannot read version 3 roots; rollback requires a
verified logical export/rebuild or recreating disposable development fixtures.
Do not run mixed root-format binaries against one prefix.
