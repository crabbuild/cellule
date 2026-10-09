# Bounded packed dependencies

Cell root JSON version 2 is the current development format. It replaces version
1 atomically; there is no legacy decoder or dual-write path. Native LTX bytes,
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

`tests/cell/roots/packed.rs` validates the header and original bytes, corrupts
header/body/index bytes, truncates and deletes the object, and rejects cross-Cell
scope and invalid extents. Coalescing, compaction, sparse activation, restore,
response-loss reconciliation and missing-origin tests cover both representations.
`tests/cell/roots/prepare_cost.rs` requires two immutable PUTs for a small root.
Runtime lineage and fenced control selection add two successful PUTs.

Follow the workspace [development format policy](../../cellule-runtime/docs/storage.md#format-policy).
All producers and consumers must deploy the current format together, including
backup, recovery and collection workers. Qualification uses fresh isolated
prefixes. An older binary cannot read version 2 roots; rollback requires a
verified logical export/rebuild or recreating disposable development fixtures.
Do not run mixed root-format binaries against one prefix.
