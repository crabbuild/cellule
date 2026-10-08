# Bundle coverage implementation

The connected protocol APIs now implement shared selection, independently
awaitable root materialization and complete live-writer closure. They are **not
enabled in the ordinary actor response path**. The prior performance regression
and failed qualification remain the baseline. This slice establishes ordering
and reconstruction evidence. The [fresh application-path benchmark](pr67-performance-reevaluation.md)
measures `7fc0793`; it does not exercise bundle-based responses or establish
write parity. The [WAL NORMAL comparison](pr67-normal-wal-reevaluation.md)
separately records seven completed diagnostic cases and an interrupted matrix.

## Celld reference and Cellule adaptation

The reference is celld `f2bf648663a610eefde71f3547ad61e9b896b1f0`.
Its [bundle loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4401)
gathers Cell tails into one node upload and separates durable coverage from
materialized positions. Its [bundle credit path](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L6620)
rechecks bucket state and epoch after the upload before crediting rows.
These are architectural references; this module is independently implemented.

Cellule also has independent Cell ownership epochs. It first reserves a
provisional catalog binding, pins the original writer in Cell control, then
opens the binding through node CAS. Interrupted enrollment remains an inventory
obligation; a lower-level Cell pin CAS cannot bypass reservation. The existing
canonical node-record CAS selects catalog and range together. A
heartbeat preserves the head; a conflicting closure requires a fresh proposal.
Uploading an immutable object supplies no coverage proof.

## Implemented contracts

| API or boundary | Behavior |
| --- | --- |
| `NodeDirectory::initialize_bundle_lane` / `bind_bundle_cell` | Establish a boot/epoch catalog; reserve a provisional inventory entry, pin the original Cell's base/code/schema/writer, then open it before issuing bundled commands |
| `NodeLogShipper::submit_assigned` / `NodeDurability::submit_assigned` | Use the existing bounded native shipping lane and return an opaque witness for every frame in a complete capture |
| `prepare_node_bundle` / `select_node_bundle` | Reject missing, overlapping, unassigned or cross-binding ranges; verify origin extents before selecting; reconcile only the exact head under the original lease |
| `BundleCoverageProof` | Retain authenticated immutable locators, logical endpoint, SQLite position and original base; retain no capture bodies |
| `load_bundle_coverage` | Reopen a selected suffix from the authority-pinned canonical catalog, including a fenced boot; grant no writer or follower-suffix closure |
| `CellPublisher::materialize_bundle` | Reconstruct the exact overlay and use normal root preparation, lineage and Cell CAS independently of selection; bounded small tails reuse canonical native coalescing and packing |
| `checkpoint_bundle_cell` / `checkpoint_bundle_cells` | Require the opaque proof matching each freshly read materialized root; drop only its identical locator prefix, retain newer suffixes, and select up to 64 checkpoints with one index upload and one node CAS |
| `close_cell_issuance` / `begin_bundle_close` / `finish_bundle_close` | Freeze complete assigned issuance, including prior Fleet ACKs above the selected prefix; retain that same opaque issuance witness through terminal closure; read only its authenticated shard |
| Cell departure CAS | Refuse release, takeover and tombstone until Closed, exact materialization and catalog checkpoint; migration refuses while bound |
| Node withdrawal/maintenance | Refuse unresolved bindings; stale fencing preserves the catalog head; one bundle-bound boot cannot rotate its native log to another epoch |
| Backup and collection | Backup refuses bound Cells. Coverage objects have no deletion path; this is retention, not a qualified collection implementation |

Failed-owner recovery now joins dependency-verified selected prefixes and the
sealed follower witness in the same file-backed builder. Complete shard
inventory includes selected-only Cells; identical native overlap is skipped,
while conflicting bytes and an omitted Cell reject before attachment. A bound
control may retain only its original recovery session/epoch after canonical
fencing. Its pin remains until complete terminal materialization/checkpoint;
the lower-level node seal enforces that obligation too. `recover_and_seal` now
materializes bound overlays under the fenced original binding, retaining normal
lineage and exact root CAS. It verifies every original root, including quiet
Cells, stages at most 64 checkpoints per immutable upload, then selects the
complete terminal catalog with one fresh-claim node CAS before sealing the log.
Only exact lost replies reconcile. Partial-root retries retain the original
manifest; verified closed roots allow resumption after transfer or interrupted
log seal. Provisional enrollment reconciliation, admitted host scheduling and
full lifecycle qualification remain unfinished. Ordinary bundle responses remain
disabled.

The original SQL/capture/submission jobs must join before `close_cell_issuance`.
Its ordered gate prevents late assignment from consuming a node sequence. The
legacy identity-free `DurabilityGate::issue` cannot produce a per-Cell closure:
using it makes that closure fail closed. Automatic actor joining and scheduling
are still required before enabling this path for application commands.

```mermaid
sequenceDiagram
    participant Cell as Original Cell writer
    participant Gate as Ordered native gate
    participant Node as Canonical node record
    participant Root as Cell root materializer
    Cell->>Gate: Complete capture assignment
    Cell->>Node: One verified cross-Cell bundle + CAS
    Node-->>Cell: Exact selected range proof
    Node->>Root: Base + bounded authenticated locators
    Root->>Cell: Ordinary exact root/lineage CAS
    Cell->>Gate: Join accepted jobs, close assignment
    Gate-->>Node: Complete issued terminal endpoint
    Node->>Node: Closing, drain exact issued tail, Closed
    Node->>Root: Materialize complete frozen terminal
    Root->>Cell: Final ordinary root/lineage CAS
    Root->>Node: Exact root checkpoint
    Cell->>Cell: Release/transfer CAS may proceed
```

## Format and bounds

Unbound Cell controls and node records retain version-1 encodings. A binding or
bundle head selects version 2; inconsistent version/field combinations and
unknown fields are rejected. Old readers must not operate on those records.
Use a fresh development prefix and initialize the lane before native issuance;
there is no live-format migration or same-boot bundle-epoch rotation.

New `CNB3` objects contain a fixed authenticated root index, changed catalog
shards, native extents and detached histories under the existing
`cells/v1/node-logs/<session>/<epoch>/coverage/v1/<digest>.cnb` path. The head digest
authenticates the 32 KiB header; shard digests authenticate each binding row and
its exact history extent; history digests authenticate every native reference.
New histories share the one immutable upload. Old native bodies are never copied
into a new history. Unchanged shards and histories keep exact object/range/digest
references without walking a predecessor chain.

A point lookup fetches one header, one shard and only the requested Cell's
history, then its exact native frames. A shared shard keeps unrelated histories
as authenticated references, including when those bodies are unavailable.
Selection verifies every participating Cell's origin and native suffix before
CAS; point selection grants no sibling drain or collection authority.
Complete maintenance inventory still verifies every shard, retaining one
bounded shard at a time and at most 4,096 duplicate-pin entries. An unresolved
history remains a drain obligation.

Hot lookups preflight selected shard lengths against 4 MiB before leaf reads,
then preflight the selected histories plus those shards against that same bound
before the first history read. This bounds protocol input, not total heap usage
or host admission. Hydration alone does not rewrite a shard; modifications to an
unloaded history reject.

`CNB1` complete catalogs and `CNB2` inline indexed shards remain readable, with
their original 32-inline-locator bound. Updating them selects `CNB3` without
changing Cell pins; unchanged inline shards can remain referenced. Older binaries
cannot read `CNB3`. Upgrade every recovery consumer before selecting it, using a
fresh development prefix. This does not migrate unbound live actor activations
or enable bundle responses.

| Development bound | Value |
| --- | ---: |
| Immutable object or individual catalog shard bytes | 4 MiB |
| Selected shard plus history encoded metadata | 4 MiB, checked before the respective reads |
| Authenticated index header | 32 KiB / 256 shard references |
| Original bindings per boot/epoch | 4,096 |
| Native frames per selection | 64 |
| Detached exact frame references per Cell | 256 |
| One encoded history | 32 KiB |
| Legacy inline locators per Cell | 32 |
| Native suffix bytes verified per Cell | 4 MiB |
| Distinct base-root origin dependencies verified per Cell | 65,536 |

Exhaustion rejects preparation while retaining the last proof. These are safety
ceilings, **not performance-qualified policies**. Old native bodies are still
reverified. The 215-command checkpoint density is now represented and measured
for a small-image fixture; total lifecycle cost is not qualified. Host memory,
native-job admission, materializer fairness and joined scheduling remain caller
obligations. See the [node performance design](../crates/cellule-runtime/docs/write-performance-design.md).

## Verification and remaining delivery

The detached-history regression first fails at the original 32-reference
ceiling. With the new representation, one actual Cell among a **2,000-binding
catalog** selects 215 commands without checkpoint and cold-restores its seed and
all 215 outcomes while excluding a later unselected command. The other bindings
are metadata fixtures; this is not 2,000 active writers or node capacity evidence.
Density alone initially costs **220 materialization PUTs**. Streaming verified
rows through the existing admitted coalescer reduces it to **four**, retaining
the 256 KiB changed-page bound. A public file-backed LTX test exercises more than
256 KiB of aggregate native input and uses two LTX PUTs, plus the runtime's lineage
and Cell CAS. The 256-reference boundary also costs four in this fixture; an
oversized changed image preserves the ordinary bundle fallback.

The new one-Cell update among 1,000 bindings uses **35,416 metadata bytes**, versus
35,223 with inline `CNB2`. Recovery uses four coverage-object ranges: header,
shard, history and native frame. Prefix checkpoint uses three ranges with no
native-body reads. The extra history read buys selective dense lookup; it is not
a claimed read TPS improvement. One shared shard test corrupts a sibling history:
the requested Cell still selects without fetching or rewriting it, while a
complete reconstruction load rejects the missing/corrupt sibling. Additional
codec tests cover original pin/session/epoch, exact count/native-byte claims,
truncation, the 256-reference limit and aggregate admission before history I/O.
Both legacy complete catalogs and inline indexed suffixes migrate with the same
Cell pin. All raw logs remain outside Git.

The frozen detached-history and streaming-materialization source passed all
twelve contributor checks: **1,908 workspace tests passed, 38 ignored; 60 local
LTX tests passed**. Its 39 focused bundle/index tests include the 215-command
exact-root and outcome regression. The file-backed recovery test also passes.
These results verify correctness and component work; the latest source has no
new end-to-end TPS measurement. Ordinary application responses still use
per-Cell publication, and production bundle ACK integration remains unfinished.

The combined selected-prefix/follower recovery snapshot passed all twelve
contributor checks: **1,916 workspace tests passed, 38 ignored; 60 local LTX
tests passed**. Its five new two-Cell cases use real managed captures and
file-backed follower fsync, verifying a pruned prefix with a prior Fleet ACK,
identical overlap, selected-only recovery, conflicting overlap and omitted
inventory. Separate tests cover a 2,000-scope manifest codec and retention of
scratch admission when a cancelled waiter leaves cache work running. The scope
codec is a metadata fixture, not 2,000 writers or throughput evidence.

The terminal recovery regressions cold-restore selected and prior Fleet outcomes
before allowing Cell transfer. They cover a partial root CAS, lost root/catalog
replies, expired claims and resumption after terminal catalog selection when one
Cell has transferred but log seal was interrupted. A **65-real-Cell** case crosses
the cohort boundary: **two immutable catalog uploads, one complete terminal node
CAS, then one log seal**. This excludes per-Cell roots, recovery manifest uploads,
enrollment and steady-state application work; it is not TPS or full M4 evidence.
The existing 215-command/four-PUT test remains passing.

The terminal-recovery snapshot passed all twelve contributor checks: **1,920
top-level workspace cases passed, 38 ignored; 60 local LTX cases passed**.
This count excludes four nested subprocess executions included in earlier
totals. Default-parallel verification first expired four unchanged lease/grant
fixtures; a four-worker control run passed the identical runtime source. The
long density fixtures now renew only after authoritative heartbeat CAS, retaining
the same production lease duration and catalog head. The final isolated suite
uses four test workers; qualification concurrency and thresholds are unchanged.
These checks do not measure application TPS or complete lifecycle qualification.

The following counts describe the earlier inline-index snapshot, not the latest
application TPS:

The index regression reproduces a **783,146-byte** metadata rewrite for one
update among 1,000 Cells on the old source. The same test measures **35,223 bytes**
with the original `CNB2` (95.5% less), retains one immutable PUT plus one node CAS, and recovers
through three coverage-object ranges: header, chosen shard and native frame.
An exact materialized-prefix checkpoint among those 1,000 Cells falls from
**253 coverage-object reads to two**, without reading old native frames. A
64-Cell checkpoint cohort selects all completed roots with **two PUTs**, keeping
a hot Cell's newer suffix; an unmaterialized participant rejects the whole cohort
without a partial CAS. That small independent materialization used the canonical
native coalescer and pack: **256 PUTs instead of 320** for those 64 roots, or four
per root. A 32-locator suffix falls from **37 PUTs to four**, and cold restore
includes every selected outcome while excluding the later unselected command.
The aggregate native rows, indexes and pack headers must fit the existing
256 KiB budget; larger tails preserve the ordinary bundle representation.
The [updated cost calculation](bundle-coverage-proof.md#quantified-checkpoint-constraint)
requires at least 215 commands per Cell checkpoint if that four-PUT path and
64/64 cohorts hold, before retries or maintenance. Real larger checkpoints must
be measured; that original 32-locator safety ceiling did not satisfy this constraint.

Additional tests cover legacy migration with the same Cell pin, a reused shard
without reading its old header, corrupt and missing reused shards, and refusal
to omit a corrupt shard from complete inventory. Bootstrap enrollment accepts
the runtime's command-zero root only before any native issuance; the first
command materializes and a quiet Cell drains without inventing a command.
An exact installed checkpoint retry preserves hot newer locators without another
PUT. A chosen cohort exceeding the metadata budget refuses before fetching its
first leaf. These regressions were reproduced before their fixes.
This is metadata/I/O evidence,
not application TPS or complete M4 qualification.


The original twenty-three focused tests cover real managed SQLite captures, canonical object-store
CAS and materialization: two-Cell shared selection, byte-identical roots, a prior
Fleet ACK above the selected prefix, held old proposals across closure, hot
siblings and dormant bindings, lost replies, lease loss, partial command groups,
overlaps, corrupt/missing objects, version rejection, checkpoint continuation,
locator pressure and materializer failure. One test removes the original
database/captures, fences the node record and reconstructs the selected outcome
from origin. Its Fleet ACK setup uses the native gate; it is not a physical
follower-fsync qualification.

The asynchronous tests select another command while an older root is pending.
The older root checkpoints a complete prefix without dropping the hot suffix;
the next materializer can continue from that root before or after the catalog
checkpoint. Origin lookup also works between the root CAS and catalog CAS.
Enrollment fault tests cover failure before pin selection and after the pin
but before catalog activation; neither grants a proof or clean maintenance.

The two-Cell selection test observes **one immutable PUT plus one node CAS**,
with both Cell roots unchanged. That excludes enrollment, root materialization,
catalog checkpoints, compaction and maintenance and must not be reported as
total PUTs/command, TPS or a latency result.

The original bundle source snapshot passed all contributor checks: **1,887 workspace
tests passed, 38 ignored; 58 local LTX tests passed**. All-feature/all-target
checking, warnings-denied Clippy and API docs, formatting, boundaries, module
ownership, document syntax/links and SQL/peer validation also passed. Ignored
environment-dependent tests and the complete production qualification remain
outside this result.

The final indexed publication/materialization snapshot passed all twelve
contributor checks: **1,902 workspace tests passed, 38 ignored; 60 local LTX
tests passed**. This includes warnings-denied Clippy and API docs, all-feature
and all-target checking, formatting, boundary/layout checks, document syntax
and links, SQL/peer validation and performance-harness tests. Its 34 focused
bundle/index tests and two public recovery-overlay tests cover the new cost,
checkpoint retry, command-zero bootstrap and larger-tail fallback behavior.
Source manifests, failed-before/pass-after logs and build outputs remain in the
external `cellule-write-perf-8ad1` evidence directory. Environment-dependent
ignored tests and production performance qualification remain outstanding.

The experimental checkpoint API now takes its `BundleCoverageProof`, and
`finish_bundle_close` takes the same retained `CellIssuedRange` used for Closing.
Consumers must pass those original capabilities; a pin or sampled endpoint is
not a substitute. The singleton checkpoint method delegates to the cohort path.

Remaining work before responses can use bundle proof:

1. Integrate actor/executor proof and query/retry endpoints, release proved
   capture retention, and schedule admitted materializers with joined shutdown.
2. Build on the authenticated index: bound admitted maintenance inventory,
   increase checkpoint density with retained-byte accounting, and measure the
   complete materialization/checkpoint/collection cost.
3. Finish interrupted provisional enrollment reconciliation and admitted
   failed-boot orchestration around combined reconstruction, exact materialization
   and atomic terminal selection. Departure and canonical seal guards still
   refuse any unfinished original binding before replacement admission.
4. Implement complete cross-Cell reference inventory, pins and grace-qualified
   collection. Preserve original catalog and base dependencies throughout.
5. Run the unchanged all-ACK cold recovery, paired Docker throughput/latency,
   debt, read, overload and drain qualification from the
   [performance proposal](write-performance-proposal.md).
