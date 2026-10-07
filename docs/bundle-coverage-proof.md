# Bundle coverage authority decision

Status: protocol model and implementation design; bundle-based bucket responses
are disabled. Shared payload upload and signed append grants are separate
implemented paths. The [proposal](write-performance-proposal.md) remains the
qualification contract.

## Cost and latency gap

The packed implementation measured 1.045 commands per selected Cell root at
2,000 offered bucket writes/s over 1,000 uniform Cells. Root, accumulating
lineage and Cell control still require approximately `3 / 1.045 = 2.87` PUTs
per command before payload and maintenance work. A shared payload changes its
cost to `1 / cohort_commands`; it cannot remove those authority writes. Under
the deterministic uniform schedule, each Cell receives a command every 500 ms.
Waiting for twelve commands per root exceeds the 200-ms bucket latency budget.

M4 therefore needs an exact recoverable range proof whose selection is shared
across Cells, with asynchronous materialization. It also needs to release proved
capture bodies from retained memory while keeping bounded authenticated locators.
A faster upload without these changes can still accumulate debt and exhaust
admission.

## Authority and transfer

Use the existing canonical node record's mutable log authority for both the
binding-catalog digest and selected immutable range head. Lease renewal,
publication, binding closure and node recovery must CAS this same record and
preserve each other's fields. A separate selector checked against a previously
read node advertisement would reinstate a fencing race.

| Object or capability | Exact meaning |
| --- | --- |
| Cell binding | Cell/incarnation, writer epoch, exact base root/schema/code, permitted node boot/log epoch, and a unique binding identity pinned by Cell control |
| Immutable binding catalog | Complete active bindings plus terminal closure endpoints; closed binding IDs cannot be re-added |
| Immutable range manifest | Contiguous ordered node ranges, exact Cell bindings and commit/transaction intervals, scoped byte extents and digests, complete native outcome/dependency coverage, predecessor head |
| Node selection | Post-upload CAS of both catalog and range head while the original node/log remains Open and every row's binding remains active |
| `BundleCoverageProof` | Opaque capability minted only after exact selection reconciliation and complete dependency verification; uploaded bytes cannot construct it |
| Materialization | Exact logical endpoint reconstructed from base and selected rows; ordinary Cell root/lineage CAS consumes the same proof |
| Closure endpoint | Complete selected prefix frozen in the same CAS that closes the binding; new Cell ownership cannot execute before reconstructing it |

A live-node Cell transfer must first close its old binding in that node's
canonical record. A proposed upload holding an earlier catalog/head version
then loses its CAS, reloads, and rejects the closed rows. The closed binding
retains all earlier selected rows and its base until exact reconstruction or
retention proof releases them. Only then can Cell authority select the next
writer. Failed-node recovery first fences the whole original node record and
freezes its selected head before closing individual bindings. This ordering
must also govern release, takeover, tombstone, migration, backup and failed
shutdown; a lower-level Cell transition cannot bypass it.

## Atomic integration required before enabling responses

| Surface | Required production change and rejection case |
| --- | --- |
| Node advertisement/log codecs | Catalog/head/materialized frontiers preserved by every heartbeat, enrollment, closure and recovery CAS; reject old formats atomically |
| Cell control and acquisition history | Pin exact binding before SQL; close/freeze it before ownership departure; reject live-node late rows |
| Runtime durability gate | Distinct bundle source and proof; exact ticket/row coverage; no proof from upload alone |
| Actor and executor | Advance proven outcome/query endpoint together; retain debt until proof; replace proved capture bodies with bounded immutable locators |
| Materializer | Coalesce selected intervals under host budgets; root and schema stay byte-identical; failure cannot invalidate an earlier bundle ACK |
| Recovery | Verify base, catalog and every required manifest/row; reject omissions, duplicates, conflicting ranges, missing outcomes and origin dependencies |
| Follower retirement | Preserve the distinction between bundle coverage and materialized-root coverage; a sealed record cannot hide an unmaterialized acknowledged suffix |
| Backup/collection | Mark selected ranges, catalog bases, dormant bindings, lineage and pins across all Cells/nodes; require maintenance and grace before deletion |
| Qualification | Owner/receiver death before root materialization, live-node Cell transfer, delayed uploads, late appends, lost CAS responses, missing/corrupt ranges and all-ACK cold retry |

The manifest needs a bounded file-backed index and a checkpoint strategy for
long histories. Merely chaining every per-command manifest leaves an unbounded
cold-recovery scan. Catalog checkpoints can advance bases only after verified
materialization; complete reference inventory must survive every intermediate
CAS and cancellation. Its object and metadata costs remain in qualification.

## Protocol evidence

`WriteProofs.tla` separates upload, exact selection, binding closure and transfer.
The positive configuration enumerated 13,356 distinct states. The node-only
negative configuration produces `Prepare → CloseBinding → Transfer → Select`,
violating `BucketCellFence`; ignoring closure also loses the frozen endpoint.
Separate negative configurations expose receiver append after a durable seal
and grant expiry extended by wall-clock rollback.

The model assumes complete verified immutable inputs and atomic authority CAS.
It does not prove manifests, Rust adapter ordering, SQLite bytes, cryptographic
identity, provider semantics, fair completion or the full failure matrix. No
bucket response or retention release uses this proposed proof in production.
