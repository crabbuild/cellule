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

The corrected M2/M3 Docker candidate at `d351d886` completed one 300-second
window at 100 offered writes/s: 100 completed writes/s, 15.6-ms scheduled p99,
zero errors/drops/unissued offers, and all 34,001 seed/warmup/window ACKs passed
warm and cold GET/retry audits. Fleet remained active and follower proofs
advanced. This is a diagnostic point, not a qualified capacity or parity claim.

| Window observation | Value | Remaining work |
| --- | ---: | --- |
| All provider PUT successes/command | 5.4842 | Amortize authority selection, not only payload upload |
| Cell-authority PUT successes/command | 2.2521 | Root lineage and Cell selection remain per-Cell work |
| Immutable PUT successes/command | 2.2499 | Includes payloads, roots and maintenance |
| Node-authority PUT successes/command | 0.9822 | Coverage selection is almost per-command at this arrival rate |
| Fresh owner + receiver enrollment GETs/command | 0.0138 | Grant budget passes in this single window; paired qualification remains required |
| Cells/shared cohort | 1.0023 | At this sparse arrival rate the bounded coordinator mostly flushes singletons |
| Commands/selected Cell root | 1.0000 | Delaying a Cell to collect many commands cannot meet the sparse latency target |

The strict three-minute trend gate failed: unpublished debt rose by
98.53 bytes/s and oldest debt by 0.152 ms/s in the fitted tail. These slopes
must remain failures in the evidence; one run cannot distinguish sustained
growth from sampling variation. Increasing cohort delay without a measured
latency/debt benefit is not a remedy.

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

## Production cutover and verification order

The authority record, Cell binding and proof must change together. Implement
the following vertical slices behind a disabled response gate, then enable the
gate only after the final slice passes. A new wire/record version is an atomic
development-format cutover; an old reader must reject it rather than silently
discarding catalog or coverage fields.

| Slice | Existing implementation to extend | Required evidence before the next slice |
| --- | --- | --- |
| Canonical authority | Node directory record/CAS and `control/authority/acquisition` | Heartbeat/coverage/closure racing on one CAS preserves every field; live-node transfer freezes the exact old binding; fresh reconciliation after a lost PUT reply |
| Ordered immutable range | Native node frames and node shipper; LTX verified upload/restore | Bounded canonical manifest, exact predecessor and full native commit/outcome bytes; missing, duplicate, overlapping and cross-binding rows rejected |
| Opaque proof and logical endpoint | `node/durability`, actor response/query gates | Upload cannot mint proof; selected exact range can; query and retry visibility stop at the same proven endpoint; lease loss stops proof release |
| Asynchronous roots | Canonical publication and LTX materialization | Root lag does not retain unbounded capture bodies; same bytes and outcomes from base plus selected suffix; materializer errors leave earlier ACKs recoverable |
| Transfer, retirement and inventory | Acquisition history, follower seal/retirement, recovery/backup/retention | Owner death before root materialization, dormant sibling, lost seal, late append and GC grace races preserve every accepted range |
| Enable and qualify | Existing pinned Docker runner/client/auditor | All-ACK warm/cold retry after original nodes are destroyed, three paired stable windows, read guardrails and overload/drain; include checkpoint and maintenance cost |

For the owner-death case, freeze root materialization, accept commands only
through selected range proofs, destroy the original owner and follower state,
then reconstruct from the bucket. Verify every acknowledged request's returned
value and exact stored retry result. Graceful drain alone does not exercise
this condition. For live-node transfer, hold an upload across binding closure
and require its old CAS to fail before the new writer can execute.

Before accepting a format, account for the full steady-state cost: immutable
data, manifest/catalog checkpoints, node selection, Cell roots, lineage,
compaction and retries. The 0.05-PUT target applies to their sum. A bundle size
calculation that omits materialization or catalog checkpoints cannot qualify
M4. Checkpoint spacing also needs a bounded cold-restore scan and the same read
guardrail; it is a measured policy decision, not an assumed saving.

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

`BundleCoverage.tla` extends the bounded model with two shared ranges and two
Cell bindings, separate upload/selection/reconciliation, delayed per-Cell
checkpoints, node fencing and exact closure endpoints. It permits incomplete
uploads but requires their rejection before selection. Its five negative
profiles test node-only selection, unchecked contents, a skipped range,
unproven reads and collection of a selected range before both Cells have
checkpoints. Exact bytes/outcomes and checkpoint authentication remain abstract
inputs; this is protocol evidence, not a production recovery implementation.
