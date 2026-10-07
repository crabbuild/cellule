# Bundle coverage authority decision

Status: protocol model and connected implementation APIs; bundle-based bucket
responses remain disabled. [Implementation and remaining gates](bundle-coverage-implementation.md)
records the verified slice and its limits. Shared payload upload and signed
append grants are separate implemented paths. The
[proposal](write-performance-proposal.md) remains the qualification contract.

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

At one command per selected root, the three per-Cell metadata PUTs alone would
require 6,000 PUTs/s at the bucket target, before payloads, node selection or
maintenance. The complete M4 budget there is 100 PUTs/s. This is a cost bound
for that sparse schedule, not extrapolated measured throughput. Fleet can
coalesce more root work because a follower proof may respond earlier, but it
still needs bounded retention and eventual object coverage; a longer root timer
cannot release an unproved capture or hide its debt.

The latest M2/M3 Docker candidate at `a3787a8d` completed three 300-second
windows at 100 offered writes/s. Scheduled p99 was 19.8 / 23.1 / 46.9 ms with
zero errors/drops/unissued offers; every run's 34,001 seed/warmup/window ACKs
passed warm and cold GET/retry audits. Fleet remained active and follower
proofs advanced. Two debt trends failed and total PUT cost was
5.229–5.433 per command. These are diagnostic points, not a qualified capacity
or parity claim. The first window supplies the detailed observations below.

| Window observation | Value | Remaining work |
| --- | ---: | --- |
| All provider PUT successes/command | 5.4330 | Amortize authority selection, not only payload upload |
| Cell-authority PUT successes/command | 2.2507 | Root lineage and Cell selection remain per-Cell work |
| Immutable PUT successes/command | 2.2422 | Includes payloads, roots and maintenance |
| Node-authority PUT successes/command | 0.9401 | Coverage selection is almost per-command at this arrival rate |
| Fresh owner + receiver enrollment GETs/command | 0.0138 | Grant budget passes in this single window; paired qualification remains required |
| Singleton Cell submissions | 98.64% | At this sparse arrival rate the coordinator uses canonical native packs |
| Cells/multi-row shared cohort | 2.7020 | Only 151 multi-row cohorts; this excludes singletons |
| Commands/selected Cell root | 1.0000 | Delaying a Cell to collect many commands cannot meet the sparse latency target |

The first window's strict three-minute trend gate failed: unpublished debt rose by
34.80 bytes/s and oldest debt by 0.0817 ms/s in the fitted tail. These slopes
must remain failures in the evidence; one run cannot distinguish sustained
growth from sampling variation. Increasing cohort delay without a measured
latency/debt benefit is not a remedy.

At the 15K Fleet target, the same candidate accumulated 70.64 MB of unpublished
log debt and its object frontier lagged by 50,548 sequences. It completed only
295.263 commands/s, failed delivery and had 432 warm-audit HTTP 503s. The
[delivery report](write-performance-delivery.md) preserves this failure.
Follower acknowledgments keep the immediate proof path short only while the
publication/retention path can make progress. M4 must reduce the complete cost
and bound unmaterialized locators, with headroom for leases and drain.

## Authority and transfer

Use the existing canonical node record's mutable log authority for both the
binding-catalog digest and selected immutable range head. Lease renewal,
publication, binding closure and node recovery must CAS this same record and
preserve each other's fields. A separate selector checked against a previously
read node advertisement would reinstate a fencing race.

| Object or capability | Exact meaning |
| --- | --- |
| Cell binding | Cell/incarnation, writer epoch, exact base root/schema/code, permitted node boot/log epoch, and a unique binding identity pinned by Cell control |
| Immutable binding catalog | Provisional enrollment obligations, complete Open/Closing bindings and terminal Closed endpoints; closed binding IDs cannot be re-added |
| Immutable range manifest | Contiguous ordered node ranges, exact Cell bindings and commit/transaction intervals, scoped byte extents and digests, complete native outcome/dependency coverage, predecessor head |
| Node selection | Post-upload CAS of both catalog and range head while the original node/log remains Open and every row's binding remains active |
| `BundleCoverageProof` | Opaque capability minted only after exact selection reconciliation and complete dependency verification; uploaded bytes cannot construct it |
| Materialization | Exact logical endpoint reconstructed from base and selected rows; ordinary Cell root/lineage CAS consumes the same proof |
| Closure endpoint | Closing freezes the complete previously issued range; Closed records its complete selected endpoint; new ownership cannot execute before reconstructing it |

A live-node Cell transfer must first quiesce SQL, join accepted capture/submission
jobs and close issuance for its old binding in the ordered node lane. A CAS
marks that binding Closing and freezes its complete assigned endpoint. Previously
issued, exactly verified rows may drain through that boundary using fresh
authority; a proposed upload holding the earlier catalog/head version loses
its CAS. Terminal Closed rejects all later rows and retains the complete selected
prefix and base until reconstruction or retention proof releases them. Only then
can Cell authority select the next writer. Failed-node recovery first fences
the whole original node record, seals the complete follower endpoint and
accounts for its accepted suffix beyond the selected head before terminal
closure. This ordering
must also govern release, takeover, tombstone, migration, backup and failed
shutdown; a lower-level Cell transition cannot bypass it.

The accepted endpoint and selected endpoint are distinct. A Fleet command can
already be acknowledged from native followers above the last selected bucket
range. Freezing only that selected prefix could let a new writer omit a prior
ACK. Do not use a sampled follower watermark as the issuance boundary: an
accepted SQL/capture job may still be assigning its ticket. Join the original
jobs, stop assignment, and retain every old range until exact object coverage
or reconstruction consumes it. If the old node cannot establish this barrier,
fence/recover its complete log before exposing the replacement Cell writer.

## Atomic integration required before enabling responses

| Surface | Required production change and rejection case |
| --- | --- |
| Node advertisement/log codecs | Catalog/head/materialized frontiers preserved by every heartbeat, enrollment, closure and recovery CAS; reject old formats atomically |
| Cell control and acquisition history | Pin exact binding before SQL; quiesce/join/stop issuance, drain its frozen accepted range and reconstruct before ownership departure; reject live-node late rows |
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
Recovery needs an authenticated lookup of the chosen binding's selected suffix,
not a scan of every unrelated Cell's data object. Bound and measure index
depth, objects/bytes read, retained locator bytes and reconstruction work before
choosing checkpoint spacing. Public receipts already name a logical Cell
incarnation/command sequence, but the read-replica implementation opens a
materialized root: delayed roots must preserve receipt-bound visibility without
claiming that an older root contains the selected suffix.

## Production cutover and verification order

The authority record, Cell binding and proof must change together. Implement
the following vertical slices behind a disabled response gate, then enable the
gate only after the final slice passes. A new wire/record version is an atomic
development-format cutover; an old reader must reject it rather than silently
discarding catalog or coverage fields.

| Slice | Existing implementation to extend | Required evidence before the next slice |
| --- | --- | --- |
| Canonical authority | Node directory record/CAS and `control/authority/acquisition` | Heartbeat/coverage/closure racing on one CAS preserves every field; live-node transfer covers the frozen issued endpoint, including prior Fleet ACKs beyond the selected prefix; fresh reconciliation after a lost PUT reply |
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

### Quantified checkpoint constraint

As a design calculation, assume a manifest is embedded in one immutable data
object and one node-selector PUT selects each 64-command cohort. Those two
requests already cost `2 / 64 = 0.03125` PUTs/command. If ordinary checkpoints
retain three root/lineage/Cell-selection PUTs, the remaining M4 budget requires
`0.03125 + 3 / commands_per_checkpoint <= 0.05`: at least 160 commands per
checkpoint, before compaction, catalog changes or retries. If manifests need a
separate PUT, three requests per 64 commands leave even less checkpoint budget.

The 160-command value is an optimistic metadata lower bound. A checkpoint that
rewrites bundle dependencies into one new independent data object has at least
four PUTs, requiring at least 214 commands per checkpoint under the same
two-PUT/64 assumption, before indexes, multipart requests or maintenance. A
root that keeps shared payload references may shorten the manifest scan, but it
cannot release those payload objects. Account for lookup checkpoints and actual
data reclamation separately; independent checkpoints cannot be treated as free
uploads.

At the uniform 1,000-Cell bucket target of 2,000 commands/s, 160 commands per
Cell span approximately 80 seconds; at 15,000 Fleet commands/s they span about
10.7 seconds. These are inferred cost constraints, not measured performance or
a recommended checkpoint delay. Production must either verify that such lag
has bounded file-backed locators and acceptable cold/sparse-read costs, or
amortize checkpoint authority and lineage work as well. Merely allowing bundle
ACKs while publishing every ordinary root would miss the target.

For the two-PUT assumption, a cohort must contain more than 40 logical commands
to leave any budget for materialization. At 2,000 commands/s, collecting 40
commands takes about 20 ms in the uniform arrival model. A flush policy must
budget assembly, upload, selector reconciliation and queueing against the
200-ms bucket p99; extending the existing 1-ms cohort policy without that
measurement would be speculation. Count logical commands separately from
manifest rows because one native capture can cover a command range.

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

`BundleCoverage.tla` extends the bounded model with two ordered ranges and two
Cell bindings, separate upload/selection/reconciliation, delayed per-Cell
checkpoints, node fencing and exact closure endpoints. It permits incomplete
uploads but requires their rejection before selection. Its five negative
profiles test node-only selection, unchecked contents, a skipped range,
unproven reads and collection of a selected range before both Cells have
checkpoints. Exact bytes/outcomes and checkpoint authentication remain abstract
inputs; this is protocol evidence, not a production recovery implementation.
At `75ae439d`, CI completed 44,048 distinct positive states and all five named
counterexamples. The separate append/closure model and its three counterexamples
also passed in that run. This extends the earlier `d30e6f11` run's 21,672 states.

The current model lets the hot Cell continue after its sibling closes.
Closure freezes the sibling's per-Cell endpoint rather than the node's global
head. Reusing an uploaded hot-Cell range after that CAS conflict requires fresh
authorization of every participating row and the unchanged predecessor; it
cannot reopen or publish rows from the closed binding. An independent checkpoint
means a fully authenticated base selected by authority; an ordinary root that
still references the bundle cannot authorize collection. The model does not
prove byte codecs, complete production reference inventory, history/backup
retention, grace boundaries or liveness.

`BindingDrain.tla` adds the missing Fleet composition: native durable ACKs may
precede object selection. Closing freezes issuance and drains exactly that old
range before terminal closure and reconstruction. Its positive profile also
checks eventual closing under weakly fair successful upload/selection. Three
negative profiles require counterexamples for selected-only transfer, late
issuance and premature follower retirement. At `954cacf`, CI passed all 801
distinct positive states, the fair closing property and the three named
counterexamples. The selected-only trace is
`Issue → Replicate → FleetAck → BeginClose → FinishClose → Transfer`: the
new writer's reconstructed endpoint omits the acknowledged command.
Exact captures/checkpoints and the Rust join/close
barrier remain production obligations, not proofs supplied by this abstraction.
The [three-model CI run](https://github.com/crabbuild/cellule/actions/runs/37620661618)
preserves all eleven required unsafe counterexamples across the independent
models. Their state counts cannot be combined into a proof of one production
protocol.
