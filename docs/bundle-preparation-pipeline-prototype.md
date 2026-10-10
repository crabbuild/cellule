# Bundle preparation pipeline prototype

October 10, 2026. An external two-cohort prototype can construct an immutable
successor before its predecessor uploads or receives authoritative selection.
Ordered selection and complete dependency verification still prevent early
coverage. **This is protocol feasibility evidence; the serving runtime remains
serial and the performance goal remains unmet.**

## Reproduction and scope

The source starts at `764fafc6be76de382c34aa086cf9d18e6fec1437`.
On that source, preparing the next contiguous capture before selecting its
predecessor fails with `bundle native range is not contiguous in its lane`.
The managed publisher awaits one complete `select` operation, including
preparation, PUT, fresh origin verification and CAS, before processing the next
cohort. Its native-byte credit remains retained until selection confirmation.

The prototype separates encoding from uploading. A successor reads its exact
predecessor's encoded metadata while other referenced objects use the original
loader. This metadata view grants no availability or authority. Selection
still performs the complete fresh object read, verifies required reconstruction
dependencies and compares the exact predecessor in the canonical CAS.

The ordinary materializer already permits 45 seconds of root age and batches
up to its existing command/history limits. Another root-delay change is not
the selected next action; the [earlier delay experiment](pr67-fleet-root-delay-measurement.md)
was rejected. The pipeline targets serialized immutable work without changing
Cell transactions, follower membership or durability proofs.

## Verified schedules

| Schedule | Observed result |
| --- | --- |
| Two 64-Cell cohorts with 2,000 catalog bindings; neither object uploaded | Successor encodes with the exact first head as predecessor; no PUT or selection credit |
| Select encoded but unuploaded first cohort | Rejects missing origin; no CAS |
| Upload successor while predecessor is absent | Selection rejects required missing history; no CAS |
| Upload both, select successor first | Rejects the predecessor mismatch; no CAS |
| Select both in order | All 64 Cells cold-restore the seed and both exact mutation/result pairs |
| Origin loses a required predecessor after successful selection | Fresh full verification rejects a later selection attempt |
| Intervening sibling binding changes the catalog | Both prepared candidates reject the new catalog; no CAS |
| Writer fences before late uploads finish | Both uploads can exist, but neither obtains coverage |
| Hold the first PUT; complete the second PUT | Second upload completes; coverage remains zero until first selection, then advances in order |

The existing 124 bundle cases and four initial probe cases pass together.
The additional held-PUT case passes separately; strict runtime Clippy passes
on the final prototype. The extended bounded bundle model checks 47,498 distinct
states without a safety violation. All five existing negative configurations
still expose their expected invariant. An additional expected counterexample
proves that two preparations before either upload/selection are reachable;
the positive result therefore covers that new schedule.

The model abstracts complete byte/dependency verification and covers two Cells
and two bundles. It does not prove the Rust adapter, memory accounting, provider
persistence, lifecycle joining, liveness or throughput.

## Required serving integration

1. Distinguish encoded and uploaded proposals in types. The external prototype
   reuses the private prepared representation solely to test the protocol seam.
2. Admit every retained stage and selected-receipt allocation before dispatch.
   Preserve existing node budgets and the 32-MiB startup case. Receipt admission
   must not deadlock behind a pipeline holding catalog ordering.
3. Retain one ordered catalog mutation path across binding, checkpoint and close,
   while heartbeat renewal continues and selection rechecks the original lease.
4. Join accepted upload/verification work after cancellation, failure and drain;
   retain unresolved original captures and exact checkpoint callbacks.
5. Integrate the actual managed publisher, then measure native-byte wait, selected
   throughput, root debt and mixed request latency under unchanged qualification.

The source, patch, logs, model counterexamples and failed setup attempt remain
outside Git at `cellule-ios-parity-20261010/bundle-preparation-pipeline-prototype`.
Its `evidence-index.json` SHA-256 is
`400ab6ec48494bb9fee21d21228cc820ef16ddebdd4c7ab5d90ebda0a87dd41f`.
