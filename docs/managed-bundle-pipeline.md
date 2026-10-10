# Managed bundle publication pipeline

October 10, 2026. The runtime-owned producer can stage and upload one successor
while its predecessor is publishing. Selection and receipt confirmation remain
ordered. The latest completed production diagnostic reaches 1,249.90 writes/s
and 12,580.70 reads/s, with 240.5-ms write request p99, dropped offers and growing
root-materialization debt. Removing the checkpoint serial guard did not improve
completed throughput in its complete pair and remains an unadopted experiment.
**The 2,000-Cell mixed-load goal remains unmet; these short runs do not qualify
sustainable capacity.**

## Publication and admission

`NodeBundlePublicationAuthority::begin_round` acquires catalog ordering and
observes its original head. The round stages immutable CNB3 proposals, uploads
them and selects each through complete fresh dependency verification and the
existing authority CAS. Heartbeat state is released during staging and upload.
Binding, checkpoint and close operations retain the same catalog ordering.
The manual `select` method uses these same three phases.

| Event | Managed behavior |
| --- | --- |
| First cohort starts | Keep the existing 20-MiB working reservation and 2-MiB shard cache |
| Another capture arrives during publication | Try to admit another 20 MiB plus conservative receipt bounds for both cohorts on the original ledger |
| Extra credit unavailable | Keep that capture for the next serial round; do not wait while holding catalog ordering |
| Successor PUT finishes first | Retain uploaded bytes; grant no coverage or response credit |
| First selection succeeds | Confirm its original receipts and release its native capture credit independently of the second PUT |
| Both uploads have joined | Select the exact successor after its predecessor; release staged bytes and extra working credit |
| Either branch fails or the lease is fenced | Join both accepted I/O lifetimes and preserve the first observed source error |

Each cohort retains the original 64-frame, 4-MiB and one-millisecond assembly
bounds. Receipt admission covers the maximum native history, live assignments,
bounded control allocation and origin path. Confirmation still checks actual
costs before transferring reservations or waking any capture.

The two-cohort window is lazy: a capture can arrive after the first PUT has
started. There is no requirement to collect both cohorts before dispatching
the first. Selection advances only the original complete assignments and
preserves heartbeat and coverage changes observed after staging.

## Checkpoints and joined lifetime

A first cohort containing root checkpoint notifications uses serial publication.
The lookahead cohort contains native captures only. Ready root notifications
join the next first cohort or the existing idle checkpoint path. This preserves
the original root-credit release path and bounds callback retention during a
held upload.

When extra receipt credit is unavailable, the round returns catalog ordering
before the existing receipt-admission wait. That wait continues to service
checkpoints that can free its own credit. The 32-MiB startup case uses this path
without increasing its budget or weakening its metadata ceiling.

Managed failure, fencing and shutdown do not cancel already-dispatched uploads
or checkpoint I/O. Cancelling a shutdown caller leaves the original worker
alive; a later shutdown joins that worker and observes its terminal result.
Manual callers continue to own their admission and accepted I/O lifetimes.

## Regression evidence

The serving regression submits a second Cell only after the first immutable PUT
has entered a held store operation. On `d42b978`, the second upload never starts
before the unchanged two-second deadline. With the pipeline it finishes while
the first PUT remains held, with zero early selection credit. After ordered
selection, both Cells cold-restore their exact mutations and outcomes.

Other cases confirm the first receipt while the successor PUT remains held,
exercise serial fallback on the original 32-MiB ledger, cancel a shutdown waiter
while two uploads are accepted, and fail the first PUT while retaining the
second until it joins. Each case returns all retained-byte credit. Existing
maximum-history and full-cohort fixtures check the conservative receipt bound.
The real example authority also exercises two staged cohorts, heartbeat renewal,
blocked binding and rejection of out-of-order selection.

A longer-stall regression holds the first PUT across 31 seconds and renews the
writer during that wait. Its initial implementation rejects successor staging
because the advertisement saved at round entry has expired. The round now pins
the catalog head and borrows the renewed local advertisement for staging;
selection retains its fresh authoritative read. If first selection completes
during successor assembly, staging from that exact selected head produces the
same successor proposal. A changed predecessor still rejects selection.

The [typed staging protocol](typed-bundle-staging.md) retains its byte
compatibility, dependency verification and two-preparation model checks. Those
model checks do not establish host memory accounting or adapter liveness; the
managed tests cover those lifetimes separately.

## Current application measurement

The completed production baseline uses `4712c4c`, whose Rust/Cargo sources match
`ea4903d` on main. One complete pair and an unpaired candidate repeat use an
8-vCPU/16-GiB owner, two required followers, RustFS and 2,000 uniformly active
Cells. Each offers 2,000 writes/s and 20,000 reads/s after a 30-second warmup,
with a 60-second measurement, the same client/auditor binaries, workload and
resource limits.
Owner and follower state use tmpfs.

| Completed run | Writes/s | Reads/s | Write request p99 | Read request p99 |
| --- | ---: | ---: | ---: | ---: |
| Production baseline | 1,249.90 | 12,580.70 | 240.5 ms | 48.1 ms |
| Checkpoint-overlap experiment | 1,209.43 | 12,058.38 | 221.2 ms | 46.2 ms |
| Unpaired experiment repeat | 1,232.40 | 12,024.07 | 219.3 ms | 51.4 ms |

The baseline's separate read-only window completes 19,656.13 reads/s. Mixed-load
queued offers are dropped in all three runs. Root debt grows
134,899,950→203,855,477 bytes in production, versus
133,416,864→250,078,928 bytes in the paired experiment. The repeat grows
127,963,421→243,394,635 bytes. The production checkpoint guard is retained.

All 402,167 ACKs across the three completed cases independently reconcile with
raw client journals and pass warm/cold state and original retry audits. The final
baseline attempt fails copying client evidence with an unexpected EOF while the
dedicated VM reports filesystem I/O errors. Its artifacts and stopped disk
images are preserved; it contributes no completed comparison or capacity claim.

Builds, failed/passed logs and snapshots are retained outside Git under
`cellule-ios-parity-20261010`. The three-run evidence index is
`checkpoint-pipeline-three-completed-evidence-index.json`, SHA-256
`3e62e448476193424d8c555ca9b021e130819f01f576ff180ff089bff2869b85`.
These diagnostics establish neither sustainable capacity nor celld parity.

CNB3 stores a 32-KiB authenticated header, changed catalog shards, new native
frames and detached Cell histories in one immutable upload. Unchanged extents
remain exact references to earlier objects. The
[format and recovery contract](bundle-coverage-implementation.md) describe its
digest chain and CNB1/CNB2 reader compatibility. The experiment changes no bytes.

## Verification

All 13 contributor routes pass on an isolated snapshot, including 2,095
workspace tests with 43 environment-dependent cases ignored, strict Clippy,
all targets/features, API docs, local LTX, boundary/layout checks and document
validators. The focused bundle suite passes 125 cases. The seven TLS-dependent
example authority cases also pass explicitly, including the 31-second stall.
Those fixture checks do not qualify provider/device durability or application
capacity. The existing two-preparation model sources remain byte-identical to
the tested typed-staging milestone.

Failed attempts are retained alongside passing logs: the baseline test's first
attempt used an unavailable candidate-only budget helper; its corrected attempt
compiled and reproduced serial publication. The first source-preservation
fixture used a permission error whose existing typed mapping omits its nested
message. The corrected permanent source-carrying fixture passes. Strict Clippy
first caught a test constructor name, and the extended renewal test reproduced
an expired-advertisement error before the round fix. No test deadlines, budgets
or qualification gates were relaxed.
