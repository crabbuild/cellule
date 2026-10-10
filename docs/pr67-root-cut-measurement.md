# PR 67: independent selected-capture cleanup

**Architectural parity and performance acceptance remain unmet. PR #67 stays
a draft. No acceptable throughput improvement is established by this change.**

## Delivered behavior

Production `970ca314` separates verified selected-capture cleanup from Cell-root
publisher ownership. A root task owns its original selected cut and publication
obligation; later selected debt has a separate obligation. Completing the first
root validates that original proof and preserves the later suffix in the native
worker. A proof that changes base waits for the original native checkpoint
witness. Old-base proofs can retire throughout root preparation and checkpoint
callbacks. Serving observations and drain include both obligations.

The managed-actor regression first failed on the preceding production path:
later selected captures stayed pending while an original callback was held.
It resumed that callback, drained both Cells, restored cold outcomes and checked
zero remaining credit before asserting the failure. The implementation passes
the strengthened regression, including cleanup during held preparation, and
three repetitions in the isolated verification snapshot. All 16 contributor
verification routes passed on `970ca314` before the evidence incident below.
Those verification logs were subsequently deleted; they must be regenerated
for reviewable delivery. Persisted formats, response proof requirements and
the existing memory, locator and materializer ceilings are unchanged.

This follows celld's separation of
[shipping, bundle publication and compaction](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4281).
The [core-path reference](write-performance-proposal.md#core-write-path-reference)
defines the remaining capture, metadata and replication work. Cellule still
performs catalog/history work absent from celld's ordinary new-entry bundle
upload; the complete write paths are not identical.

## Actual measurement attempts

The unchanged diagnostic profile is 2,000 uniformly addressed Cells, 96-byte
SQL-ledger values, 128 clients/queue slots, 2,000 offered writes/s, no reads,
30 seconds warmup and 60 measured seconds. One owner, two followers, object
store and clients share the eight-CPU/~8-GiB Docker VM. State is tmpfs and SQLite
uses NORMAL. The baseline is `6e2ba162`; celld is `f2bf6486`.

| Attempt | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers | Evidence limits |
| --- | ---: | ---: | ---: | ---: | --- |
| Candidate, first attempt | Not reached | Not reached | Initialization HTTP 503 | Not measured | 1,826 seed ACKs; owner drain exceeded 120 seconds; no cold audit |
| Preceding Cellule | 416.12 | 568.58 | 34,466 | 60,405 | 52,517 ACK rows and warm/cold audits reconciled before later deletion of evidence |
| celld | 1,937.07 | Not retained here | 0 | 3,761 | ACK and warm/cold audits reconciled before later deletion of evidence |
| Candidate, separate fresh attempt | 262.35 | 400.17 | 72,396 | 31,829 | Rescued measured-window journals only; interrupted case and missing source/binary manifests; no cold qualification |

TPS counts in-window successful completions. Successful p99 starts at scheduled
arrival and includes trailing successful completions, excluding fast errors.
The rescued candidate journals reconcile 15,741 in-window successes, 34 trailing
successes, all 88,171 measured attempts and all 120,000 offers. Its retained RAM
is 63.38→63.75 MB, unpublished log bytes 18.87→40.84 MB, capture bytes 8.27→12.74
MB and oldest publication age 52.74→96.44 seconds. These are observations from
an interrupted case, not evidence of bounded sustainable debt.

A concurrent `rm -rf` targeted the live artifact directory, including its
object-store files, source manifests and controller logs. The command was
paused reversibly when discovered. It also overlapped the wider comparison;
the exact onset of interference in each window is unknown. Neither a causal
improvement nor a causal regression can be assigned from these numbers. The
first initialization failure's underlying source remains unidentified and
must not be attributed to that deletion without evidence.

All original build, verification, measurement and analysis handles were joined.
The interrupted controller exited 1; its owner eventually exited 0. Remaining
followers were stopped and exited 137, so that cleanup is not a successful
graceful fleet drain. Their attempted scratch exports were empty; their logs
and terminal states were retained. The rescued journals, reconciliation and
surviving source fragments are outside Git at
`/Users/haipingfu/.codex-workspaces/active-cellule-evidence-rescue-20261009`.

## Remaining qualification

Regenerate isolated verification and complete source/binary provenance, then
repeat matched measurements in an idle environment with protected artifacts.
Resolve initialization/overload availability and demonstrate complete all-ACK
cold recovery, joined draining, bounded debt and read guardrails. The
[proposal's acceptance gates](write-performance-proposal.md#decision-and-success-criteria)
remain unchanged. This report supplies no qualified capacity or parity claim.
