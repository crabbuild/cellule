# PR 67: independent selected-capture cleanup

**The fresh integrated candidate regresses throughput and successful-write
latency and fails drain. PR #67 is not ready to merge. Architectural parity
and performance acceptance remain unmet.**

## Fresh integrated comparison: 2026-10-10

Measured production is `e151e779`, including selected-capture cleanup and main
`36e1f3f6` (PR #64). The baseline is `6e2ba162`; celld is `f2bf6486`. This pair
measures the combined changes, not the isolated effect of root cleanup.

All cases use 2,000 uniform Cells, 96-byte SQL-ledger values, 128 clients and
queue slots, 2,000 offered Fleet writes/s, no reads, 30 seconds warmup and 60
measured seconds. One owner, two followers, clients and a pinned RustFS instance
share an eight-CPU/~8-GiB Linux Docker VM. Node state is tmpfs, SQLite uses
NORMAL and each case has a fresh object-store volume. Cellule retains its
64-MiB retained-memory and 1-GiB managed-disk ceilings. The source, fixture,
image and driver manifests are retained; both Cellule arms use identical
Rust HTTP driver binaries. This fresh artifact directory was outside the
completed cleanup's targets.

| System | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers | Warm/cold audit |
| --- | ---: | ---: | ---: | ---: | --- |
| Cellule baseline | 566.45 | 373.10 | 26,335 | 59,520 | Both pass; 63,875 ACKs |
| Integrated candidate | 276.62 | 1,010.32 | 46,954 | 56,317 | Warm passes for 42,210 ACKs; cold not reached |
| celld | 1,985.03 | 151.61 | 0 | 891 | Both pass; 181,087 ACKs |

TPS counts successful completions inside the 60-second window. Successful p99
starts at scheduled arrival and includes trailing successful completions;
fast errors are excluded. Candidate all-attempt p99 is 732.01 ms and would
understate successful-write latency. No producer requests are unissued. Every
original output, offer/error/drop counter, complete ACK inventory and reported
latency is independently reconciled from raw journals. Every Cell has a
successful measured write in each arm.

The observed candidate throughput is 51.17% lower and successful p99 is 2.71
times the baseline. One short pair does not isolate which integrated change
causes the regression. The baseline drains in 54.48 seconds; celld in 14.09.
The candidate owner exceeds the unchanged 120-second drain deadline. Its logs
show fenced materialization jobs and a fenced node-log drain; both followers
exit 1. This is failed draining and unverified cold recovery, not evidence of
acknowledged-data loss. The original comparison controller exits 1. All
original verification, build, comparison and analysis handles are joined.

The measured native submission phases form an exact closed partition. Mean
submission time is 0.14 ms before and 0.30 ms after, with publication-slot wait
below 0.001 ms in both; separately sampled Fleet proof waits average 77.43 and
83.83 ms. These populations differ and must not be added as a per-command
latency partition. Followers group about 8.93 versus 9.44 frames per data sync,
with zero measured append failures. These tmpfs observations do not qualify
physical power-loss durability or establish hardware fsync cost.

| Publication observation | Baseline: start → end | Candidate: start → end |
| --- | ---: | ---: |
| Retained memory, MiB | 47.64 → 60.87 | 48.00 → 47.76 |
| Retained capture bytes, MiB | 11.31 → 17.25 | 10.20 → 12.44 |
| Unpublished log bytes, MiB | 37.40 → 74.14 | 29.55 → 51.54 |
| Oldest unpublished age, seconds | 50.79 → 71.86 | 54.20 → 101.44 |

Lower retained memory does not establish sustainable publication. Both cases
accumulate unpublished bytes; the candidate's oldest debt grows through nearly
the entire measured interval. Admission availability, publication progress and
complete issued-range drain remain blockers.

All 16 isolated contributor/qualification routes pass on the integrated code:
format, all features/targets, workspace tests, local LTX, Rust 1.97/1.99 Clippy,
API docs, boundaries/layout, document fences/links, SQL/peer contracts, script
tests and three repetitions of the root-cut regression. Initial merge compile
errors, the owner-reader durability test failure and two imported broken links
are retained as failed attempts, followed by passing repairs. Standalone writer
and owner reader remain FULL; explicit externally durable mode sets both to
NORMAL and fences the session on partial configuration failure. These local
checks do not imply a remote CI result or passing performance qualification.

The raw evidence and independent audits remain outside Git at
`/Volumes/Workspace/crabbuild-target/native-root-cut-delivery-20261010-970ca31`.
This is an HTTP/SQL application comparison: native Rust/Axum for Cellule and a
JavaScript Worker/Durable Object in celld's Rust/V8 daemon. The core write paths,
internal budgets and runtime interfaces remain unequal. Celld uses its pinned
four-round, zero-window, ordered one-shot HTTP defaults; Cellule uses eight
rounds and a general four-millisecond assembly interval. None of these short
cases passes the unchanged qualification gates; no read guardrail or common
Rust core qualification was run in this pair.

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
Those verification logs were subsequently deleted. Fresh isolated verification
was regenerated for the integrated delivery above. Persisted formats, response proof requirements and
the existing memory, locator and materializer ceilings are unchanged.

This follows celld's separation of
[shipping, bundle publication and compaction](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4281).
The [core-path reference](write-performance-proposal.md#core-write-path-reference)
defines the remaining capture, metadata and replication work. Cellule still
performs catalog/history work absent from celld's ordinary new-entry bundle
upload; the complete write paths are not identical.

## Earlier interrupted attempts

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

Fresh provenance and isolated verification are now retained. Resolve the
integrated regression, initialization/overload availability and fenced drain;
then repeat matched measurements with protected artifacts. Demonstrate complete all-ACK
cold recovery, joined draining, bounded debt and read guardrails. The
[proposal's acceptance gates](write-performance-proposal.md#decision-and-success-criteria)
remain unchanged. This report supplies no qualified capacity or parity claim.
