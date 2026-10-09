# PR 67: publication admission handoff and write measurement

**ACK availability and draining improve in this diagnostic; write performance
regresses and parity remains unmet. PR #67 stays a draft.** The corrected runtime
completes 451.70 Fleet writes/s versus 481.80 before and 1,996.33 for fresh celld.
Successful scheduled p99 worsens 285.52→1,285.01 ms. All 47,471 candidate ACKs
pass warm/cold mutation and original retry audits; fleet drain takes 47.68 seconds.
One short pair establishes no repeatable causal performance change.

## Failure and implementation

The preceding native pipeline already moves publication-capacity waits before
global ordering, pipelines eight original follower rounds, groups canonical
follower append/fsync and reuses encoder-checked metadata after a complete fresh
origin match. Its [measurements](pr67-native-pipeline-measurement.md) show that
expensive historical/base verification and checkpoint work remain.

A warning-only reproduction at `f858ed3cdab6699cd9c5e837ba7025c9b36f80b4`
identifies a separate post-commit failure. Two measured requests on Cells 1294
and 1405 return `outcome_unknown`; both owner warnings preserve the original
`Capacity("pending publication bytes")`. These exact Cell IDs account for all
38 warm audit HTTP 503s. The original owner process wait exceeds 120 seconds
while drain logs `PendingPublication`. Unavailability does not establish data loss.

After SQL has committed and returned its physical capture, the actor previously
tried a fresh global RAM reservation while retaining the command's declared
maximum result allowance. The SQL example admits up to 1 MiB of result data even
for a tiny actual result. Concurrent retained work can fill the ledger before
that second reservation; ingress headroom cannot guarantee it succeeds.

`cf4785c675698afe786f566242d6d3dace32e775` transfers unused credit from the
original completed command to the full publication charge when it fits. The
input and actual result remain charged until their command drops; the capture
retains its own reservation until publication releases it. The total ledger
charge never increases during transfer. Larger cuts still require their entire
fresh charge under the original limit. Ordinary commands and native SQL groups
use the same handoff; group members retain their own original result sizes.
Opaque failed jobs keep their original allowance. Cancellation/deadline paths
retain admissions until the original worker actually exits.

This fixes a demonstrated admission race without raising the 64-MiB retained,
1-GiB disk or 20-MiB publication-working budgets. It does not remove publication
I/O or promise capacity for every possible capture size.

## Write results

Same profile: 2,000 uniform Cells, 96-byte SQL values and durable request/result
ledger, one owner plus two followers, WAL NORMAL/tmpfs, 128 clients and queue
slots, 2,000 offered writes/s, 30-second warmup and 60-second measured window.
The before and after builds have byte-identical driver and auditor binaries,
fixture sources and images, distinct serving binaries and recorded source
identities. Celld `f2bf648663a610eefde71f3547ad61e9b896b1f0` runs fresh after
the candidate. Our builds, contributor suites and independent replay do not
overlap the timed windows.

| Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: |
| Cellule before, `f858ed3` | 481.80 | 285.52 | 66,660 | 24,296 |
| Cellule handoff, `cf4785c` | 451.70 | 1,285.01 | 68,219 | 24,578 |
| Fresh celld, `f2bf648` | 1,996.33 | 107.12 | 0 | 155 |

TPS counts successful completions inside the measured window. Scheduled p99
counts measured successful offers through client drain and excludes failures;
it is not the all-attempt histogram. Successful request p99 is 166.59, 651.04
and 79.79 ms respectively. Candidate throughput is 6.25% lower and successful
scheduled p99 is 350.06% higher in this pair. Even restricting candidate
successes to in-window completions gives 347.97-ms p99, versus 281.77 before;
that restricted figure does not replace the qualification metric.

Independent replay reconciles all original successful outputs, payload bytes,
per-Cell counts, offers, attempts, trailing completions and complete ACK
provenance. All 2,000 Cells have measured successes in every arm. Candidate
counts are 27,102 in-window successes and 101 trailing successes. All 68,219
measured errors are `unavailable`, with zero `outcome_unknown`; warmup has
41,727 errors and six dropped offers. Before has 66,658 measured `unavailable`
and two `outcome_unknown`, plus 30,150 warmup errors and 6,857 warmup drops.
Celld has 1,730 warmup drops. No arm passes the unchanged performance gates.

| Arm | Complete ACK cohort | Warm audit | Cold audit / fleet drain |
| --- | ---: | --- | --- |
| Before | 54,038 | 38 HTTP 503s; 54,000 retries checked | Cold not reached; owner wait exceeds 120 s |
| Handoff | 47,471 | All mutations and original retries pass | All pass; 47.68 s |
| Fresh celld | 180,116 | All mutations and original retries pass | All pass; 22.74 s |

The candidate records no owner-fence warning. All 2,000 original Cells report
Idle after drain; owner and both followers exit successfully without OOM.
The passed audits verify the complete ACK cohort, including setup and warmup.
This is one successful recovery/drain observation, not qualification of every
failed-owner, transfer or collection schedule.

## Remaining bottleneck

The candidate's 27,175 completed native submissions have identical counts in
all seven phases, whose nanosecond totals reconcile exactly. Mean ordered-lock
wait is 0.00106 ms, publication-slot wait 0.00037 ms and total native submission
0.20103 ms. Fleet-proof wait is 53.58 ms in a separate 27,129-event cohort.
Follower window counts average 12.50 frames per sync on each member. Caller
waits overlap and these cohorts are not a serial TPS service-time partition.

Publication debt grows during the measured window: pending entries 4,335→4,382,
oldest age 45.05→55.04 seconds, retained captures 4.20→7.84 MB and unpublished
native-log bytes 19.38→37.01 MB. Retained runtime memory rises 48.63→63.54 MB;
active Cells stay at 2,000. The window records 102,880 immutable GETs and
73,784 node-authority range starts. Removing a global admission wait and avoiding
owner fencing has not made that publication work sustainable at the offered load.

The next architectural step remains a bounded authenticated append/lookup and
checkpoint representation that avoids rereading previous roots/history for every
selection while preserving exact original range, root and cold-recovery proofs.
It needs a deterministic work-count regression, unchanged corruption/recovery
checks and a fresh paired TPS/latency measurement. Increasing queue or memory
limits cannot establish that improvement. Larger follower shipping concurrency
also needs measured group-commit and latency evidence under original byte bounds.

## Verification and limits

The new real-follower regression holds publication after SQL returns a capture,
fills the entire remaining node RAM budget, then resumes the original request.
Before, it reproduces `OutcomeUnknown` with `Capacity("pending publication bytes")`.
After, three repetitions obtain a real Fleet ACK, resolve the original retry
without rerunning SQL, release all charges after joined shutdown and cold-restore
the exact published mutation/outcome. Three unit tests verify independent credit
lifetimes, oversized-cut fallback and invalid-result retention. All 41 durability
tests pass; the existing group test now checks original inputs plus actual
completed replies after unused allowances are released. Worker-cancellation
ownership assertions remain unchanged.

The first broad suite fails the existing eight-second stalled-read assertion
before that test issues a mutation. Three unchanged-before and three unchanged-
after isolated probes pass. They support a timing/load hypothesis without proving
its cause. No read code or deadline changes. The final controlled isolated rerun
passes all 13 contributor routes: 1,993 reported workspace test/doctest executions,
38 ignored environment tests, 60 local LTX tests, Rust 1.97/1.99 Clippy, targets,
rustdoc and boundary/layout/document/contract/script gates. All failed attempts
remain recorded; no qualification profile or deadline is relaxed.

The shared Docker VM has eight CPUs and 8,306,286,592 total memory bytes across
all roles; it does not qualify a dedicated 8-vCPU/16-GiB owner. tmpfs does not
qualify physical-media durability. Native peer protocols differ: Cellule uses
pinned mTLS and signed protobuf with a reused client per member; celld's fixture
uses native internal HTTP on loopback. This run cannot attribute the throughput
gap to that difference. No new Bucket, read-only, mixed or physical-media result
is claimed. Three paired repetitions of at least five minutes and the unchanged
zero-error/drop, latency, recovery, read and debt gates remain required.

Raw sources, builds, journals, replay, audits, telemetry and passing/failed
verification attempts stay outside Git in
`/Volumes/Workspace/crabbuild-target/native-availability-20261009`.
Only this concise report and implementation/regression tests enter the repository.
