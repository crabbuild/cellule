# Checkpoint continuity: measured write results

The later [release repeat](pr67-release-repeat-measurement.md) of the same
binary completes 107.95 Fleet and 268.12 Bucket writes/s. It retains failed
cases and isolates a shared-selection deadline failure path.

**This comparison's completion rates: 185.83 Fleet writes/s and 215.08 Bucket
writes/s. No acceptable performance improvement or celld parity is established.**
Fleet still returns 304,151 measured errors and fails its warm ACK audit. Bucket
has zero request errors and passing ACK audits, but completes 5.3% fewer writes
than the immediate baseline and has worse successful-write p99. Every point
fails qualification. PR #67 remains a draft.

## Tested change and provenance

Candidate: `4a8f55cc99414e6a3907531c774a184842e1de3e`. Immediate functional
baseline: `6d62d4117712877f2c250d58f9b1cd60b529819b`, the candidate in the
[previous asynchronous-root measurement](pr67-async-root-measurement.md).
This comparison is against that release, not a fresh origin/main build.
Celld is unchanged at `f2bf648663a610eefde71f3547ad61e9b896b1f0` and its pinned
image. Both Cellule binaries come from committed source.

The follow-up fixes a reproduced selected-prefix continuity failure across
confirmed asynchronous checkpoints. It preserves receipts selected against an
intermediate base while an older root prepares. Process-local hash-chain
witnesses admit only removal of the identical materialized prefix and require
the complete remaining suffix. They retain no frame bodies, expire beyond the
256-locator bound, and transfer heap admission acquired before root I/O to the
worker. They grant no new ACK, authority or origin-availability proof.

Before the fix, the real actor checkpoint case failed twice; a successive-base
case also failed. Both now pass, including live writes, read/retry visibility,
joined shutdown and cold SQLite checks. All 81 bundle tests pass. On the final
frozen snapshot, all 11 contributor verification routes pass with **1,961 tests
passed, zero failed and 38 ignored**. An earlier broad run failed an unchanged
peer-HTTP concurrency test; its isolated rerun and the final full suite pass.
These results do not remove the measured application availability failure below.

The Linux build's 1,905-file source manifest matches the frozen verification
snapshot exactly. The load generator and auditor are byte-identical across
arms; comparison verifies fixture, runner, image and resource provenance.

| Artifact | SHA-256 |
| --- | --- |
| Frozen/framework source manifest | `4a48fac79be76d9aa037090a3a7b64a16ff81ac1412d07ae83f548df920f52a4` |
| Adapted Linux build source | `0503aa0800940982a15a79fdc96dc68113d85f14f480f2c2376fe2ccd1b24430` |
| Candidate SQL binary | `4c7874206d84885064595e04a8759075ea45ce44bc90b4eb02bfae97fa945c13` |
| Common load generator | `417f07b0424d27df75b1dca22666a7adb621db3cd11fdb1048eb9247a664e60b` |
| Common auditor | `6595c24b0be217e181e8a9aa7de5cf478669ac341b37b76754fdcf0b3865b7b2` |

## Workload and limits

Six fresh cases ran sequentially: before/after/celld in Fleet, then Bucket.
Each uses 1,000 uniform Cells, 96-byte values, INSERT plus SELECT per command,
a two-hour durable retry/result ledger, 128 clients and 128 queue slots.
Warmup is 30 seconds and the measured window 60 seconds, with one repetition.
Fleet offers 15,000 writes/s with two followers; Bucket offers 2,000/s.

The ARM64 Linux VM has **8 CPUs and 8 GiB total shared memory**. Serving
containers retain an 8-CPU/16-GiB ceiling and 4-GiB tmpfs; the client has a
4-CPU/4-GiB ceiling and RustFS 2 CPUs/8 GiB. Ceilings exceed VM resources.
The unchanged external provider adaptation, fresh Linux volumes, 64-MiB Cellule
retention budget, 1-GiB managed disk budget and original acceptance gates remain.
No builds or contributor checks overlap the timed windows.

This is a constrained, overloaded SQL application diagnostic. It is not the
bounded KV laptop workload, a dedicated 8-vCPU/16-GiB serving node, a 2,000-Cell
qualification, or the required three paired five-minute repetitions. tmpfs
does not qualify physical-device durability.

## Reconciled measurements

TPS counts successful logical writes completed inside the measured window.
Successful p99 is independently replayed from journals, includes trailing
successes, and excludes errors and drops. Scheduled latency starts at offered
arrival; request latency starts at issuance. The qualification reporter also
checks all-attempt latency. Its percentile is not interchangeable with the
successful-write percentile below.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Measured errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / baseline | 164.73 | 2,636.38 | 1,718.73 | 312,664 | 577,381 |
| Fleet / candidate | 185.83 | 2,002.08 | 1,646.30 | 304,151 | 584,699 |
| Fleet / celld | 4,041.48 | 255.24 | 122.96 | 1,948 | 655,524 |
| Bucket / baseline | 227.15 | 5,289.89 | 3,522.02 | 0 | 106,115 |
| Bucket / candidate | 215.08 | 6,146.30 | 5,376.70 | 0 | 106,839 |
| Bucket / celld | 391.45 | 3,768.85 | 2,120.32 | 210 | 96,257 |

All six cases have zero warmup request errors, but drop warmup offers.
Independent replay reconciles all 900,000 Fleet or 120,000 Bucket offers to
attempts or drops, every attempt to success/error, window/trailing completions,
and complete ACK cohort counts and digests. **These rates are overloaded
completion counts, not sustainable capacities.**

Fleet completes 12.8% more writes in this pair, but its errors and failed
availability audit prevent an acceptable-gain claim. Lower survivor p99 does
not establish overall improvement. Bucket completes 5.3% fewer writes with
16.2% higher successful scheduled p99. Its fixture bypasses the managed bundle
producer, so these single-pair differences cannot be attributed to the fix.
Celld's owner failures below also prevent a qualified reference-rate claim.
No repeatable improvement, read-capacity result or parity is established.

## Availability, recovery and lifecycle

| Mode / system | Complete ACK cohort | Warm audit errors / retry checks | Cold read/retry | Recorded successful drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / baseline | 20,027 | 14,689 / 5,338 | not reached | absent |
| Fleet / candidate | 21,678 | 14,681 / 6,997 | not reached | absent |
| Fleet / celld | 464,260 | 463,665 / 595 | not reached | absent |
| Bucket / baseline | 25,134 | 0 / 25,134 | pass: all 25,134 | 5.98 |
| Bucket / candidate | 23,835 | 0 / 23,835 | pass: all 23,835 | 8.51 |
| Bucket / celld | 44,814 | 44,814 / 0 | not reached | absent |

Both Cellule Fleet audits report HTTP 503 failures. The candidate owner exits
zero during cleanup, but the failed case has no successful aggregate drain
record or cold audit; its exit alone cannot supply them. Celld Fleet's original
owner is **OOMKilled=true, exit 137**. Celld Bucket's owner is **OOMKilled=false,
exit 3**; its log records ambiguous renewals and node-lease watchdog self-fencing.
No case is omitted or silently retried. These failures establish unavailable
audits, not proven mutation loss.

Failed cases lack required later provider filesystem/lifecycle observations.
Absence of those records does not prove provider failure and cannot pass the
provider health gate. Passing Bucket cold checks follow graceful joined drain;
they do not qualify recovery of a failed owner's entire Fleet-ACK suffix.

## Remaining measured bottleneck

The canonical Fleet window-cost report shows PUTs per completed command moving
from **0.5083 to 0.4697**, GET/range attempts from **13.0395 to 13.8594**, and
materialized commands per root from **11.30 to 11.70**. Steady Bundle response
counts remain zero. These are storage API observations; SDK-internal retry
attempts are not separately measured.

Candidate retained memory grows from **28.41 to 59.52 MiB of 64 MiB**, accounted
unpublished node-log bytes grow, and oldest publication reaches **47,453 ms**.
Two boundary samples do not prove bounded debt. The checkpoint density target
is 215 commands and the conditional PUT target 0.05 per command; neither is met.
The subsequent [diagnostic and release repeat](pr67-release-repeat-measurement.md)
records selection deadlines that fence Cells and sampled pre-SQL publication
backlog refusals. Fixing the focused continuity cases has not fixed the
original load-test failure.

Next work must isolate application refusal/read visibility and resource
admission under this reproduced load, establish admitted materializer progress,
connect Bucket to shared selection, and reduce complete verification/publication
cost. Successful all-ACK availability/drain, sustainable A/A capacity, read-only
and mixed guardrails, complete failed-owner recovery, safe collection and the
unchanged qualification gates remain open.

Raw journals, failures, manifests, binaries, comparisons and the immutable
evidence index remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/`, with prefix
`checkpoint-transition-20261008`. Only this compact report belongs in the repo.
The version-2 index covers **17,146 files / 1,687,273,628 bytes**; every entry
was rehashed and matched. Its SHA-256 is
`9566c2896270f252d5636a0bd975ffe96e625e8853632bf0f90654eec2550934`.
