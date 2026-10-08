# Coverage race fix: measured recovery improvement, no throughput gain

**Latest measured code: 100.20 Fleet writes/s and 271.63 Bucket writes/s.**
Compared with the immediately preceding code, Fleet completed 5.5% fewer writes
and Bucket 5.3% fewer in these single overloaded pairs. Successful scheduled p99
increased 9.3% and 14.2%, respectively. There is no demonstrated throughput or
latency improvement. The coverage fix restored Fleet warm availability and
joined drain in this run; all acknowledged commands then passed cold read/retry
verification. **Parity is not achieved. PR #67 remains a draft.**

## Changes and immutable identities

Measured candidate: `e40ecd6b6b2972ee91a52846737cbc12bd48f406`.
Before-fix code: `2dc19172ce71d1f7b051705d70f005a70a47e554`.
Celld: `f2bf648663a610eefde71f3547ad61e9b896b1f0`, the same pinned image as the
[previous producer comparison](pr67-managed-producer-measurement.md).
The baseline reuses the exact previously verified release binary, with a fresh
case, prefix and provider volume. Its immutable build manifest records export
HEAD `7b986b0`; a complete Git-tree comparison established that the exported
contents equal committed `2dc1917`. That manifest was not rewritten.
The new candidate was exported and built from committed `e40ecd6`; its frozen
verification snapshot and release source manifest match that commit.

| Fix | Verification |
| --- | --- |
| `5193c7c`: join root coverage overtaken by native bundle selection | The real original-authority regression failed before the fix with `node log object coverage regressed`, then passed. An older root acknowledges an already persisted prefix with zero PUTs; the higher frontier stays unchanged. Local root confirmation covers only its own tickets. Open-epoch, expiry, lease and complete-assignment checks remain mandatory. |
| `e40ecd6`: box the full-compaction future | The existing 65-command backlog test overflowed the default worker stack on Rust 1.97, including an exact pre-fix baseline reproduction. The same test passes after boxing the joined compaction transition, without increasing the stack or changing publication/compaction ordering. |

These fixes add no persisted or wire format change. A separate warning-only
diagnostic captured 30 coverage-regression warnings; its TPS is excluded because
it used a debug subscriber and briefly overlapped an interrupted native build.
Compiler type-size probes are diagnostic evidence only. Neither diagnostic is
included in the six windows below.

## Matched workload and limits

All six cases ran sequentially with the same client/auditor, fixture hashes,
loaded runner, pinned images and Docker host/resource contract. Independent
comparison verified those identities. Each used 1,000 uniform Cells, 96-byte SQL
INSERT plus SELECT, a two-hour request/result ledger, 128 clients and 128 queue
slots, 30-second warmup and one 60-second measured window. Fleet offered 15,000
writes/s with two followers; Bucket offered 2,000/s. This is the SQL application
profile, not bounded KV overwrite. Standalone read and mixed capacity were not
measured.

The ARM64 Docker VM has **8 CPUs and 8 GiB total shared RAM**. Each serving node
has an 8-CPU/16-GiB container ceiling and 4-GiB tmpfs; the client ceiling is
4 CPUs/4 GiB. RustFS has 2 CPUs and an 8-GiB memory/swap ceiling, using the same
external diagnostic adaptation for all arms. The ceilings exceed actual shared
VM resources, and another workstation VM was present. Every case used a fresh
Linux provider volume. This does not qualify the dedicated 8-CPU/16-GiB node,
2,000-Cell population, physical-media durability or repeatability requirements.
No delivery, durability, recovery or qualification gate was weakened.

## Reconciled windows

TPS counts successful logical commands completed inside the measured window.
Successful p99 is nearest-rank replay of the request journals, including trailing
successes. Scheduled latency starts at offered arrival; request latency starts
at actual request issuance. Errors and drops are excluded from successful
percentiles and remain explicit failures. The unchanged delivery gate uses
all-attempt scheduled latency as well as delivery and recovery evidence.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / before fix | 106.05 | 3,507.00 | 2,379.51 | 76 | 893,306 |
| Fleet / latest | 100.20 | 3,832.17 | 2,298.88 | 0 | 893,732 |
| Fleet / celld | 4,202.05 | 179.78 | 41.64 | 13,715 | 634,002 |
| Bucket / before fix | 286.93 | 3,561.76 | 3,087.89 | 0 | 102,528 |
| Bucket / latest | 271.63 | 4,065.98 | 3,599.15 | 0 | 103,446 |
| Bucket / celld | 1,472.08 | 672.38 | 342.38 | 0 | 31,425 |

Streaming replay reconciled every measured offer to a success, error or drop,
including trailing completions, and reconciled ACK cohorts and their hashes.
**Every point fails delivery qualification.** These overloaded completion rates
are not sustainable capacities. One pair cannot establish an attributable
performance regression or improvement. The previous producer regression remains
historical evidence; these results do not reverse it or establish parity.

## Recovery and availability

| Mode / system | ACK cohort | Warm read/retry | Bucket-only cold read/retry | Drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / before fix | 11,855 | fail: 184 HTTP 503 errors | not reached | failed: owner wait exceeded 120 seconds |
| Fleet / latest | 11,197 | pass | pass | 25.40 |
| Fleet / celld | 468,386 | fail: 378,252 HTTP 500 errors | not reached | failed cleanup |
| Bucket / before fix | 26,858 | pass | pass | 5.51 |
| Bucket / latest | 28,009 | pass | pass | 6.29 |
| Bucket / celld | 134,965 | pass | pass | 3.58 |

Cohorts include setup, warmup, steady and trailing successes. Passing audits
GET and retry every original ACK, checking stored outcomes and incarnation.
Both latest Cellule cases also passed the cold contract retry. Cold cases start
from empty local state after joined graceful drain; these are not failed-owner
recovery or owner-loss-before-materialization tests.

Celld Fleet's owner has `OOMKilled: true`, exit 137, in retained Docker state.
Its high completed-write rate is not qualified durable capacity. Warm audit and
cleanup failures establish availability/recovery-evidence failures, not proven
data loss. Missing post-failure provider/cold observations remain failed evidence
in the reports rather than being filled with assumed values.

## Bottleneck still measured

Latest Fleet costs **3.65 successful PUTs and 24.03 GET/range attempts per
completed write**, versus 3.65 and 24.20 before the fix. Materialization density
is **1.12 commands/root**, versus 1.10 before. Mean capture is 0.29 ms, worker
round trip 3.24 ms and Fleet proof 6.00 ms; mean publication is 5,479.53 ms and
Fleet response 1,216.75 ms. These phase samples have different cohorts and cannot
be summed as a per-request critical path.

The bundle response counter remains two across the steady window: it adds zero
steady Bundle ACKs. The original native epoch stays Fleet-active, but pending
native object sequences grow 309 → 486. Retained runtime bytes stay around
19 MiB under the 64-MiB ledger, including the producer's 16-MiB reservation.
These two boundaries do not establish stable debt over five minutes.

Code inspection shows the publication feed applies backpressure before native
sequence issuance, and the same producer alternates selection and root
checkpoint work. Together with the measured I/O and sparse root density, this
identifies publication/verification work as the next optimization target. The
data does not support attributing the throughput gap to SQLite sync or capture
cost. Dense admitted materialization, reduced repeated verification, Bucket
producer integration, failed-owner complete-suffix recovery, collection and
full read/write/mixed qualification remain open. The original 215-command
checkpoint model and 2,000-Cell/10K-write/50K-read gates remain unchanged.

## Verification and evidence

The exact frozen candidate passed all eleven contributor routes on Rust 1.97:
format, workspace targets/features, workspace tests (**1,955 passed, zero failed,
38 documented ignored**), local LTX, Clippy with `-D warnings`, API docs,
boundaries, module layout, Rust fences, links and SQL/peer contracts. Linux
release builds used the same pinned Rust 1.98.1 image. The earlier stack failure
and the previously recorded Rust 1.99 Clippy deprecation remain retained; no
warning or stack setting was suppressed to pass.

Raw journals, binaries, logs, source manifests and provider volume records stay
outside Git under `/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`.

| Evidence | Identity |
| --- | --- |
| Latest source manifest | `83114274a5fe95e14ac9eedced32553c71ba38429578b58152080740a849d416` |
| Latest SQL binary | `0bd881641c72cbee662f22d1a0751823ff1a3c54c3115d73bebb76e8c5d00f22` |
| Baseline source manifest | `8c13c9ec32ba2dad6ca0857a6165ad661fdac49af3e4e2b9ccfd38a8f41498f1` |
| Baseline SQL binary | `51d12763a1d81886d171b4c1592a86ec296f1eb50e57504d54206f8f933697b8` |
| Identical client | `417f07b0424d27df75b1dca22666a7adb621db3cd11fdb1048eb9247a664e60b` |
| Identical auditor | `6595c24b0be217e181e8a9aa7de5cf478669ac341b37b76754fdcf0b3865b7b2` |
| Evidence index: 1,923 files | `fd5100792b8853d84211949e0863e518bb36b4eec5fddd3d8c5f00b277b54ab2` |

The index is `coverage-race-stack-20261008-evidence-index.json`. Independent
replay is `coverage-race-stack-20261008-reconciled.json`; paired matrices and
comparisons are `coverage-race-stack-20261008-{fleet,bucket}-{matrix,comparison}.json`.
They preserve failed cases and explicitly report `qualification_pass: false`.
