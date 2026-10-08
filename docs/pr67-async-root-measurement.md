# Asynchronous roots: measured Fleet availability regression

**Measured candidate: 183.65 Fleet writes/s and 204.10 Bucket writes/s.**
The Fleet candidate returns **297,811 measured request errors**, versus zero
before the change, and fails its warm ACK audit. Its higher completion rate and
lower successful-write p99 do not establish an acceptable improvement.
**Every point fails qualification. PR #67 is a draft and is not ready to merge.**

## Tested implementation

Candidate: `6d62d4117712877f2c250d58f9b1cd60b529819b`.
Functional baseline: `9d4e6328bceb2f88087698809c3b4167f94959ef`; its subsequent
`ec127bad` commit changes documentation only. Celld:
`f2bf648663a610eefde71f3547ad61e9b896b1f0`, using the unchanged pinned image.
Both Cellule releases were built from committed source.

The managed actor now verifies and retires the complete oldest selected capture
prefix through its original publisher and native assignments. It keeps one
authenticated root-debt obligation per Cell, while the worker retains the latest
selected proof and keeps retry results in SQLite. SQL sequence assignment
includes that selected endpoint after physical captures leave the worker queue.
Grouped responses accept the same original Bundle proof as individual commands.

Root materialization runs asynchronously through the canonical root and catalog
checkpoint paths. It admits memory before origin reads, orders candidates by
oldest debt and Cell identity, permits at most eight jobs, and joins accepted
jobs during shutdown. The logical checkpoint target is 215 commands; physical
locator/byte pressure also gates new commands. Root age of 45 seconds, drain,
migration and ordinary publication fallback request materialization. Due hints
use the exact selected head while a root lags. No persisted format or signed
message changes.

Two-Cell tests hold root preparation and verify 65 sequential commands per Cell
or 215 grouped commands per Cell, reads, recorded retries, exact capture cleanup,
joined shutdown and cold SQLite contents. **The 215-command test is grouped**;
it does not establish 215 sequential physical captures under sustained load.
A longer sequential held-root test failed during development and remains in
external evidence. Active-write checkpoint continuation and application receipt
visibility under pressure still need regressions and successful integration runs.

## Matched Docker workload

All six cases ran sequentially with fresh prefixes and fresh Linux provider
volumes. Comparison verifies identical client/auditor binaries, fixtures, pinned
images, runner and Docker resource contract. Each uses 1,000 uniform Cells,
96-byte values, INSERT plus SELECT in the command, a two-hour durable result
ledger, 128 clients and 128 queue slots. Warmup is 30 seconds; each measured
window is 60 seconds with one repetition. Fleet offers 15,000 writes/s with two
followers. Bucket offers 2,000/s without followers.

The ARM64 VM has **8 CPUs and 8 GiB total shared memory**. Serving containers
have an 8-CPU/16-GiB ceiling and 4-GiB tmpfs, the client 4 CPUs/4 GiB, and RustFS
2 CPUs/8 GiB using the unchanged external provider adaptation. Ceilings exceed
VM resources. Cellule keeps the same 64-MiB retention ledger and 1-GiB managed
disk budget. This is an overloaded SQL application diagnostic, not the bounded
KV laptop workload or dedicated 8-vCPU/16-GiB node qualification. Acceptance
gates were preserved; a short diagnostic cannot pass the five-minute gate.

## Reconciled results

TPS counts successful logical writes completed inside the measured window.
Successful p99 uses independent nearest-rank journal replay, including trailing
successes. Scheduled latency starts at offered arrival; request latency starts
at issuance. These successful percentiles exclude errors and drops, which stay
explicit. The qualification reporter also checks all-attempt latency.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Measured errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / baseline | 115.27 | 4,538.19 | 2,560.72 | 0 | 892,828 |
| Fleet / candidate | 183.65 | 1,782.74 | 1,159.62 | 297,811 | 591,170 |
| Fleet / celld | 2,878.73 | 544.60 | 297.91 | 0 | 727,020 |
| Bucket / baseline | 196.27 | 6,566.99 | 4,797.93 | 0 | 107,968 |
| Bucket / candidate | 204.10 | 5,492.60 | 4,666.86 | 0 | 107,498 |
| Bucket / celld | 1,319.80 | 779.30 | 437.71 | 0 | 40,560 |

The candidate also returns **27 warmup errors**; the other five cases return
zero warmup errors. All cases drop warmup offers. Replay reconciles every
generated measured offer to an attempt or drop, every attempt to success or
error, window/trailing completions, and each complete ACK cohort and digest.
**These are completion rates under overload, not sustainable capacities.**

Fleet completes 59.3% more writes in this pair, but the errors and failed ACK
availability make the change a regression. Its successful latency statistics
describe a different surviving cohort and cannot establish overall improvement.
The Bucket fixture bypasses the managed producer, so its 4.0% paired difference
cannot be attributed to this implementation. The unchanged baseline previously
completed 95.35 Fleet/s and 274.53 Bucket/s in the
[cohort-origin measurement](pr67-cohort-origin-measurement.md); observed variance
remains outside the required A/A agreement. No repeatable gain or parity is proven.

## Availability, drain and recovery

| Mode / system | Complete ACK cohort | Warm read/retry | Cold read/retry | Successful drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / baseline | 12,289 | pass | pass | 22.26 |
| Fleet / candidate | 20,689 | fail: 16,340 errors | not reached | no successful record |
| Fleet / celld | 335,996 | fail: 335,996 errors | not reached | no successful record |
| Bucket / baseline | 21,491 | pass | pass | 14.06 |
| Bucket / candidate | 22,766 | pass | pass | 10.58 |
| Bucket / celld | 114,418 | pass | pass | 0.88 |

Passing cases read and retry every seed, contract, warmup, measured and trailing
ACK, and pass the cold contract retry. Cold restore starts with empty local
state after joined graceful drain; it does not qualify owner loss before
materialization or recovery of the complete owner-lost Fleet suffix.

Candidate Fleet errors include HTTP 503 temporary unavailability. Its warm
audit checks all 20,689 records, successfully retries 4,349, and reports 16,340
errors. The runner aborts before cold recovery; absent successful joined-drain
evidence remains failed. This does not establish acknowledged-data loss, but
acknowledged-data recovery is unverified for this candidate.

Celld Fleet's owner exits with **code 3, OOMKilled false**. Its retained log
records ambiguous lease renewals followed by the node lease watchdog self-fence.
All warm queries fail and cold recovery is not reached. This differs from the
earlier OOM run; neither failure is omitted or converted into a passing result.
Provider startup byte/inode gates pass. The aborted Fleet cases lack later
filesystem/cold lifecycle observations, so their complete provider evidence also
fails. Missing observations do not prove the provider itself exited.

## Publication work and remaining bottleneck

| Fleet window metric | Baseline | Candidate |
| --- | ---: | ---: |
| Successful provider PUTs per completed write | 3.669 | 0.482 |
| Total GET/range attempts per completed write | 21.239 | 13.304 |
| Materialized commands per selected root | 1.120 | 10.247 |
| Steady Bundle ACKs | 0 | 0 |
| Mean capture ms | 0.239 | 0.190 |
| Mean follower-proof ms | 5.487 | 5.811 |
| End retained MiB / 64-MiB ledger | 22.41 | 58.77 |
| End oldest publication debt ms | 3,633 | 49,054 |
| End active Cells | 1,000 | 995 |

The candidate reduces root/publication work in this window, but still misses
the conditional **215 commands/checkpoint** and **0.05 PUTs/command** targets.
Native-authority range attempts grow from 20,729 to 104,200 in absolute terms;
total GET/range work falls per completed write because immutable reads fall and
the denominator changes. Steady responses still use Fleet proofs; some Bundle
responses occur outside this window. Phase samples describe different cohorts
and cannot be summed into a request's critical path.

Candidate retained bytes grow **27.93 → 58.77 MiB**, while accounted unpublished
native-log bytes grow **10.58 → 22.70 MiB**. Oldest root/publication debt grows
**43,786 → 49,054 ms**. Selected physical capture bytes fall, but total retained
pressure remains. New publication counts include coalesced root obligations,
so their counts are not directly comparable with the former per-capture counts.
Two boundary samples do not prove stable bounded debt or isolate the cause of
HTTP 503 failures.

The next fixes must prove materializer admission/progress under the same ledger,
exact checkpoint continuation while writes remain active, and ordinary
application minimum-receipt read/retry availability while roots lag. Preserve
the failed snapshot and rerun matched measurements after each demonstrated
correction. Bucket producer integration, complete failed-owner issued-suffix
recovery including prior Fleet ACKs, safe cross-Cell collection, physical-device
durability and full read/write/mixed qualification remain open.

## Verification and retained evidence

The exact frozen functional source passes all eleven contributor routes on
Rust 1.97: format, features/targets, workspace tests (**1,958 passed, zero failed,
38 documented ignored**), local LTX, Clippy with warnings denied, API docs,
boundaries, layout, document fences/links and SQL/peer contracts. Linux releases
use the unchanged pinned Rust 1.98.1 image. These checks do not qualify performance.

Raw material stays outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`.

| Evidence | SHA-256 |
| --- | --- |
| Frozen verification source manifest | `17ae0d679452b1ec00a7ba3589af22502928514c593e87f7b5dc533c0f251ee5` |
| Candidate release source manifest | `8879b6c26aad10432eb0781bbbdb0642e2de6662f50f6355cfd09e7666a85644` |
| Candidate SQL binary | `077503622744d5bd3f6cb5fc5de45bb74806ba4e35ef2a72546d140bda01b176` |
| Identical client | `417f07b0424d27df75b1dca22666a7adb621db3cd11fdb1048eb9247a664e60b` |
| Identical auditor | `6595c24b0be217e181e8a9aa7de5cf478669ac341b37b76754fdcf0b3865b7b2` |
| Evidence index: 5,762 files | `006d03a60f9505685a77e8de4863c89386b859f226f3c09945009af85af0f8fb` |

`async-root-20261008-evidence-index.json` covers 1,219,891,080 bytes, including
source, immutable builds, all six cases, failed development logs and final
verification. Every indexed file was rehashed without mismatch. Independent
replay, telemetry and paired comparisons are
`async-root-20261008-{reconciled,telemetry,fleet-comparison,bucket-comparison}.json`.
Both comparisons verify fixture/host/runner provenance and report
`qualification_pass: false`. Provider volumes remain retained through recorded
metadata. No measurement process or serving container remains running.
