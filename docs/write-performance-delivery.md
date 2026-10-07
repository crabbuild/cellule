# Write performance implementation and verification

This delivers packed dependencies, shared publication, signed append grants and
a quantified gap report, not the completed M0–M5 plan. A pinned Docker
comparison retains exact retry and cold-state audits. **Celld write parity
has not been established.** The [proposal](write-performance-proposal.md) remains
the acceptance contract; completing tests or a load run does not pass its gates.

## Delivered behavior

| Change | Measurable result | Preserved contract |
| --- | --- | --- |
| Small native LTX and index share one `.pack` | One dependency PUT instead of two; at most 256 KiB | Exact native bytes, body/index digests, complete object digest |
| Small directory leaf lives in the root | Removes its separate PUT, GET and cached-origin HEAD | Canonical leaf validation; 2 KiB leaf and 32 KiB root bounds |
| Packed compaction input supplies both scratch streams | One full GET per selected pack instead of full plus range GET | Complete verification; bounded transfer and file ownership through cancellation |
| Window telemetry separates response, proof and publication | Logical commands per selected root; capture/checkpoint, worker, peer and sync histograms | Counters and frontiers confer no authority or proof |
| Storage families distinguish owner/receiver enrollment | Summed enrollment GET cost across all three nodes | Fresh authorization issues signed windows; every append checks its local window and fence |
| Application-scoped shared capture objects | One payload PUT for up to 64 scoped rows and 256 KiB | Per-Cell root selection, exact recovery and complete reference collection remain mandatory |
| Signed follower append windows | Up to 512 sequences/five seconds per fresh issuance | Pinned mTLS, signed RPCs, fsync, local monotonic expiry and durable closure |
| Docker runner and reports preserve failures | Source/binary identities, fresh provider volumes, scheduled-arrival latency, all-ACK audits | Errors, drops, unissued offers, provider failures and failed drain cannot pass |
| Arrival producer preserves delayed offers | Final wakeup cannot erase a request scheduled inside the window | Original arrival time still determines latency and completion; full queues count drops |
| ACK collection and audit stream bounded records | Disk-backed uniqueness index; 256 queued records and at most 128 GET/retry pairs | Seed, warmup, steady, trailing, overload and recovery successes all reconcile |

An ordinary small root needs two immutable PUTs plus lineage and fenced Cell
selection: **four successful PUTs instead of six**. A small scheduled
compaction composed with an append still needs two packs, the final root,
lineage and selection: **five PUTs**. Node coverage and maintenance remain in
the window numerator. M1's universal four-PUT gate has therefore not passed.

The current root development format is version 3. The
[format specification](../crates/cellule-ltx/docs/packed-root-format.md) covers
all readers, producers, sparse-read locators, recovery inventories, backup and
collection paths. There is no legacy decoding or automatic migration.

## Milestone status

| Milestone | Implementation | Exit gate |
| --- | --- | --- |
| M0 | Measurement and comparison harness delivered | Three A/A capacity pairs unverified; storage API totals reconcile, but SDK-internal HTTP retries need provider telemetry |
| M1 | Packs, inline leaves and bounded compaction spooling delivered | Ordinary append meets four PUTs; composed compaction needs five. Paired cold/sparse-read guardrail unverified |
| M2 | Bounded file-backed shared publication coordinator implemented; exact scope, restore, cancellation, minimum-budget and dormant-sibling retention checks added | Corrected active-Fleet window costs 5.484 PUTs/command and fails the debt trend. Three per-Cell authority PUTs remain; M4 is required |
| M3 | Signed 512-sequence/five-second grants, bounded local registry, lifecycle gate and signed HTTP fixture implemented | Full isolated checks and native lifecycle suite pass; one active-Fleet window costs 0.0138 enrollment GETs/command. Three-repetition gate pending |
| M4 | Binding/selector and delayed-materialization models plus [authority decision](bundle-coverage-proof.md) delivered | Production bundle proof, atomic transfer/recovery/collection and bundle ACKs not implemented |
| M5 | Matched Fleet/read points and target stress with exact ACK audits delivered | Three repetitions, read/failure/overload matrix and absolute/relative parity unverified |

## Shared publication checkpoint

Shared publication is implemented in the runtime and LTX layer, with the
existing fenced per-Cell response gate. It uses one 64-entry lane, a 1-ms assembly
bound with immediate idle flush and a 256-KiB shared object bound. Fixed cohort counters and cumulative
queue/upload histograms permit windowed comparison. The new format requires a
fresh isolated prefix and coordinated deployment of all producers and consumers.

The prior evidence below measures the packed implementation, **not this shared
coordinator**. Its improvement percentages must not be attributed to M2. New
source identities, checks and performance results will be recorded separately.
Signed append grants are implemented with a fresh issuance path and local
durable closure gates. Bundle coverage ACKs remain disabled: the proposed
Cell-binding catalog, atomic transfer, exact range recovery and collection
contracts still need production integration.

The first isolated M2/M3 snapshot passed 1,857 workspace tests (38 documented tests
ignored), 58 local LTX tests without replica features, all-target/all-feature
checking, Rust 1.99 Clippy with warnings denied, API documentation and all
boundary/layout/document/SQL-peer/Python gates. The write-proof model checked
13,356 distinct states and all three required unsafe counterexamples. These
checks validate the implementation contracts; they do not qualify throughput,
physical-device durability or bundle-based bucket acknowledgments.

The corrected production snapshot at `d351d886` passed 1,861 workspace tests
(38 ignored), 58 local LTX tests and all contributor checks. A separate complete
serial native lifecycle suite passed 382 tests, including expiry, pruning,
receiver refusal and shutdown. Framework Rust/Cargo source hashes match the
pinned release build; later reporter/model/document changes do not alter that
production binary. Earlier failing snapshots and CI routing results remain
retained outside Git.

## Measured-path corrections

The first M2/M3 Docker run exposed two implementation faults before any gain
could be qualified. At 1,000 resident Cells, ordinary handles consumed the
entire descriptor ledger, so all 29,999 measured shared submissions fell back.
Publication now has dedicated bounded descriptor credit. Cell and reader
admission cannot borrow it; aggregate usage includes both classes, and drain
must return all credit.

Fresh enrollment could also observe object coverage ahead of the first queued
frame. Signing that higher pruning floor let the receiver skip the frame the
shipper required as an exact witness. Grants now sign the lesser of fresh
coverage and `first_sequence - 1`; verification rejects a floor inside the
authorized window. The retained run had no follower proof advancement and
inactive Fleet shipping, so its zero enrollment GETs are a failure diagnostic.
Reports now require healthy Fleet frontiers throughout the window and actual
follower-proof advancement, separately from publication debt stability.

CI's four paired routing repetitions found 14–23% lower command throughput in
the first implementation; steady query rates stayed within approximately 4% of
baseline. Shared upload created an additional synced temporary file. Its writer
now closes before the exact length/digest-verified object upload, without a
local durability barrier for that disposable source. Native captures and
follower logs retain their barriers. The contribution of this change requires
a fresh routing comparison; the earlier failure remains evidence.

Fresh routing CI at `120e4aa6` still found 11.5–19.8% lower leased command
throughput across four paired runs, despite removing the temporary-file fsync.
The next candidate therefore delegates a one-input/one-row cohort's verified
captures to the canonical native-pack root factory, without constructing,
reading back or cleaning up a shared upload file. Multi-row cohorts still share
one upload. Separate singleton counters preserve actual shared-object cost and
upload timing. The new path's regression test checks exact native/coalesced
roots, byte-identical restore and absence of shared objects; its fresh
verification and measurements remain pending. The `d351d886` results below
must not be attributed to this subsequent change.

## Corrected shared/grant measurement

Candidate `d351d886` and main `831877cf` use separate source-content-isolated
release builds, with no measurement overlay. Celld is pinned to v0.6.1
`f2bf6486`. Clients, ACK auditors and provider images are byte-identical across
the arms. The current profile is the SQL ledger workload below on one shared
8-CPU/16-GiB VM, with tmpfs node state and fresh RustFS volumes.

The first corrected candidate window offered 100 writes/s for 300 seconds after
30 seconds warmup. All original node containers were removed after drain and
before the bucket-only cold audit. Paired repeats are in progress; the following
is one diagnostic window, not a sustainable-capacity or parity claim.

| Candidate window/audit | Measured value |
| --- | ---: |
| Successful commands/s inside window | 100.000 |
| Scheduled p50 / p95 / p99, ms | 5.5 / 7.9 / 15.6 |
| Errors / dropped / unissued offers | 0 / 0 / 0 |
| Warm and cold GET plus exact retry checks | 34,001 each; zero failures |
| Original fleet drain | 5.305 s |
| All provider PUT successes/command | 5.4842 |
| All GET attempts/command, including ranges | 2.4048 |
| Fresh enrollment GETs/command, owner + receivers | 0.0138 |
| Shared cohorts / Cell submissions | 29,931 / 30,000 |
| Shared pressure / large fallbacks | 0 / 0 |
| Logical commands/selected Cell root | 1.0000 |
| Final debt / oldest-age slope | +98.53 bytes/s / +0.152 ms/s |

Fleet stayed active, unfenced and non-rotating throughout the sampled window;
its follower proof frontier advanced. Delivery, latency and all-ACK audits pass
at this point. The strict publication stability gate fails, and PUT cost is
well above M2's 0.25 budget. Near-singleton cohorts show why shared payloads
alone cannot amortize sparse per-Cell authority work. The
[bundle decision](bundle-coverage-proof.md) describes the remaining atomic
authority/recovery changes and cost accounting.

Reports require Fleet activity and proof advancement independently of debt.
An inactive fallback run cannot qualify Fleet even if it has zero enrollment
GETs or no remaining follower debt. Window cost subtracts cumulative counters
and raw histogram buckets; decreasing summary percentiles/means are not
monotonic counters. Reporter source hashes accompany reevaluated evidence.

## Historical packed-implementation evidence

The fixture is **SQL application parity**, not the user's bounded KV workload:
1,000 Cells, 96-byte values, INSERT plus in-command SELECT, and a two-hour
durable request/result ledger in both applications. Nodes have 8-CPU/16-GiB
ceilings and tmpfs state, RustFS has 2 CPUs/2 GiB, and the client has 4 CPUs/4 GiB
on one 8-CPU/16-GiB Linux VM. Summed CPU ceilings exceed VM capacity. This
profile qualifies neither device persistence nor independent-node isolation.

The earlier baseline is `18eff0f7af47fac09b993157bb444e582072d7cf`, which merged PR 65.
Its production source matches the audited `397f500a` foundation; three additional
main files are design documents. The baseline explicitly overlays measurement
hooks. Celld is v0.6.1, `f2bf648663a610eefde71f3547ad61e9b896b1f0`, using the
pinned container digest. Both framework arms use byte-identical clients/auditors.

Each arm offered 100 Fleet writes/s for 300 seconds after 30 seconds warmup,
serially with a fresh provider volume. These are individual points, not a
capacity search or three-repetition qualification.

| Window or audit | Latest main + telemetry | Packed candidate | celld |
| --- | ---: | ---: | ---: |
| Successful commands/s inside window | 99.993 | 99.990 | 100.000 |
| Scheduled p50 / p95 / p99, ms | 66.8 / 265.3 / 868.1 | 24.0 / 200.1 / 386.5 | 5.6 / 11.8 / 44.3 |
| Errors / dropped offers | 0 / 0 | 0 / 0 | 0 / 0 |
| Commands checked by GET and exact retry, warm and cold | 34,001 each | 34,001 each | 34,001 each |
| Original fleet drain, seconds | 7.737 | 10.918 | 11.742 |
| All successful storage API PUTs/command | 6.688 | 4.992 | Not instrumented |
| All GET attempts/command, including ranges | 3.327 | 4.059 | Not instrumented |
| Fresh enrollment GETs/command, owner + receivers | 1.256 | 2.347 | Different authorization protocol |
| Logical commits per selected command root | 1.000 | 1.000 | Not instrumented |
| Final debt slope, bytes/s | +2,455.9 | +2,386.2 | Not instrumented |
| Final oldest-publication age slope, ms/s | +3.711 | +4.435 | Not instrumented |
| 50-ms delivery latency gate | Fail | Fail | Pass at this point |

All original node containers were removed before bucket-only cold audits. This
checks recovery after successful drain, not owner loss before materialization.
The candidate lowered p99 by 55.5% and PUTs/command by 25.4% in this one pair,
but its p99 remains **8.72 times celld's**. Both Cellule arms fail the proposal's
publication stability gate. No sustainable-rate improvement is established.

Compaction mean fell from 444.8 to 25.6 ms; publication mean from 1,288.7 to
489.4 ms; Fleet proof wait mean from 88.0 to 52.0 ms. Capture remained about
1.1 ms and tmpfs follower data sync about 0.0005–0.0007 ms. The faster candidate
also performed more compactions and enrollment GETs. Reduced batching is a
possible explanation for the enrollment increase; this run does not prove it.
The provider reached its two-CPU ceiling. Publication amplification and fresh
peer work remain priorities in this profile; device sync performance is untested.

The measured one-command-per-root result cannot satisfy M2's authority-write
budget by sharing data alone. With three per-Cell selection PUTs, the floor is
three PUTs/command before shared data, node coverage or compaction. M3's measured
2.347 enrollment GETs/command is 46.9 times its 0.05 budget. Meeting those gates
requires the specified coalescing/proof/grant protocols and their failure tests.

For the deterministic uniform schedule at 2,000 bucket writes/s, a Cell receives
one command every 500 ms. Waiting for its next command would already exceed the
200-ms bucket p99 target. The existing three-PUT per-Cell selection floor cannot
reach the proposed 0.05 PUT/command budget through cross-Cell data bundling alone.
This is a cost/latency bound under that schedule, not a measured capacity. M4
needs a complete authority-pinned range proof before bucket responses can use
shared coverage; an uploaded bundle or live node epoch alone cannot supply it.

### Fleet target stress and immediate step-down

The rebuilt driver offered 15,000 writes/s for 300 seconds after 30 seconds
warmup. The candidate completed **476.80 writes/s inside the window**, with
3,224.5-ms scheduled p99, 61 request errors and 4,356,649 measured queue drops.
It generated every one of the 4.5 million scheduled offers. This is an overloaded
completion rate, not qualified capacity. All 190,175 successful acknowledgements
from setup, warmup, stress and both later phases passed warm and bucket-only cold
GET/exact-retry audits. Original fleet drain took 24.615 seconds.

The window selected 28,468 command roots covering 145,103 logical commits:
5.097 commits/root. All provider PUT successes/acknowledged command fell to
1.000; fresh owner/receiver enrollment GETs/command were 0.083. These are
time-window ratios with maintenance included, not complete cohort accounting
of trailing work. Neither M2's 0.25 nor M3's 0.05 budget passed.

Publication mean was 4,046 ms and dirty admission mean 5,409 ms. Worker round
trip averaged 9.28 ms and capture 0.65 ms. Follower worker calls averaged
47.9–48.4 ms, while their data-sync barriers averaged 0.0011–0.0013 ms on tmpfs.
The worker timing includes the native call; it does not identify its internal
CPU or filesystem costs. At window end, oldest unpublished work was 224.7 seconds
old and 76,435 node sequences awaited contiguous object coverage. The final
three-minute debt slope was +19,406.5 bytes/s and age slope +903.4 ms/s.

Immediately after stress the harness offered 150 writes/s for 60 seconds, then
50 writes/s for 30 seconds, each without fresh warmup. Both had zero request
errors, queue drops or unissued offers, but scheduled p99 remained 471.6 and
495.0 ms. The 50/s phase failed latency recovery. The nominal reference of
100 writes/s was not qualified capacity; this exercises the phase/audit path,
not M5's required overload at 1.5 times a qualified reference.

Celld's matching stress case failed through lease watchdog self-fencing:
both followers and the owner exited with code 3, without container OOM.
Its mixed healthy/failed window completed 627.14 writes/s with 7,071.1-ms p99,
435 request errors and 4,311,172 drops. The service was unavailable for its warm
audit of 337,803 acknowledgements; cold audit did not run. This establishes
availability and qualification failure, **not acknowledged-state loss or a
clean Fleet capacity comparison**. All failed journals, node logs and the
provider volume remain in the external artifacts.

Latest main's retained attempt completed 121.84 writes/s with 3,683,013 request
errors and 780,436 queue drops, then zero successful step-down writes. Its
54,771 warm checks failed and drain exceeded 120 seconds. **This attempt is
excluded from framework performance attribution:** the shared provider ran out
of inodes by the end of the case, and the original harness had no inode samples.
The following bucket owner received S3 `InternalError: Disk full`; Linux reported
only five free inodes despite 44 GiB of free bytes. Main's window recorded 205
transient PUT outcomes, including seven node-authority writes. The exact timing
and contribution of exhaustion are unknown. These failures cannot establish a
candidate availability advantage or a clean three-arm stress comparison.
The exclusion is retained in the machine-readable evidence, rather than deleting
the failed attempt.

The first candidate and celld bucket attempts produced no throughput sample:
the former failed startup against the exhausted provider, and the latter could
not restart its control container. The dedicated Docker disk was expanded from
80 to 200 GiB without changing CPU/memory ceilings or tmpfs node state. The
runner now checks byte and inode headroom before, during and after each case;
missing or exhausted required observations fail qualification. One diagnostic
volume was archived with a SHA-256 manifest; other retained volumes remain.

New artifact identities are `implementation-m1-bounded-audit` and
`implementation-main-bounded-audit`. Their SQL binary hashes remain identical
to the respective earlier builds; both use client `417f07b0424d` and auditor
`6595c24b0be2`. Fixture, runner and Docker host identities are recorded and must
match. Do not combine these results with the older client's paired point.

### Fresh bucket target after storage reset

With the drain-fixed candidate and monitored provider headroom, 2,000 offered
bucket writes/s for 300 seconds yielded 192.243 successful commands/s, zero
HTTP errors, 542,069 measured drops and 51,687 warmup drops. All 600,000
measured offers were generated; scheduled p50/p95/p99 were 819.1/4,559.0/8,491.8
ms. All 67,245 acknowledged commands passed warm and cold GET/exact-retry audits,
and original owner drain took 3.955 seconds. These overloaded completions do
not qualify sustainable capacity.

All provider PUT successes/command were 4.241 and GET attempts/command 1.291;
the window materialized 1.038 commands per selected root. Worker round trip
averaged 1.539 ms and capture 0.535 ms, versus 460.5-ms publication and 648.1-ms
object-response wait. The 1,823 compaction observations averaged 5,707.4 ms.
These intervals overlap and must not be added into a latency breakdown.
Node-log debt was zero in bucket mode, but oldest-publication age had a positive
51.8-ms/s final trend. All 69 provider filesystem observations passed; minimum
free space was 166.3 GB and minimum free inodes 7,573,521. The earlier disk-full
failure is not an explanation for this target miss.

Celld's matching window completed 905.077 writes/s with 1,075.3-ms scheduled
p99, three request errors, 328,218 measured drops and 24,986 warmup drops. All
600,000 offers were generated, and its warm audit passed all 307,794 acknowledged
commands and exact retries. At this overloaded point the candidate completed
21.24% of celld's rate, with 7.90 times its scheduled p99. These are point ratios,
not qualified-capacity ratios. Original owner drain took 9.761 seconds. During
cold audit the 2-GiB provider was OOM-killed at 05:27:57 UTC; the cold owner
then self-fenced after lease renewal failed. Only nine exact cold retries
completed. This is a provider availability failure: acknowledged-state loss is
unproven, and celld's cold durability is unverified in this case. Provider state
and the explicit cold-audit exclusion are retained in the report.

Latest main's fresh matching window completed 104.710 writes/s, with zero HTTP
errors, 568,331 measured drops and 57,138 warmup drops. Scheduled p50/p95 were
1,367.0/7,131.3 ms; p99 exceeded the 10,000-ms histogram bound (954 overflow
samples), so no exact p99 or percentile ratio is reported. All 35,532 ACKs
passed warm and cold GET/exact-retry audits; original owner drain took 4.212
seconds. All 70 filesystem observations passed, with at least 160.6 GB and
6,564,266 inodes free.

The final framework build `19927d16` repeated the same offered point with the
same frozen runner, fixture, client, auditor and resources. It completed
163.033 writes/s, with zero HTTP errors, 550,834 measured drops and 54,079 warmup
drops; all 600,000 scheduled offers were generated. Scheduled p50/p95/p99 were
1,014.5/5,513.4/8,906.2 ms. All **56,088 ACKs** passed warm and cold GET/exact-retry
audits, and original owner drain took **9.656 seconds**. Cold startup took
32.710 seconds. Provider headroom passed all 69 filesystem observations, with
at least 158.2 GB and 6,136,282 inodes free. Oldest-publication age still grew
55.5 ms/s over the final three minute segments, so stability failed.

| Fresh bucket target point | Latest main | Final packed candidate | celld |
| --- | ---: | ---: | ---: |
| Successful writes/s inside window | 104.710 | 163.033 | 905.077 |
| Scheduled p99, ms | >10,000 | 8,906.2 | 1,075.3 |
| HTTP errors / measured drops | 0 / 568,331 | 0 / 550,834 | 3 / 328,218 |
| Warm + cold ACKs checked by GET/exact retry | 35,532 each | 56,088 each | Warm 307,794; cold unavailable |
| Original owner drain, seconds | 4.212 | 9.656 | 9.761 |
| All successful storage API PUTs/command | 5.874 | 4.186 | Not instrumented |
| GET attempts including ranges/command | 1.592 | 1.218 | Not instrumented |
| Logical commands per selected root | 1.041 | 1.045 | Not instrumented |
| Delivery qualification | Fail | Fail | Fail |

The final overloaded candidate completed 55.7% more writes than retained main,
with 28.7% fewer PUTs/command. It reached 18.01% of celld's measured rate and
8.28 times its scheduled p99. Logical value throughput was 15,651 bytes/s;
provider PUT bytes were 2.65 MB/s, including publication and coordination.
These are different numerators, not user payload versus wire-equivalent rates.
Its publication/object-response means were 536.7/766.4 ms; worker round trip
and capture averaged 1.852/0.644 ms. Compaction averaged 6,427.3 ms over 1,501
observations. These overlapping intervals are not additive CPU service costs.

The earlier candidate completed 83.6% more writes than main, with 27.8% fewer
PUTs/command and publication/object-response means of 460.5/648.1 ms. The final
candidate's completion rate was 15.2% lower than that sample. This is not an A/A
pair: the revision changed. Preserve both samples; three qualified A/A and
paired repetitions remain missing. Main's final publication-age trend was
negative; both candidate samples were positive. Neither the throughput ratios
nor successful audits establish sustainable capacity, stability improvement,
read guardrails or celld parity. The provider lifecycle checker now records
cold/final state as well as byte/inode headroom; an OOM or missing required
lifecycle observation fails future cases.

The earlier immutable candidate is `a1be4caa`, artifact
`implementation-m1-drain-fixed`, SQL hash `7c774549a8f6`. The final candidate is
`19927d16`, artifact `implementation-m1-preserved-contract`, SQL hash
`0560f70c65fc`. The baseline artifact is
`implementation-main-drain-comparison`, SQL hash `85d30c0f93df`; client/auditor
hashes remain `417f07b0424d`/`6595c24b0be2`. Comparisons use the new identical
filesystem-monitoring runner `d70fad321f81`. Results from the former runner
remain separate. The final comparison is indexed by
`matched-verified-bucket-final-matrix.json` and
`matched-verified-bucket-final-report.json` outside the repository.

The new lifecycle runner separately completed a five-second, one-write/s
diagnostic on the final binary: all 1,036 ACKs passed warm and cold GET/retry
audits, with all five required lifecycle observations healthy. The target
comparison deliberately retains its frozen runner; it does not acquire the
new runner's lifecycle evidence retroactively. A diagnostic is not capacity
qualification.

### Read saturation evidence

The same frozen clients offered 10,000 reads/s for five minutes after 30 seconds
warmup, without writes beyond setup. Every arm passed warm and cold GET/retry
audits of its 1,001 seed/contract mutations, with zero HTTP errors. Every arm
failed delivery qualification through dropped offers. These completion rates
are saturation observations, not qualified read capacities or M1's read guardrail.

| Window | Latest main + telemetry | Packed candidate | celld |
| --- | ---: | ---: | ---: |
| Successful reads/s inside window | 9,893.81 | 9,934.98 | 9,321.13 |
| Scheduled p50 / p95 / p99, ms | 1.6 / 2.9 / 11.1 | 1.6 / 2.8 / 8.2 | 2.1 / 26.6 / 73.4 |
| Measured queue drops | 31,834 | 19,494 | 203,645 |
| Warmup queue drops | 14,240 | 902 | 12,138 |
| Unissued measured offers | 4 | 10 | 4 |
| Original fleet drain, seconds | 24.235 | 23.321 | 10.491 |

Inspection and regression tests reproduced why the producer omitted those final
offers: its wall-clock stop could precede emission of an arrival already due
inside the window. The new producer emits the full scheduled cohort and keeps
lateness in the original arrival time. This fixes accounting; it cannot erase
the real queue drops in these historical runs. Three tests failed against the
old guard and passed after its removal. The updated client also accepts the
explicit zero-warmup overload/recovery phases used by the runner.

The new auditor reads JSONL through bounded queues and checks every original
command, rather than retaining a multi-million-response vector. Collection
rejects duplicate IDs, missing successful journal records and partial input.
The runner verifies stream hashes before and after both audits and records its
loaded source and Docker host limits. New comparisons require matching fixture,
runner, host and client identities; older cases remain marked as lacking that
complete provenance. Rebuild both arms before comparing the revised driver;
historical and new clients are not interchangeable.

| Frozen artifact | Run/build identity | SQL binary SHA-256 prefix |
| --- | --- | --- |
| Latest main + measurement overlay | `implementation-main-logical-metrics` | `85d30c0f93df` |
| Candidate | `implementation-m1-one-fetch` | `ff4c6ede25f4` |
| celld | v0.6.1 container digest pinned in `build.json` | Container digest |

The client and auditor hashes are respectively `cc1d47522078` and
`35368139e4c5` for both builds. Full digests and every exported source hash live
in the retained manifests. All candidate production bytes match PR 66's
`a1c48fcd`; the final test expectation and documentation were edited after the
binary export. A Git base revision alone does not identify an overlaid build.

The initial isolated all-feature workspace suite passed **1,839 tests** with **38 ignored**
environment-dependent tests. Clippy and API documentation passed with warnings
denied; format, boundaries, layout, Rust fences, links and SQL/peer contract
checks passed. The first compaction run exposed an old range-GET expectation;
the corrected test now requires zero range GETs and two complete pack GETs.
That failure remains in the external evidence.
The revised client/auditor passed 18 targeted Rust tests and warnings-denied
Clippy; local LTX without replica features passed 58 tests including its doctest.
The Python comparison, report, collector and provider checks passed 28 tests
with warnings treated as errors. These checks do not substitute for live overload, fault or
capacity qualification.

The native fleet process suite exposed an empty-epoch shutdown loop after a
local owner fence. A regression test failed before the fix; the existing
fenced-owner evacuation test stalled at canonical shutdown. Empty coverage
queues now perform no new writer CAS, while pending tickets still reject
fencing and contiguous rotation/member/authority checks remain required. The
original process case passed in 1.30 seconds after the fix, and all 14 runtime
durability tests passed. The complete isolated fleet process suite then passed
all **382 tests** in 589.07 seconds. The full workspace suite, all-target/all-feature
check, warnings-denied Clippy/API documentation, format, architecture/layout,
document and SQL/peer gates passed again after the fix. Process tests are counted
separately from workspace tests.

Final framework revision `19927d16` passed **1,843 workspace tests**, with
**38 ignored**, and all **382 fleet process tests** in 602.78 seconds. Its
all-target/all-feature check, Rust 1.99 warnings-denied Clippy, and API docs
passed; local LTX without replica features passed **58 tests** including its
doctest. All 1,240 Rust/Cargo source files in the isolated verification snapshot
match the checkout. The Linux SQL binary is `0560f70c65fc`; the client and
auditor remain `417f07b0424d` and `6595c24b0be2`.

The recovery fault fixture now accepts only the original typed `Fenced` error
from the deliberately fenced source, retaining the `Draining` state and every
zero-resource-ledger assertion. Healthy receivers must still stop successfully.
A new deterministic case shuts down that source before receiver takeover,
checks that the selected authority record is unchanged, then verifies exact
reconstruction and the stored retry result. Production shutdown preserves
authority-release failures; closing local handles grants no successful release.
Three publication assertions also cover lease expiry, node fencing, and live
release. The failed contract-changing cleanup experiment and its test failures
remain outside Git as excluded evidence; that behavior was reverted.

## Reproduce and inspect

Follow the [harness instructions](../scripts/perf/README.md). Build each revision
into a fresh external directory. Run serially and preserve failed cases.
Inspect `case.json`, `build.json`, `storage-format-smoke.json`, `summary.json`
and generated `report.json`. Content-based cache namespaces and persisted-root
checks prevent stale codec reuse. Source contains reusable drivers and compact
conclusions; caches, volumes, journals, metric windows, binaries and logs stay
outside Git. Each retained provider volume has `store-data/volume.json`.
The external `delivery-evidence-index.json` hashes the final comparison, build,
case, runner, ACK stream, format smoke and verification manifest. The latter
pins the 1,240 Rust/Cargo sources and all final check logs. Failed and excluded
attempts retain their own scope; they are not overwritten by passing reruns.

`scripts/perf/compare.py` accepts an external JSON matrix containing `baseline`,
`candidate` and `celld` lists of case directories. It rejects different workload,
resources, images or client identities. Matching completion rates cannot establish
the full proposal from one pair. Reports include full and range GETs, copy and
multipart calls. SDK-internal retries need provider telemetry for exact HTTP counts.

## Cutover and rollback

1. Use fresh isolated prefixes for qualification and recreatable development data.
2. For a persisted format cutover, stop admission, drain accepted commands and
   preserve the bucket and recovery logs. Upgrade every root reader, producer,
   recovery, backup and collection worker together.
3. Retained version-1 data needs a separately verified logical export/rebuild
   using the old binary. No automatic conversion route is delivered here.
4. A rollback binary cannot read version-3 roots. Use a verified logical
   export/rebuild if available; otherwise preserve artifacts and roll forward.
   Bucket listing and completed uploads never select authority.

## Remaining delivery

Complete M0's reproducibility gate before attributing sustainable-rate changes
to the framework. Measure M2 cohort fill, per-Cell selection cost and debt, and
M3's summed issuance GETs and renewal stalls. The shared coordinator and signed
grant protocol are implemented; their exit budgets require measured evidence.
The authority cost floor requires M4's atomic binding/selector, transfer,
recovery and collection integration before shared coverage can support bucket
acknowledgments. The [authority decision](bundle-coverage-proof.md) defines that
integration; it is not an enabled response path.

Finally run three paired five-minute repetitions, bounded KV, read-only and
mixed/hot-read guardrails, 1.5× overload with immediate recovery, owner loss
before materialization, and device durability. Preserve failures and report
the measured gap until every required gate passes.
