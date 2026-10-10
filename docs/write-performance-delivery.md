# Write performance implementation and verification

The latest [publication-credit report](pr67-publication-pressure-measurement.md)
fixes working memory held after completed root I/O, without raising any bounds.
Two fresh write pairs observe 388.70→589.85/s and 535.17→583.92/s. The repeat
worsens successful scheduled p99 345.91→408.79 ms and fails 713 warm retry
checks; cold recovery is not reached. Total store bytes/success and unpublished
log debt grow in both candidate windows. No acceptable repeatable performance
gain or parity is established; PR #67 remains a draft. The report and harness
now explicitly identify Cellule's Rust/Axum application and celld's JavaScript
Worker/Durable Object: these are matched HTTP/SQL application comparisons,
not an isolated comparison through identical Rust application interfaces.

The latest [composed-publication diagnostic](pr67-composed-publication-measurement.md)
reserves the complete ready root-callback cohort before native batching and
shares one exact catalog/CAS with new captures. Its fresh corrected comparison
records 407.03→468.80 successful Fleet writes/s with unchanged ~389-ms successful
scheduled p99; every one of 53,527 ACKs passes warm/cold mutation and retry
audits. The initial composed candidate regresses 364.02→328.98/s and remains
recorded separately. Total store bytes/success and publication debt still grow;
errors/drops and the remaining transport/qualification gaps keep PR #67 a draft.
Earlier milestone reports below are historical; acceptance gates are unchanged.

This delivers packed dependencies, shared publication, signed append grants and
a quantified gap report, not the completed M0–M5 plan. A pinned Docker
comparison retains exact retry and cold-state audits. **Celld write parity
has not been established.** The [proposal](write-performance-proposal.md) remains
the acceptance contract; completing tests or a load run does not pass its gates.

The latest [fresh write comparison](pr67-base-pipeline-measurement.md) completes
579.10 Fleet writes/s for unchanged production code, 570.25/s for the trial and
1,999.83/s for celld. The trial is reverted: this pair establishes no acceptable
TPS/latency improvement. Cellule's warm ACK audits fail with 467 and 2,779 HTTP
503s; neither reaches cold audit. Celld passes all 182,001 warm/cold mutations
and original retries. All profiles remain unqualified and PR #67 stays a draft.

The preceding [paired 2,000-Cell comparison](pr67-metadata-window-measurement.md)
measures `4a5b001` at **502.28 Fleet writes/s versus 455.73 before and 1,999.83
for fresh celld**. Observed GET/range work falls, but successful scheduled write
p99 worsens 827.14→1,454.42 ms and returned errors increase. Read-only throughput
falls 17,905.02→17,347.03/s; celld completes 19,983.30/s. The candidate passes all
48,888 ACK warm/cold reads and original retries and drains in 66.52 seconds.
The baseline write case fails two warm retries and never reaches cold recovery.
Both Cellule read arms drop offers, the read guardrail fails and every performance
profile fails. This single short pair establishes no acceptable, repeatable or
attributable gain. PR #67 remains a draft. The
[preceding encoder comparison](pr67-encoder-cost-measurement.md) and earlier
passing and failed evidence remain separate observations.

The separate [publication-path diagnosis](pr67-publication-path-diagnosis.md)
adds external timing only to the same production revision. It reproduces
468.42 writes/s versus 1,999.77 for celld and reconciles complete warm/cold
ACK audits. Serial selection takes 106.68 ms per 59.30 captures, with checkpoint
work consuming another 14.83% of the interval. Ordered-lock wait averages
108.52 ms in an exact submission partition. Write errors/drops remain; this
supports the bottleneck mechanism and makes no new production gain claim.

The earlier [selection-readiness comparison](pr67-selection-readiness-measurement.md)
measures `6c909a6` at **184.77 Fleet writes/s and 248.68 Bucket writes/s**, versus
195.13 and 247.55 before. Its delayed-selection actor regression passes, but
Fleet still returns 328,243 measured errors and fails 19,797 of 21,366 warm
ACK checks. Bucket passes all 26,423 warm/cold checks but drops 104,823 offers.
No throughput improvement or parity is established; PR #67 remains a draft.

The earlier [release-build repeat](pr67-release-repeat-measurement.md) measures
the unchanged `4a8f55c` binary at **107.95 Fleet writes/s and 268.12 Bucket
writes/s**. Fleet returns 499,450 measured errors and fails 16,057 of 16,190
warm ACK checks. Bucket passes all 24,880 warm/cold checks but drops 103,657
offers and misses latency/throughput targets. Celld completes 4,470.70 Fleet
and 1,392.55 Bucket writes/s; Fleet is OOM-killed, while Bucket audits pass.
Every point fails qualification. Diagnostic logs identify selection deadlines
that fence Cells and publication backlog refusals. No acceptable improvement
or parity is established; PR #67 remains a draft.

The earlier [checkpoint-continuity measurement](pr67-checkpoint-continuity-measurement.md)
records runtime commit `4a8f55c`: **185.83 Fleet writes/s and 215.08 Bucket
writes/s**, versus 164.73 and 227.15 for its immediate `6d62d41` baseline.
Fleet still returns 304,151 measured errors and fails its warm ACK audit.
Bucket has passing ACK audits but a 5.3% lower completion rate and higher p99.
Focused checkpoint regressions and all contributor checks pass; the original
application load failure persists. Celld also fails these fresh Fleet/Bucket
cases through OOM/self-fencing. Every point fails qualification; no acceptable
improvement or parity is established, and PR #67 remains a draft.

The earlier [asynchronous-root measurement](pr67-async-root-measurement.md)
records runtime commit `6d62d41`: 183.65 Fleet writes/s and 204.10 Bucket writes/s
versus 115.27 and 196.27 in fresh paired windows. Fleet successful scheduled p99
is 1,782.74 ms, but the candidate returns 297,811 measured errors and fails its
warm ACK audit; cold recovery and successful drain are unverified. This is an
availability regression, not an acceptable performance gain. Root density rises
from 1.12 to 10.25 and PUTs fall from 3.67 to 0.48 per completed write, while
retention and publication age grow. Bucket audits pass, but its fixture bypasses
the producer. Every point fails qualification; PR #67 is a draft.

The earlier [cohort-origin measurement](pr67-cohort-origin-measurement.md)
records runtime commit `9d4e632`: 95.35 Fleet writes/s and 274.53 Bucket writes/s
versus 88.28 and 230.58 for the immediate predecessor in fresh paired windows.
Fleet successful scheduled p99 is 4,414.52 ms; Bucket is 4,367.63 ms. The
64-Cell regression reduces reads of one fresh bundle from 187 to one; native
GET/range work falls, but total Fleet GET/range work remains near 20.4 requests
per completed write. Steady Bundle ACKs are zero and root density is 1.08.
All Cellule ACK audits and joined drains pass. Single short pairs, including a
Bucket fixture that bypasses the optimization, do not establish attributable
throughput gains. Every point fails qualification; PR #67 remains a draft.

The earlier [coverage-race measurement](pr67-coverage-race-measurement.md)
records runtime commit `e40ecd6`: 100.20 Fleet writes/s and 271.63 Bucket writes/s,
5.5% and 5.3% lower than the immediately preceding code in one fresh pair.
Successful scheduled p99 also worsened. Fleet warm availability, joined drain
and all-ACK cold read/retry now pass; there is no demonstrated throughput or
latency gain. Fresh celld completed 4,202.05 Fleet/s with failed warm audit and
OOM, and 1,472.08 Bucket/s with passing ACK audits. Every point fails delivery
qualification; PR #67 remains a draft.

The earlier [managed-producer measurement](pr67-managed-producer-measurement.md)
records runtime commit `2dc1917`: 93.57 Fleet writes/s versus 525.67 before
the producer, an 82.2% decrease in one matched overloaded pair. Successful p99,
warm availability and drain regressed. Bucket current code completed 227.78/s
versus 246.20/s; its fixture bypasses the producer. Celld completed 4,238.78
Fleet/s with failed warm audit and OOM, and 1,368.50 Bucket/s with passing
ACK audits. Every point failed delivery qualification. PR #67 remains a draft.

The earlier [selected-capture release measurement](pr67-selected-capture-release-measurement.md)
records committed code at `3e43601`: 461.53 Fleet writes/s and 168.92 Bucket
writes/s in one matched 60-second Docker diagnostic. Fleet was 5.9% higher than
baseline; Bucket was 18.2% lower, with worse successful-write latency. All ACK
audits passed, but every system failed delivery targets. The actor can release
exact selected captures through its original publisher; the application still
does not install a node bundle producer, and bundle response counters are zero.
These results establish neither an attributable improvement nor parity. The
older comparisons below remain historical evidence.

The [PR 67 reevaluation](pr67-performance-reevaluation.md) measures the earlier
protocol implementation at `7fc0793` in nine fresh matched Docker cases. Fleet
100/s p99 is 19.9 ms versus main's 34.7 ms and celld's 16.0 ms. Target-load
delivery still fails: candidate Fleet completion is below main, bucket is
modestly better, and provider/recovery failures remain explicitly recorded.
The older measurements below are historical and are not measurements of the
latest bundle APIs.

The [WAL NORMAL reevaluation](pr67-normal-wal-reevaluation.md) records a later
interrupted comparison at `075b2cd`; it does not qualify parity. The
[indexed bundle implementation](bundle-coverage-implementation.md) reduces one
1,000-Cell catalog update from 783,146 to 35,223 metadata bytes and checkpoints
64 exact roots with two shared PUTs. Those are protocol component measurements;
ordinary actor responses still use the previous publication path.
Canonical small-tail materialization also reduces the 64-root cohort from 320
to 256 PUTs and a 32-locator suffix from 37 PUTs to four, with exact cold restore.

Detached histories now retain 256 exact references per Cell under the original
4 MiB native-suffix bound, fetching only requested histories. A small-image
215-command regression among 2,000 catalog bindings initially materializes with
220 PUTs; the streaming bounded coalescer reduces it to four. These are component
measurements, not 2,000-writer or application TPS qualification. The
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md) records
the 8-vCPU/16-GiB node target: 2,000 Cells, 10K write TPS and 50K read TPS.

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
| M2 | Bounded file-backed shared publication coordinator implemented; exact scope, restore, cancellation, minimum-budget and dormant-sibling retention checks added | Earlier three active-Fleet windows cost 5.229–5.433 PUTs/command; two fail the debt trend. Per-Cell authority work remains; M4 is required |
| M3 | Signed 512-sequence/five-second grants, bounded local registry, lifecycle gate and signed HTTP fixture implemented | Full isolated checks and native lifecycle suite pass; earlier three active-Fleet windows cost 0.0138 enrollment GETs/command. The latest 15K diagnostic still fails delivery despite passing ACK audits |
| M4 | [Connected protocol APIs](bundle-coverage-implementation.md), exact capture retirement, coalesced root debt, admitted asynchronous materializers, retained producer, fair native/checkpoint turns and joined closure | Latest Fleet availability and ACK audit regress. Materializer progress, checkpoint continuation, application receipt visibility, Bucket connection, failed-node orchestration, collection and qualification remain open |
| M5 | Three paired low-rate Fleet repetitions and target diagnostics with exact ACK audits delivered | Publication stability and target delivery fail; qualified capacity, read/failure/overload matrix and absolute/relative parity remain unverified |

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
durable closure gates. Bundle ACKs now require the installed original producer
and admitted exact proof. Its Fleet connection remains experimental after the
measured regression; sustained materializer progress, full recovery orchestration, collection
and qualification still need production integration.

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
isolated verification passed 1,862 workspace tests (38 ignored), 58 local LTX
tests, all contributor checks and the complete 382-test serial native lifecycle
suite. The release binary is pinned to `a3787a8d`; all 1,248 Rust/Cargo source
hashes match the verified snapshot. Three matched Docker repetitions and fresh
routing CI are recorded below. The `d351d886` results further below
must not be attributed to this subsequent change.

Fresh routing CI for `75ae439d` passed all four paired command comparisons in
both modes against `831877cf`. Median command-throughput ratios are
0.992–1.069 in leased mode and 0.967–1.143 in object-only mode. Steady local and
forwarded query-throughput ratios are 0.958–1.015. These native routing workloads
have different transaction/arrival contracts from the SQL Docker comparison.
Their CI latency limit is two times baseline, not the proposal's 1.2 read limit:
object-only forwarded/local concurrency-one query p99 ratios were 1.614 / 1.241.
Passing routing CI therefore does not pass the M5 read guardrail.

The same head's workspace CI initially failed one combined-maintenance lifecycle
test with a nested fleet-action-journal conflict (381/382 passed). The complete
unchanged-source rerun passed, as did the isolated 382-test native suite. Both
CI results are retained; a passing rerun does not establish the conflict's cause.
Fresh [Rust CI at `954cacf`](https://github.com/crabbuild/cellule/actions/runs/37620661541)
also passes all 382 native lifecycle tests. That revision changes reporting and
models, not the frozen production Rust/Cargo source.

## Latest singleton/shared/grant measurement

Release candidate `a3787a8d`, current main `831877cf` and celld `f2bf6486`
completed three matched Fleet repetitions at 100 offered writes/s. Each used
300 seconds plus 30 seconds warmup, 1,000 uniform Cells, 96-byte SQL-ledger
values and 128 clients on the shared Docker VM. System order alternated;
clients, auditors, images and workload contracts match. Neither Cellule arm
uses a measurement overlay.

| System | Window commands/s, repetitions 1 / 2 / 3 | Scheduled p99, ms, repetitions 1 / 2 / 3 | Point delivery/latency result |
| --- | --- | --- | --- |
| Main `831877cf` | 100.000 / 99.977 / 100.000 | 123.9 / 140.2 / 158.6 | All exceed 50 ms |
| Candidate `a3787a8d` | 100.000 / 100.000 / 100.000 | 19.8 / 23.1 / 46.9 | All pass; two publication-debt trends fail |
| celld `f2bf6486` | 100.000 / 100.000 / 100.000 | 15.9 / 15.6 / 18.2 | All pass; publication-debt age unavailable |

All nine cases checked every one of 34,001 ACKs with warm GET/retry and cold
GET/retry, with zero audit failures. All original owner/follower containers
were destroyed after drain and before cold restore. Candidate Fleet remained
active, unfenced and non-rotating with advancing valid follower frontiers in
all three windows; its drain took 5.41 / 8.29 / 15.50 seconds.

Candidate total PUTs/command were 5.4330 / 5.3979 / 5.2295, versus main's
5.4189 / 5.4156 / 5.3684. Summed fresh owner/receiver enrollment GETs/command
were 0.0138 in each candidate window. Candidate debt slopes were
+34.80 / +18.58 / −239.85 bytes/s and oldest-age slopes were
+0.0817 / +0.0200 / −1.2583 ms/s. The first two remain failures; the third
passes. Main fails all three debt trends. This does not establish stable
capacity or a repeatable publication-cost reduction.

The candidate used 29,592 / 29,456 / 27,950 singleton publications out of
30,000 Cell submissions per window. Multi-row cohorts accounted for
408 / 544 / 2,050 submissions; no pressure or large-input fallback occurred.
Near-singleton arrival patterns explain why payload sharing has little effect
on the per-Cell authority floor. The singleton optimization removes extra local
work while preserving the canonical native pack and exact root.

In the first window, mean capture was 0.550 ms, Fleet proof 3.690 ms, Fleet
response 5.582 ms and publication 22.676 ms. These phases overlap and cannot
be added. The worker round trip was 1.522 ms, including queueing. Mean shared
upload was 43.744 ms over only 151 multi-row cohorts; it must not be compared
with earlier histograms that mixed singleton and multi-row uploads. Sampled
Docker CPU averaged approximately 0.51 owner cores and 1.63 of RustFS's two
cores. This identifies provider occupancy at this point, not CPU capacity or
a sustainable-rate ceiling.

## Target-load failures

One matched 300-second Fleet target pair offered 15,000 writes/s after 30
seconds warmup. All three arms fail delivery qualification:

| System | Window commands/s | Scheduled all-attempt p99, ms | Request errors / dropped offers | ACK audit outcome |
| --- | ---: | ---: | --- | --- |
| Main `831877cf` | 507.040 | 1,391.8 | 254,842 / 4,092,793 | All 198,356 ACKs pass warm/cold GET and retry; 15.04-s drain |
| Candidate `a3787a8d` | 295.263 | 143.1 | 3,667,746 / 743,667 | 432 warm 503s; cold not reached |
| celld `f2bf6486` | 1,008.373 | 117.8 | 2,714,671 / 1,482,817 | Owner self-fences; all warm requests fail transport; cold not reached |

The candidate completes fewer commands than main at this overloaded point;
its lower all-attempt p99 includes fast failures and cannot establish a latency
or capacity win. Main coalesces 4.856 materialized commands/selected root,
versus the candidate's 2.080. These observations do not isolate a causal change,
but they require preserving coalescing and retained-memory headroom when making
the follower proof path faster. Main also fails the debt-age trend despite
remaining in active Fleet throughout its window.

The frozen candidate's 15,000 offered Fleet writes/s diagnostic completed
295.263 commands/s inside its 300-second window. It had 3,667,746 request
errors, 743,667 dropped offers and scheduled all-attempt p99 of 143.1 ms.
Its 104,442 actual seed/warmup/window/trailing ACKs reconcile with the journals,
but warm audit returned 432 HTTP 503 errors. Cold restore was not reached;
the case is failed, not a data-loss or durability success claim.

Follower proofs advanced while sampled object coverage fell 50,548 sequences
behind. Tail debt grew by 361,853 bytes/s and oldest age by 837.56 ms/s.
Window PUT cost of 1.750/command excludes its substantial unpublished suffix
and trailing work; it cannot be credited as an improvement. Dirty admission
averaged 348 ms and publication 3,968 ms, while capture averaged 1.282 ms.
These are overlapping waits/populations, not additive CPU service times.
The final sample had 997 active Cells and 54.79 MiB of the 64-MiB retained
budget charged. The audit records status but not the underlying source error,
so pressure eviction, pending logical proof and unfinished accepted work remain
diagnostic possibilities rather than an established cause.

The two followers' window mean durable append times were 16.61 / 17.21 ms
per append operation; native worker time was 16.30 / 16.87 ms and worker queue
wait 0.158 / 0.185 ms. Their data-sync means were only 0.0010 / 0.0021 ms
on tmpfs. These populations are batched append operations, not commands.
This profile does not identify filesystem sync as the dominant wait and cannot
answer physical-storage fsync versus RocksDB performance. Preserve the native
barrier; profile the remaining worker work in a separate diagnostic run.
The native append path calls `prune_covered`, which scans and hashes current
open-chunk records before appending and can rewrite/reconcile after coverage
advances. This is a concrete source of additional work, not a measured fraction
of the 16–17-ms worker time. Measure scan bytes, record count and rewrite time
before changing it; current-disk corruption and uncovered-witness checks cannot
be replaced by an unchecked cached watermark.

The matched celld Fleet target completed 1,008.373 commands/s before delivery
failure. Its owner evicted a gray follower, temporarily fell back to bucket,
then self-fenced after an ambiguous lease renewal and exited with code 3.
All 440,676 warm audit requests failed transport; cold restore was not reached.
Neither target point qualifies capacity or the reported laptop's bounded-KV
result. Failed cases and exact binaries remain retained outside Git.

The matched bucket target offered 2,000 writes/s under the same resources:

| System | Window commands/s | Scheduled all-attempt p99, ms | Request errors / dropped offers | ACK audit outcome |
| --- | ---: | ---: | --- | --- |
| Main `831877cf` | 182.913 | 7,900.9 | 0 / 544,870 | All 61,988 ACKs pass warm/cold GET and retry; 6.01-s drain |
| Candidate `a3787a8d` | 172.283 | 6,632.2 | 0 / 548,059 | All 59,526 ACKs pass warm/cold GET and retry; 7.43-s drain |
| celld `f2bf6486` | 756.387 | 813.5 | 0 / 372,827 | All 260,506 warm checks pass; cold has 137 HTTP 500 errors |

All measured offers were generated, but every arm dropped requests in both
warmup and measurement. Candidate PUT cost was 3.823/command versus main's
4.220, with 1.026 versus 1.045 materialized commands/root. Candidate publication
age grew 17.37 ms/s and retained-capture debt grew 464.38 bytes/s; main also
fails age growth. The lower window PUT ratio does not qualify M2 or stable
capacity. Celld's provider remains healthy through cold and final snapshots;
the 137 cold errors' source is unresolved. They cannot be attributed to the
historical provider OOM or labeled acknowledged-state loss without further
evidence. All six target cases remain in the matched comparisons.

## Qualification decision

This delivers the quantified gap report permitted by M5. It does not qualify
rollout or complete M0–M5. Each target has only one matched pair; three
repetitions at 100/s are diagnostic points rather than a capacity search.

| Gate | Current result |
| --- | --- |
| Contributor checks, native lifecycle and bounded protocol models | Pass within their documented scope |
| Latest three Fleet 100/s delivery/latency and every warm/cold ACK retry | Pass; candidate remains slower than celld in each p99 comparison |
| Three stable Fleet windows and M2's 0.25 PUT/command budget | Fail: two candidate debt trends grow; cost 5.229–5.433 |
| 15K Fleet and 2K bucket absolute targets | Fail in every arm; candidate also completes fewer overloaded commands than main |
| M3's 0.05 fresh enrollment GET budget | Within budget at the three 100/s points; full qualification remains unverified |
| M4's complete production integration, atomic fault matrix and 0.05 total PUT budget | Incomplete; protocol tests do not enable ACKs or GC and no new TPS/cost qualification has passed |
| Read-only/mixed capacity and 1% hot-Cell guardrails | Unverified in Docker; two native routing p99 ratios exceed 1.2 |
| A/A capacity variance and relative parity | Unverified; overloaded completions cannot supply the reference |
| Qualified overload, safe refusal before SQL and immediate recovery | Unverified; the historical step-down failed latency |
| Bounded KV and persistent-device failure profile | Unverified; SQL-ledger/tmpfs results do not establish these |

## Earlier corrected shared/grant measurement

Candidate `d351d886` and main `831877cf` use separate source-content-isolated
release builds, with no measurement overlay. Celld is pinned to v0.6.1
`f2bf6486`. Clients, ACK auditors and provider images are byte-identical across
the arms. The current profile is the SQL ledger workload below on one shared
8-CPU/16-GiB VM, with tmpfs node state and fresh RustFS volumes.

Healthy celld shipping waits for every selected follower; the fixture checks
initial enrollment of both followers. Celld can reconfigure down to one follower,
while this Cellule configuration retains two required members. Fault/availability
qualification must align that policy separately. Celld's publication-debt age
and structured window membership are unavailable in the current fixture.

Three paired repetitions offered 100 writes/s for 300 seconds after 30 seconds
warmup, alternating system order. All original node containers were removed
after drain and before each bucket-only cold audit. These are matched diagnostic
points, not a sustainable-capacity or parity claim.

| System | Window commands/s, repetitions 1 / 2 / 3 | Scheduled p99, ms, repetitions 1 / 2 / 3 | Point delivery/latency result |
| --- | --- | --- | --- |
| Main `831877cf` | 99.997 / 100.000 / 99.997 | 123.2 / 89.3 / 90.0 | All exceed 50 ms; first also loses active Fleet shipping |
| Candidate `d351d886` | 100.000 / 99.987 / 100.000 | 15.6 / 36.4 / 31.2 | All pass; all three publication-debt trends fail |
| celld `f2bf6486` | 100.000 / 99.997 / 100.000 | 11.1 / 15.8 / 14.1 | First/third pass; second has one request-identity validity error |

Every actual ACK passed warm and cold GET plus exact retry: 34,001 per Cellule
run and 34,001 / 34,000 / 34,001 for celld. The celld error is not an ACK-loss
observation. Its underlying clock/identity cause remains unestablished; no
backdating or policy change is applied. Main's first follower rejected a signed
deadline above its allowed horizon before directory verification; zero final
debt after fallback does not qualify Fleet. The original failed cases remain
in the comparison.

Candidate PUTs/command were 5.4842 / 5.3301 / 5.3969 and summed fresh enrollment
GETs/command were 0.0138 / 0.0141 / 0.0138. Candidate tail-debt slopes were
+98.53 / +1,588.95 / +12.54 bytes/s; all remain failures. Celld debt age is not
exposed by this fixture, so its stability is unavailable rather than passing.
Fleet stayed active with advancing follower proofs in all three repetitions.
The publication stability and 0.25-PUT M2 budget still fail. These earlier
histograms include singleton shared uploads; their timings cannot be compared
with the latest multi-row-only histogram.

Reports require Fleet activity and proof advancement independently of debt.
An inactive fallback run cannot qualify Fleet even if it has zero enrollment
GETs or no remaining follower debt. Window cost subtracts cumulative counters
and raw histogram buckets; decreasing summary percentiles/means are not
monotonic counters. Reporter source hashes accompany reevaluated evidence.

## Historical results retained

Earlier builds and clients remain separate evidence. They are not pooled with
`a3787a8d` or used as qualified capacity. The matched profile is the SQL ledger,
not bounded KV; tmpfs and the shared VM do not establish device persistence or
independent-node isolation.

| Earlier profile | Main | Packed candidate | celld | Result |
| --- | --- | --- | --- | --- |
| Fleet, 100 offered/s | 99.993/s; p99 868.1 ms | 99.990/s; p99 386.5 ms | 100.000/s; p99 44.3 ms | All 34,001 ACKs/arm pass warm/cold GET and retry; both Cellule debt trends fail |
| Fleet, 15K offered/s | 121.84/s; provider-exhausted case excluded from attribution | 476.80/s; p99 3,224.5 ms | 627.14/s; p99 7,071.1 ms | All fail delivery; candidate audits 190,175 ACKs; celld self-fences before warm audit |
| Bucket, 2K offered/s, fresh provider | 104.710/s; p99 above 10,000-ms histogram bound | Final `19927d16`: 163.033/s; p99 8,906.2 ms | 905.077/s; p99 1,075.3 ms | All fail delivery; main/candidate cold audits pass, celld provider OOM during cold audit |
| Read-only, 10K offered/s, older client | 9,893.81/s; p99 11.1 ms | 9,934.98/s; p99 8.2 ms | 9,321.13/s; p99 73.4 ms | All drop offers; 4 / 10 / 4 final offers unissued; all 1,001 seed ACKs/arm pass warm/cold audit |

The first Fleet point reduced candidate PUT cost from 6.688 to 4.992/command,
but candidate p99 remained 8.72 times celld's and its debt grew. The earlier
`a1be4caa` bucket candidate completed 192.243/s at p99 8,491.8 ms, versus final
`19927d16` at 163.033/s. The revision changed: this is not an A/A pair. Both
candidate bucket samples had positive publication-age trends. Final PUT cost
was 4.186/command and logical commits/selected root 1.045, preserving the
sparse per-Cell cost/latency gap.

The old candidate's immediate Fleet step-down offered 150/s for 60 seconds
then 50/s for 30 seconds. It had zero delivery errors/drops but p99 remained
471.6 / 495.0 ms. The 100/s nominal reference was not qualified capacity;
this is a failed latency-recovery diagnostic, not the required overload gate.

The provider-exhausted main attempt had only five free inodes despite 44 GiB
free bytes. Its contribution to failures is unknown; initial bucket attempts
produced no throughput sample. Later cases use fresh provider volumes plus
byte/inode and cold-lifecycle checks. Celld's later bucket case passed all
307,794 warm ACK/retry checks, then its 2-GiB provider was OOM-killed during cold
audit. This is availability failure with cold durability unverified, not proof
of acknowledged-state loss. All exclusions and failed journals remain retained.

The producer's final-offer omission was reproduced and fixed; historical drops
and unissued requests remain failures. Bounded JSONL ACK collection now checks
every original retry, stream integrity and complete counts. The empty-epoch
shutdown loop was also reproduced and fixed without weakening pending-ticket
fencing. A cleanup experiment that weakened release contracts was reverted;
its failures remain excluded evidence. The final packed build passed 1,843
workspace tests (38 ignored), 58 local LTX tests and 382 native lifecycle tests.
Current verification and source identities appear above.

## Reproduce and inspect

Follow the [harness instructions](../scripts/perf/README.md). Build each revision
into a fresh external directory. Run serially and preserve failed cases.
Inspect `case.json`, `build.json`, `storage-format-smoke.json`, `summary.json`
and generated `report.json`. Content-based cache namespaces and persisted-root
checks prevent stale codec reuse. Source contains reusable drivers and compact
conclusions; caches, volumes, journals, metric windows, binaries and logs stay
outside Git. Each retained provider volume has `store-data/volume.json`.
The external `singleton-r4-evidence-index.json` hashes the latest comparisons,
builds, case reports, loaded runners, ACK manifests and verification results.
`singleton-r4-qualification-status.json` records each passed, failed or unverified
gate. The production verification manifest pins all 1,248 Rust/Cargo sources. Failed and excluded
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
