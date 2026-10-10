# Write performance implementation and verification

The October 10 checkpoint-verification diagnostic compares production sources
matching `25a76c0` with bounded concurrent root verification. At 2,000 offered
writes/s across 2,000 Cells, Cellule completes 408.10/s before and 573.72/s after;
successful scheduled p99 worsens from 341.71 to 422.65 ms. Fresh celld completes
1,999.80/s at 30.99 ms. All 46,747 baseline, 66,091 candidate and 181,995 celld
ACKs pass warm/cold state and original-retry audits. Cellule drain falls from
78.01 to 57.95 seconds, but errors, dropped offers and growing unpublished debt
remain. This single 60-second write-only pair on a shared 8-CPU/8-GiB VM proves
neither sustainable capacity nor the requested 8-vCPU/16-GiB mixed-load target.
The subsequent change overlapping checkpoint authority reads is not included
in those measurements. Its separate 60-second run completes 598.72 writes/s at
443.01-ms successful scheduled p99, with 16,152 returned errors and 67,752 dropped
offers. All 68,049 ACKs pass independently reconciled warm/cold state and retry
audits; drain takes 58.43 seconds. This is another unqualified diagnostic, and
does not measure the subsequent live-prefix extension experiment.

The live-prefix experiment's separate 60-second pair completes 852.28 writes/s
at 388.69-ms successful scheduled p99, but fails 4,696 of 91,268 warm ACK checks
with HTTP 503s. Cold audit is therefore unverified. Measured requests include
12,066 errors and 56,563 dropped offers. Fresh celld completes 1,999.85/s at
13.95-ms successful scheduled p99, with all 182,001 warm/cold state and retry
checks passing. Independent journal reconciliation covers every original ACK
in both arms; it does not turn failed availability checks into a correctness
pass. Cellule's retained capture bytes rise from 0.62 to 1.05 MB while the
root-materialization debt counter rises from 58.25 to 85.70 MB. Those are
different obligations; diagnosis of the 503s remains open. This experiment
establishes neither an acceptable improvement nor parity.

Three subsequent runs with application error logging complete 839.17, 825.03
and 872.72 writes/s. All 92,183, 88,976 and 92,795 ACKs respectively pass warm
and cold reads and original retries. They do not reproduce or resolve the
earlier warm-audit failure. Load-time errors are predominantly publication
backlog refusals; the first two probes also report 136/204 node-retained-byte
refusals and 15/16 resource-ledger refusals. A focused admitted-query probe
confirms that native admission pressure alone does not fence the original
Cell. It remains external diagnostic evidence, with no runtime fix inferred.
These runs use the same runtime source with external application probes; the
provider VM disk was expanded from 240 to 320 GiB after an initial attempt
failed the unchanged inode-health gate. CPU and memory stayed unchanged, and
all earlier volumes remain preserved. A native test build overlapped only
the second probe's post-audit drain, so that drain is not a performance sample.
All three probes remain unqualified; background materialization and foreground
work still contend for retained-memory admission.

A subsequent fresh pair tests eight ordered native range reads during
materialization, within the unchanged memory allowance. Baseline throughput is
852.55 writes/s and the trial is 790.80/s; measured request errors rise from
11,700 to 15,270, and successful scheduled p99 worsens from 368.62 to 514.08 ms.
All 91,575 baseline and 86,452 trial ACKs pass warm/cold state
and original-retry audits. Fresh celld completes 1,999.78/s with three measured
request errors and all 181,994 ACKs passing both audits. The trial passed all
twelve contributor checks, including 2,075 tests (43 ignored), but this single
60-second pair establishes no acceptable throughput improvement. The parallel
reconstruction change is reverted; its frozen source and failed-before,
passed-after concurrency/cancellation tests remain in the external
`materialization-candidate` evidence. Qualification remains unmet.
Immutable manifests, journals and independent audits
are retained under `cellule-ios-parity-20261010` in the external build volume.

The fresh read/mixed baseline uses the retained live-prefix implementation,
with serial reconstruction. Read-only throughput is 18,081.33/s, with 115,097
measured offers dropped and 30.8-ms scheduled p99 across attempted reads.
Simultaneous offered load of 2,000 writes/s and 20,000 reads/s yields only
191.17 writes/s and 4,153.95 reads/s. The mixed window has 420 write errors,
108,100 dropped writes and 950,513 dropped reads; scheduled p99 across attempts
is 146.3 ms for writes and 91.4 ms for reads. Independent reconciliation covers
both phases' raw counters, successful outputs and all 21,691 acknowledged
writes, which pass warm/cold state and original-retry audits. The same shared
8-CPU/8-GiB VM and 60-second diagnostic limits apply. A subsequent actor trial
skips full-resident materialization scans after ordinary read events, retaining
scans after publication, mutation, lifecycle and timer events and throughout
shutdown. In the matching trial, read-only throughput rises to 19,115.47/s and
scheduled p99 falls to 4.6 ms, with 53,047 dropped measured offers. Mixed traffic
rises to 663.02 writes/s plus 7,583.73 reads/s; scheduled p99 across attempts
falls to 99.4 ms for writes and 67.5 ms for reads. The mixed phase still has
8,344 write errors, 71,842 dropped writes and 744,753 dropped reads. All 74,097
ACKs pass warm/cold state and retry audits. The same client/auditor binaries and
images were used, and all twelve contributor checks passed, including 2,073
workspace tests (43 ignored). The read-scan change is retained as an improvement
in this single diagnostic pair, without claiming repeatability or qualification.
Mixed-load materialization debt still grows from 56.02 to 76.25 MB while retained
capture bytes stay below 0.3 MB at both window boundaries; healthy bucket
publication alone does not establish sustainable root materialization.

The fresh pinned celld v0.6.1 reference reaches 19,994.73 reads/s alone and
1,989.33 writes/s plus 19,832.78 reads/s under the same mixed offer. Mixed
scheduled p99 is 27.2 ms for writes and 13.9 ms for reads. It still reports
11 measured write errors, 612 dropped writes and 9,932 dropped reads, so it
also fails the strict diagnostic gates. All 181,020 ACKs pass warm/cold state
and retry audits. These results demonstrate substantial remaining Cellule
headroom in this environment. The architectural source review used v0.6.2;
these measurements remain v0.6.1, and full resource and repetition
qualification is still missing.

The affected-Cell eligibility trial checks only that Cell after ordinary
command ingress, non-fenced execution/proof completions and successful bundle
selection. It requests a full fleet scan only when that Cell is eligible,
using the same readiness predicate as the dispatcher. Existing timer ticks,
root-publication completions, lifecycle events and shutdown retain full scans.
All twelve verification routes pass, including 2,073 workspace tests. Read-only
throughput is 18,883.92/s with 3.5-ms scheduled p99. Mixed throughput rises to
802.18 writes/s plus 12,304.78 reads/s, but returns 29,305 write errors and 275
read errors, drops 42,540 writes and 461,412 reads, and has scheduled p99 of
108.1 ms and 70.8 ms respectively. All 92,319 ACKs pass independently reconciled
warm/cold state and original-retry audits. The same client/auditor binaries and
images were used. This source remains the working diagnostic candidate; the
higher error counts and missed capacity/latency gates preclude acceptance as a
qualified improvement.

The external root-pressure probe completes 822.22 writes/s plus 12,916.13
reads/s, with 30,232 write errors and 17 read errors. All 93,262 ACKs pass
independently reconciled warm/cold state and original-retry audits. Across the
full owner lifetime, 174 of 508 sampled soft admission refusals occur with no
root working reservations. All five logged hard node-reservation failures
observe seven active roots charging about 33 MB. These observations identify
both publication backlog and background working-memory pressure; they do not
attribute every measurement-window failure to either cause. The counters are
sampled separately and include drain/retry work. A further external diagnostic
measures publication phase wall times before choosing a production change.
The instrumented results remain unqualified. No budget is raised.

The publication-phase probe completes 824.77 writes/s and 13,688.65 reads/s,
with 28,634 write errors and 87 read errors. All 92,449 ACKs pass independent
warm/cold state and original-retry reconciliation. Excluding initialization,
1,716 completed publication rounds spend 40.88 seconds in combined encoding
and upload, 26.98 seconds loading catalogs, 6.85 seconds in selection CAS, and
0.03 seconds waiting for the example authority mutex. These sums include
warmup, measured load and outstanding publication drain; they are wall times,
not measurement-window CPU profiles. They point to preparation throughput,
not lock acquisition, as the next optimization target. A further external probe
separates encoding, its validation passes, and object-store PUT duration.

That finer probe completes 645.87 writes/s and 11,609.13 reads/s, with 36,469
write errors and 737 read errors; all 82,575 ACKs pass independent warm/cold
state and retry reconciliation. Its 1,604 post-initialization rounds spend
28.79 seconds in object PUTs and 12.62 seconds encoding, including 5.47 seconds
in encoder self-verification. These instrumented single runs are not a clean
performance comparison. The next isolated hypothesis trial doubles the cohort
from 64 to 128 rows while retaining the 4-MiB object limit, 20-MiB producer
reservation and 64-binding verification working sets. It is not a production
format change: older readers reject larger cohorts, so adoption requires
explicit format compatibility and resource validation in addition to throughput
evidence. Canonical self-verification remains enabled.
The corrected external trial passes 269 node-level tests, retaining the original
64-Cell historical-read and 65-root recovery checks. Its new 128-Cell test checks
cold reconstruction and corruption rejection in the second bounded verification
chunk. Verification chunks follow historical object position to preserve read
coalescing. The load test nevertheless fences its owner after publication reaches
69 captures: receipt admission retained a separate hard-coded 64-capture limit.
The warm audit returns 503 for all 6,410 acknowledged writes, and no cold audit
runs. This is a failed availability/recovery qualification, not a throughput gain
or proof of acknowledged-state loss. Its artifacts remain retained. The next
external trial shares the publication bound with receipt admission and adds an
end-to-end 128-capture receipt, credit-release and shutdown regression.
The new regression first reproduces `Capacity("selected capture cohort")` on
the failed candidate. The corrected candidate passes all 270 node tests,
including that regression and a larger queued-producer case. Its verification
snapshot differs from the release source only in a test fixture's lookup of each
Cell's final submitted capture; production bytes and client/auditor identities
match. The corrected run completes 19,778.55 reads/s alone and 816.10 writes/s
plus 14,134.73 reads/s under mixed load. The mixed window still returns 31,527
write errors and 192 read errors and drops 39,477 write offers and 351,498 read
offers. All 98,520 ACKs pass independently reconciled warm/cold state and
original-retry checks; drain takes 44.38 seconds. Bucket lag at measurement
start is 148 frames, rising to 1,441 at the end. This improves the initial
publication backlog but establishes neither sustainable performance nor parity.
The larger batch remains an external, incompatible format experiment. The next
separate trial preserves foreground memory headroom when admitting background
root materialization, with one hard-budget root allowed for progress and the
existing shutdown admission unchanged. That trial passes 863 runtime tests
(4 ignored) but reduces mixed throughput to 770.52 writes/s plus 9,163.38
reads/s. Write errors fall to 3,484, while dropped writes rise to 70,285 and
dropped reads to 650,152. All 95,494 ACKs pass independently reconciled warm/cold
state and original retries. Root debt grows from 97.63 to 117.02 MB and drain
takes 48.65 seconds. Fewer returned errors do not establish an improvement;
this policy remains external. A follow-up tests skipping the full eligible-Cell
scan when sampled capacity cannot fit even the minimum root working set.
Actual reservations still recheck the current ledger atomically. The follow-up
passes 866 runtime tests (4 ignored), and its release source matches the tested
snapshot. Its fresh mixed run reaches 1,080.37 writes/s plus 16,034.03 reads/s,
but returns 31,426 write errors and drops 23,748 writes and 237,894 reads.
Read-only throughput is 17,373.42/s. All 112,838 ACKs pass independently
reconciled warm/cold state and original retries. Root debt grows from 93.15 to
300.62 MB, only 444 roots materialize during the measured minute, and drain
takes 56.66 seconds. Faster scheduling does not establish sustainable cleanup;
the combined experiment remains external and unqualified. Twelve Docker
samples inside the mixed window average 2.61 CPUs for the owner and 4.73 CPUs
for the fleet, so aggregate shared-VM CPU saturation is not established. A
repeat reaches 1,258.30 writes/s plus 16,238.52 reads/s, but still returns 22,163
write errors and drops 22,325 writes and 225,629 reads. All 125,519 ACKs pass
independent warm/cold state and retry audits; drain takes 56.56 seconds. Root
debt again grows, from 99.00 to 352.08 MB. Its intended CPU sampling collected
no samples because the process detector used a Docker `top` format without the
required PID column. The workload is retained, but it is not CPU-profile
evidence. The corrected collector has a verified process trigger and reports
unexpected detection errors instead of continuing silently.

The corrected collector records 2,488 and 3,930 user-CPU samples in two
windows, with no lost samples. Before the root-age threshold, the sampled
`blocks_commands` path accounts for 5.18% self CPU and its `Error` destructor
another 14.27%. Caller stacks lead through `dispatch` and `ready_since`.
`native_suffix_bytes` eagerly constructs a capacity error for each successful
locator addition. The candidate instead folds checked additions as `Option`
and constructs the same error once on overflow. Exact totals, memory budgets
and persisted formats stay unchanged; arithmetic tests cover both summation
and materialization-cost overflow. All 12 isolated verification routes pass,
including 2,074 workspace tests (43 ignored) and strict Clippy. The experimental
configuration passes 867 runtime tests (4 ignored). The profiled workload
itself reaches 1,155.20 writes/s and
14,884.40 reads/s, with 19,386 write errors and 31,286 write/306,906 read offers
dropped. All 118,699 ACKs pass independent warm/cold state and retry audits.
Sampling overhead excludes this run from performance qualification.

The unprofiled sum-fix trial reaches 18,396.40 reads/s alone and 1,102.97
writes/s plus 17,275.42 reads/s under mixed load. It returns 37,174 write errors
and drops 16,647 write offers and 163,354 read offers. All 117,718 ACKs pass
independent warm/cold state and original-retry reconciliation; drain takes
57.45 seconds. Root debt grows from 103.31 to 325.09 MB, with only 311 roots
materialized during the minute. The comparison does not establish sustainable
improvement. The next isolated trial retains the sum fix but removes the
experimental headroom admission and capacity precheck, returning to the
128-row baseline's root admission. This tests whether the headroom policy is
restricting cleanup; all publication, authority and hard memory gates remain.

That trial passes 860 runtime tests (4 ignored), then reaches 19,850.92 reads/s
alone but only 693.95 writes/s plus 13,450.03 reads/s under mixed load. It returns
37,575 write errors and drops 40,788 writes and 392,962 reads. Root debt falls
from 90.46 to 81.33 MB as 1,788 roots materialize. All 89,046 ACKs pass
independent warm/cold state and original retries; drain takes 47.86 seconds.
Restoring cleanup does not recover the target throughput. The shared 8-GiB VM
and Cellule-only 64-MiB retained-work admission remain diagnostic restrictions,
not the requested owner hardware contract. Further qualification needs explicit
owner/support resource separation and measured memory accounting; historical
profiles and their failed gates remain unchanged.

The first isolated-owner diagnostic uses a 12-vCPU/24-GiB VM, verified guest
CPU sets 0–7 for the 8-vCPU/16-GiB owner and 8–11 for support roles, and no
container swap. The same binary and 64-MiB work budget reach 19,686.80 reads/s
alone, then 735.40 writes/s plus 15,416.13 reads/s together. Mixed load returns
45,771 write errors and 487 read errors and drops 30,078 writes and 274,462
reads. Root debt falls from 103.36 to 68.29 MB. The warm audit checks all 95,687
ACKs and retries without error, but owner drain exceeds 120 seconds and cold
audit is not reached. Timestamped logs show materialization fencing about
39 seconds after drain begins, followed by a fenced heartbeat and repeated
node-log drain failure. Live scratch archives preserve 5,339 owner files and
both follower journals before forced cleanup; archives are not cold-recovery
proof. A FIFO authority mutex shared by heartbeat and per-Cell close work is a
renewal-starvation hypothesis requiring a direct reproducer. The planned larger
work-budget comparison is deferred while this failure is investigated.

An external timing-only build reproduces the failed drain on the same profile.
The last heartbeat waits 19.676 seconds for the authority mutex; a preceding
checkpoint releases it with only 50 ms left on the original lease. Close and
checkpoint queues reach 29.03 and 28.69 seconds respectively. The heartbeat's
directory refresh returns after terminal local expiry and correctly cannot
revive the guard. The candidate makes each authority operation recheck the
lease after acquiring the mutex and perform the existing signed refresh when
20 seconds or less remain. It keeps the 30-second lease, terminal fencing and
the original cached node-log closure proof. All 13 general verification routes
pass. Two fixture-backed regressions fail against the original boundary and
pass against the candidate: queued work preserves renewal, and a heartbeat
fenced while queued leaves the signed directory progress unchanged. Strict
Clippy also passes with these tests. A first candidate test attempt reused the
old native Cargo executable and is excluded; the passing candidate uses a
fresh target directory and compiled source. The workload rerun without the
authority timing probe fails after RustFS reaches its 2-GiB container limit
and is OOM-killed. Sampled provider memory rises from 417.9 MiB to 1.997 GiB;
the final sample precedes the kill by less than a second. Independent raw
journal reconciliation covers all 74,041 ACKs, but every warm state check
returns HTTP 503, no original retry is verified, and cold audit is not reached.
The bucket volume and owner/follower scratch archives are preserved. This is
an availability failure with recovery unverified, not evidence of lost ACKs
or a passing workload test of the lease fix. No throughput claim follows.

The pinned RustFS source already has container-aware runtime sizing and
object-cache memory accounting. Its cache configuration defaults to disabled;
the actual startup path still needs verification. Docker memory totals alone
cannot distinguish retained heap, active request buffers and charged kernel
memory. A fresh external probe keeps the binary, resources and workload fixed
and samples cgroup memory categories and process resident memory. Provider
configuration and the larger Cellule work-budget experiment remain unchanged
until the failure is understood. The probe reproduces exit 137 with Docker's
OOM flag set. Its final sample has 2,066,649,088 anonymous bytes, including
1,904,214,016 transparent-huge-page bytes, with only 39,079,936 file-cache bytes
and 41,275,392 kernel bytes left. Reclaiming file cache therefore does not
prevent the observed failure. This distinguishes anonymous-memory growth from
file-cache pressure, but does not yet distinguish live allocations from
allocator retention. Of 90,912 warm ACK checks, 6,943 fail and 83,969 original
retries are checked. The next external comparison changes only the provider's
`MIMALLOC_ALLOW_THP=0` setting; the owner and provider memory limits, Cellule
work budget, binaries, workload and correctness gates stay fixed.

That single-setting comparison completes with all 97,948 ACKs independently
reconciled against the raw journals and passing warm/cold state and original
retry checks. Owner drain takes 42.78 seconds. Sampled anonymous memory peaks
at 1,218,736,128 bytes, with no OOM kill; total cgroup memory still reaches
2 GiB as file cache occupies the remaining allowance. The harness now uses
the tested RustFS setting for both applications. Its 46 script tests and local
documentation-link checks pass. This short run supports the provider setting
and exercises the queued-renewal fix through drain, but does not qualify
long-run stability. Mixed throughput is 793.13 writes/s plus 14,275.65 reads/s,
with 35,801 write errors, 35 read errors, 36,589 dropped writes and 343,324
dropped reads. Performance parity remains unmet. The next diagnostic changes
only the retained-work allowance from 64 to 256 MiB, keeping the owner at
8 CPUs/16 GiB and preserving the publication and recovery gates.

The 256-MiB trial returns no measured request errors, but completes only
916.97 writes/s and 9,017.80 reads/s while dropping 64,946 writes and 658,711
reads. Scheduled p99 is 295.0 ms for writes and 159.5 ms for reads. Root debt
grows from 108.99 to 162.20 MB; selected roots advance by 1,550 during the
window. All 108,277 ACKs pass independent raw reconciliation and warm/cold
state and original-retry audits. Drain takes 45.42 seconds. Thus admitting
more work does not establish sustainable capacity. Sampled owner CPU averages
329.9% of one core within its eight-core allowance; this cannot distinguish
a busy serial coordinator from time waiting on storage. A fresh CPU profile
of this exact binary and configuration is the next diagnostic.

Inspection also finds the runnable SQL example's 16-MiB retained-work default
below the shared publisher's 20-MiB startup reservation. The example now uses
a named 64-MiB allowance; the external benchmark adapter still supplies its
explicit per-case budget. This repairs example configuration without changing
the frozen binaries used for either memory-budget diagnostic. Its fresh
verification snapshot passes all 13 routes, including 2,074 workspace tests
(43 ignored), 12 example tests (two fixture-dependent tests ignored), strict
Clippy, API docs, and the script and documentation checks. The benchmark
adaptation smoke check covers both runtime constructors. The earlier explicit
queued-renewal regression results remain separate from these default tests.

The next CPU observer times out while checking the Docker phase; the workload
continues and all 101,610 ACKs pass independent warm/cold reconciliation.
A recovered recording attaches to the same owner. Its initial five seconds
are bounded inside the measured window with about six seconds of end margin,
using recorded wall-time bounds and a host/guest clock-offset check. The full
recording crosses into audit and is excluded from load attribution; no early
window was recovered. In the retained subset, materialization dispatch costs
24.74% inclusive user CPU, with 16.31% self CPU in `blocks_commands`. The caller
stack runs through fleet candidate collection and `ready_since`.

The candidate now returns before collecting candidates when all eight root
slots are occupied, and treats successful `BundleSelectionReady` notifications
as changes to their own Cell. Existing timers, error handling, oldest-first
ordering, memory reservations and drain scans remain. This targets measured
CPU work without changing admission or durability policy. All 13 main-source
verification routes pass, including 2,074 workspace tests (43 ignored). The
Linux candidate is built with matching clients, images and workload fixtures;
its experimental 128-row runtime suites and doctest also pass (1,273 passed,
11 ignored). In the matched 256-MiB short diagnostic, successful mixed writes
rise 916.97→1,062.83/s and reads 9,017.80→10,654.25/s. Returned errors remain
zero, but 56,121 write offers and 560,598 read offers are dropped. Write request
p99 rises 217.7→239.8 ms; read p99 falls 82.0→64.8 ms. Root debt grows
118,847,699→217,716,863 bytes during the candidate's measured window, compared
with 108,988,873→162,200,876 bytes in the baseline. This is not sustainable
capacity or performance qualification. Independent raw reconciliation and both
warm/cold state and retry audits pass for all 120,236 ACKs; drain takes 46.24 s
and cold startup 38.01 s.

Twelve mixed-window resource samples show owner CPU averaging 287.5% of one
core, versus 329.9% in the baseline. Publication cohorts bounded by the two
tiered-through snapshots average 117.5 captures and 103.9 ms each, including
45.6 ms catalog loading and 40.2 ms encoding/upload. The first cohort can
straddle the initial snapshot; these are wall times, not CPU measurements.
The next investigation must separate catalog read count, bytes and latency
from scheduling and CPU, then evaluate bounded publication preparation and
pipelining without weakening the canonical selection CAS or recovery proof.


The fresh [integrated root-cut comparison](pr67-root-cut-measurement.md) includes
main PR #64 and separates selected capture cleanup from original root tasks.
At 2,000 offered Fleet writes/s across 2,000 Cells, successful throughput falls
566.45→276.62/s and successful scheduled p99 rises 373.10→1,010.32 ms. Celld
completes 1,985.03/s at 151.61 ms. The candidate passes all 42,210 warm ACK/retry
checks but fails the unchanged 120-second drain deadline; cold recovery is not
reached. Its retained memory falls while unpublished debt grows. All 16 isolated
code verification routes pass, but every performance profile fails. This short
combined-change pair cannot attribute the regression to root cleanup alone.
Fresh raw evidence and failed earlier attempts remain outside Git. PR #67 is
not ready to merge; acceptable improvement and architectural parity are unmet.

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
