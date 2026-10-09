# PR 67: bounded catalog reads and fresh paired measurements

**Write throughput is higher in this pair, read throughput is lower, and
performance parity remains unmet.** The candidate completes 551.73 Fleet
writes/s versus 211.37 before and 1,999.88 for celld at 2,000 offered/s.
Read-only throughput is 16,631.43/s versus 18,778.07 before and 19,994.58 for
celld at 20,000 offered/s. Every case drains and passes complete ACK warm/cold
read and original-retry audits. Errors, drops and failed performance gates
remain. PR #67 is a draft.

## Reproduced scheduling bottleneck and change

Bundle preparation previously read selected catalog shards and requested
detached histories serially. Two regressions hold real metadata reads during
`prepare_node_bundle` for sixteen enrolled SQLite Cells. Both fail in each of
three before runs; both pass in each of three after runs.

The canonical point-lookup path now overlaps at most **eight raw shard bodies**
after its existing aggregate byte preflight. Those jobs join before a separate
phase overlaps at most eight requested raw history bodies. Authentication and
decoding remain serial and canonical. The original selected-shard plus
requested-history **4-MiB bound**, 20-MiB producer admission, authority checks,
proof policy, persisted formats and maintenance inventory path remain intact.
Planning retains at most 256 one-byte shard IDs and eight history indices.
No cross-operation availability cache is introduced.

The regressions enforce no ninth held read, no upload or authority after
cancellation, fresh successful retry and exact cold reconstruction of seed and
command outcomes. Previously verified metadata cannot authorize a later
preparation whose origin metadata is missing. This establishes bounded overlap
and preserved proof behavior; it does not establish application capacity.

The ranked hypotheses preceding measurement were serial metadata I/O,
remaining base/root verification cost, and provider/admission saturation.
The positive write-rate difference supports the scheduling hypothesis. The
remaining lock waits and adverse read result keep the other costs unresolved.

## Workload and identity

| Dimension | This diagnostic |
| --- | --- |
| Before | `d12eac881523cf00fcfdee9662e82e10c9627991` |
| Candidate | `98a368acdf51c7cb7af1e06eb544f08ab5a55f68` |
| Fresh celld control | `f2bf648663a610eefde71f3547ad61e9b896b1f0` |
| Population | 2,000 uniformly active Cells; every timed case verifies all 2,000 |
| Command | 96-byte SQL INSERT/SELECT values, two-hour durable request/result ledger |
| Fleet | One owner, two followers; local state and follower logs on tmpfs |
| Arrival and timing | 128 clients/queue slots, 30-second warmup, one 60-second window |
| Offered load | 2,000 writes/s or 20,000 reads/s, in separate cases |
| Cellule admission | Original 64-MiB retained budget, 1-GiB managed disk, 20-MiB producer reservation |

All six cases use fresh namespaces and provider volumes, the same immutable
runner, client/auditor binaries, fixture settings and images. Builds and
contributor suites finish before all timed windows. Both managed SQLite write
paths already use WAL NORMAL. The candidate has no diagnostic timing overlay.

The shared ARM64 Docker VM has **8 CPUs and 8 GiB total RAM**. Owner and follower
ceilings are each 8 CPUs/16 GiB, provider 2 CPUs/8 GiB and client 4 CPUs/4 GiB;
aggregate ceilings exceed VM capacity. Internal policies remain asymmetric.
The host has 12 logical CPUs/32 GiB, another running 8-CPU/16-GiB Colima profile,
and 5,107 MiB of swap used at environment capture. No VM settings change within
this comparison. These observations do not isolate interference or qualify
the specified dedicated 8-vCPU/16-GiB serving node.

## Actual rates, latency and failures

TPS counts successful completions inside the window. Table latency covers
successful responses, including trailing completions; errors and drops remain
failures. The canonical delivery gate also retains all-attempt histograms.

| Point | Successes/s | Scheduled p99 ms | Request p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cellule write, before | 211.37 | 8,087.95 | 6,458.85 | 40,094 | 67,168 |
| Cellule write, candidate | 551.73 | 566.39 | 342.85 | 38,642 | 48,068 |
| celld write | 1,999.88 | 16.16 | 14.85 | 2 | 0 |
| Cellule read-only, before | 18,778.07 | 21.93 | 12.14 | 0 | 73,289 |
| Cellule read-only, candidate | 16,631.43 | 27.33 | 13.15 | 0 | 202,055 |
| celld read-only | 19,994.58 | 3.86 | 2.71 | 0 | 300 |

Write rate is **2.61 times** the fresh before observation. Read rate is
**11.43% lower**. The canonical matched read guardrail fails: throughput ratio
0.886 and all-attempt scheduled-p99 ratio 1.245, outside its 0.90/1.20 bounds;
both Cellule arms also drop offers. A single short pair establishes neither a
repeatable gain nor a repeatable regression attributable to this change.
The previous observation from the same before binary was 339.30 writes/s;
that separately recorded run illustrates variance and is not this paired control.
These offered points do not establish either system's maximum capacity.

All Cellule write errors preserve HTTP 503 (`Cell is temporarily unavailable`).
Celld's two timed and three warmup errors preserve HTTP 400 (`request identity
expired or not yet valid`); they are not omitted from qualification. Write
warmup errors/drops are 42,732/12,898 before, 14,801/24,555 candidate and 3/0
celld. Read warmup drops are 406,792 before, 448,045 candidate and 4,407 celld,
with zero read errors. All six setups complete; the earlier initialization
failure remains in its [original report](pr67-closure-admission-measurement.md).

| Case | Complete ACK cohort | Warm reads / original retries | Joined drain | Cold reads / original retries |
| --- | ---: | --- | ---: | --- |
| Cellule write, before | 19,109 | All pass | 54.28 s | All pass |
| Cellule write, candidate | 55,935 | All pass | 46.59 s | All pass |
| celld write | 181,996 | All pass | 17.55 s | All pass |
| Cellule read-only, before | 2,001 | All pass | 32.65 s | All pass |
| Cellule read-only, candidate | 2,001 | All pass | 37.27 s | All pass |
| celld read-only | 2,001 | All pass | 4.69 s | All pass |

Cohorts include seeds, contract checks, warmup and trailing successful writes.
Journals, per-Cell distribution, receipt identity, expected outputs and ACK
counts reconcile independently. Every ACK mutation and original retry passes
both audits, with zero errors or changed incarnations. Lifecycle success does
not qualify the failing performance points.

## Remaining architectural gap

| Submission phase, mean ms | Before | Candidate |
| --- | ---: | ---: |
| Validation, native-byte and shipping-slot admission | 0.00053 | 0.00043 |
| Local load | 0.13732 | 0.09691 |
| Global ordered-lock wait | 367.45810 | 117.14341 |
| Publication capacity with lock held | 3.78531 | 1.37390 |
| Ticket assignment and enqueue | 0.00587 | 0.00728 |
| Complete submission | 371.38713 | 118.62193 |

All eight phase counts reconcile with 12,682 before and 33,168 candidate
successful assignments; the partition residual is zero. The candidate still
spends **98.75%** of submission time waiting for the global ordering lock.
[`assign_capture`](../crates/cellule-runtime/src/node/log_shipper/mod.rs)
reserves publication capacity under that lock before follower shipping. Mean
SQL-worker, capture and follower-proof timers are 0.21, 0.14 and 20.98 ms in
their respective overlapping cohorts; they are not additive to this partition.

Windowed GET/range attempts per completed write are 5.97 before and 7.01
candidate; successful PUTs are 0.350 and 0.330. Materialized commands/root
are 4.45 and 13.07. These include background work and exclude SDK retries;
they are not exact command costs. Overlap changes scheduling, not an operation's
request count. A total request-amplification reduction is not demonstrated.

Candidate pending publications grow 1,463→2,405 and retained bytes
63,881,005→66,341,791 against 67,108,864 available. Unpublished node-log bytes
grow 17,767,294→54,088,305; oldest unpublished age is 45.09→54.50 seconds.
Two endpoints do not establish a sustained debt slope. Read-only windows still
materialize seed debt: 897 roots before and 973 candidate. Their lower read
rate and host interference require controlled follow-up, not causal attribution
from this pair alone.

Celld's [Fleet loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4514)
pipelines ordered rounds independently of bucket publication. Its
[follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160)
groups already-delivered append frames before durable append. Cellule awaits
each append batch. Similar storage components still have different critical
paths; WAL NORMAL alone does not close this gap.

Next work is bounded authenticated root/checkpoint verification, recoverably
bounded separation of native admission from publication debt, ordered follower
pipelining and diagnosis of read regression and overload availability. Bucket
adapter integration, mixed load, safe collection and full fault lifecycle remain
unfinished. Qualification still requires three matched five-minute repetitions,
zero errors/drops, Fleet p99 ≤50 ms, Bucket p99 ≤200 ms, bounded debt and every
ACK surviving cold recovery on the specified serving node.

## Verification and retained evidence

All contributor routes pass in the exact frozen candidate: **1,980 workspace
tests**, 60 local LTX tests, 94 bundle tests, both new regressions in three
repeats, Rust 1.97/1.99 Clippy with warnings denied, all-feature checks, format,
API docs and boundary/layout/document/SQL-peer gates. The 38 ignored tests
retain their documented environments. All **1,312 Rust/Cargo files** match
the measured Linux build and final production source. Later changes are this
report and status text. Initial compile/lint/build-identity failures are retained;
the first formatting-mismatched build is unused for measurement.

Raw source, build identities, binaries, failures, journals, metrics, audits and
rehash inventories remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/catalog-overlap-20261009-*`.
The inventory rehashes the preceding frozen comparisons as well. All six
canonical reports and their comparison remain explicitly unqualified.

[Implementation](bundle-coverage-implementation.md),
[capacity contract](../crates/cellule-runtime/docs/write-performance-design.md).
