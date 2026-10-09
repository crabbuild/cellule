# PR 67: receipt admission and architecture diagnosis

**Performance parity remains unmet.** Candidate `addc1cecefb42a094bc6a1c42289d43334c0e645`
fixes a reproduced producer pressure failure. A fresh Fleet diagnostic completes
154.72 writes/s versus 122.97 before and 4,001.65 for celld. All three fail
qualification and warm ACK audit. One overloaded pair establishes neither a
repeatable performance improvement nor either system's sustainable capacity.

## Reproduced failure and change

The producer selects durable coverage, then reserves receipt metadata. An older
root can retain the remaining credit until its checkpoint callback joins.
Previously, exhausting that credit terminated the producer and fenced the node.
The real producer/checkpoint regression fails three times before the fix: the
first observes `Shared(Capacity("resource ledger"))`; two repeats observe its
closed checkpoint channel. Initial test/build mistakes are retained externally.

The candidate admits all receipt metadata atomically. When credit is unavailable,
it retains the original selected cohort and services canonical checkpoint
callbacks while waiting. It does not repeat durable selection. It transfers the
admitted credit into shared receipts and rechecks the original live gate and
lease before confirming coverage. The synchronous confirmation API retains its
immediate-capacity contract. The producer's 20-MiB working credit, 32-MiB regression
budget and 64-MiB application budget remain unchanged.

The regression passes three times after the fix, including a real intermediate
checkpoint, exact later receipt, cold SQLite results and zero retained credit
after joined closure/shutdown. All eleven contributor checks and Rust 1.99
Clippy pass in the immutable final verification snapshot: 1,963 workspace tests
and 60 local LTX tests pass; 38 environment-dependent tests remain ignored.
Production Rust/Cargo bytes match the measured pinned commit. Documentation
corrections also pass syntax and link checks.

## Fresh Fleet diagnostic

Baseline: `11843f6cfb97ea86fb2647a37478039dbfbe0f54`.
Celld: `f2bf648663a610eefde71f3547ad61e9b896b1f0`.
The three sequential cases use identical clients, auditors, fixture bytes and
images: 1,000 uniform Cells, 96-byte SQL values, INSERT plus SELECT and a two-hour
outcome ledger, 128 clients/queue slots, 30-second warmup, 60-second window and
15K offered writes/s. No build or contributor suite overlaps a timed window.

| System | Completed writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cellule baseline | 122.97 | 3,087.23 | 1,955.00 | 594,959 | 297,663 |
| Cellule candidate | 154.72 | 2,754.51 | 1,546.26 | 497,981 | 392,480 |
| celld | 4,001.65 | 88.31 | 37.53 | 113,787 | 546,114 |

TPS counts successes completed inside the window. Successful p99 includes
trailing measured successes and excludes errors/drops; scheduled latency starts
at offered arrival. Qualification also checks all-attempt latency. Candidate
has 9,283 successes inside and 256 after the window. The observed completion
increase is 25.82%; it is not an accepted sustainable throughput gain.

| System | Complete ACK cohort | Warm errors / completed retries | Cold audit | Qualified drain |
| --- | ---: | ---: | --- | --- |
| Cellule baseline | 16,664 | 3,546 / 13,118 | not reached | unverified |
| Cellule candidate | 20,098 | 5,318 / 14,780 | not reached | unverified |
| celld | 469,993 | 452,930 / 17,063 | not reached | unverified |

The warm failure fraction worsens from 21.28% to 26.46% in this pair. Higher
completion does not establish an availability improvement.

Sampled Cellule audit failures are retry POSTs after a matching GET, indicating
an availability gap for existing authenticated outcomes under pressure. The
summaries do not classify every failure by stage. Both Cellule owners exit zero
and log 1,000 Cell drains during cleanup; neither logs a fatal producer error.
That does not replace the missing controlled drain and cold audit. Celld owner
is OOM-killed, exit 137. Missing required after/cold provider observations also
fail qualification. None of these missing gates by itself proves mutation loss.

The shared ARM64 Docker VM has 8 CPUs and 8 GiB total RAM. Node containers have
8-CPU/16-GiB ceilings and 4-GiB tmpfs; client/provider ceilings also exceed VM
resources. The runner supplies the 64-MiB retained-work and 1-GiB managed-disk
limits only to Cellule. Recording their values in celld case metadata does not
configure equivalent internal limits. This is workload-matched evidence with
asymmetric resource policies, not dedicated standard-node or device durability
qualification. Earlier descriptions of resource parity were too strong.

## What still costs time

Window-only canonical provider observations count all endpoint storage API
attempts, with successful in-window writes as the denominator. SDK-internal
retries and trailing publication are excluded; background work may cover other
command cohorts.

| Observation | Baseline | Candidate |
| --- | ---: | ---: |
| GET/range attempts per completed write | 13.82 | 14.32 |
| PUT attempts per completed write | 0.7373 | 0.5212 |
| Materialized commands per selected root | 10.11 | 13.57 |
| Retained work at window end, MiB / 64 MiB | 58.39 | 27.62 |
| Oldest publication debt at window end, ms | 47,115 | 35,995 |
| Mean capture / worker / Fleet proof, ms | 0.241 / 2.268 / 7.912 | 0.262 / 2.595 / 7.678 |
| Mean Fleet response, ms | 464.37 | 367.36 |
| Mean background publication phase, ms | 59,825.96 | 63,587.41 |

Different overlapping cohorts produce these timings; they cannot be added as
CPU cost. Background publication age includes delayed root scheduling and is
not command ACK latency. Bundle response counts are zero: Fleet wins the live
response race. The shared producer still supplies background coverage/cleanup.
The pressure fix is reproduced, but its fatal failure is absent from both fresh
application runs, so the completion difference cannot be isolated to that branch.

Celld [bundles dirty Cell tails and pipelines ordered shipping](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4411)
and [groups delivered follower appends before fsync](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160).
Cellule already uses managed SQLite WAL NORMAL, but still awaits one shipping
batch before the next and revalidates live historical/base dependencies during
selection. Bucket benchmark wiring bypasses the managed producer and publishes
per Cell. No new Bucket or read-only capacity measurement is made here.

Next work must let existing read/retry outcomes remain available during pressure,
reserve materializer/cleanup progress before foreground admissions consume
credit, and reduce repeated dependency work while preserving exact authenticated
range, base and recovery contracts. Then measure ordered follower pipelining
and Bucket shared selection. Qualification still requires three matched
five-minute repetitions, zero errors/drops, all-ACK warm/cold recovery, bounded
debt, complete failed-owner suffix recovery, safe collection and read/mixed
profiles. The 2,000-Cell / 10K-write / 50K-read target remains unqualified.

## Evidence

Raw sources, binaries, failed attempts, journals, provider observations and
independent replay remain outside Git at
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1` under
`receipt-admission-20261008-*`, plus the fresh baseline case in the retained
baseline build directory. Every offer/attempt/completion and complete ACK cohort
reconciles independently. See the external evidence index and verification
certificate for hashes and the complete retained inventory. The index contains
12,460 files / 1,843,123,095 bytes; every entry independently rehashes. Its SHA-256
is `ede0f3083302324f1b4eee8a05bf501127eb156a7035d1ad981185f2e81c7974`.

[Historical-read experiment](pr67-historical-read-measurement.md),
[bundle implementation](bundle-coverage-implementation.md),
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md).
