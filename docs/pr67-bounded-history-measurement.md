# PR 67: historical reads within original admission

Candidate `90f099a295158c6effe51079d35db07eab204d17` groups fresh historical
ranges without increasing the original 20-MiB publication reservation. In one
original-budget Fleet pair, completed writes rise from **144.50 to 307.60/s**,
but successful tail latency worsens. A separately matched 1-GiB control reaches
237.67/s versus 181.45 before, with zero request errors and passing complete
warm/cold audits. These short overloaded diagnostics do not establish parity or
repeatable capacity. PR #67 remains a draft.

## Delivered change and reproduction

Selection still reads the complete new cohort from origin and compares every
byte with its proposal. After that match, it shares the proposal allocation and
reuses the released origin-buffer allowance for 2 MiB of historical read scratch
and at most 2 MiB of checked facts and planning metadata.

At most eight historical reads overlap. Compact indices group contiguous or
nearby ranges from the same object; padding cannot exceed useful union bytes.
Each frame keeps canonical digest, native envelope and Cell-scope verification.
Checked facts are validated in the original per-Cell order, including exact
command, transaction, checksum and final endpoints. No body or availability
cache survives the selection operation. Base dependencies remain freshly
verified on the original serial path. Individual extents above 2 MiB also use
the canonical serial verifier, avoiding an oversized semaphore acquisition.

The real 64-Cell regression fails in three baseline executions with 64 historical
reads; the candidate needs one. A separate held-read regression reproduces
serial I/O before and verifies overlap and cancellation after. Corrupt or missing
historical dependencies still prevent new authority selection. A deterministic
large-extent test verifies the serial route and exact cold payload/outcome
recovery. Maximum metadata, including vector growth and 256-KiB headroom, fits
2 MiB at 64 Cells × 256 locators. The existing 20-MiB receipt-pressure and FIFO
contracts pass unchanged. This does not restore the earlier experiment's extra
3-MiB admission, which caused its severe application regression.

## Fresh original-budget comparison

Baseline is `8bde901c7b5237c5f28f1da70231f1283f0f1b0f`; celld is
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. All cases use the same client,
auditor, fixtures and images: 1,000 uniform Cells, 96-byte SQL values with a
two-hour outcome ledger, 128 clients/queue slots, 15K offered writes/s,
30-second warmup and a 60-second window. Before/after request configuration and
execution metadata match, apart from source/binary and fresh namespace identity.
No build or contributor suite overlaps a timed window.

| System | Completed writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before | 144.50 | 837.55 | 814.04 | 349,995 | 541,292 |
| Candidate | 307.60 | 1,690.07 | 950.06 | 201,942 | 679,542 |
| celld | 4,622.43 | 144.82 | 73.54 | 0 | 622,398 |

TPS counts successes inside the window. Successful percentiles include trailing
measured successes and exclude errors/drops; scheduled time begins at offered
arrival. The independent replay also reconciles all attempt and drop counts.
Candidate has 18,456 in-window and 60 trailing successes; baseline has 8,670 and
43. Warmup request errors are 128 after versus 847 before. The 112.87% completion
difference is a single-pair observation; both successful latency measures worsen.
All-attempt percentiles are lower because they include many quick refusals and
cannot substitute for successful-command latency or zero-drop qualification.

| System | Complete ACK cohort | Warm audit | Cold audit | Joined drain s |
| --- | ---: | --- | --- | ---: |
| Before | 18,438 | all reads/retries pass | all reads/retries pass | 27.60 |
| Candidate | 33,817 | all reads/retries pass | all reads/retries pass | 27.21 |
| celld | 455,122 | all reads/retries pass | not reached | unverified |

Celld's owner is OOM-killed during shutdown, exit 137, after its passing warm
audit. Missing cold/drain gates do not themselves prove mutation loss. Cellule's
provider health and lifecycle observations pass; celld's cold/final provider
observations are incomplete. Every complete ACK count and journal reconciles.

## Matched larger-budget control

This pair changes only retained-work credit from 64 MiB to 1 GiB. Managed disk
remains 1 GiB; it uses the same workload, binaries and VM. Candidate ran before
this pair's fresh baseline. It is a separate diagnostic, not a substituted
qualification profile or matched celld resource-policy comparison.

| Code | Completed writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops | Complete warm/cold ACK cohort | Drain s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before | 181.45 | 2,523.49 | 1,473.94 | 0 | 888,857 | 19,924; all pass | 30.37 |
| Candidate | 237.67 | 2,348.86 | 1,459.35 | 0 | 885,484 | 29,284; all pass | 31.42 |

Completion rises 30.98%; scheduled p99 improves about 6.9%, while request p99
changes little. Both pairs remain overloaded, short and unqualified. These
observations support reducing selection work; they do not establish a repeatable
throughput/latency gain or meet any requested capacity target.

## Cost and remaining bottleneck

Original-profile GET/range attempts per in-window completion fall from 13.95 to
9.38; successful PUTs fall from 0.626 to 0.355. Materialized commands per root
rise from 11.72 to 16.90. The API counters include background cohorts, exclude
SDK retries and have window boundaries, so they are not exact per-command costs.
Total API work can increase as more writes complete. The conditional
215-command / 0.05-PUT target remains unmet.

In the error-free control, exact submission partitions are:

| Phase | Before mean ms | Candidate mean ms |
| --- | ---: | ---: |
| Local load | 0.193 | 0.270 |
| Ordered issuance lock | 651.079 | 485.908 |
| Publication capacity, with lock held | 5.386 | 4.141 |
| Assignment/enqueue | 0.00695 | 0.00851 |
| Complete submission | 656.665 | 490.328 |

Native credit, shipping-slot and validation means are below 0.001 ms. Every
phase count matches its successful assignment cohort and durations sum exactly.
The cohorts differ from in-window HTTP response counts; other worker/proof
timers are not additive to them. In the original pressure pair, mean lock wait
instead rises from 163.63 to 213.15 ms as the admitted population changes. Do not
combine these two budget profiles into a single latency conclusion.

The selector still gates the global native lane. Fresh base traversal remains
serial; repeated historical body verification, index work and root publication
remain expensive. Native shipping awaits one complete append round before the
next. Next work must bound and reduce those costs, preserve authenticated prefix
and availability witnesses, and separate native progress from recoverable
publication debt. Queue enlargement alone cannot increase sustainable selection
throughput. Failed-owner suffix recovery, safe collection, Bucket integration
and read/mixed qualification still require their complete evidence.

## Why the architecture still differs from celld

Both systems use per-Cell SQLite, LTX capture, fenced ownership and follower logs.
Their dependencies before a Fleet acknowledgement differ:

| Concern | Cellule candidate | Pinned celld reference |
| --- | --- | --- |
| Admission before native issuance | Global ordered lock reserves bounded publication capacity before assigning the ticket | Fleet capture/shipping loop proceeds independently of bucket waits |
| Follower rounds | A complete append round finishes before the next starts | Ordered member lanes allow multiple rounds in flight; credits apply in submission order |
| Follower commit | Canonical batched append and durable watermark | Ordered stream groups already delivered frames into one durable append batch |
| Publication work | Fresh bases and historical ranges are verified again; about 9.38 GET/range attempts per completed write in this window | Separate node-bundle upload and coverage work; the Fleet loop does not wait for it |
| Local SQLite policy | Managed sessions use WAL `NORMAL` with external durability proofs | Steady-state Cell connections also use WAL `NORMAL` |

The [Cellule issuance path](../crates/cellule-runtime/src/node/log_shipper/mod.rs)
holds `order` across `publication.reserve`; its shipper awaits each `append_batch`.
Celld's [Fleet loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4514)
uses ordered in-flight rounds and explicitly avoids bucket waits. Its
[follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160)
groups delivered frames before the durable append. Its
[Cell storage setup](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/storage.rs#L444)
uses WAL `NORMAL`; Cellule's managed sessions also select that policy. These
source differences explain the mechanism; they do not assign a measured
fraction of the total TPS gap to every difference.

In the earlier exact submission diagnostic, a 6.14-ms publication wait plus
0.007-ms assignment while holding the global lock implies about 163 serial
submissions/s, consistent with its 158.37 completed writes/s. This is a queueing
interpretation of that run, not a capacity forecast. The latest range grouping
reduces publication I/O, but leaves the dependency itself in place. Fleet ACKs
still need a complete recoverable issued range, bounded debt and joined drain;
simply bypassing publication admission would violate those contracts.

## Verification and limitations

All contributor routes and Rust 1.99 Clippy pass in an immutable source snapshot:
1,970 workspace tests and 60 local LTX tests pass; 38 environment-dependent tests
remain ignored. The new regressions pass three times after correction. All 1,299
Rust/Cargo files match that snapshot and the pinned production build source.

The shared ARM64 Docker VM has 8 CPUs and 8 GiB total RAM. Container ceilings
are 8 CPUs/16 GiB per node, with 4-GiB tmpfs, and exceed aggregate VM resources.
Only Cellule receives explicit retained-work and managed-disk limits; celld
metadata does not apply equivalent internal limits. No case qualifies a dedicated
8-vCPU/16-GiB node or physical-media durability. All five performance reports
fail qualification. There is no new Bucket or read-only measurement. The
revised 2,000-Cell / 2K-write / 20K-read objective and three matched five-minute
repetitions remain unqualified.

Raw sources, failed baseline tests, binaries, fixtures, verification, journals,
metrics, reconciliation and identity checks stay outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/bounded-history-20261008-*`,
including the two new baseline cases under the reused instrumented build.
The external evidence index and rehash certificate cover the retained inventory
and reused dependencies. No raw performance journal is committed.

[Submission bottleneck](pr67-submission-timing-measurement.md),
[withdrawn earlier experiment](pr67-historical-read-measurement.md),
[implementation](bundle-coverage-implementation.md),
[capacity contract](../crates/cellule-runtime/docs/write-performance-design.md).
