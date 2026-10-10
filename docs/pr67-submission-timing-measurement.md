# PR 67: publication stalls the global issuance lane

**The measured bottleneck is a publication wait inside the global issuance
lock.** Instrumented revision `8bde901c7b5237c5f28f1da70231f1283f0f1b0f`
completes 158.37 Fleet writes/s. Across 9,719 successful native submissions,
mean submission time is 733.65 ms: 727.30 ms waiting for the ordered lock and
6.14 ms waiting for publication capacity while holding it. This is diagnosis,
not a delivered throughput improvement. PR #67 remains a draft.

## Measurement and limits

The unchanged SQL-ledger diagnostic uses 1,000 uniform Cells, 96-byte values,
128 clients/queue slots, 15K offered writes/s, 30-second warmup and a 60-second
window. Retained credit is 1 GiB; managed disk remains 1 GiB. Client, auditor,
fixtures and images match the previous large-credit control. No build or test
suite overlaps the window. Managed SQLite already uses WAL NORMAL.

| Result | Observation |
| --- | ---: |
| Successful in-window writes | 9,502; 158.37/s |
| Successful trailing writes | 256 |
| Request errors | 0 |
| Dropped measured offers | 890,242 |
| Successful scheduled p99 | 3,951.43 ms |
| Successful request p99 | 2,401.45 ms |
| Complete ACK cohort | 19,675 |
| Warm/cold reads and original-outcome retries | All 19,675 pass in each audit; zero errors |
| Joined original Fleet drain | 31.44 s |

Every offer, attempt, completion and complete ACK count reconciles independently.
Provider health and lifecycle checks pass. Performance qualification fails.
The prior same-profile candidate observation was 191.08/s: this new unpaired run
is 17.12% lower, with worse tail latency. It establishes neither an improvement
nor a repeatable regression attributable to instrumentation.

The shared Docker VM has 8 CPUs and 8 GiB total RAM; container ceilings exceed
that capacity. This is not dedicated 8-vCPU/16-GiB standard-node qualification.
There is no fresh paired celld, Bucket or read-only result in this slice.
The previous celld observation, 4,473.32/s, has asymmetric internal resource
policies, dropped offers and failed warm recovery; it is not qualified capacity.

## Exact submission partition

All eight histograms have 9,719 observations, matching successful assignment
callbacks. Their summed phase durations equal the total exactly; no failed or
cancelled submissions are observed in this window. This cohort includes
assignment completions across the window boundaries and differs from the
9,502 in-window HTTP responses. It excludes SQL, later follower proof and reply.

| Submission phase | Mean ms |
| --- | ---: |
| Validation | 0.00014 |
| Native byte credit | 0.00025 |
| Shipping slot | 0.00029 |
| Local load and validation | 0.19809 |
| Ordered issuance lock | 727.30445 |
| Publication slot, with lock held | 6.14275 |
| Ticket assignment and enqueue | 0.00702 |
| Total | 733.65298 |

The ordered-lock wait accounts for 99.13% of submission time. Publication-slot
waits sum to 59.70 seconds in this completion cohort. A serial 6.15-ms
publication/assignment interval permits approximately 163 submissions/s,
consistent with the observed 158.37 HTTP completions/s. This is a queueing
interpretation, not a prediction of capacity after a future change.

[`assign_capture`](../crates/cellule-runtime/src/node/log_shipper/mod.rs)
holds the ordered mutex across publication-slot reservation, then issues the
ticket and enqueues both consumers. A slow selector therefore queues every
writer before follower proof starts. The queue deliberately prevents sequence
gaps and bounds original publication debt; removing those contracts is not a fix.
Moving the same capacity wait outside the mutex alone would move the measured
wait without increasing the selector's sustainable consumption rate.

Background cost remains high: 14.40 GET/range attempts and 0.618 successful PUTs
per in-window completion; 11.43 materialized commands per root. These API counters
include background cohorts and exclude SDK retries, so they are not exact
per-command costs. Historical ranges and base dependencies are checked again
for each affected binding. Mean worker and Fleet-proof timers are 2.05 and
7.40 ms in their respective overlapping cohorts; they are not additive to this
submission partition.

Celld's [Fleet loop](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4514)
explicitly avoids bucket waits and pipelines ordered rounds. Its
[follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L160)
groups already delivered appends before fsync. Sharing SQLite, LTX and follower
logs does not imply the same scheduling, proof work or acknowledgement path.

The next implementation must lower selection I/O and safely separate original
native progress from publication debt, with bounded recoverable backlog,
complete issued-suffix recovery and joined drain. Pipeline follower rounds after
addressing this dominant pre-proof queue. Qualification targets stay unchanged.

## Verification and evidence

The instrumentation preserves admission, ticket order, byte limits and durability
policy. Cancellation telemetry is exercised by the real full-publication-queue
regression, including no sequence gap and zero retained credit. All contributor
routes pass in an immutable snapshot: 1,965 workspace tests, 60 local LTX tests,
both Rust 1.97 and 1.99 Clippy, docs, layout, boundaries and document/peer gates.
The 38 environment-dependent ignored tests remain unqualified.

Raw source, binaries, fixtures, verification, metrics, journals and independent
reconciliation stay outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1/submission-timing-20261008-*`.
The external index and rehash certificate record the complete evidence inventory.

[Previous replay-pressure comparison](pr67-replay-admission-measurement.md),
[implementation](bundle-coverage-implementation.md),
[capacity contract](../crates/cellule-runtime/docs/write-performance-design.md).
