# PR 67: compose checkpoints with native publication

**Write parity remains unmet; PR #67 remains a draft.** The corrected candidate
completes 468.80 successful Fleet writes/s versus 407.03/s in its fresh baseline.
Successful scheduled p99 is unchanged at about 389 ms. The candidate passes all
53,527 warm/cold mutation and original retry checks and joins its drain. Errors,
dropped offers, growing publication debt and expensive historical reads still
fail performance acceptance. This short pair establishes an observation, not a
repeatable capacity improvement.

## Implemented behavior

Ready exact materialized checkpoints and new complete captures now share one
fresh catalog load, upload and fenced native-selection CAS. The canonical
checkpoint validator checks original authority, scope, pin, root endpoint and
identical historical prefix before pruning only the covered locators. Native
selection retains fresh origin matching, base/history verification and original
lease checks. No origin-availability cache crosses operations or grants an ACK.

The producer reserves rows for the entire ready callback cohort before filling
the native batch. All original callbacks complete after successful canonical CAS,
before receipt admission; coalescing newer same-pin notifications retains every
callback. The bound remains 64 native frames plus checkpoint notifications, with
4 MiB native bytes and the existing 20 MiB working reservation. Idle, full-capture,
actual receipt-pressure and shutdown paths still join standalone checkpoints.
Ready receipt credit wins over unrelated callbacks; lease fencing wins over both.

This extends the existing admission and shipping changes in the
[native pipeline](pr67-native-pipeline-measurement.md): publication capacity waits
precede global ordering, eight original rounds use independent ordered member
lanes, and delivered frames use canonical follower group commit. Each member
still awaits one grouped RPC at a time. Celld's
[ordered shipping](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs#L4530),
[ordered receiver/group commit](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L179)
and [new-entry upload](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs#L6628)
remain architectural references; Cellule does not yet implement their full cost
and concurrency model. No celld source is copied in this change.

## Fresh matched diagnostics

Each case uses 2,000 uniform Cells, 96-byte SQL values, the same persisted
request/result ledger, 128 clients and queue slots, one owner and two followers,
mTLS, WAL NORMAL, tmpfs and pinned RustFS. Offered load is 2,000 writes/s with no
reads, 30-second warmup and a 60-second measured window. Driver, auditor, fixture
bytes, images and workload manifests match. All original build and test handles
join before timed runs; independent replay runs after the complete run controllers
join. No contributor suite, build or independent replay overlaps timed windows.

Baseline serving revision `cf4785c675698afe786f566242d6d3dace32e775` has production
bytes identical to retained `a4ad3401e4d398363c54d000a279d010ddba3abc`. Initial
composed candidate `beda5dbf99c540e92d017c5b434fae2218d5a8d0` reserves only one ready
checkpoint row before native assembly. Corrected candidate
`80caaeab6d935a93b16803475a98449beb52e265` reserves the entire ready cohort. Each
comparison has a freshly executed baseline and celld `f2bf6486`; the corrected
run order is candidate, baseline, celld. Neither comparison is a repeated A/A
variance study. Documentation-only delivery commits preserve measured source.

| Comparison | Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial | Cellule baseline | 364.02 | 261.94 | 79,807 | 18,251 |
| Initial | Composed candidate | 328.98 | 334.94 | 61,020 | 39,239 |
| Initial | celld | 1,949.30 | 213.77 | 0 | 3,012 |
| Corrected | Fresh Cellule baseline | 407.03 | 389.17 | 53,131 | 42,447 |
| Corrected | Ready-cohort candidate | 468.80 | 389.62 | 42,229 | 49,517 |
| Corrected | Fresh celld | 1,973.50 | 34.00 | 0 | 1,585 |

TPS counts successful completions inside the measured window. Successful
scheduled p99 is reconstructed from successful measured offers through client
drain, excluding fast failures; request p99 for the corrected baseline/candidate
is 209.50/209.38 ms. The initial candidate loses 9.62% TPS and worsens p99 by
27.87%. The corrected candidate gains 15.17% observed TPS over its fresh baseline;
p99 rises 0.12%, so there is no demonstrated latency improvement. The same
baseline binary yields 364.02 and 407.03/s, and celld p99 varies substantially:
these runs cannot establish a repeatable causal gain or maximum sustainable TPS.

Independent replay reconciles all planned offers, attempts, errors, drops,
trailing completions, per-Cell successes, payload bytes, complete ACK provenance
and every original successful response. All 2,000 Cells have measured successes.
The corrected candidate has 28,128 in-window successes and 126 trailing successes.

| Comparison/arm | Complete ACK cohort | Warm audit | Cold audit and joined drain |
| --- | ---: | --- | --- |
| Initial baseline | 42,864 | 12,573 HTTP 503s | Not reached |
| Initial candidate | 41,984 | All mutations/retries pass | All pass; 71.51 s |
| Initial celld | 178,731 | All mutations/retries pass | All pass; 25.49 s |
| Corrected baseline | 49,878 | 187 HTTP 503s | Not reached; owner cleanup times out |
| Corrected candidate | 53,527 | All mutations/retries pass | All pass; 54.80 s |
| Corrected celld | 180,416 | All mutations/retries pass | All pass; 19.85 s |

HTTP 503 audit failures establish failed availability, not acknowledged-data loss.
Both Cellule candidates complete exact cold restoration and all original retries.
No case passes qualification; even celld drops offers in these runs.

## Cost and debt findings

In the corrected comparison, node-authority PUT starts per successful write fall
0.08017 to 0.06303 (21.38%), while total observed store bytes per success rise
72,635 to 78,940 (8.68%). Node authority accounts for 60,269/66,650 bytes per
success. All operation families, including reads and coordination, are included;
these ratios normalize a concurrent window and are not per-request tracing.
Fewer catalog transactions do not establish lower total publication cost.

Native submission's seven phase totals reconcile with its complete phase cohort.
Mean global-order wait is 0.00106/0.00050 ms for baseline/candidate; publication-slot
wait is 0.00040/0.00036 ms. The separate Fleet-proof cohort averages 69.48/71.34 ms,
and follower frames/sync are about 11.39/10.54. These overlapping caller waits
cannot be added as serial service time or attributed causally to batching.

The corrected candidate retains 2,000 active Cells and logs no owner-capacity
fences; the baseline falls to 1,991 and logs nine. All candidate Cells drain idle;
the baseline warm audit fails and cleanup does not join normally. Candidate
pending publication count falls 2,973 to 2,102 and retained captures fall 2.45 to
0.71 MB, but oldest age rises 51.74 to 62.80 seconds and unpublished node-log bytes
rise 28.33 to 47.17 MB. Retained RAM rises 53.63 to 64.52 MB. Endpoint observations
and a short window do not prove bounded sustained debt. Shared root-packing
counters are zero: materialized roots remain per Cell.

## Verification and remaining delivery

All contributor gates pass on an isolated snapshot of the measured source: all
features/targets, full workspace tests, local LTX, Rust 1.97/1.99 Clippy with
warnings denied, API docs, boundaries/layout, Rust fences/links, SQL/peer
contracts and script tests. The workspace reports 1,999 passing test/doctest
executions (including child reports), zero failures and 38 environment-dependent
ignores; local LTX reports 60 passes. All 97 bundle tests, 18 shipping tests and
three repeated six-test composed suites pass. Final documentation changes
receive separate syntax/link/format checks and preserve measured production bytes.

The component fixture first fails with four PUTs and passes with two, with exact
expected-root/cold-image equality. Additional tests cover eight independent
roots, retained later suffixes, stale endpoints, count overflow, origin loss and
lease fencing. The real producer test joins queued checkpoints, complete native
ranges and shutdown with zero retained credit. The saturated-native test first
fails with a largest cohort of seven out of eight ready callbacks; the corrected
scheduler joins all eight in one selection and passes repeated runs without
changing its deadline or recovery assertions. The initial experiment and external
analyzer-name-filter failures remain recorded separately; production, workload,
profiles and expected evidence are not weakened.

This is an 8-vCPU/approximately-8-GiB shared Docker VM, not a dedicated owner with
8 CPUs and 16 GiB plus isolated followers/store. Container ceilings of 8 CPUs and
16 GiB do not supply those resources. Cellule's 64 MiB retained and 1 GiB managed
disk budgets remain unchanged; celld's internal budgets are not matched. Tmpfs
and NORMAL do not qualify physical device/power-loss durability. These limits
prevent extrapolation to the laptop KV result or production capacity.

Remaining delivery prioritizes lower total catalog/history bytes and verification
work using exact authenticated proofs, measured bounded materialization admission,
ordered transport with multiple frames in flight and follower group commit, and
warm overload availability. Acceptance still requires three matched repetitions
of at least five minutes, zero errors/drops, target latency/throughput, bounded
memory/debt, all-ACK fault recovery and read-only/mixed guardrails. Read throughput
and fault qualification are not measured by these write-only diagnostics.

Raw evidence, failed attempts, source manifests, binary hashes and original logs
remain outside Git at
`/Volumes/Workspace/crabbuild-target/native-composed-selection-20261009`.
The complete frozen hash index records their provenance; mutable compiler caches
and disposable independent SQLite replay indices are excluded.
