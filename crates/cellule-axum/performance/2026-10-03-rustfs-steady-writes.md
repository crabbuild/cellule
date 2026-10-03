# Sustained RustFS HTTP writes

The compaction pipeline is faster, but this comparison does **not** establish a
consistent improvement in write throughput or HTTP tail latency. The current-main
confirmation is still running. Keep the optimization goal open.

## Scope

[Completed CI run](https://github.com/crabbuild/cellule/actions/runs/37159544003)
compares baseline `4c627d3fdc95fbb86558088927aec72f51d0046c` with candidate
`f65712e7b4b527754f8dc0089389d7a74ffbae5e`. Both release binaries use identical
example telemetry. The runner has four logical CPUs, Ubuntu 24.04, Rust 1.97.0,
and the RustFS image pinned in the workflow. Builds finish before measurements.

The matrix has three alternating baseline/candidate pairs per Cell count,
four SQL workers, four HTTP Tokio workers, and sixteen clients. Each point uses
five seconds of timed warmup followed by 120 seconds of closed-loop POST load.
Each POST inserts a small order and performs a receipt-bound SELECT. Throughput
includes the drain of requests admitted before the deadline. Response throughput
counts JSON response-body bytes, not database bytes or total network traffic.

[Full data](2026-10-03-rustfs-steady-writes.json) retains every pair, phase
histogram summary, CPU/RSS measurement, interval stability, binary hashes, and
raw-client ledger hashes. CI retains the original replies and service logs.

## Results

The percentage changes below are medians of the three **paired ratios**. Lower
latency is better. They are not ratios of independently selected medians.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 60.25 | 60.99 | +2.6% | +0.9% | +4.6% |
| 4 | 99.58 | 97.86 | -0.2% | -3.8% | -3.0% |
| 16 | 103.72 | 104.00 | +0.2% | -2.1% | +0.5% |

| Cells | Repeat | TPS change | p95 change | p99 change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | +1.2% | +0.9% | +4.6% |
| 1 | 2 | +2.6% | +1.4% | +4.9% |
| 1 | 3 | +7.3% | -8.6% | -9.3% |
| 4 | 1 | -0.2% | -3.8% | -4.8% |
| 4 | 2 | +0.4% | -4.2% | -3.0% |
| 4 | 3 | -14.8% | +14.2% | +12.5% |
| 16 | 1 | +0.2% | +10.4% | +2.4% |
| 16 | 2 | -2.4% | -2.1% | +0.5% |
| 16 | 3 | +0.3% | -13.5% | -4.7% |

The third four-Cell pair regresses substantially and is retained in the analysis.
Its cause is unresolved. Three repeats do not provide a statistical confidence
interval, and the modest median gains must not be presented as established gains.

## Bottleneck evidence

Median paired compaction mean durations fall by 11.0%, 13.3%, and 5.1% at
1, 4, and 16 Cells respectively. The improvement does not translate into
consistent end-to-end gains. Throughput largely plateaus between four and
sixteen Cells, while preparation time rises sharply.

Baseline preparation averages approximately 10 ms at one Cell, 24 ms at four,
and 128 ms at sixteen. Authority publication averages approximately 5, 12,
and 21 ms respectively. Server CPU usage is approximately 0.3, 0.5, and 0.5
cores; the driver uses at most approximately 0.05 cores. CPU measurements alone
do not distinguish provider latency from framework admission waits.

`Host::default` derives dirty-memory admission from Rust's available CPU count;
it would admit four jobs if all four runner CPUs are available to the process.
Root preparation retains a slot through immutable uploads. This is a candidate
explanation for the sixteen-Cell wait, not yet a measured attribution. The next
diagnostic must record effective capacities and separate admission wait from
admitted preparation and provider operations. Increasing admission
ceilings alone would change the resource budget rather than prove a framework
efficiency improvement.

Phase telemetry includes warmup. Worker and queue histograms also include exact
replay verification; they cannot be treated as measurements of SQL mutations
alone. Compaction is a subset of preparation, so those durations are not additive.

## Durability coverage

All eighteen points pass live and fresh-SQLite cold-recovery verification for
all 199,761 acknowledged rows and exact retries, including warmup. The measured
windows complete 190,954 successful writes. The harness verifies contiguous
per-Cell receipts, conflicting and expired identities, exclusive ownership,
authority drain, a new recovery fence, and the next independently recorded write
in every Cell. There are no write errors in the measured windows.

An independent raw-artifact audit rechecks timed client ledgers, unique request
identities, reply contents, per-Cell sequence coverage, drained sequence counts,
new recovery fences, and the TPS/percentiles computed from raw replies. The JSON
report records 199,473 independently checked timed receipts and client-file
SHA-256 digests. The remaining 288 acknowledgments are the sixteen manual warmup
writes per point, checked by the live and cold harness verification.

[Current-main comparison](https://github.com/crabbuild/cellule/actions/runs/37160512080)
uses candidate `f9995af7a584ce6c0da9d2f5567163df1af811db` against main
`5724959a79be90df1ed8e85f6e123c109455d2f5`. Its results are pending. Production
compaction is unchanged by the subsequent cancellation-test commit.
