# Sustained RustFS HTTP writes

This report retains the comparisons before native command grouping. The later
[group-commit comparison](2026-10-04-rustfs-grouped-paired-writes.md) establishes
gains at one and four Cells, with unresolved sixteen-Cell tail regressions.

The compaction pipeline is faster, but this comparison does **not** establish a
consistent improvement in write throughput or HTTP tail latency. A second
comparison against current main and a third instrumented comparison also have
mixed results. Keep the optimization goal open.

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

## Current-main confirmation

[Completed comparison](https://github.com/crabbuild/cellule/actions/runs/37160512080)
uses candidate `f9995af7a584ce6c0da9d2f5567163df1af811db` against main
`5724959a79be90df1ed8e85f6e123c109455d2f5`, with the same
three-pair, 120-second protocol and correctness checks. Production compaction
is unchanged by the subsequent cancellation-test commit.
[Full current-main data](2026-10-03-rustfs-current-main-writes.json) retains
every point and raw-client hash.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 57.15 | 61.15 | +5.7% | -13.0% | -12.8% |
| 4 | 109.39 | 97.36 | -6.2% | +5.3% | +2.4% |
| 16 | 101.32 | 102.99 | -4.7% | -7.0% | +5.0% |

| Cells | Repeat | TPS change | p95 change | p99 change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | +5.7% | -15.9% | -19.3% |
| 1 | 2 | +0.4% | -1.8% | +4.3% |
| 1 | 3 | +16.9% | -13.0% | -12.8% |
| 4 | 1 | -11.0% | +5.3% | +2.4% |
| 4 | 2 | -0.1% | -3.0% | -2.1% |
| 4 | 3 | -6.2% | +7.5% | +7.5% |
| 16 | 1 | +1.6% | -17.7% | -3.8% |
| 16 | 2 | -4.7% | +23.3% | +5.0% |
| 16 | 3 | -6.0% | -7.0% | +6.0% |

The four-Cell median paired throughput regresses by 6.2%. At sixteen Cells,
two pairs regress throughput despite the candidate having a higher independent
median TPS. This is why the report uses paired ratios. All pairs remain in the
analysis; this run does not establish a consistent performance improvement.

Baseline preparation means are approximately 11, 22, and 131 ms at 1, 4, and
16 Cells; authority means are approximately 5, 11, and 21 ms. Server CPU remains
about 0.3 to 0.5 cores. These observations reinforce the publication-path
bottleneck but still do not separate admission wait from admitted provider work.

All eighteen points pass live and cold verification for 200,247 acknowledged
writes, including warmup, with zero errors in the measured windows. Those windows
complete 191,177 writes. The independent artifact audit checks 199,959 timed
receipts; the remaining 288 manual warmup acknowledgments are covered by the
harness. Source rows, exact replays, contiguous sequences, drained authority,
new recovery fences, and independent next writes all pass.

## Instrumented admission diagnosis

[Completed instrumented comparison](https://github.com/crabbuild/cellule/actions/runs/37164312064)
uses candidate `1b73491650462c38de8b4a34c07619f117b5d948` against main
`5724959a79be90df1ed8e85f6e123c109455d2f5`, with the same three-pair,
120-second protocol. This candidate adds observations to the compaction overlap;
it does **not** include the subsequent recovery-admission fix.
[Full diagnostic data](2026-10-04-rustfs-admission-diagnostic-writes.json)
retains all eighteen points, raw-client hashes, capacities, provider operation
histograms, and dedicated RustFS cgroup CPU counters. The observation backport
and full baseline diff pass their retained SHA-256 checks. The backport matches
the tracked fixture and changes only example wiring and phase observations.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 60.00 | 62.53 | -0.7% | +4.4% | +11.7% |
| 4 | 100.11 | 99.08 | -0.3% | -2.9% | -1.9% |
| 16 | 105.24 | 104.67 | -0.5% | -5.2% | +1.4% |

| Cells | Repeat | TPS change | p95 change | p99 change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | +4.2% | +0.5% | +6.3% |
| 1 | 2 | -0.7% | +4.4% | +13.1% |
| 1 | 3 | -2.6% | +14.1% | +11.7% |
| 4 | 1 | -0.3% | -2.9% | -1.9% |
| 4 | 2 | +0.2% | -4.2% | -4.5% |
| 4 | 3 | -6.0% | -0.9% | -1.6% |
| 16 | 1 | -0.5% | -10.1% | +1.4% |
| 16 | 2 | -12.7% | +6.8% | +15.8% |
| 16 | 3 | +2.2% | -5.2% | -8.2% |

The sixteen-Cell second pair regresses throughput by 12.7%; it remains in the
analysis. These results still do not establish consistent end-to-end gains.

Both variants report the same capacities: four dirty-memory slots, two recovery
slots, four blocking jobs, thirty-two I/O slots, 65,536 MiB of scratch admission,
and a 1 GiB local disk budget. The baseline phase means below are medians across
the three repeats; root admission and admitted work have matching counts.

| Cells | Root admission (ms) | Admitted root preparation (ms) | Total root preparation (ms) | Recovery admission (ms) |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.003 | 8.059 | 8.065 | 0.000 |
| 4 | 0.003 | 20.205 | 20.212 | 0.721 |
| 16 | 83.205 | 32.259 | 115.469 | 13.851 |

| Cells | HTTP service CPU (cores) | RustFS CPU (cores) | Provider PUT mean (ms) |
| --- | ---: | ---: | ---: |
| 1 | 0.279 | 2.281 | 5.282 |
| 4 | 0.487 | 2.989 | 13.495 |
| 16 | 0.503 | 2.996 | 22.431 |

At sixteen Cells, admission accounts for about 72% of mean root preparation.
The candidate has similar waits: approximately 83 ms before admission and 33 ms
of admitted work. This confirms substantial framework queueing under the current
host budget; it does not justify increasing that budget without measuring
provider and memory pressure.

RustFS consumes approximately three of the runner's four logical CPU cores at
four and sixteen Cells, while the HTTP service consumes about half a core.
Provider cgroup counters show no CPU throttling. Rising PUT durations and high
provider CPU support investigating provider work and request amplification;
they do not isolate provider CPU, disk, or network as the sole cause. Provider
operation counts and phase histograms include startup and verification, so they
must not be divided by measured-window writes to claim an exact per-write cost.
Service and provider CPU windows include timed warmup. Overlapping operations
are not additive latency.

All eighteen points pass live replay and fresh-SQLite recovery for 201,844
acknowledged writes, including warmup. Measured windows complete 192,920 writes
with zero errors. The independent raw audit checks 201,556 timed receipts;
288 manual warmup acknowledgments are covered by the harness. Per-Cell sequences,
exclusive ownership, drained authority, new fences, and independent next writes
all pass. This HTTP/object-proof coverage does not substitute for separate
follower durability qualification.

The separate comparison below isolates the recovery-admission change against
the instrumentation-only commit. It removes a reproduced dirty-capacity
reservation by recovery waiters while preserving resource ceilings and ledger
charges.

### Follow-up operation attribution

The controlled warm-append test records two predecessor-metadata HEADs and five
successor PUTs, with no full-body or range reads. Compacting its two segments
then records four range reads: one LTX body and one index per segment. These are
test-path counts, not an attribution of every request in the sustained run.
They direct further work toward compaction traffic and publication amplification
rather than warming SQLite pages on the ordinary append path.

A prototype overlapped cached predecessor presence checks with successor
uploads. With 100 ms injected storage delays it reduced preparation from 200 ms
to 100 ms, but violated the existing zero-upload contract when inherited metadata
is missing. It was rejected. The production path still checks predecessor
presence before any successor upload; the unchanged missing-origin test passes.

## Isolated recovery-admission comparison

[Completed admission comparison](https://github.com/crabbuild/cellule/actions/runs/37166813306)
uses candidate `ec19f8c638ffb3840c50062947a72f1af7ff3875` against
instrumentation-only `1b73491650462c38de8b4a34c07619f117b5d948`.
The baseline observation diff is empty, both examples have identical observations,
and the retained backport matches the tracked fixture and its SHA-256 manifest.
[Full paired data](2026-10-04-rustfs-admission-paired-writes.json) retains every
point, raw-client hashes, phase histograms, and provider CPU observations.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 57.16 | 58.20 | -0.3% | -6.7% | -7.2% |
| 4 | 111.28 | 100.00 | +0.9% | -1.1% | -0.4% |
| 16 | 103.83 | 102.34 | -0.9% | -24.3% | -6.2% |

| Cells | Repeat | TPS change | p95 change | p99 change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | -0.3% | -14.0% | -11.1% |
| 1 | 2 | -1.6% | +4.5% | +11.9% |
| 1 | 3 | +3.9% | -6.7% | -7.2% |
| 4 | 1 | -15.0% | +21.6% | +20.4% |
| 4 | 2 | +0.9% | -1.1% | -0.4% |
| 4 | 3 | +2.8% | -4.3% | -6.6% |
| 16 | 1 | +0.1% | -26.0% | -6.2% |
| 16 | 2 | -0.9% | -10.6% | -6.5% |
| 16 | 3 | -13.7% | -24.3% | +9.4% |

All three sixteen-Cell pairs improve p95, with a paired median reduction of
24.3%. Throughput remains essentially flat in the paired medians. Four-Cell
repeat one regresses TPS by 15.0%; sixteen-Cell repeat three regresses TPS by
13.7% and p99 by 9.4%. These pairs remain in the analysis, so this establishes a
p95 improvement in this workload rather than consistent throughput and tail
latency gains across the matrix.

At sixteen Cells, median mean root admission remains about 84 ms and admitted
preparation about 34 ms in both variants. Recovery semaphore acquisition drops
from 11.769 ms to 1.226 ms, but the new cohort-gate wait is included only in total
compaction time. Total compaction means are 209.459 ms and 223.080 ms; the
semaphore timing must not be treated as total recovery queueing. RustFS consumes
about three CPU cores in both variants on the four-CPU runner. The remaining
bottleneck still includes shared publication admission and provider work.

All eighteen points verify 202,186 acknowledged writes, including warmup,
with 193,131 measured writes and zero measured errors. The independent raw
audit checks 201,898 timed receipts; 288 manual warmup acknowledgments are
covered by the harness. Live exact replay, conflicts and expiry, exclusive
ownership, drained authority, fresh-SQLite recovery, and an independent next
write under a new fence pass for every Cell. Separate object/follower
qualification remains required. The broader performance objective remains open.
