# Queued native write groups against RustFS

Bounded per-Cell group commit improves sustained HTTP write throughput and
latency when multiple commands queue in the same Cell. It does not improve
the sixteen-Cell, sixteen-client profile, where commands do not form groups.

[Completed paired run](https://github.com/crabbuild/cellule/actions/runs/37173160571)
compares baseline `ec19f8c638ffb3840c50062947a72f1af7ff3875` with candidate
`1e21c171ac38f1de4b180fc61c5365df4a178c26`. Framework changes are in
`16ac5d85b72ea9b5a8970fc8eda029927679027e`; the candidate additionally repairs
recognition of a byte-identical, already-instrumented baseline.

## Protocol and verification

Three alternating baseline/candidate pairs at 1, 4, and 16 Cells use sixteen
persistent HTTP clients, four SQL workers, four HTTP Tokio workers, five
seconds of timed warmup, and 120 seconds of measured admission per point.
Every POST inserts a small order and performs a receipt-bound SELECT.
Both release builds finish before load. Each point uses a fresh prefix.

All eighteen points pass live and fresh-file cold recovery for all **268,020
acknowledged writes**, including warmup. The measured windows contain 255,428
successful writes and zero errors. Independent raw-client verification checks
267,732 timed receipts; the remaining 288 manual warmup writes are checked by
the harness. Checks cover exact retries, conflicting and expired identities,
exclusive ownership, contiguous individual commit sequences, drained roots,
new recovery fences, and the next command after recovery.

The baseline instrumentation diff is empty. Retained patch hashes match the
fixed observation fixture, and both example observer files are byte-identical
between revisions. The [complete dataset](2026-10-04-rustfs-grouped-paired-writes.json)
retains every pair, raw-client hashes, binary hashes, phase observations,
provider and server CPU, and interval stability.

## Results

Changes are medians of three paired candidate/baseline ratios, rather than
ratios of independently selected medians. Lower latency is better.

| Cells | Baseline median TPS | Candidate median TPS | Paired TPS change | Paired p95 change | Paired p99 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 57.66 | 147.85 | +156.3% | -57.2% | -57.1% |
| 4 | 114.07 | 163.19 | +64.3% | -22.4% | -16.2% |
| 16 | 104.45 | 104.54 | -3.1% | +6.2% | +4.5% |

| Cells | Repeat | TPS change | p95 change | p99 change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | +152.6% | -57.2% | -57.5% |
| 1 | 2 | +156.3% | -50.5% | -48.8% |
| 1 | 3 | +204.0% | -59.8% | -57.1% |
| 4 | 1 | +38.4% | -8.1% | +2.7% |
| 4 | 2 | +64.3% | -22.4% | -16.2% |
| 4 | 3 | +72.9% | -26.9% | -25.3% |
| 16 | 1 | -3.1% | +14.6% | +4.6% |
| 16 | 2 | +0.3% | -21.7% | +2.6% |
| 16 | 3 | -8.6% | +6.2% | +4.5% |

Every one- and four-Cell pair improves TPS and p95. Four-Cell repeat 1 regresses
p99. All sixteen-Cell p99 measurements regress; their cause remains unresolved.
Three repeats are not a statistical confidence interval. These results qualify
the measured queued-write workloads, not a universal latency improvement.

## What changed the cost

| Cells | Roots per acknowledged command, baseline / candidate | Uploaded objects per command, baseline / candidate | RustFS CPU cores, baseline / candidate |
| --- | ---: | ---: | ---: |
| 1 | 1.000 / 0.320 | 5.551 / 2.252 | 2.527 / 2.484 |
| 4 | 1.000 / 0.583 | 5.314 / 3.093 | 2.726 / 2.771 |
| 16 | 1.000 / 1.000 | 5.259 / 5.259 | 3.009 / 3.007 |

Values are independent repeat medians. Publication and upload counts include
warmup; uploads also include startup. The response workload completes more
logical commands for similar provider CPU use by amortizing capture and
publication across queued commands. Every command still has its own request
ledger row and logical sequence, and every new result waits for its covering
root. Group size is bounded at four, with no batching timer or budget increase.

At sixteen Cells, the fixed client stride gives approximately one client per
Cell. There is no publication amortization. Root admission averages about
83 ms in both variants; admitted root preparation averages about 33 ms, and
RustFS uses about three cores of the four-CPU runner. The shared publication
and provider limits remain the next bottleneck to investigate. Those mean
phase observations do not explain the individual p99 regressions.

## Qualification and remaining work

The group implementation passes [workspace CI](https://github.com/crabbuild/cellule/actions/runs/37172549353)
and three fresh repeats each of [object and follower qualification](https://github.com/crabbuild/cellule/actions/runs/37172549422).
Independent qualification verification covers 48,144 acknowledged writes,
597 raw TSV hashes, matching source/binary evidence, and clean process exits.
The tested merge tree exactly matches `16ac5d8`.

An additional [64-client comparison](https://github.com/crabbuild/cellule/actions/runs/37175252712)
failed during one-Cell baseline warmup: 522 replies succeeded and fourteen
returned HTTP 503. No steady-state comparison was measured. The failure and
raw replies are retained, not excluded from the evidence. Its exact error
source must be diagnosed before retrying; resource ceilings and zero-error
checks remain unchanged.

A subsequent [error-source probe](https://github.com/crabbuild/cellule/actions/runs/37176372337)
reproduces five baseline warmup failures. Every failed follow-up read reports
`InvocationError::NotStarted(Capacity("Cell mailbox requests"))`. Both binaries
have identical error-only probes and retained source diffs. A real actor
regression reproduces the same refusal after a durable reply is delivered
while its completed request still holds a slot. The admission fix releases
finished request slots before terminal delivery, including all group members,
while retaining byte charges until completion data is dropped and all admission
for workers still running after timeout. It passes the regression and the
local runtime suite; a fresh 64-client comparison is still required.

The performance objective remains open for multicell publication limits,
the sixteen-Cell tail regressions, and admission behavior at higher concurrency.
Response throughput in the dataset measures JSON response-body bytes, not
database bytes or network-wire throughput. Phase timings include warmup and
some verification work; compaction is a subset of preparation.
