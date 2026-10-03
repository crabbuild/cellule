# Steady Axum reads and SQLite allocator contention — 2026-10-03

A bounded SQLite lookaside arena improved median read throughput by **4.4%,
11.2%, and 12.7%** at 1, 4, and 16 Cells in three paired 60-second Linux runs.
At 16 Cells, p95 fell from **1.96 to 1.71 ms**, p99 from **2.78 to 2.41 ms**,
and server CPU per read fell **10.2%**. All **14,755,546 measured reads** and
126 individual Cell cold restores passed. The memory cost is 24 KiB per
active Cell, fully charged to runtime admission.

## Diagnosis and change

The earlier [multicell measurements](2026-10-03-rustfs-multicell.md) used
2,048 reads per point, often finishing in a fraction of a second. The Python
driver assigned strided orders to connections, so each connection visited only
a subset of Cells. Those results cannot establish a scaling curve or be used
as the baseline for the new Rust driver.

The replacement driver continuously reads acknowledged orders after warmup.
Every client visits every order, checks the output and minimum receipt, and
records all attempts in a bounded 10 µs histogram. It reports request rate,
successful JSON body bytes/s, p50/p95/p99, errors, coverage, and exact maximum
latency. Latency includes body decoding and validation. Both paired servers
use the same driver. CPU windows include warmup; server CPU per read includes
warmup reads in its denominator.

A ten-second macOS sample at 16 Cells showed this wait stack:

```text
sqlite3Prepare
  dbMallocRawFinish
    sqlite3Malloc
      _pthread_mutex_firstfit_lock_slow
        _pthread_mutex_firstfit_lock_wait
          __psynch_mutexwait
```

The runtime telemetry separates actor queue wait, SQL worker round trip, and
primitive SQL execution. In the local probes, SQL execution became more
expensive as independent Cells used multiple workers. Small SQL allocations
still passed through SQLite's shared heap allocator because managed
connections explicitly disabled lookaside.

The change requests a fixed arena with 512-byte slots and a count of 16.
[SQLite allocates the arena itself](https://sqlite.org/c3ref/c_dbconfig_defensive.html)
when the supplied buffer pointer is null. Larger or excess allocations fall
back to the ordinary heap. The three managed connections add a total of
24 KiB per active Cell; runtime native reservation rises from 64 to 88 KiB.
A fixed native admission budget can therefore admit fewer Cells. Page-cache
targets, SQL authorization, fenced ownership, and durable publication rules
are unchanged. These reservations are not RSS limits.

An actor scheduling experiment was rejected. Eighteen paired 60-second runs
validated 20,858,961 reads, but throughput gains were inconsistent and server
CPU per read increased in the 16-Cell comparison. The PR keeps the original
actor scheduling.

## Local allocator probes

Apple M2 Max, 12 logical CPUs, 32 GiB, macOS 26.5.2; Rust 1.97.0 release.
One service process, four SQL workers, 16 HTTP clients, 16 Cells; two paired
30-second windows after five-second warmups. Baseline and candidate contain
the same telemetry and driver instrumentation. Pair order alternates.

| Pair | Variant | Reads/s | p50 ms | p95 ms | p99 ms | Server CPU µs/read | Mean SQL µs |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Baseline | 19,312 | 0.52 | 2.20 | 3.99 | 140.2 | 39.9 |
| 1 | Lookaside | 21,485 | 0.40 | 2.31 | 5.26 | 95.5 | 16.4 |
| 2 | Lookaside | 5,939 | 0.82 | 8.70 | 27.96 | 108.8 | 35.4 |
| 2 | Baseline | 4,749 | 1.06 | 12.46 | 39.40 | 128.2 | 66.1 |

All 1,545,315 steady reads passed validation. Both pairs show lower server CPU
per read (15–32%) and SQL time (46–59%), with higher throughput (11–25%). The
first pair's p95/p99 became worse. Heavy unrelated workstation load was
observed during the probes; these observations motivate the dedicated comparison
and do not establish production capacity or a universal latency improvement.

## Dedicated Linux comparison

[CI run](https://github.com/crabbuild/cellule/actions/runs/37154982549) on a
fresh Ubuntu 24.04 x86_64 runner: AMD EPYC 7763, four logical CPUs reported
(two cores, two threads per core), Rust 1.97.0 release, Python 3.12.3.
Baseline `a277e5282badab55ceb58433fdddf0dee4dc8542` versus candidate
`408d9fcf876117d8a10fbe6231ff62404e90720e`. Both use the candidate Rust
driver. Candidate query telemetry adds timing and atomic counters; the
pristine baseline does not contain those counters. The matched local probes
above isolate the allocator change with telemetry on both sides.

One native service process, four SQL workers, 16 HTTP clients; 16 active-Cell
slots, 16 MiB retained-byte setting, and 1 GiB local disk budget. Native
reservation increases from 64 to 88 KiB per active Cell. HTTP Tokio workers
use the system default; the driver uses four Tokio workers. RustFS runs in
a container on the same runner with the pinned image
`ghcr.io/rustfs/rustfs:1.0.0-glibc@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.

Each variant has three 60-second read windows after five-second warmups.
Baseline/candidate order alternates across repeats. Every point holds 48
acknowledged orders distributed across its Cells. Values below are medians
of per-run statistics; latency percentiles are not pooled. MB/s uses decimal
megabytes of successful JSON response bodies.

| Cells | Variant | Reads/s | Payload MB/s | p50 ms | p95 ms | p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Baseline | 8,654 | 1.677 | 1.88 | 2.09 | 2.28 |
| 1 | Candidate | 9,035 | 1.751 | 1.80 | 2.01 | 2.14 |
| 4 | Baseline | 14,048 | 2.723 | 0.95 | 2.45 | 2.95 |
| 4 | Candidate | 15,628 | 3.029 | 0.89 | 2.12 | 2.58 |
| 16 | Baseline | 16,332 | 3.149 | 0.87 | 1.96 | 2.78 |
| 16 | Candidate | 18,408 | 3.549 | 0.77 | 1.71 | 2.41 |

| Cells | Baseline rate range | Candidate rate range | Paired rate gains, repeats 1 / 2 / 3 | Server CPU µs/read, baseline → candidate |
| ---: | ---: | ---: | --- | ---: |
| 1 | 8,526–8,785 | 8,923–9,350 | +4.7% / +8.0% / +2.8% | 192.1 → 185.7 |
| 4 | 13,812–14,073 | 15,353–15,674 | +11.2% / +9.1% / +13.5% | 151.3 → 134.3 |
| 16 | 16,292–16,357 | 18,124–18,535 | +13.3% / +11.0% / +13.0% | 139.6 → 125.3 |

All nine paired comparisons improved request rate and p50/p95/p99.
At 16 Cells, median server CPU usage was 2.28 → 2.31 logical cores while
driver usage was 1.07 → 1.16 cores: the server completed more reads for
roughly the same CPU time. CPU is measured through `ps` and `/usr/bin/time`,
with OS reporting precision; RSS is sampled before and after the read window.
Median 16-Cell RSS after reads was 25.95 → 26.53 MiB. Three repeats support
a repeatable improvement in this profile; they are not a capacity qualification.

Candidate lifetime telemetry after drain:

| Cells | Mean actor queue µs | Mean worker round trip µs | Mean primitive SQL µs |
| ---: | ---: | ---: | ---: |
| 1 | 1495.6 | 100.2 | 16.1 |
| 4 | 503.5 | 186.7 | 17.9 |
| 16 | 138.0 | 346.1 | 18.9 |

These means include warmup and correctness checks. Increasing Cells reduced
per-Cell queue wait but increased worker round-trip time; dispatch, read-only
setup, worker admission, and runtime scheduling remain costs beyond the SQL
handler. The four SQL workers and the shared runner/client CPU limit scaling.
No baseline SQL-timing comparison is available in this Linux run.

## Interpretation

More Cells share the same four SQL workers; adding Cells does not add CPU.
Owner queries remain ordered per Cell. More Cells can expose independent
queries to multiple workers, but shared allocator contention, worker dispatch,
actor queues, HTTP processing, and the client can then limit throughput.
The lookaside change targets the allocator cost observed in this workload.

GETs read the active owner's local SQLite database. RustFS validates durable
publication and cold recovery in the same test; JSON body throughput is not
S3 read bandwidth. The workload is a small resident table under closed-loop
load, with no concurrent writes during steady reads. It does not determine
maximum offered-load capacity, large-response throughput, distributed-host
scaling, or mixed read/write tail latency.

## Reproduction

Use the [steady-read runner instructions](../README.md#verify-http-against-rustfs).
The dedicated Linux workflow builds both servers before measuring and uses
three paired 60-second windows for each of 1, 4, and 16 Cells, four SQL workers,
and 16 clients. Each window has five seconds of read warmup, 16 warmup writes,
32 measured writes, and 64 initial correctness reads. Fresh provider prefixes
and local SQLite files isolate each variant.

Every point also verifies published sequences, exact live and recovered retries,
identity conflicts and expiry, exclusive ownership, Idle drain, fresh-file cold
restore, independent Cell request ledgers, and continued fenced publication.

## Validation and retained evidence

All 18 points passed the per-Cell correctness gates: 576 measured durable
writes, 288 warmup writes, 864 live exact retries, 864 cold recovered rows
and exact retries, 126 conflict and expiry checks each, 18 active-owner
refusals, and 126 recovered next publications. Every service drained to Idle
and removed its temporary SQLite directory. Every steady read proved its
acknowledged output, Cell, incarnation, and minimum sequence. No measured
read failed or exceeded the histogram range.

Local isolated verification passed: 1,257 workspace tests; nine Axum
integration/example tests; 46 local-LTX tests; all-target/all-feature checks
and Clippy; warning-free API docs; format, boundaries, layout, document
syntax/links, runtime contract checks, and workflow lint. The lookaside
regression test failed against the original code and passed with the arena.

[Rust workspace CI](https://github.com/crabbuild/cellule/actions/runs/37154968087)
and [unchanged write-capacity qualification](https://github.com/crabbuild/cellule/actions/runs/37154968260)
also passed. The latter verified 36,313 acknowledged writes across three
object-proof and three follower-proof repeats against the PR merge snapshot
`0e981d807aebc7f1dea564d31e9e718377e97c1f`; its qualification profiles
were unchanged. These write results provide regression evidence, not a
before/after write-throughput comparison.

[Aggregate JSON](2026-10-03-rustfs-steady.json) retains environment, binary
and driver hashes, all paired point results, per-order coverage, Cell receipts,
shutdown/recovery evidence, the local allocator probes, qualification proof
identities, and hashes of the raw CI artifact files. The CI artifact retains
request envelopes, initial read/write responses, full driver outputs, service
logs, and source revisions for seven days. A local copy remains under
`$HOME/Workspace/crabbuild-target/cellule-axum-steady-54f709e0/ci-final`.
