# Pinned capture jobs: R11 multicell comparison

Candidate performs each small pinned capture read and verification/merge in
one admitted job. Control already uses bounded captured-delta coalescing. Both sources share the private final compaction
directory, coverage after admission, clock correction, native caps, original
retry deadline and response gate.

Control `581946b` completed **165.33 write TPS**; candidate `738595e`
completed **123.89 write TPS** (**-25.06%**).
This is one ordered shared-VM comparison, not evidence of a consistent capacity
gain. This pair records a throughput and latency regression. The delayed-executor
diagnostic independently verifies fewer dispatch waves, but that does not prove
a node throughput improvement. Provider PUT means also increased from 34.65 to
46.02 ms; the same control binary measured 136.81 TPS in R10 and 165.33 TPS here.
The entire difference cannot be attributed to the source change or dismissed as
environmental variation.

| Critical metric | Control | Candidate |
| --- | ---: | ---: |
| Completed write TPS | 165.33 | 123.89 |
| Write request p50 / p95 / p99, ms | 301.10 / 832.09 / 2,451.11 | 358.87 / 1,447.97 / 3,301.37 |
| Write response-body throughput, MiB/s | 0.031800 | 0.023814 |
| Read response-body throughput, MiB/s | 0.000264 | 0.000198 |
| Mixed-read TPS | 1.407 | 1.053 |
| Read request p50 / p95 / p99, ms | 7.07 / 76.29 / 199.73 | 9.04 / 133.30 / 293.87 |
| Write queue drops / request errors | 500,485 / 0 | 525,344 / 0 |
| Peak owner RSS, MiB / descriptors | 678.05 / 16,147 | 658.11 / 16,152 |
| Owner CPU cores, setup/warmup/drain included | 0.992 | 0.945 |
| Original writes / reads audited | 110,394 / 936 | 82,330 / 685 |
| Publications per audited write | 0.863 | 0.821 |
| Provider PUTs per audited write | 5.599 | 5.425 |

Each point used 2,000 Cells, 30-second warmup and a 600-second measured window,
1,000 offered writes/s and 10 offered reads/s, 64 clients, queue capacity 256,
16 SQL workers and eight Tokio workers. Order was control then candidate with
a fresh RustFS store per point. The pinned Linux compiler, original driver,
SQL identity source, frozen source archives and binaries were verified.
Native caps stayed dirty 8, recovery 2, I/O 32 and jobs 8.

TPS counts completions inside the measured window. Request percentiles include
measured attempts drained afterward. Response-body throughput measures small
HTTP response bodies. JSON retains all ten one-minute completion bins and
scheduled latency including queue delay. Sparse mixed reads do not establish
read capacity.

| Lifetime phase mean, ms | Control | Candidate |
| --- | ---: | ---: |
| Dirty admission | 2,124.24 | 3,171.98 |
| Total publication | 2,444.40 | 3,956.09 |
| Admitted root work | 51.66 | 71.13 |
| Compaction | 15,820.50 | 31,264.67 |
| SQL worker round trip | 26.44 | 43.90 |
| Provider PUT | 34.65 | 46.02 |

Lifetime phases include startup, warmup and drain, overlap and have different
populations. Overflowed percentiles stay unavailable with counts retained.
Do not subtract means. Admission, compaction and provider I/O remain expensive.

Both original follower gates and every journal-hash/count recheck passed.
All 2,000 Cells drained to idle, cold-restored, accepted the next write at the
next sequence and advanced their fencing epoch. Complete provider archives
passed gzip/tar, inode-count and SHA-256 verification before owned removal.
Original journals, fixtures, logs, source archives and binaries remain outside
Git. No original identities or receipts were replaced or sampled.

Auxiliary lifetime observations found roughly 18.45 GB read and 3.98 GB written
per follower, with 13.83 million read and 8.73 million write calls while the
open log was about 8.89 MB. These counters include setup and warmup, are not a
matched steady-state measurement, and do not prove causality. The current
follower pruning path repeatedly scans open history, rewrites retained records
and rescans the lane. The completed [native follower diagnostic](2026-10-05-follower-history-cost.md)
separates read syscall count from bytes traversed: buffering reduces calls,
but its elapsed medians regress and it was not adopted. Frame checks and fsync
ordering remain required.

The target remains unestablished: three 30-minute windows on dedicated
8-vCPU/16-GiB hardware, 10K durable write TPS, 50K owner-ordered read TPS, and
original owner-loss/follower-only recovery qualification. These processes share
the 8-CPU/16-GiB Colima VM with the provider and followers.

[Controlled native diagnostic](2026-10-05-captured-coalescing.md) and
[critical metrics, source pins and evidence hashes](2026-10-05-pinned-coalescing-r11.json).
