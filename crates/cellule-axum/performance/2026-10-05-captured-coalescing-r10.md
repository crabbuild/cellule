# Bounded captured deltas: R10 multicell comparison

Candidate combines eligible small captured deltas into one verified immutable
delta before publication. Both sources share the private final compaction
directory, coverage after admission, clock correction, native caps, original
retry deadline and response gate.

Control `9f43416` completed **68.81 write TPS**; candidate `581946b`
completed **136.81 write TPS** (**+98.83%**).
This is one ordered shared-VM comparison, not evidence of a consistent capacity
gain. The fixed-latency diagnostic independently proves fewer body/index PUTs and
byte-identical recovery; stage savings alone do not establish node throughput.

| Critical metric | Control | Candidate |
| --- | ---: | ---: |
| Completed write TPS | 68.81 | 136.81 |
| Write request p50 / p95 / p99, ms | 720.76 / 2,346.10 / 3,527.25 | 317.54 / 1,123.11 / 3,091.18 |
| Write response-body throughput, MiB/s | 0.013224 | 0.026304 |
| Read response-body throughput, MiB/s | 0.000093 | 0.000216 |
| Mixed-read TPS | 0.493 | 1.150 |
| Read request p50 / p95 / p99, ms | 22.62 / 471.85 / 1,151.65 | 8.15 / 91.77 / 338.89 |
| Write queue drops / request errors | 558,396 / 0 | 517,595 / 0 |
| Peak owner RSS, MiB / descriptors | 647.54 / 16,164 | 684.97 / 16,182 |
| Owner CPU cores, setup/warmup/drain included | 0.961 | 1.006 |
| Original writes / reads audited | 47,836 / 340 | 94,017 / 791 |
| Publications per audited write | 0.802 | 0.859 |
| Provider PUTs per audited write | 6.657 | 5.626 |

Each point used 2,000 Cells, 30-second warmup and a 600-second measured window,
1,000 offered writes/s and 10 offered reads/s, 64 clients, queue capacity 256,
16 SQL workers and eight Tokio workers. Order was candidate then control with
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
| Dirty admission | 6,363.56 | 2,153.78 |
| Total publication | 7,747.43 | 2,732.29 |
| Admitted root work | 124.88 | 59.31 |
| Compaction | 41,938.99 | 22,839.66 |
| SQL worker round trip | 123.37 | 31.82 |
| Provider PUT | 69.40 | 37.48 |

Lifetime phases include startup, warmup and drain, overlap and have different
populations. Overflowed percentiles stay unavailable with counts retained.
Do not subtract means. Admission, compaction and provider I/O remain expensive. Owner CPU averages
about one core in both points, so this data does not identify CPU saturation
or a shortage of SQLite threads as the primary limit.

Provider PUT mean also changed from 69.40 to 37.48 ms. The same executable
control source completed 119.34 write TPS as the R9 candidate, versus 68.81
here; only tracked performance reports differ between those commits. These
variations prevent attributing the entire TPS difference to coalescing. PUTs
per audited write fell 15.48%, and the controlled native diagnostic separately
establishes the body/index reduction. Repeated target-hardware runs remain
required to establish consistent end-to-end capacity.

Both original follower gates and every journal-hash/count recheck passed.
All 2,000 Cells drained to idle, cold-restored, accepted the next write at the
next sequence and advanced their fencing epoch. Complete provider archives
passed gzip/tar, inode-count and SHA-256 verification before owned removal.
Original journals, fixtures, logs, source archives and binaries remain outside
Git. No original identities or receipts were replaced or sampled.

The target remains unestablished: three 30-minute windows on dedicated
8-vCPU/16-GiB hardware, 10K durable write TPS, 50K owner-ordered read TPS, and
original owner-loss/follower-only recovery qualification. These processes share
the 8-CPU/16-GiB Colima VM with the provider and followers.

[Controlled native diagnostic](2026-10-05-captured-coalescing.md) and
[critical metrics, source pins and evidence hashes](2026-10-05-captured-coalescing-r10.json).
