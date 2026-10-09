# Final directory upload: R9 multicell comparison

Candidate retains one verified compaction leaf privately through the following
append and uploads only the final directory. Both sources share coverage after
admission, clock correction, native caps, original retry deadline and response gate.

Control `4d22ebb` completed **115.92 write TPS**; candidate `3eb41c4`
completed **119.34 write TPS** (**+2.95%**).
This is one ordered shared-VM comparison, not evidence of a consistent capacity
gain. The fixed-latency diagnostic independently proves one fewer PUT and exact
final-root identity; stage savings alone do not establish node throughput.

Write p95 improved 22.65%, while p50 and p99 worsened 9.49% and 9.80%.
Lifetime compaction and admission means grew despite slightly lower root work
and PUTs per audited write. Further matched runs and causal work are required.

| Critical metric | Control | Candidate |
| --- | ---: | ---: |
| Completed write TPS | 115.92 | 119.34 |
| Write request p50 / p95 / p99, ms | 349.31 / 1,790.26 / 3,275.30 | 382.47 / 1,384.75 / 3,596.30 |
| Write response-body throughput, MiB/s | 0.022290 | 0.022948 |
| Mixed-read TPS | 0.855 | 0.913 |
| Read request p50 / p95 / p99, ms | 6.49 / 135.07 / 400.89 | 6.58 / 104.35 / 267.54 |
| Write queue drops / request errors | 530,132 / 0 | 528,106 / 0 |
| Peak owner RSS, MiB / descriptors | 672.66 / 16,178 | 659.50 / 16,173 |
| Owner CPU cores, setup/warmup/drain included | 0.949 | 0.950 |
| Original writes / reads audited | 77,348 / 561 | 78,114 / 593 |
| Publications per audited write | 0.857 | 0.845 |
| Provider PUTs per audited write | 6.446 | 6.367 |

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
| Dirty admission | 2,550.27 | 2,727.33 |
| Total publication | 3,227.93 | 3,415.56 |
| Admitted root work | 71.91 | 70.87 |
| Compaction | 21,058.29 | 24,473.50 |
| SQL worker round trip | 41.45 | 47.17 |
| Provider PUT | 41.41 | 42.01 |

Lifetime phases include startup, warmup and drain, overlap and have different
populations. Overflowed percentiles stay unavailable with counts retained.
Do not subtract means. Admission, compaction and provider I/O remain expensive.

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

[Controlled native diagnostic](2026-10-05-final-directory-upload.md) and
[critical metrics, source pins and evidence hashes](2026-10-05-final-directory-r9.json).
