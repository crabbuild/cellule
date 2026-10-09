# Native capture diagnostic

The instrumented `03b9002` service passes the original 2,000-Cell audit after a
30-second warmup and 180-second window offering 1,000 writes/s plus 10 reads/s.
It uses a fresh 4-vCPU/6-GiB RustFS fixture on the shared 8-vCPU/16-GiB Colima VM
and the same framework budgets, 16 SQL workers, 8 Tokio workers, 64 clients and
queue capacity 256 as the [longer pair](2026-10-05-local-resume.md).
The observation-only source and shorter window differ; this is not another
paired performance gain or target qualification.

| Lifetime phase | Count | Mean, ms | p50 / p99, ms |
| --- | ---: | ---: | ---: |
| Native capture | 41,352 | 5.776 | 1.6 / 46.3 |
| WAL read | 41,352 | 0.177 | 0.1 / 1.9 |
| LTX encoding | 41,352 | 0.099 | 0.1 / 1.2 |
| Local write | 41,352 | 0.240 | 0.1 / 5.1 |
| Capture checkpoint | 41,352 | 1.226 | 0 / 15.9 |
| SQL worker round trip | 39,352 | 31.415 | 23.1 / 154.9 |
| Root admission | 38,014 | 6,166.855 | Overflow / overflow |
| Admitted root work | 38,014 | 48.784 | 43.3 / 190.4 |

Capture includes bootstrap; worker measures commands; root preparation can
cover multiple commands. Do not subtract quantiles or sum overlapping means.
Unvisited capture phases contribute zero. There are 6,331 executed checkpoints,
zero capture failures/fallback reads, and a largest WAL-image allocation of
65,952 bytes. Capture fsync is zero because these LTX cuts are deferred; this
does not measure SQLite COMMIT or follower fsync and does not weaken durability.
The observer does not isolate SQL execution from worker admission/queue time.

Shared root admission still dominates the observed wait. Native WAL parsing
and LTX encoding are small here. The next causal test should distinguish
publication-cohort occupancy, fair background compaction, and provider I/O
before changing memory admission or SQL scheduling.

The run completes 165.71 writes/s and 1.52 reads/s inside its window, with
write HTTP p50/p95/p99 of 333.3/709.6/934.3 ms and zero request errors. It drops
149,854 write offers and 1,524 read offers. All 39,352 writes and 349 reads
cold-audit with original scoped receipts; all Cells drain Idle and pass next
write/epoch/sequence checks. Follower append failures and unsupported/unavailable/
rejected submissions are zero. The [dataset](2026-10-05-native-capture.json)
retains phases, strategy/byte counters, binary/source hashes and external
original-journal reverification. The capacity target remains open.
