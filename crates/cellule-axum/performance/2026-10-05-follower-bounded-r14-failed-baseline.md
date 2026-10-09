# R14 baseline: shutdown deadline failed

The frozen `ff0d79b` baseline completed the original 600-second, 2,000-Cell
HTTP load with zero request errors, then failed the original 60-second initial
shutdown deadline. Cold recovery, exact identity replay and next-epoch writes
were not reached. The paired comparison stopped before dispatching its candidate.

| Load-only diagnostic | Result |
| --- | --- |
| Write / mixed-read completion TPS | 118.94 / 1.06 |
| Write request latency p50 / p95 / p99 | 408.1 / 1,250.5 / 2,368.6 ms |
| Write / read offers dropped | 528,317 / 5,361 |
| Publication / follower-append failures observed | 0 / 0 |

These histogram latencies include measured requests completed during drain;
TPS counts only completions within the window. Offers were round-robin, but
driver drops can skew completions. Mixed reads do not establish read capacity.

The harness's subsequent abort cleanup returned after the log printed 2,000
Idle Cells, and the SQLite temporary directory was absent. Those observations
do not satisfy the original successful shutdown deadline or recovery audit.
The final lifetime telemetry contained substantial dirty-admission/publication
waits; those populations overlap and differ from HTTP latency.

The provider archive passed gzip integrity, complete enumeration of all
495,804 original inodes and SHA-256 verification before the owned container
and volume were removed. Journals, fixture, logs and resource observations are
preserved externally. The [adjacent JSON](2026-10-05-follower-bounded-r14-failed-baseline.json)
records the failure, critical metrics, hashes and evidence references.

This shared-Colima baseline is failed evidence, not a passing paired control.
The bounded-log candidate will be evaluated separately with unchanged
deadlines and recovery gates. No comparative throughput gain or target
qualification is claimed. The full node capacity and recovery target remains open.
