# Failed original control window: follower verification R12

The preserved R11 control (`738595e`) failed the original offered-load gate.
The candidate was not started by the comparison runner. This point cannot support
a paired performance gain or node-capacity claim. Original deadlines, retry
policy, resource caps, request identities and expected recovery evidence remain
unchanged.

| Critical metric | Failed control |
| --- | ---: |
| Resident Cells / measured window | 2,000 / 600 seconds |
| Offered write / read TPS | 1,000 / 10 |
| Completed write / mixed-read TPS | 66.73 / 0.488 |
| Write request p50 / p95 / p99, ms | 691.33 / 2,314.29 / 4,099.89 |
| Measured write / read errors | 41 / 1 |
| Write / read queue drops | 559,602 / 5,704 |
| All original journal rows, writes / reads | 44,995 / 317 |
| Most active Cell's share of successful measured writes, drain included | 0.0942% |

The driver offered writes round-robin. Completion bins drop from roughly
88–94 writes/s early in the window to 35–48 writes/s in several later minutes.
The original journals independently reproduce every summary attempt, error and
in-window success count. Percentiles are raw nearest-rank request latencies from
measured attempts, including drain; queue drops are excluded from request latency.
Sparse mixed reads do not establish read capacity.

Write errors comprise four deadlines, 33 unavailable responses, three unknown
outcomes and one already-durable receipt whose application follow-up read failed.
The read error is unavailable. The source's pre-abort telemetry records no
node-log append failures or rejected/unavailable/unsupported submissions.
This does not prove the missing cold audit: the driver failure stops the original
harness before its all-Cell restore and next-write gates. No successful recovery
claim is made for this point.

Two retained-PID observations show each follower reading about 22.12 GB and
writing about 0.50 GB over its lifetime by the ending observation. Over the
336.82-second observation interval, each reads about 14.1 GB and consumes about
228 CPU seconds. These counters include setup or drain and are auxiliary,
not causal phase shares or a matched candidate comparison. They support
investigating repeated history scan/rewrite work alongside provider and admission
costs. Current-byte traversal remains after scoped LTX validation reuse.

The owner, two followers and driver share the eight-CPU, 12-GiB assets container
in the eight-CPU, 16-GiB Colima VM with RustFS (four CPUs, six GiB). This is a local
diagnostic. The complete provider archive passed gzip, full tar-entry versus
original inode count (446,872), and SHA-256 checks before owned fixture removal.
Original journals, TLS/follower fixture, logs and source/binary fingerprints
remain external. Only critical metrics and evidence references are tracked here.

The candidate needs its own unchanged complete audit. The original target remains
unestablished: 2,000 resident Cells, 10K durable writes/s, 50K owner-ordered reads/s,
three 30-minute target-hardware windows and owner-loss/follower-only recovery.

[Critical metrics and evidence hashes](2026-10-05-follower-verified-r12-failed-control.json),
[native validation-reuse evidence](2026-10-05-follower-verified-reuse.md), and
[qualification requirements](node-capacity.md).
