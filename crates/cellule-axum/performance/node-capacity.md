# Node capacity target

Target: one Cellule owner process on an **8-vCPU, 16-GiB node**, with
**2,000 resident independent SQLite Cells**, **10,000 aggregate durable
writes/second**, and **50,000 aggregate owner-ordered reads/second** under uniform
load. This is a target, not an established capacity claim. Record latency
p50/p95/p99 and maximum; the target does not yet specify an absolute latency SLO.

## Measurement

`http_capacity` drives the SQL Axum example with scheduled arrivals through a
bounded queue. Every Cell starts empty and receives a seed write. Measured
writes insert distinct IDs; reads verify the acknowledged seed rows. Every
successful response must match the exact output and Cell/incarnation-scoped
minimum receipt. No uncertain request is retried with a replacement identity.

```json
{
  "address": "127.0.0.1:3000",
  "cells": 2000,
  "concurrency": 256,
  "queue_capacity": 4096,
  "write_rate": 10000,
  "read_rate": 50000,
  "warmup_seconds": 30,
  "seconds": 1800,
  "evidence_directory": "/evidence/capacity-run"
}
```

Build `sql` and `http_capacity` in release mode from an isolated source snapshot.
Start `sql` with `CELLULE_AXUM_CELLS=2000`, `CELLULE_AXUM_WORKERS=8`, the
loopback listener, and a fresh real-S3 prefix using the documented example
credentials. Run `http_capacity CONFIG.json` on the same loopback network.
The evidence directory must be new. Local files are temporary; bootstrap and
restore still use ordinary authority and publication paths.

For 2,000 resident writers, reserve at least 16,000 descriptors plus sockets
and temporary artifacts. The density probe uses an OS soft limit of 32,768.
The example sets a 512-MiB native admission ceiling with pressure headroom;
this reserves no actual memory and is not an RSS bound. Runtime pressure
thresholds and the separate 16-MiB retained-cut and 1-GiB disk budgets remain
unchanged. Record actual RSS, descriptors and disk use under load.

`scripts/bench-node-capacity.py --binary SQL_BINARY --driver CAPACITY_BINARY
--output NEW_DIRECTORY --cells 16 64 256 2000 --write-rates 1000 3000 10000
--read-rate 50000`
coordinates each point, refuses a second live owner, drains the service, cold
restores every Cell, replays and reads every acknowledged write with its original
identity/receipt, and checks a next write and increased fencing epoch per Cell.
It streams audits in bounded batches. Use `--seconds 1800 --warmup-seconds 30`
for sustained windows. Existing 16-Cell benchmark profiles are unchanged.

| Evidence | Meaning |
| --- | --- |
| Planned/generated offers, unissued offers, queue drops | Exposes generator starvation and overload; none count as successful TPS |
| Successes within window | TPS denominator is the requested measurement duration; drained completions do not inflate TPS |
| Scheduled latency | Includes driver queue wait and HTTP request/validation time |
| Request latency | HTTP dispatch through full response/receipt validation |
| Per-Cell successes | Shows actual coverage rather than catalog count alone |
| Warmup attempts/errors/drops | Retained separately from the measurement |
| JSONL journals | Original mutation identities and all observed terminal/uncertain outcomes, streamed per client |
| Histogram overflow and exact maxima | Percentiles beyond the finite histogram are null, never clipped into a passing value |

## Required qualification

First use short diagnostic runs at increasing offered rates and Cell counts.
Then run at least three 30-minute steady windows on the target hardware. Offer
enough traffic to measure 10,000 successful writes/sec and 50,000 reads/sec within
the window. Retain errors, overload and regressions. Compare unchanged workloads
and budgets before and after each optimization.

Record the exact source and binary hashes, hardware/CPU allocation, whole-node
memory and peak RSS, file descriptors, local disk and retained bytes, object
provider resources, SQL/publication admission waits, follower fsync latency,
and backlog stability. Bound and observe all accepted work through drain.
Provider and follower resource costs must be reported separately from owner
capacity; a shared development VM cannot prove independent-node capacity.

Performance success alone is insufficient. Independently audit all retained
acknowledgments, verify every Cell after fresh cold recovery and exact identity
replay, and exercise owner loss, fencing and a next recovered write. Existing
qualification profiles remain unchanged. The current SQL example uses object
proofs; adding a follower-backed HTTP assembly is a separate required step,
using the existing framework durability gate and real fsynced follower logs.
