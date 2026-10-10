# PR 67: release-build write measurement repeat

**Latest completion rates: 107.95 Fleet writes/s and 268.12 Bucket writes/s.
No acceptable performance improvement or celld parity is established.** Fleet
fails request delivery and warm ACK availability. Bucket passes its ACK audits
but misses throughput and latency targets. PR #67 remains a draft.

## Source and workload

The branch head at measurement was `a52fe28542f2839b30b077ca775adba7fe8b1809`.
Its differences from functional commit
`4a8f55cc99414e6a3907531c774a184842e1de3e` are documentation only. This repeat
uses that verified release binary, not the separate diagnostic logging build.
Celld remains pinned at `f2bf648663a610eefde71f3547ad61e9b896b1f0` and the
same image. No production changes were made during this measurement.

Four fresh cases ran sequentially: Cellule Fleet, celld Fleet, Cellule Bucket,
celld Bucket. Each has 1,000 uniformly selected Cells, 96-byte values, SQL
INSERT plus SELECT, a two-hour durable retry/result ledger, 128 clients and
128 queue slots. Each has a 30-second warmup and 60-second measured window.
Fleet offers 15,000 writes/s with two followers; Bucket offers 2,000/s.
Prefixes and provider volumes are fresh. No build or contributor suite overlaps
the timed windows. The owned Docker context is `colima-c67r2`.

The ARM64 Linux VM has **8 CPUs and 8 GiB total shared memory**. Serving
containers have 8-CPU/16-GiB ceilings and 4-GiB tmpfs; the client has a
4-CPU/4-GiB ceiling and RustFS 2 CPUs/8 GiB. These ceilings exceed VM resources.
The existing external provider adaptation, 64-MiB Cellule retention budget,
1-GiB managed disk budget and acceptance gates remain unchanged.

This SQL diagnostic does not qualify a dedicated 8-vCPU/16-GiB serving node,
the laptop KV workload, physical-device durability, or the 2,000-Cell goal.
Three paired five-minute repetitions, a sustainable capacity search and read
guardrails remain outstanding.

## Independently reconciled results

TPS counts successful logical writes completed inside the measured window.
The successful p99 below is replayed from request journals, includes trailing
successes and excludes errors and dropped offers. Scheduled latency starts at
the offered arrival; request latency starts at issuance. The qualification
report also checks all-attempt latency. These overloaded completion counts
are not sustainable capacities.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Measured errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / Cellule | 107.95 | 5,243.30 | 3,137.32 | 499,450 | 394,049 |
| Fleet / celld | 4,470.70 | 158.21 | 70.29 | 2,395 | 629,337 |
| Bucket / Cellule | 268.12 | 4,059.96 | 3,399.88 | 0 | 103,657 |
| Bucket / celld | 1,392.55 | 586.52 | 323.98 | 0 | 36,198 |

All four warmup request-error counts are zero, but all drop warmup offers.
Independent replay reconciles every 900,000 Fleet or 120,000 Bucket offer,
attempt, success, error, drop and window/trailing completion. Complete ACK
cohort counts and hashes match. Comparisons verify identical fixture,
load-generator, auditor, image, host and runner provenance. Every point fails
qualification; no failed case is omitted or silently retried.

The [previous measurement](pr67-checkpoint-continuity-measurement.md) of this
same Cellule binary completed 185.83 Fleet and 215.08 Bucket writes/s. The
repeat changes those counts by -41.9% and +24.7%. This variation cannot be
attributed to a new optimization. The earlier before/after pair against
`6d62d41` also failed availability/delivery and did not establish improvement.

## ACK availability and drain

| Mode / system | Complete ACK cohort | Warm errors / retry checks | Cold read/retry | Recorded successful drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / Cellule | 16,190 | 16,057 / 133 | not reached | absent |
| Fleet / celld | 463,838 | 447,870 / 15,968 | not reached | absent |
| Bucket / Cellule | 24,880 | 0 / 24,880 | pass: all 24,880 | 9.14 |
| Bucket / celld | 129,029 | 0 / 129,029 | pass: all 129,029 | 1.79 |

Cellule Fleet's warm audit returns HTTP 503 failures. Its owner exits zero
during cleanup, which does not supply a successful aggregate drain or cold
audit. Celld Fleet's owner is OOM-killed, exit 137; its audit records HTTP 500
failures. These establish unavailable audits, not proven mutation loss.
Required later provider health/lifecycle observations are missing for failed
cases and fail their gates. Passing Bucket cold checks follow graceful drain;
they do not qualify recovery of a failed owner's complete Fleet-ACK suffix.

## Reproduced failure path and remaining bottleneck

A separate, explicitly marked diagnostic build records **466 distinct Cells**
with `exact shared selection failed` / `Deadline`, plus 128 sampled HTTP 503
responses with `NotStarted(Capacity("publication backlog"))`. Its warm audit
fails all 16,059 checked ACKs. Those bounded logging counts identify failure
paths; its timing is not included in the release comparison above.

The current [selection task](../crates/cellule-runtime/src/cell/actor/materialization/mod.rs)
holds the Cell publisher while awaiting the producer's selected capture prefix
and wraps that wait plus worker cleanup in a ten-second timeout. A timeout
fences the Cell even when a Fleet proof has already granted ACKs. Root dispatch
also requires that publisher, so it cannot start for the Cell during the wait.
Separating selection readiness from admitted cleanup/root ownership needs a
regression with delayed selection, valid Fleet ACKs, read/retry visibility,
bounded capture debt and joined drain. Increasing the timeout alone does not
resolve that coupling. This diagnosis does not establish the sole throughput
bottleneck or demonstrate a fix.

The release Fleet window records 11.00 materialized commands/root, 0.7403
PUT attempts/completed write and 13.2234 GET/range attempts/completed write.
These are storage API boundary deltas, including work for earlier pending cuts;
provider SDK internal retries are not separately counted. Steady Bundle ACKs
remain zero. Retention grows from 27.80 to 59.72 MiB of 64 MiB, unpublished
node-log bytes grow, and oldest publication reaches 49,784 ms. Two boundary
samples do not establish bounded debt. The 215-command and conditional
0.05-PUT targets remain unmet. Bucket wiring still bypasses the managed producer.

## Evidence and verification

Production source is unchanged from the frozen snapshot whose eleven
contributor routes passed: 1,961 tests passed, zero failed and 38 ignored.
Those checks do not establish application throughput or availability.

Raw case journals, complete ACK cohorts, provider objects/logs, failures,
binaries, manifests and the diagnostic overlay remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`.
The retained repeat/diagnostic evidence index covers 1,559 files and
1,679,263,260 bytes; all entries were rehashed successfully. Index SHA-256:
`3e663130645404ae76d157de12bac01e5180a4a51322957cbfad3ffc90fb3944`.

The [delivery report](write-performance-delivery.md) and
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md)
retain the remaining availability, shared Bucket selection, publication cost,
failed-owner recovery, safe collection and full qualification requirements.
