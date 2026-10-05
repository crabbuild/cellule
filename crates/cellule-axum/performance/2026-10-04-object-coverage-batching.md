# Coalescing completed object coverage

Concurrent independent Cell roots now share node-log coverage updates while an
authority CAS is in flight. There is no batching timer. Each exact ticket remains
staged until the authority update and fresh node-lease check succeed; local
proofs and contiguous truncation coverage advance together. Failed or cancelled
updates retain original tickets for retry, including shutdown. Unpublished gaps
remain uncovered.

The deterministic regression reproduces 64 independent publishers completing
during authority I/O. The baseline performs 64 updates; the candidate performs
two. This proves the mechanism, not a service throughput multiplier. Rejected
batches, cancelled callbacks, mixed scopes, fencing and out-of-order roots are
covered by tests. All 117 node tests and 613 runtime library tests pass; three
existing library tests remain ignored. Strict runtime/Axum Clippy and release
builds pass in the isolated source snapshot.

## Matched development runs

Both pairs use 16 Cells, eight SQL workers, 64 HTTP clients, a queue of 256,
30-second warmup, and offered rates of 100 writes/sec and 10 reads/sec. Owner
admission stays at 1 GiB disk, 512 MiB native memory and 16 MiB retained cuts.
Each point has fresh object keys and empty persistent follower stores, real
RustFS, two fsynced followers, pinned mTLS and original-session authorization.
Server source changes only the framework coverage path; driver binary hashes
match. No build or test runs concurrently with a measurement.

The first pair runs baseline then candidate for 120 seconds each. The repetition
runs candidate then baseline for 180 seconds each. TPS counts successes inside
the requested window. HTTP latency covers every issued request; scheduled
latency additionally includes generator queue wait. Drops remain losses.

| Metric | Baseline, 120 s | Candidate, 120 s | Baseline, 180 s | Candidate, 180 s |
| --- | ---: | ---: | ---: | ---: |
| Successful writes/sec | 85.242 | 96.483 | 88.594 | 95.361 |
| Write HTTP p50 / p99, ms | 354.6 / 2,739.3 | 113.6 / 1,928.7 | 323.4 / 3,283.5 | 102.5 / 1,414.5 |
| Write scheduled p50 / p99, ms | 1,056.9 / 8,230.4 | 127.0 / 5,384.3 | 954.0 / 7,289.2 | 107.1 / 5,615.3 |
| Write queue drops | 1,482 | 342 | 2,052 | 832 |
| Warmup write queue drops | 296 | 0 | 0 | 517 |
| Successful reads/sec | 8.483 | 9.642 | 8.844 | 9.489 |
| Read HTTP p50 / p99, ms | 148.3 / 1,808.2 | 39.0 / 884.9 | 122.3 / 1,786.9 | 31.4 / 783.3 |
| Read queue drops | 161 | 39 | 208 | 92 |
| Owner peak RSS, bytes | 112,340,992 | 125,362,176 | 117,469,184 | 119,521,280 |
| Cold-verified writes / reads | 13,238 / 1,309 | 14,674 / 1,461 | 18,964 / 1,892 | 19,667 / 1,950 |

All four points have zero issued-request and warmup errors, zero failed follower
append batches, and zero rejected/unavailable/unsupported submissions. Every
Cell drains to Idle, restores under a fresh owner boot, replays original mutation
identities and receipts, and passes its next-write and fencing-epoch check.
Cold populations include seed, warmup and drain. This establishes graceful
object-root recovery; owner-crash follower-only recovery is a separate gate.

The two observed TPS increases are 13.2% and 7.6%; HTTP write median falls about
68% in both pairs, and p99 falls 29.6% and 56.9%. These are diagnostic observations
from a shared eight-vCPU Linux ARM64 VM with 16,732,606,464 bytes RAM. Owner,
driver and followers share an eight-core/12-GiB container; RustFS has four cores
and 2 GiB on the same VM. Other workloads are uncontrolled. Two pairs provide
no confidence interval or isolated owner-capacity qualification. Neither
candidate sustains every offer; completion intervals remain uneven.

## Remaining work and evidence

The candidate completes more roots: cumulative publication counts change from
4,055 to 11,607 and from 5,179 to 15,965. Coalesced ranges become smaller as
publication catches up, increasing preparation and metadata work per command.
Compaction mean rises from 135 to 346 ms and from 154 to 275 ms. These phase
populations include seed, warmup and drain and differ between binaries; they
identify the next profiling targets, not an isolated compaction regression or
a precise attribution of service latency.

Next, submit every ticket covered by one coalesced Cell root together, profile
capture/compaction and publication admissions, and address pessimistic disk
reservations before increasing Cell counts and offered rates. Keep the original
[sustained qualification](node-capacity.md): 2,000 resident Cells, 10,000 durable
writes/sec and 50,000 owner-ordered reads/sec remain unqualified.

The [dataset](2026-10-04-object-coverage-batching.json) retains configurations,
source/binary hashes, exact maxima and histogram overflows, per-Cell successes,
phase counters, completion intervals, resource peaks and original journal
hashes. Raw logs and source artifacts remain outside the checkout. A preliminary
baseline using previously populated follower stores had one append failure and
then object fallback; its original cause is unestablished, no cold audit runs,
and it is excluded from the matched empty-store comparisons. Its failed gate
and counters remain in the dataset.
