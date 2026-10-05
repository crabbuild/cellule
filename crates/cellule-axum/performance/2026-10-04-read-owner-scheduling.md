# Read-only owner scheduling probe

A 64-Cell owner sustains 9,703.544 successful reads/sec in a 180-second window,
with zero read errors. It drops 53,345 offers and leaves 16 unissued, so it does
not qualify the offered 10,000 reads/sec. The workload has no measured writes;
64 seed writes establish rows and receipts. It does not prove the combined
2,000-Cell, 10,000-write/sec and 50,000-read/sec target.

| Evidence | Result |
| --- | ---: |
| Read HTTP p50 / p95 / p99 | 0.4 / 23.3 / 81.7 ms |
| Read HTTP maximum | 2,466.909 ms |
| Scheduled p50 / p99 | 1.9 / 683.4 ms |
| Owner peak RSS | 67,276,800 bytes |
| Owner peak descriptors | 815 |
| Owner CPU, including setup/warmup/drain | 1.089 cores |
| Mean actor wait / worker round trip / primitive work | 2,908.5 / 825.3 / 9.0 us |
| Cold-audited original reads / seed writes | 2,046,639 / 64 |

The CPU mean covers the full 211.125-second resource window, not only steady
state. Phase means have different boundaries and must not be added as causal
shares. Ten-second read completion rates range from 8,079.0 to 10,263.0/sec;
backlog and latency remain uneven despite low average CPU. All 64 Cells restore
under a fresh owner, original reads and seed mutation identities retain their
receipts, and each Cell accepts one next write with an increased fencing epoch.

The unchanged SQL/driver artifacts use eight SQL and eight Tokio workers,
256 clients, a 4,096-offer queue, 30-second warmup, and the existing 1 GiB disk,
512 MiB native memory and 16 MiB retained budgets. RustFS has four CPU/two GiB
limits; two real fsynced followers use pinned mTLS. Followers exercise the seed
writes: 64 fleet submissions, zero rejected/unavailable/unsupported submissions,
60 successful appends and no append failures. The proof race returns 57 seed
responses from followers and seven from objects. These counters do not describe
read durability or high-rate write capacity.

## CPU evidence and next check

An owner-only, user-space profile takes 1,207 samples at 99 Hz over 20 seconds,
with zero lost samples. Inclusive sample weights include 36.79% under the SQL
worker callback, 13.75% under runtime metadata reads (8.20% statement preparation)
and 8.62% under application SQL query execution. These weights overlap; they are
not independent percentages of total wall time. The small profile is exploratory
and excludes kernel work. Symbolization also consumed CPU in the shared VM during
measurement, so this point is not an unprofiled baseline for comparison.

Metadata reads compile a constant `SELECT` for every request. Application SQL
installs and removes SQLite authorizers around each batch; prepared-statement
reuse across changing policies needs explicit safety analysis. A generic cache
substitution must not let application SQL reuse a statement authorized for
protected runtime tables. Measure unprofiled repeats and scheduling stalls before
claiming a CPU or latency improvement. The
[write admission regression](2026-10-04-worker-admission-and-followers.md) remains
the next write-path change.

The [dataset](2026-10-04-read-owner-scheduling.json) tracks original source/binary
hashes, completion intervals, admission and error populations, resource and
storage health, cold proof gates, and profile artifacts. Raw journals, profiles
and process logs remain external. The
[node target qualification](node-capacity.md) remains active and unqualified.
