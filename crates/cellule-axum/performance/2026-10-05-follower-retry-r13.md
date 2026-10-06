# Balanced 2,000-Cell write run: R13

Source `ff0d79b` passed the original development gates on a fresh RustFS
fixture. All 2,000 Cells cold-restored, replayed 103,180 original write identities
and 1,014 reads, and accepted exactly one next write at a higher fencing epoch.
No request, publication or follower-append failures were observed.

| Measured window | Result |
| --- | --- |
| Duration / warmup | 600 s / 30 s |
| Cells / clients / queue | 2,000 / 64 / 256 |
| Offered write / mixed-read TPS | 1,000 / 10 |
| Completed durable write TPS | **158.085** |
| Write latency p50 / p95 / p99 | **332.67 / 875.34 / 1,698.11 ms** |
| Completed mixed-read TPS | 1.5833; this does not establish read capacity |
| Write / read offers dropped by the driver | 504,829 / 5,050 |
| Owner peak RSS / descriptors | 685.45 MiB / 16,156 |

Offers remain round-robin across Cells; admission drops can skew completions.
Measured writes including drain ranged from 32 to 67 per Cell, with a mean of
47.59; the largest Cell accounted for 0.0704% of completions.
Write completion TPS per successive minute was 168.83, 144.72, 129.58, 141.53,
167.30, 176.07, 184.73, 218.15, 145.07 and 104.87. The average is not proof of
stable capacity. Latencies use raw nearest-rank measured attempts including
drain; TPS counts completions inside the window. Response-body throughput and
the exact metrics are in the adjacent JSON.

The remaining shared work is substantial. Over a 297.22-second interval, each
unchanged follower process added approximately 54.67 GB of process read bytes
and 8.72 GB of process write bytes. Its near-end lifetime totals were 82.45 GB
read and 15.74 GB written. These observations are consistent with repeated
open-log scanning and prefix rewrites; they do not provide exclusive phase
attribution. Lifetime dirty-admission and publication means were 8.48 s and
7.39 s, with histogram overflow. Phase populations overlap and differ from
request latencies; do not subtract their means.

The source archive, release binary, original SQL/workload and driver hashes
were checked before dispatch. The provider archive passed gzip integrity,
full tar enumeration of all 711,541 original inodes, and SHA-256 verification
before the owned fixture was removed. The adjacent JSON records hashes and
external raw-evidence references.

This is a candidate-only diagnostic on a shared 8-CPU/16-GiB Colima VM. Owner,
two followers and driver share an 8-CPU/12-GiB container; RustFS has a
4-CPU/6-GiB ceiling on the same VM. The previous
[R12 control failed](2026-10-05-follower-verified-r12-failed-control.md), so it
is not a passing paired baseline. No comparative gain or dedicated-node
capacity qualification is claimed. The original three 30-minute target-hardware
windows, 10K write TPS, 50K owner-ordered read TPS and owner-loss/follower-only
recovery qualification remain open.

Next: bound live open-log scanning and pruning rewrites, then examine
publication admission lifetime, retaining independent Cell state and the
original durability, identity and recovery gates.
