# Fair quiet-compaction admission

`8fb50ac` queues a bounded number of quiet Cells fairly for the existing dirty
and recovery permits, before excluding their publisher. New commands, fencing
and shutdown cancel admission. Dispatched native work retains its permits and
is joined. No resource limits or durable response gates change.

A public runtime regression continuously writes to 32 independent foreground
Cells while a quiet Cell needs compaction. The preceding source fails three
runs; the change passes three runs. A separate cancellation case drains and
shuts down while both recovery permits remain externally held. Exact restore
and returned permits are checked. Full runtime/LTX suites passed before the
final replica boxing; the frozen boxed source passes the five compaction cases,
strict Clippy and API docs.

The first two matched 600-second comparisons are invalid. The first candidate exhausts
Docker ext4 inodes with roughly 47 GiB still free. After verified archive cleanup,
the second candidate has ample inodes but fails the original follower-durability
gate: one append fails, stopping shipping; 43,367 later submissions are rejected.
Follower 2 reports `PeerAuthorization("invalid capacity log request")`.
The specific validation condition is not recorded in that frozen source.
Its commands switch from follower to object publication, so it cannot support
a performance gain claim. Both failed providers and original journals remain
preserved. Neither candidate reaches the canonical cold audit.

| Earlier R5 baseline (`03b9002`) | Value |
| --- | ---: |
| Resident Cells | 2,000 |
| Write TPS | 129.10 |
| Write HTTP p50 / p95 / p99, ms | 320.83 / 1,722.74 / 3,611.51 |
| Write response-body throughput, MiB/s | 0.0248 |
| Read TPS (sparse mixed load) | 1.078 |
| Read HTTP p50 / p95 / p99, ms | 5.72 / 70.20 / 516.44 |
| Write / read offers dropped | 522,222 / 5,353 |
| Request errors | 0 |
| Original writes / reads cold-audited | 85,503 / 702 |
| Owner peak RSS, MiB | 733.41 |

Baseline write TPS per minute is 160, 205, 199, 117, 66, 49, 73, 176, 117 and
130. A single average hides this variation. Response-body throughput excludes
request and provider traffic. The preceding baseline is retained separately in
the dataset; cleanup changed the fixture state, so it is not reused for the
second comparison.

This is a shared 8-vCPU/16-GiB Colima development fixture, with RustFS limited
to 4 CPUs/6 GiB. It offers 1,000 writes/s and 10 reads/s for 600 seconds after
30 seconds of warmup, using 64 clients, queue capacity 256, 16 SQL workers and
8 Tokio workers. Sparse mixed reads do not establish read capacity. All original
receipt, drain, cold replay, next-write and epoch assertions remain enabled.
It does not establish the 10K-write/50K-read target or owner-loss qualification.

Two older, stopped, owned providers were completely archived outside Docker.
Gzip integrity, complete tar entry counts against quiesced source inode counts,
and SHA-256 hashes passed before removing their containers and volumes. The
second matched comparison checks at least two million free inodes and 15 GiB
before each point, samples both throughout load, and archives each complete
provider between points. The second baseline archive preserves all 1,005,447
quiesced source entries with matching integrity and hash checks. The failed
second candidate is also archived with all 1,022,389 source entries verified;
its owned volume was removed only after complete integrity and hash checks.

The transport now distinguishes malformed, expired and excessively future
deadlines, and records the validation phase and bounded envelope timing when
a request fails. The next diagnostic run reproduces a backward wall-clock step of at least
82 ms during directory verification: its first check accepts the signed
horizon, then the second check observes `now_ms=1791230316817` and
`deadline_ms=1791230326899` and rejects it as excessively future-dated.
This run also fails the unchanged follower-durability gate and reaches no cold
audit; it supplies a causal diagnostic, not a performance result.

The correction retains the initial signed horizon and uses the greater of
current wall time and initial wall time plus monotonic elapsed time for the
second check. Rollback cannot extend expiry; forward wall-clock steps still
expire the request. All six SQL example tests and strict Clippy pass in the
isolated snapshot, including backward/forward-clock and expiry-boundary cases.
Signatures, frame limits and qualification gates are unchanged. The correction
also passes all six SQL example tests and strict Axum Clippy for all targets and
features on current main plus the fix (`3290ec8`), in a fresh isolated snapshot.

With the same application clock correction in both frozen sources, R7 completes
both 600-second points and passes the original follower, receipt, cold replay,
next-write and epoch gates for all 2,000 Cells. The control (`1672a95`) is
`03b9002` plus only the three follower application files from candidate
`4277e16`; the candidate includes fair framework admission. Source archives,
binaries, compiler, unchanged driver and SQL identity are pinned in the dataset.
These historical sources differ from current main, which has additional changes.

| R7 metric | Control | Fair admission |
| --- | ---: | ---: |
| Write TPS | 134.06 | 102.78 |
| Write HTTP p50 / p95 / p99, ms | 342.99 / 1,426.71 / 2,678.19 | 435.58 / 1,874.38 / 3,741.12 |
| Write response-body throughput, MiB/s | 0.0258 | 0.0198 |
| Read TPS (sparse mixed load) | 1.035 | 0.793 |
| Read HTTP p50 / p95 / p99, ms | 7.45 / 102.64 / 320.94 | 9.20 / 144.73 / 474.65 |
| Read response-body throughput, MiB/s | 0.000194 | 0.000149 |
| Write / read offers dropped | 519,245 / 5,378 | 538,014 / 5,523 |
| Request errors / follower failures / rejected submissions | 0 / 0 / 0 | 0 / 0 / 0 |
| Original writes / reads cold-audited | 88,908 / 680 | 70,469 / 543 |
| Owner peak RSS, MiB | 721.68 | 705.57 |
| Owner peak file descriptors | 16,165 | 16,163 |
| All-Cell startup, seconds | 47.29 | 84.35 |

TPS counts completions inside the window; raw nearest-rank request percentiles
include drain. The dataset retains per-minute TPS, scheduled latency including
queue delay and separate drain counts. High offer drops mean this fixture did
not sustain the offered rate. Sparse reads share the loaded queue and do not
measure read capacity.

The pair shows a 23.33% write-TPS regression. Lifetime root admission averages
3,999 / 5,802 ms, admitted root work 56.7 / 71.5 ms, SQL worker time 31.1 /
54.2 ms, capture 7.13 / 13.26 ms and provider PUT 34.4 / 44.3 ms (control /
candidate). Completed compactions are 2,279 / 1,990. Root-admission and
compaction histogram quantiles overflow and are unavailable. These phases
include setup, warmup and drain, overlap and have different populations; their
means cannot be subtracted to explain HTTP latency.

Root admission and provider I/O remain the bottlenecks. Startup, capture,
worker and provider timings also worsen, so one ordered pair on a shared VM
cannot attribute the whole regression to admission fairness. A subsequent
causal test must distinguish foreground/background admission and provider
variation while preserving the original resource caps and qualification gates.
This result supports no performance gain or target-capacity claim.

Both complete R7 providers were archived with matching source entry counts
(1,043,266 / 859,158), gzip/tar integrity and SHA-256 before removing their owned
containers and volumes. Original journals and all evidence remain outside
Docker. Inode preflight and sampling remain enabled.

The [dataset](2026-10-05-fair-admission.json) retains critical results and source
identities. Full logs, original journals, TLS fixtures, binaries, verification
logs and provider archives remain under its external evidence root. Performance
results stay here; the PR description contains behavior and validation only.
