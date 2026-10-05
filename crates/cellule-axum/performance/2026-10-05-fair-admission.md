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

Two matched 600-second comparisons are invalid. The first candidate exhausts
Docker ext4 inodes with roughly 47 GiB still free. After verified archive cleanup,
the second candidate has ample inodes but fails the original follower-durability
gate: one append fails, stopping shipping; 43,367 later submissions are rejected.
Follower 2 reports `PeerAuthorization("invalid capacity log request")`.
The specific validation condition is not recorded in that frozen source.
Its commands switch from follower to object publication, so it cannot support
a performance gain claim. Both failed providers and original journals remain
preserved. Neither candidate reaches the canonical cold audit.

| Latest fully audited baseline (`03b9002`) | Value |
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
second candidate remains stopped and intact.

The transport now distinguishes malformed, expired and excessively future
deadlines, and records the validation phase and bounded envelope timing when
a request fails. Limits, signatures, freshness checks and qualification gates
are unchanged. Its adversarial transport tests pass; further reproduction is
required to identify the failure condition.

The [dataset](2026-10-05-fair-admission.json) retains critical results and source
identities. Full logs, original journals, TLS fixtures, binaries, verification
logs and provider archives remain under its external evidence root. Performance
results stay here; the PR description contains behavior and validation only.
