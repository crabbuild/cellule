# Worker concurrency, safe refusals and follower continuity

Two framework defects have deterministic regressions. A pre-SQL disk refusal
must preserve an owner whose earlier pending commit already has follower proof.
The old path returned `OutcomeUnknown` and fenced that owner although the
callback never ran. The new path preserves the refusal and permits an exact
identity retry; ambiguous and unproved cuts still fence, and shutdown still
drains publication. Completed node-log coverage also no longer retains one set
entry per historical frame: after 102,400 contiguous completions the sparse set
holds zero entries instead of 102,400. Entries above unresolved holes can still
grow. These fixes do not establish the node throughput target.

Isolated verification passes 36 durability integration tests, 618 runtime unit
tests (three existing ignored), strict runtime/Axum Clippy and release builds.
Source and binary provenance, red/green logs and raw journals remain external.

## Steady-state probes

Each point offers 100 writes/sec and 10 reads/sec across 64 Cells and clients,
with a 30-second warmup and 180-second measurement. It retains the 1 GiB local
disk, 512 MiB native memory and 16 MiB retained-cut budgets, real RustFS and two
fsynced followers over pinned mTLS. The VM is shared; no builds or tests overlap
measurement. The baseline points predate the RustFS volume reset, so this table
is diagnostic evidence, not a controlled before/after speedup claim.

| Point | SQL workers | Successful writes/sec | Write errors / drops | Successful reads/sec | Read errors / drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline plus pre-drain metrics | 4 | 93.167 | 0 / 1,227 | 9.289 | 0 / 128 |
| Same baseline binary | 16 | 70.106 | 5,366 / 0 | 7.128 | 515 / 0 |
| Safe-refusal/history fixes after storage reset | 16 | 99.217 | 140 / 0 | 10.000 | 0 / 0 |

| Point | Write HTTP p50 / p99, ms | Write scheduled p99, ms | Read HTTP p50 / p99, ms |
| --- | ---: | ---: | ---: |
| Baseline, four workers | 122.1 / 3,591.2 | 6,623.6 | 5.7 / 2,691.8 |
| Baseline, sixteen workers | 16.7 / 1,395.0 | See dataset | 0.4 / 887.8 |
| Fixed, sixteen workers after reset | 12.3 / 1,363.9 | 2,101.0 | 0.4 / 1,090.0 |

Histograms include failed attempts; smaller percentiles cannot independently
prove an improvement. Four workers pass the original cold audit for 19,837
writes and 1,972 reads, but queue drops prevent offered-rate qualification.
The other points fail before cold audit. Do not infer an optimal worker count
from one shared-VM point per setting.

## Remaining failures

The clean fixed run drains all 64 Cells to idle with zero publication failures,
but has one failed node-log append. Both followers report
`PeerAuthorization("invalid capacity log request")`. Subsequent submissions
include 15,463 rejections; responses are 3,923 follower proofs and 17,001 object
proofs. Their counters include seed, warmup and drain. This fallback prevents a
follower-backed capacity claim even though aggregate success nearly meets the
small offered rate. The exact validation reason requires targeted diagnostics.
The run retains at least 2,578,002 free inodes; storage exhaustion is not its
failure mode.

A second clean run adds only temporary error-source and signed-request
validation diagnostics in an isolated snapshot. It sustains 98.700 successful
writes/sec and 9.983 reads/sec, with 209 write errors, 24 write drops and three
read drops. Write HTTP p50/p99 is 42.0/1,539.0 ms; read HTTP p50/p99 is
0.8/1,099.9 ms. All 209 errors are
`InvocationError::NotStarted(Capacity("local disk bytes"))`. All 64 Cells drain
to idle. Submissions are 20,831 fleet and zero rejected/unavailable/unsupported;
10,654 appends succeed and none fail. Responses use 13,545 follower proofs and
7,286 object proofs. The earlier signed-request rejection does not reproduce;
its exact cause remains unresolved. Driver failure still prevents cold audit
and capacity qualification. Diagnostic sources and hashes remain outside the
checkout, and do not establish a controlled performance gain.

The transaction path still reserves 128 MiB before a default-limit write. Eight
concurrent reservations consume a 1 GiB envelope before existing files. This
is a reproduced admission ceiling, not evidence for every HTTP error's cause.
A deterministic isolated regression holds each admitted callback until all 16
independent Cells have either entered SQL or been refused. Only seven enter;
nine are refused before SQL. Peak reservations reach 939,994,848 bytes, versus
470,752 settled bytes before and 500,929 after the seven completed captures.
Every reservation releases on close. The requirement for all 16 tiny writes to
fit the 1 GiB budget fails in 0.61 seconds. This expected red regression is
preserved externally for the next implementation; it is not added to passing CI.
Actual-growth admission must retain capture capacity for committed cuts and
preserve rollback, oversized full-image capture and fencing contracts.

The earlier `durable-pending-16` probe exhausted Docker inodes and is excluded
from code performance comparisons. Its raw failures remain archived; see the
[storage reset](2026-10-04-docker-storage-reset.md). The
[dataset](2026-10-04-worker-admission-and-followers.json) preserves intervals,
error populations, exact maxima and overflows, proof-source counts and hashes.
The [2,000-Cell target](node-capacity.md) remains active and unqualified.
