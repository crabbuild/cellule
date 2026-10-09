# Node lease and failed density probes

Node-leased owners no longer publish periodic control renewals for each quiet
Cell. The previous three-second cadence requests about 667 control PUTs/sec at
2,000 idle Cells. This is an estimate from the runtime cadence, not an isolated
measured renewal rate. Node-session expiry already controls takeover; publication
still checks each exact owner, epoch and root.

The public actor regression fails on the old implementation: an idle Cell's
revision advances from 2 to 3 while its root stays unchanged. With `19752ad`, four
leased Cells retain unchanged controls, accept an ordinary next write, and refuse
query output and new SQL after fencing. Long preparation still checks its shared
lease, and an unleased runtime retains control renewal. Isolated release checks
pass: 619 runtime unit tests, 234 runtime integration tests, strict runtime/Axum
Clippy and the default SQL/driver build. Existing ignored tests remain ignored.

## Diagnostic runs

All points use 2,000 Cells, 16 SQL/8 Tokio workers, 64 clients, a 256-offer queue,
1,000 offered writes/sec and 10 reads/sec, with 30-second warmup and a requested
180-second window. Owner disk/native/retained admission remains 1 GiB/512 MiB/
16 MiB. The provider has four CPU/two GiB limits. Real fsynced followers use
pinned mTLS. Each point has a fresh prefix; the provider volume contains prior
points. The development VM also hosts the owner, driver and followers.

| Point | Source | Result |
| --- | --- | --- |
| r1 | `5d00d10` | Provider hits its 1,024-file soft limit; owner fences; zero successful measured writes or reads |
| r2 | `5d00d10` | Provider soft limit raised to 32,768; 2,000 SQLite paths observed; seed 89 returns an uncertain outcome after 88 valid seeds; no measured window |
| r3 | `19752ad` | All 2,000 seeds validate; measured window begins, then provider is OOM-killed with exit 137 |
| r4 | `19752ad` | Fresh empty provider volume; zero request errors; follower checks pass; cold startup fails with node pressure |

r2 records 81,616 owner PUTs across startup/setup/drain, not all attributable to
renewals. Provider logs show 59 slow rename operations, 50 on Cell controls,
taking 6.7–10.1 seconds. It records no EMFILE events or owner-observed transient
storage failures. r3 has 10,605 successful measured writes and 72 reads, but also
19,777 write errors, 195 read errors and extensive dropped offers. Neither point
completes cold audit; neither qualifies capacity or an HTTP speedup.

r3 samples 2,000 distinct open SQLite main/WAL/SHM paths through the workload,
with peak sampled owner RSS 717,201,408 bytes and 16,147 descriptors. This proves
observed tiny-database residency before provider failure; it does not prove
sustained residency, larger databases or the combined target. Successful-write
HTTP p50/p95/p99 is 208.7/3,271.7/14,917.2 ms. These exact journal percentiles
exclude fast failed responses, which otherwise make the failed run look faster.

r4 keeps the same image, CPU/memory limits, descriptors, workload and Cellule
budgets on a fresh empty provider volume. It measures 117.356 writes/sec and
0.906 reads/sec, with write HTTP p50/p95/p99 of 117.2/2,036.0/9,322.8 ms. It drops
158,554 write offers and 1,634 read offers and leaves six write offers unissued.
All 2,000 original Cells drain to Idle. The cold-start attempt records 978 root
opens before `Capacity("node pressure")`; that counter does not prove 978 completed
restores. The full cold audit and recovery qualification remain incomplete.

Healthy r4 publication telemetry records mean root-admission wait 3,408.6 ms,
admitted preparation work 45.9 ms, worker round trip 31.5 ms and quiet compaction
11,625.6 ms. These populations have different boundaries and must not be added
as causal shares. Investigate compaction's shared admission wait before it
claims a Cell, and instrument the actual recovery pressure ledger. Keep the
existing budgets, exact-root checks and response gates.

The [dataset](2026-10-05-node-lease-renewals.json) retains populations, latency,
source/binary hashes and external evidence references. Original receipts,
uncertain identities, provider logs and follower stores remain outside the
checkout. The historical provider volume remains retained; r4 uses a separate
fresh volume. r4 supplies a healthy-provider diagnostic, but its cold
pressure refusal still prevents qualification. The
[full qualification](node-capacity.md) remains open.
