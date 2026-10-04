# Inline root preparation

Baseline `c78ea0f`; candidate `ddd8770`. [Dataset](2026-10-04-inline-root-preparation.json).
A tail of up to 32 segment descriptors is authenticated inside the existing
bounded root, removing an external descriptor object for small roots.

| Preparation fixture | Baseline PUTs / HEADs | Candidate PUTs / HEADs |
| --- | ---: | ---: |
| First small root | 5 / 0 | 4 / 0 |
| Warm small-root append | 5 / 2 | 4 / 1 |
| Native pressure append | 9 / 2 | 8 / 1 |
| Schema migration pressure append | 9 / 2 | 8 / 1 |

Counters stop before authority CAS and later verification. Pressure counts
include mandatory lineage retention. No checks, admission budgets, or durability
barriers were removed. Full 96-descriptor page boundaries remain stable for reuse.

Fresh-origin recovery reproduces exact database bytes at 32, 33, 96, 97, 128,
and 129 descriptors. Missing cached metadata and malformed inline descriptors
prevent successor uploads. Native and lineage upload pause tests cover both
representations and keep readiness and authority unchanged until both finish.
Existing exact-root, failure, and cancellation checks pass.

Isolated runtime/LTX verification passes 1,277 tests; local LTX without replication
passes 54. Targeted Clippy, workspace API docs, format and architecture checks pass.
The dataset records source equality, log hashes and fixed-observer admission.
New-source workspace/MSRV CI and provider/process qualification now pass; see
[correctness qualification](2026-10-04-inline-root-qualification.md).

This updates the one current development-v1 schema atomically. Earlier development
roots require recreation under the [format policy](../../cellule-runtime/docs/storage.md#format-policy).
The 32 KiB root, 64 KiB page and 4,096-descriptor graph limits remain enforced.

These operation counts do **not** establish an HTTP throughput or latency gain.
Completed three-pair 120-second RustFS comparisons at 1, 4 and 16 Cells are
tracked separately: [inline versus composed](2026-10-04-rustfs-inline-root-writes.md)
and [full change versus main](2026-10-04-rustfs-inline-root-main-writes.md).
Earlier sustained results remain separate evidence.
