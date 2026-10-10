# Single-pass bundle encoding diagnostic

Measured October 10, 2026. Canonical shard encoding uses less CPU in the isolated
release replay. Two application pairs give conflicting throughput results.
**The 2,000-Cell mixed-load goal remains unqualified.**

## Change and compatibility

Baseline is main `aaed329e28236b6cb3314c40add0669809d180b1`; candidate is
`73d8a384b358e054859067d31b6a623f93c9b81d`. Both include the bounded catalog
shard cache. The candidate retains each touched shard's first canonical encoding
and fills its same-width history extent fields after assigning native offsets.
It avoids serializing every sibling control record twice. Plans exist only during
encoding and retain no authority or availability proof.

CNB3 bytes, 64-frame/4-MiB cohorts, admission budgets, complete local
self-verification, fresh origin verification and authoritative selection CAS are
unchanged. The 64-Cell/2,000-binding fixture compares every encoded byte, header
digest and resulting catalog against the original encoder over six captures,
then cold-restores every participating Cell. A separate test checks preserved
sibling metadata and rejects missing history and extent relayout.

## Isolated encoder result

The pinned Rust 1.98.1 Linux release replay alternates original and candidate
order on identical captures, excluding input cloning from the timer. Each encoder
runs 120 times across six captures; every comparison and final cold restore pass.

| Encoder | Mean ms | Median ms |
| --- | ---: | ---: |
| Original two-pass | 7.112 | 7.040 |
| One-use canonical plan | 5.856 | 5.818 |

Mean encoding time falls 17.7%. This measures one component and does not establish
an application throughput improvement.

## Full request results

Both pairs use fresh provider data and uninstrumented release binaries. The
second pair reverses the execution order and reuses the exact binaries.
Each run has 2,000 uniform Cells, 96-byte SQL-ledger values, a read-only phase,
then simultaneous offers of 2,000 writes/s and 20,000 reads/s. Each phase warms
for 30 seconds and measures 60 seconds, with 128 clients and 128 queued offers.

| Execution order | Completed writes/s | Completed reads/s | Write request p99 ms | Read request p99 ms | Dropped writes / reads |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1: baseline | 991.58 | 9,818.27 | 244.9 | 66.0 | 60,396 / 610,757 |
| 2: candidate | 897.75 | 9,216.67 | 293.5 | 82.2 | 65,998 / 646,881 |
| 3: candidate, repetition | 891.28 | 9,247.25 | 294.0 | 67.4 | 66,431 / 644,999 |
| 4: baseline, repetition | 771.82 | 7,063.63 | 441.5 | 92.7 | 73,577 / 776,057 |

These are overloaded completion rates. All four measured windows have zero
returned errors and zero producer-unissued offers, but drop queued offers and
miss the delivery and latency gates. The table's p99 starts when the client
issues a request and includes all attempted requests. Qualification uses scheduled-arrival
latency; those p99s also fail and remain in the machine reports.

| Run | Root-materialization debt at window boundaries, decimal MB | ACKs reconciled and checked warm/cold |
| --- | ---: | ---: |
| Baseline | 114.69 → 129.94 | 114,626 |
| Candidate | 109.04 → 141.12 | 107,208 |
| Candidate repetition | 94.37 → 129.99 | 102,084 |
| Baseline repetition | 99.40 → 127.51 | 96,733 |

All 420,651 ACK records across the four cases independently reconcile with raw
request journals. Warm and bucket-only cold reads and exact original retries
pass for every ACK. This does not qualify owner-loss or physical-device fault
durability. Root-materialization debt is distinct from pending bundle offsets;
its growth prevents a sustainable-capacity claim.

## Scope and remaining work

The owner has eight dedicated virtual CPUs and 16 GiB RAM in a 12-CPU/24-GiB
Linux VM. Followers, RustFS and client share the other four CPUs with fixed
quotas. RustFS retains its 2-CPU/2-GiB quota with transparent huge pages disabled.
Owner and follower state use tmpfs. Retained-work admission stays at 256 MiB,
managed disk at 1 GiB and the catalog cache at 2 MiB. Client/auditor binaries,
fixtures, images and resource placement match across arms. No build or heavy
audit overlaps timed work.

All 13 contributor checks pass, including 2,082 workspace tests with 43
environment-dependent tests ignored. The checked and measured candidate matches
all 1,356 Rust/Cargo files. Application throughput attribution remains open:
the first pair regresses and the reverse-order pair improves. Neither pair
reaches the goal. Publication is still serial and native-byte admission credit
remains held until authoritative selection.

The [performance proposal](write-performance-proposal.md) retains the sustained,
repetition, overload, recovery and device requirements. Raw journals, binaries,
failed prototypes, volumes, reports and source manifests remain outside Git under
`cellule-ios-parity-20261010`. The external evidence index is
`single-pass-encoding-two-pair-evidence-index.json`, SHA-256
`bd602525a07e922d9e2d857707a32e9a93d59dcc1218cf72fb5d5816fd7b2d86`.
