# Managed bundle pipeline diagnostic

Measured October 10, 2026. Two fresh serial-baseline/pipeline pairs complete
more requests with the pipeline. Absolute rates vary, write p99 does not improve
consistently, and root-materialization debt grows faster in both candidate runs.
**The 2,000-write/s + 20,000-read/s target remains unmet.**

## CNB3 format

CNB3 is Cellule's binary node-bundle format. One immutable object contains:

| Region | Contents |
| --- | --- |
| Fixed 32-KiB header | Session, log epoch, predecessor, selected position, object length, 256 catalog-shard slots and new native extents |
| Changed catalog shards | Authenticated Cell bindings and references to their histories |
| Native frames | Encoded LTX replication data from the participating Cells |
| Detached histories | Authenticated per-Cell lists of native object/range/digest references |

Unchanged shards and histories keep their exact earlier references. A point
lookup loads the relevant shard and history instead of walking every prior
bundle. The authority-pinned header digest authenticates the referenced ranges;
recovery verifies each required dependency. Uploading the object alone grants
no publication credit: fresh verification and authority selection still apply.

The [encoder](../crates/cellule-runtime/src/node/bundle/index/dense/mod.rs)
defines the physical ordering; the
[header codec](../crates/cellule-runtime/src/node/bundle/index/codec.rs) and
[history codec](../crates/cellule-runtime/src/node/bundle/index/history.rs)
define the fields and bounds. The pipeline leaves these bytes unchanged.

## Full request results

Baseline is `d42b978c40a9c83c5bca0e8425c49e7134de7e4c`; candidate is
`4712c4cbbf3b3685368209e61d3dbc73e6dd06cb`. Both use single-pass encoding,
the 2-MiB catalog cache and the original 64-frame/4-MiB cohort bounds.
Each run activates 2,000 uniform Cells with 96-byte SQL-ledger values, measures
reads alone, then offers 2,000 writes/s and 20,000 reads/s simultaneously.
Each phase warms for 30 seconds and measures 60 seconds. The second pair reverses
execution order and reuses the exact binaries and fixtures with fresh provider
data. All four results are retained.

| Execution order | Completed writes/s | Completed reads/s | Write request p99 ms | Read request p99 ms | Dropped writes / reads |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1: serial baseline | 720.60 | 6,301.73 | 401.8 | 80.7 | 76,630 / 821,774 |
| 2: managed pipeline | 873.35 | 8,877.55 | 689.1 | 72.9 | 67,478 / 667,212 |
| 3: pipeline repetition | 1,038.80 | 10,897.50 | 268.3 | 60.4 | 57,597 / 546,022 |
| 4: baseline repetition | 614.35 | 6,475.47 | 708.2 | 102.9 | 83,012 / 811,341 |

| Run | Root-materialization debt, decimal MB | ACKs independently reconciled and checked warm/cold |
| --- | ---: | ---: |
| Baseline | 94.11 → 125.33 | 91,809 |
| Pipeline | 124.03 → 194.01 | 110,342 |
| Pipeline repetition | 130.28 → 206.52 | 122,043 |
| Baseline repetition | 110.37 → 162.89 | 90,684 |

The request quantiles cover attempted requests from issue time; the scheduled
quantiles also include client queuing. Dropped offers receive no response and
are separate failures. Every measured window has zero returned errors and zero
producer-unissued offers. These overloaded completion rates do not establish
sustainable capacity. Candidate scheduled-arrival write p99s are 699.9 and
283.2 ms; baseline values are 483.3 and 761.7 ms. All exceed the Fleet write gate.

| Read-only run | Completed reads/s | Dropped offers |
| --- | ---: | ---: |
| Baseline | 19,544.70 | 27,296 |
| Pipeline | 19,698.48 | 18,071 |
| Pipeline repetition | 19,681.40 | 19,084 |
| Baseline repetition | 17,550.60 | 146,925 |

## Stage evidence and limits

Native-byte admission's measured mean falls from 98.86 to 50.91 ms in the first
pair, while its p99 rises from 338.4 to 605.2 ms. In the reverse-order pair the
mean falls from 103.33 to 30.89 ms and p99 from 607.8 to 184.9 ms. Fleet-proof
means remain 33–44 ms. These histograms have different populations and cannot be
summed into a latency decomposition. The serving regression proves the overlap
path; these application windows do not count successor admissions or serial
fallbacks.

All four windows retain active required-follower proofs, valid provider lifecycle
and filesystem evidence, and independently reconciled client counters and
outputs. All 414,878 ACKs pass warm reads, bucket-only cold reads and exact
original retries after graceful drain. This does not qualify abrupt owner-loss
or physical-device fault durability.

The owner has eight dedicated virtual CPUs and 16 GiB RAM in the same
12-CPU/24-GiB Linux VM. Followers, RustFS and client share the other four CPUs
with fixed quotas. Owner/follower state is tmpfs. Admission remains 256 MiB,
managed disk 1 GiB, clients 128 and queued offers 128. Client/auditor binaries,
fixture sources and pinned compiler/provider images match across arms. No build
or heavy audit overlaps timed work. All 1,361 candidate Rust/Cargo files match
the isolated tested snapshot.

Both pairs show higher candidate completion rates, with substantial variation
between runs. They do not establish sustainable capacity. The unchanged
[qualification proposal](write-performance-proposal.md) still requires sustained
paired runs, low latency, complete delivery, stable debt, overload behavior and
fault/device evidence. The [managed integration checks](managed-bundle-pipeline.md)
cover correctness separately.

Raw journals, source manifests, binaries, provider volumes, reports and
independent audits remain outside Git under `cellule-ios-parity-20261010`.
The external comparison is `managed-pipeline-comparison-two-pairs.json`; its
evidence index is `managed-pipeline-two-pair-evidence-index.json`, SHA-256
`a4879ac8debc39a6c20840b4ed9271f1bad4246ef2681e892dd0b10d27436d1c`.
