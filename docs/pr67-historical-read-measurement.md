# PR 67: historical-read experiment and architecture gaps

**No performance improvement is accepted.** Experimental commit `399e908` groups
fresh historical verification reads, but regresses Fleet completion from 201.82
to 18.18 writes/s and fails ACK availability. It is reverted by `2257384`.
Production Rust source is byte-identical to the measured baseline `11843f6`.
PR #67 remains a draft; performance parity and merge gates remain unmet.

## What the experiment established

The before regression makes 64 historical requests for contiguous native extents
from 64 Cells. The candidate makes one, retaining fresh origin observation,
exact frame digests, scope checks and per-Cell sequence/checksum continuity.
Cold SQLite reconstruction preserves all expected results. Separate tests check
corruption before authority CAS, overlapping reads, cancellation and bounded
verification metadata. These establish request grouping, not application speed.

The first 24-MiB permanent reservation fails the unchanged 32-MiB managed-runtime
test with publication backlog. A lazy planner reduces it to 23 MiB, versus the
baseline's 20 MiB. That version passes all eleven contributor routes plus
Rust 1.99 Clippy: 1,966 workspace tests and 60 local LTX tests pass, with
38 environment-dependent tests ignored. Initial compile mistakes and the failed
24-MiB test are retained externally. Passing these checks did not predict the
application regression.

## Matched diagnostic

Six new sequential cases compare `11843f6cfb97ea86fb2647a37478039dbfbe0f54`,
`399e908336490063bd0d12bf0383509215225a7d`, and celld
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. All use 1,000 uniform Cells,
96-byte SQL values, INSERT plus SELECT, a two-hour outcome/retry ledger,
128 clients/queue slots, 30-second warmup and a 60-second measured window.
Fleet offers 15K writes/s with two followers; Bucket offers 2K/s.
Client/auditor binary, fixture, image, host and runner provenance match.

The ARM64 Docker VM shares 8 CPUs and 8 GiB RAM across the cluster. Serving
containers have 8-CPU/16-GiB ceilings and 4-GiB tmpfs; ceilings exceed VM resources.
The 64-MiB retained-work and 1-GiB managed-disk budgets remain unchanged.
No build or contributor suite overlaps the timed windows. An initial six-case
setup attempt fails the original one-million-free-inode precheck and produces
no TPS result. Expanding the dedicated VM disk from 120 to 240 GiB retains old
data and restores free inodes. Every measured arm runs after that expansion;
CPU, RAM, workload and gates are unchanged. Both attempts are retained.

TPS counts successes completed inside the window. Successful scheduled p99
includes trailing measured successes and excludes errors/drops; request p99
starts at issuance. Qualification also examines all-attempt latency.

| Mode / system | Completed writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / baseline | 201.82 | 1,648.38 | 1,121.67 | 305,973 | 581,854 |
| Fleet / withdrawn candidate | 18.18 | 774.41 | 505.40 | 860,359 | 38,550 |
| Fleet / celld | 4,362.33 | 74.18 | 41.03 | 8,836 | 629,424 |
| Bucket / baseline | 277.25 | 4,254.18 | 3,731.24 | 0 | 103,109 |
| Bucket / withdrawn candidate | 284.88 | 3,665.12 | 3,206.01 | 0 | 102,651 |
| Bucket / celld | 1,288.35 | 1,366.52 | 448.48 | 0 | 42,443 |

Fleet completion regresses about 91%. Bucket's +2.75% single-pair difference is
not an attributable gain: this adapter bypasses the managed producer being
changed. Every point fails qualification. These overloaded counts do not
establish sustainable capacities or capacity ratios.

| Mode / system | Complete ACK cohort | Warm errors / retries checked | Cold read/retry | Successful drain seconds |
| --- | ---: | --- | --- | ---: |
| Fleet / baseline | 22,846 | 15,954 / 6,892 | not reached | absent |
| Fleet / withdrawn candidate | 16,032 | 16,032 / 0 | not reached | absent |
| Fleet / celld | 464,790 | 464,790 / 0 | not reached | absent |
| Bucket / baseline | 28,081 | 0 / 28,081 | all pass | 9.30 |
| Bucket / withdrawn candidate | 28,443 | 0 / 28,443 | all pass | 5.36 |
| Bucket / celld | 126,604 | 0 / 126,604 | all pass | 1.16 |

The candidate's sampled audit failures are GETs. Its owner logs report heartbeat
fencing and drain retrying `Shared(Capacity("resource ledger"))`. The additional
permanent reservation is a headroom hypothesis, not an isolated attribution of
every failure. Celld Fleet is OOM-killed, exit 137. Missing cold audits and joined
drain are unverified gates, not evidence by themselves of mutation loss.

## Architecture evidence and next action

The baseline still makes 13.43 GET/range attempts per completed Fleet write,
materializes 12.77 commands per selected root, and ends at 61.15 MiB of the
64-MiB retained-work budget. Oldest publication debt reaches 45,523 ms.
Its mean native capture is 0.186 ms, worker phase 1.180 ms, Fleet proof phase
6.499 ms, Fleet response phase 420.481 ms and publication phase 52,719 ms.
These samples cover different overlapping cohorts and cannot be added as CPU
cost. Bucket materializes about one command/root and makes 3.60 PUT attempts
per completed write. The expensive path is publication and retained work.

Celld's [bundle and shipping implementation](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/ltx_repl.rs)
collects dirty Cell tails into node bundles, credits coverage without immediately
advancing per-Cell materialized roots, and pipelines ordered shipping rounds.
Its [follower stream](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/node_log.rs)
groups already delivered appends before the fsync chain. Cellule still awaits
one shipper batch before the next, repeatedly verifies live historical/base
dependencies, and its Bucket adapter publishes through the per-Cell path.
Cellule's managed SQLite sessions already use WAL NORMAL.

The next experiment must bound verification scratch and metadata within the
existing working credit, pre-admit selected receipt metadata, and preserve
progress headroom through root/checkpoint overlap. Reproduce the resource-ledger
failure before changing it. Pressure must reject new mutations before SQL while
allowing authenticated existing outcomes to be read/retried. Then measure ordered
follower pipelining and Bucket shared selection. None of these proposed changes
has a new qualified TPS result.

Merge still requires zero-error/drop sustainable capacity, bounded debt,
successful all-ACK warm/cold recovery and joined drain, failed-owner full issued
suffix recovery, safe collection, three paired five-minute repetitions and
read-only/mixed guardrails. The 2,000-Cell / 10K-write / 50K-read target remains
unqualified.

## Evidence

Raw journals, binaries, frozen sources, failed attempts and provider observations
remain outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`.
Canonical reports and independent journal replay reconcile all six measured
cases, every offer/attempt/completion and every complete ACK cohort. The external
`historical-ranges-20261008-evidence-index.json` inventories and hashes the retained
material: 17,318 files / 2,153,161,698 bytes independently rehash. Its SHA-256
is `65c4706d2c9acdb789d0c17b2e2be5f24f2c51d72e1191db16283f0ce8782b8c`.
The withdrawn implementation remains reproducible by its pinned commit.

[Previous selection-readiness measurement](pr67-selection-readiness-measurement.md),
[runtime design](../crates/cellule-runtime/docs/write-performance-design.md),
[delivery](write-performance-delivery.md).
