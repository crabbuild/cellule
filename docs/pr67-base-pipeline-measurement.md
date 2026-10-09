# PR 67: continuous base verification did not improve write TPS

**The trial is reverted; performance parity remains unmet.** The fresh unchanged
build completes 579.10 Fleet writes/s, the continuous-slot candidate 570.25/s,
and celld 1,999.83/s at the same 2,000/s offered load. Candidate throughput is
1.53% lower and successful scheduled p99 is 6.61% higher in this single pair.
Both Cellule cases fail warm ACK availability checks and never reach cold
recovery. This establishes no acceptable or attributable performance gain.
PR #67 remains a draft.

## Change and verification

The unchanged framework revision is `10370d20f52c0b2c6103b0df61c56a1252238d33`;
its production code is the preceding `4a5b001` implementation. Experimental
revision `2b6db11a21ef81e906a46b092e172f326840faab` replaces groups of eight
base reads with eight continuously refilled slots. Each slot retains one fresh
root through complete dependency verification; larger graphs remain serial.
The 4-MiB base allowance and 20-MiB producer admission are unchanged.

The real SQLite regression holds one first-group root, verifies later bases
can progress, cancels before authority selection and retries with fresh reads.
It fails three times on the unchanged verifier and passes three times on the
candidate. All 92 tests in the focused native bundle inventory pass. The
isolated candidate passes all 13 contributor routes: 1,986 workspace tests,
60 local LTX tests and both Rust 1.97/1.99 clippy checks. The 38 environment
dependent workspace tests remain ignored, not qualified.

A copied native Cargo cache initially reused the baseline binary. That attempt
is retained as invalid; distinct source and identical binary hashes expose it.
Forcing the changed crate to rebuild produces a different binary and the
passing results above. A controller's historical expected count of 100 was
applied to the wrong scope: `node::bundle::tests::` selects 92 tests,
while `node::bundle::` selects 100. The full workspace suite passes all 100,
including the eight additional index/proof tests. No tests or qualification
thresholds are dropped.

The trial is reverted because the application measurement supplies no
performance benefit. Its commit, sources, regression and failed measurements
remain available for review. Production source after the revert matches the
unchanged revision exactly.

## Fresh matched diagnostic

Both Linux release builds use the canonical pinned builder. The driver,
auditor, fixture bytes, images and loaded runner match; only four intended
framework files differ. Celld is pinned to
`f2bf648663a610eefde71f3547ad61e9b896b1f0`. Runs execute sequentially, with
2,000 uniform Cells, 96-byte SQL-ledger values, 128 clients/queue slots,
30-second warmup and a 60-second Fleet window. Both SQLite paths use WAL NORMAL.
No build, contributor check or independent journal audit overlaps a timed window.

| Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | ---: | ---: | ---: | ---: |
| Cellule unchanged | 579.10 | 416.19 | 54,145 | 30,984 |
| Cellule trial, reverted | 570.25 | 443.71 | 49,849 | 35,866 |
| celld | 1,999.83 | 35.24 | 0 | 0 |

The p99 cohort includes successful measured offers through client drain;
fast failed attempts do not reduce it. Warmup errors/drops are respectively
19,116/18,264 for the unchanged build, 7,560/26,503 for the trial and 0/0 for
celld. Every Cell has successful measured responses in every arm.

Independent replay reconciles every client attempt, original successful output,
in-window/trailing completion, per-Cell count, payload counter, dropped offer,
loaded identity and complete ACK stream. ACK counts are 59,492, 62,223 and
182,001. Cellule warm audits encounter 467 and 2,779 HTTP 503s respectively;
neither reaches cold audit. These are availability failures, not evidence of
lost data. Celld verifies all 182,001 mutations and original retries in both
warm and cold audits, and drains the original fleet in 16.55 seconds.

All canonical qualification reports fail. The shared VM has eight CPUs and
8,306,286,592 bytes of total memory across all roles; container ceilings exceed
it. Cellule's original 64-MiB retained and 1-GiB disk policies have no equivalent
celld fixture settings. These cases match workload and container ceilings, not
effective internal admission. A short overload observation is not dedicated
8-vCPU/16-GiB capacity, maximum celld throughput or physical-media durability.
Failed Cellule audits also leave required later lifecycle evidence incomplete.

## Architectural implication

The [publication diagnosis](pr67-publication-path-diagnosis.md) remains the
mechanism: publication capacity blocks global issuance before follower work,
while serial selection repeatedly verifies bases and history and competes with
checkpoints. The trial improves a component's scheduling under a held read but
does not remove this dependency or establish application TPS improvement.
Delivery must separate bounded recoverable native progress from publication,
pipeline ordered follower lanes with group commit, reduce repeated publication
work and preserve ACK read/retry availability under pressure. The
[capacity contract](../crates/cellule-runtime/docs/write-performance-design.md)
is unchanged; write/read/mixed and full recovery qualification remain open.

Fresh snapshots, builds, logs, journals, independent audits and the invalid
attempt records remain outside Git under
`/Volumes/Workspace/crabbuild-target/base-pipeline-20261009-independent`.
Earlier raw diagnostic directories disappeared; their committed reports remain
historical records, and missing raw evidence is not claimed to be reverified.
