# Composed compaction correctness qualification

The integrated source passes workspace/MSRV CI and independent provider proof
checks. [Dataset](2026-10-04-composed-compaction-qualification.json).

Candidate `ce93119794182c01c5d876568799cdb4dbe94628` and actual CI merge
`1ca9759282fb5b1d5a15059b8b235bf34c819f2e` share tree
`8cfad7e150ed12e1b22731f1933aa4a3c0cb1ad9`.

| Evidence | Critical result | Source |
| --- | --- | --- |
| Workspace/MSRV | All required steps pass; composition, recovery, lineage, dependency-failure and cancellation regressions ran | [CI](https://github.com/crabbuild/cellule/actions/runs/37187645416) |
| Object/follower qualification | Three fresh repeats each; 44,409 acknowledged writes; 597 raw files independently checked | [CI](https://github.com/crabbuild/cellule/actions/runs/37187645547) |
| Entity qualification | 80 Cells; 36 windows; 12,310 acknowledged writes; 377 raw files checked | [CI](https://github.com/crabbuild/cellule/actions/runs/37187645453) |
| Reader qualification | Four 60-second mixed windows; 1,500 writes; 281,737 reads; reader-loss/replacement proof; 46 raw files checked | [CI](https://github.com/crabbuild/cellule/actions/runs/37187645453) |

Independent checks rerun the existing raw-evidence verifiers and authenticate
source/build/role-binary identities. Provider qualification verifies one CPU,
1 GiB memory, no swap, no OOM, clean shutdown and withdrawn node sessions.
Reference qualification verifies kernel/Docker limits, unique scratch volumes
for all 21 roles, clean exits, and intentional reader loss/replacement.

These are correctness and resource-limit proofs. Counts are not paired throughput
gains; the scaling qualification uses independent processes with a shared fixed
provider. Source, binary and audit hashes are retained in JSON; raw artifacts
remain linked through CI.
