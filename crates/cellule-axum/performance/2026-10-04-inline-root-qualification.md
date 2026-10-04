# Inline-root correctness qualification

Workspace/MSRV CI and independent provider/recovery audits pass.
[Dataset](2026-10-04-inline-root-qualification.json).
Candidate `9d212463d1429e77147234d31e6b1ee84abe2f55` and actual CI checkout
`4a711819cd0ad85784ff7614d50256b55fd76fed` share tree
`5df075e68065e2a6c81adfe4e7bf0355a81a15b0`.

| Evidence | Critical result | Source |
| --- | --- | --- |
| Workspace/MSRV | All required steps pass; 1,721 workspace tests pass; 37 documented tests ignored | [CI](https://github.com/crabbuild/cellule/actions/runs/37192886964) |
| Object/follower qualification | Three fresh repeats each; 55,580 acknowledged writes; 627 raw files checked | [CI](https://github.com/crabbuild/cellule/actions/runs/37192887119) |
| Entity qualification | 80 Cells; 36 windows; 12,862 acknowledged writes; 377 raw files checked | [CI](https://github.com/crabbuild/cellule/actions/runs/37192886934) |
| Reader qualification | Four mixed windows; 1,500 writes; 284,478 reads; reader loss/replacement; 46 raw files checked | [CI](https://github.com/crabbuild/cellule/actions/runs/37192886934) |
| HTTP adapter | Four points; 48 acknowledged writes; exact live/cold replay and next writes | [CI](https://github.com/crabbuild/cellule/actions/runs/37192886964) |
| Contracts/applications | Contract/profile checks and 21 application scenarios pass | [Contracts](https://github.com/crabbuild/cellule/actions/runs/37192886952) · [Applications](https://github.com/crabbuild/cellule/actions/runs/37192886924) |
| Decoder fuzz smoke | Five configured 90-second targets pass without sanitizer findings | [CI](https://github.com/crabbuild/cellule/actions/runs/37192886943) |

Independent audits rerun existing raw-evidence verifiers, authenticate source and
role binaries, and check receipts, conditional ownership, follower durability,
failover, exact cold recovery, next writes and shutdown. Provider roles retain
one CPU, 1 GiB memory, no swap and no OOM. Reference checks verify 21 roles with
unique scratch volumes, clean exits and intentional reader loss/replacement.

Regression coverage includes stable descriptor-page boundaries, malformed
metadata, missing origin objects, exact restoration, native/lineage upload
barriers and cancellation. Development-v1 readers and writers change together;
earlier development roots require recreation. Qualification counts and HTTP
smoke are correctness proofs, not throughput comparisons. Sources, binary and
audit hashes are retained in JSON; raw artifacts remain linked through CI.
