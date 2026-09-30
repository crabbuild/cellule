# Executable performance plans

These plans were prepared against Cellule commit `dc387a8` on 2026-09-29.
Read the whole plan and its STOP conditions before execution. Keep raw provider
and process evidence outside the checkout.

| Order | Plan | Priority | Effort | Status |
| --- | --- | --- | --- | --- |
| 1 | [Cut forwarded routing work and prove the capacity gain](001-forwarded-routing-and-capacity.md) | P1 | L | DONE: bounded adapter hint and three paired comparisons; product ingress follow-up is external |
| 2 | [Measure the write-throughput limit](002-write-throughput-bottleneck.md) | P1 | M | IN PROGRESS: three object-proof measurement rounds; hosted-runner saturation remains inconclusive, follower lane pending |
| 3 | [Reduce the hot Cell publication critical path](003-hot-cell-publication-critical-path.md) | P1 | M+ | TODO: controlled baseline and subphase attribution required before a code change |

Status values: TODO, IN PROGRESS, DONE, BLOCKED (with reason), REJECTED (with
reason). Update the row after executing the plan.

## Dependency and scope notes

Plan 001 begins with measurement and keeps an explicit stop gate before adding
an owner hint. It covers the optional Cellule peer HTTP adapter. The embedding
application owns ingress routing; its integration work requires its own repo.
Plan 002 is independent of Plan 001 and measures the hot-Cell and fleet-wide
write limit. Plan 003 narrows the next target to serial object publication
and requires a controlled baseline and exact subphase attribution before
changing code or durability policy.
