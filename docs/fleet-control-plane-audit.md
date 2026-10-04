# Cellule control plane production design audit

Audit date: October 1, 2026, America/Vancouver.
Framework baseline inspected: `58721227e56dac3ebcda6b74b4ed2a514f8cc42b`.
Original plan SHA256: `4e31ac8b2b76aff1a454c1ba43d09c599d947e8f7346b4d67e74e44a0ab5816b`.

The original design provided a sound direction for fenced execution, but did
not yet specify a complete production product with low application-team effort
or a scalable execution path. The [revised plan](fleet-control-plane-plan.md)
resolves the identified design omissions with explicit contracts, source work,
dependencies, supported deployment boundaries and measurable release gates.
These are design resolutions. Implementation and production qualification
remain outstanding.

The review covered architecture, correctness of proposed coordination,
application integration, scale and resource bounds, lifecycle automation,
identity, operational recovery, API/UI behavior, rollout and release evidence.
It inspected the existing journal, roster, profile and reconciliation contracts.
It did not execute runtime tests, benchmark providers, certify security of
unimplemented endpoints, or audit unrelated application code. Concurrent native
reader work was treated as unqualified until its own evidence is recorded.

## Findings and required resolution

Evidence citing an original section refers to the plan fingerprint above;
those sections have now been revised. Framework links identify inspected source.
P0 means a safety prerequisite for the proposed production capability; P1 means
a production delivery or scale blocker. Effort describes implementation, not
the documentation edit. All findings below have high confidence as design gaps.

| ID | Priority | Original evidence and impact | Resolution and package | Effort and implementation risk |
| --- | --- | --- | --- | --- |
| A01 | P1 | Delivery contract and application layout offered a standalone reference example. No maintained distribution or support envelope completed the requested production product. | Supported binary/image, CLI, UI, worker SDK, deployment profiles and compatibility manifest; CP1/CP13. | L; medium, new product packaging and compatibility surface. |
| A02 | P1 | Architecture assigned enrollment, signing, observer, transport, journal and endpoint construction to embedding applications. Every team would repeat substantial fleet engineering. | Shipped SDK owns those facilities; teams supply existing providers and business hooks. One-engineer-day onboarding gate; CP1/CP12. | L; high, lifecycle integration must preserve one runtime and accepted work. |
| A03 | P1 | Controller ownership used one lease per fleet. [FleetProfile](../crates/cellule-runtime/src/fleet/operations/records.rs) and [operation limits](../crates/cellule-runtime/src/fleet/operations/mod.rs) cap the whole current scope at two moves and 8 GiB. This supplied no path to large-fleet progress. | Versioned execution partitions, per-partition leases and bounded fair scheduling under atomic aggregate permits; CP0/CP4/CP5. | L; high, partitioning changes authorization and budgeting. |
| A04 | P1 | Observation retained all terminal enrollments under the [10,000-row roster cap](../crates/cellule-host/src/fleet/roster/mod.rs). Repeated boot/role churn can eventually prevent collection regardless of current live fleet size. | Active closure indexes, immutable terminal archive/exclusion roots and exact replay lookup; ten-million-history qualification; CP3/CP12. | L; high, compaction must never recreate Pending from absence. |
| A05 | P1 | Whole-head/registry comparison and full page traversal in [roster collection](../crates/cellule-host/src/fleet/roster/scan.rs) were the only observer design. Continuous unrelated changes can repeatedly invalidate a fleet scan. | Consistent immutable snapshots, transactional deltas, checkpoint reconciliation and action-local revalidation; CP3. | L; high, equivalence of completeness and finalization barriers must be proved. |
| A06 | P1 | Management Cells and events specified one fleet read-model Cell and fleet-wide ordered sequence. These introduce serial hot paths and duplicate polling pressure without sizing evidence. | Bucketed Cell projections, per-partition ordering/cursors, shared inventory and bounded merge/read views; CP3/CP10. | M; medium, snapshot/split/replay correctness. |
| A07 | P0 | Partition parallelism was absent, so the original plan contained no shared budget transaction or cross-partition movement/reassignment contract. Adding more controllers without these would break global resource bounds. | One source-owned attempt, atomic crossing receiver reservation, assignment epochs and aggregate permits; CP4. | L; high, crash and stale-envelope races. |
| A08 | P1 | Overload/no-capacity handling ended with a request to add capacity. No supplied capacity reconciliation or safe provider removal protocol reduced that operator burden. | Desired pool capacity, qualified Kubernetes adapter, enrollment-based readiness and exact-instance drain/removal; CP8. | L; high, provider actions can remove the wrong instance without preconditions. |
| A09 | P0 | Controller maintenance had a one-node rule, but there was no fleet/failure-domain unavailable-capacity ledger covering concurrent drains and unexpected failures. | Atomic disruption permits with minimum service/role redundancy and actual unavailable capacity; CP4/CP8/CP9. | L; high, stale redundancy evidence or independent drains could violate availability. |
| A10 | P1 | API exposed individual requests but lacked a declarative fleet specification, bulk selector/cursor, rolling upgrade, retirement and precise post-acceptance cancellation workflow. | FleetSpec, dry-run/explain, durable parent operations, canary rollout and safe control commands; CP6/CP8/CP9. | L; medium, request replay and partial completion semantics. |
| A11 | P0 | Journal/node transport ownership did not specify how workers access authoritative transactions without broad database credentials. Making the new gateway depend on the controller's own completed enrollment would also create a bootstrap cycle. | Scoped gateway on all replicas, SDK remote journal adapter and independent authenticated bootstrap service; own controller boot uses the same local journal implementation; CP1/CP2. | L; high, identity and acceptance boundaries. |
| A12 | P1 | Mutual authentication and roles were stated, but identity issuance, renewal, revocation, returning stale peers, queued-user revocation and tenant resource isolation were unspecified. | Supplied workload identity/OIDC integration, exact boot credentials, reauthorization on adoption, quotas and endpoint policy; CP5/CP6/CP11. | L; high, trust lifecycle and scope isolation. |
| A13 | P0 | Backup restore only said to reconcile and fence old sessions. Restoring an old journal can also restore old controller epochs and omit accepted actions. | Independently anchored control-plane generation, exclusion of old writers/gateways, recovery mode and scope-local unknown-obligation barriers; CP0/CP11. | L; high, stale restore must not revive authorization or erase native effects. |
| A14 | P1 | Acceptance had no quantified large-fleet size/load/SLO/soak envelope; the initial 128-stream limit was unrelated to operator demand. | Small and large profiles, including 10,000 nodes/one million Cells, latency/freshness/fairness/takeover targets and 72-hour soak; CP12. | L; medium, targets require measured hardware and realistic native behavior. |
| A15 | P1 | Runbooks listed topics but supplied no automated preflight, dependency diagnosis, grouped alerts, credential/backup supervision or redacted support bundle. | Preflight/doctor/explain commands, service objectives, bounded traces, actionable alerts and automated housekeeping; CP6/CP11/CP13. | M; medium, diagnostics must be truthful and avoid leaking application data. |
| A16 | P1 | Partial capability stages could be delivered indefinitely while the product was called complete. Missing identity/migration/support artifacts had no release blocker. | Explicit baseline feature list, signed release/compatibility artifacts and machine-checked evidence ledger that fails on missing/skipped native gates; CP0/CP12/CP13. | M; medium, release enforcement across source and provider versions. |
| A17 | P0 | Health receiver exclusions were planner inputs only. Ordinary writer/read/follower acquisition could bypass the intended operational exclusion. | SDK installs canonical revisioned receive admission gates across every new-role path, independently of sticky maintenance and pressure; CP7. | L; high, must preserve existing owners and canonical recovery safety. |
| A18 | P0 | The single-runtime controller model did not explain how a three-node pool could manage many application FleetScopes without mixing boot identity, journal permissions or management Cell authority. | Dedicated management application/runtime and boot scope, explicit per-fleet service grants, worker scope preserved in every action, cross-fleet moves rejected; CP0/CP1/CP2/CP11. | M; high, identity isolation across applications. |

## Decisions preserved after review

- PostgreSQL remains the supplied production journal backend. This is an
  explicit tradeoff to keep atomic application transactions and independent
  controller bootstrap. A managed service and maintained adapter reduce team
  effort; a Cellule journal would require additional atomicity/bootstrap proof.
- Cellule hosts real management Cells with ordinary durability and recovery.
  They are rebuildable projections so their loss cannot block authoritative
  journal takeover. Their partitioning prevents a single projection writer from
  becoming the fleet's throughput ceiling.
- Runtime authority, leases and canonical recovery remain the data safety path.
  Health suspicion does not prove writer failure, and finalization still needs
  all native obligations and exact session withdrawal.
- The baseline has one journal transaction domain per installation. Regions
  have independent installations; cross-database/region Cell migration remains
  an explicit unsupported operation. Large fleet support within one supported
  installation is a mandatory release gate.
- Kubernetes is the fully supplied initial capacity/lifecycle platform. VM
  hosting uses the same binary, while automated provisioning is advertised only
  for qualified built-in providers. Application teams should not implement these
  adapters to obtain the baseline feature set.

## Remaining release evidence

All eighteen identified gaps now have a design decision, implementation owner
package and acceptance requirement. That does not prove that every possible
failure has been discovered. The revised plan requires canonical protocol models,
native fault tests, database and platform qualification, compatibility/restore
drills, measured full-scale behavior and independent operator onboarding before
production readiness can be claimed.

Record the final design fingerprint alongside each future implementation run.
Changes to partitioning, archive/closure evidence, restore fencing or disruption
accounting reopen their corresponding audit finding until the revised protocol
and qualification evidence are accepted. Do not close a production finding
solely because its documentation was updated.
