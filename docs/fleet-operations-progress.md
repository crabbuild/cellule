# Fleet operations implementation evidence

The [implementation plan](fleet-operations-plan.md) remains the full scope.
This page records focused checkpoints; it does not establish complete fleet
balancing, maintenance, or deployment qualification.

## October 1 2026 atomic absence reconciliation checkpoint

The public reconciler can now settle an unknown dispatch that never reached
acceptance. `ResolveUnaccepted` proves absence for the exact attempt, effect,
physical node and boot in the same transaction as the head CAS. The revision
advances even when the attempt state otherwise stays identical. This fences
delayed old envelopes before a fresh retry. If acceptance wins the race,
resolution fails and the original work remains available for adoption.

Both permits and receiver charges remain retained. Expired preparation or
release moves toward independent cleanup without inventing source release or
serving evidence. Accepted source release without proof still cannot be
repeated. This closes the unaccepted-dispatch recovery gap; failed-source and
receiver-session adoption, complete observations, maintenance and the other
full-plan work remain outstanding.

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | `801bc4f5cf1cbc56385f09f7a1e9442247aa92da` |
| Source manifest | 565 Rust/Cargo files; SHA256 `9ea5990da8e182094387747d1bc80ca447e61a4fe38612a67ecd0f07fdac1fd3` |
| `cargo test -p cellule-runtime --lib fleet::operations --locked` | 60 passed; 419 filtered. |
| `cargo test -p cellule-host --example fleet_operations --locked` | 34 passed; none ignored or filtered. Includes independent acceptance/absence races, lost CAS replies, delayed envelope fencing, reconstructed driver retry and the existing real three-node scenario. |
| `cargo clippy -p cellule-runtime -p cellule-host --lib --example fleet_operations --locked -- -D warnings` | Passed. |
| Static gates | Format, boundaries, module layout, 85 Rust snippets, 1136 Markdown links, 28 SQL/peer assertions and 562 protocol links passed. |

Commands used the existing checkout target with `CARGO_INCREMENTAL=0` and
Rust 1.97.0. Initial new-test compilation failures and the failure that exposed
the unchanged-state revision bug were fixed before these passing runs.
No persisted format or journal SQL schema changed. These are focused local
results; no additional process/provider qualification is established.

CI for the baseline passed its contract, MSRV, website and decoder-fuzz jobs.
The workspace job failed in the website Rust example compiler because it passed
only app/runtime externs while newer main documentation also imported store,
types, bytes and object_store. That checker failure needs repair; it does not
establish a passing workspace qualification result.

## October 1 2026 real three-node overload checkpoint

The reference executable now supports `overload`, using the same scenario
implementation as its focused test. Three independently leased CellNodes share
strict in-memory object storage and initialize twelve real SQLite Cells. A held
seven-GiB disk admission reservation drives the actor's actual ledger samples
and normal hysteresis dwell to Shedding. The exported reconciler allocates two
charged moves, prepares real receivers, and resumes through an independently
reopened SQLite controller client. Both other nodes acquire successor authority,
resolve the original acknowledged command outcomes, read the restored values,
and reject the old source handles. Retirement follows fresh current inspections.

The command checks all three runtimes reach Stopped with empty resource ledgers,
then closes both journal clients after joining their accepted jobs. Setup and
scenario errors still join every registered runtime before dropping private
paths. The fixture preserves original scenario errors and reports additional
cleanup errors. It uses the canonical local-owner lookup for restored actors;
the fully resident fast path can legitimately be unavailable during hydration.

Observed executable output:

```text
released=2 activated=2 retired=2 receipt_checks=2 max_inflight=2 max_restore_bytes=2550136832 joined_nodes=3 receiver_nodes=2 blocker_count=1
blockers=[IncompleteObservation]
```

This is a finite **admission-pressure batch**, not physical disk utilization,
sustained-overload convergence, or provider/process qualification. Its fixed
ownership-only collector pins the three boot endpoints and signs actual local
classifier/capacity values. It reports incomplete role coverage and disables
count balancing. The actor retains its existing local idle eviction behavior;
reported fleet movement counts cover only the destination-reserved batch.
Pressure is released and new scheduling stopped after that batch is allocated.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | PR foundation `9138a51d3b1b9691c16d21bf81784677ca55445c` |
| Source manifest | 564 Rust/Cargo files; SHA256 `0add021e98e10002f8aacf1dad914d097233ead7bd75f175dfe8e725633c3bed` |
| Journal SQL | Unchanged SHA256 `6fd5c72f5e8f4bd245797dbc6f433baca0cece4d519aa954f7526b2ae61ad668` |
| Toolchain/target | Rust 1.97.0; existing checkout target, `CARGO_INCREMENTAL=0`. Package artifacts were cleaned with Cargo after a failed compile exhausted the mounted volume. That failed compile establishes no test result. |

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations --locked` | 31 passed; no ignored/filtered cases. Includes the shared real-node scenario. |
| `cargo run -p cellule-host --example fleet_operations --locked -- overload` | Exit zero with the output above and all three nodes joined. |
| `cargo test -p cellule-app --test integration host::three_node_host_recovers_published_state_after_owner_loss --locked -- --exact` | 1 passed; 43 filtered. |
| `cargo clippy -p cellule-host --example fleet_operations --tests --locked -- -D warnings` | Passed. |
| `cargo clippy -p cellule-app --test integration --locked -- -D warnings` | Passed. |

The owner-loss fixture now recognizes only the original terminal Fenced error
inside the retained `cell-runtime-drain` source chain, and only when the node
lease was actually fenced. It also checks zero Cell, memory, job, descriptor and
disk charges. Unrelated facility errors still fail. This fixes the assertion
regression found in the foundation PR's workspace and contract CI runs.

The foundation commit's third object-capacity repeat remains a failed
qualification result: run `36876658271`, artifact
`cell-write-capacity-36876658271-1`, window `capacity-3-hot-2`, arrival 25 was
`scheduler_late` (scheduled 4166666 us; started 4387343 us). Two earlier repeats
passed. No threshold, profile or evidence requirement was weakened; that
campaign must pass on the updated PR before qualification is claimed.

Before this entry, module/layer gates, 85 Rust snippets, 1136 Markdown links,
28 schema/protocol assertions and 562 protocol links passed. Format and diff
whitespace checks are repeated after the checkpoint. Full W1–W10 completion is
unproven: complete production observers/producers, failed-source/receiver
adoption, busy maintenance, role evacuation/finalization, remaining scenarios,
qualification and operator rollout remain in the full plan.

## October 1 2026 public reconciler checkpoint

The host now exports a caller-driven `FleetReconciler` that claims the journal
controller, advances charged attempts before planning, and publishes each phase
by exact head/registry CAS before dispatch. It uses the existing placement
planner and reducer, projects all unresolved receives, and reads committed
incarnation-specific cooldown and post-batch barriers from the journal.
Scheduling stop prevents new permits while existing work continues to settle.

Fresh inspection binds the entire action, current registry, exact node/boot,
nonce and original capture interval. The node checks current actor and authority
state without starting recovery or consulting a historical Inspect receipt as
current proof. Unknown outcomes retain both fleet permits. Confirmed activation
and retirement require fresh serving evidence; unused receiver resources settle
independently. Finite-action shutdown joins every sibling job before returning
the retained original failure.

Each charged attempt receives a bounded share of the pass deadline, leaving time
for healthy siblings and planning. The report retains original endpoint errors;
a timeout requires rereading journal state and the controller epoch before
continuing. Journal failures stop the pass. Dispatch counts include timed-out
waiters; confirmed release, activation, recovery and cancellation counts follow
committed transitions. Forward progress requests an immediate next pass;
unresolved failures and cancellations retain periodic retry to avoid hot loops.
The supplied application clock is read directly at every boundary and regression
fails closed; deadlines remain monotonic.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Implementation before PR commit; 560 Rust source, Cargo manifest and lock files. |
| Source manifest SHA256 | `83289340c715081f7c56113255a9f52a94597f83020c23476797823750adf31d` |
| Example `journal/schema.sql` SHA256 | `6fd5c72f5e8f4bd245797dbc6f433baca0cece4d519aa954f7526b2ae61ad668` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Pure reducer, local SQLite transactions, signed synthetic driver observations/effects, and selected real host action/lifecycle tests. No real three-node driver scenario or process/provider qualification. |

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations --locked` | 30 passed, none filtered or ignored: 18 journal and 12 driver/model tests. Includes lost replies, healthy-sibling progress after timeout, retained permits, clock regression, competing controllers, stop-new-moves and pressure relief. |
| `cargo test -p cellule-host --test node fleet --locked` | 26 passed, 38 filtered. Includes fresh actor inspection and failure drain joining a sibling inspection. |
| `cargo test -p cellule-host --test node lifecycle --locked` | 11 passed, 53 filtered. |
| `cargo test -p cellule-runtime --lib fleet::operations --locked` | 57 passed, 419 filtered. |
| `cargo clippy -p cellule-host --lib --example fleet_operations --tests --locked -- -D warnings` | Passed. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-host -p cellule-runtime --all-features --no-deps --locked` | Passed. |

Format, layer and module validators, 1136 local Markdown links/anchors, 85 Rust
snippets, 28 schema/protocol assertions and 562 protocol-document links passed
before this checkpoint. `git diff --check` passed. The documentation link gate
is rerun after this entry. Broad suites remain CI work; no provider/process
campaign result is claimed.

### Remaining work at the public reconciler checkpoint

Connect complete authenticated node/role observations, enrollment producers and
the startup intent barrier. Implement failed-source/receiver adoption and the
remaining unknown/absence reconciliation rules. The example executable still
supports only `inspect-journal`; its driver tests use simulated effects, not
three independently leased runtimes. Implement and qualify real overload,
maintenance, controller-restart and receiver-loss scenarios, busy-work
quiescence, role evacuation/finalization, and operator rollout. W5–W10 are not
complete, and earlier work packages retain the gaps listed in the full plan.

## October 1 2026 durable local journal checkpoint

The [embedding example](../crates/cellule-host/examples/fleet_operations/README.md)
now implements `FleetJournal`, `FleetEnrollmentJournal`, and `FleetActionJournal`
against one SQLite file. Each call uses `BEGIN IMMEDIATE`; current head/registry,
intent checks and record publication share the transaction. WAL with FULL
synchronization retains committed state for independently reopened clients.
The library adds no SQLite provider dependency; the dependency belongs to the
embedding example and its dev dependencies.

Maintenance stores the immutable original request separately from mutable
deadline/session progress. Older operations and cordons survive later work;
return-to-service cannot rewrite the current maintenance head. Action records
include their executing physical node and boot, allowing distinct source and
receiver inspections. Retiring permits commits their exact progress page
atomically. Loading rejects malformed head bytes and missing referenced
operations. Acquisition and recovery results require matching retained input
and evidence rather than reconstructed success from an arbitrary later root.

Each client admits at most 32 owned blocking jobs. A dropped caller cannot
cancel an accepted transaction. Close stops admission, joins every accepted
job, settles slots and closes the connection. Tests inject both rollback before
commit and lost replies after commit, retaining original errors and proof times.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Uncommitted implementation; 551 Rust source, Cargo manifest, and lock files. |
| Source manifest SHA256 | `e2220e1dbcede1c01a364a983dedb21e73dd38a7d71cb02551cbf994a7a0c43a` |
| Example `journal/schema.sql` SHA256 | `6fd5c72f5e8f4bd245797dbc6f433baca0cece4d519aa954f7526b2ae61ad668` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Local SQLite transactions, independent clients, injected commit replies, adapter reconstruction and bounded close. No process-crash, filesystem-fault, distributed provider or three-node movement qualification. |

The source manifest uses the reproduction script below. SQL is compiled into
the example via `include_str!`; its separate fingerprint is required because
that script covers Rust and Cargo files only.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations --locked` | 17 passed; none ignored or filtered. Includes racing claims/allocations/acceptance/enrollment, retained permits and cordons, original acquisition/recovery records, lost enrollment replies, atomic history, malformed/missing references, rollback and bounded cancellation-safe close. |
| `cargo clippy -p cellule-host --example fleet_operations --tests --locked -- -D warnings` | Passed for the example and selected test compilation. |
| `cargo run -p cellule-host --example fleet_operations --locked -- inspect-journal <temporary-database>` | Two consecutive executable runs exited zero. Both printed revision 0, registry revision 0, bootstrap false, scheduling false, zero unresolved attempts/restore bytes. Raw retained head/registry bytes matched after reopening. |

Format, crate-boundary and module-layout validators, 1134 local Markdown
links/anchors, 85 documented Rust snippets, 28 SQL/peer schema assertions and
562 protocol-document links passed before adding this checkpoint. `git diff
--check` passed. Documentation links are checked again after this entry.
Broad suites and provider/process gates were not run for this local checkpoint.

### Remaining work at the durable local journal checkpoint

Connect every enrollment producer and startup intent barrier. Implement fresh
observation envelopes, the complete observer and W5 reconciler, and consume this
journal through the public host action path with real independently leased
nodes. Retained inspection results are historical; the existing stable Inspect
key does not establish current serving and must not be counted as fresh evidence.
Complete cross-session recovery and W4 refusal/unknown reconciliation.

The example currently supports journal inspection only. W6–W10 still require
busy maintenance, role evacuation and finalization, three-node scenarios,
process/fault/provider and mixed-binary qualification, and operator rollout
runbooks. W1 and W8 are not complete; the full implementation goal remains active.

## October 1 2026 registry contract checkpoint

W1 now defines bounded retained intent/enrollment records and their codecs.
An enrollment request binds its exact physical nodes, boot sessions, checked
intent revisions, role and original time. Reader roles name the Cell and
published position; follower roles name the leader boot and node-log epoch.
Boot enrollment carries retained mode and cannot open readiness under a cordon.
Unknown enrollment remains Pending after expiry. Only checked completion,
definite refusal, or canonical closure/retirement advances it. Tombstones retain
immutable input and evidence; duplicate results do not refresh capture time.

`RegistryVersion` carries the shared mutation revision, controlled bootstrap
marker and revision-checked scheduling policy. New registries start stopped.
Enabling requires bootstrap; stopping preserves charged attempts and cordons.
Its allocation gate checks current retained source/receiver rows. A draining
source is eligible only for the current evacuation operation, and a cordoned
receiver cannot receive a new planned Cell. Registry pages include older
cordons and failed-session obligations with at most 128 sorted rows per page.

Maintenance adoption of a different boot now advances the intent revision.
The previous behavior changed the session while leaving that revision unchanged.
The retained intent update rejects older revisions and prevents an Active boot
default from overwriting a cordon.

The host now exposes `FleetJournal`, `FleetJournalSnapshot`, and
`FleetEnrollmentJournal` transaction contracts alongside `FleetActionJournal`.
Implementations must share one durable transaction domain for controller CAS,
permits, scheduling, retained intents/operations, enrollment and action evidence.
These are contracts and pure gates; the reference backend and complete observer
are still missing. No new trait implementation is claimed as durable evidence.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Uncommitted implementation; 544 Rust source, Cargo manifest, and lock files. |
| Source manifest SHA256 | `0236712e5d971a8455b09e333ad8ae86191f111860a07ac31e444b49c20b3eb8` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Pure deterministic registry transitions/codecs and existing public local host action regressions. No backend reconstruction, process restart, complete roster, or provider qualification. |

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-runtime --lib fleet::operations:: --locked` | 53 passed; 419 filtered out. Ten new registry cases cover intent/reboot revisions, exact enrollment inputs, ambiguous obligations, immutable evidence/time, bootstrap, cursor/page bounds, malformed codecs, stop/resume, and allocation gates. |
| `cargo test -p cellule-host --test node node::fleet --locked` | 22 passed; 38 filtered out. Existing source, receiver and recovery actions still pass through the public host path. These tests retain the earlier in-memory action journal, not the new complete journal contracts. |
| `cargo clippy -p cellule-runtime --lib --test runtime --locked -- -D warnings` | Passed for the selected runtime library and public runtime target. |
| `cargo clippy -p cellule-host --lib --test node --locked -- -D warnings` | Passed for the selected host library and public node target. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-runtime -p cellule-host --no-deps --locked` | Both API documentation targets compiled without warnings. |

Boundary and module-layout validators, document links and Rust fences,
SQL/peer contracts, format checking and `git diff --check` passed. New registry
record kinds 12 through 16 require the plan's reader deployment gate. The
bootstrap marker is an adapter assertion, not proof that a filtered observation
is complete.

### Remaining work at the registry contract checkpoint

Implement a strict durable reference journal against all three host contracts,
with racing transactions, lost commit replies and backend reconstruction.
Connect every enrollment producer and startup intent check, then complete
fresh observation envelopes and the W5 reconciler. Cross-session recovery,
remaining W4 inspection/refusal gates, and W6–W10 remain in scope. The full
implementation goal remains active.

## October 1 2026 receiver and failed-source recovery checkpoint

The host executor now prepares and activates admitted receivers, records exact
acquisition input, joins unused preparation credit, and inspects current
serving through the canonical actor and authority paths. It owns finite action
work across dropped transport waiters. Retained results keep their original
position when a successor publishes a newer root.

Failed-source recovery has a separate `Recover` action and `Recovered` outcome.
The runtime's ordinary Idle acquisition and takeover paths confirm a durable
`RecoveryBasis` before ownership CAS and a `RecoveryEvidence` position before
actor admission. A lost input-record reply prevents CAS. A lost recovered-
position reply prevents admission and follows canonical rollback. Inspection
can later combine the retained position with fresh serving evidence; it cannot
fabricate a clean source release. Receiver cleanup remains an independent fact
required before the attempt's charged permit can retire.

Public fault tests exercise both a failed Serving owner and an unobserved source
release CAS to Idle. The retained follower-tail integration fixture exercises
the same recording boundaries around actual tail materialization. Its signed
advertisement timestamps now precede the simulated operations rather than
being future-dated. Fatal finite-action task failures retain their original
error across repeated shutdown while resource cleanup continues.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Uncommitted implementation; 538 Rust source, Cargo manifest, and lock files. |
| Source manifest SHA256 | `77ae052ba23864a1ea795f1e0f205fecb857cb1e359ef60f4725c21a96eb338b` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Focused local contracts and public leased runtime/host behavior with an atomic in-memory reference action journal. No restart, process, mixed-binary, or provider qualification. |

All selected commands below exited zero. Together the selected test suites
reported 98 passed and one environment-dependent ignored test. The ignored
RustFS case requires its documented provider environment and supplies no
passing provider evidence.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --test node node::fleet --locked` | 22 passed; 38 filtered out. Includes source actions, admitted receivers, recovery, lost journal replies/waiters, and retained task errors. |
| `cargo test -p cellule-runtime --lib fleet::operations:: --locked` | 43 passed; 419 filtered out. Includes five pure recovery contract/codec cases. |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::ownership::recovery:: --locked` | 7 passed; 1 ignored; 172 filtered out. Includes actual retained-tail materialization and both recording failure boundaries. |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::receiver:: --locked` | 9 passed; 171 filtered out. |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::idle::release:: --locked` | 6 passed; 174 filtered out. |
| `cargo test -p cellule-host --test node node::lifecycle:: --locked` | 11 passed; 49 filtered out. |
| `cargo clippy -p cellule-runtime --lib --test runtime --locked -- -D warnings` | Passed for the selected runtime library and public runtime target. |
| `cargo clippy -p cellule-host --lib --test node --locked -- -D warnings` | Passed for the selected host library and public node target. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-runtime -p cellule-host --no-deps --locked` | Both API documentation targets compiled without warnings. |

Boundary and module-layout validators, document links and Rust fences,
SQL/peer contracts, format checking, and `git diff --check` passed. Recovery adds
strict new record kinds, phases, action, and outcome discriminators; the plan's
reader deployment gate remains required. These local checks do not qualify
mixed deployed readers.

### Remaining work at the recovery checkpoint

Recovery execution currently targets the original preferred receiver boot.
Complete cross-session recovery and refusal/unknown reconciliation, fresh
observation envelopes, retained intent/enrollment registries, and durable
adapter semantics. Connect the complete observer and reusable W5 reconciler.
W6–W10 still require busy maintenance, role evacuation/finalization, the runnable
reference fleet, fault and process/provider qualification, and operator
runbooks. The full implementation goal remains active.

## October 1 2026 accepted source action checkpoint

The runtime now exposes a bounded `AcceptedFleetAction` record and codec.
First acceptance checks the current journal head and exact executing endpoint.
Replay compares all immutable movement inputs, including physical identities,
authority epoch, conservative cost, snapshot, and deadline. The stable action
key remains an index. An accepted record supplies no remote authentication or
Cell authority.

The host's [`fleet` module](../crates/cellule-host/src/fleet/mod.rs) defines the
application-owned atomic journal contract and a startup-bound finite-action
executor. Its current effect is settled source Release. It journals acceptance,
uses canonical `release_idle_cell_at`, and retains checked evidence through a
dropped waiter or failed publication. A retry republishes the same result.
An existing acceptance without a result becomes Unknown and never authorizes
releasing a newer actor generation.

Public fault tests exposed a deadline-resumption problem in host drain. The
host now retains one runtime shutdown task and its original result. A timed-out
waiter leaves that task owned; a later drain joins it. Lease withdrawal waits
for successful runtime drain. These are local lifecycle foundations, not the
complete W7 role-evacuation and fleet-finalization protocol.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Uncommitted implementation; 522 Rust source, Cargo manifest, and lock files. |
| Source manifest SHA256 | `d0171432e9321c7e514a9f793d02388048aab6d74690e78fd7b40ad6e63d1f14` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Focused local contracts and public leased CellNode behavior with an atomic in-memory reference journal; no provider or process qualification. |

Use the source-fingerprint script below to reproduce this manifest. Older
checkpoint hashes identify their earlier source, not the current diff.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-runtime --lib fleet::operations:: --locked` | 30 passed; 419 filtered out. Includes five accepted-action tests and malformed acceptance deadline checks. |
| `cargo test -p cellule-host --test node node::fleet_actions:: --locked` | 7 passed; 38 filtered out. Concurrent duplicates, lost waiter, lost journal reply after successor publication, stale authorization, unknown accepted effect, resumed publication drain, and accepted-query drain with retained lease maintenance. |
| `cargo test -p cellule-host --test node node::lifecycle:: --locked` | 11 passed; 34 filtered out. Existing node drain, scale-down, deadline, lease-withdrawal, and pressure/cordon behavior. |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::receiver:: --locked` | 8 passed; 169 filtered out. Prepared resource ownership, refusal, cancellation, exact restoration, lost replies, and shutdown. |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::idle::release:: --locked` | 6 passed; 171 filtered out. Exact identity/final-root release and accepted-work barriers. |
| `cargo test -p cellule-runtime --lib cell::actor::tests:: --locked` | 4 passed; 445 filtered out. Includes preservation of unobserved original release errors. |
| `cargo clippy -p cellule-host --lib --test node --locked -- -D warnings` | Passed for the selected host library and public node target. |
| `cargo clippy -p cellule-runtime --lib --test runtime --locked -- -D warnings` | Passed for the selected runtime library and public runtime target. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-runtime -p cellule-host --no-deps --locked` | Both API documentation targets compiled without warnings. |

The test journal linearizes head checking and acceptance under one lock and
retains accepted/results independently of transport waiters. It does not prove
a production backend's conditional writes, process restart durability, a
complete enrollment registry, or a controller's reconciliation behavior.

### Remaining work at the source action checkpoint

Integrate prepared receiver and maintenance actions into this host executor.
Persist immutable checked acquisition and recovery basis, complete the distinct
recovered outcome, and implement source inspection/refusal reconciliation.
Then connect the complete observer, durable registries, and reusable W5 driver.
W6–W10, the runnable reference fleet, broad verification, and measured
process/provider qualification remain required before the full objective is
complete. The goal remains active.

Boundary and module-layout validators, document links and Rust fences,
SQL/peer contracts, format checking, and `git diff --check` passed for this
checkpoint. They supplement the focused evidence; broad workspace and
process/provider gates remain pending.

## September 30 2026 disk credit checkpoint

W4 now has an admitted disk-credit boundary. `DiskReservation::into_budget`
transfers an existing reservation into the canonical LTX host's operation
budget. Preparing work retains the full parent envelope; child reservations
use that credit. `finish_preparation` returns unused bytes and retains actual
charges. Later growth competes with ordinary parent admission.

The public activation test checks the restored payload against the original
bytes, performs another SQLite transaction and capture, and closes the
database. The cancellation test pauses an accepted filesystem job, aborts its
initiating future, and verifies that parent credit stays charged until the job
closes. A runtime test verifies the same credit against `LedgerDiskAdmission`.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Uncommitted implementation; 511 Rust source, Cargo manifest, and lock files in the source fingerprint below. |
| Source manifest SHA256 | `11801d0adf23955539d3be78b341279e6c571286d0e3daf1574caaf2dce1b8f0` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)`; `cargo 1.97.0 (c980f4866 2026-06-30)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Local focused unit and public library behavior; no provider or process qualification. |

All commands below exited zero. Cargo commands used the recorded target
directory. Their scope is the selected modules and targets.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-ltx --no-default-features --lib prepared_disk_budget --locked` | 8 passed; 35 filtered out. |
| `cargo test -p cellule-ltx --features replica --lib prepared_disk_budget --locked` | 8 passed; 97 filtered out. |
| `cargo test -p cellule-ltx --features replica --test host host::hooks::activation:: --locked` | 13 passed; 2 ignored; 55 filtered out. Includes both new public disk-credit scenarios. |
| `cargo test -p cellule-runtime --lib fleet::resource::tests --locked` | 10 passed; 434 filtered out. Includes real runtime-ledger credit inheritance. |
| `cargo clippy -p cellule-ltx --features replica --lib --test host --locked -- -D warnings` | Passed for the selected library and public host target. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-ltx --features replica --no-deps --locked` | API documentation compiled without warnings. |

The ignored activation cases are the directory-cache restart diagnostic and
the RustFS sparse-activation diagnostic. Neither supplies evidence for this
checkpoint. The latter requires the environment in the
[LTX example guide](../crates/cellule-ltx/examples/README.md).

Boundary/layout validators, document link and Rust-fence validators, the
SQL/peer validator, format checking, and `git diff --check` also passed during
this checkpoint. These checks do not replace the plan's broad gates.

Reproduce the source fingerprint from the workspace root:

```python
import hashlib
import pathlib
import subprocess

root = pathlib.Path.cwd()
tracked = subprocess.check_output(["git", "ls-files", "-z"]).split(b"\0")
untracked = subprocess.check_output(
    ["git", "ls-files", "--others", "--exclude-standard", "-z"]
).split(b"\0")
paths = sorted({
    p.decode() for p in tracked + untracked
    if p and (p.endswith(b".rs") or p.endswith(b"Cargo.toml") or p == b"Cargo.lock")
})
manifest = "\n".join(
    f"{hashlib.sha256((root / p).read_bytes()).hexdigest()}  {p}" for p in paths
) + "\n"
print(len(paths), hashlib.sha256(manifest.encode()).hexdigest())
```

### Remaining work at the disk credit checkpoint

Integrate disk credit into the host's exact-session receiver preparation and
canonical activation. Transfer memory, descriptor, Cell-slot, and job tokens
through the same admission path. Add durable action acceptance and checked
release/acquisition/recovery evidence, then exercise duplicate dispatch,
controller restart, and shutdown through public CellNode actions. W5–W10 and
the full acceptance matrix remain pending.

## October 1 2026 prepared receiver checkpoint

W4 now has a trusted local receiver path in
[`cell/actor/receiver.rs`](../crates/cellule-runtime/src/cell/actor/receiver.rs).
`CellRuntime::prepare_receiver` holds a real Cell slot, conservative native
memory and descriptors, the incoming Cell's affine SQL worker permit and
ledger charge, and scoped LTX disk credit. Partial refusal returns its tokens.
Canonical Idle acquisition consumes those tokens without releasing and
reacquiring the envelope.

The runtime retains at most two local receipts, charged to the ordinary
retained-byte ledger. Opaque caller references are weak: a lost reply can be
looked up by attempt, and keeping a caller reference cannot prevent shutdown.
Accepted activation belongs to a runtime-owned task. Dropping its RPC waiter
cannot cancel takeover. Shutdown cancels unused preparation, joins accepted
acquisition, then follows the existing actor and worker close path.

The public receiver suite checks exact root and SQL value readback, resolution
of the original acknowledged mutation, duplicate preparation, admission
refusal before release, explicit expiry/cancellation, and resource cleanup.
It also checks a failed restore after ownership CAS, a lost activation waiter
plus a lost CAS response, shutdown during an accepted claim, and restoration
of the current root after another canonical owner commits and releases.

Local lifecycle hints are not durable results. `Failed` does not prove that
ownership is absent; `Activated` still requires fresh authority and actor
readiness. The host executor must supply authorization, journal acceptance and
results, and checked release/acquisition/recovery evidence. This checkpoint
establishes neither a deployed fleet controller nor process/provider behavior.

| Source and environment | Recorded value |
| --- | --- |
| Baseline revision | `e07670e2348231ed401cc7280a47e3ab97596ffe` |
| Working tree | Uncommitted implementation; 513 Rust source, Cargo manifest, and lock files. |
| Source manifest SHA256 | `1f7577f4ba6604898295d9680e4f6d18974345c8317660de49e9d400791a2a32` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)`; `cargo 1.97.0 (c980f4866 2026-06-30)` |
| Target directory | `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations` |
| Proof level | Focused local runtime library behavior against in-memory CAS, with real SQLite and resource ledgers. No process, provider, mixed-binary, or measured fleet qualification. |

The fingerprint uses the reproduction script above. The disk credit
checkpoint remains evidence for its earlier source; it does not certify this
later receiver integration.

All commands below exited zero. Cargo commands used the recorded target
directory and default features; these results cover only the selected targets.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::receiver:: --locked` | 8 passed; 168 filtered out. Includes original mutation resolution after movement and current-root readback after an intervening owner. |
| `cargo test -p cellule-runtime --test runtime runtime::lifecycle::idle::acquire:: --locked` | 2 passed; 174 filtered out. Existing ordinary receiver failure and cordon behavior. |
| `cargo test -p cellule-runtime --lib cell::worker::tests --locked` | 6 passed; 2 ignored; 436 filtered out. Existing shared-ledger, inventory, sparse SQL, and hydration scenarios. |
| `cargo clippy -p cellule-runtime --lib --test runtime --locked -- -D warnings` | Passed for the selected library and public runtime target. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-runtime --no-deps --locked` | API documentation compiled without warnings. |
| `cargo check -p cellule-host --lib --test node --locked` | Selected host library and node target compiled with the runtime API changes. This is not execution of host actions. |

The two ignored worker tests are RustFS interference diagnostics. Their
isolated provider environment was not supplied, and they provide no evidence
for this checkpoint. The workspace's broad feature, process, model, and fault
campaign gates remain outstanding.

Boundary/layout validators, 85 documented Rust snippets, 1119 local Markdown
links, 28 SQL/peer assertions with 561 validator links, format checking, and
`git diff --check` also passed. These static checks do not establish fleet
relocation or maintenance completion.

### Remaining W4 work

Connect the runtime path to authenticated CellNode actions and durable action
acceptance/results. Bind physical node and fleet scope, capture immutable
checked source release and acquisition/recovery evidence, and adopt unknown
outcomes across controller replacement. Complete host shutdown/role barriers
and concurrent ordinary activation/reader admission scenarios. W5–W10 and the
plan's full acceptance matrix remain pending.
