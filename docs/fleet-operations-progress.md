# Fleet operations implementation evidence

The [implementation plan](fleet-operations-plan.md) remains the full scope.
This page records focused checkpoints; it does not establish complete fleet
balancing, maintenance, or deployment qualification.

## October 1 2026 managed follower producer checkpoint

The host now binds follower production to the shared journal through
`install_fleet_node_durability_provider`. An application-owned read-only provider
supplies one opaque signed attempt and exact boot-bound transport/authority.
The existing retained supervisor owns preparation, every member's Pending
acceptance, its single original-token enrollment CAS, and original establishment
publication. It neither constructs a second scheduler nor reselects on unknown
results. Ship configuration is delivered only after all publication replies
confirm. Before acceptance, canonical gate/shipper validation and the shared
one-MiB reservation fail closed; at most 32 epochs remain inventoried.

Unknown CAS outcomes reconcile only the original signed source/epoch/member set
or its original-token conditional refusal. Joined nonexecution uses a new atomic
journal exclusion: exact Pending becomes Refused, or an absent key becomes a
terminal refusal tombstone. A late acceptance returns that original row without
opening a native role. This also replaces the managed reader's absence-based
cleanup. Existing opened-reader cases still require Retired; the two explicit
nonexecution cases now require Refused and terminal replay. Independent SQLite
clients qualify the shared transaction domain, immutable timestamps, lost replies
and reconstruction. No persisted format or native qualification profile changed.

The managed authority receives complete native retirement observations and
requires every original member fence before canonical closure and durable
retirement. Native closure is recorded before fallible evidence construction;
lost publication retries the original events without repeating confirmed close.
Shutdown preserves its original errors and retries instead of caching a terminal
transient failure. Undelivered enrollment cleanup joins after the supervisor,
using the original limits and retained durability object; runtime drain closes
delivered epochs. Caller cancellation/deadlines leave inventory and byte charges
owned until settlement. Local completion captures original native and journal
errors independently; removed history proves no fleet fact.

Nine new public example scenarios use signed directory authority and two actual
native follower stores. They cover every Pending-before-CAS boundary, partial
acceptance and receiver cordon, lost acceptance/establishment/close/retirement
replies, cancelled/deadline drain waiters, failed member fences, invalid shipper
limits, and an acknowledged SQL mutation. The latter confirms nonzero contiguous
object coverage, every original member watermark, byte-identical root restore,
counter state and the original stored `sys_requests` response. One public host
case rejects an unmanaged provider before it can start on a configured fleet
boot. Three new runtime cases check refusal codecs, startup-held versus confirmed
maintenance state, and token-bound/domain-separated evidence replay.

The final scoped pipeline passed with Rust/Cargo 1.97.0, all features, the lockfile,
`CARGO_INCREMENTAL=0`, and this checkout's Workspace target directory. Its
Rust/Cargo manifest stayed identical after every command.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `cc7aa3063556b03f8c165f1dc74def0aae60a88e` |
| Rust/Cargo manifest | 607 tracked/nonignored Rust/Cargo paths; sorted SHA256, two spaces, relative path lines. SHA256 `ce6f869aec2f6705a260aea141b3837c0db37e0d77cf07c7db9b956c02bb95c9`. |
| Complete public host node target | 81 passed; none filtered or ignored. |
| Complete fleet example target | 71 passed; none filtered or ignored. Includes nine new public native follower scenarios and two new shared-journal cases. |
| Public runtime durability selection | 24 passed; 176 filtered; none ignored. |
| Runtime node library selection | 101 passed; 408 filtered; none ignored. |
| Runtime fleet operations selection | 66 passed; 443 filtered; none ignored. |
| Runtime admission selection | 5 passed; 504 filtered; none ignored. |
| Distinct scoped cases | 348 passed. Earlier/intermediate executions are not added to this total. |
| Lints | Host/app all targets, runtime library/public runtime target, all features, warnings denied; passed. App process drivers compiled under Clippy; they were not executed locally. |
| Host/runtime API documentation | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, Rust snippet and Markdown link checks, and SQL/peer validators passed. |

Intermediate attempts exposed obsolete reader settlement assertions, premature
fixture readiness, missing advertised follower capacity, a Cell replica limits
mismatch, one missed test-only authority signature, and two lint issues. Fixtures
now follow startup order and existing capacity/application contracts; opened
readers still require Retired, while proven nonexecution requires Refused and
terminal replay. Production limits, profiles and required evidence are unchanged.
Logs and the exact manifest use `/tmp/cellule-managed-follower-*`; the intermediate
pipeline failures remain in the `intermediate-verification` and
`pre-lint-verification` logs. No process/provider suite ran locally.

Baseline `cc7aa30` passes workspace/MSRV (36947731399), both capacity campaigns
(36947731308), contracts (36947731662), website (36947731302), decoder fuzz smoke
(36947731443), fast/negative TLC (36947731392), and Compose smoke
[job 110653610813](https://github.com/crabbuild/cellule/actions/runs/36947731323/job/110653610813).
Broad TLC and the deterministic simulator were skipped. Routing
[job 110653610627](https://github.com/crabbuild/cellule/actions/runs/36947731323/job/110653610627)
was authoritatively in progress at the frozen-binary comparison step when inspected.
The new source requires its own CI qualification. The original `6092203` reader
scaling failure and artifact below remain evidence; later green smoke does not
establish that failure's cause.

The full W1–W10 objective remains active. Complete authenticated revisioned role
observations, failed-process/boot evidence, replacement-policy proof, maintenance
role actions/finalization, remaining primitive/fault matrices (including Cron and
Blob external owners), sustained convergence, full maintenance/receiver-loss
examples, process/provider/mixed-binary qualification and operator rollout/runbooks
remain required. The existing ownership-only collector still reports incomplete
role coverage. This producer checkpoint alone cannot complete SettleRoles,
Finalize, or the full plan.

## October 1 2026 prepared follower enrollment checkpoint

Runtime recruitment now shares one canonical selector and conditional write
with a read-only preparation bridge. Opaque selection retains every original
signed follower boot, complete member set and provider-assigned advancing epoch.
An opaque attempt revalidates those exact boots and receiver admission/capacity,
then captures a fresh source CAS token before registry acceptance. Commit uses
that original token and ensemble; it never substitutes a receiver or rebases an
ambiguous effect. Directory instance binding permits clones and rejects other
instances even with identical fleet metadata. Ordinary recruitment preserves
its caller's observed-version CAS behavior.

Fresh inspection reconciles the original source boot, epoch and complete member
set across activation, coverage and heartbeat changes. A conditional refusal
fence competes with enrollment using the same original token and next generation.
Its checked successor prevents that attempt's delayed CAS. Missing or newer empty
records cannot create this proof; enrollment followed by closure cannot be
reported as refusal. These proofs do not assert native follower fsync, current
receiver authority, fleet journal publication or role retirement. The managed
host producer still needs to journal all members Pending before native dispatch
and consume these retained attempts through settlement.

Retirement now caches complete checked member responses before authority closure.
After a lost close reply or a cancelled close waiter, strict and ordinary retries
reuse that exact observation and retry the original authority callback. They no
longer send unauthorized retire requests against an already-closed epoch. Final
shutdown proof still requires callback success. Partial best-effort closure and
contradictory receipts preserve their original contracts.

Eleven new directory cases exercise read-only selection, exact signed boot
metadata, heartbeat rebasing before acceptance, stale dispatch tokens, current
inspection, absent/withdrawn source, scope isolation, expired/replaced followers,
receiver capacity/mode/pressure, outbound enrollment from a cordoned leader,
signer replacement and conditional refusal races. Three new public runtime
cases use actual Cell commands, native follower stores and signed directory
authorization/CAS. They lose closure replies, cancel closure waiters, and cancel
reconciliation after the backend accepted an enrollment/refusal but lost its
reply. Original member observations, persisted native fences, exact-root state
and the original `sys_requests` response remain checked. These in-process faults
do not establish provider/process or complete fleet-role qualification.

The final scoped pipeline passed against the exact Rust/Cargo manifest below.
The selections establish these contracts and regression evidence; they do not
qualify the full W1–W10 implementation.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `60922033dc3ecc9a01f3f15877aa62c3cdabd62c` |
| Rust/Cargo manifest | 601 tracked/nonignored `.rs`, `Cargo.toml` and `Cargo.lock` paths; sorted SHA256, two spaces, relative path lines. SHA256 `787c1c77da2b553be326a312b05cfbc32143a5f98ad5ab45a15abb0bbc7057ee`. |
| Complete public host node target | 80 passed; none filtered or ignored. |
| Public runtime durability selection | 24 passed; 176 filtered; none ignored. Includes the three new signed-directory/native-follower cases. |
| Runtime node library selection | 100 passed; 406 filtered; none ignored. Includes eleven new directory cases. |
| Complete fleet operations example target | 60 passed; none filtered or ignored. |
| Distinct scoped cases | 264 passed. Earlier and intermediate selections are not counted again. |
| Host lint | All targets/features, warnings denied; passed. |
| Runtime lint | Library/public runtime target, all features, warnings denied; passed. |
| Host/runtime API documentation | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 110 Rust snippets, 1150 Markdown links and 28 SQL/peer assertions with 566 links passed. |

An intermediate retirement-only run passed five existing cases and failed both
new signed fixtures because their heartbeat lifetime exceeded the existing
30-second contract. The fixtures now use 20 seconds; the limit and verification
assertions are unchanged. Final logs and manifest use
`/tmp/cellule-fleet-enrollment-*`, Rust/Cargo 1.97.0, all features, the lockfile,
`CARGO_INCREMENTAL=0` and this checkout's Workspace target directory. Persisted
formats, signed message bytes and object paths are unchanged.

Baseline `6092203` passed Rust (36944383297), capacity (36944383244), contract
(36944383282), website (36944383388), fuzz (36944383341), and fast/negative TLC
(36944383262 / 110642975391). Broad TLC and the simulator corpus were skipped.
Compose smoke **failed** at the reader-scaling step in
[job 110643075559](https://github.com/crabbuild/cellule/actions/runs/36944383259/job/110643075559).
Its three initial provider-backed tests passed; the mixed three-node reader
driver then failed at `process_scaling.rs:548` with `ReplicaUnavailable` after
successful reader readiness and distribution. The last retained mixed write is
arrival 184 at about 36.8 seconds, with acknowledged sequence 246/count 216.
This is a symptom, not an established cause. The raw artifact
`cell-reference-compose-36944383259-1` (ID `11201985421`) includes original
driver/node/container logs, TSV arrivals and reads, binary hashes and source
identity. The downloaded ZIP SHA256 is
`2ec63e359d13b01ca86ef2fd16ae3763d53e0517480e32d723772fc14a6c06da`.
The artifact pins synthetic PR merge source
`68943f6c8b0e1bd7fd57e766590779c067983d34`, whose parents are baseline
`0dc04a658bd99668936f7ec58032d054f6fbc141` and PR head `6092203`.
The scaling driver binary SHA256 is
`d8796fd2b9a195c62fe7c4020aadab033a27a1f013b2ad56ee2bc56ef239ded6`.
Local copies are `/tmp/cellule-fleet-6092203-compose-smoke*`. Routing job
`110643075291` is authoritatively Cancelled (completed at 2026-10-02 00:47:34 UTC)
after the subsequent push.
Reader availability and process/provider qualification remain open. No profile,
required evidence or assertion was weakened.

The full W1–W10 goal remains active. Next work includes managed follower
production before CAS, journal retirement, complete revisioned role observations,
failed-owner/replacement-policy proof, role evacuation/finalization and the
remaining primitive, example, fault, deployment and operator deliverables.

## October 1 2026 requested host rotation checkpoint

`CellNode::request_node_log_rotation(epoch)` wakes the existing durability
supervisor without waiting for a frame threshold. Acceptance is linearized with
automatic rotation: duplicate epochs share progress, and an automatic
best-effort rotation already in flight is refused. One pending and one latest
completed request are charged to the node byte ledger. Weak handles can inspect
or recover local progress after a lost caller; they cannot retain the node's
resources after drain. Evicted local history proves neither absence nor success.

Requested work keeps confirmed retirement through all retries. Its original
epoch, complete member set and object-coverage barrier cannot be weakened by a
timer. Local completion requires every old member's append fence, canonical
authority closure and a newer binding installed through expected-binding
replacement. The runtime checks boot, physical leader, epoch advancement and
the exact old binding under its replacement lock. Foreign replacement inputs
are rejected before construction or any attempt to close their scope. Tuple
identity getters supply configured metadata, not signed current authority.

The host retains the single supervisor's join outside the cancellable task-group
watcher. Cancelling a caller, watcher or facility join leaves accepted native
retirement/recruitment owned. A replacement that returns during drain is closed
through the existing canonical path; lost close replies retry that same object.
Lease maintenance now waits for required facility joins as well as runtime
closure. Deadlines leave Draining and retain progress; Interrupted never becomes
Completed. The original first and latest source failures remain independent of
eventual success.

Eight new public host cases use actual Cell commands and native follower stores.
They cover strict retirement and recruitment retries while writes continue,
old/new follower ensembles and ordinary object-proof fallback, duplicate and
stale requests, dropped handles, cancelled shutdown waiters, an automatic
rotation already in flight, bounded history/zero final resource charges,
invalid replacement boot/node/epoch, accepted recruitment across a host deadline,
and lost cleanup close replies across repeated deadlines. Exact-root readback
checks Cell state and the original `sys_requests` result. Independent follower
reopening checks old durable fences. The source authority fixture records and
reconciles callbacks; it does not exercise signed directory CAS or provider/
process fault behavior.

Intermediate full-target verification caught a startup regression: reserving
fixed supervisor bookkeeping through the leased runtime rejected provider
installation before a lease existed. Fixed supervisor ownership remains under
the facility/task bounds; accepted request records retain ordinary ledger
admission. The existing pre-lease installation contract now passes unchanged.
Lease withdrawal ordering and generation cleanup were repaired without weakening
the shutdown deadline or expected evidence.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `29780a36c477bb283d19f0f8e3fef11e4c249c1a` |
| Rust/Cargo manifest | 599 tracked/nonignored `.rs`, `Cargo.toml` and `Cargo.lock` paths; sorted SHA256, two spaces, relative path lines. SHA256 `8092eea2db8eb158ddea7ae56c19a6549be8da94c11a7c44e39df81a2dd9cabb`. |
| Complete public host node target | 80 passed; none filtered or ignored; includes eight new cases. |
| Public runtime durability selection | 21 passed; 176 filtered; none ignored. Includes atomic rejection of stale bindings, foreign boots/physical nodes and non-advancing epochs. |
| Runtime node library selection | 89 passed; 406 filtered; none ignored. |
| Complete fleet operations example target | 60 passed; none filtered or ignored. |
| Distinct scoped cases | 250 passed. Initial retirement-only and intermediate target runs are not counted again. |
| Host lint | All targets/features, warnings denied; passed. |
| Runtime lint | Library/public runtime target, all features, warnings denied; passed. |
| Host/runtime API documentation | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 110 Rust snippets, 1150 Markdown links and 28 SQL/peer assertions with 566 links passed. |

Commands use all features, Rust/Cargo 1.97.0, the lockfile,
`CARGO_INCREMENTAL=0` and the checkout's Workspace target directory. The source
manifest and final logs are retained under
`/tmp/cellule-fleet-requested-rotation-*`. The host adds the already locked
`bytes` crate only to test dependencies. `replace_node_durability` now takes
the exact expected binding; all workspace callers are updated. Persisted IDs,
object paths, LTX formats and signed message bytes are unchanged.

Baseline `29780a3` passed Rust (36940992641), capacity (36940992651), contract
(36940992648), website (36940992677), fuzz (36940992676), model (36940992681) and
Compose smoke (36940992647 / 110632500524). Routing 110632500048 remained running
when inspected. Optional broad model/simulator campaigns require job-level
evidence; workflow success alone does not establish their execution. Earlier
failure evidence remains below and this new source requires its own CI gates.

Next, journal follower production before recruitment CAS and consume complete
revisioned role observations, replacement policy and failed-owner recovery
evidence. The new request is a local host primitive: the application still owns
authorization, deadlines, maintenance-node exclusion, enrollment/results and
redundancy policy. The reference collector remains explicitly incomplete;
requested rotation alone cannot complete SettleRoles or Finalize. All remaining
W6–W10 matrices, sustained convergence, the four executable scenarios,
process/provider/mixed-binary qualification and operator rollout/runbooks remain
part of the active goal.

## October 1 2026 confirmed member retirement checkpoint

`NodeDurability::shutdown_for_maintenance` now shares the existing shipper drain,
contiguous object-publication barrier and authority close path. It joins every
original member, retains individual source errors and requires every exact
retirement receipt before authority closure. A failed response remains retryable
with the original leader, epoch, complete member set and watermark. The opaque
member proof is cached only after canonical authority closure succeeds.

Ordinary `shutdown`, `rotate_node_log` and `close_node_log` preserve documented
best-effort retirement. Their successful epoch closure cannot establish complete
member confirmation when a response was lost. Contradictory successful receipts
still block authority closure in both modes. An observation's `confirmed()`
proof describes member append fences only; it does not assert directory closure,
lane deletion, journal settlement, replacement policy or safe physical shutdown.

Five new public cases use actual SQLite commands, native follower stores and
canonical Cell publication. They cover lost retirement replies after fsync,
joining a delayed healthy sibling before returning another member's failure,
exact-scope retry and cached proof, cancellation before authority close,
best-effort closure that cannot be upgraded into proof, contradictory receipts
in both modes, and incomplete object publication blocking all retirement RPCs.
Every case reopens follower storage, observes persisted Retired fences and
rejects old appends, then restores the exact authority root and reads the counter
and original `sys_requests` response. The source authority fixture records
callbacks; these cases do not qualify authenticated directory CAS, separate
processes or provider failure.

| Source and focused verification | Recorded value |
| --- | --- |
| Baseline | `9c99d3ac620821fa21e11fe462b1046cca898305` |
| Rust/Cargo manifest | 595 tracked/nonignored `.rs`, `Cargo.toml` and `Cargo.lock` paths; sorted SHA256, two spaces, relative path lines. SHA256 `f2d51309c3797d0a27e57b7dcf5da8459e66835f9911529e368c1e16fcadfe22`. |
| Public durability selection | `cargo test -p cellule-runtime --test runtime --all-features --locked runtime::lifecycle::durability::`: 21 passed, 176 filtered, none ignored; includes five new cases. |
| Node library selection | `cargo test -p cellule-runtime --lib --all-features --locked node::`: 89 passed, 406 filtered, none ignored. |
| Distinct scoped cases | 110 passed. The separate initial five-case retirement run is included in the public selection, not counted again. |
| Runtime lint | `cargo clippy -p cellule-runtime --lib --test runtime --all-features --locked -- -D warnings`: passed. |
| Host compatibility | `cargo check -p cellule-host --lib --test node --all-features --locked`: passed; this is compilation, not host scenario execution. |
| Runtime API documentation | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 110 Rust snippets, 1150 Markdown links and 28 SQL/peer assertions with 566 links passed. |

Commands use Rust/Cargo 1.97.0, the lockfile, `CARGO_INCREMENTAL=0` and
`$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations`.
The source manifest is retained at
`/tmp/cellule-fleet-member-retirement-rust-cargo-manifest.txt`. Persisted IDs,
object paths, receipt fields, LTX formats and signed message bytes are unchanged.
Additional node, lint, host compilation and documentation logs are retained
under `/tmp/cellule-fleet-member-retirement-{node,clippy,host,docs}.log`.

Baseline `9c99d3a` passed Rust (36937784705), capacity (36937784385), contract
(36937784326), website (36937784644), fuzz (36937784399), model (36937784598) and
Compose smoke (36937784915 / 110622398881). Compose routing
110622399162 remained running when inspected. A model workflow's success does
not establish that optional broad campaigns ran. Earlier failure evidence
remains below; these baseline results do not qualify the new implementation.

The host's automatic rotation still uses ordinary best-effort closure. Requested
maintenance rotation, durable follower enrollment before recruitment CAS,
complete role observation and journal retirement remain unwired. A member
proof alone must not complete SettleRoles or Finalize. Remaining W6–W10 matrices,
sustained convergence, all scenario deliverables, process/provider/mixed-binary
qualification and operator rollout/runbooks remain part of the active goal.

## October 1 2026 managed reader producer checkpoint

`CellNode::install_fleet_reader_enrollment` binds the existing manager to the
shared fleet journal before start and the first activation. Configured fleet
hosts require the owned binding before readiness. Ordinary peer hints and
prepared activation use the same source selection, admission and native opening
path. Both signed physical boots and fresh intent revisions participate in
Pending acceptance; only New starts the first opening.

The manager retains 32 bounded finite activation jobs and charges job/record
storage to the existing runtime byte ledger. Waiter cancellation cannot cancel
accepted opening or result publication. Exact original source, opening/closure
evidence and independent native/publication errors remain inspectable through
`enrollment_completion`. A lost Established reply replays original evidence
before ordinary refresh. Policy eviction, refresh fencing, explicit removal and
shutdown join canonical closure before Retired. Failed or cancelled retirement
keeps the fenced view and original event for retry. Missing acceptance plus
proof this owner never began opening settles only the local entry; removal does
not create another Pending request. Unobserved establishment and unjoined task
failure remain blocked.

Nine new public cases use the durable SQLite journal with actual readers,
signed advertisements and the host lifecycle. Seven cover Pending before native
work, a cancelled activation, lost acceptance/Established/Retired replies,
independent backend reconstruction, cancelled removal, a real native VFS stall
across a host shutdown deadline, local cordon refusal with independent retained
errors, and journal cordon winning between intent observation and acceptance.
Two cover required startup binding and shutdown with a missing binding. A typed
counter query checks receipt-bound readback. These are local role fixtures, not
process/provider or complete fleet-observer qualification.

Intermediate verification caught two relevant issues. An exact-version registry
scan correctly conflicted while the owned producer advanced its version; the
fixture now retries only that typed conflict with a bounded fresh scan. Bulk
shutdown had removed healthy siblings before the last native join, contradicting
the established cancelled-shutdown inventory contract. The final implementation
retains the complete collection until all native joins finish. Closed reader
queries return Fenced; the new fixture initially expected RuntimeClosed and was
corrected to the existing contract. Profiles and production refusal semantics
were not weakened.

| Final source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `c67c4e5d291e0c908277556788b3237e52af3c4f` |
| Rust/Cargo manifest | 593 tracked/nonignored `.rs`, `Cargo.toml` and `Cargo.lock` paths; sorted SHA256, two spaces, relative path lines. SHA256 `06a97bce23936f849659154cc923dfedc980ca3d0821cc8044734d44dd24b234`. This includes five existing nested Cargo locks omitted by the previous manifest, plus three new Rust modules. |
| Complete public host node target | 72 passed; none filtered or ignored. |
| Complete fleet operations example target | 60 passed; none filtered or ignored. |
| Public runtime reader selection | 14 passed; 177 filtered; one isolated RustFS case ignored. |
| Application reader integration selection | 5 passed; 40 filtered; none ignored. |
| Distinct scoped cases | 151 passed; nine new example cases. |
| Host all-target/all-feature Clippy | Passed with warnings denied. |
| Host/runtime API documentation | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 110 Rust snippets, 1150 Markdown links and 28 SQL/peer assertions with 566 links passed. |

Commands use Rust/Cargo 1.97.0, the lockfile, `CARGO_INCREMENTAL=0` and the
checkout's Workspace target directory. Raw final command logs and the source
manifest are retained under `/tmp/cellule-fleet-reader-producer-*` on the execution
host. The host remove/shutdown APIs now return Result so durable retirement
failure reaches the canonical facility drain. Persisted production IDs, object
paths and message formats are unchanged. The example adds an additive typed
counter query and uses freshly compiled example releases.

Baseline `c67c4e5` passed workspace/MSRV (36933281069 / 110607525964 and
110607525809), object/follower capacity (36933281010 / 110607525081 and
110607524697), contract (36933281080), website (36933281071), fuzz (36933281047),
fast/negative model (36933281034) and Compose smoke (36933281024 / 110607805140).
Broad model/simulator were skipped. Its routing job 110607804555 remained
running when inspected. Earlier `e472441` routing 110587330915 is authoritatively
Cancelled, with smoke passed. Historical failure evidence below remains retained;
later green baselines do not establish the causes of those failures or qualify
this new source.

Next, wire follower production and complete revisioned role observation, then
consume canonical replacement/failed-process evidence for evacuation and node
finalization. The movement commands still declare incomplete role coverage.
All remaining W6–W10 matrices, sustained convergence, four scenario deliverables,
process/provider/mixed-binary qualification and operator rollout/runbooks remain
part of the active goal.

## October 1 2026 exact reader enrollment source checkpoint

`ReadReplicaSource` is opaque metadata prepared through canonical authority and
signed live-boot validation. It carries the exact target, incarnation, code,
schema, owner, epoch, root, physical source node and fleet. Preparation opens no
view and reserves no native resources. `CellReadReplica::open_source` opens the
original pinned root through the same admitted native path as ordinary opening.
Newer publication cannot replace it. Installation and query release still
check authority and the original signed boot identity. Ordinary opening retains
its admission-before-provider-I/O order.

The host manager prepares selected sources and offers initial `activate_source`
through its existing activation lane. An already installed view is refused
without refreshing it; ordinary authenticated hints retain refresh behavior.
After entering the lane, a retained prepared activation joins opening on manager
closure, closes an uninstalled view and returns RuntimeClosed. The adapter must
retain its accepted future across transport waiter cancellation. Source metadata
cannot by itself authorize an enrollment or establish durable role coverage.

Three new public cases verify old-root SQL readback after newer publication,
normal refresh to the newer root, stale-source and cordon refusal, manager
initial-only behavior and native-open closure. The last case pauses a real VFS
range read only while the reader's SQL job ledger is charged, closes the manager,
then verifies opening remains owned until release and all charges settle.
The first runtime fixture initially invoked source drain twice after deliberately
releasing it to test fencing; the repeated call returned CellDraining. The final
fixture joins the already-released runtime directly. No production error,
profile or assertion was weakened.

| Final source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `e472441c99ac7e3c5da95809a874f5138cfe37d0` |
| Rust/Cargo manifest | 585 tracked/nonignored paths; sorted SHA256, two spaces, relative path lines. SHA256 `5da2afb2fc2f7e2cb0c6f52f36f7e6d8d4941ca835702609d94299e61438b8cf`. |
| `cargo test -p cellule-runtime --test runtime --all-features --locked read_replica::` | 14 passed; 177 filtered; one isolated RustFS case ignored. |
| `cargo test -p cellule-host --test node --all-features --locked` | 72 passed; none filtered or ignored. |
| `cargo test -p cellule-host --example fleet_operations --all-features --locked` | 51 passed; none filtered or ignored. |
| `cargo test -p cellule-app --test integration --all-features --locked host::replicas::` | 5 passed; 40 filtered; none ignored. |
| Distinct scoped cases | 142 passed. One runtime and two host cases are new. |
| Selected host/runtime libraries, runtime/node targets and example Clippy | All features, warnings denied; passed. |
| Host/runtime API documentation | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 109 Rust snippets, 1149 Markdown links and 28 SQL/peer assertions with 566 links passed. |

Commands use Rust/Cargo 1.97.0, `CARGO_INCREMENTAL=0` and this checkout's
Workspace target directory. Local fixtures use in-memory CAS and actual SQLite;
the ignored RustFS case supplies no provider evidence. The opaque type and API
inventory are additive; persisted IDs, descriptors and peer formats are unchanged.

Baseline CI passed workspace/MSRV (36927118058), object/follower capacity
(36927117222), contract (36927117305), website (36927117280), fuzz (36927117240),
fast/negative TLC (36927117235) and Compose smoke (36927117229 / 110587330500).
Broad TLC/simulator were skipped. Routing (36927117229 / 110587330915) remained
running when inspected. These results qualify the baseline only, do not identify
the cause of earlier failures, and do not qualify this new source.

Next, wire every reader producer to Pending acceptance before initial activation,
retain finite completion/result ownership, reconcile ambiguous acceptance and
publish checked retirement after joined closure. Wire follower producers and
complete observer coverage before enabling maintenance finalization. The full
plan, all W6–W10 gaps and their qualification requirements remain active.

## October 1 2026 joined reader closure checkpoint

`CellReadReplica::close_and_join` fences admission, detaches the current
snapshot across every clone and joins accepted queries, authority reads,
refreshes and native SQLite opens. Snapshot and accepted-operation lifetimes
share one atomic closure gate. A native open returns the complete owned
snapshot, so cancelling its refresh waiter cannot hide an uninstalled view.
Resource fields drop before the lifetime signals completion. Historical
`receipt()` metadata survives detachment without asserting current authority.

The host reader manager retains each view until joined removal succeeds.
Cancelled remove/shutdown waiters leave ownership inventoried and joinable.
Shutdown fences all views first, then joins at most 16 concurrently through
the existing activation and host drain lanes. A host deadline leaves the node
Draining until accepted native work settles; retained peer clones cannot keep
snapshot charges after successful closure.

Four new public runtime cases cover retained clones, old queries across a
refresh, cancelled query/close waiters, a cancelled refresh blocked inside an
actual native-open VFS read, and a delayed authority/readiness reply. A public
host case covers cancelled removal/shutdown and a host drain deadline; the
existing reader inventory case also retains peer clones through shutdown.
Fixtures check real SQLite paths and memory, retained-byte, descriptor, worker
and private disk ledgers. Initial fixtures shared the process-wide default disk
budget, so zero reader disk assertions included the source's charges. Separate
node budgets corrected those fixtures; no production threshold was changed.

| Final source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `135492e75cfb142e4b918ce63d3e5c97ba103453` |
| Repository Rust/Cargo manifest | 585 tracked/nonignored paths; sorted SHA256, two spaces, relative path lines. Manifest SHA256 `23f6d06cc2a3788db971204ea2f2acecc325deecbd926c85d6fd5509247b4811`. |
| `cargo test -p cellule-runtime --test runtime --all-features --locked read_replica::` | 13 passed; 177 filtered; one isolated RustFS case ignored. Four cases are new. |
| `cargo test -p cellule-host --test node --all-features --locked` | 70 passed; none filtered or ignored. One case is new. |
| `cargo test -p cellule-host --example fleet_operations --all-features --locked` | 51 passed; none filtered or ignored. |
| `cargo test -p cellule-app --test integration --all-features --locked host::replicas::` | 5 passed; 40 filtered; none ignored. |
| Distinct scoped cases | 139 passed; the RustFS case remains ignored. |
| `cargo clippy -p cellule-runtime -p cellule-host --lib --test runtime --test node --example fleet_operations --all-features --locked -- -D warnings` | Passed after naming the test fixture's query-gate type. No lint was suppressed. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-host -p cellule-runtime --all-features --no-deps --locked` | Passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 108 Rust snippets, 1149 Markdown links and 28 SQL/peer assertions with 566 links passed. |

Commands use Rust/Cargo 1.97.0, `CARGO_INCREMENTAL=0` and
`$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations`.
These local fixtures use in-memory CAS and actual SQLite. The ignored RustFS
case supplies no provider evidence. Broad workspace and process/provider
qualification remains assigned to CI and controlled environments.

This implements a reader closure prerequisite for W3/W7. Reader/follower
enrollment producers, complete observations, replacement-policy evidence,
durable retirement and finalization remain required. The last reader receipt
alone cannot settle a fleet enrollment or establish safe node removal.

Baseline `135492e` passed workspace/MSRV (36921861844), contracts (36921861824),
website (36921861940), fuzz (36921861753), fast/negative TLC (36921861711),
follower capacity (36921862099 / 110569602571) and Compose smoke
(36921861765 / 110569778663). Broad TLC/simulator were skipped; routing remained
running when inspected. Object capacity (36921862099 / 110569602323) failed
repeat 2: hot rate 2 completed 59 of 60, with one `scheduler_late` arrival, and
the driver reported `hot: no fully served capacity point`. Repeat 1 passed;
repeat 3 did not run. The original log is retained at
`/tmp/cellule-fleet-ci-36921862099-object.log` and the full original artifact at
`/tmp/cellule-fleet-capacity-36921862099`. The qualification profile remains
unchanged. A passing workspace on this baseline does not identify the cause
of the earlier controller-restart failure or qualify this new reader source.

## October 1 2026 boot enrollment and startup barrier checkpoint

Configured fleet hosts hold the runtime's existing writer, reader and follower
gate before a lease can expose acquisition. `with_fleet_startup_intent` validates
the retained physical row before runtime construction. Confirmation reads the
exact Established boot and current intent through the new required atomic
`FleetEnrollmentJournal::load_boot` contract. It records checked intent while
leaving admission held. `start()` checks required facilities/task supervision
before removing the hold. Cordon/drain and pressure remain independent and
sticky; changing admission preserves the original measurement timestamp.

A maintenance reboot enters `NodeState::Maintenance`: `is_ready()` stays false,
while `is_management_ready()` exposes authorized action and inspection paths.
The executor remains bound to this host's physical node, scope and session.
Lifecycle/startup locking rejects a delayed proof older than a newer confirmed
intent and rejects reads completing after shutdown. Applications still own
authorization and delivery of intent transitions after startup confirmation.

The executable now accepts Pending before actual signed canonical directory
creation, publishes checked Established evidence, derives the lease guard from
that advertisement, confirms the boot and starts the host. Existing ambiguous
acceptance cannot repeat an unobserved creation. Independent SQLite reopening
adopts a lost Established reply with the original evidence and time. Boot
advertisements publish zero receive capacity until readiness; all samples are
the actual runtime classifier output.

Application boot owners remain retained through joined runtime shutdown,
guard fencing, canonical directory withdrawal and registry retirement. A
permanent exact-session tombstone adopts an already committed withdrawal;
absence/expiry alone cannot settle an obligation. Replay checks the original
spec and Established evidence and preserves a committed retirement. Both real
movement commands require all three boot rows Retired at the final registry
revision in addition to their existing resource-ledger checks.

Seven public startup cases cover missing/Pending/foreign rows, contradictory
same-revision modes, wrong Active boots, lost acceptance/Established replies,
required-component checks, racing cordon, maintenance reboot management,
delayed stale reads/shutdown and canonical withdrawal/retirement replay. Tests
use actual local hosts, directory signing/validation and SQLite transactions.
Initial fixtures incorrectly used a 60-second advertisement lifetime, mismatched
directory image, a timestamp preceding the runtime sample and a stale registry
version. Existing validators rejected them. A closed-journal assertion expected
an IO error; the final test verifies its original `RuntimeClosed` source. Those
fixture corrections did not alter any production qualification profile.

| Final source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `5fc341bc31b098b3bacee3b7a8e98aca4f4c1d07` |
| Complete repository Rust/Cargo manifest | 588 tracked/nonignored paths; sorted SHA256, two spaces, relative path lines. Manifest SHA256 `fbb3715e14316c2943866ab4c2a07a6b1d5517a90ed8db5b1cd850e9c320aa7d`. |
| Runtime admission library selection | 4 passed; 491 filtered, none ignored. Two cases are new. |
| Complete public host node target | 69 passed; none filtered or ignored. |
| Complete executable example target | 51 passed; none filtered or ignored. Seven cases are new. |
| Distinct scoped cases | 124 passed. |
| Host/runtime library, public node and example Clippy | All features, warnings denied; passed. |
| Host/runtime API documentation | All features, warnings denied; passed. |
| Actual executable commands | Overload and controller-restart: each released/activated/retired two Cells, checked two receipts, joined three nodes and retired three boots. Peak restore charge 2,550,136,832 bytes under the unchanged 8-GiB/two-move budget. Replacement also adopted two lost replies at epoch 2 and joined two expired unused reservations. |
| Static gates | Format, diff whitespace, module/boundaries, 108 Rust snippets, 1148 Markdown links, and 28 SQL/peer assertions with 565 links passed. |

Commands use Rust/Cargo 1.97.0, the lockfile, `CARGO_INCREMENTAL=0` and the
checkout's Workspace target directory. This finite local reference has no
production heartbeat provider, complete reader/follower enrollment producers,
complete observer, process-fault campaign or distributed journal qualification.
The full plan remains active: role evacuation/finalization, remaining primitive
and failure matrices, continuous-load convergence, runnable maintenance/receiver
loss and W9–W10 deployment/operational qualification remain required.

Baseline CI `5fc341b` passed MSRV (36914686533 / 110545650679), contract
(36914686490), fuzz (36914686514), website (36914686782), fast/negative TLC
(36914686630), object/follower capacity (36914686675) and Compose smoke
(36914686644 / 110546093110). Broad TLC/simulator jobs were skipped. Routing
(36914686644 / 110546092620) was still running when recorded.
Workspace (36914686533 / 110545650944) failed the real controller-restart example
at its strict two-lost-release assertion: 43 passed, one failed. Original log is
retained at `/tmp/cellule-fleet-ci-36914686533-workspace.log`. The failing report
did not identify which condition failed. The final source retains every assertion
and the same three-second profile and now includes the complete bounded report,
lost-reply count and original node completion in failure output. Local complete
example runs pass, but they do not establish the CI failure's cause or resolution.
This source requires its own CI; earlier routing failure evidence remains below.

## October 1 2026 public primitive maintenance checkpoint

Five new public cases exercise `release_maintenance_cell_at` with actual typed
Effect and Activity claims. They wait for actor quiescence, prove release stays
pending while the exact live lease validates and authority remains Serving,
and check new claims and ordinary reads are refused. Native acknowledgement,
Activity validation/extension/completion and exact-root release use the existing
registry, actor, worker and publication paths. No production API changed.

Effect cases cover normal acknowledgement, a dropped maintenance waiter and
normal lease expiry. An authenticated destination publication precedes source
release. Ordinary receiver activation preserves the original command resolution
and destination inbox receipt. A late acknowledgement loses its lease. Unclaimed
work resumes through the registered native Effect driver. The expiry case keeps
the existing retry backoff, verifies Ready state, reclaims with a new token and
attempt 2, and delivers to the same inbox without applying the command twice.

Activity cases cover extension/completion and normal expiry. A settled completion
replays as Duplicate on the receiver; an expired token is rejected before and
after normal reclamation. The retry preserves activity ID and definition digest,
increments its attempt and replaces its token. The registered native Activity
driver completes an unclaimed blocking activity. Running Workflow waits retain
their run/state, accept a signal on the receiver and deduplicate its replay.
Preserved due timers advance through the registered Tick after restoration.
Every receiver and source joins with zero retained bytes.

Initial fixture synchronization incorrectly awaited an advisory cached work
sample during the separate maintenance preflight read; those runs timed out.
The final cases synchronize on installed quiescence and directly validate the
lease, bounded pending release and authority state. Initial expiry assertions
also incorrectly expected reclaimed work ahead of an already-ready Activity and
an immediately claimable Effect despite its existing backoff. Correcting those
fixtures preserves canonical ordering/backoff and checks actual reclaim and
deduplication. No runtime invariant, profile or threshold was changed.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `e7aa6923bce13d4cc97e069016040a5e1e356fae` |
| Complete repository Rust/Cargo manifest | 586 paths, sorted from tracked and nonignored files; each line is SHA256, two spaces, relative path. Manifest SHA256 `c42597897d612a37a112bab937f6785c725ebf0aebf2bbeb1220d25584ddfff2`. |
| Public maintenance selection, primitives + protocol | 9 passed: six primitives (43 filtered), three protocol (29 filtered); none ignored. Five cases are new. |
| Complete public client target | 30 passed; two manual throttled-provider measurements ignored; none filtered. |
| Public Workflow API selection | 7 passed; 42 filtered, none ignored. |
| Distinct selected cases across these commands | 41 passed; maintenance/client/workflow selections overlap in the five new cases. |
| Selected primitives/protocol Clippy | All features, warnings denied; passed. |
| Static gates | Format, diff whitespace, boundaries/layout, 108 Rust snippets, 1144 Markdown links and 28 SQL/peer assertions with 564 links passed. |

Commands used Rust/Cargo 1.97.0, all features and the lockfile, with
`CARGO_INCREMENTAL=0` and this checkout's Workspace target directory. Fixtures
use in-memory authority and local runtimes. They do not qualify distributed
providers, process interruption, continuous traffic, Cron dispatch or external
Blob owners. The full W6–W10 and earlier enrollment/observation gaps remain open.

CI for the baseline above now passed workspace/MSRV (36912160616), contract
(36912160634), website (36912160311), fuzz (36912160309), TLC fast/negative
(36912160379) and object capacity (36912160550 / 110537198649). Follower capacity,
Compose smoke and routing remained running when recorded. Broad TLC/simulator
jobs were skipped. These results belong to that pushed baseline; the prior
routing failure remains recorded below and this new test source requires its
own CI.

## October 1 2026 busy demand and host acceptance checkpoint

The actor exposes a separate `maintenance_cost` from validated per-Cell LTX
limits. It uses the same conservative native/cache/descriptor/job envelope and
configured disk ceiling as receiver admission, independently of the measured
worker sample. Mutations still invalidate ordinary cost, timestamps and stability;
the configured envelope supplies no readiness or release proof. Blob owners have
no busy envelope and refuse release before foreground closure.

The driver selects this envelope only for the exact physical node and boot of a
retained, unexpired Evacuating maintenance operation. The pure planner also
requires an explicit maintenance demand and a draining donor. Fresh authenticated
collection/source/receiver evidence, receiver projection and the shared two-move/
8-GiB budget remain required. Local execution, publication, external lease and
unknown primitive conditions are deferred to the canonical source barrier;
foreign reader/follower/facility blockers are retained. Ordinary pressure/count
movement still requires settled worker samples. A Draining advertisement or
pressure alone cannot force busy work to move.

Planner-input domain v3 hashes both costs (presence and every admission dimension),
role, code/schema identity and individual blocker classes alongside the existing
capture interval, generation, source boot and root. Retained older digests remain
opaque. Focused digest cases check each new field and expired collection refusal.

The public host case exercises actual accepted SQL work under Cordon and explicit
action 8, closes new foreground admission, joins the original durable receipt,
and restores it on a second public host through ordinary exact-root acquisition.
It checks normal completion, a dropped action waiter and lost result publication.
Release replay preserves the exact original proof after the receiver owns the
Cell. Both hosts join with zero active Cells and retained bytes. These fixtures
use in-memory authority and local lease guards; they do not qualify a distributed
lease provider or complete role evacuation.

The driver model uses real SQLite journal transactions with signed synthetic
observations/effects. It plans busy rows with no measured cost/time/stability,
charges the distinct peak cost, retains permits after a lost action-8 reply and
adopts the exact stored result without issuing ordinary Release or repeating the
effect. Negative cases cover draining/pressure without maintenance, Blob roles,
foreign follower blockers, missing peak cost and missing published position.
This model is distinct from the public host and leased-node example evidence.

Initial digest fixtures used invalid empty module/schema inventories and zero
placement totals; the existing validators refused them. Correcting the fixtures
supplied valid signed inventories and capacities. The lost-response fixture first
expected an OutcomeUnknown blocker after a transport error; the actual contract
retains the unresolved dispatch phase and original transport failure. Its corrected
assertion checks phase 13, absent release proof, refused retirement, retained
permits and exact later adoption. No production gate or qualification profile was
weakened. A private-method fixture assertion and unsupported Default initializer
were also corrected before execution.

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | `5d98eb0e7fa961c5420022c1da07e8cad16e3433` |
| Implementation and main sync | `df55295`; merged `origin/main` (`0dc04a6`) as `869f0e4`. Only the website checker conflicted; the upstream feature-group implementation was retained. The merge introduced no Rust-source delta. |
| Source manifest | 579 Rust/Cargo files; SHA256 `0ffe6084e1f2bcc0f65b70695b42e37892cb7f2733b442ba9889db8167885ef2` |
| Runtime library placement / actor inventory | 16 passed (477 filtered) / 7 passed (486 filtered). |
| Runtime public placement / ownership inventory | 10 passed (16 filtered) / 5 passed (182 filtered). |
| Runtime maintenance selection | 37 passed: 30 library (462 filtered), four Queue (43 filtered), three runtime (184 filtered). One isolated RustFS library case ignored; no provider evidence. Two selected cases overlap the placement/ownership commands. |
| Host digest library | 3 passed; none ignored or filtered. |
| Complete executable example test target | 44 passed; none ignored or filtered. |
| Host fleet node selection | 31 passed; 38 filtered. The new public case exercises three waiter/publication modes. |
| Application local reader selection | 5 passed; 40 filtered, including both cases that timed out in prior workspace CI. This scoped result does not resolve broad CI failure. |
| Selected all-feature runtime/host library/tests/example Clippy | Passed with warnings denied. |
| Host/runtime all-feature API documentation | Passed with warnings denied. |
| Website Rust example checker after merge | All examples compiled across 12 authored guides using the upstream checker. |
| Static gates | Format, diff whitespace, boundaries/layout, 108 Rust snippets, 1143 Markdown links and 28 SQL/peer assertions with 563 links passed. |

These final checks ran on the merged Rust source. Cargo used Rust 1.97.0,
`CARGO_INCREMENTAL=0` and the checkout target directory recorded in the prior
checkpoint. The tables name focused command selections; broad workspace, process,
provider and mixed-binary campaigns remain separate proof obligations.

The prior `b8ec303` routing job (36899568202 / 110495509870) is now terminal:
**failed** `object_only/forwarded_command/c16`. Across its four frozen runs,
median candidate p99 was 343.037178 ms versus baseline 305.3395325 ms (ratio
1.1234614), exceeding the unchanged 1.10 limit. Throughput ratio was 0.9706314
and p95 ratio 1.0280569. The retained manifest identifies baseline `c51dd12` and
synthetic candidate merge `970d874`; this is evidence for that CI source, not
this later checkpoint. Logs and artifact remain at
`/tmp/cellule-fleet-ci-36899568202-routing.log` and
`/tmp/cellule-fleet-routing-36899568202` (remote artifact 11185357976).
Correctness passing inside each benchmark does not erase this performance
failure. No threshold or profile was changed. The updated PR must qualify its
own source and the failure still requires investigation.

Full W6 remains incomplete: every primitive's public acceptance/fault matrix,
Blob external owners and continuous-load qualification still need coverage.
Enrollment/startup barriers, complete role observations/evacuation, finalization,
remaining failure adoption, sustained convergence, runnable maintenance/receiver-
loss scenarios and process/provider/mixed-binary qualification remain required
by the full plan. The previous canonical busy release checkpoint below retains
its original source and proof limits.

## October 1 2026 canonical busy release checkpoint

`CellRuntime::release_maintenance_cell_at` now owns an exact-source maintenance
request independently of its reply waiter. It uses the existing actor movement
permit, shared admission capability, serialized worker, publication barrier and
canonical deactivation/release path. Foreground work closes first. A readiness
read is serialized behind accepted SQL without waiting for the ingress queue to
become idle. Native completion then closes, accepted work/publication joins, and
a second fresh readiness read must pass before canonical release is confirmed.
Lease renewal remains active until confirmation. A newly extended live lease
restores native completion only and waits within the preflight deadline.

`MaintenanceCellRelease::Refused` proves that this request started no canonical
release and retains any source error. Quiescence remains sticky if installed;
native completion is restored on the same admission capability. The deadline
bounds preflight, not confirmed release. A timed-out request's owned read is
joined; its effect ID cannot classify a newer request. Blob owners remain
unproven and are refused before foreground closure.

The journal appends explicit phase 13 (`MaintenanceReleasing`) and action 8
(`ReleaseMaintenance`). Exact Evacuating maintenance identity is required before
dispatch. The host executes that policy through the new runtime method and
preserves definite refusal versus unresolved release. Controller reconstruction,
action replay, atomic absence and retained source inspection keep the exact
release kind; ordinary phase/action values and policies remain unchanged.
Readers must understand the new tags before producers are enabled. Mixed-binary
qualification is still required.

Public SQL and Queue cases cover accepted mutation settlement, exact final-root
release and ordinary receiver restoration, original outcome resolution, surviving
unclaimed Queue work, native validation/acknowledgment, dropped release waiters,
and deadline refusal followed by native completion and a later release. The SQL
case also exercises overlapping expired and newer maintenance requests. These
in-process cases do not establish every late-read interleaving or provider fault.
Pure action cases cover intent binding, codecs, distinct action keys/results,
unknown/absence permit retention, controller replacement and preserved ordinary
release policy. The executable driver model checks explicit action 8 dispatch.

The first public run found that an actor guard refused native completions during
all transfers. The guard now retains that refusal for ordinary idle transfers
and routes maintenance completion through the canonical kernel admission check.
The corrected run passes without weakening expected evidence. Earlier fixture
compile errors (private re-export scope and an invalid test control-state variant)
were corrected before the passing run; they are not qualification results.

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | `b8ec303942d112ce8ca36dc1a6761c5191574d49` |
| Source manifest | 578 Rust/Cargo files; SHA256 `530c407930cbab52a16876136a1cf16ba8e20921e485be6beb02a8431e5865a0` |
| `cargo test -p cellule-runtime --lib fleet::operations --all-features --locked` | 65 passed; 427 filtered. |
| `cargo test -p cellule-runtime --lib --test primitives --test runtime maintenance --all-features --locked` | 35 passed: 29 library (462 filtered), four Queue (43 filtered), two SQL (184 filtered). One isolated RustFS library case ignored; no provider evidence. Eight operation tests overlap the 65-test command. |
| `cargo test -p cellule-host --example fleet_operations --all-features --locked` | 41 passed; none ignored or filtered. |
| `cargo test -p cellule-host --test node node::fleet --all-features --locked` | 30 passed; 38 filtered. These existing public host cases do not qualify the new busy maintenance action. |
| `cargo clippy -p cellule-runtime -p cellule-host --lib --tests --example fleet_operations --all-features --locked -- -D warnings` | Passed after removing an unused lint expectation; no lint or qualification bound was weakened. |
| Static gates | Format, diff whitespace, boundaries/layout, 108 Rust snippets, 1143 Markdown links, and 28 SQL/peer assertions with 563 validator links passed. |

Cargo commands use Rust 1.97.0, `CARGO_INCREMENTAL=0` and
`$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations`. Proof is
focused in-process behavior with real SQLite and in-memory authority, plus the
existing local leased-node overload/restart examples. Broader suites belong to
CI or isolated qualification snapshots.

New demand collection still requires ordinary settled eligibility. Busy demand
planning, public host acceptance/fault coverage, Effect/Activity and remaining
primitive acceptance cases, Blob owners, enrollment/startup intent barriers,
reader/follower evacuation, finalization, sustained convergence and process/provider
qualification remain open. This checkpoint does not complete W6 or the full plan.

Baseline `b8ec303` CI: MSRV, contract, website, decoder fuzz, TLC fast/negative,
both capacity jobs and smoke passed. Routing remained pending. Workspace tests
failed in `pending_reader_activation_retains_a_new_publication_hint` and
`publication_hints_reach_readers_beyond_the_activation_concurrency`, both timing
out at `crates/cellule-app/tests/host/replicas.rs:285`. The original failed log is
retained at `/tmp/cellule-fleet-ci-36899568285-workspace.log`. Qualification
thresholds are unchanged; the updated PR must qualify its own source in CI.
Skipped broad simulator/TLC jobs supply no evidence.

## October 1 2026 foreground quiescence checkpoint

`CellRuntime::quiesce_cell_at` installs a sticky exact-generation foreground
gate without interrupting previously actor-admitted SQL. It checks runtime
boot, generation, incarnation and ownership epoch before closing the shared
admission capability. Unavailable publisher state refuses conservatively.
This API is a local runtime boundary: applications must retain and authorize
maintenance intent before calling it. It supplies no release proof.

Native Queue/Effect lease commands and validation, and Activity completion,
extension and validation have private registry admission classification. New
claims and foreground work close; original outcome resolution stays available.
All calls use the existing bounded mailbox, worker, fencing, transaction and
durability gates. Duplicate binding errors now preserve the original handler,
preventing an application replacement from inheriting native classification.
Descriptor/release bytes and persisted IDs are unchanged. New background
hydration and compaction stop, while required inventory can refresh.

The serialized worker reports separate maintenance readiness alongside ordinary
idle readiness. It retains simultaneous live lease classes, refuses malformed
lease/schema/time observations, and leaves expired lease rows unchanged. Pending
durable messages, Effects, timers and Workflow waits can be carried by an exact
root after claims close. Blob stream/upload/pin coverage is explicitly blocked.
The actor exposes optional readiness and its quiescence flag; the planner hashes
these under input domain v2 without rewriting retained attempt digests.

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | `7be792bf5a71fc7d6ec9b82d615f512631ed4833` |
| Source manifest | 573 Rust/Cargo files; SHA256 `67b06c0b29f8dc6e17d4ec3e008ea8f311f0eb3c594ea742d8eb82e518edd91c` |
| `cargo test -p cellule-runtime --lib --test primitives --test runtime maintenance --all-features --locked` | 23 passed: 21 library (462 filtered), one Queue (43 filtered), one accepted SQL (184 filtered). One library RustFS case ignored because its isolated provider environment was not supplied; it establishes no provider evidence. |
| `cargo test -p cellule-host --example fleet_operations --all-features --locked` | 41 passed; none ignored or filtered. |
| `cargo test -p cellule-host --test node node::fleet --all-features --locked` | 30 passed; 38 filtered. |
| `cargo clippy -p cellule-runtime -p cellule-host --lib --tests --example fleet_operations --all-features --locked -- -D warnings` | Passed. |
| Static gates | Format, boundaries/layout, 108 Rust snippets, 1143 Markdown links, 28 SQL/peer assertions and 563 validator links passed. |

The Queue case checks all source identity mismatches before closure, sticky
replay, refusal of new sends/claims/info and raw callbacks, exact validation,
acknowledgment, live-to-settled inventory refresh and joined shutdown with zero
retained bytes. It also attempts duplicate application replacement bindings and
verifies unchanged release bytes plus the original native handlers. The SQL
case closes admission while an accepted handler is blocked, then joins its
commit, resolves its original outcome and shuts down with zero retained bytes.

The first public Queue run failed because the test's raw query requested a
zero-byte reservation; the existing byte validator correctly refused it before
admission. Correcting that fixture to a valid reservation produced the passing
run above. No production bounds or expected evidence were weakened.

Commands use Rust 1.97.0, the checkout target directory recorded below and
`CARGO_INCREMENTAL=0`. These are local focused checks with real SQLite and
in-memory authority. Effect/Activity maintenance public acceptance cases,
Blob owner coverage, journal-bound busy release, enrollment/startup barriers,
complete reader/follower evacuation and finalization, continuous-load
maintenance and process/provider qualification remain open. The full plan is
still incomplete; this checkpoint does not certify W6 or complete maintenance.

On baseline `7be792b`, workspace, MSRV, contract, website, decoder fuzz, both
capacity jobs and smoke passed. Routing remained pending at inspection. The
older object-capacity failure remains recorded below; baseline successes do not
qualify the new source, whose full gates must run in CI after push.

## October 1 2026 maintenance admission checkpoint

Maintenance Cordon now runs through the public node-owned action executor.
Exact journal acceptance binds the physical node, boot, operation and retained
intent before closing the existing writer, new-reader and new-follower role
gate. Existing Cell owners remain available; this step releases no role and
takes no shutdown lane. Unsupported maintenance role, finalization and
inspection effects fail before acceptance or inventory work.

The driver dispatches Cordon from Requested, commits Cordoned only after a
checked durable result, then commits BeginEvacuation on a later pass. Lost
replies, unconfirmed publication and Unknown retain the phase and intent.
Maintenance has a bounded deadline share and a separate original error in
`maintenance_failure`; an ambiguous timeout rereads the journal and epoch.
Stopping optional scheduling still allows these intent steps. Deadline expiry
keeps the cordon, prevents new maintenance allocations, and does not discard
accepted work. New movement deadlines also respect the operation deadline.

Retained intents now overlay signed placement samples before donor selection.
This fixes normal-pressure maintenance donors being skipped when the roster is
partial. Fresh partial observations can allocate settled evacuation through the
existing planner and shared two-attempt budget. Empty permits still leave the
operation Evacuating: complete role inventory, busy-work quiescence and joined
shutdown/withdrawal evidence are required before completion.

A rerun exposed an immediate publication-retry race: the watch result could
arrive before its task exited, so the next dispatch joined the old unconfirmed
receipt. Result delivery now joins the owned task's short exit sequence. A
subsequent dispatch can retry the retained publication; dropped waiters still
leave the task owned. The original failing run was 29 of 30 host cases; the
final suite and ten exact-case repetitions below pass after this repair.

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | `b61768f95fe159f07db975f68950c0588ab563cc` |
| Source manifest | 568 Rust/Cargo files; SHA256 `66e9bf466b87fd4c3c9f0ab745a726c4a505d916337fa6ba1ada2ef4e95afe44` |
| Journal SQL | Unchanged SHA256 `6fd5c72f5e8f4bd245797dbc6f433baca0cece4d519aa954f7526b2ae61ad668` |
| `cargo test -p cellule-host --test node node::fleet --locked` | 30 passed; 38 filtered. Four new public maintenance cases cover closed sticky admission, existing-owner query readback, duplicates, lost reply/waiter, wrong boot, stale intent and unsupported effects. Shutdown joins and checks Stopped with zero actor/retained charges. |
| `cargo test -p cellule-host --example fleet_operations --locked` | 41 passed; none ignored or filtered. Six new driver cases use real SQLite with synthetic effects; both real-node overload/controller-restart cases also pass. |
| Exact immediate-retry case repeated ten times | Each passed; 67 filtered per run. No delay added to the retry. |
| `cargo clippy -p cellule-host --lib --tests --example fleet_operations --all-features --locked -- -D warnings` | Passed. |
| Static gates | Format, boundaries, module layout, 108 Rust snippets, 1140 Markdown links, 28 SQL/peer assertions and 562 protocol links passed. |

Commands used Rust 1.97.0, the recorded checkout target and
`CARGO_INCREMENTAL=0`. Public maintenance tests use a local in-memory acceptance
journal with real runtime/SQLite owners. The separate driver tests use durable
SQLite and synthetic endpoint effects. These establish admission and driver
sequencing; they do not establish complete three-node maintenance, enrollment
producer/startup barriers, role evacuation, busy primitive quiescence or
process/provider qualification. The full plan remains incomplete.

### Baseline CI qualification result

On baseline `b61768f`, workspace and MSRV
[run 36888176019](https://github.com/crabbuild/cellule/actions/runs/36888176019),
contract, website and decoder fuzz passed. Follower capacity in
[run 36888175986](https://github.com/crabbuild/cellule/actions/runs/36888175986)
and smoke in
[run 36888176067](https://github.com/crabbuild/cellule/actions/runs/36888176067)
also passed; routing was still pending at inspection.

The same capacity run's object-proof job **failed**: the skewed window at two requests per second per node
completed 59 of 60 planned arrivals. Arrival 49 was `scheduler_late`
(scheduled 8,166,666 us, started 8,464,809 us); the driver reported no fully
served skewed capacity point. Artifact 11175158203 retains the failed evidence
with ZIP SHA256 `34b6aad753434eacd208c1c03ec49952fdea202adb62e435790b3690b807605a`.
This is failed qualification, irrespective of other passing jobs or local
functional tests. Profiles, deadlines and expected evidence remain unchanged;
the updated PR must complete its own CI and the full fleet qualification work.

## October 1 2026 atomic API lint repair

Workspace CI `36885649739` failed the warnings-denied lint gate on deprecated
`AtomicU64::fetch_update` calls. All eleven workspace calls now use its renamed
`try_update` API, preserving orderings, update closures, return handling and
counter behavior. A minimal compiler probe and the selected builds below
confirm availability on the declared Rust 1.97 minimum. No lint allowance,
toolchain pin, profile threshold or qualification requirement changed.

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | Controller checkpoint `ecec878` |
| Source manifest | 565 Rust/Cargo files; SHA256 `5fff33c4ab3ab4da25f73c4280ee62049ee08f5d85d39a18da6d26bc2164939c` |
| `cargo test -p cellule-store --lib --features test-support read_admission --locked` | 18 passed; 147 filtered. |
| `cargo test -p cellule-ltx --lib --features replica environment::tests::prepared_disk_budget --locked` | 8 passed; 98 filtered. |
| `cargo test -p cellule-runtime --lib fleet::operations --locked` | 60 passed; 419 filtered. |
| `cargo test -p cellule-host --example fleet_operations --locked` | 35 passed; none ignored or filtered. Includes both real-node scenarios. |
| `cargo clippy -p cellule-store -p cellule-ltx -p cellule-runtime -p cellule-host --lib --tests --example fleet_operations --all-features --locked -- -D warnings` | Passed on Rust 1.97.0. |

Commands used the recorded checkout target with `CARGO_INCREMENTAL=0`.
Format, boundary/layout, whitespace and 1140 Markdown links passed. This is
selected local evidence; latest stable workspace lint remains a CI gate.
On baseline PR head `734650d`, both object- and follower-capacity jobs in
run `36885649670` passed with the existing qualification profiles. The earlier
failed object-capacity run remains recorded below. Routing and smoke jobs
were still pending at the last inspection; they establish no passing evidence
for this checkpoint or the full goal.

## October 1 2026 controller replacement checkpoint

`fleet_operations controller-restart` now runs real three-node movement with
two source replies lost after durable release. Both unconfirmed attempts retain
their permits and exact inputs. A bounded real-time wait expires the original
controller lease; an independently reopened journal and a different claimant
acquire epoch 2. The old claimant receives the original journal Fenced error
without changing the successor's snapshot. Fresh source inspection adopts the
retained exact release proofs instead of repeating release.

The driver now joins expired unused receiver credit before first activation
after a proven release. Committed cleanup preserves the release and both fleet
charges; canonical ordinary admission restores the same Cells afterward.
`receiver_resources_settled` exposes this existing committed fact without
changing codecs. Both other nodes verify original acknowledged outcomes and
SQL readback, old source handles fail, and shutdown joins all three nodes and
their resource owners.

```text
released=2 activated=2 retired=2 receipt_checks=2 max_inflight=2 max_restore_bytes=2550136832 joined_nodes=3 receiver_nodes=2 lost_release_replies=2 controller_epoch=2 expired_receiver_cleanups=2 blocker_count=1
blockers=[IncompleteObservation]
```

| Source and evidence | Recorded value |
| --- | --- |
| Baseline | `734650d35a355506a76775cab2f75015789d8792` |
| Source manifest | 565 Rust/Cargo files; SHA256 `a6795e35b4b07c6bf5113a12d526e51c530bd497b93c3a342fe0fb08670229a7` |
| `cargo test -p cellule-host --example fleet_operations --locked` | 35 passed; none ignored or filtered. |
| `cargo test -p cellule-runtime --lib fleet::operations --locked` | 60 passed; 419 filtered. |
| `cargo run -p cellule-host --example fleet_operations --locked -- controller-restart` | Exit zero with the output above. |
| `cargo clippy -p cellule-runtime -p cellule-host --lib --example fleet_operations --tests --locked -- -D warnings` | Passed. |
| Static gates | Format, boundaries, module layout, 108 Rust snippets, 1140 Markdown links, 28 SQL/peer assertions and 562 protocol links passed. |

Commands used Rust 1.97.0 and the existing checkout target with
`CARGO_INCREMENTAL=0`. The fixed profile has a three-second controller lease
and 500-ms interval; node leases, prepared-credit expiry and all observations
use actual time. This establishes in-process controller replacement and local
durable reconstruction. It does not establish process crash, receiver-session
loss, complete fleet observations, busy maintenance or provider qualification.
All full-plan gaps remain required.

Baseline CI `36885649739` passed through tests and website Rust compilation,
then failed workspace Clippy because newer stable Rust deprecated
`AtomicU64::fetch_update`. The contract, MSRV, website, decoder-fuzz and
object-capacity jobs passed. Remaining jobs were still running when inspected.
The renamed API compiled on Rust 1.97.0 in a separate minimal probe; workspace
call sites still need updating before that lint failure is resolved.

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

### Main sync and example compiler repair

Main revision `c51dd12` merged without conflicts in `cddb5c9`. The updated
565-file Rust/Cargo manifest is SHA256
`b7028a45fefa23a2f780c74ef49c301c692ff1e6111d5bacb2d286825626952d`.
After the merge, the runtime operation tests again passed 60 cases (419
filtered), and the example passed all 34 cases. Clippy for runtime/host
libraries, example and tests passed with warnings denied. Format, boundaries,
module layout, 108 Rust snippets, 1139 Markdown links and the unchanged SQL/peer
validators passed.

The website compiler now includes all six documented libraries from Cargo's
reported artifacts. An explicit host target separates target libraries from
host build dependencies; both artifact directories remain available for
procedural macros. Earlier intermediate checker runs exposed a wrong `bytes`
crate identity and a missing procedural-macro search path. The final command
`python3 scripts/check-web-rust-examples.py` exited zero and compiled all 12
authored guides. Checker SHA256:
`6d3d912944910fd784b1672cb8fcce38e92dd9ae42c1796afc64d1d667c34564`.
It used the same checkout target and toolchain. Updated full CI remains
required; the baseline workspace failure stays a recorded failed run.

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
