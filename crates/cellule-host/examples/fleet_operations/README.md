# Fleet operations reference example

Status: durable journal and public-driver models, plus a finite real three-node
admission-overload and controller-restart scenarios with receipt readback.
The `balance` command exercises real count convergence. Complete maintenance,
receiver-loss, production observations and
qualification remain required by the [fleet plan](../../../../docs/fleet-operations-plan.md).

## Run

Run the real-node scenario from the workspace root:

```sh
cargo run -p cellule-host --example fleet_operations --locked -- overload
```

It creates three independently leased CellNodes over shared strict in-memory
object storage, initializes twelve real SQLite Cells, and acknowledges a command
on each. A held seven-GiB disk admission token causes the existing actor's
measured ledger classifier to enter Shedding after its normal dwell. The public
reconciler allocates its bounded two-move batch; the scenario releases that
pressure token, stops new scheduling, prepares real receivers, and reopens an
independent SQLite controller client before settling movement. Both receiver
nodes restore state, resolve the original command outcomes and confirm the old
source handles are fenced. Exit joins all three runtimes and the journal and
checks their resource ledgers are empty. Output separates release, activation,
retirement, receipt checks, shared budget maxima and advisory blockers. It also
reports three confirmed boot retirements after joined runtime shutdown.

Each boot now registers its retained physical intent, builds with the shared
startup hold, journals Pending, and creates its actual signed advertisement in
the canonical `NodeDirectory`. Established publication and an atomic boot/intent
read precede `start()`. Confirmation preserves the role hold until the ordinary
required-component checks pass. Boot advertisements expose zero receive capacity
before readiness; later observations publish actual runtime samples through
canonical directory CAS. The finite example derives its local lease guard from
the canonical advertisement's expiry and advances it only after confirmed CAS.
The caller drives these heartbeats while observing; there is no new background
scheduler or production heartbeat/provider implementation.
Before each renewal it calls `refresh_fleet_intent` against the original Established
boot. A retained maintenance intent closes the shared role gate and is reflected
in the actual signed operational sample even if the Cordon RPC was lost. Failed
intent reads prevent that heartbeat and guard renewal. Existing writer routing
remains available until its own quiescence or terminal drain.

The application retains each original boot advertisement and binds confirmed
boots to native closing before readiness. That owner joins runtime and lease
maintenance, withdraws through the canonical directory, checks the permanent
session tombstone, and retires the exact registry obligation. The retained
application cleanup also handles partial startup and fences its lease guard. Both
paths require `is_withdrawn`: a tombstone retaining a log or recovery claimant
cannot settle boot retirement. Replay
can adopt an already committed withdrawal or retirement. A missing advertisement,
lease expiry or unresolved Pending record alone cannot prove closure.

This measures **admission pressure**, not physical disk usage or throughput. The
actor retains its existing local eviction budget; fleet counts cover only its
journal-backed batch. The fixed three-boot transport pins identities in trusted
composition. The reconciler supplies its fully traversed durable `FleetRoster`
to the collector, including Pending work, failed boots and terminal records,
and rechecks the original full snapshot after collection. Count balancing also
requires exact established boot advertisements, settled enrollment and writer
rows matching signed counts. Its collector uses request-bound native snapshots,
the original enrolled signing keys, both directory session scans and fresh Cell
authority. It rereads authority and actor topology and confirms the full journal
barrier. Complete coverage is supported for this private constructor's bounded
writer-only profile: twelve catalog-backed SQL Cells and three managed boots.
Unexpected advertised boots, Pending/role enrollment, native role installations,
expired/fenced log obligations, changed topology or stale ownership prevent
complete counts. Unbound pages alone supply no absence proof; the closed
constructor profile and durable/current-authority checks are also required.
It stops new scheduling after the bounded batch; this does not demonstrate
sustained-overload convergence. It is not a production complete observer, deployment authentication system,
process-crash test, or distributed-provider qualification. Reader/follower
deployment collectors still need complete native-role and replacement-policy
evidence before enabling maintenance finalization.

Run ordinary count balancing with real residence and repeated batches:

```sh
cargo run -p cellule-host --example fleet_operations --locked -- balance
```

The scenario starts at 12/0/0 and honors the planner's actual 60-second residence
rule and post-batch sample barrier. Each pass renews canonical boot leases and
drives the same shared two-move budget. Eight distinct Cells converge to 4/4/4,
with durable cooldowns preventing repeat movement. The scenario resolves all
eight original acknowledged outcomes, reads their SQLite values, verifies
successor authority and fences the old handles. It issues five-minute command
receipts for this longer scenario; overload and controller-restart retain their
original one-minute receipts and profiles. A 120-second convergence deadline
fails with retained attempt diagnostics, then exit joins all nodes and retires
their exact boot obligations. This is real-time in-process count convergence;
sustained workload, process/provider and complete role qualification remain
required.

Run controller replacement after ambiguous source replies:

```sh
cargo run -p cellule-host --example fleet_operations --locked -- controller-restart
```

This uses the same real nodes, journal, bounded driver and receipt checks.
The transport loses both replies after source release commits. The scenario
keeps both attempts charged, waits for the actual controller lease to expire,
reopens an independent journal client and resumes with a different claimant.
Epoch 2 adopts retained source proofs; the old claimant's renewal is fenced.
Expired unused receiver credit is cancelled and joined before first activation
through the canonical ordinary admission path. Output also reports lost replies,
controller epoch and confirmed expired-credit cleanup. No release is repeated.
The fixed test profile uses a three-second controller lease and 500-ms interval;
node leases, reservation timestamps and observations use actual time. This is
in-process controller replacement with durable local reconstruction; process
crash, provider and receiver-session loss qualification remain outstanding.

To inspect a retained example journal, supply a database path in an existing directory,
outside canonical Cell storage. The command creates the journal if absent or
reopens it with the same scope and profile, prints its bounded head summary,
and closes the connection after joining accepted work.

```sh
cargo run -p cellule-host --example fleet_operations --locked -- inspect-journal /tmp/cellule-fleet.sqlite
```

The example uses fleet identity `[200; 32]`, application identity `[3; 16]`, and
`FleetProfile::default()` for overload and journal inspection. A new journal starts with bootstrap incomplete and
scheduling stopped. Inspection does not bootstrap the registry or enable
movement. Reopening with a different scope or profile fails.

On workstations with the mounted Workspace volume, set `CARGO_TARGET_DIR` to
a directory for this checkout beneath `$HOME/Workspace/crabbuild-target`.

## Transaction and lifecycle contract

The reader producer cases use `fleet_reader_enrollments_page` while original
acceptance, establishment and native open replies are paused. They check original
requests, separate errors, accepted preparation before a row exists, page memory
admission/release, stable pagination and cursor invalidation. Native reader views
remain a separate inventory; the collector still requires complete role and
authority evidence before maintenance can finalize.

Follower producer cases capture `fleet_follower_enrollments_page` while provider
preparation and member acceptance are paused. They preserve all original requests
and proof identities, check the shared page charge and memory refusal, reject
changed/missing continuations, and keep original failed retirement/error Arcs.
Idle protocol or absent rows do not establish a joined supervisor or empty native
lanes. The reference collector still needs authenticated complete role envelopes.

The public host durability suite also captures `fleet_durability_supervisor`
while original retirement/recruitment is unresolved, after byte admission
closes during drain, and after local rotation completion. It compares original
proof/error references and verifies fixed metadata admission and release.
Supervisor completion and bank availability are separate from producer row
counts; these captures still cannot certify fleet settlement.

```sh
cargo test -p cellule-host --test node --all-features --locked node::durability
```

[`SqliteJournal`](journal/mod.rs) implements all three host journal interfaces
in one local SQLite transaction domain. It lives in the embedding example;
the host library remains provider neutral.

| Boundary | Implementation |
| --- | --- |
| Controller and permits | `BEGIN IMMEDIATE` compares the complete head and registry version, then invokes the pure reducer and current intent allocation gate. |
| Maintenance | Publish the physical-node intent and retained operation with the head. Keep the original request separately from later deadline and boot changes. Earlier cordons survive later operations. |
| Enrollment | Check exact source/target intent rows and publish Pending in one transaction. Full input comparison precedes current intent checks on replay. Pending remains an obligation after expiry. |
| Boot confirmation | Read current physical intent and the exact Established node enrollment in one transaction; reject pending/settled/foreign records. No cached older intent can open readiness. |
| Action acceptance | Check current head, permit, endpoint and intents before inserting the immutable acceptance. Key includes action digest, physical node and boot; source and receiver inspections remain distinct. |
| Unaccepted action | `ResolveUnaccepted` checks that the exact attempt/effect/endpoint has no acceptance in the same `BEGIN IMMEDIATE` transaction that advances the head revision. A delayed old envelope then fails; an acceptance that won the race prevents retry. Both permits remain charged. |
| Results and recovery | Retain original acquisition/recovery inputs and positions. Require matching basis/evidence before publishing successful activation or recovery. Unknown may advance to checked completion; terminal results are immutable. |
| Fresh inspection | Check the request's exact head action, registry version and endpoint intent in one read transaction. Create no effect acceptance or cached observation result. The public host path performs the actual current check. |
| History | Insert the exact progress page in the same transaction that retires permits. Loading verifies the page digest and head reference. |
| Durability | SQLite WAL with `synchronous=FULL`, foreign keys and a five-second busy timeout. Each client owns one connection; independently opened clients serialize through SQLite. |
| Bounds | At most 32 accepted blocking jobs per client. Excess calls return Capacity. Pages contain at most 128 sorted records; canonical codecs bound record sizes. |
| Cancellation and close | An accepted native transaction survives a dropped waiter. Close stops admission, joins all accepted jobs, releases their slots and closes SQLite. Backend and codec errors retain their sources. |

Applications must retry an ambiguous commit with the same immutable request
identity and reread retained state. A timeout cannot free a permit or turn an
unknown enrollment into a refusal.

## Evidence and integration work

```sh
cargo test -p cellule-host --example fleet_operations --locked
cargo clippy -p cellule-host --example fleet_operations --tests --locked -- -D warnings
```

The focused tests exercise independent connections, competing controller and
allocation CAS, lost replies after commit, rollback before commit, reopening,
immutable action inputs/results, retained cordons/enrollments, malformed head
and missing operation references, bounded admission, and drain after waiter
cancellation. Recovery fixtures use canonical failed-session proof and synthetic
record positions; actual Cell restoration is covered separately by runtime and
host integration tests. These journal tests do not activate Cell actors.

SQLite reconstruction here establishes the local adapter behavior. Process
crash, filesystem faults, distributed journal providers and mixed deployed
readers require the plan's qualification campaign. The schema has no migration
contract yet; format mismatch fails closed.

The example test target also invokes the exported `FleetReconciler` with this
SQLite adapter and signed synthetic observations. Its simulated transport checks
phase publication before dispatch, fresh activation/retirement, independent
cleanup, retained permits after lost replies/timeouts, competing controllers,
stop-new-moves behavior, and pressure relief using remaining shared budget.
The separate `scenario::tests` cases invoke the same real-node implementations
as the `overload` and `controller-restart` commands. The model tests do not create Cell actors. Planned
maintenance and receiver-loss scenarios still require their complete barriers;
controller replacement here retains all three live node sessions.

The host now owns each whole closing attempt after a shutdown waiter disappears.
It retains the same lane through facility/runtime/lease cleanup and exposes local
`drain_observation()` results with original failure history. Successful stop
joins the original task; retrying an incomplete deadline resumes the same native
resource owners. Each confirmed example boot now installs its original directory
version and SQLite enrollment row into that owner before readiness. Native
shutdown checks canonical withdrawal and committed boot retirement before
Stopped; a lost retirement reply retains Draining until an exact replay confirms
the original evidence. Partial startup still uses the retained application's
boot cleanup. These local observations supply no relocation or redundancy proof
and do not enable the still-refused fleet Finalize action.
Publish native rotation/reader proofs before starting terminal shutdown: weak
native handles can disappear when an autonomous successful stop clears owners.

Public startup cases cover missing/Pending/foreign records, lost acceptance and
Established replies, a cordon racing accepted enrollment, a drained-mode reboot
with management available, delayed stale confirmation, shutdown racing a read,
original backend errors, and canonical boot withdrawal/retirement replay. Native
closing cases cover sole-waiter cancellation, an ambiguous committed retirement,
a retirement deadline, a late signed heartbeat, missing canonical storage and
failed role closure without invented retirement or Stopped. They
use real local runtimes/directory/SQLite with explicit reply faults. They do not
cover process crashes, a distributed journal or a complete role registry.

The large reader pagination case opens 128 native views and checks all restored
values and joined ledgers. Its explicit fixture budgets are 128 slots, 256 MiB
of retained credit and four GiB of native memory credit. It holds 127 views
through at least 1,500 ms of real pressure samples and requires Normal admission
before the final Pending request. This avoids depending on a fast opening loop
to beat the classifier's 1,000-ms dwell. It renews both original signed boot
advertisements through directory CAS while opening and reading; its receiver
guard advances only after confirmation. Ordinary memory budgets and production
pressure thresholds and qualification profiles are unchanged.

The public managed follower scenarios use two native `FollowerStore` instances,
signed directory authorization/CAS, and this same SQLite journal. They check all
members Pending before enrollment; partial acceptance and receiver cordon; lost
acceptance, establishment, close and retirement replies; shutdown deadlines and
cancelled waiters; cold retirement and actual acknowledged SQL command/root
readback; and invalid shipper limits before acceptance. They exercise
`CellNode::install_fleet_node_durability_provider` through the existing supervisor.
The fixture provider prepares signed immutable inputs only; its authority
fresh-loads exact scope and retains its checked close receipt across lost replies.
These in-process scenarios do not establish complete role coverage or sustained
replacement policy for role-enabled deployments. The executable's count
collector supports only its declared writer profile.

`FleetEnrollmentJournal::refuse_unexecuted_enrollment` is one SQLite transaction:
validate complete inputs, refuse an exact Pending row or create an exact terminal
exclusion when absent, and advance RegistryVersion only on a real change. The
retained finite owner must prove native work never started (or retain the checked
original-token exclusion proof). Reader removal and follower refusal use it to
fence delayed acceptance; neither treats an absence read as settlement. Independent
clients exercise lost commit replies, reconstruction, and acceptance/refusal races.

The journal's cooldown and post-batch queries compare the complete expected
snapshot and walk only its committed progress chain, one bounded page at a time.
Cancellation history and orphan pages cannot establish movement time. This
reference traversal is not a measured large-history performance result; a
production index must update atomically with the same retirement transaction.

Fresh inspection records bind a nonce and original capture interval. Use the
host's `inspect_fleet_action` and validate its response against the complete
request and a finite age bound. Legacy retained Inspect results are historical.
The journal authorizes a capture; it does not supply current actor evidence.

Before enabling fleet execution, the embedding application must provide the
controlled bootstrap/import barrier, wire every enrollment producer, authenticate
management requests, retain canonical enrollment proofs, produce complete fresh
observations, and supervise the public reconciliation driver. The reference
adapter does not infer these facts from an empty directory or a bootstrap flag.
See the [implementation evidence](../../../../docs/fleet-operations-progress.md)
for exact source fingerprints and remaining work.

The synthetic controller deadline cases use Tokio's test clock after fixture
setup to inject timeout at an observed endpoint boundary. They separately prove
retained permits before acceptance and after durable acceptance without a
result. The two-endpoint fault cases keep their 150 ms pass budget. The
single-cordon and unavailable-first-endpoint cases keep their 100 ms and 300 ms
budgets, advancing only after the original first acceptance. Journal rereads and
the healthy sibling finish on the paused clock without a fabricated second
fault. Original Elapsed sources and all phase/permit assertions remain required.
These cases measure protocol behavior; native/process latency qualification
uses its real clocks and committed profiles.

## Managed reader producer evidence

The example's test target also exercises `install_fleet_reader_enrollment` with
real admitted SQLite readers and this journal. It checks Pending before native
opening, caller cancellation, lost acceptance/publication replies, joined
retirement, cancelled removal, a native VFS stall across a host deadline,
independent journal reconstruction and startup binding requirements. A typed
counter query checks the exact reader receipt. The installed periodic loop
also repairs original lost replies and cancelled removals without another hint
or shutdown, retains a still-owned opening, and retries temporary inventory
credit refusal. A fenced-node case confirms that the original never-started
request can receive its nonexecution exclusion while new activation and native
inventory remain fenced. A public host case drives reconciliation before lease
installation and confirms healthy startup and joined shutdown. These fixtures do not
establish complete observer coverage or replacement redundancy for the movement
commands.

```sh
cargo test -p cellule-host --example fleet_operations --all-features --locked
```

The long-partition pagination case retains 128 source Cells and 128 native reader
views in one process. Provision OS descriptor headroom for both collections.
Colima's default 1,024 soft limit fails during source bootstrap in both the
parent and current code. The controlled Linux run passed with the existing
524,288 hard limit exposed as the soft limit, preserving two CPUs, four GiB,
all 110 cases and their original assertions. In a dedicated verification
container, inspect and raise the soft limit before invoking the target:

```sh
ulimit -Sn
ulimit -Hn
ulimit -Sn "$(ulimit -Hn)"
cargo test -p cellule-host --example fleet_operations --all-features --locked
```
