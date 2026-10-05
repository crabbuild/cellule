# Fleet operations implementation evidence

The [implementation plan](fleet-operations-plan.md) remains the full scope.
This page records focused checkpoints; it does not establish complete fleet
balancing, maintenance, or deployment qualification.

## October 4 2026 managed-reader maintenance checkpoint

The reference observer now accepts the full enrollment roster, including
non-node roles, and retains the installed native role graph instead of rejecting
readers or follower logs. It collects current reader and follower evacuation
evidence separately from the maintenance replacement-policy check, so missing
policy proof remains a blocker.

A new minion end-to-end scenario starts with a managed reader on the maintenance
node, activates a replacement on another managed node, publishes and verifies
the durable evacuation policy, then drives SettleRoles and Finalize through the
public reconciler and native node path. It confirms the source boot is stopped,
withdrawn and retired, the old reader no longer serves, and the replacement still
reads the expected value. The focused case passes; the full minion scenario
suite passes (343 passed, 0 failed).

The same reference observer now handles the four-boot follower fixture. Its
global role coverage check validates an Established replacement lane that has
not appended yet against the original producer and physical follower references;
the generic per-node enrollment check alone rejected that valid empty lane. A
new live-follower-only minion scenario verifies the two replacement members,
reconciles SettleRoles and Finalize, confirms the donor boot is stopped,
withdrawn and retired, and checks the new epoch's exact membership. Its focused
case and formatting pass. At this checkpoint, dead-owner process closure was
still open; the next checkpoint records that path.

## October 4 2026 failed-owner maintenance checkpoint

The reconciler now handles a maintenance target whose original boot has already
been retired. `FleetRoleSettlement` binds settlement to the exact failed-boot
closure, canonical fence, full journal head and registry. A dedicated transport
path accepts and publishes the exact `RolesSettledAt` result without sending
SettleRoles to the absent endpoint. Closing uses a matching closed-boot Finalize
path; its default transport fails closed, while the reference SQLite adapter
publishes Stopped only from the current retirement closure and exact maintenance
evidence. Matching durable results are adopted idempotently.

The new end-to-end case captures the failed process and recovered-follower
closures, collects the original maintenance enrollment set and rechecks native
role inventories plus physical follower references. It reconciles through
Closing to Completed with an empty node endpoint list, proving the retired
session is never contacted. The fixture uses a joined process-lifetime test
stand-in. It also drops the Finalize reply after the exact `Stopped` result is
durably published, waits for the real 2.5-second journal lease to expire, then
starts a different controller session. The replacement replays the same
accepted action from the retained SQLite result and completes the operation;
provider and production process qualification remain open.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations --locked failed_owner_maintenance_settles_and_finalizes_only_from_fresh_process_closure` | 1 passed. |
| `cargo test -p cellule-host --example fleet_operations --locked` | 344 passed; 0 failed; 68.26 seconds. |
| `cargo check --workspace --all-targets --all-features --locked` | Passed. |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Passed. |
| `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked` | Passed. |
| Format, boundary, module-layout, Rust-fence, Markdown-link, and SQL/peer-contract checks | Passed; 137 Rust snippets, 1307 local links, 28 protocol assertions and 570 validator links. |

After adding the lost-Finalize/controller-restart assertion, the focused case,
all 344 minion cases, formatting, and `git diff --check` passed again. The
workspace-wide checks above predate this scenario-only change.

GitHub reports PR #37 merged and PR #56 clean, mergeable, with all listed
checks passing. The three newly reported conflict files are unchanged in this
checkout and contain no conflict markers. These local changes remain uncommitted
and are not included in PR #56.

### Highest remaining work

1. Qualify failed-owner and live role maintenance under cancellation, provider
   failure, controller restart and real process loss; add missing role/fault
   combinations.
2. Complete receiver-session loss/recovery/adoption and add the canonical
   receiver-loss executable scenario.
3. Finish W9 physical process/provider faults, mixed-version and load/soak
   qualification, then exercise W10 runbooks and staged rollout/rollback.
4. Re-run the complete qualified source and hosted CI on the eventual PR head.

## October 4 2026 source reader policy checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`. The full W1–W10 plan remains active.

`FleetReaderEvacuationVerifier::collect_source_readers` now enumerates the exact
original/current request set and composes retained native retirement with the
actual managed writer successor, canonical final-root derivation/current origin,
current reader policy and actual ready replacement requests. It uses the same
managed-writer validator as original failed-boot writers and the same current
reader confirmation loop as donor history. Whole-capture clocks/deadlines and
full roster/head checks bound the collection. Provider absence remains unknown;
changed allocations/presence and original backend errors refuse the capture.

`FleetObservation::with_source_reader_policies` retains those native capsules and
checks their original capture, full barrier, signed successor/replacement boots
and captured writer rows. Planner identity v12 binds the new inputs. The complete
maintenance matcher marks only the exact checked source reader `SourceReader`;
omitted evidence remains `SourceSuccessor`. Attachment preserves incomplete
observation and grants no SettleRoles/Finalize rights. Persisted enrollment,
evacuation and signed peer formats are unchanged.

Eleven real minion cases pass on the isolated source snapshot. They refresh past
the original opening, drain the original writer, restore on another managed boot,
join the exact original reader and install a new request on the same Active
receiver. They cover missing/changed evidence, foreign physical/origin mappings,
clock bounds, cancellation, deadline, source error preservation, policy/head/
writer changes behind actual signed native replies, duplicate/late attachment,
outer boot/native-row mismatch and independent SQLite client/public reconciliation.
Every successful fixture joins all native owners and checks released ledgers.
An initial fixture activation used the departed writer's signer and was correctly
refused; the fixture now authenticates the actual successor boot through the same
signature/principal path. Initial compile/setup failures are retained as diagnostics.

All 13 fresh Rust commands pass on the frozen Rust 1.99 source: 1,711 framework
passes with 37 documented ignores, all 331 minion cases, local LTX 54, Axum without
default features 16 and cookbook 256 plus check, Clippy, docs and binary builds.
The framework/minion total is 2,042 distinct passes; focused repeats and overlapping
local LTX are excluded. Workspace features/targets, Clippy, warning-denied API docs
and the minion build pass. Nine static gates pass; all 137 Rust snippets parse and
1,307 local links resolve. All 1,133 Rust/Cargo paths match manifest SHA256
`2e382bfbcbe4682a383628848c3187d6c06007e8d0e89661ef488785d17760a5`.
Exact source/manifests, commands/results and initial failures are retained under
`/Users/haipingfu/Workspace/crabbuild-target/cellule-source-reader-9110095/evidence`.
A one-variable negative control verifies the opening root instead of the final
joined root. The exact-prefix regression fails as intended, distinguishing both
digests, transaction positions and sequences. The production source is restored
byte-identically; initial control-root/LTX-root type failure is retained and excluded
from regression evidence. The restored source also passed broad qualification.

PR #37 was merged as `fa548bb` while qualification ran. Its Git tree is byte-identical
to qualified baseline `9110095`; the native source reader increment continues on
`codex/native-source-reader-policy` from that new main. Full follow-up head CI,
physical process/provider retention and the complete W1–W10 scope remain required.

The new [source reader recipe](../crates/cellule-host/docs/source-readers.md) documents
provider ownership, collection/attachment and the runnable cases.

### Failed receiver source succession continuation

`FleetSourceReaderRetirement` now accepts either the retained native reader
join or a `FleetFailedReaderClosure`. The collector checks the failed proof's
original reader, process request and current boot row, then verifies the original
opening root through the actual writer successor. Current replacement policy
still applies: desired count 1 with no available replacement blocks collection;
an explicit update to zero allows it. The source-policy subdigest moved to v2,
and planner input identity moved to v13 to bind the new proof variant.

All 12 source-reader minion cases pass, including the new failed-receiver case;
all 10 failed-reader lifetime cases pass. The host crate passes all-target,
all-feature `cargo check`, and formatting passes. The fixtures prove the API
composition using process evidence tied to a stopped native reader; production
OS termination and external accepted-work providers remain unqualified. The
follow-up is published on PR #56; hosted qualification is pending.

### Highest remaining work

1. Qualify the follow-up head in CI and both routing campaigns.
   Earlier follower-observation and overload failures still need independent
   diagnosis if reproduced; a later green run alone does not establish their cause.
2. Qualify production durable provider/process retention; complete failed-owner
   follower succession, full native/external accepted-work coverage and unknown
   outcomes.
3. Implement SettleRoles/Finalize through original action joining, native Stopped,
   withdrawal, retired boots and committed completion; finish Cron/Blob owners
   and primitive fault matrices.
4. Complete receiver-session loss/recovery/adoption and sustained convergence;
   deliver canonical minion maintenance and receiver-loss commands.
5. Complete W9 physical process/provider faults, mixed binaries and load/soak;
   W10 exercised runbooks and staged rollout/rollback.

## October 4 2026 replica retry routing checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`, as confirmed by the user. The full W1–W10 scope remains active.

Current-head `f0a15a6` Compose smoke fails the existing balance assertion after
12 successful replica reads. The original hosted artifact and failed log are
retained under
`/Users/haipingfu/Workspace/crabbuild-target/cellule-smoke-f0a15a6/evidence`.
A new three-process public-host regression injects one initial replica refusal
and reproduces a 7/5 split with the original routing cursor. The fallback uses
an index from the shorter retry list to advance the next full-list query.

Routing now retains each candidate's original placement position across retries;
removal still uses the current list index. Outstanding load remains the first
selection criterion. The four focused routing cases pass, including wraparound,
overlapping candidate sets, cancellation and deadlines. The corrected native
three-process regression passes with a 6/6 split, receipt readback, automatic
reader recruitment/refresh, eviction and all three joined hosts. CI explicitly
selects this regression alongside the ordinary process smoke and checks that
the exact selector exists. Qualification profiles, assertions and deadlines are
unchanged. This reproduces and fixes the retry pattern; the original hosted
artifact lacks an attempt trace, so it does not independently establish which
transient refusal occurred there. Future balance failures retain concise attempt
outcomes.

The original unmodified native process run also passes; this does not by itself
resolve the intermittent hosted failure. A preliminary run that began before
the fixed build completed is retained and excluded from qualification. Fresh
pre-merge restored-source qualification passes all 13 Rust commands and nine
static gates: 1,683 framework passes with 37 documented ignores, all 320 minion
cases, local LTX 54, Axum without default features 16, and cookbook 256 plus
check, Clippy, docs and binary builds. The framework/minion total is 2,003 distinct
passes; focused and process repeats are excluded. All 136 Rust snippets parse
and 1,280 local links resolve. Exact source, command/results, manifests and
original failures are retained in that mounted evidence directory.

Parent `f0a15a6` also fails a separate minion follower-observation test with
`fleet roster collection deadline elapsed`; its original log is retained and its
cause remains unresolved despite that case passing locally. Its leased
forwarded-command throughput at concurrency one fails at 88.40% of baseline
against the unchanged 90% gate; latency gates pass. Object-only routing passes,
with its minimum gated throughput at 94.21%. The unchanged comparator reproduces
both original 178-file campaigns, whose binaries, raw rows and hashes are
retained. The routing aggregate fails. These results do not qualify a later head.

Merge commit `85f731e` integrates `main` at `4c1c982` and resolves both LTX document
conflicts and the actor admission conflict. Fleet admission kinds coexist with
main's optional request permits; the docs retain both prepared disk/origin
contracts and compaction/cohort behavior. The merged tree passes all-target,
all-feature workspace check, boundary/layout and documentation gates. The routing
correction also passes both ordinary and one-refusal native three-process
scenarios against this merged actor, with the same 6/6 read distribution,
receipt checks and joined hosts. Full merged-head CI qualification remains
required; the earlier broad pass qualifies only its frozen pre-merge source.

### Highest remaining work

1. Finish restored-source and current-head CI qualification. Diagnose the
   follower-observation deadline, ordinary routing throughput and original
   overload endpoint failure without weakening gates or deadlines.
2. Compose exact native reader joining/final roots with fresh source/failed-owner
   successor policy, full original/current requests and durable process/provider
   retention. Complete follower succession and native/external accepted-work
   coverage.
3. Implement SettleRoles/Finalize through original action joining, native Stopped,
   withdrawal, boot retirement and committed completion. Finish Cron/Blob owners
   and primitive fault matrices.
4. Complete receiver-session loss/recovery/adoption and sustained convergence;
   deliver canonical minion maintenance and receiver-loss commands.
5. Complete W9 physical process/provider faults, mixed binaries and load/soak
   campaigns; W10 exercised runbooks and staged rollout/rollback.

SettleRoles/Finalize remain refused; this routing correction does not complete
the fleet implementation plan.

## October 3 2026 final reader root identity checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`. The full W1–W10 scope remains unfinished.

Source succession needs the exact last installed reader prefix. The native
shared reader state now retains its immutable Cell/incarnation-scoped root
through snapshot detachment, deriving receipts from that same root. Refresh
installs root and view under the existing lock only after admission and authority
checks; failed, cancelled or uninstalled refreshes cannot substitute a published
source root. The lifecycle observation and exact retirement capsule expose this
historical root without adding work, a retention pin or authority rights.

Native reader inventory fingerprint v2 binds every root field, including digest,
transaction position and commit sequence. This changes only ephemeral scan
identity; prior continuations fail their normal topology check. Persisted
enrollment/evidence bytes and signed peer contracts are unchanged.

A new real native case checks old publication versus installed state, failed and
successful refresh, peer clones, detachment, original ledger release and shutdown.
Existing cancelled-query/close cases also retain the same final root. A new
fingerprint regression distinguishes every root field, including a substituted
digest at identical counters. The real host handoff case refreshes after its
original opening, moves the writer, joins the exact original reader and verifies
the final root through the successor's canonical lineage/origin path. A forged
digest with unchanged scope and counters refuses. SQL readback remains intact.

### Verification and remaining work

All three focused commands pass: six native runtime closure cases, the exact-root
fingerprint regression and all nine exact-removal cases. Two isolated one-variable
negative controls fail the intended regression: keeping the previous installed
root after refresh, and omitting the root digest from the native inventory hash.
Both production files are restored byte-identically before broad qualification.
Original negative sources and failed logs are retained.

Preliminary failures correct a test-only LTX position construction, an invalid
idle-transfer assumption immediately after the new command, and the fixture's
insufficient retained-byte capacity for the newly exercised canonical origin
verifier. The exact busy handle now joins/releases through ordinary drain, and
the receiver is provisioned for the verifier's existing metadata envelope. Idle
graces, production admission, qualification profiles and evidence remain unchanged.
All original failed logs and source snapshots are retained.

All 13 fresh Rust commands and nine static gates pass on the isolated Rust 1.99
snapshot under
`/Users/haipingfu/Workspace/crabbuild-target/cellule-reader-final-root-bd381d6`.
The framework passes 1,682 tests with zero failures and 36 documented ignores;
minion passes all 320 cases. These are 2,002 distinct passes, excluding focused
repeats, negative controls and overlapping local LTX. Local LTX passes 54 tests,
Axum without default features passes 16, and the fresh cookbook passes all 256
tests plus check, Clippy, warning-denied docs and binary builds. Workspace
features/targets, Clippy, warning-denied API docs and the minion build pass.
All 136 Rust snippets parse and 1,280 local links resolve. All 1,117 Rust/Cargo
paths match manifest SHA256
`42c25e30a926cabb324f48d3f9ea632b1ce47ffb1ab9418e634843b6949dd104`.
Exact source archives, manifests, commands, preliminary failures, negative
controls and restored qualification logs are retained in its `evidence` directory.

Parent `bd381d6` remains MERGEABLE. All hosted checks are terminal: 32 successes,
two failures and three documented skips. Workspace, follower/object capacity,
MSRV, contracts, cookbook quality/scenarios, Compose smoke, model, website and
fuzz checks pass. Leased routing passes, with its lowest gated throughput at
92.68% of baseline. Object-only forwarded-command and local-command throughput
at concurrency 16 fail at 89.83% and 88.60% against the unchanged 90% gate;
latency gates pass. The routing aggregate also fails. The unchanged comparator
reproduces both original 178-file campaigns. Original binaries, manifests, stage
rows, file hashes and the failed job log are retained. Baseline and candidate
binaries are byte-identical to their respective earlier campaigns, whose failing
lanes differed; this does not independently isolate publication cost, provider/
runner variation or execution order. These parent results do not qualify this
new source or establish an independent performance correction. No gate, expected
evidence, profile, production admission or deadline is weakened.

The next delivery is source/failed-owner reader and follower succession policy:
bind exact native joining and final-root derivation to fresh current replacement
policy, durable provider/process retention and the complete original/current
request set. `SourceSuccessor` remains blocking until that composition is checked.
Then complete SettleRoles/Finalize and accepted native/external work, including
Cron/Blob owners; receiver-session recovery/adoption and maintenance/receiver-loss
commands; W9 process/provider/mixed-binary/load campaigns; and W10 exercised
runbooks and rollout/rollback. Ordinary routing throughput and the original overload
failure still require independent diagnosis and current-head CI qualification.

## October 3 2026 exact original reader joining checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`.

`ReadReplicaManager::remove_enrolled(original, deadline)` now binds native removal
to the exact Established request under the existing activation lane. Both
physical endpoints, acceptance and canonical native opening evidence must match
the retained producer. The current journal row is checked before closure. A
delayed source-maintenance request cannot remove a newer reader for the same Cell.
Ordinary removal and exact removal share the same native join and producer
retirement implementation; no task, scheduler, authority or admission lane is added.
The producer returns the actual confirmed terminal response before discarding its
local record. Existing evidence bytes and persisted codecs are unchanged.

The local `ReaderEnrollmentRetirement` retains original request/source, final
joined prefix, confirmed terminal row and a bounded original interval. Dynamic
copies remain charged to the existing metadata ledger until dropped. Unknown or
absent native ownership supplies no proof. Lost publication replies preserve their
source errors and original closed view/event for same-request retry. Deadline or
cancelled publication waiters cannot reopen the original view. A terminal row alone
cannot recapture native joining after its owner was discarded.

Nine new cases exercise original peer clones, a real writer release/restoration
with retained SQL readback, a stale request after a newer reader opens for the
same Cell, substituted opening evidence, foreign receiver boot, unknown acceptance,
lost retirement replies, cancelled publication and deadlines. An Active receiver
can join the old source request after actual handoff. Every successful fixture
joins its native and backend owners and checks released ledgers. Bypassing only the
original native input-binding call in the isolated snapshot causes the stale
request regression to fail because the newer role is closed. The original binding
is restored byte-identically; negative source and failed logs are retained.

The first broad run passes the framework but fails one existing overload case:
318 of 319 minion cases pass; the final movement loop reports only
"real movement endpoint failed". The minion now retains every affected attempt
and original endpoint error through cleanup, with a regression for boxed source
identity and sibling errors. Ten unchanged isolated overload runs pass after
this diagnostic correction; they do not reproduce or fix the original failure.
The original failed run and preliminary diagnostic compile repair are retained.
The final full minion run passes all 320 cases, including overload, with its
ordinary two-case scheduling bound. Independent diagnosis of the original
overload failure remains required; the diagnostic correction preserves its next
failure's evidence rather than establishing a cause or performance fix.

This is a source-succession prerequisite. Current reader replacement policy,
original writer lineage, accepted producer/external work, full roster and durable
process/provider retention remain separate requirements. Applications authenticate
both endpoints and establish successor/current policy before source-maintenance
removal. This local result grants no SettleRoles/Finalize or complete-observation
upgrade. Failed receivers still require the independent original process path.

### Verification and CI

All 13 fresh Rust commands and nine static gates pass against the restored
isolated source under
`/Users/haipingfu/Workspace/crabbuild-target/cellule-exact-reader-removal-2a3726b`.
The fresh full framework pass has 1,680 tests, zero failures and 36 documented
ignores; minion passes all 320 cases. These are 2,000 distinct passes, excluding
focused repeats, ten diagnostic overload runs and overlapping local LTX.
Local LTX passes 54 tests, Axum without default features passes 16, and the fresh
cookbook passes all 256 tests plus check, Clippy, warning-denied docs and binary
builds. Workspace features/targets, Clippy, warning-denied API docs and the normal
minion build pass. All 136 Rust snippets parse and 1,280 local links resolve.
All 1,116 Rust/Cargo paths match manifest SHA256
`da50d5e485b377d4040d1ab28dffabb89e3bf81c78a89e72a4ec8cac232ca8ff`.
Frozen source, manifests, commands, original failed broad run, diagnostic repeats,
preliminary repairs, negative regression and restored qualification logs are
retained in that mounted directory's `evidence` subdirectory.

Published parent `2a3726b` is MERGEABLE. Its hosted workspace, follower/object
capacity, MSRV, contract, cookbook quality/scenarios, Compose smoke, model,
website and fuzz checks pass. All checks are terminal: 32 successes, two failures
and three documented skips. Leased local-command throughput fails at 89.38% of
baseline against the unchanged 90% gate; latency gates pass. The routing aggregate
also fails. Object-only routing passes, with its lowest gated throughput at
92.29%. Replaying both original 178-file campaigns through the unchanged
comparator reproduces their published results. All original binaries, manifests,
stage rows, hashes and failed job logs are retained. Baseline and candidate
binaries match their respective earlier campaigns byte-identically. This
establishes the hosted minion scheduling
control's workspace pass; it does not establish an independent performance
correction or qualify this new source. No profile, gate, deadline or expected
evidence is weakened.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Complete current-head CI, including ordinary leased and object-only routing and their aggregate. Independently diagnose/correct leased throughput and the original overload endpoint failure; retain failed evidence. |
| 1 | Compose exact installed reader joining with fresh source/failed-owner successor policy and durable process/provider retention. Complete follower succession, durable unknown actions and native/external accepted-work coverage. |
| 2 | Implement SettleRoles/Finalize with original action joining before terminal drain handoff, native Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider faults, mixed binaries and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The full W1–W10 plan remains active and unfinished. SettleRoles/Finalize remain refused.

## October 3 2026 original nonexecution confirmation checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`, as confirmed by the user.

`FleetMaintenanceNonexecution::collect` now confirms independently retained
original nonexecution through the application's read-only authenticated provider.
The same complete original/current request set selects eligible terminal rows;
Pending and installed roles cannot be certified. Exact original acceptances,
terminal rows and settlement witnesses bind stable request identities. Every
provider result is checked twice, including absence, and the complete journal
barrier is reconfirmed. Whole-capture clocks and deadlines refuse regression,
expiry and changed inputs. Source errors stay retained; missing evidence stays
unknown. The provider must confirm earlier native/external work joining and that
the role effect never committed, then exclude delayed execution of that request.
A terminal row or evidence constructor supplies no such independent proof.

Public observation attachment retains the original full head, roster, capture
digest and interval. Matching exposes checked Nonexecution separately, including
source-side requests; installed source/failed-owner policy remains blocking.
Changing checks after matching, duplicate attachment and older-head restamping
refuse. Planner digest v11 binds all original provider inputs and results;
persisted codecs are unchanged. This cannot upgrade complete observation, join
other accepted work or grant SettleRoles/Finalize rights.

Twelve new cases include actual native reader admission refusal followed by
original producer joining, and actual follower exclusion after a lost member
acceptance reply. Retained witnesses are synchronized and consumed after provider
and SQLite-client reconstruction. The reader case covers both physical endpoints;
the follower case verifies that no native epoch or follower RPC started and delayed
acceptance replays the exclusions. Other cases cover missing/substituted/foreign
evidence, second-read errors, changed proof presence, head changes with unchanged
registry, original digest stability, bad clocks/deadlines, cancelled read-only
confirmation, duplicate/late attachment, installed roles and new unknown source
work. These demonstrate in-process joining; production provider/process
qualification remains required. Removing only the request-binding guard in the
isolated snapshot makes the foreign-original regression fail. The guard is restored.

### Verification and CI

All 13 fresh Rust commands and nine static gates pass on the isolated Rust 1.99
snapshot `/tmp/cellule-maintenance-nonexecution-1b65e0a`, using its mounted target.
The framework passes 1,679 tests with zero failures and 36 documented ignores;
minion passes all 310 cases. These are **1,989 distinct passes**, excluding focused
repeats and overlapping local LTX. Local LTX passes 54 tests, Axum without default
features passes 16, and the fresh cookbook passes 256 plus check, Clippy,
warning-denied docs and binary builds. All 135 Rust snippets parse and 1,280 local
links resolve. All 1,112 Rust/Cargo paths match manifest SHA256
`1b010d9a79510a0f09706d0761832e2c058fdc48f4c12f5a072c27ec5d7d9598`.
Frozen source, commands, preliminary repairs, original negative regression and
restored qualification logs are retained under
`/tmp/cellule-maintenance-nonexecution-evidence`.

Preliminary repairs correct a controller-lease field name, inspect the preserved
native refusal's source chain, isolate full-head rejection from the earlier
interval guard, and collapse two nested conditions flagged by Clippy. Original
failed logs are retained. No production error was flattened or assertion removed.

Published parent `1b65e0a` is MERGEABLE with 31 successful checks, three failed
checks and three documented skips. The workspace minion run passes 297 of 298
cases; follower policy observation exceeds the unchanged five-second roster
deadline. The hosted minion step now bounds simultaneous case fixtures to two,
retaining every case's internal concurrency, deadlines and assertions. Fresh
hosted verification remains required; this is a scheduling control, not proof
that the failure's cause is independently isolated.
The exact CI command with `--test-threads=2` also passes all 310 cases locally;
that repeat is excluded from the distinct pass count.

Object-only routing passes; its lowest gated throughput is 91.93% of baseline.
Leased forwarded-command throughput fails at 85.50%; latency gates pass. Leased
routing and its aggregate therefore fail. The previously failing leased
local-command lane passes at 101.09%. Both original 178-file campaigns, frozen
binaries/source, stage TSVs, job logs, manifests and hashes are retained. Replaying
all original measurements through the unchanged comparator reproduces both results.
Baseline and candidate binaries are byte-identical to their respective `0eabc9a`
and `8c49a81` campaigns. Candidate preparation means are higher in all four failing
forwarded-command pairs, while two pairs have higher candidate throughput and two
have substantially lower throughput. Publication cost, runner/service variation
and execution order remain hypotheses; no independent performance correction is
established. No profile, gate, expected evidence or deadline was weakened. The new
head requires fresh qualification.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Verify the hosted minion scheduling control; diagnose and fix ordinary leased routing throughput, then qualify the current head through both profiles and their aggregate. Keep all failed evidence. |
| 1 | Complete source/failed-owner successor policy for installed roles and production nonexecution evidence retention. Complete durable unknown actions and native/external accepted-work coverage. |
| 2 | Implement SettleRoles/Finalize with original action joining before terminal drain handoff, native Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider faults, mixed binaries and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The full W1–W10 plan remains active and unfinished. SettleRoles/Finalize remain refused.

## October 3 2026 automatic maintenance policy lookup checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`, as confirmed by the user.

The reader and follower verifiers now expose `collect_maintenance`. Each uses the
same complete original/current request set as policy matching, looks up every
eligible retired donor through the native journal's latest pointer, and rechecks
authenticated reader readiness or follower source/member graphs. Exact full head,
registry, roster, current rows and originally Established acceptance digests are
required. The entire lookup has one bounded monotonic capture interval and caller
deadline; the roster is reconfirmed before and after all reads, including missing
pointers. Missing history remains an explicit MissingPolicy gap. Source-side,
failed-owner and unknown-nonexecution obligations remain blocking.

Both actual-role minion observers consume automatic lookup and complete matching.
Nine new actual-role cases cover independent SQLite clients, missing publication,
an older head with an unchanged registry, regressing/expired clocks and deadlines,
head or redundancy-policy changes during native probes, and a new source request
after original capture. Tests retain actual ready readers and rotated follower
ensembles, join fixture resources and verify that observation does not mutate
authority or start effects. The damaged-reader-history case also refuses automatic
lookup before matching. Planner digest v10 and persisted codecs are unchanged.

### Verification and CI

All 13 fresh Rust commands and nine static gates pass on the isolated Rust 1.99
snapshot `/tmp/cellule-maintenance-policy-lookup-0eabc9a`, using its mounted target.
The framework passes 1,679 tests with zero failures and 36 documented ignores;
minion passes all 298 cases. These are **1,977 distinct passes**, excluding focused
repeats and overlapping local LTX. Local LTX passes 54 tests, Axum without default
features passes 16, and the fresh cookbook passes 256 plus check, Clippy,
warning-denied docs and binary builds. All 134 Rust snippets parse and 1,280 local
links resolve. All 1,107 Rust/Cargo paths match manifest SHA256
`3e940b30cadfcaf5d82d5dc9d24834746b680286aa5bcaab5700e0c962bf4e46`.
Frozen source, commands, the corrected preliminary unused-fixture warning,
qualification logs and original routing artifacts are retained under
`/tmp/cellule-maintenance-policy-lookup-evidence`.

Published parent `0eabc9a` is MERGEABLE with 32 successful checks, two failed
routing checks and three documented skips. Follower/object capacity, workspace,
MSRV, contract, cookbook quality/scenarios, Compose smoke, model, website and fuzz
pass. Object-only routing passes; its lowest gated throughput ratio is 91.64%.
Leased local-command throughput is 85.27% of baseline against the unchanged 90%
gate; its latency gates pass. Leased routing and the aggregate therefore fail.

Both original 178-file campaigns, frozen binaries/source, stage TSVs, job log,
manifests and hashes are retained. Replaying all original measurements through
the unchanged comparator reproduces both results. Baseline and candidate binaries
are byte-identical to their respective `8c49a81` campaigns, despite different
object-only and expired-query gate outcomes. This demonstrates run variation;
it does not remove the repeated leased local-command failure. Candidate
preparation means are higher in all four leased local-command pairs. Publication
cost, runner/service variation and execution order remain hypotheses; no
independent cause or correction is established. No gate, profile, expected
evidence or deadline was weakened. The new head requires fresh CI qualification.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Diagnose and fix leased routing throughput, then qualify the current head through both profiles and their aggregate. Keep all failed evidence. |
| 1 | Complete source/failed-owner successor policy and checked nonexecution for every enumerated request. Complete durable unknown actions and native/external accepted-work coverage. |
| 2 | Implement SettleRoles/Finalize with original action joining before terminal drain handoff, native Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider faults, mixed binaries and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The full W1–W10 plan remains active and unfinished. SettleRoles/Finalize remain refused.

## October 3 2026 maintenance policy matching checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`, as confirmed by the user.

`FleetObservation::check_maintenance_policies` now matches the complete immutable
original capture and every currently unresolved related reader/follower request
against all attached current native policy checks. Original terminal rows cannot
disappear behind an empty current unresolved set. New source requests after
capture stay required. Missing original history refuses matching; explicit zero
requires a committed empty original manifest and no current unresolved requests.

Every checked policy must name the exact full current row. An originally
Established donor also requires its exact original acceptance digest; a
self-consistent substituted historical digest is refused. Originally Pending
acceptance remains retained separately when current progress advances. Full head,
registry, roster and original fresh intervals are compared with other evidence.
Changing the policy collections after matching is refused. Planner digest v10
binds the canonical coverage and source inputs; persisted codecs are unchanged.

Each obligation exposes checked Reader/Follower policy or an explicit Pending,
Established, MissingPolicy, SourceSuccessor or UnprovenNonexecution gap. The public
reconciler reports advisory counts with their own full head revision, registry
and interval; later allocations cannot restamp them to a newer report snapshot.
Absent coverage remains unknown. Request matching never upgrades complete node
observation, joins accepted work or grants SettleRoles/Finalize rights.

Four new journal cases cover absent history/explicit zero, retained terminal
originals, source and unknown-nonexecution gaps, new Pending/Established source
requests, duplicate matching, changed evidence inputs and an older full head
with an unchanged registry. Two new actual-reader cases expose a supplied subset
and reject substituted original acceptance history. Existing public reader and
follower cases consume coverage and its counts, retaining actual ready readers
and rotated native ensembles in both attachment orders. Incomplete observations
still start no effects. Removing only the original-digest guard in the isolated
snapshot makes the damaged-history regression fail; the qualified guard is restored.

### Verification and CI

All 13 fresh Rust commands and nine static gates pass on the isolated Rust 1.99
snapshot `/tmp/cellule-maintenance-policies-8c49a81`, using its mounted target.
The framework passes 1,679 tests with zero failures and 36 documented ignores;
minion passes all 289 cases. These are **1,968 distinct passes**, excluding focused
repeats and overlapping local LTX. Local LTX passes 54 tests, Axum without default
features passes 16, and the fresh cookbook passes 256 plus check, Clippy,
warning-denied docs and binary builds. All 133 Rust snippets parse and 1,279 local
links resolve. All 1,102 Rust/Cargo paths match manifest SHA256
`d908574b3211444144f0e5d0e0b0ece8596467a54d53443499668a2038420d3d`.
Frozen source, commands, preliminary compile repairs, original negative regression,
restored verification and original routing artifacts are retained under
`/tmp/cellule-maintenance-policies-evidence`.

Published parent `8c49a81` is MERGEABLE and has 31 successful checks, three failed
routing checks and three documented skips. Follower/object capacity, workspace,
MSRV, contract, cookbook quality/scenarios, Compose smoke, model, website and fuzz
pass. Both ordinary routing profiles fail, so their aggregate fails. Leased
local-command throughput is 88.52% of baseline; leased expired-query p99 is
2.03 times baseline. Object-only forwarded-command throughput is 87.41%.

Both original 178-file campaigns, frozen binaries/source, stage TSVs, job log,
manifests and hashes are retained. Replaying every original measurement through
the unchanged comparator reproduces all three gates. Candidate preparation means
are higher in all four failing command pairs, with varying margins. The leased
expired-query candidate p99 is also higher in all four pairs. Publication cost,
runner/service variation and execution order remain hypotheses; no independent
cause is established. The earlier same-source control and passing `617819a`
campaigns remain retained and do not qualify this failed head. No gate, profile,
expected evidence or deadline was weakened. The new head requires fresh CI.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Diagnose and fix ordinary routing qualification, then qualify the new head through both profiles and their aggregate. Keep all failed evidence. |
| 1 | Complete authenticated policy lookup, source/failed-owner successor policy and checked nonexecution for every enumerated request. Complete durable unknown actions and native/external accepted-work coverage. |
| 2 | Implement SettleRoles/Finalize with original action joining before terminal drain handoff, native Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider faults, mixed binaries and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The full W1–W10 plan remains active and unfinished. SettleRoles/Finalize remain refused.

## October 3 2026 original maintenance enrollment checkpoint

The canonical executable is `crates/cellule-host/minion`, Cargo target
`fleet_operations`, as confirmed by the user.

The first Cordoned-to-Evacuating CAS now commits a complete immutable manifest
and every page of original unresolved reader/follower acceptances. Both physical
endpoints and earlier boots are included; node boot closure remains separate.
The same SQLite transaction scans the entire retained enrollment table, validates
scope and canonical keys, and refuses over 10,000 rows. Each page contains at most
128 full original records. The manifest retains the pretransition full head,
bootstrapped registry, original Cordoned operation and capture time. Additive
codec tags and tables preserve all existing persisted record encodings.

Retirement, deadline extension and successor session adoption retain the first
set. Lost commit replies replay the same bytes. A failed phase CAS rolls back the
manifest and every page. A missing manifest or page stays unknown, including old
already Evacuating operations; phase replay cannot backfill zero from the current
roster. A committed empty manifest is an explicit historical statement. The minion initializes a one-way capture
anchor in the original BeginMaintenance transaction and binds it to the manifest
with BeginEvacuation. Missing history after boot adoption cannot become a new
first capture. Missing anchors, including operations predating this contract,
remain unknown and refuse capture; no current-roster migration is supplied.

`FleetMaintenanceEnrollments::collect` traverses every immutable page, matches
original acceptance times and establishment evidence against a fresh complete
roster, and reconfirms the exact full head and registry. The observer retains this
capture inside its outer interval. Attachment compares full snapshot and roster
with the existing policy, native graph, writer successor and failed-boot checks,
in either order. Planner digest v9 binds presence and the full retained digest.
The minion observer consumes it during Evacuating and Closing.

Four pure cases exercise canonical multipage history, missing/reordered/duplicate
pages, both physical endpoints, earlier boots, empty/invalid bases and bounds.
Seven journal cases exercise rollback, restart, lost replies, independent source
acceptance versus phase CAS, session/deadline changes and original corruption
errors. Public actual-reader cases preserve Established originals after Retired,
validate both attachment orders, refuse restamping/duplicates and reject an older
full head with an unchanged registry. Reconciliation remains incomplete and starts
no effects merely because original history or individual policy checks exist.
The isolated adoption/history-loss regression fails before the capture anchor:
the earlier implementation commits a new evacuation capture from retired rows.
The correction refuses both missing original history and missing anchors, without
advancing the phase or creating replacement rows.

### Verification and CI

All 13 fresh Rust commands and nine static gates pass on the isolated Rust 1.99
snapshot `/tmp/cellule-maintenance-enrollments-617819a`, using its mounted
checkout-specific target. All 132 documented Rust snippets parse and all 1,278
local Markdown links resolve. Qualification gates are unchanged.

| Verification | Result |
| --- | --- |
| Framework, all features, locked | 1,679 passed; zero failures; 36 documented ignored cases. |
| Canonical minion | 283 passed; zero failures or ignores. |
| Distinct framework/minion passes | 1,962; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | 54 passed. |
| Features/targets, Clippy and API docs | Pass; lint/docs deny warnings. |
| Normal executable and Axum without default features | Build and 16 tests/docs pass. |
| Fresh cookbook | 256 tests, check, Clippy, warning-denied docs and binary builds pass. |

All 1,099 Rust/Cargo paths match the qualified manifest, SHA256
`2f42dbb660a550003d9a9bb2f159a525fc217a333b853b69f38d50fdc00fa1ae`.
Frozen source, committed archive, commands, failed regression and repair logs are
retained in `/tmp/cellule-maintenance-enrollments-evidence`.

The first broad run failed two unchanged reader-discovery tests while cookbook
compilation ran concurrently: an empty cached selection and ten reads against
five expected reads. That original log is retained. The final full command runs
with suites sequenced, and both tests pass without assertion or cache-limit
changes. The adoption/history-loss regression and its original unguarded source
are retained separately; all seven corrected journal cases pass.

Published `617819a` passes follower/object capacity, workspace, MSRV, contract,
cookbook quality and all cookbook scenarios, Compose smoke, model, website and
fuzz checks. Both ordinary routing profiles and aggregate routing pass: the lowest gated
throughput ratios are 91.16% leased and 93.57% object-only. All 34 reported
checks pass, with three documented skips; PR merge state is CLEAN and MERGEABLE.
The original routing campaigns and hash manifests are retained. Parent `b1390b2`
passes leased routing but its object-only campaign fails concurrent local-query
throughput at 84.45% of baseline, below the unchanged 90% gate. Latency gates pass.
The original 178-file campaign, frozen binaries/source, stage TSVs, manifest and
job log are retained in the current evidence directory. The same-source control
previously passed both profiles; it does not qualify the baseline comparison or
isolate the cause of these failures. No qualification profile was weakened.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Qualify the new head, including both ordinary routing profiles; retain and diagnose failures without weakening gates. |
| 1 | Match every original and current reader/follower obligation to authenticated replacement policy or failed-owner successor evidence. Complete durable unknown action and native/external accepted-work coverage. |
| 2 | Implement SettleRoles/Finalize with original action joining before terminal drain handoff, native Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider faults, mixed binaries and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

New source-side replacements accepted after capture remain visible in the current
roster and native graph. This original set grants no aggregate settlement or
finalization rights. SettleRoles/Finalize remain refused; W1–W10 remains active.

## October 3 2026 source enrollment closure checkpoint

The canonical executable is `crates/cellule-host/minion`, Cargo target
`fleet_operations`, as confirmed by the user.

First reader/follower acceptance now checks the operation named by the source
intent in the same SQLite transaction as both endpoint intents and Pending
publication. The pure `EnrollmentRecord::pending` constructor requires this
retained operation for a Draining source and compares its ID, node, boot and
intent revision. Requested, Cordoned and Evacuating may recruit replacements;
Closing and Completed fence new requests even when the intent revision is
unchanged. Active sources and boot enrollment require no source operation input.
There is one canonical constructor; persisted records and codecs are unchanged.

Full original request replay precedes current intent/operation reads. Already
accepted requests retain their original timestamp and can still establish or
retire after Closing. Missing, mismatched or corrupt operation records refuse
new requests and retain original operation/codec errors. An older source operation
is checked after another node becomes the current maintenance target.

Two isolated regressions fail on published parent `b1390b2`: first acceptance
after Closing and delayed acceptance after an independent client commits Closing.
The corrected journal suite passes all 32 cases. New cases cover both reader and
follower roles, reconstruction, original completion/retirement, independent
acceptance versus Closing CAS, and missing/corrupt operation records without
registry changes or new rows. Pure cases cover all phases, foreign bindings,
missing/spurious inputs and exact successor boot adoption. Reducer drain fixtures
exercise journal ordering; they do not certify a real process as drained.

### Verification and CI

All 13 fresh Rust commands pass on the isolated Rust 1.99 snapshot with its
mounted checkout-specific target. All nine static gates pass; 132 documented Rust
snippets parse and 1,278 local links resolve. Qualification gates are unchanged.

| Verification | Result |
| --- | --- |
| Framework, all features, locked | 1,675 passed; zero failures; 36 documented ignored cases. |
| Canonical minion | 274 passed; zero failures or ignores. |
| Distinct framework/minion passes | 1,949; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | All 54 passed. |
| Features/targets, Clippy and API docs | Pass; lint/docs deny warnings. |
| Normal executable and Axum without default features | Build and tests/docs pass. |
| Fresh cookbook | 256 tests, check, Clippy, warning-denied docs and binary builds pass. |

All 1,091 Rust/Cargo paths match the qualified source manifest, SHA256
`71358161e10d7156f377cd36c1cebb5418230aa3539eddbb25c1e471da104d4e`.
Frozen source, commands, original failing regressions and all repair logs are
retained in `/tmp/cellule-enrollment-closing-evidence`.

Published parent `b1390b2` passes follower/object capacity, workspace, MSRV,
contract, cookbook quality, Compose smoke and fuzz checks. The ordinary leased routing
profile passes with a lowest gated throughput ratio of 91.36%; object-only routing
is still running. The pinned same-source control at `9037a04`
([run 37163481429](https://github.com/crabbuild/cellule/actions/runs/37163481429))
passes both profiles and aggregate routing. Its gated throughput minimum is
95.37% leased and 92.47% object-only; the existing 90% gate is unchanged. Both
177-file original campaigns, manifests, frozen binaries, stage TSVs and analyses
are retained. This control does not qualify the PR baseline comparison or isolate
the cause of the earlier routing failures.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Qualify the current head, including both ordinary routing profiles; retain and diagnose failures without weakening gates. |
| 1 | Complete authenticated aggregate observation of all original reader/follower policies, failed-owner successors, durable unknown actions and native/external accepted work. |
| 2 | Implement SettleRoles/Finalize with original action joining before terminal drain handoff, native Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider faults, mixed binaries and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The admission fence supplies no aggregate settlement or finalization proof.
SettleRoles/Finalize remain refused; the complete W1–W10 goal remains active.

## October 3 2026 original accepted fleet work checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`.

Native observations now include the original finite fleet executor alongside
actors and reader/follower owners. `CellNode::fleet_action_work()` reads that bank
without submitting, reaping, joining or executing work. It retains exact effect,
inspection and capture keys, independent response/task lifecycle, canonical result
digests, publication and known-outcome flags, and original response, execution,
publication, join and bank failures. The two-job bound is unchanged.

Every `FleetNodeSnapshot` excludes only its exact originally retained capture;
other accepted captures remain visible. No job handle or recursive snapshot body
is retained, preventing result-bank cycles. Native responses share their admitted
4-KiB metadata charge; encoding scratch is separately admitted before copying.
Full inventories copy this bounded metadata into application-accounted buffers
and keep its original interval and errors after native pages drop.

Effect/inspection admission and removal advance a checked revision, detecting
turnover between empty reads. A separate all-job revision detects concurrent
capture turnover inside one native interval. Fresh read-only captures leave the
cross-capture fingerprint stable. Native before/after reads, every traversal page,
and all global native rechecks compare the original work fingerprint. Coverage
digest v2 binds this metadata. Persisted and transport codecs are unchanged.

Public cases cover lost capture/inspection waiters, retained allocation sharing,
exact self-exclusion, sibling acceptance during native capture, stable successive
read-only captures, and original fatal join evidence while drain still owns a
paused inspection. Closed-runtime capture errors supply no empty proof. A real
managed minion graph retains this barrier; accepted inspection turnover under the
same full journal/roster invalidates its original global native recheck.

### Verification and CI

All 13 fresh Rust commands pass on the isolated Rust 1.99 snapshot with its
mounted checkout-specific target. All nine static gates pass: format,
boundaries/layout, document syntax/links, SQL/peer contracts, cookbook format/layout
and diff. The documentation gates cover 132 Rust snippets and 1,278 local links.
Qualification gates are unchanged.

| Verification | Result |
| --- | --- |
| Full framework, all features, locked | 1,673 passed; zero failures; 36 documented ignored cases. |
| Complete canonical minion | 270 passed; zero failures or ignores. |
| Distinct framework/minion passes | 1,943; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | All 54 passed. |
| Framework check, Clippy and API docs | All targets/features pass; lint/docs deny warnings. |
| Normal executable and Axum without default features | Build and tests/docs pass. |
| Fresh cookbook | All 256 tests, check, Clippy, warning-denied docs and binary builds pass. |

All 1,091 Rust/Cargo paths match the qualified source manifest, SHA256
`eb6271a7ebdc507b4ab51b9c118fafda3948d8fe8bf0dfa74b13fff53019742b`.
Commands, frozen source, source manifests, original failures, raw qualification
artifacts and analyses are retained under `/tmp/cellule-action-work-evidence`.

Published parent `9037a04` is mergeable. Follower/object capacity, workspace,
MSRV, contract, cookbook quality/scenarios and Compose smoke pass. Its leased
routing campaign fails local command c1 (88.72%) and uncached forwarded query c16
(89.40%) against the unchanged 90% throughput gate; all latency gates pass.
Object-only local command c16 also fails at 89.74%. Local command c1 preparation
means are 8.68 → 10.28 ms, authority means 4.47 → 4.61 ms; object-only c16
preparation means are 9.53 → 11.02 ms, authority means 4.59 → 4.89 ms.
These results do not independently isolate a cause. The original four pairs, frozen binaries, source identities, stage TSVs
and job log are retained. A pinned same-source comparison is a separate diagnostic
control at `9037a04` ([run 37163481429](https://github.com/crabbuild/cellule/actions/runs/37163481429))
and cannot replace the PR's ordinary baseline qualification.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Diagnose the retained routing failures with controlled provider/publication evidence and qualify the new head without weakening gates. |
| 1 | Complete authenticated aggregate observation: all reader/follower policies, failed-owner successor policy, durable unknown actions, remaining native primitives and external accepted work. The finite executor barrier alone cannot prove settlement. |
| 2 | Implement SettleRoles/Finalize, joining the original accepted action before terminal drain handoff; confirm native Stopped, directory withdrawal, boot retirement and committed operation completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider, mixed-binary and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The complete W1–W10 goal remains active.

## October 3 2026 retained replacement policy checkpoint

`FleetObservation::with_role_evacuations` retains fresh reader/follower policy
checks through the public controller. Each confirmation now preserves its full
immutable durable record. Checks agree on one exact full head, registry and
roster, retain their fresh intervals inside the outer observation, and compare selected signed boots with their
existing reader/follower producer identities. Duplicate original obligations,
including overlapping retired ensembles, refuse. Matching current writer rows
and follower epoch/ensemble cannot contradict the confirmed policy. Attachment
order with other retained proof collections preserves the same checks.

Planner digest v8 binds presence, canonical record order, full barriers, original
fresh intervals, current authority, actual ready reader prefixes and retained
native source/member inventories. Persisted history, enrollment/action and
transport formats are unchanged. Partial input stays partial; supplying a subset
does not establish complete policy, accepted-work joining or finalization.

Five public minion cases use actual managed readers and follower rotation. Reader
policy is reconstructed through an independent SQLite client and consumed by the
public reconciler; stale full heads, duplicates, restamping and missing signed
replacement boots refuse. Follower checks retain three source/member inventories
beside a four-boot/four-physical-node graph in either attachment order, then pass
through public reconciliation. An earlier full head with the same registry
refuses in both orders. Both partial controller paths remain blocked and dispatch
no effect or inspection. One host unit case binds collection presence in the
planner digest and refuses replacement without upgrading coverage.

### Verification and CI

All six focused cases pass. Both intervening `main` merges are incorporated,
through `5724959`. The final isolated Rust 1.99 source passes eight fresh Rust
commands, including the normal minion executable build and Axum without default
features. Nine static gates pass, covering 132 Rust snippets and 1,278 Markdown
links; the process qualification harness passes 49 Python unit cases.

| Verification | Result |
| --- | --- |
| Full framework, all features, locked | 1,667 passed; zero failures; 36 documented ignored cases. |
| Complete canonical minion | 269 passed; zero failures or ignores. |
| Distinct framework/minion passes | 1,936; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | All 54 passed. |
| Framework check, Clippy and API docs | All targets/features pass; lint/docs deny warnings. |
| Cookbook | All 256 tests, check, Clippy, warning-denied docs and binary builds pass. |

All 11 commands passed on the preceding merged snapshot. Its five cookbook
results are reused only after comparing exact source inputs: cookbook's own
lockfile and every production dependency remain byte-identical. All latest-merge
changes lie outside those build inputs; 1,512 source files match the earlier
snapshot. All 1,089 committed Rust/Cargo paths match the final qualified inputs,
with manifest SHA256
`703808afa4d01192ee0b1cc2147d6ecfb08a263a0b31dc737fdb9a182454abb3`.
Commands, exact source archives, original failures, scope comparison and results
are retained under `/tmp/cellule-role-evacuation-observation-evidence`, including
`merged-final` and `latest-main-final`.

Published parent `7ebc0c1` passes every required CI check, including both routing
and capacity profiles. Merge resolution preserves both minion and Axum HTTP CI
gates, the new public Axum error envelope, and the bounded original receipt/root
capture alongside exact restored-byte evidence. Fresh CI must qualify the new
head. No profile, deadline, throughput/latency gate or proof is weakened.
The preceding `fe378b0` routing campaign fails only leased forwarded commands at
concurrency 16 (88.92% of baseline) and object-only local commands at concurrency
16 (86.19%), against the unchanged 90% throughput gate. All latency gates pass.
Median publication preparation means are 7.90 → 8.98 ms and 9.17 → 11.12 ms,
respectively; authority means are 4.80 → 4.89 ms and 4.64 → 4.97 ms. Provider
counts retain about one extra PUT per command, while read counts match. These
observations do not independently isolate required lineage publication from
provider/runner variation. Frozen binaries, source manifests, all four pairs,
stage TSVs, raw hashes and analysis are retained; no gate or proof is weakened.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Resolve routing qualification with controlled publication/provider evidence; qualify the new head and preserve any capacity unknown-result recurrence. |
| 1 | Complete authenticated production observation with all required reader/follower policy, failed-owner successor coverage and every original native/external accepted-work barrier. Retaining individual checks cannot prove the complete aggregate. |
| 2 | Implement SettleRoles/Finalize, joining original accepted actions before terminal drain handoff; confirm Stopped, withdrawal, boot retirement and committed completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider, mixed-binary and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The complete W1–W10 goal remains active.

## October 3 2026 committed process binding checkpoint

The canonical executable remains `crates/cellule-host/minion`, Cargo target
`fleet_operations`.

`FleetFailedBootProcessRequest::confirm` now binds process evidence to an already
Retired boot's immutable settlement digest before dependent writer/suffix/successor
collection. Previously two equal provider reads could supply a different witness
or request basis. Both public regressions fail on the preceding implementation
and pass with the correction. A third case preserves an original terminal v1
retirement. The original fenced v2 request also remains valid; no persisted or
transport encoding changes.

Two public minion cases combine fresh failed-boot closure and actual native
original-writer successor inventory across two applications. They retain four
successor proofs and two original recovery suffixes at one full head/registry,
then consume both attachment orders through the public reconciler. A changed
full head with the same registry is refused in either order. The scoped
observation remains partial: it cannot settle roles or finish maintenance.
The process child is still a joined lifetime stand-in, requiring separate
physical CellNode/provider/external-work qualification.

### Verification and CI

All five focused cases pass. All 11 full Rust 1.99 verification commands complete
with exit zero on the isolated source. Nine static gates pass.

| Verification | Result |
| --- | --- |
| Full framework, all features, locked | 1,654 passed; zero failures; 36 documented ignored cases. |
| Complete canonical minion | 264 passed; zero failures or ignores. |
| Distinct framework/minion passes | 1,918; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | 54 passed. |
| Framework check, Clippy and API docs | All targets/features pass; lint/docs deny warnings. |
| Separate cookbook | Check, Clippy, warning-denied docs, all 256 tests and binary builds pass; locked. |
| Static gates | Format, boundaries, layout, 128 Rust snippets, 1,245 Markdown links, 28 SQL/peer assertions, 570 protocol links, cookbook layout/format and diff pass. |

The frozen Rust/Cargo manifest has 1,075 paths and SHA256
`a87bb01c7430c1419bf95f3fae0669cecfccb1851b61394334f2ce92dd65c2ed`.
Before/after failures, focused logs, exact source archive and verification
commands/statuses are retained under `/tmp/cellule-retired-process-binding-evidence`.
The previous isolated checkout and mounted target are reused after their prior
readers finish; previous committed archives remain preserved.

Published parent `fe378b0` is mergeable and passes workspace/MSRV, both
object/follower capacity checks, Compose smoke, cookbook quality, contracts,
website, fuzz, all cookbook scenarios and the fast model matrix. Both routing
profiles are still running at the latest read. Successful capacity
qualification does not isolate the preceding SQL-deadline/unknown-result failure.
Workloads, proof requirements, deadlines and routing gates remain unchanged.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Complete current/new-head qualification, including both routing profiles; retain and diagnose any capacity unknown-result recurrence. |
| 1 | Complete authenticated production observation with reader/follower replacement policy and every original native/external accepted-work barrier. Joint original-writer/closure consumption alone cannot make coverage complete. |
| 2 | Implement SettleRoles/Finalize, join original accepted actions before terminal drain handoff, then confirm Stopped, withdrawal, boot retirement and committed operation completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss/recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider, mixed-binary and load/soak qualification; W10 exercised runbooks and staged rollout/rollback. |

The complete W1–W10 goal remains active. This correction enforces committed
evidence identity; it grants no replacement policy or maintenance finalization.

## October 3 2026 fresh failed boot observation checkpoint

`FleetFailedBootRetirement::confirm` freshly rechecks an already committed
original Retired boot without publishing an enrollment event or starting a native
or process effect. It retains the original row, immutable process witness and
closure digest. Capture and both provider reads share one monotonic thirty-second
interval and the exact complete head/registry. The ordinary publication final
check supplies the terminal authority, related-role and physical-reference
barriers. Missing retirement, changed process evidence and a changed full head
refuse; provider source errors remain inspectable.

`FleetObservation::with_failed_boot_closures` retains these capsules in the public
observer/reconciler path. It refuses duplicate boots or attachment, interval
restamping, inconsistent full barriers and current advertisements/Cell rows from
a retired session. Retained roster rows and matching original writer evidence
must agree. Planner input digest v7 binds collection presence, canonical boot
order, full barriers, closure digests and original intervals. This changes no
persisted enrollment, action or transport encoding and cannot upgrade partial
coverage.

Five public minion confirmation cases cover read-only replay, independent SQLite
client reconstruction, missing retirement, both failing or changed process reads,
full-head changes with unchanged registry, and clock/deadline refusal. Four
observation cases consume actual surviving native inventories and physical
reference scans through the public reconciler and refuse stale, duplicate,
restamped or retired-session inputs. The complete observation fixture has two
surviving managed boots, three physical nodes and no writers; its child is a
joined lifetime stand-in. Combined original-writer/closure qualification, actual
failed CellNode processes, replacement policy and external accepted work remain
required.

### Current CI and HTTP error correction

Published `f655139` passes framework tests and all 250 preceding minion cases,
including the corrected lease fixtures. Follower capacity, Compose smoke,
cookbook quality/scenarios, contracts, website, fuzz, MSRV and the fast model
matrix pass. Workspace fails Rust 1.99 Clippy because the Axum handler's HTTP
error contains at least 136 inline bytes. Its private response body is now boxed;
the integration size guard fails on the original source and passes with the
correction. Existing public HTTP behavior tests preserve status, JSON, receipts,
pending evidence and typed source downcasting. No lint allowance was added.

Object capacity repeat 1 passes; repeat 2 fails on an unresolved original mutation
in the skewed 128 actions/node/s window after SQL deadlines and active Cell counts
fall from 4 to 2/3 on two nodes. Earlier skewed 64 completes. The raw artifact's
per-command execution/publication/provider files are empty, so these observations
do not isolate the cause. Driver/node/provider logs, resources and raw hashes are
retained. Current object-only routing passes. Leased routing fails local command
concurrency 1 throughput at 85.71% of baseline against the unchanged 90% gate;
its concurrency 16 and all other gates pass. Median preparation means increase
from 8.46 to 10.51 ms, and authority means from 4.66 to 4.99 ms. These stage
observations do not isolate required lineage work from provider/runner variation.
Both profiles retain frozen
binaries, manifests, samples and comparisons. This increment does not claim
a capacity or leased-routing fix. Profiles, workloads, deadlines and proof gates remain
unchanged.

Rust 1.99 cookbook Clippy also flags fixed-size `chunks_exact` loops. Five
bounded hex decoders now use `as_chunks::<2>()`; their exact length/lowercase
validation, decoded bytes, source errors and encoded identities remain unchanged.
No lint allowance or qualification change is used. Only these five cookbook
Rust paths change after successful framework/minion verification; cookbook cannot
supply dependencies to the framework. All cookbook gates are repeated on the
final isolated source.

### Verification

| Verification on the isolated Rust 1.99 source | Result |
| --- | --- |
| Full framework workspace, all features, locked | 1,654 passed; zero failures; 36 documented ignored cases. |
| Complete canonical minion target | 259 passed; zero failures or ignores. |
| Distinct framework/minion passes | 1,913; focused repeats and overlapping local LTX excluded. |
| Focused new public behavior | Five confirmation and four observation cases pass; HTTP behavior and before/after size regression pass. |
| Local LTX without default features | 54 passed. |
| Framework check, Clippy and API docs | All targets/features pass; lint/docs deny warnings. |
| Separate cookbook quality | Check, Clippy, warning-denied docs, all 256 tests and binary builds pass; locked. |
| Static gates | Format, boundaries, layout, 128 Rust snippets, 1,245 Markdown links, 28 SQL/peer assertions, 570 protocol links, cookbook layout/format and diff pass. |

All 11 final verification commands completed with exit zero. All 1,074 final
Rust/Cargo paths match the qualified isolated inputs. Manifest SHA256:
`4ddf546a61fe8cc73fb7d4d2598ac395044a3973262430ac8b8652ff130ec849`. The only post-framework source changes are the
five cookbook decoders, each included in the repeated cookbook gates.

Evidence remains under `/tmp/cellule-failed-boot-observation-evidence`. The prior
isolated checkout/target `cellule-inherited-successors-final-831b83a` is reused only
after its prior readers finished; earlier committed source archives remain under
`/tmp/cellule-inherited-successors-evidence`. Current verification uses Rust 1.99,
matching workspace CI. Exact Rust/Cargo inputs, raw failures, commands/statuses,
source archives and final merge checks are retained.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Complete capacity SQL-deadline/unknown-result diagnosis and both routing qualifications; qualify the new published head without weakening proofs or gates. |
| 1 | Complete authenticated production observation with combined original writer/closure evidence, reader/follower replacement policy and every original accepted-work barrier. |
| 2 | Implement SettleRoles/Finalize, join original actions before terminal drain handoff, and confirm Stopped, withdrawal, boot retirement and committed operation completion. Finish Cron/Blob external owners and primitive fault matrices. |
| 3 | Complete receiver-session loss, recovery/adoption, refusal/unknown supervision and sustained convergence; deliver canonical minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider, mixed-binary, load/soak and capacity qualification; W10 exercised runbooks and staged rollout/rollback. |

The complete W1–W10 goal remains active. Fresh failed-boot confirmation supplies
no writer availability, replacement-policy or maintenance-finalization grant.

## October 3 2026 inherited original recovery checkpoint

Six public minion cases now exercise complete original successor collection with
inherited overlays across two applications. An earlier boot publishes real
SQLite predecessors and seals two fsynced follower suffixes. Removing the actual
canonical manifest causes two ordinary intermediate native takeovers to commit
their ownership CAS and fail before materialization. Both preserve their exact
recovery input without a successful acquisition record. The fixture joins and
fences that CellNode, restores the same manifest bytes, renews the later boot
through canonical heartbeat CAS, and uses ordinary native takeover to recover
both Cells. No fabricated successful Control, prepared root or acquisition
record supplies the proof.

The aggregate preserves the earlier manifest's Cell epoch 1, the original failed
owner epoch 2 and materializing acquisition epoch 3. It confirms both SQLite
values and every exact canonical acquisition input. Missing/corrupt historical
manifests, a substituted backend, missing earlier owner history and missing
materialization history refuse the complete collection despite warm native
actors. A later acknowledged publication advances the current root while the
exact earlier materialized prefix remains required.

Shared private fixture extraction keeps existing default identities, clocks,
lease lengths, follower inputs and qualification profiles unchanged. Changes are confined to minion and host fixture/test Rust code; no framework
production, persisted or transport codec changes. Process evidence remains a joined lifetime stand-in;
physical process/provider, complete role policy, external accepted work and
maintenance finalization remain unqualified.

### CI lease fixture correction

The preceding `831b83a` head passes follower/object capacity, Compose smoke,
cookbook quality and scenarios, contracts, website, fuzz, MSRV and the fast model
matrix. Workspace CI reports `node lease bounds are invalid` in the dropped
fleet-action waiter case: five fleet fixtures build `now` and `expires` from two
separate clock readings. Advancing a millisecond between them exceeds the
unchanged maximum 60-second lease. Each now captures one observation timestamp
for both bounds. Production lease validation and test assertions are unchanged.

Both routing profiles still fail local command concurrency 16 throughput:
leased reaches 89.53% and object-only 88.24% of baseline against the unchanged
90% gate. Forwarded commands and query gates pass. Their publication traces put
most of the difference in preparation: median per-command means increase from
9.13 to 10.65 ms (leased) and 8.95 to 10.54 ms (object-only). This locates the
stage without isolating required lineage I/O from backend/runner variation.
Both profiles' frozen binaries, manifests, raw samples and stage analysis are
retained with this checkpoint. Routing qualification remains open.

### Verification

| Verification on the isolated source | Result |
| --- | --- |
| Full framework workspace, all features, locked | 1,652 passed; no failures; 36 documented ignored cases. |
| Complete canonical minion target | 250 passed; no failures or ignored cases. |
| Distinct framework/minion passes | 1,902; focused repeats and overlapping local LTX excluded. |
| Focused inherited aggregate | All six new cases passed. |
| Local LTX without default features | 54 passed. |
| Framework check, Clippy and API docs | Passed; all targets/features, locked; lint/docs deny warnings. |
| Separate cookbook quality | Check, Clippy, warning-denied docs, all 256 tests and binary builds passed; locked. |
| Static gates | Format, boundaries, layout, 127 Rust snippets, 1,245 Markdown links, 28 SQL/peer assertions, 570 protocol links, cookbook layout/format and diff passed. |

All 11 original verification commands and all four supplemental verification
groups completed with exit zero. The final isolated snapshot differs only in
the four host test files containing the five lease constructions. Full workspace
tests again passed 1,652 cases with the same 36 documented ignores; all-target/
feature check, warning-denied Clippy and 20 consecutive repeats of the affected
case passed. Minion, framework production and cookbook Rust/Cargo inputs are
identical across both snapshots. All 1,070 Rust/Cargo paths match the final
isolated source. Manifest SHA256:
`dda0346019faf75be5541737dbe8d7110201edba68151e4448bcbbf0c9236922`.
Complete commands/statuses, logs, source manifests/archives, initial fixture
failures and raw routing evidence are retained. All owned local verification
processes completed.

Evidence remains under `/tmp/cellule-inherited-successors-evidence`; the
original snapshot and mounted target use suffix
`cellule-inherited-successors-831b83a`, and the final snapshot uses
`cellule-inherited-successors-final-831b83a`. No local cloud/provider service was
started. Fresh CI must qualify the published increment; preceding checks do not
qualify it.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Qualify the current CI head, especially both routing profiles, without weakening required lineage proof or the existing throughput gate. |
| 1 | Complete authenticated production observation with failed-boot coverage, reader/follower replacement policy and every original accepted-work barrier. Current role coverage explicitly refuses failed follower owners before canonical recovery/retirement. |
| 2 | Implement SettleRoles/Finalize, join original actions before terminal drain handoff, and confirm stop, withdrawal, boot retirement and committed operation completion. Finish Cron/Blob owners and primitive fault matrices. |
| 3 | Complete receiver-session loss, recovery/adoption, refusal/unknown supervision and sustained convergence; deliver minion maintenance and receiver-loss commands. |
| 4 | W9 physical process/provider, mixed-binary, load/soak and capacity qualification; W10 exercised runbooks and staged rollout/rollback. |

The complete W1–W10 goal remains active. This inherited collector evidence
supplies no retention pin, complete role settlement or node finalization grant.

## October 3 2026 original successor observation checkpoint

`FleetObservation::with_original_writer_successors` now retains the complete
original proof inventory across applications, validates matching planner rows
and signed physical node/session/endpoint/compiled release, and preserves its
capture interval. Complete scans must include every scoped successor. The public
reconciler compares the full retained journal head and registry, including when
the registry alone is unchanged. Role coverage must share that same snapshot.
The local planner digest binds the complete original proof set; persisted and
transport codecs are unchanged. Partial observations remain partial and cannot
settle roles or complete maintenance.

Six new minion cases use actual native successors and the public observation
and reconciler APIs. They cover complete cross-application proof retention,
duplicate attachment, eight protected native-row mismatches, scoped omission,
missing physical boots, restamped intervals, fresh partial consumption, stale
full heads and actual native publication changing the digest. Their samples
publish fresh signed advertisements through canonical directory refresh. The
original process remains a lifetime stand-in and this observer remains partial.

Current head `c5a572f` passes follower/object capacity and cookbook quality.
Its workspace CI failed the existing cancellation test's fixed 100-ms stage
assumption. The corrected test polls the actual origin reservation alongside
the original three-second collection; an early return or unchanged deadline
fails it. Cancellation and post-cancellation ledgers still undergo the existing
checks. No workload, profile, qualification gate or expected proof was weakened.

The leased-routing comparison on this head fails forwarded and local c1 command
throughput at 86.6% and 87.4% of baseline against the existing 90% gate. Median
preparation means increase from 8.98 to 10.94 ms and 8.21 to 9.95 ms respectively.
Raw paired measurements, binaries, manifest and stage analysis are retained
under `/tmp/cellule-original-observation-evidence`. The preparation stage locates
the difference; it does not isolate required lineage I/O from backend/runner
variation. Fresh routing qualification and diagnosis remain required.

### Verification

| Verification on the isolated source | Result |
| --- | --- |
| Full framework workspace, all features, locked | 1,652 passed; no failures; 36 documented ignored cases. |
| Complete canonical minion target | 244 passed; no failures or ignored cases. |
| Distinct framework/minion passes | 1,896; focused repeats and overlapping local LTX excluded. |
| Focused observation and cancellation | All six new cases passed; corrected cancellation passed 20 consecutive repeats. |
| Local LTX without default features | 54 passed. |
| Framework check, Clippy and API docs | Passed; all targets/features, locked; lint/docs deny warnings. |
| Separate cookbook quality | Check, Clippy, warning-denied docs, all 256 tests and binary builds passed; locked. |
| Static gates | Format, boundaries, layout, 127 Rust snippets, 1,245 Markdown links, 28 SQL/peer assertions, 570 protocol links, cookbook layout/format and diff passed. |

All 11 verification commands completed with exit zero. All 1,068 Rust/Cargo
paths match the isolated tested source. Manifest SHA256:
`a7c5de87bf0f24d263529fa8751cf0df19094d4d55be201b069f6053a5ca67d7`.
Source archive, commands/statuses, complete logs and initial fixture failures are
retained under `/tmp/cellule-original-observation-evidence`. Snapshot and mounted
target use suffix `cellule-original-observation-c5a572f`. All owned local
verification processes completed; no local cloud/provider service was started.
Fresh CI must qualify the published increment; earlier checks do not qualify it.

### Highest remaining work

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Qualify current CI, including the corrected cancellation case and both routing modes. Diagnose command preparation without weakening the unchanged qualification gate. |
| 1 | Complete authenticated production observation: inherited-overlay aggregate success/faults, reader/follower replacement policy and all original accepted work. The public proof attachment alone does not supply these barriers. |
| 2 | Implement SettleRoles/Finalize, join original actions before terminal drain handoff, and confirm stop, withdrawal, boot retirement and committed operation completion. Finish Cron/Blob external owners and primitive fault matrices. |
| 3 | Complete cross-session receiver loss, recovery/adoption, refusal/unknown supervision and sustained pressure/count/concurrency evidence. |
| 4 | Complete canonical minion maintenance/receiver-loss commands, W9 process/provider, mixed-binary, load/soak and capacity campaigns, then W10 exercised runbooks and staged rollout/rollback. |

The full W1–W10 plan remains active. Minion is canonical at
`crates/cellule-host/minion`, Cargo target `fleet_operations`.

## October 3 2026 complete original native successor checkpoint

`FleetOriginalWriterSuccessorInventory::collect` verifies every retained
original writer across applications against an actual managed native successor.
It combines complete original boot suffix inputs with the original root,
exact sealed suffixes and inherited-overlay manifest rows. Rootless originals
still require complete current origin verification. Historical overlay selection
preserves its original Cell epoch. Missing mappings, wrong physical boots,
changed publication, missing origin dependencies and provider errors refuse
collection without exposing a partial result.

The shared `CellRuntime::observe_serving` joins ordinary FIFO admission and
checks exact authority position and native generation at a later ownership
epoch. Movement uses this same observation. The collector repeats every
retained successor after all prefix reads, then rechecks the original
process/log/full-journal and roster barriers. One absolute deadline spans the
inline collector; existing providers retain accepted work and native memory/I/O
uses shared admission. The application authenticates canonical backends and
accounts bounded copied result buffers.

Seven new minion cases use four original writers across two applications,
ordinary native takeover, two rootless originals and two genuine sealed suffixes.
Cases cover omitted/error mappings, physical-node substitution, changed
publication during another lookup, deleted origin bytes despite a warm actor,
cancellation cleanup and an expired deadline before provider work. Two native
cases cover exact later serving, changed position, foreign runtime/incarnation,
drain and closure. Complete inherited-overlay aggregate success/fault scenarios
remain required. The provider currently accepts actual `Arc<CellNode>` references;
remote/process integration, full role policy, accepted work and finalization
remain unfinished. The original process child remains a lifetime stand-in.

Main is incorporated through `39a3d0b`. Conflicts retain native lineage metadata,
verified descriptor reuse, the original-owner root capture and its two-second
qualification bound. Main's final drained-root restore evidence is added to the
existing strict coverage checks. Minion remains canonical at
`crates/cellule-host/minion`, Cargo target `fleet_operations`.

### Verification

| Verification on the final isolated merged source | Result |
| --- | --- |
| Full framework workspace, all features, locked | 1,652 passed; no failures; 36 documented ignored cases. |
| Complete canonical minion target | 238 passed; no failures or ignored cases. |
| Distinct framework/minion passes | 1,890; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | 54 passed. |
| All-target/all-feature check, Clippy, API docs | Passed; warnings denied for lint and docs. |
| Cookbook quality | Check, Clippy, warning-denied docs, all 256 tests and binary builds passed; locked dependencies. |
| Python qualification contracts | All 44 passed, preserving both root-barrier and drained-restore requirements. |
| Format, boundaries, layout, fences, links, SQL/peer, diff | Passed; 127 Rust snippets, 1,245 Markdown links, 28 SQL/peer assertions and 570 protocol links. |

All 1,065 Rust/Cargo paths match the final isolated source. Manifest SHA256:
`ec39634292a6e7ff6066662c27063c1703359e59e26fc9bfdfb339c6cdeff713`.
Source archive, commands, statuses and complete logs remain under
`/tmp/cellule-original-successors-final-evidence`; the separate snapshot and
mounted target use suffix `cellule-original-successors-final-5e19875`.
Earlier focused failures and the first merged snapshot's independent verification
are retained under `/tmp/cellule-original-successors-evidence`.

### CI and remaining delivery

Published `c78ab9d` passed follower/object capacity, both routing profiles and
their aggregate, workspace/MSRV, Compose smoke, contract, website, fuzz and
enabled fast models. Cookbook quality failed because its separate lock omitted
`cellule-host`'s existing `blake3` dependency; process scenarios were skipped.
The repair adds that single dependency entry and retains `--locked` and all
qualification gates. These earlier CI passes do not qualify the newer merged
source; fresh CI remains required.

| Priority | Remaining delivery stream |
| --- | --- |
| 1 | Consume the complete original-writer/suffix/native-successor collector in authenticated production observation. Exercise inherited-overlay aggregate success/faults and complete reader/follower replacement policy and original accepted-work barriers. |
| 2 | Implement SettleRoles/Finalize, join original actions before terminal drain handoff, and confirm stop, withdrawal, boot retirement and committed operation completion. Finish Cron/Blob external owners and primitive fault matrices. |
| 3 | Complete cross-session receiver loss, replacement, recovery/adoption, refusal/unknown supervision and sustained pressure/count/concurrency scenarios. |
| 4 | Complete canonical minion maintenance and receiver-loss commands, then W9 process/provider, mixed-binary, load/soak and capacity qualification. |
| 5 | W10 exercised operator runbooks, staged rollout and rollback evidence. |

The full W1–W10 plan remains active. This point collector grants no retention
pin, role settlement, stopped-node proof or complete maintenance support.

## October 3 2026 complete original boot suffix input checkpoint

`FleetOriginalBootSuffixInventory::collect` combines the committed original
writer set with the exact original boot's canonical sealed/retired log and its
complete digest-verified recovery manifest. It retains every application and
matches each suffix to its exact original owner epoch and predecessor. Fresh
process, physical fence, canonical log and full journal checks surround manifest
I/O. Missing commitment, unresolved recovery, omitted owners, corrupt bytes,
changed predecessors and changed process/registry evidence refuse collection.
One absolute deadline bounds inline collection; existing providers retain
accepted storage work and its original errors.

The native `NodeDirectory::recovered_session` supplies this read boundary without
retiring the log or granting node closure. No manifest means canonical no-suffix
input for that log, while every original object-covered/rootless writer remains
retained. Earlier-boot overlays preserved in original Controls still require
independent exact manifest and successor proofs. This collector is an exported
input building block, exercised by minion; complete production observation and
maintenance finalization remain unfinished.

Eight new minion cases use actual SQLite roots, later captured mutations,
two fsynced followers and ordinary two-application recovery/pinning/sealing.
Independent journal reconstruction retains all 66 original owners and both
sealed suffixes. Fault cases cover uncommitted/omitted owners, changed
predecessors, missing/corrupt manifest bytes, process failures/witness changes,
registry changes and an expired deadline before provider work. Native tests
cover no-log, unresolved, sealed and retired boundaries and exact physical
identity/live claimant. The process child remains a lifetime stand-in; these
cases do not establish an OS-crashed CellNode or external-job qualification.

The branch incorporates main through `c23ca51`. Its qualification-driver
conflict retains the exact original-owner root capture and two-second bound,
and adds upstream's receipt-monotonic readback check.

### Verification

| Verification on the isolated merged source | Result |
| --- | --- |
| Full framework workspace, all features, locked | 1,630 passed; no failures; 36 documented ignored cases. |
| Complete canonical minion target | 231 passed; no failures or ignored cases. |
| Distinct passes | 1,861; focused repeats and overlapping local LTX excluded. |
| Focused original writer cases | All 21 selected cases passed. |
| Local LTX without default features | 54 passed. |
| All-target/all-feature check, Clippy, API docs | Passed; warnings denied for lint and docs. |
| Format, boundaries, layout, Rust fences, links, SQL/peer, diff | Passed; 125 Rust snippets, 1,224 Markdown links, 28 SQL/peer assertions and 569 protocol links. |

The frozen source contains 1,045 Rust/Cargo paths, including upstream's separate
cookbook workspace; the test commands above cover the framework workspace and
canonical minion. Manifest SHA256:
`dadb6aec396ca1a7f35249c18de8df817c06452937ce8a30f95e293c1793c70c`.
Source archive, exact commands, statuses and complete logs are retained under
`/tmp/cellule-original-suffix-inventory-evidence`; snapshot and mounted target
use suffix `cellule-original-suffix-inventory-37e3184`. Initial compile and
fixture-assertion failures remain beside their corrected passing runs.

### Current CI and highest remaining work

Published `37e3184` passes follower/object capacity, workspace/MSRV, Compose
smoke, contract, website, fuzz and enabled fast models. Its routing comparisons
fail four unchanged command lanes: leased forwarded c1, leased local c1/c16 and
object-only local c1. Throughput is respectively 80.2%, 82.1%, 89.8% and 88.4%
of baseline against the existing 90% gate. Original artifacts and comparison
rows are retained under this checkpoint's evidence directory. Parent `704fa11`
passed both Linux routing modes and their aggregate; that earlier result does
not qualify the newer head. Fresh CI and routing diagnosis remain required.
No workload, pair count, threshold or expected evidence was weakened.

| Priority | Remaining delivery stream |
| --- | --- |
| 0 | Diagnose and qualify both routing profiles on the current merged head. |
| 1 | Complete production observation: every original root, original/inherited suffix, exact current native serving, physical boot/process, operation barrier, reader/follower replacement policy and accepted work. |
| 2 | Implement SettleRoles/Finalize, joining original actions before terminal drain handoff; confirm stop, withdrawal, boot retirement and committed operation completion. Finish Cron/Blob external owners and primitive fault matrices. |
| 3 | Complete cross-session receiver loss, replacement, recovery/adoption, refusal/unknown supervision and sustained pressure/count/concurrency scenarios. |
| 4 | Complete canonical minion maintenance and receiver-loss scenarios, then W9 process/provider, mixed-binary, load/soak and capacity campaigns. |
| 5 | W10 exercised operator runbooks, staged rollout and rollback evidence. |

The complete W1–W10 plan remains active. This checkpoint grants no authority,
current serving, role settlement, stopped-node proof or maintenance completion.

## October 3 2026 committed original writer reload checkpoint

`FleetOriginalWriterInventory::load` reconstructs the original writer manifest
through the existing journal's committed operation/process pointer and every
ordered immutable page. It validates the bounded header before allocation and
the complete page set before returning. Capture replay uses this same path.
No partial inventory escapes when a final page is missing or corrupt. Adapter
errors retain their source; one absolute deadline bounds the inline reader.
The journal's existing finite owner retains accepted storage work.

`None` means no committed inventory at that read. A validated returned manifest
with zero owners represents explicitly retained empty input. Neither establishes
that an accepted capture cannot publish later. Original scope, boot, Controls,
catalog witnesses and collection times remain unchanged. This opaque value is
historical input, not current serving, suffix availability or role settlement.
The [host integration recipe](../crates/cellule-host/docs/original-writers.md)
documents both capture and reconstruction.

Five new public minion cases cover absence versus complete empty capture, stale
full snapshots, missing/corrupt final pages, original SQL errors and expired
deadlines/invalid keys. Existing reconstruction and canceled publication cases
now use the loader, preserving all 66 original epochs across two pages. The
joined process remains a lifetime stand-in, not an OS-crashed CellNode or
external-job qualification.

| Verification on the isolated source snapshot | Result |
| --- | --- |
| Full workspace, all features, locked | 1,627 passed; no failures; 36 documented ignored cases. Nested filtered child summaries excluded. |
| Complete canonical minion target | 223 passed; no failures or ignored cases. |
| Distinct passes | 1,850; focused repeats and overlapping local LTX excluded. |
| Focused original writer cases | 13 selected and passed, including all five new cases. |
| Local LTX without default features | 54 passed. |
| All-target/all-feature check, Clippy, API docs | Passed; warnings denied for lint and docs. |
| Format, boundaries, layout, links, Rust fences, SQL/peer, diff | Passed; all 125 Rust snippets parse. |

The snapshot contains 755 Rust/Cargo paths; manifest SHA256:
`38a5f2367e124f66b5273bc0c6125232ccb801a362bc7473865777bf12b335ef`.
Source archive, command/status records and logs are retained under
`/tmp/cellule-original-writer-reload-evidence`. Snapshot and mounted target use
suffix `cellule-original-writer-reload-704fa11`. This checkpoint adds no persisted
format, authority, retry owner, scheduler or production maintenance effect.

Highest remaining implementation: aggregate every authenticated original writer
and sealed suffix with current native serving, original physical boot/process,
reader/follower replacement policy and accepted-work barriers. Then implement
SettleRoles/Finalize and join original actions before terminal drain handoff.
Cross-session failure adoption, Cron/Blob owners, complete maintenance and
receiver-loss minion scenarios, W9 campaigns and W10 exercised operations remain
required. The full W1–W10 plan remains active.

## October 3 2026 lineage publication I/O checkpoint

The native publisher strictly creates fresh lineage without an absence GET.
A create conflict reads and validates the existing record, then performs at most
one additive ETag merge. Delayed merges cannot erase earlier inputs. Failed and
lost create/update replies retain the original source error unless exact retained
inputs are independently confirmed. The existing publisher owns retries.

An opaque native `RootPreparation` allows this metadata write to overlap root
and dependency uploads. All native graph checks precede that observation. Its
construction is private; it supplies no uploaded-root, authority, restore,
serving or acknowledgement rights. The same caller-owned preparation future
joins uploads and metadata, using the shared native origin I/O permits. Only
completion of both yields `PreparedRoot`. Cancellation drops inline work and
releases its permit. A bounded private one-entry confirmation avoids repeating
that exact metadata write before authority CAS. External and rebased compaction
proposals retain their exact inputs through the canonical path before CAS.
Verified private compaction composition supplies the final original predecessor
in the native factory before metadata runs. That context is cleared from read
views; the complete proposal and its retained derivation name the same input.

The runtime recovers its original typed metadata errors while preserving native
LTX retry classes and hints. No detached task, second retry owner, authority,
queue, scheduler, persisted codec or dependency is added. Native prepared views
do not retain the callback. Minion remains canonical at
`crates/cellule-host/minion`, with Cargo target `fleet_operations`.

Eleven new cases cover fresh publication I/O, competing/stale additive merges,
failed/lost replies, ordering with paused native uploads and metadata, failed
uploads, original source/retry preservation, exact native predecessor identity
and single-permit cancellation cleanup. Existing foreground compaction and
migration cases verify the original published prefix through the rebased path.

### Verification and remaining qualification

Final verification passed 1,627 workspace tests and all 218 minion cases:
**1,845 distinct passes**, with 36 documented workspace cases ignored. Local LTX
without default features passed 54 overlapping cases. Workspace all-target,
all-feature check, Clippy and API documentation passed with warnings denied.
Format, boundaries, layout, Rust fences, links, SQL/peer and diff gates pass.
Both unchanged routing profiles were collected against the same 754 frozen
Rust/Cargo paths. Manifest SHA256:
`0958df40474c8f2caaee7753962c5a8f230bfaf5e98b7b20cc359f876b9fdb08`.
Raw commands, source archives, errors and provider evidence remain under
`/tmp/cellule-lineage-routing-evidence`. The isolated checkout and mounted target
use suffix `cellule-reader-prelease-8698183`.

Earlier published `a2edbe5` is mergeable and passes follower/object capacity,
workspace/MSRV, Compose smoke, contract, website, fuzz and enabled fast models.
Both routing profiles and their aggregate fail: all eight command lanes measure
79.5–88.1% of baseline against the unchanged 90% throughput gate, with
1,024–1,025 extra reads per 1,024 commands. Downloaded original artifacts are
retained. A failing native regression recorded 16 absence GETs for 16 roots;
fresh strict creation removes those GETs. One diagnostic local pair then showed
the remaining serial lineage PUT adding about 3 ms to the authority phase.
That incomplete comparison is retained and does not qualify a fix.

The first overlapping candidate, published as `4f646b5`, completed a local
leased diagnostic pair. Its compaction metadata named the private compacted
predecessor before the complete proposal was rebased. The second strict create
then hit Store's conflict retry schedule. The candidate's 53 local-c1 spikes
added roughly 25.8 seconds to authority work; mean authority time was 28.24 ms
against 3.05 ms baseline. The incomplete comparison is retained, and the next
pair was interrupted with its child processes joined after this cause was found.
A new real 41-publication compaction regression failed with one lineage read
instead of zero. Moving verified compaction composition into the native factory
before metadata retention passes with zero reads and exactly 41 lineage writes.
Public LTX coverage also compares metadata with the final rebased proposal under
one shared I/O permit. Final full verification was rerun after this correction.

The final candidate overlaps that PUT and retains the final predecessor under
existing admission. The complete local four-pair leased comparison passes every
unchanged gate. Object-only fails six lanes: forwarded command c1, local command
c16, forwarded query c1, uncached forwarded routing c16, and local/forwarded
expired bursts c16. Command throughput ratios are 77.2% and 86.8% in the two
failing command lanes; all query read counts match baseline. These Mac/ARM runs
are diagnostic local evidence, not Linux deployment qualification. All sixteen
benchmark processes confirmed 6,144 commands and exact final sequence recovery.
The owned provider was inspected, logged and removed after every child exited.

The temporary wrapper passed both modes to its final single-mode summary and
was rejected. The unchanged comparator was then applied to the saved complete
rows, separately for each mode and together, preserving every failure without
rerunning measurements. The published `4f646b5` Linux run also failed all eight
command lanes; its original artifacts are retained. At published `704fa11`,
follower/object capacity, workspace/MSRV, Compose smoke, contract, website, fuzz
and enabled fast models pass. Its Linux object-only routing artifact also passes
all unchanged gates, with command throughput ratios of 91.7–97.2%; original
artifacts are retained separately from the failed Mac diagnostic. Its Linux leased
routing artifact and aggregate also pass at that head. Later-head qualification
remains open; see the current checkpoint above. Workload, pair
count, thresholds and expected
correctness evidence are unchanged. The initial diagnostic wrapper mistakenly
compared one pair and was rejected by the unchanged four-repeat comparator; the
final wrapper compares only complete profiles. A pre-admission verification
snapshot passed but was superseded by the shared I/O-permit correction. Initial
test compilation also caught a shadowed test helper. None qualifies final source.

The [complete remaining streams](#ci-blocker-and-remaining-work-streams) still
apply. Next after routing qualification: complete authenticated original writer,
suffix, physical boot/process, reader/follower policy and accepted-work
aggregation; then SettleRoles/Finalize and original action joining before drain.
Cross-session failure adoption, Cron/Blob owners, maintenance/receiver-loss
minion scenarios, W9 deployment campaigns and W10 exercised operations remain
unfinished. Full W1–W10 remains active.

## October 3 2026 original sealed suffix checkpoint

`VerifiedRecoveryPrefix` binds one exact original `PinnedRecoveryCell` to the
retained closed owner and canonical acquisition metadata, then verifies native
materialization lineage and the complete current successor origin graph. The
selected Serving owner, epoch, incarnation and root are rechecked after that walk.
The original manifest epoch and every sealed boundary remain unchanged. An
interrupted ownership claim can materialize at a later epoch; missing earlier
acquisition metadata cannot replace or alter the original suffix.

The runtime wrapper reuses existing shared transient-memory admission and LTX
I/O facilities. The caller owns a finite deadline; no detached work, authority,
publication or scheduler is added. Missing original owner/acquisition metadata,
wrong original scope, corrupt records and unavailable current origin data refuse.
Direct authority callers own equivalent admission and authenticated mappings.

Host recovered serving compares the entire journal input/result with canonical
acquisition metadata. A pinned overlay requires a read-only lookup through the
existing recovery provider, the original digest-verified manifest row and this
exact suffix proof. An 8-MiB transient token covers acquisition/manifest metadata
before I/O and stays charged through verification. The host then repeats the
ordinary native actor, selected authority and inventory checks. Historical
result replay remains historical and supplies no refreshed serving observation.

Two new authority refusal tests preserve original read errors and reject matching
endpoints with different recovery inputs. A genuine public interrupted-claim case
leaves no acquisition record at its first claimed epoch, materializes through the
ordinary later takeover, advances the root and verifies the original suffix.
Normal and interrupted cases mutate all 16 original scope/boundary fields, remove
and corrupt canonical acquisition metadata, remove current origin bytes and check
memory refusal and token cleanup. Two host cases require canonical acquisition
metadata despite a live actor. Two further host cases build real captured SQLite
frames, fsync them to a follower, canonically seal and materialize the tail, advance
the successor and require the original manifest on fresh serving inspection.
Missing/corrupt evidence refuses; restoring exact original bytes permits fresh
adoption. Complete node shutdown asserts zero retained resources.

| Verification | Result |
| --- | --- |
| Full workspace, all features and locked dependencies | 1,616 passed; no failures; 36 documented cases ignored. Nested crash-fixture child summaries excluded. |
| Complete canonical minion target, all features | 218 passed; no failures or ignored cases. |
| Distinct final passes | 1,834; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | 54 passed; no failures or ignored cases. |
| Workspace check, all targets/features | Passed. |
| Workspace Clippy and API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer and diff | Passed; 124 Rust snippets parse. |
| Frozen Rust/Cargo source, including nested qualification locks | 750 paths; SHA256 `fa5b6cdb82e924c3053c7dbb64490f02514761ea0b59fba8973f3bd71e38d67c`. |
| Isolated checkout / mounted Workspace target suffix | `/tmp/cellule-reader-prelease-8698183` / `cellule-reader-prelease-8698183`. |
| Published parent | `1898f05db4d6f29aabb177b38510f75bdf41b4f1`. |

Raw commands, frozen source, final logs, downloaded routing evidence and initial
diagnostics are in `/tmp/cellule-recovered-prefix-evidence`. The first negative
fixture selected Recovering rather than the required Serving state; the initial
interrupted-claim advertisement exceeded the existing lifetime limit. Both
fixtures were corrected without weakening production contracts. Full suites and
gates ran on the final frozen Rust/Cargo source. Initial runs do not qualify it.

### CI blocker and remaining work streams

Published parent `1898f05db4d6f29aabb177b38510f75bdf41b4f1` is mergeable.
Workspace, MSRV, follower/object capacity, contract, website, fuzz, fast models
and Compose pass. Both routing performance jobs and their aggregate fail.
Their command lanes fail the unchanged 90% throughput gate. The downloaded
leased artifact records 0.807–0.888 candidate/baseline throughput and approximately
one additional origin read for each of 1,024 commands. The new lineage preflight
read is a concrete investigation lead; causality and a qualified fix remain open.
Passing correctness tests cannot close this performance gate. The earlier
unchanged reader-balance failure also remains causally unexplained. Profiles,
workloads, thresholds and expected evidence remain unchanged.

| Priority | Stream | Required delivery |
| --- | --- | --- |
| 0 | Routing CI regression | Reproduce the measured command regression, distinguish lineage metadata I/O from other costs, fix the cause while retaining exact durable prefix proof, and pass both unchanged routing profiles. |
| 1 | Complete observation and writer proof | Aggregate every original writer and sealed suffix with authenticated complete backend, physical boot/process and operation scope. Consume current native successor serving, reader/follower replacement policy and accepted-work observations through the existing reconciler. |
| 2 | Maintenance completion | Implement SettleRoles/Finalize, join original accepted actions before terminal drain handoff and confirm checked stop/withdrawal/boot retirement. Finish Cron/Blob external owners and primitive fault matrices. |
| 3 | Movement convergence | Finish receiver-session replacement, recovery/adoption, refusal/unknown and source/receiver-failure reconciliation; complete sustained pressure/count and concurrency evidence. |
| 4 | Canonical minion scenarios | Deliver complete runnable maintenance and receiver-loss scenarios at `crates/cellule-host/minion`; Cargo target remains `fleet_operations`. |
| 5 | W9 qualification | Complete process/provider fault, mixed-binary, load/soak and capacity campaigns in their documented environments. |
| 6 | W10 operations | Exercise operator runbooks and collect staged rollout evidence. |

This checkpoint supplies one exact original sealed-suffix observation, including
interrupted claims. It does not aggregate all original writers/suffixes, prove
complete role replacement, pin storage or finish maintenance. SettleRoles and
Finalize remain refused. The full W1–W10 goal remains active.

## October 3 2026 exact root prefix and current origin checkpoint

The canonical publisher and recovered-overlay materialization retain native
`PreparedRoot` links before their root authority CAS. `CellRootLineage` uses the
additional version 1 Cell/incarnation/digest path with an 8-KiB checksummed record.
Up to 64 distinct verified inputs accumulate through ETag CAS; delayed writers
cannot erase links. This supports byte-identical roots derived from different
inputs, including representation-only compaction. Failed authority publication
can leave verified proposal metadata; it grants no ownership or acknowledgement.
Existing control/LTX formats, paths and signed peer messages are unchanged.

`VerifiedRootPrefix` reaches the exact original digest, scope, TXID, checksum and
sequence through native verified preparation links. Higher counters cannot prove
that prefix. The canonical LTX origin inventory then authenticates every current
successor dependency, including body/index bytes and complete directory coverage.
Missing legacy/manual-publication metadata, unrelated prefixes, corrupt metadata
and unavailable origin data refuse. Old root documents may disappear after
compaction while retained links still prove derivation and the current graph
remains fully available. Metadata retention is not a root pin.

The host's serving checks consume the exact released or materialized recovery
root, then repeat the native actor query, selected-root/owner authority check and
native inventory check after origin verification. Fresh inspection uses these
checks; historical action result replay is unchanged. The runtime wrapper charges
transient metadata through its existing shared retained-byte ledger before I/O.
Its conservative envelope covers fixed graph/cache/fetch/decode work, bounded
lineage maps and at most 10,000 origin objects. Accepted fleet work stays under
its existing finite action owner; no scheduler or task lane is added. Direct
callers own equivalent admission and deadlines, and application Store adapters
supply bounded stream chunks.

Seven new lineage unit cases exercise the maximum codec, every truncation and
byte corruption, malformed scope/position/order, additive replay/reconstruction,
competing inputs, original write failure, lost replies, native publication
ordering and corrupt/foreign metadata. A new peer case preserves the existing
Unavailable/NotStarted error contract. Four new public runtime cases exercise
real acknowledged writes through movement/compaction, missing old documents,
missing/corrupt current origin bodies, legacy/unrelated/scope/limit refusal and
shared memory admission/release on cancellation, success, error and shutdown.
Two new public host cases retain a live successor actor while deleting its
lineage or current root, require fresh inspection refusal, restore the exact
original bytes and verify fresh adoption. Existing native quiet/foreground
compaction, migration and genuinely recovered pinned-tail cases also check exact
prefixes; the LTX inventory case checks exact and insufficient count limits.

| Verification | Result |
| --- | --- |
| Full workspace, all features and locked dependencies | 1,609 passed; no failures; 36 documented cases ignored. Nested crash-fixture child summaries excluded. |
| Complete canonical minion target, all features | 218 passed; no failures or ignored cases. |
| Distinct final passes | 1,827; focused repeats and overlapping local LTX excluded. |
| Local LTX without default features | 54 passed; no failures or ignored cases. |
| Workspace check, all targets/features | Passed. |
| Workspace Clippy and API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer and diff | Passed; 123 Rust snippets parse. |
| Frozen Rust/Cargo source, including nested qualification locks | 747 paths; SHA256 `d6a8e49f73a539a7f59ad2e116c265ed03b48e921e4ab1282a4aa2cca4fcf3b0`. |
| Isolated checkout / mounted Workspace target suffix | `/tmp/cellule-reader-prelease-8698183` / `cellule-reader-prelease-8698183`. |
| Published parent | `6a9443f097c69143f984c0e109b0454f3f3a4c84` |

Raw commands, frozen source archives, final logs and initial diagnostics are
retained in `/tmp/cellule-root-prefix-evidence`. Initial compilation caught a
missing export/import and test API errors. The memory-admission test needed a
`Duration` import. The first Clippy run rejected the new two-root error's large
payload; its exact details are now boxed. Full suites and gates were rerun on
the final frozen source. Initial runs do not qualify it.

Published parent `6a9443f` is mergeable and passes every enabled CI check,
including follower/object capacity, both routing modes and their aggregate,
Compose, workspace, MSRV, contract, website, fuzz and fast models. The earlier
unchanged reader-balance failure remains causally unexplained; passing repeats
establish no fix. Profiles, workloads, expected evidence and thresholds remain
unchanged. Fresh CI must qualify this new checkpoint.

### Remaining work streams

| Priority | Stream | Required delivery |
| --- | --- | --- |
| 1 | Complete observation and writer proof | Bind every original writer and sealed recovery suffix to its authenticated canonical backend, exact successor prefix/current native serving and the full revisioned fleet barrier. Consume complete writer, reader/follower replacement-policy and accepted-work observations in the existing reconciler. |
| 2 | Role evacuation and maintenance completion | Implement SettleRoles/Finalize with all original role proofs, join original accepted actions before terminal drain handoff, and confirm checked stop/withdrawal/boot retirement. Finish Cron/Blob external-owner integration and primitive fault matrices. |
| 3 | Movement resilience and convergence | Finish recovery/adoption across receiver sessions, refusal/unknown and source/receiver-failure reconciliation, ongoing intent supervision, sustained pressure/count convergence and concurrency evidence. |
| 4 | Canonical minion scenarios | Deliver complete runnable maintenance and receiver-loss scenarios through `crates/cellule-host/minion`; Cargo target remains `fleet_operations`. |
| 5 | W9 qualification | Run complete process/provider fault, mixed-binary compatibility, load/soak and capacity campaigns with their documented environments. |
| 6 | W10 operations | Exercise operator runbooks and collect staged rollout evidence. |

This checkpoint proves one per-movement prefix and availability observation. It
does not aggregate every original physical-boot writer or recovery suffix, prove
reader/follower replacement policy, pin storage, or complete fleet maintenance.
SettleRoles and Finalize remain refused. The full W1–W10 goal remains active.

## October 3 2026 canonical acquisition metadata checkpoint

The ordinary Idle acquisition, published takeover and rootless takeover now
retain the exact successful ownership-CAS input and claimed/materialized Control
before actor admission. The immutable `CellAcquisitionRecord` preserves pinned
recovery overlays and their canonical materialized positions across later
publication, release and compaction. Strict creation adopts only identical
committed data after a lost reply; competing different data refuses. The bounded
origin reader validates the typed Cell/incarnation/epoch path, version, canonical
Controls and their ordinary Takeover/PublishRecovery transitions.

This is acquisition metadata, not complete successor-prefix proof. It can
survive failed activation; it establishes no current serving, root-retention pin,
exact dependency availability or maintenance settlement. Initial bootstrap,
direct activation of already-claimed Controls, older binaries and cancellation
before retention can leave no record. Absence must block any proof requiring
that record. Published acquisition retains ordinary rollback; rootless failure
leaves its claimed Recovering Control available to the ordinary recovery path.
Existing control/LTX formats, paths and peer messages remain unchanged; the new
version 1 acquisition path is an additional persistence contract.

Eight unit cases cover canonical round trips, all truncations, malformed scope,
root/owner/epoch/code drift, immutable replay and conflicts, competing writers,
lost replies, source-read failure, cancellation before publication and corrupt or
oversized metadata. Two new public Idle cases exercise actual metadata failure,
rollback, actor non-admission, exact-root SQL after a lost reply and receiver
resource release using a dedicated disk budget. Existing genuine recovered-tail,
rootless takeover and prepared-receiver cases assert exact metadata; accepted
receiver work retains it after a dropped waiter and lost CAS/metadata replies.
The storage guide documents ordering, bounds, reconstruction and proof limits.

| Verification | Result |
| --- | --- |
| Full workspace, all features and locked dependencies | 1,595 passed; no failures; 36 documented cases ignored. Nested crash-fixture child summaries excluded. |
| Complete minion target, all features | 218 passed; no failures or ignored cases. |
| Distinct final passes | 1,813; focused repeats and the overlapping local LTX run excluded. |
| Local LTX, no default features | 54 passed; no failures or ignored cases. |
| Workspace check, all targets/features | Passed. |
| Workspace Clippy and API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer and diff | Passed; 122 Rust snippets parse. |
| Frozen Rust/Cargo source, including nested qualification locks | 740 paths; SHA256 `9aa13b336df94e30e4211720f30da368897dcbbf23507b20ce4a5937974d8475`. |
| Isolated checkout / target suffix | `/tmp/cellule-reader-prelease-8698183` / `cellule-reader-prelease-8698183` beneath the mounted Workspace target. |
| Parent revision | `e532964006d56a510f2663628356e9283f4d991a` |

Raw commands, initial diagnostics and logs are retained in
`/tmp/cellule-acquisition-history-evidence`.
The initial compile failed on two test-only API names; a later corrupt-object
fixture incorrectly used immutable Store publication to overwrite its original
object. The first public disk assertion used the process-wide default budget and
observed unrelated tests' charges. It now uses a dedicated receiver budget while
preserving all zero-resource assertions. Those initial runs are not passing
evidence.

Published parent `e532964006d56a510f2663628356e9283f4d991a` is mergeable; all
enabled CI checks pass, including follower/object capacity, both routing modes
and their aggregate, Compose, workspace, MSRV, contract, website, fuzz and fast
models. The earlier unchanged reader-balance failure remains causally unexplained;
passing repeats establish no fix. Minion remains the canonical executable source
at `crates/cellule-host/minion`, with Cargo target `fleet_operations`.

### Highest remaining priorities

Implement complete successor lineage, every original acknowledged prefix and
exact dependency availability, then fresh native serving. Higher sequence values
and historical acquisition metadata cannot substitute for that proof after
compaction. Consume complete writer, reader/follower replacement-policy and
accepted-work observations in the existing controller before `SettleRoles` or
`Finalize`. Join original action work before terminal drain handoff; finish
cross-session receiver recovery/adoption, Cron/Blob external owners, maintenance
and receiver-loss minion scenarios, W9 process/provider/mixed-binary/load gates and
W10 exercised runbooks/rollout. The full W1–W10 goal remains unfinished.

## October 3 2026 complete original writer inventory checkpoint

`FleetOriginalWriterCapture` joins the original failed boot through its existing
process provider, traverses every authenticated application/tenant catalog and
canonical owner history, and retains each full original Control and target.
Rootless, recovering and fully object-covered originals remain in the set even
when a later canonical takeover has removed the original owner. Complete catalog
entries with no initial Control and ownership epochs outside the original boot
remain explicit in the per-scope witness. Missing legacy/restored owner history
is a typed blocker.

`FleetOriginalCatalogs` is an application authentication boundary. Its stable
witness attests the original complete configuration and actual canonical backend
mappings; source construction validates shape only. Collection rereads that set,
revalidates all original catalog heads/ETags, joins the original process again and
rechecks the full boot/intent/registry/head/claimant barrier. At most 128 scopes,
10,000 total catalog entries and 10,000 inspected ownership epochs are permitted;
excess refuses without truncation. Original observations pack in canonical
64-row pages with the ordinary one-MiB envelope and a 64-KiB manifest.

`FleetOriginalWriterJournal` uses minion's existing accepted SQLite work owner.
First publication checks the full fresh barrier and commits all pages, the one
immutable operation/process pointer and one registry advance in the same
transaction. Exact replay returns original bytes and capture times, without
provider recollection or another registry advance. Different original inputs
conflict. Independent clients serialize through the same SQLite file; lost replies
and canceled waiters preserve the accepted durable commit.

The [integration recipe](../crates/cellule-host/docs/original-writers.md) records
ordering, bounds, source authentication and reconstruction. New codec kinds 24–26
use the existing version/domain; kinds 1–23 and all existing IDs, control formats,
object paths and signed messages are unchanged. Minion remains canonical, with
Cargo target `fleet_operations`.

Seven runtime cases cover full-Control round trips, all truncations and envelopes,
explicit authenticated empty sets, duplicate/reordered/missing/foreign pages,
scope/count/time/budget failures, root/code/schema binding and repeated original
epoch ordering across page boundaries. Eight minion cases retain 66 actual original
owner Controls across two canonical application/tenant catalogs and 68 entries,
with real sole-authority takeovers. They cover atomic replay/reconstruction,
provider causes, missing history, stale barriers, catalog/process changes,
independent competing clients, dropped waiters and lost commit replies.
Their child process is a lifetime stand-in, supplying no OS-crashed CellNode,
accepted external-job, successor-prefix or dependency-availability qualification.

| Verification | Result |
| --- | --- |
| Full workspace, all features and locked dependencies | 1,585 passed; no failures; 36 documented provider/process/performance/manual or documentation cases ignored. |
| Complete minion target, all features | 218 passed; no failures or ignored cases. |
| Distinct final passes | 1,803; focused repetitions and the nested LTX child-process result excluded. |
| Workspace Clippy, all targets/features | Passed with warnings denied. |
| Workspace API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer and diff | Passed. |
| Frozen Rust/Cargo source, including nested qualification locks | 737 paths; SHA256 `ef4ac129a38eac68f5b1244a01d1cf46b3617b9d1c72b37dfc55f248d8ba7eb8`. |
| Isolated checkout / target suffix | `/tmp/cellule-reader-prelease-8698183` / `cellule-reader-prelease-8698183` beneath the mounted Workspace target. |
| Raw commands, logs, initial diagnostics and manifests | `/tmp/cellule-original-writer-evidence` |
| Parent revision | `b0552ee3b60b8cd453844876d84a64eb535e63f2` |

The original report counted one nested LTX crash-fixture child result twice;
the corrected workspace and distinct counts above exclude that child summary.

Initial checks found a missing Digest import and typed IDs used as ordered keys;
byte-array keys preserve the existing ID contract. Test compilation then exposed a
missing test-only snapshot import. An initial stale-barrier test requested an
already-stopped scheduling state; it now changes the state and asserts the actual
registry advance before requiring refusal. All initial diagnostics remain
retained and provide no passing evidence.

### Parent CI and reader balance investigation

Parent `b0552ee` is mergeable. Both complete write-capacity modes, both routing
modes and their aggregate, Compose smoke, MSRV, contract, website, fuzz and fast
model checks passed. Rust workspace run `37112788891` failed in the unchanged
balanced three-process smoke at `process_replica.rs:193`: the twelve successful
replica reads were not evenly split within its unchanged one-read tolerance.
The failure log has no actual per-reader counts before that assertion.

An isolated container used the exact CI RustFS image digest and disposable
fixture credentials, a fresh bucket and unique object prefixes. The exact
process test passed once, followed by five same-binary repeats. These local
passes do not explain the CI failure and establish no fix. Ten further same-binary repeats during workspace verification also passed. These
contention reproduction logs and the original binary SHA256 remain with the evidence.
Qualification profiles, workload counts and acceptance thresholds are unchanged.
Fresh CI is required after publication; parent results do not certify new source.

### Highest remaining priorities

Verify every retained original acknowledged prefix and exact root dependencies,
then verify current successor authority and actor serving. Historical metadata
provides no root-retention pin or successor proof; sequential catalog heads are
not a global transaction. Integrate complete writer, reader/follower policy and
native accepted-work observations through the existing observer/controller.
`SettleRoles` and `Finalize` still refuse without their actual evidence. Join
original action work before terminal drain handoff, finish failed-receiver and
cross-session recovery/adoption, Cron/Blob external owners, runnable maintenance
and receiver-loss scenarios, and W9–W10 process/provider/mixed-binary/load
qualification, exercised runbooks and rollout. The full W1–W10 goal remains open.

## October 3 2026 complete catalog traversal checkpoint

`CellCatalog::scan_all(limit)` now captures all 256 original heads before reading
pages and streams through the ordinary verified shard reader. Opaque
`CatalogScanReceipt` requires observed end-of-stream, a successful cumulative row
bound and final checks of every populated or absent head, including revision,
locators and ETag. A read/verification failure prevents completion and preserves
its original error. Later `revalidate()` uses the same original scoped adapter
without replacing the captured set. The receipt exposes tenant/application,
entry count and each original revision/page-digest list. This path starts no task,
storage listing, authority mutation or second scheduler.

Eight new public cases cover empty heads, multi-page/multi-shard iteration,
independent adapters, tenant isolation, partial consumption, bounds before I/O,
cumulative failure, old immutable pages after provisioning, populated/absent
head changes, deletion, same-body ETag changes, missing pages, corrupt bytes and
later receipt invalidation. Catalog codecs, authority/history and routing
qualification bytes are unchanged. Minion remains canonical.

| Qualification | Result |
| --- | --- |
| Focused public catalog tests | 19 passed; no failures or ignored cases, including eight new complete-scan cases. |
| Full runtime public integration suite, all features and locked | 209 passed; no failures; four documented provider cases ignored. The 19 catalog passes are included. |
| Runtime Clippy, all targets/features | Passed with warnings denied. |
| Runtime API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer, diff | Passed. |
| Frozen Rust/Cargo source | 729 paths; SHA256 `b585bf9e802c1f843ab46a8c3419a9ef08811683a0694b8307da83703c1dcebb`. |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence | `/tmp/cellule-catalog-scan-evidence` |
| Parent revision | `8659f49fde2f46a2a5cf1121fa8f114ba0d92fae` |

Raw commands, logs, manifests and initial invocation diagnostics are retained.
An initial preparation invocation used the snapshot as its source, failed with
SameFileError and launched an old-source invocation. That invocation was deliberately
terminated with exit 143 and supplies no test evidence. The corrected frozen build
completed; process inspection during its slow mounted-target I/O is also retained.
These 209 distinct passes are not full workspace/minion, process/provider,
mixed-binary or load qualification for this new source.

Parent PR head `8659f49` is mergeable and every required CI check passed, including
both capacity modes, both routing modes, Compose smoke, Rust/MSRV, contract,
website, fuzz and fast model checks. The leased routing artifact's actual virtual
merge candidate is `522b7b2ffd835036319d81251e9aac5e3eef8811`, against baseline
`18a1244a98c50a04c52da21325f95331055e9884`. Independent replay verified frozen binary
and harness SHA256, all raw samples/counts/recovery proof and the exact 120-row
comparison. There are no gate failures; the prior failing uncached lane's median
throughput ratio is 1.108014.

Identical-binary calibration run `37110515841` passed both complete modes using
head `8659f49` as baseline and candidate. Independent replay verified identical
frozen binary hashes, all raw evidence and both exact comparisons. Leased uncached
throughput ratio is 0.962518; object-only is 0.974094. Leased uncached p99 generated
its retained 10% review alert (1.117907), below the unchanged 2.0 blocking limit.
The earlier parent's 89.669% throughput failure remains reproducible from its raw
measurements and causally unexplained. A passing repeat or four-pair calibration
cannot explain that earlier failure. Profiles and thresholds were not changed.
Parent CI does not qualify this new source; fresh CI is required after publication.

Highest priority next is authenticated complete application/tenant enumeration
and durable operation-bound aggregation of every original affected Cell, with
original process/accepted-work joining, followed by fresh successor prefix/serving
verification. Sequential catalog heads are not a global transaction, durable
object pin or complete physical-node proof. Role/controller integration, failed
receivers, original action joining before drain handoff, maintenance commands and
W9–W10 qualification/runbooks/rollout remain unfinished. SettleRoles/Finalize remain
blocked on their actual required evidence; the full W1–W10 goal stays active.

## October 3 2026 original owner history before authority departure checkpoint

`CellAuthority::transition` now durably retains the full original owner control
before the ordinary release, takeover or tombstone CAS can remove or replace it.
This covers unpublished, recovering and fully object-covered controls, including
their original incarnation, epoch, code/schema, root and recovery overlay. It
starts no second authority, scheduler or task bank. Same-owner publication and
renewal add no history write. Storage work remains in the original caller's
accepted lifetime; unresolved history publication prevents owner departure.

The new typed layout path is
`cells/v1/apps/<app>/cells/<cell>/owner-history/v1/<inc>/<epoch-hex16>.json`.
Its body uses the existing canonical control codec and 8 KiB bound. Competing
departure proposals advance history with ETag CAS. Delayed older proposals cannot
replace later observations. Equal or later original observations can reconcile
lost history replies; otherwise the original write error returns to the existing
coordinator for full predicate revalidation. A retained proposal proves no
departure CAS. The control codec, transition table, original authority implementation
apart from this retention call, and all 26 existing layout methods are byte-identical
to the parent. Existing persisted paths and signed message domains are unchanged.

`owner_observation` reads one exact retained epoch. Opaque `CellOwnerHistory`
collects every closed epoch in the current incarnation plus its current owner,
with caller row bounds and an exact authority recheck. Ordinary release closes
the same ownership epoch; tombstone adds a fence epoch without inventing an owner.
Missing legacy, restored or older-binary history returns the typed
`OwnerHistoryIncomplete` with Cell/incarnation/epoch. Foreign, malformed,
noncanonical, oversized, conflicting or changing observations fail closed.
Metadata grants no ownership, process joining, complete catalog scope, immutable
root pin, successor serving or maintenance completion. Immutable collection does
not delete the history metadata and does not pin its historical root graphs.

Thirteen new authority cases cover original rootless/recovering succession,
object-covered release/acquisition/tombstone, idle tombstones, same-owner work,
source errors, lost history and authority replies, delayed and cancelled proposals,
missing history, bounded/canonical/scope conflicts and authority progress during
collection. A new peer case rejects an empty/successful interpretation of missing
history. Existing public recovered-tail and unpublished-takeover cases now reopen
full original history through independent authority adapters after actual successor
serving. The typed path test covers the exact new version 1 layout.

| Final qualification | Result |
| --- | --- |
| Full workspace tests, all features and locked dependencies | 1,571 passed; no failures; 36 documented provider/process/performance/manual or documentation cases ignored. |
| Complete minion test target, all features | 210 passed; no failures or ignored cases. |
| Distinct final passes | 1,781; earlier focused/selected repetitions excluded. |
| Workspace Clippy, all targets/features | Passed with warnings denied. |
| Workspace API documentation | Passed with warnings denied. |
| Workspace all-target/all-feature compilation | Passed before the final test-only borrowed-slice cleanup; final Clippy compiled those targets again. |
| Format, boundaries, layout, Rust fences, links, SQL/peer, diff | Passed. |
| Frozen Rust/Cargo source | 728 paths; SHA256 `ee6a914c66822ee253f39b58493381070cc7af9fe167d5b4a8008c3228288e39`. |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-owner-history-evidence` |
| Parent revision | `de010246a8f32c368dfe1628b3ec940471b623dc` |

Raw commands, logs, build environment, source manifests and archives are retained.
Initial private-helper, exhaustive-error mapping and test-import compile diagnostics
are recorded. The selected pre-Clippy suites passed 1,053 runtime/host/executable
tests and two path cases; Clippy then rejected four unnecessary test clones. The
final assertions use borrowed slices with identical expectations. Production
validators, qualification profiles and thresholds were not weakened. Final full
workspace qualification uses the isolated snapshot. Ignored cases supply no evidence
for provider, full process, mixed-binary or fleet-load completion.

Parent CI passed follower/object capacity, workspace/MSRV, contract, website,
fuzz, Compose smoke and fast model checks. Its leased routing comparison failed
`forwarded_query_uncached_route/c16`: median throughput was 89.669% of baseline,
below the declared 90% minimum; read/hop counts and correctness matched. The exact
virtual merge candidate was `714b6c97ba121baaaf5c8ad3cb45e7abaa1bff2c`, compared with
`18a1244a98c50a04c52da21325f95331055e9884`. Raw logs and the frozen comparison artifact
are retained under this evidence directory. Its cause remains unproven; a later
passing run cannot explain this failure. Parent CI does not qualify this new source.

Highest priorities next are diagnosing that measured routing regression, then
authenticated complete catalog traversal and durable operation-bound aggregation
of original owner history with original boot/process joining. Fresh successor
prefix/serving verification remains required. Per-Cell history does not establish
the original physical-node set. Receiver failure adoption, complete role/controller
integration, original action joining before drain handoff, remaining primitive
faults, complete maintenance commands and W9–W10 qualification/runbooks/rollout
remain unfinished. SettleRoles/Finalize stay blocked on their required evidence;
the full W1–W10 plan remains active. Minion remains canonical.

## October 3 2026 original process confirmation before recovery checkpoint

`NodeDirectory::fenced_session` reads the immutable original physical boot fence
while its log still needs recovery. `closed_session` continues to require a
canonically Retired log. Neither observation proves process termination or Cell
relocation. This distinction permits original process and accepted-work joining
before retaining affected Cells and starting dependent recovery effects.

`FleetFailedBootProcessRequest::capture_fenced` binds the original Established
boot and permanent fence in a version 2 request identity. Recovery phase,
manifest, claim adoption and observation times cannot change that identity.
`confirm` uses the existing application process provider twice, checking the
complete current roster, original boot, permanent fence and live claimant around
the reads. It preserves provider errors and rejects changed evidence, stale
barriers and nonmonotonic or excessive intervals. The provider owns authentication,
durable joining of the original process and all accepted native/external work,
and session nonreuse; a PID observation alone is insufficient.

`FleetFailedBootRetirement::capture_retained` reuses that original request after
related roles close and the native log becomes Retired. Its fresh terminal
capture and publication rechecks do not restamp the original request interval.
The existing terminal version 1 request and retirement event encoders remain
byte-identical. Version 1 and version 2 requests are distinct: restart must retain
the original basis and witness, and cannot silently convert a published event.
The public request `canonical()` getter now returns an optional original terminal
observation; `fence()` is always available. Local callers and documentation use
the updated API. This is a source API change, not a persisted format rewrite.

Seven new executable cases cover actual child joining before canonical recovery,
stable identity through sealing and retirement, independent adapter reconstruction,
lost committed replies, a still-running original child, original provider errors,
changed witnesses, suspended-provider registry races, log progress, foreign request
evidence and regressing clocks. One new runtime case distinguishes early fencing
from strict closure through claim adoption and recovery progress. The child is a
process-lifetime stand-in in a cold follower ensemble: these cases do not qualify
an OS-crashed CellNode, an original Cell tail or external job joining.

| Final selected qualification | Result |
| --- | --- |
| Runtime library | 545 passed; three existing provider cases ignored. |
| Runtime lifecycle | 144 passed; four existing provider cases ignored. |
| Host library / public node / minion | 34 / 106 / 210 passed. |
| Distinct selected tests | 1,039 passed; no failures; focused repeats excluded. |
| Runtime/host Clippy, all targets/features | Passed with warnings denied. |
| Runtime/host API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer, diff | Passed. |
| Frozen Rust/Cargo source | 726 paths; SHA256 `dbd6094dbcec4a15eed39a667c7214c7dac52d751b71dd49e1875041464c5f31`. |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-fenced-process-evidence` |
| Parent revision | `8e4976e4893edde7658cccb208305f033712e340` |

Commands, build environment, raw logs, complete source manifests and the source
archive are retained. The first focused test run had 14 passes and one incorrect
fixture error-class assertion; the complete-roster barrier correctly returned its
original conflict error. The final assertion checks that exact error. Initial
syntax and compatibility-audit script diagnostics are also retained. Production
validators and qualification requirements were not weakened. Seven ignored cases
require their documented isolated provider environment. These selected suites do
not establish full workspace, provider, mixed-binary or measured load qualification.

Highest priority next is operation-bound retention of every original affected Cell
before recovery/relocation effects, including object-covered, unpublished and
transitional writers, followed by fresh current successor prefix and serving
verification. A scan after takeover cannot reconstruct the original ownership set.
Process confirmation supplies no complete Cell inventory. SettleRoles/Finalize
remain blocked on that evidence and complete authenticated observation. Receiver
failure adoption, reader/follower controller integration, original action joining
before drain handoff, remaining primitive faults, complete maintenance commands
and W9–W10 qualification/runbooks/rollout remain unfinished. The full W1–W10 plan
stays active. `crates/cellule-host/minion` remains the user-approved canonical
executable location; the Cargo target stays `fleet_operations`.

## October 2 2026 complete recovered-suffix metadata checkpoint

`RecoveryManifestStore::load_manifest` exposes every original recovered scope in
one digest-verified immutable manifest, including other applications. Its bounded
read shares the overlay loader's canonical decoder, digest and original leader/epoch
checks. Publication and both readers share one conversion to control-ready rows.
The decoder now rejects duplicate or reordered full scopes; existing producers
already sort and reject duplicate scopes. Version 1 bytes, object paths and signing
domains are unchanged. Original storage errors remain available to the caller.

The opaque `RecoveryManifestInventory` retains original application, Cell,
incarnation, epoch, predecessor and final prefix references after successors clear
their overlay pointers. Reading it starts no recovery, authority mutation, native
admission or task owner. It verifies metadata, not bundle availability or current
successor state. Object-covered Cells without a recovered suffix are absent; an
empty suffix has no manifest. The full original writer set must still be retained
before dependent effects, and every successor must be verified separately. This
checkpoint cannot settle roles or finalize a failed node.

Seven new unit cases cover independent adapter reconstruction, three genuine
recovered scopes across two applications, ordinary bundle reopening, unavailable
bundles, original storage errors, invalid scope/digest/path, duplicate/reordered
rows, noncanonical or malformed metadata and bounded reads. The public takeover
case reads the canonical sealed identity before materialization and reopens the
same original metadata after actual successor serving. Existing lost-observation,
object-covered and unpublished-owner cases remain intact. The follower guide now
shows the actual version 1 persisted schema and the public read recipe.

| Final selected qualification | Result |
| --- | --- |
| All-feature manifest library cases | 14 passed; no failures or ignored cases. |
| All-feature public recovery cases | Seven passed; no failures; one existing RustFS case ignored without its documented isolated environment. |
| Runtime/host Clippy, all targets/features | Passed with warnings denied. |
| Runtime/host API documentation | Passed with warnings denied. |
| Format, boundaries, layout, Rust fences, links, SQL/peer, diff | Passed. |
| Frozen Rust/Cargo source | 722 paths; SHA256 `f4a4aae3d86025bdbb5321c92bf5dbfc82593513c668efb8cba371952a818a85`. |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-recovery-inventory-evidence` |
| Parent revision | `2a155238311fa1bfc61ffb07f136dbb9a49b6af1` |

These 21 selected passes do not represent a full workspace rerun or provider,
process, external-job, mixed-binary or load qualification. Commands, raw logs,
source manifests and build environment are retained. The initial static command
attempted Git's diff check in the non-Git snapshot; that invocation failed and the
proper active-worktree diff check passed. No Rust failure or weakened expectation
was involved. Parent CI has passed both follower/object capacity, workspace, MSRV,
contract, website, fuzz and the fast model checks; Compose was still running when
inspected. Those parent results do not qualify this new source.

Highest priority remains operation-bound retention of every original affected Cell
before recovery/relocation effects, including fully object-covered and transitional
writers, followed by fresh current successor/prefix/serving and failed-process
proof. Complete authenticated production observation, receiver-session failure
adoption, reader/follower controller actions and original action join before drain
handoff remain required. SettleRoles/Finalize, remaining primitive faults, complete
maintenance/receiver-loss commands and W9–W10 qualification/runbooks/rollout remain
unfinished. `crates/cellule-host/minion` is the user-approved canonical executable
location; the Cargo target stays `fleet_operations`. The complete plan stays active.

## October 2 2026 durable live-owner follower replacement checkpoint

`FollowerEvacuationRecord` now retains the complete original Retired ensemble,
the original Established donor digest, object-covered retirement watermark,
signed source boot, full capture head/registry, maintenance operation and interval.
Every replacement binds its exact Established request and signed boot identity.
The bounded codecs support the canonical one/two-member ensembles; existing
record kinds and signing domains remain unchanged. Shape validation supplies no
native retirement or authentication rights.

`FleetFollowerEvacuationJournal` stores revisioned application redundancy policy,
immutable captures and the latest operation/request pointer in the existing
registry transaction domain. Policy absence blocks publication. First publication
checks the full barrier, live controller, current operation/intent, policy and
complete original/replacement rows before advancing the registry. Exact replay
returns history without restamping, advancing the registry or restoring an older
pointer. The SQLite adapter retains accepted work in its original finite backend
owner through cancelled waiters and lost replies.

`FleetFollowerEvacuationVerifier` reloads current policy and complete roster,
checks the current canonical ensemble and pinned signed boots, and collects
complete source/receiver inventories through `FleetSnapshotTransport`. Requests
use the existing finite native snapshot owner, including its 32-epoch enrollment
page limit. All categories are rechecked after authority discovery. A native
source fence cannot hide behind a still-live directory record. Installed epochs
before their first append need the actual source binding and delivered producer;
inactive directory enrollment alone supplies no installation proof.

Refresh during Evacuating or Closing retains the original retirement, including
after later rotations evict its original local receipt. It observes the current
ensemble and policy without starting another rotation, retirement or recovery.
Publication and fresh confirmation preserve original errors independently;
capture clocks are monotonic and bounded by thirty seconds and caller/operation
deadlines. Copied collector buffers remain the application's accounting duty.
Failed original owners require canonical recovery and affected-Cell successor
evidence; this live-owner record cannot settle every role or finalize a node.

Twelve public cases use four actual managed CellNodes, signed canonical boots,
native follower stores, an acknowledged SQLite Cell and independent journal
clients. They cover full ensemble publication, policy CAS races, cancellation,
lost replies/source errors, policy changes during publication, superseded pointer
replay, missing history/stale barriers, regressing clocks, withdrawn receivers,
original leader shutdown, local fencing, Closing/deadline refresh and a third
rotation after original receipt eviction. Shutdown joins original owners and
checks empty native resource ledgers. Five codec cases cover bounded envelopes,
complete row identities, policy/scope/time faults and operation adoption.

Initial compile, directory-only, snapshot-limit and codec-fixture failures remain
under `initial/`. The codec fixture now uses the required maintenance transitions
and drain evidence; production validators and qualification requirements are
unchanged. One unchanged queued-query lifecycle case hit its outer seven-second
waiter timeout during final qualification. Its standalone and full lifecycle
reruns passed with the same assertions; the raw failure remains under
`initial/minion-queued-timeout/`. The executable was relocated to
`crates/cellule-host/minion` during verification. Final qualification uses that
canonical location; its Cargo example target remains `fleet_operations`.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `72c5d3a704b69cc1cbc8d3736652dffbceb7dd7d` |
| Frozen source | 721 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `5a5c48f26dfece662f4abaccadc3bdbbca1dc77ab995c13f05774181a9a26864` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-follower-policy-evidence` |

Final all-feature qualification passed 537 runtime library, 144 runtime lifecycle,
34 host library, 106 public node and 203 executable tests: 1,024 distinct passes,
no failures. Seven existing provider cases remain ignored without their documented
isolated RustFS environment and supply no evidence. Focused repeats are excluded.
Runtime/host Clippy across all targets and API documentation passed with warnings
denied. Complete active/isolated Rust/Cargo path sets and bytes match qualification.
Commands, statuses and build environment remain in `verification.json` and
`build-environment.json`; format, boundaries, layout, documentation links/Rust
fences, SQL/peer and diff gates passed. These selected in-process tests supply no
OS-crash, external-job, provider-deployment, mixed-binary or measured fleet-load
qualification.

Highest priorities next are failed-owner replacement/successor evidence, complete
production observation and reader/follower controller integration. Receiver-session
recovery, failed Pending producers and live-receiver joining, affected writer
relocation and SettleRoles/Finalize remain open. Terminal drain handoff must join
the original fleet action before entering the existing drain owner. Remaining
primitive faults, maintenance/receiver-loss executable scenarios and W9–W10
process/provider/mixed-version/load/runbook/rollout qualification remain required.
The complete fleet operations plan remains unfinished.

## October 2 2026 durable reader replacement evidence checkpoint

Reader evacuation now produces immutable operation-bound manifests and ordered
replacement pages. The full 10,000-reader policy bound fits 79 pages, each with
at most 128 entries and the existing 64-KiB envelope. The manifest retains the
full original head digest and bootstrapped registry version, exact original
Established row digest and Retired history, serving authority, policy revision
or absence, required prefix and capture interval. Pages bind the complete capture
basis, ordinal, signed boot identities, exact Established replacement requests
and observed native prefixes. Complete validation rejects missing/reordered/foreign
pages, duplicate physical nodes/sessions/request keys and regressed prefixes.
Existing persisted record kinds and domains remain unchanged.

`FleetReaderEvacuationJournal` commits all pages, the manifest and its latest
per-operation/original-request pointer in the existing journal transaction domain.
First publication compares the full snapshot, live controller, current operation,
original retirement and active replacement rows/intents, then advances the shared
registry. Exact replay returns immutable history without advancing the registry
or restoring a superseded pointer. The reference SQLite implementation uses its
existing finite backend owner and immediate transaction; accepted writes survive
caller cancellation. No new task bank, authority or native closing path is added.

`FleetReaderEvacuationVerifier` loads every page and the latest pointer after
adapter reconstruction. It checks complete roster barriers, operation/intent,
current source authority and read policy, exact selected signed boots/enrollment
rows and ready native prefixes twice. Its separate fresh interval does not
restamp history. Policy, owner, boot or operation adoption can produce a refreshed
candidate from the same original native retirement through the ordinary current
recruitment path. Evacuating and Closing phases both permit this fresh metadata;
refresh starts no native opening or closure. Publication preserves durable history
and original shared errors independently of a failed final confirmation.
Native capture and fresh verification are bounded by monotonic thirty-second
intervals and the caller/operation deadlines. Historical records grant no complete
role settlement or terminal node finalization.

Eight public example cases use actual managed CellNode readers, SQLite and the
existing signed directory/native peer path. They cover immutable page publication,
independent journal reconstruction, lost replies and cancelled waiters, policy
changes during publication, zero-reader refresh, superseded-pointer replay,
unavailable replacements, corrupted pages/stale barriers, suspended probes across
source authority change and refresh during Closing. Fixture shutdown joins the
original owners and checks zero native resource ledgers. Seven runtime codec cases
cover the full policy bound, malformed envelopes and scope/lifetime/history rules.
These establish selected in-process behavior; they supply no OS-crash, external-job,
provider-deployment, mixed-binary or measured fleet-load qualification.

The initial reply-pause fixture was not wired into the post-commit response and
three cases failed to reach their intended race. That raw failure remains under
`initial/`. Qualification below uses the corrected final source. An empty directory
left from an earlier isolated module relocation caused the first layout check to
fail; removing that directory changes no Rust source or compiled path set.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `92313e8293ecc76d525cf3659f837af0f014217a` |
| Frozen source | 708 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `eaa4010b17ff44ce37f62c0236320ae445e17cef8c297d233f2831077ad74066` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-reader-policy-evidence` |

Final all-feature qualification passed 532 runtime library, 144 runtime lifecycle,
34 host library, 106 public node and 191 example tests: 1,007 distinct passes, no
failures. Seven existing provider cases remain ignored without their documented
isolated RustFS environment and supply no evidence. Focused repeats are excluded.
Runtime/host Clippy across all targets and API documentation passed with warnings
denied. Complete active/isolated Rust/Cargo path sets and every byte match final
qualification. Commands/statuses and build environment are retained in
`verification.json` and `build-environment.json`. Format, dependency boundaries,
module layout, document links/Rust fences, SQL/peer and diff gates passed. The
host recipe and example guide describe the public APIs and their proof scope.

Highest priorities next are durable follower replacement-policy evidence and
revalidation, complete production observation and reader/follower controller
integration. Failed receiver/source/Pending producer reconciliation, affected
writer relocation and SettleRoles/Finalize still require complete barriers and
the original action join before terminal drain handoff. Remaining primitive faults,
maintenance/receiver-loss executable scenarios and W9–W10 process/provider,
mixed-version, load, runbook and rollout qualification remain open. The complete
fleet operations plan remains unfinished.

## October 2 2026 failed receiver reader closure checkpoint

`FleetFailedBootProcessRequest::capture` now exposes the original Established
boot and permanent canonical session fence at a complete bootstrapped roster
while role requests remain unresolved. It starts no native/process effect and
permits only provider confirmation. Boot retirement still requires all related
requests settled and complete physical follower-reference checks. Snapshot,
mutable boot status and collection times are metadata; the original request
digest remains unchanged across role publication, recapture and reconstruction.
Existing boot publication shares the same canonical/process confirmation path.

`FleetFailedReaderRetirement` binds one original Pending, Established or replayed
Retired request to its exact receiver physical node/session and original boot.
Source failure cannot retire a view on a live receiver. The existing application
process provider must authenticate and durably retain original termination or
nonexecution, join all accepted native/external work and producers, and exclude
session reuse. Expiry, native absence, recovery or another lifetime cannot prove
it. The capsule creates no drain task, recovery path or second action bank.

Publication confirms the entire original journal barrier again after the provider
read, then publishes through the existing enrollment journal. Its deterministic
event binds original full spec/acceptance and immutable process identity/witness;
legitimate establishment completion cannot change that event. Returned history,
committed rows and original shared source failures remain independently inspectable
when a reply or final read fails. Fresh full roster, original boot/reader, permanent
canonical receiver fence and unchanged process evidence confirm within the original
monotonic thirty-second interval. Recapture adopts lost replies without refreshing
original timestamps. Journal/backend owners join accepted work after cancellation.

Ten public example cases use two actual canonical SQLite readers with application-
owned Pending acceptance before native activation, an actual receiver CellNode,
joined shutdown, retained reader clones and the durable SQLite journal. One native
opening retains an unresolved establishment result. Original lifetime evidence is
stored only after shutdown, local joining, rejected cloned-view queries and zero
native resource ledgers. The source retains readable acknowledged data. Cases cover
running-original refusal, failed-source/live-receiver refusal, exact original process,
payload and acceptance comparison, Pending/Established history, boot closure ordering,
replay, independent adapter reconstruction, lost publication replies, cancelled waiters,
suspended provider reads, stale barriers, regressing/expired capture clocks and failed
or changed final process evidence. Original process identity survives role publication.

The first eight-case run found a fixture expecting Fenced after the entire runtime
had closed; canonical query admission correctly returns RuntimeClosed first. The
fixture now checks that exact native outcome together with actual lifetime joining.
That raw failure and source manifest remain under `initial/`; qualification below
uses the corrected final source. These are in-process native lifetime cases with no
external-job workload, not OS-crash or provider-deployment qualification.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `a9cee96cda6d944dad6e5711a0ac8352baa1f6e9` |
| Frozen source | 696 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `dda6c960a80859e941de7d407f28d74de56c84cd73711af0e7bb6d81cf33e568` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-failed-reader-evidence` |

Final all-feature qualification passed 525 runtime library, 144 runtime lifecycle,
34 host library, 106 public node and 183 example tests: 992 distinct passes, no
failures. Seven existing provider cases remain ignored without their documented
isolated RustFS environment and supply no evidence. Focused repeats are excluded.
Runtime/host Clippy across all targets and API documentation passed with warnings
denied. Complete active/isolated Rust/Cargo path sets and every byte match final
qualification. Commands/statuses and retained build environment are in
`verification.json` and `build-environment.json`. Format, dependency boundaries,
module layout, document links/Rust fences and SQL/peer gates passed; the public
host recipe and example command document the exact scope.

Highest priorities next are persisted reader/follower replacement-policy evidence
and revalidation, complete production role observation and controller integration.
A failed source with a live receiver still needs ordinary joined reader closure;
failed owner/Pending producer reconciliation remains required. Affected-writer
relocation, SettleRoles/Finalize and the original action join before terminal drain
handoff remain open, followed by remaining primitive faults, complete maintenance/
receiver-loss executable scenarios and W9–W10 process/provider/mixed-version/load/
runbook qualification. This closes individual failed receiver reader enrollments;
the complete physical maintenance operation and full plan remain unfinished.

## October 2 2026 original failed boot closure checkpoint

`NodeDirectory::closed_session` now exposes a fresh opaque permanent fence for
an exact physical node/session, with no leader log or its exact Retired log.
Missing/live/expired advertisements and Open/Recovering/Sealed logs refuse,
including inactive enrolled logs. The original fencing/expiry and terminal
ensemble/manifest survive claim renewal. No record, wire or persistence codec
changes; this canonical read starts no retirement or process effect.

`FleetFailedBootRetirement` binds that fence to the complete bootstrapped roster
and the original Established request/history. Missing/duplicate boot requests,
unresolved original reader/follower/Pending rows and contradictory live foreign
references prevent capture. A new boot may retain other registered foreign epochs
on the same physical node; old closure cannot retire or substitute that role.
`FleetFailedBootProcesses` is a read-only application boundary for authenticated
durable original process/nonexecution evidence. Providers must join the process
and its accepted external jobs/producers, exclude reuse of the same session and
retain the same original witness across reconstruction. Expiry, takeover,
missing inventory, recovery success and a replacement process cannot supply it.
The evidence constructor validates shape, never provider authentication.

Publication rechecks the entire original journal barrier after the provider
read, then uses the existing enrollment journal. A committed row and its
original shared source failure remain inspectable when a publication reply or
final check fails. Fresh complete roster, exact returned original history,
canonical authority, physical references and the same durable process witness
must confirm within the original monotonic thirty-second interval. Fresh
recapture adopts original times after cancellation/lost replies or controller
restart. The application/adapter retains and joins accepted backend work;
this capsule creates no task, kill/drain operation or second action bank.
Active intent rebinding now documents either checked planned withdrawal or
complete failed-boot/process/role closure as the old-session prerequisite.

One runtime case covers missing/live/expired, exact physical identity (distinct
from SessionId), claimant expiry and immutable original closure through claim
renewal. Eight Unix example cases combine actual cold canonical recovery and
two-member retirement, the SQLite journal and a real child lifetime. They cover
running-original refusal, kill/wait before durable evidence, independent adapter
reconstruction, replay without timestamp refresh, lost retirement reply with
original source/Arc retention, cancellation followed by backend joining,
unretired logs, unresolved roles, duplicate requests, foreign process witnesses,
regressed clocks, stale snapshots, final process error/changed evidence, delayed
original-session responsibility and a new boot's foreign role on the same node.
The child is a lifetime stand-in, not a CellNode process or external-job workload;
those fixtures do not supply multi-process/provider deployment qualification.
The existing real acknowledged-tail recovery case remains in the lifecycle suite.

The first runtime-focused case passed. Host compilation then exhausted the
mounted Workspace volume before running any host case. That source manifest and
raw build failure remain in `disk-full/`. Only this task's generated isolated
target artifacts were removed. Final qualification disables incremental build
storage and debug symbols, preserving all features, test assertions, workloads
and named qualification profiles. Its environment is retained separately.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `6fba32403c13f3a9ecf8444cf1763990bedc4524` |
| Frozen source | 688 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `a1cab1a03e91c77a97df6cf8a3407d2162125b205233af7a87b857874368dfda` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-failed-boot-evidence` |

Final all-feature qualification passed 525 runtime library, 144 runtime lifecycle,
34 host library, 106 public node and 173 example tests: 982 passes, no failures.
Seven existing provider cases remain ignored without their documented isolated
RustFS environment and supply no evidence. Earlier/focused repeats are excluded.
Runtime/host Clippy across all targets and API documentation passed with warnings
denied. Format, boundaries, module layout, document links/Rust fences and SQL/peer
gates passed. Complete active/isolated Rust/Cargo path sets and every byte match
qualification. Commands/statuses and the build environment are retained in
`verification.json` and `build-environment.json`; guides include the public recipe
and focused command.

Highest priorities next are failed-reader/process integration, persisted
replacement-policy revalidation and complete production role observation.
Affected-writer relocation, `SettleRoles`/`Finalize`, the original action's join
before terminal drain handoff, remaining primitive faults, four executable
scenarios and W9–W10 provider/compatibility/load/runbook qualification remain
required. This closes one original boot enrollment under explicit application
process evidence, not a physical maintenance operation. The full plan stays active.

## October 2 2026 recovered follower enrollment publication checkpoint

`FleetRecoveredFollowerRetirement` binds a fresh canonically Retired epoch to
its complete original member requests in the durable roster. The receiver-side
runtime authorization now exposes the original physical leader from the exact
tombstone; a session-derived NodeId cannot replace it. Capture requires the
bootstrapped full journal barrier, exact leader/epoch/ensemble/manifest, uniform
original source endpoint and one non-refused request per original member.
Missing, duplicate, foreign or differently settled requests refuse the capsule.

Publication uses the existing enrollment journal and deterministic events bound
to immutable specs and first acceptance. It preserves establishment history and
original retirement timestamps across fresh recapture after a lost reply. All
member waiters settle with separate retained errors; the adapter continues to
own accepted backend work after cancellation/deadline. Successful writes remain
observable when a sibling or the final check fails. Confirmed closure requires
the complete post-publication roster, unchanged original member records, fresh
canonical authority and a non-regressing interval of at most thirty seconds.
The API starts no native RPC, recovery path, task or second finite action bank.

Seven public example cases use actual signed boot enrollment, a cold two-member
native ensemble, canonical recovery/sealing/retirement and the SQLite journal.
They cover Pending/Established originals, independent-client reconstruction after
native collection, stable duplicate times/digests, lost publication reply and
original I/O source/Arc retention, cancellation followed by backend joining,
unretired authority, stale barriers, expired claimants, duplicate requests,
clock regression and a delayed new request at the final roster barrier.
The failed leader's boot remains an unresolved obligation after follower closure.
These cold-lane fixtures establish no Cell suffix pinning or failed-process proof;
the existing real lost-acknowledgement runtime recovery case remains in the
qualified lifecycle suite.

Initial compilation found fixture PathBuf ownership and an incorrect helper
name. The first executable run also found a test that inspected facility display
text instead of its original I/O source. Those fixtures now use the public
journal API and check the actual source type/message. The lost-reply assertion
tracks the actual member selected by concurrent completion. Earlier manifests
and raw failure/focused logs remain separate from final qualification.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `db339147f7308ce11c3f599ccea0fbe4ddd6939a` |
| Frozen source | 681 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `696bb5d9801515b1585c7ce7ff30bdc853b921a112b8ffee6fe384b65772aad8` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-recovered-enrollment-evidence` |

Final all-feature qualification passed 524 runtime library, 144 runtime lifecycle,
34 host library, 106 public node and 165 example tests: 973 passes, no failures.
Seven existing provider cases remain ignored without their documented isolated
RustFS environment and supply no evidence. Focused repeats are excluded from
these counts. Runtime/host Clippy across all targets and API documentation passed
with warnings denied. Format, boundaries, module layout, document links/Rust
fences and SQL/peer gates passed. The complete active/isolated Rust/Cargo path
sets and every byte match after qualification. All command arguments and exit
statuses are retained in `verification.json`; the host guide contains the public
publication recipe and the example guide supplies its focused command.

Highest priorities for the next increment are failed-boot/process closure,
persisted replacement-policy revalidation and consumption through complete role
observation/maintenance actions. Affected-writer relocation, `SettleRoles`/
`Finalize`, original-action joining before terminal drain handoff, remaining
primitive faults and W8–W10 remain required. This closure covers only the
original follower enrollment rows; the full implementation plan stays active.

## October 2 2026 recovered follower retirement checkpoint

Recovered failed-owner tails now close through an explicit capability of the
existing node-log transport. Each receiver authenticates a live requester and
rechecks the exact canonical Sealed/Retired epoch, original member and pinned
manifest. The native store accepts no caller-selected watermark. It uses the
existing lane lock, byte ledger, verified scanner and durable retirement marker;
active lanes require their exact original seal. Inactive enrollment only fences
empty lanes, refusing unexpected records even when they have a native seal.

All original member calls join and retain their individual responses/errors.
Incomplete or contradictory receipts cannot produce the opaque confirmation
required for the canonical Retired CAS. The tombstone preserves the epoch,
ensemble and recovery manifest. Takeover and original recovery completion remain
valid. Exact canonical retirement can be adopted after controller restart or
local grace collection, without inventing missing member receipts. Grace-aged
collection still requires the unchanged native marker and external authority.

Three unit cases cover live/Recovering and foreign/expired authorization,
unsealed and corrupt-seal refusal, different original prefixes, lost member
replies, contradictory receipts, durable fences, exact replay, native collection
and inactive unexpected records. Their authority fixtures do not establish
full recovery pinning. The real lost-acknowledgement lifecycle case does: it pins
the recovery overlay, retires the suffix, loses the committed retirement CAS
reply, adopts its canonical result, then restores the successor and verifies the
original command outcome without re-execution. Earlier qualification logs are
retained separately before the seal and inactive-lane refinements.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `af40830ebf495e348235fcdc016626844986b99a` |
| Frozen source | 676 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `7f70f0f5930eacc13553247c3a543f2fc1970abad8c1accaacf8b3ccbfe7fffb` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-recovered-retirement-evidence` |

Final all-feature qualification passed 524 runtime library, 144 runtime lifecycle,
34 host library, 106 public node and 158 example tests: 966 passes, no failures.
The runtime suites ignored seven existing provider cases whose required isolated
RustFS environment was not supplied; they provide no evidence here. Focused
repeats are excluded from these counts. Runtime/host Clippy across all targets
and API documentation passed with warnings denied. Format, boundaries, module
layout, document links/Rust fences and SQL/peer gates passed. The complete
active/isolated Rust/Cargo path sets and every byte match after qualification.
All command arguments and exit statuses are retained in `verification.json`.

This establishes runtime tail closure, not failed-process joining, replacement
policy or fleet finalization. Durable publication against every original member
request, failed-boot closure, complete production observation, affected-writer
relocation, `SettleRoles`/`Finalize`, terminal action handoff, remaining primitive
faults and W8–W10 remain required. The full implementation plan stays active.

## October 2 2026 retained role observation checkpoint

`FleetObservation::with_role_coverage` retains the original checked graph and
binds its digest to canonical planner inputs. Attachment rejects a second graph,
scope/registry mismatches and intervals outside the original observation.
After confirming the journal, the reconciler compares the graph's exact full
head, registry and roster digest before planning. A controller renewal invalidates
an earlier graph even when enrollment and intent revisions are unchanged.
The reference observer now retains its graph through this exported path.
Partial adapter coverage remains partial after attachment.

Three public cases use actual managed follower boots, native collectors and the
durable journal. They verify retained original metadata and rejected replacement
or restamping, an earlier head with unchanged registry, and complete role graph
attachment to a deliberately partial adapter observation. The latter runs the
exported driver, retains IncompleteObservation and allocates no count movement.
The stale-head case checks that no effect or inspection was dispatched and no
attempt was allocated. All fixtures join shutdown and exact boot/member retirement.

Initial runs found two fixture mistakes: the registry advance API requires its
expected revision, and managed fixtures deliberately keep scheduling disabled.
The driver cases now explicitly enable reference scheduling before collecting
their evidence. Production checks and expected barriers remain unchanged.
Earlier compile/failure logs are retained separately from final qualification.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `8a43d80aafbc581e39e5fd3e13b355c7de3099dc` |
| Frozen source | 673 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `28331425f9220b9e84ecf665e6bc8fb35580c6c2baa6a131e54f43b60a46f7a8` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-retained-role-evidence` |

Final qualification passed 34 host library, 106 public node and 158 example
cases: 298 distinct tests, no failures or ignores. Host Clippy and host/runtime
API documentation passed with warnings denied. The complete active/isolated
Rust/Cargo path sets and every byte match after qualification. Focused repeats
are excluded from the final count.

This retains one checked input barrier; it does not establish complete production
observation or maintenance completion. Replacement-policy publication and
revalidation, failed-process closure, dead-owner recovery, affected-writer
relocation, `SettleRoles`/`Finalize`, terminal action handoff, remaining primitive
faults and W8–W10 remain required. The full implementation plan stays active.

## October 2 2026 cross-node role coverage checkpoint

`FleetRoleCoverage` checks every required original native boot and every
retained physical node's complete follower reference scan. All initial captures
precede the global native round; every exact foreign recheck follows that entire
round. Missing or duplicate inputs, regressed/stale intervals and incomplete
rechecks are refused. Beginning a new native or foreign recheck invalidates its
earlier confirmation, even when the new attempt fails or is dropped.

An Established follower can have no persisted lane until its first append.
Coverage matches that obligation to the delivered original managed producer,
installed source binding, original Established member records and matching
current Open authority on every ensemble member. The local enrollment check
remains strict without this combined witness. Failed/unobserved owners and
quarantined receiver stores remain blockers. The canonical input digest binds
the original roster, all native fingerprints, exact foreign rows and actual
collection/recheck intervals. The reference writer observer consumes this check
before its final full journal confirmation.

Six public cases exercise ordered global rounds, missing/duplicate inputs,
order-independent digests, dropped native rechecks, failed foreign rechecks,
actual enrolled empty lanes after four-node rotation and changed full rosters.
They retain original rows/timestamps and join real managed fixture shutdown
with exact retirement counts. The earlier five-case focused run is retained
separately from final-source qualification.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `7b6df307d6730826fd6609656a7d92372a7840c1` |
| Frozen source | 672 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `c75db3afe1fa91164b5d6e363030c37d0f713288d8410365a128382d99a277d7` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-role-coverage-evidence` |

Final qualification passed 34 host library, 106 public node and 155 example
cases: 295 distinct tests, no failures or ignores. Host Clippy and host/runtime
API documentation passed with warnings denied. The complete active/isolated
Rust/Cargo path sets and every byte match after qualification. Focused repeats
are excluded from the final count.

This supplies combined role coverage, not settled roles or shutdown permission.
Authentication, unexpected advertisement discovery, current Cell authority,
replacement-policy publication/revalidation and failed-process closure remain
required. Pending work remains an obligation. Complete observer integration,
dead-owner recovery, affected-writer relocation, `SettleRoles`/`Finalize`, terminal
action handoff, remaining primitive faults and W8–W10 remain unfinished.

## October 2 2026 live-owner follower evacuation checkpoint

`CellNode::follower_evacuation` checks the original requested rotation against
the current Evacuating operation and full durable roster. It preserves the
original completion Arc and retirement timestamps, requires every old member's
Retired row, and verifies the complete newer ensemble outside the donor against
an explicit application member minimum. The returned evidence retains original
signed replacement boots, current Open authority, Established member rows,
exact intent revisions and the checked head/registry. Native owner readiness,
admission mode, binding and actual lease are rechecked alongside directory
authority and member boots. Temporary and retained metadata use the existing
node ledger; deadlines retain original source errors.

The API starts no effects. The same supervisor owns accepted retirement and
recruitment across cancellation, deadlines and lost replies. Missing local
completion, insufficient replacement, changed/withdrawn boots, fenced owner or
stale operation produces no settlement evidence. No-spare recruitment remains
outstanding after object-covered old retirement; this does not invent a newer
binding or a completed maintenance operation.

Seven public cases use actual three/four-node managed boots, native persisted
follower lanes, an acknowledged SQL command, the canonical object barrier and
the durable reference journal. They cover exact replay, no spare, lost member
reply with the original error Arc, invalid originals/member requirements,
expired waiters, replacement withdrawal, owner fencing and deadline extension.
Retired files/fences remain present. Every fixture joins shutdown, checks empty
native resource ledgers and requires exact boot/follower retirement counts.
The reference heartbeat now signs actual retained follower bytes as well as
available receiving credit.

Initial qualification found two test API assumptions and a query attempted
after its actual drain closed query admission. Readback now precedes the
publication barrier and the case verifies its resulting covered watermark.
The first broad run passed 287 cases; Clippy found a redundant capacity default
after all fields became explicit. Earlier logs remain separate from final
source qualification under `/tmp/cellule-follower-evacuation-evidence`.
The explicit Open/Active authority check subsequently exposed a source boot
still carrying its closed startup sample. The managed fixture now publishes
that source's real post-start heartbeat before recording its live-owner proof;
the production admission check remains required.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `3721ee64d260d57eec2e59dda72632bb77c789ee` |
| Frozen source | 669 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `169abf0111599ceb01b9f0ff02e61e4a835e348566e48a3471e6d6814c4ad05b` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-follower-evacuation-evidence` |

Final qualification passed 34 host library, 106 public node and 149 example
cases: 289 distinct tests, no failures or ignores. Host Clippy and host/runtime
API documentation passed with warnings denied. Format, boundaries, module
layout, document syntax/links and SQL/peer contract gates pass. The complete
active/isolated Rust/Cargo path sets and every byte match after qualification.
Focused repeats and earlier-source runs are excluded from the final count.

Complete observer/failed-process barriers, policy evidence publication and
revalidation, dead-owner recovery, affected-writer relocation,
`SettleRoles`/`Finalize`, terminal action handoff, remaining primitive faults
and W8–W10 remain required. This checkpoint supplies one live-owner role proof,
not fleet-wide settlement or shutdown permission.

## October 2 2026 foreign follower authority checkpoint

`FleetFollowerReferences` traverses every canonical directory page for a physical
follower, including expired advertisements and fenced tombstones. It matches
references to the original full roster and rechecks exact authority rows after
native collection. Coverage and leader liveness are compared even though native
continuation fingerprints omit them. Changed or incomplete captures retain the
original rows and interval; no reference absence settles Pending work.

The closed writer example now uses this collector and its final recheck. The
canonical heartbeat advertises real available follower-store bytes when that
component is installed. A new managed fixture uses three actual boot owners,
node-owned follower stores, canonical enrollment and an acknowledged SQL mutation.
Its object-coverage callback is paused to exercise the actual follower response
proof, then resumed and joined before observation.

Four public cases cover original producer/foreign lane matching, lost retirement
replies with shared original errors, real object publication changing coverage
without changing topology, and deadlines/regressed clocks/expiry. A retired local
lane still has a foreign obligation until canonical owner closure confirms.
Two directory cases exercise real canonical continuation pages and refuse a
foreign member or regressed capture. They are directory contract fixtures,
separate from the managed three-node cases.

Initial qualification caught two fixture assumptions: object proof can win before
log activation, and the supervisor retains its old binding while replacement
recruitment waits. The fixture now explicitly proves the follower path and the
rotation case checks confirmed old retirement plus the outstanding Recruiting
phase. No replacement is invented. Initial logs and a passing pre-layout run
are retained separately; final qualification follows the corrected module layout.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `57559a5ef3fe534ab3081f8e6f8eb9da4d3011cd` |
| Frozen source | 667 Rust/Cargo paths, including nested qualification locks. |
| Sorted JSON manifest SHA256 | `a16464289aca4f39375582da32328fa1f0e2b006e338cb5409bb8eb07ec01ee3` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-follower-references-evidence` |

The final source passed 34 host library, 106 public node and 142 fleet example
tests: 282 distinct cases, zero failures or ignores. Host Clippy and host/runtime
API documentation passed with warnings denied. Format, boundary, module layout,
document syntax/links and SQL/peer contract gates pass. The complete path set and
every Rust/Cargo byte are compared against the isolated snapshot after the run.

This supplies complete reference discovery and managed aggregate regression
coverage. Complete observer/policy/failed-process barriers, live-owner replacement
ensembles, dead-owner recovery, affected-writer relocation, `SettleRoles`/`Finalize`,
terminal action handoff, remaining primitive faults and W8–W10 still remain.

## October 2 2026 merge qualification checkpoint

Merged `main` at `18a1244a98c50a04c52da21325f95331055e9884` into the native
aggregate traversal checkpoint. The qualification driver retains `main`'s
canonical arrival collector and both new regression tests, with evidence writes
after the original arrival and drain clocks. Qualification thresholds remain
unchanged.

The merged source passed 44 app integration tests, 144 runtime lifecycle tests,
32 host unit tests, 106 public node tests and 138 fleet example tests in the
isolated checkout: 464 passing cases and no failures. Twenty documented manual
or environment-dependent tests were ignored. App/host Clippy and host/runtime
API documentation passed with warnings denied, together with format, boundary,
module layout, document and SQL/peer contract gates.

The exact 662 Rust/Cargo paths match the isolated snapshot. Their sorted JSON
manifest SHA256 is
`38d8c01b33be4254f8f5feac653081f8f9dffa53bed2a80d17550a4bc6bdb1cf`.
Merged-source logs and the manifest are retained in
`/tmp/cellule-native-inventory-merge-evidence`; the preceding checkpoint's
manifest and results are preserved there with `before-merge-` filenames.
Provider/process CI qualification remains separate from these local checks.

## October 2 2026 native aggregate traversal checkpoint

`FleetNodeInventoryScan` now traverses every native category under the exact
bootstrapped full roster, preserves native continuations, and rechecks complete
category fingerprints. Its result retains writer transitions, reader producer
jobs and original errors, persisted follower lanes, follower preparation state
and supervisor rotations. `validate_enrollments` matches exact original requests,
accepted timestamps and published evidence; missing bindings and failed boots
never become empty roles. Applications account these bounded copied buffers.

The reference observer uses this collector and rechecks all seven categories
after its fleet-wide Cell authority scan. It retains independently revalidated
writers for pressure relief when topology invalidates a full traversal. Count
coverage remains disabled for that interval. Actor maps can overlap during
release; the collector preserves transitional keys rather than double-counting
them or treating them as absent.

Seven new public-path cases cover native continuation, nonce reuse, incomplete
and changed rechecks, partial pressure inputs, a real canonical owner renewal,
managed reader capture before/after evacuation, and original errors after a
lost retirement reply. They use actual managed boots, actor/query lifecycles,
canonical authority and the durable journal. Shutdown checks empty resource
ledgers through the existing fixtures.

Qualification found two regressions. First, a changing actor topology caused an
observer error during real movement; this now disables completeness and preserves
only independently checked pressure rows. Second, the equilibrium example
required immediately complete captures after its last movement. Instrumented
runs observed canonical renewals changing revision/progress without changing
owner, fence or root; the deterministic renewal case verifies that the exact
authority recheck correctly invalidates such an interval. The example now
requires two consecutive complete, fresh post-batch samples within its original
120-second convergence deadline. Extra allocation, active attempts and native
failures still fail immediately; the final complete count scan also remains
required. No residence, count, deadline, or receipt requirement was reduced.
The earlier failure logs and diagnostic traces are retained separately.

| Final source and evidence | Recorded value |
| --- | --- |
| Parent revision | `b36f76321539e306bbce5d25973f6595dbac8df5` |
| Frozen source | 662 Rust, Cargo manifest and Cargo lock paths, including nested qualification locks. |
| Manifest SHA256 | `9610c21eb6045a461676c449c849cbb91330d05d5b560106c23abbbc2d3dc428` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Evidence and source archive | `/tmp/cellule-native-inventory-evidence` |

The manifest SHA256 hashes sorted-path JSON with two-space indentation and a
final newline. The complete path set and every Rust/Cargo byte are compared
against the isolated snapshot and current checkout after qualification.

| Final-source command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --lib --test node --example fleet_operations --all-features --locked -- --test-threads=2` | 32 library, 106 public node and 138 example cases passed; zero failures/ignores. |
| `cargo clippy -p cellule-host --lib --test node --example fleet_operations --all-features --locked -- -D warnings` | Passed with warnings denied. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-host --all-features --no-deps --locked` | Passed with documentation warnings denied. |

These are 276 distinct passing cases; focused repeats are excluded. Format,
crate boundaries, module layout, Markdown links/Rust syntax and SQL/peer
contract gates also pass. This provides canonical local traversal and reference
consumption. It does not finish W3/W7: complete foreign log authority discovery,
replacement-policy and failed-process evidence, role-enabled aggregate follower
fixtures, affected-writer relocation, `SettleRoles`/`Finalize`, terminal action
handoff, remaining busy-primitive/fault work, full W8 examples and W9–W10 remain
required. The full plan stays active.

## October 2 2026 managed reader evacuation checkpoint

`ReadReplicaManager::evacuate` checks one original Established reader against
the current Evacuating operation. It traverses the durable roster, requires
current managed boots and canonical policy selection outside the donor, and
uses authenticated native status before closure and after original retirement.
The final probe covers a refresh completed through a retained peer clone after
the first probe. Capacity refusal preserves the open donor; changed policy,
authority, boot or registry refuses completion evidence.

Managed Draining reconciliation now preserves open views until explicit
evacuation or terminal native shutdown. Canonical closure and retirement keep
their original owners across cancelled waiters and lost replies. Roster pages,
retained records, candidate observations and returned evidence use the existing
runtime metadata ledger. Deadline clamping uses the actual operation clock;
timeout sources remain available.

Ten new cases use three real leased/enrolled nodes, signed peer dispatch, native
SQLite readers and the durable reference journal. They cover no spare, nonzero
replacement, cancellation, deadline, lost retirement reply, policy change,
metadata refusal, replacement withdrawal before/after closure, and peer refresh
racing the first probe. Each case joins shutdown, checks empty native resource
ledgers, and confirms original boot withdrawal/retirement. Three roster unit
cases cover retained credit, capacity refusal and ambiguous/obsolete boots.

A repeat of the existing movement suite exposed a valid BusyExecution refusal
after command acknowledgement. That fixture assumed acknowledgement implied
idle transfer readiness. It now waits for two stable, unblocked native samples
at the acknowledged prefix before dispatch. The production refusal contract and
qualification thresholds are unchanged. The first evacuation run also caught
reply-pause assertions that expected a receiver to survive waiter cancellation;
the corrected cases verify its disappearance, retained unpublished retirement
and exact replay. Both failure logs are retained.

| Source and evidence | Recorded value |
| --- | --- |
| Parent revision | `7541774e8cc4b66d9b8dac2ef13abb6dbcce2aca` |
| Frozen source | 655 Rust, Cargo manifest and Cargo lock paths, including nested qualification locks. |
| Manifest SHA256 | `a67ee362cde52a8ab4eba3b7086b4f39ae572d505d1db7e51d0550ba62170c34` |
| Isolated checkout | `/tmp/cellule-reader-prelease-8698183` |
| Logs and source archive | `/tmp/cellule-reader-evacuation-evidence` |

The manifest hashes sorted `SHA256  relative-path` lines with a final newline.
The isolated source and current working tree are byte-compared against the
complete manifest and path set. Documentation changes after freezing do not
change those Rust/Cargo bytes.

| Final-source command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --lib --test node --example fleet_operations --locked` | 32 library, 106 public node and 131 example tests passed; zero failures/ignores. The example includes overload, controller restart and real-time count equilibrium. |
| `cargo clippy -p cellule-host --lib --test node --example fleet_operations --all-features --locked -- -D warnings` | Passed with warnings denied. |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p cellule-host -p cellule-runtime --all-features --no-deps --locked` | Passed with documentation warnings denied. |
| `cargo test -p cellule-runtime --lib node:: --all-features --locked` | 106 passed; zero failures/ignores and 418 filtered out. |

These are 375 distinct passing cases; focused repeats are excluded. This
checkpoint proves a per-reader path, not complete fleet settlement. Affected
writer relocation, foreign follower/replacement and failed-process evidence,
complete observation barriers, terminal action handoff, `SettleRoles`/`Finalize`,
maintenance/receiver-loss examples and W9–W10 qualification remain required.

## October 2 2026 native boot withdrawal checkpoint

Managed boots can now bind their original authenticated directory version,
Established enrollment row and journal to the retained host drain before
readiness. The reference example installs that binding after atomic startup
confirmation. The original closing task joins facilities, runtime and lease
maintenance, withdraws the exact boot, checks its canonical terminal state,
and confirms durable retirement before exposing Stopped. Cancellation of the
sole caller leaves the original closing task owned. A deadline or ambiguous
committed retirement retains Draining and the same evidence for replay.

Eight native example cases cover success/replay, caller cancellation, lost
retirement replies, a retirement deadline, an actual later signed heartbeat,
immutable boot binding, missing canonical storage and failed role closure.
They use the exported host API and the real directory/SQLite journal. No
runtime shutdown is repeated and no storage absence invents retirement.

Review also found that canonical withdrawal could accept a stale-collected
tombstone that still carried node-log authority when the caller's original
token preceded recruitment. A new runtime regression reproduced that success
before the fix and passed afterward. Withdrawal now refuses that retained log.
`NodeDirectory::is_withdrawn` confirms a tombstone without log authority or a
recovery claimant; `is_retired` continues to mean a permanent fence. Native
closing and the reference application's partial-startup cleanup use the
stronger check. Persisted and signed record formats are unchanged.

The previous count-equilibrium failure remains unresolved; the scenario now
retains its failed pass report. A passing replay is not a fix. Parent `238b816`
CI completed successfully for workspace/MSRV, decoder fuzz and follower proof,
but object-capacity repeat three failed: its skewed two-client window completed
59 of 60 planned requests and found no fully served capacity point. That raw
driver log and the provider artifact are retained with this checkpoint. These
results concern the parent binary and do not qualify this new source.

Evidence is retained under `/tmp/cellule-boot-withdrawal-238b816-evidence`.
The final isolated run byte-checks all 648 Rust/Cargo/lock paths and complete
path sets before and after each command. Manifest SHA256:
`ced2be60817e744d551e84045adeebe9dca394579542d99b70f78e99c135d359`.

| Final native scope | Result |
| --- | --- |
| Runtime library / public fleet, all features | 521 / 26 passed; three / one existing ignores. |
| Host library / public node, all features | 29 / 106 passed. |
| Fleet reference example, all features | 121 passed, including all eight new closing cases, overload, controller restart and count equilibrium. |
| Workspace all targets/features, locked | Check and Clippy passed with lint warnings denied. |
| Runtime/host API documentation and static gates | Passed with documentation warnings denied; format/diff, boundaries/layout, Rust fences, links and SQL/peer contracts passed. |

These are 803 distinct passed cases and four existing ignores. Focused repeats
are excluded. The count pass does not erase its earlier failure or establish
its cause. No new process/provider, sustained-traffic or mixed-binary evidence
is claimed for this source.

Complete role/replacement and affected-writer relocation evidence, the terminal
action's join and handoff, committed maintenance completion, the remaining
examples and W9–W10 qualification are still required. `SettleRoles` and
`Finalize` remain refused; boot closure alone cannot authorize them.

## October 2 2026 pressure selection checkpoint

The unchanged overload scenario reproduced a competing local eviction. The
trace binds the same Cell, actor generation and last-use timestamp: native
oldest-first eviction began at `1790968652472`, fleet allocation followed at
`1790968652474`, and receiver preparation refused the fenced source. The first
movement completed while the second was safely cancelled. This is an advisory
selection race, not permission to weaken source authority or count cancellation
as successful relocation.

`CellTransferDemand` now carries the actor's actual `last_used_ms`. Under
Shedding or Critical pressure on an Active donor, the pure planner prefers
recent settled Cells. Native emergency eviction remains oldest-first and keeps
its existing budget and dwell rules. Normal balance and maintenance preserve
Cell identity ordering; invalid recency timestamps are excluded. This is a
contention reduction, not an actor reservation or a general convergence proof.
Exact source release and receiver cleanup still handle racing closures.

The native fixture now boots Cells in ascending identity order, making the
lowest identities the oldest eviction candidates. This adversarial ordering
removes accidental success from randomized HashMap iteration. The corrected
fixture fails before the policy change with one movement. The initial fixture
compile error (CellId has no Ord implementation) and its correction to stable
identity bytes remain recorded separately. Pressure load, deadlines, residence,
receiver budgets, two-movement results and zero-resource assertions are intact.

Two private planner regressions and a public permutation case cover the new
order, settlement, timestamp refusal, stable identity ties and unchanged drain
and ordinary balance behavior. All 20 paired native replays pass: 20 overload
and 20 controller-restart cases. Repeats add no distinct test coverage. These
results do not establish process/provider or mixed-version qualification.

### Current verification

All 646 Rust/Cargo/lock paths are byte-checked between the active checkout and
isolated snapshot before and after each command. Their final path sets also
match. Manifest SHA256:
`c7005daa497dca9ad9893176faf129b0beefbfbd9446e9e003d06f42d140bf70`.

| Native scope | Result |
| --- | --- |
| Complete runtime library, all features | 520 passed; three existing ignores. |
| Complete public fleet suite, all features | 26 passed; one documented provider test ignored. |
| Complete host library and public node suite | 29 and 106 passed. |
| Complete fleet example, all features | 112 passed; count equilibrium check failed. Overload and controller restart passed. |
| Workspace all-target/all-feature locked check and Clippy | Passed; lint warnings denied. |
| Runtime/host API docs and static gates | Passed with documentation warnings denied; format, boundaries/layout, Rust fences, links and SQL/peer contracts passed. |

These are 793 distinct passed cases, four existing ignores and one failure.
The balance failure states that equilibrium did not remain settled while
scheduling stayed enabled; its current error lacks the failed pass detail.
Its cause is unproven. The full failure, frozen executable and source remain
retained under `/tmp/cellule-pressure-selection-a2806db-evidence`, alongside
before/after, diagnostic and replay evidence. Required profiles and expected
results were not weakened. Parent `a2806db` CI has 112 example passes and the
original overload failure; native replay does not erase that CI evidence.

### Remaining delivery streams

| Stream | Unfinished work |
| --- | --- |
| W2–W5 | Complete role/current-authority observation, ongoing intent supervision, remaining failure/concurrency coverage and reliable count/pressure convergence. |
| W6 | Remaining primitive maintenance fault matrix, Blob external owners and sustained-traffic evidence. |
| W7 | Complete reader/follower replacement and failed-owner evidence, role evacuation, terminal action handoff and checked stop/withdrawal. `SettleRoles` and `Finalize` remain refused. |
| W8 | Runnable complete maintenance and receiver-loss scenarios. |
| W9 | Full process/provider fault campaign, soak and actual mixed-version qualification. |
| W10 | Exercised runbooks and staged rollout evidence. |

The complete implementation plan remains active. This checkpoint does not
complete W1–W10 or establish fleet operation qualification.

## October 2 2026 actual successor inspection checkpoint

Fresh movement inspection can now target another physical node, or a new boot
of the preferred node, after the journal proves the exact clean source release.
The additional endpoint cannot authorize an effect or establish failed-source
recovery. Source aliases, mismatched preferred session/node pairs, insufficient
positions, changed requests and stale journal/registry barriers remain refused.
Original effect acceptance, replay and cleanup endpoints are unchanged.

The reconciler first checks the preferred or retained serving endpoint directly.
If that check is unresolved, a bounded authenticated Cell observation under the
rechecked durable roster selects an established current boot. Its native actor
query and authority reads supply serving evidence. The original endpoint error
is retained when fallback succeeds. Retirement checks the actual successor;
unused receiver credit stays charged until its original cleanup proof joins it.
Partial role coverage supplies no absence or finalization proof.

The public node regressions use independent resource budgets and actual
ordinary acquisition, authority, restored SQL and native inspection. They cover
another node and another session on the preferred physical node, reject Idle or
closed actors, and refuse activation effects on the alternate endpoint. The
second case qualifies session separation in the local API; it does not simulate
process death or qualify the durable reboot/enrollment protocol.
The durable example uses three leased nodes and the SQLite journal. It adopts
an ordinary winner, proves receiver cleanup before permit retirement, resolves
the original receipt and reads its original SQL value. No acquisition effect
is accepted on the alternate node and authority advances once.

The corrected parent-code regressions fail at the valid successor request and
at the driver's missing adoption. Initial fixture failures and corrections,
original executables and sources are archived separately under
`/tmp/cellule-successor-inspection-1bd98d8-evidence`. An eager fleet scan also
failed the unchanged driver's observer-count assertion (five versus one);
fallback discovery preserves that assertion. A nested-if lint was corrected
without changing qualification profiles or assertions.

Existing record bytes, indexes and action keys are unchanged. Older inspection
readers refuse the newly allowed endpoint shape. Deploy and qualify upgraded
readers before enabling it in a mixed-version fleet; this checkpoint adds no
mixed-binary or process/provider qualification.

### Verification

All 646 Rust/Cargo/lock paths match the active source and isolated snapshot
before and after every final command.
Manifest SHA256:
`5c92a99e5846aa5bbff3527a2224c0f24fced77735eae01976d282ff0eb67053`.

| Final native scope | Result |
| --- | --- |
| Complete runtime library, all features | 518 passed; three existing ignores. |
| Complete host library, all features | 29 passed. |
| Complete public host node suite, all features | 106 passed, including both successor endpoint cases. |
| Complete fleet example, all features | 112 passed; required overload case failed. The new actual-successor scenario passed. |
| Workspace all targets/features, locked | Check and Clippy passed; lint warnings denied. Runtime/host API docs passed with warnings denied. |

These are 765 distinct passed cases, three existing ignores and one failed
case. Focused repeats are excluded. The unchanged overload case proves one
successful movement and safely cancels the other, retiring both permits; it
fails the required two-release/two-activation result. Its original source,
executable and complete log are retained. No count, deadline, pressure profile
or assertion was relaxed. The separate controller-restart case passes this
native run, which cannot erase its parent CI failure or establish reliable
convergence across runs.

Parent `1bd98d8` CI passes contract, MSRV, fuzz, fast models, website, object and
follower qualification and Compose smoke. Its workspace job passes 111 fleet
example cases and fails the controller-restart case with an unresolved Releasing
attempt after reply loss. Routing comparisons were pending at inspection.
The failure log is retained; its cause is not established by this change.
Reliable overload/controller-restart convergence, complete role/authority and
replacement-policy barriers, automatic intent supervision, Blob external
owners, fleet maintenance/failure scenarios and the complete deployment/fault
matrix remain open. `SettleRoles` and `Finalize` remain refused. Full W1–W10
is incomplete.

## October 2 2026 Cron maintenance checkpoint

The public primitive regression in
[`blob_cron/maintenance`](../crates/cellule-runtime/tests/primitives/blob_cron/maintenance/mod.rs)
now exercises three independently budgeted runtimes, real SQLite, authority
CAS, immutable publication and signed Effect delivery to the compiled target
command. Its source Tick uses the public scheduler inside an accepted native
command transaction with pinned logical time and an explicit worker gate.
The barrier closes typed Tick and Effect claim admission while that transaction
is running. The successor's Ticks use the registered maintenance command.

| Boundary | Checked result |
| --- | --- |
| Accepted source Tick | Its occurrence and durable outcome survive quiescence; original request resolution succeeds before release and after restoration. |
| Source release | Canonical maintenance release returns the authority's exact Idle root and closes the original actor. |
| Successor acquisition | Serving authority uses the successor session, increments the epoch, and retains the released root. |
| Due work during quiescence | The unfired schedule retains generation, occurrence and due time; a stale Tick generates nothing and a current Tick generates its first occurrence. |
| Retried Tick and delivery | Tick replay returns its original receipt. Two deliveries of each Effect return the same destination receipt and leave exactly one application row per occurrence. |
| Shutdown | All three runtime ledgers return memory, descriptors, disk, retained bytes, jobs, slots and unpublished log bytes to zero. |

The fixture initially failed compilation on codec usage, then rejected its
application table's protected `cron_` prefix. A later zero-disk assertion
correctly observed reservations of other live runtimes through the convenience
Host's process-wide budget. Each simulated node now has an explicit independent
disk budget; the zero-resource assertions remain intact. Earlier sources and
failure evidence are retained separately from the final source under
`/tmp/cellule-cron-maintenance-9ed469c-evidence`.

PR 37 is mergeable. Parent `9ed469c` CI's workspace job still fails the
required two-movement overload scenario: 111 example cases
pass, one movement succeeds and the other is safely cancelled. This checkpoint
adds Cron qualification without changing production scheduling, pressure,
admission or release rules. Reliable overload convergence, the remaining
primitive/fault matrix, Blob external owners, full role settlement and node
finalization, executable maintenance/failure scenarios, and process/provider
qualification remain open. Full W1–W10 is incomplete.

### Isolated native verification

All 643 Rust/Cargo/lock paths match between the active checkout and isolated
snapshot before and after every command. Manifest SHA256:
`32bc2b69c178f9e8295b3fa187e5d976995eb52654761e4e3a475d0934c2057f`.

| Scope | Result |
| --- | --- |
| Complete public primitive suite, all features | 50 passed; none ignored or filtered. |
| Complete public protocol suite, all features | 30 passed; two existing provider diagnostics ignored. |
| Public runtime maintenance suite, all features | Two passed; 202 other runtime cases unselected. |
| Focused Cron replay, all features | Passed; excluded from the 82 distinct passed cases above. |
| Workspace all targets/features, locked | Check and Clippy passed with lint warnings denied. |

The initial corrected fixture also passes its focused default-feature run.
These results establish the selected native public behavior; they add no
process, provider, mixed-version or fleet-wide maintenance qualification.

## October 2 2026 exact source refusal checkpoint

The unchanged overload scenario reproduced its unknown-release failure on
focused replay 12. Native local eviction selected partition 4 at
`1790959690253`; fleet Release was accepted at `1790959690255` and returned
Unknown with the original `CellNotActive` at `1790959690256`. The original
source, two tagged diagnostic probes, trace, identity derivation and frozen
executable are archived under `/tmp/cellule-pressure-race-5d350d1-evidence`.
The diagnostic executable SHA256 is
`2db7c5baa947dfda20d73fab2443226d4d597505f2c0555b43a797da51401dc4`.
The separate unchanged full diagnostic scope passed all 112 cases; that pass
cannot invalidate the reproduced race or qualify the final source.

The canonical actor now identifies exact-position release refusal before this
request reaches deactivation using `Error::CellReleaseRefused`. It retains the
original validation, admission or inventory cause. Canonical close, authority
publication and response-loss errors remain uncertain. The host records a
Rejected outcome and the underlying original error, allowing existing
release-refusal transitions to join unused receiver credit. An independent
local eviction cannot count as this attempt's Released or Activated evidence.
Neither the automatic pressure classifier nor its movement budget changes.

A deterministic public-host regression prepares actual receiver credit, joins
canonical source eviction and then submits the exact fleet release. Before the
fix it failed with Unknown instead of Rejected; the original executable SHA256
is `2aa151c5a4e75382266fb2e110a2ee7898deb36795f0c7da77c49171f2ed8a6e`.
The qualified regression source and failure are archived separately from an
initial compile-only fixture error (comparing the opaque VersionedControl).
After the fix this regression passes, preserving source authority and joining
all receiver credit. A second public regression proves that prior journal
acceptance without its original result remains Unknown after independent local
eviction, with receiver credit charged. Both focused cases pass.

### Final isolated native verification

All 641 active/snapshot Rust/Cargo/lock paths match before and after each final
command. Manifest SHA256:
`4e84a44ffa02ed0b7f55e21cedecffa015664b3a9e2d42a46ac38de4de47395e`.

| Scope | Result |
| --- | --- |
| Exact idle-release integration | 6 passed; 198 other integration cases unselected. |
| Runtime library | 515 passed; 3 existing ignores. |
| Host library | 29 passed. |
| Complete public host node suite | 104 passed, including both new cases and the immediate zero-disk-credit assertion. |
| Complete fleet example | 112 passed; none ignored or filtered; 71.41 seconds. |
| Application integration | 39 passed; same 16 documented manual ignores. |
| Workspace all targets/features, locked | Check and Clippy passed; lint warnings denied. |
| Runtime/host API documentation | Passed with warnings denied. |

These are 805 distinct passed cases; focused repeats are excluded. A separate
pre-assertion source run also passed, and remains archived independently.
Format/diff, boundaries/layout, 110 Rust snippets, 1173 Markdown links and the
28 SQL/peer contract assertions pass. Final sources, logs, complete manifests
and executables are retained. No Linux, provider/process or mixed-binary
qualification was added in this checkpoint.

Parent `5d350d1` CI still failed the original overload requirement with 111
passes and one successful movement. Its object-capacity repeat 3 also failed
`skewed: no fully served capacity point`; the driver completed 58 of 60 planned
requests at that point. Original CI logs and the complete object-capacity
artifact are retained. Neither failure is erased by the positive native run;
capacity qualification and reliable overload convergence remain required.

This checkpoint fixes refusal classification. It does not complete the required
two successful movements in the overload scenario: preparation can also refuse
an invalidated source, and the scenario disables new scheduling after the first
batch. Required movement/readback counts, deadlines, native pressure behavior
and qualification profiles remain unchanged. Full W1–W10, role settlement,
fleet finalization, executable maintenance/failure scenarios and deployment
qualification remain open.

## October 2 2026 retained whole-host drain checkpoint

The host now retains the complete canonical closing attempt in one fixed slot.
Shutdown transfers the existing drain-lane guard to that task before awaiting
its result. Cancelling the only caller cannot abandon an accepted facility
callback, release the lane early or prevent autonomous completion. The terminal
scale-down step uses this same owner and joins the original epilogue even when
it already observes Stopped. Native runtime shutdown is still invoked once.

The existing reverse facility order, work cancellation, runtime close and lease
withdrawal phases remain canonical. A phase deadline leaves Draining; a later
attempt resumes the same underlying resource owners. Original task failures
remain fatal. Local drain observations retain original return/join results and
first/latest source errors; they do not establish role coverage or authorize
fleet completion. SettleRoles and Finalize remain refused.

Three public regressions cover sole-waiter cancellation, scale-down queued past
its deadline on the original lane, and original failure history after retry.
The earlier pre-fix cancellation regression failed because the accepted
facility callback was abandoned. Its source manifest SHA256 is
`f1657e7eb530c26ac45d99401f20c69cd86f3a4e8af48b58475c70a13aaeee19`.
Original test source, log and executable are archived.

The native-retirement fixture now pauses after the original supervisor joins,
before automatic host completion clears weak rotation receipts. All original
retirement, interruption, single-close, withdrawal and exact-readback assertions
remain. It additionally verifies joining the same host attempt and invalidation
of the weak request after Stopped. An earlier fixture version failed the public
suite, with 100 cases passing and one failing; its source and logs are preserved.

Two synthetic controller tests retain their original 100 ms and 300 ms budgets,
starting the injected timeout at the observed first acceptance. A paused model
clock keeps unrelated SQLite setup and journal rereads from injecting an extra
fault. Original Elapsed, phase, permit and healthy-sibling assertions remain.
The frozen original Linux example replay reproduced the first-endpoint failure;
its full scope passed 109 cases and failed that case, count convergence and
controller restart. This replay is separate from final-source verification.

### Verification scope and outstanding overload failure

Complete source-set and byte checks cover all 641 active/snapshot/staged
Rust/Cargo/lock paths before and after each final command. Manifest SHA256:
`4377077c0762a8422f73642fdbdced6b6d105c0ce70d50656ff53c9dcedb33a6`.

| Complete isolated native scope | Result |
| --- | --- |
| Host library | 29 passed. |
| Public host node suite | 102 passed; none ignored or filtered. |
| Fleet example | 111 passed; overload failed; 69.10 seconds. |
| Application integration | 39 passed; the same 16 manual ignores. |
| Host all-target/all-feature Clippy and API docs | Passed with warnings denied. |

These scopes contain 281 passed cases and one failure; focused repeats are
excluded. Format/diff, boundaries/layout, 110 Rust snippets and 1173 Markdown
links pass. This is a closing-owner checkpoint, not successful fleet qualification.

The pinned Rust 1.97.1 Debian 12/aarch64 runner uses two CPUs, four GiB, 256 PIDs,
and the existing descriptor hard limit of 524288. Only its inherited descriptor
soft limit is raised from 1024. Its application/host default-feature command
passes: application 10 library, three contracts, 39 integration with the same 16
manual ignores, host 29 library, 102 public cases and five application doctests.
The full example passes 111 cases and fails overload, in 138.79 seconds; both
corrected controller models pass. Complete source checks pass at every boundary.
The runner exits 101 without OOM. A launcher source-copy attempt failed the
preflight before Cargo; its log is retained separately.

The parent merge `4cc8ed3` also fails the full example in
[the Rust workflow](https://github.com/crabbuild/cellule/actions/runs/37030073904/job/110914390958):
111 passed and overload failed, in 79.70 seconds. It settled one movement while
retiring two attempts. The current native failure has the same counts; the Linux
failure leaves one original Releasing attempt unknown after 12 passes. No
required movement count, qualification profile or deadline has been weakened.
The local pressure/observed-generation race remains under investigation; these
results do not prove its cause or repair. Complete role observation, terminal
action handoff, cross-session recovery and all remaining W1–W10 work stay open.

## October 2 2026 PR 37 merge conflict resolution

The branch integrates main `0f4ca09` while preserving its retained maintenance
and shutdown ownership. Four conflicts involved actor admission, replica hint
fixtures, task supervision fixtures and the split durability module.

- Transfer admission carries the upstream admitted owner fence and the existing
  maintenance preflight record.
- Expiry-driven rotation uses the existing bounded supervisor claim. After
  provider I/O, the claim is rechecked so accepted maintenance remains strict.
  Fleet-bound recruitment delegates the same provider membership-read hook.
- Publication-hint tests retain their paused-clock guard, original bounds and
  assertions, alongside upstream recruitment and directory diagnostics.
- The upstream rotation fixture uses the canonical retirement observation and
  the same signed session as its host. The original deadline and close/recruit
  count assertions remain unchanged.

Verification uses an isolated snapshot with complete source-set and byte checks
before and after each command. Its 638 Rust/Cargo/lock paths have manifest SHA256
`c1393c29486a7bc1bffcdf61e88db8cc3a5f6110abe4d9c4fa3d46d4fe62f784`.

| Complete native command scope | Result |
| --- | --- |
| Workspace check, all targets and features, locked | Passed. |
| Runtime library, all features, locked | 515 passed; three existing ignores. |
| Runtime admitted-owner-fence integration filter | Three passed; 201 unrelated cases filtered. |
| Host library, all features, locked | 29 passed. |
| Public host node suite, all features, locked | 99 passed; none ignored or filtered. |
| Fleet example, all features, locked | 112 passed; none ignored or filtered; 68.65 seconds. |
| Application integration, all features, locked | 39 passed; the same 16 documented manual ignores. |

These scopes contain 797 distinct passed cases. Host Clippy across all targets
and features and host API documentation pass with warnings denied. Format,
boundaries, module layout, 110 Rust snippets, 1173 Markdown links and the SQL/peer
validator's 28 assertions and 567 links pass.

Initial compile failures and
an invalid-session fixture run are archived separately; they do not qualify
this source. Merged-source Linux, process/provider, mixed-binary and complete
W1–W10 qualification remain open. Prior checkpoint evidence below retains its
original source scope; earlier green results do not qualify this merge.

## October 2 2026 retained node task joins checkpoint

The parent `0132b60` task group removed handles from its bank before joining.
A failed first shutdown consumed the work failure. The next shutdown could
find no failure, withdraw lease maintenance and report Stopped. A new public
node regression reproduced that false success before the production change;
its first shutdown failed and its second returned Ok. The complete 637-path
before-source manifest SHA256 is
`6d53399d1ac48d31852cfdcda62bc77dc4d457bf9348f8c304e8fbc1f439ea77`.
The original failure, test source and executable are archived. An initial
compile-only attempt needed an explicit test-helper reference lifetime; it is
retained separately and supplies no behavioral evidence.

The existing 256-task supervisor now retains every handle and terminal result
in its original bank. Concurrent callers share the exact join. Cancelling a
caller drops its waiter; no accepted task ownership disappears. After joining,
the first and later drains expose the same original error object through the
source chain. Sibling joins continue after a task failure. Node shutdown still
closes the runtime but stays Draining with lease maintenance retained when a
required task fails; a retry cannot erase that failure.

Ordinary deadline abortion now targets the work task, retaining the supervisor
until it joins that work's cancellation and destructor. The cancelled work's
original JoinError remains on later drains. Group destruction still aborts its
remaining owned tasks; task admission and the 256-slot bound are unchanged.

### Native facility watcher and resumable deadlines

An intermediate implementation retained all errors but continued aborting the
node-log facility watcher on deadline. The full public host suite rejected it:
96 cases passed and two existing native-cleanup deadline cases failed on retry
with the watcher's stored cancellation error. Its source manifest SHA256 is
`fe4d15f94d557a466fc243797bcbe9d0de388f7954228ab5869e58cc09901911`.
Those logs and source are archived and do not qualify the final change.

The watcher now uses a private retained-work registration in the same bounded
supervisor. Its deadline drops the waiter while the existing facility continues
to own the native supervisor; it does not abort that watcher. A later shutdown
joins the same native cleanup and original watcher result. Genuine native or
watcher failures still remain errors. No second scheduler, authority, public
configuration or alternative cleanup path was added. The existing deadline,
cleanup, withdrawal and zero-resource assertions remain unchanged.

Five public regressions cover repeat shutdown without premature withdrawal,
cancelled waiters, concurrent joins, exact deadline cancellation and original
panic retention. They check original error identity and sibling completion.
The corrected source also passes both previously failing native-cleanup cases.

### Verification scope

Final native commands use an isolated source snapshot, all features and locked
dependencies. Complete source-set and byte checks run before and after each
command; all 637 Rust/Cargo/lock paths match final manifest SHA256
`6e921d09fca19a8e91438e1bc078e315ff42847252691cb21dffc13a49184db4`.
The snapshot reuses its existing checkout-specific target directory beneath
`$HOME/Workspace/crabbuild-target`; previous qualified evidence and frozen
executables remain archived independently.

| Complete final native scope | Observed result |
| --- | --- |
| Host library | 29 passed. |
| Public host node suite | 98 passed; none ignored or filtered. |
| Fleet operations example | 112 passed; none ignored or filtered; 67.98 seconds. |
| Application integration | 37 passed; the same 16 documented manual cases ignored. |

These are 276 distinct native cases; the initial 16-case focused run is excluded
from that count. Host all-target/all-feature Clippy and API documentation pass
with warnings denied. Format/diff, workspace boundaries/layout, 110 documented
Rust snippets and 1173 local Markdown links pass. This source supplies no new
process, provider, mixed-binary or full fleet finalization proof.

The isolated Linux ARM run exits zero with the same complete source checks
before and after each command:

| Default-feature, locked Linux command | Observed result |
| --- | --- |
| `cargo test -p cellule-app -p cellule-host --locked` | Application library 10, contracts 3, integration 37 (the same 16 manual ignores); host library 29, public node 98; five application doctests. Host doctests contain zero cases. |
| `cargo test -p cellule-host --example fleet_operations --locked` | All 112 cases pass; none ignored or filtered; 81.35 seconds. |

Environment: pinned Rust image, Debian 12/aarch64, Rust/Cargo 1.97.1, two CPUs,
four GiB and 256 PIDs. The inherited descriptor soft limit is raised from 1024
to its existing 524288 hard limit, matching the earlier frozen-binary descriptor
diagnosis. No task, scenario, deadline or qualification bounds change. A launcher
attempt failed before any Cargo command because the copied script lacked its
shell interpreter line; its separate log/container record is retained. Adding
that line changed only the launcher, and the complete subsequent commands
supplied the results above. Logs, final sources, manifests and frozen native/
Linux executables are archived independently. The task-owned container is
removed only after terminal status and evidence are captured.

### Parent CI and remaining scope

The parent qualification contract, coordination model, website, fuzz and
capacity workflows passed. Its Rust MSRV and workspace test steps passed,
but [the complete fleet example step](https://github.com/crabbuild/cellule/actions/runs/37016038610/job/110867015361)
failed: 111 cases passed and the overload scenario settled one movement rather
than its required two. The raw failure log SHA256 is
`f0f5be0fa5e3a133b5880143e973887eb9db547abc21ae69f60484651caec3ea`.
This checkpoint's complete local example pass does not establish that failure's
cause or claim it repaired. The historical x86 publication-hint cause also
remains unestablished. Neither failure changes the required profiles, resource
bounds, scenario counts or deadlines.

This is a W4/W7 shutdown prerequisite. Complete authenticated role/current-
authority aggregation, replacement/failed-owner evidence, role evacuation,
terminal finalization handoff without self-join, cross-session recovery,
Cron/Blob owners, maintenance/failure examples, qualification and rollout
remain required. SettleRoles and Finalize remain refused. The full W1–W10
implementation objective remains active.

## October 2 2026 reader reconciliation lease-boundary repair checkpoint

The parent `8698183` qualification contract failed during reader fixture startup,
before its native-opening scenario: `install_node_lease` returned
`Control("CellNode task group is unhealthy")`. Its workspace/MSRV campaign
passed, so that success did not supersede the separate contract failure.
The [failed contract job](https://github.com/crabbuild/cellule/actions/runs/37009635155/job/110846004625)
and original log are retained; the log SHA256 is
`1e6a9dfda287a5ecba829b02e0763ddfb4c8036ae363d86f7adb6cd74ea838df`.

The new periodic temporary inventory used `try_reserve_node_bytes`, whose
live-lease check returns Fenced before lease installation. The immediate first
tick could therefore end the installed reader task before host startup. Thirty
unchanged native repetitions of the original closure case passed; those negative
reproductions did not disprove the ordering race. A controlled public loop
before lease installation then failed with the original Fenced source.
A second real producer case fenced the local lease after a lost acceptance
reply; the old reservation prevented periodic exclusion for two real ticks.
Both failures are archived, with their source manifests and executables.

Both temporary inventory paths now use the existing metadata reservation API,
including the bound producer's retained-record index. This charges the same
bounded byte ledger without granting native admission or lease authority.
Native opening, activation jobs and public native inventory retain their live
lease checks. The activation lane, five-second interval, 64-attempt batch,
30-second attempt deadline and all resource bounds remain unchanged.

The new public host case confirms healthy startup after the pre-lease loop and
joined shutdown. The new bound-producer case observes the original Refused
exclusion after fencing, preserving its spec and acceptance time; it also checks
fenced inventory, refused activation, no native opening and zero final resources.
Neither local result supplies failed-process closure or replacement policy.

### Snapshot retry test synchronization

The first Linux application/host run reached the previously failing reader
closure scenario successfully, then failed a separate snapshot case: it counted
four authorizations instead of two. The fixture spawned a retry and used
`yield_now` before releasing the original blocked job. A yield did not guarantee
that the retry had reached the retained job. If the original completed and was
joined first, the later retry could correctly begin another fresh capture.

A diagnostic copy delayed only that retry by ten milliseconds and reproduced
the same four-versus-two failure. The fixture now explicitly polls the same retry
to Pending while the original job is blocked, before releasing it. The focused
case passes. Its exact two-call assertion, original request/page, authority
comparison, retained-byte checks and shutdown checks are unchanged. No snapshot
production code, deadline or qualification profile was changed. Diagnostic delay
code lives only in the evidence archive.

### Deadline model fault boundaries

The next Linux application/host command passed, including all 93 public host
cases and the reader-closure/snapshot-retry cases. Its complete example run
passed 110 cases and failed the existing accepted-endpoint timeout case: the
first Prepare acceptance row was absent. Its 150 ms real-time pass could expire
before acceptance under load. A diagnostic copy delayed only acceptance by
160 ms and reproduced that same missing-row assertion. Those failures remain
archived and rejected.

The timeout is a model fault injection, not a measured SLO. That current-thread
model now pauses the test clock after setup, drives each of the two sequential
endpoints to confirmed acceptance, then expires its original deadline share.
The pass deadline remains 150 ms; the first endpoint uses one third and the
second uses half of the remaining budget, as the public driver does. One timer
resolution tick permits delivery of the Elapsed event. Both original deadline
sources, two retained attempts, 8 KiB restore permits, Preparing phase and
accepted-without-result assertion remain required.

A separate public driver case pauses each endpoint before acceptance and proves
both Preparing permits remain charged, both acceptance rows remain absent, no
effect was dispatched, and no release occurred. It preserves the original
Elapsed sources. Host dev dependencies explicitly enable Tokio test-clock
utilities; product configuration, runtime deadline behavior, qualification
profiles and native/process SLO clocks are unchanged. A first helper assumed
concurrent endpoint waits and correctly failed its gate; it was corrected to
the driver's actual sequential partitions. A relative-copy diagnostic invocation
also failed to overlay the intended test source; it is archived and excluded.

### Qualification scope

Final source manifest: all 637 Rust/Cargo/lock paths, SHA256
`8df5a9f2a4929493142ac67e0b860965580308fddcdf93141de1c962c5c4ef4e`.
The isolated driver checks complete active/isolated path sets and every byte
before and after each final command. Final results and original failures are
archived under `/tmp/cellule-reader-prelease-evidence`.

| Final isolated native scope | Passed | Exceptions |
| --- | ---: | --- |
| Complete host library | 29 | None |
| Complete public host node suite | 93 | None |
| Complete fleet example | 112 | None |
| Complete application integration | 37 | 16 documented manual cases ignored |
| Runtime reader integration scope | 14 | 186 filtered; one documented RustFS case ignored |

285 distinct native cases pass; focused diagnostic repetitions are excluded
from that count. Native commands use all features and locked dependencies with
Rust/Cargo 1.97.0. Host/runtime all-target/all-feature Clippy and API docs pass
with warnings denied. Format/diff, boundaries/layout, 110 Rust snippets, 1173
Markdown links, 28 SQL/peer assertions and 567 validator links pass.

The final Linux ARM run passes the exact application/host CI command with
default features and locked dependencies: application library 10, contracts 3,
integration 37 (the same 16 manual ignores), host library 29, public node 93,
and five application doctests. The complete default-feature example passes all
112 cases in 88.83 seconds. No case in the host/example suites is ignored or
filtered. Complete Linux source sets and bytes are checked before/after both
commands. The container terminates zero with no OOM. Its environment is Debian
12/aarch64, Rust/Cargo 1.97.1, the pinned Rust image, two CPUs, four GiB and 256
PIDs. The inherited descriptor soft/hard limits are 1,024/524,288; only the soft
limit is raised to that existing hard limit, as documented by the earlier
frozen-binary descriptor diagnosis. No source/profile/Cell-count or resource
assertion is changed for this environment.

The earlier source manifests
`4391e7ed6b8a5696e82134d320a06078db6d228a6da0cd968eca740437b06eff` and
`a042f610b8ccce4922781eb8bafe084cacadf51ba09f60a25f21d6cf80370fb7`
passed their native scopes but retained the respective snapshot/timer fixture
races. Their logs, source versions and frozen executables are preserved
separately and do not qualify the final manifest. Both failed Linux runs remain
rejected. The corrective run uses the same pinned Rust image, two CPUs, four
GiB, 256 PIDs and documented descriptor headroom.

Full W1–W10 remains active. Complete role/current-authority observation,
replacement and failed-owner evidence, evacuation/finalization without
self-join, cross-session recovery, Cron/Blob owners, runnable maintenance and
receiver-loss scenarios, fault/mixed-binary qualification and rollout remain
required. SettleRoles and Finalize remain refused. The historical publication
hint failure has a separate, unestablished cause and is not claimed fixed here.

## October 2 2026 periodic reader producer repair checkpoint

The canonical reader manager's existing five-second loop previously scanned
only installed views. A lost acceptance before opening had no view and therefore
received no periodic repair. Selected views also retained an unconfirmed opening
result until another hint, and a cancelled removal could leave a fenced view
whose refresh could never succeed.

The same loop now scans the sorted union of managed views and retained producer
requests. Its original 64-attempt batch and 30-second per-attempt deadline remain.
The two original indices are bounded independently; their captured sizes are
charged before copying, with the charge retained through the batch. Capacity
refusal preserves responsibilities for a later tick. No additional scheduler,
activation path, journal, configuration or wire format was introduced.

Repair takes the original activation lane. A still-owned opening cannot be
classified from a missing view. After that owner releases the lane, canonical
retirement atomically excludes a never-started request or publishes the joined
native refusal. An unjoined native failure remains blocked. Selected open views
replay the original Established event before refresh. Already-fenced views resume
the same closure and Retired publication before remote reads. Finished finite
jobs are reaped from their existing bank; original failures remain observable.
Activation also joins its exact task before returning, so its completion implies
release of the finite-job byte token. A dropped join waiter leaves the same job
owned; a failed task returns its original join error before the derived channel
closure error.

### Scoped verification

The old loop fails the new lost-acceptance case after two real periodic ticks;
its original Pending request never settles. The initial build selected zero
cases with a short exact filter; that is excluded from evidence. The corrected
fully qualified case ran and failed. Before/after source bytes and the complete
636-path baseline set are retained with the original failing executable.

Five focused cases now pass: lost acceptance, lost opening result, cancelled
removal, a still-owned cancelled-waiter opening, and temporary inventory-credit
refusal. The latter retains the request through a refused tick and repairs it
when credit returns. Checked replay keeps the original spec, acceptance time,
settlement event and error Arc. Readback and joined cleanup remain required.

| Final isolated scope | Result |
| --- | --- |
| Complete host library / public node / fleet example | 29 / 92 / 110 passed; none filtered or ignored. |
| Complete application integration | 37 passed; none filtered; 16 documented manual cases ignored. |
| Runtime reader integration scope | 14 passed; 186 filtered; one documented RustFS case ignored. |
| Distinct native cases | 282 passed; five new periodic repair cases. Focused repetitions are excluded. |
| Host/runtime all-target/all-feature Clippy and API docs | Passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1173 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |
| Linux ARM complete fleet example | 110 passed; none filtered or ignored; 82.35 seconds, with the documented descriptor headroom. |

All final native commands used all features and locked dependencies. An earlier
complete pass before tightening the activation task join is archived separately;
its results cannot qualify the final source.

The first two-CPU/four-GiB Linux run inherited a 1,024-descriptor soft limit and
failed while creating a source Cell in the existing 128-reader long-partition
case: 109 passed, one failed with OS error 24. A parent-production control also
failed at that same source-bootstrap boundary, with SQLite CannotOpen. The
unchanged parent executable passes its selected case when only the soft limit
is raised to the existing 524,288 hard limit. The unchanged final executable
then passes all 110 cases under that same headroom. Original failures are
retained; the low-limit run remains rejected. No source, Cell count, memory
ceiling, deadline, qualification profile or expected assertion changed.

Both controlled runs check their entire source sets and executable hashes
before/after. Linux used the pinned repository Rust image, Rust/Cargo 1.97.1,
Debian 12 aarch64, default features, locked dependencies, two CPUs, four GiB and
256 PIDs. Its final executable SHA256 is
`46aadad9901d79c0e4476751da92b7d4085b30ce87ae64690b9d8d1aec3e751f`.
The 636-path parent-production control uses its own manifest and frozen binary.
Both containers were removed only after terminal results and evidence retention.

All final native commands compare the active/isolated complete 637-path
Rust/Cargo/lock set and every byte before and after execution. Source manifest:
`14669eae88f753561e4873eaa40798fc5625f5eccaaaad0786f82303c23cf274`.
Before/final logs, sources, comparison driver and executable hashes are retained
under `/tmp/cellule-reader-reconciliation-evidence/` and the matching
`/tmp/cellule-reader-reconciliation-final-*` files.

### Remaining scope

Parent `d5eebffe05b6d9514861d85f54d0d1f7c404e083` passed workspace/MSRV,
qualification contracts and both capacity campaigns at inspection. Compose
qualification and fresh CI for this change remain required. The historical x86
publication-hint failure still has no established cause; this repair is not
claimed to fix it.

Local producer repair does not reconcile failed processes, certify replacement
redundancy or supply current remote authority. Complete role observation,
reader/follower evacuation, finalization without executor self-join and every
remaining W1–W10 exit assertion remain required. SettleRoles and Finalize remain
refused by the host executor.

## October 2 2026 canonical advertisement verification checkpoint

Canonical advertisement decoding previously verified signatures and called the
storage encoder for exact-byte comparison. That encoder verified the same
immutable signatures again. Decode now checks shape and signatures, then uses
one private canonical serializer for byte equality. The storage encoder verifies
before calling that same serializer. Directory scope, liveness and signature
checks remain independent; there is no cached authorization or changed wire
format, signing domain, lease budget or recruitment scheduling path.

A deterministic thread-local test counter measures verification passes without
wall-clock thresholds or parallel-test interference. The regression first failed
on the old decoder with two passes instead of one. After the fix, legacy,
schema-2 and schema-3 decode each take one signature-set pass and reproduce the
original bytes. Additional cases retain exact signature errors for corrupted
identity and understood placement signatures, reject noncanonical whitespace
and reject oversized input before verification. The first broad run rejected a
new fixture that inserted an unknown root field; it now corrupts the actual
`identity.signature`. The typed error assertion was retained.

### Scoped verification

| Evidence | Result |
| --- | --- |
| Complete isolated runtime library | 510 passed; none filtered; three documented provider cases ignored; 89.40 seconds. |
| Complete runtime fleet integration | 25 passed; none filtered; one documented provider case ignored. |
| Runtime reader integration scope | 14 passed; 186 filtered; one documented RustFS case ignored. |
| Complete host library / public node / fleet example | 29 / 92 / 105 passed; none filtered or ignored. |
| Complete application integration | 37 passed; none filtered; 16 documented manual cases ignored. |
| Distinct native cases | 812 passed; three new canonical codec cases. Diagnostics and second-platform repetitions are excluded. |
| Host/runtime all-target/all-feature Clippy and API docs | Passed with warnings denied. |
| Linux ARM application integration | 37 passed; none filtered; 16 documented manual cases ignored; 22.28 seconds. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1173 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |

All final native commands used the isolated snapshot, all features and locked
dependencies. Before and after each command, the comparison driver checked the
complete active/isolated 635-path Rust/Cargo/lock set and every source byte.
Manifest SHA256:
`234d7c78aee2c5ce3aa257d242e7415200a63b01956fa470172f37c8fd768741`.
Logs, before/after source manifests, the original failing unit executable and
Linux executable are archived in `/tmp/cellule-signature-evidence/`. Final logs
and the source comparison driver are `/tmp/cellule-signature-final-*` and
`/tmp/cellule-signature-verify.py`.

Linux verification used the pinned repository Rust image
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`,
Rust/Cargo 1.97.1, default features, locked dependencies, two CPU credits and
four GiB. All manifest hashes were checked before and after its complete target.
The Linux integration executable SHA256 is recorded with the original logs;
its container was removed after retaining the evidence.

### Publication-hint diagnosis and remaining work

The original x86 contract failure remains open. Twenty byte-checked isolated
macOS repetitions and one two-CPU Linux ARM repetition of the original failing
case passed before this change. Those are negative reproductions, not proof of
a cause. The Linux ARM full-target pass after the change likewise cannot
establish the x86 failure's cause. Ranked hypotheses remain hint coalescing,
synchronous verification cost, membership/lease rejection and delayed bounded
queue progress. The confirmed duplicate verification is removed; no assertion,
profile or expected evidence was weakened.

Previous head `4dc71df0df4117ca48c1c6aa313f6fbdf8e3f0c2` passed workspace/MSRV,
contract, both capacity campaigns, website, decoder fuzz and fast/negative TLC.
Its Compose smoke and routing comparisons were in progress at inspection.
Root-capture head `284dd8c` subsequently completed both routing comparisons and
the aggregate routing job successfully. Broad TLC/simulator remain skipped.
Earlier failures stay in their own execution records. Fresh CI for this source,
full role/current-authority observation, replacement/failed-owner evidence,
maintenance settlement/finalization and the remaining W1–W10 work are required.

## October 2 2026 reader continuation coherence checkpoint

Managed reader continuation now fingerprints all native reader observations,
including entries outside the returned page. The fingerprint binds the exact
manager/session, sorted receipt positions and incarnations, canonical lifetime
counts, reader admission closure, snapshot attachment, manager closure and node
admission. Each reader is captured once and that same observation is copied if
selected for the page. A changed node admission during capture rejects the page.
The original activation lane, 10,000-view bound, 128-row page limit and one-MiB
native reservation remain in use; there is no new task, provider call or cache.

Previously the continuation depended only on the manager's topology UUID and
session. A retained peer can close, detach or refresh shared native state outside
that manager lane. The new public regression first failed on the old code:
continuation still succeeded after both peer closure and detachment outside the
first page. It now rejects those mixed scans. Two further cases observe native
query start/completion and an exact-root refresh beyond the returned page. They
check successful fresh traversal, SQL value and position, retained peer handles
and empty native resource ledgers after shutdown. The existing inventory case
also requires restart after cordon.

Fingerprints describe intervals, not an atomic whole-node state. The native query
case deliberately verifies that an open lifetime count can return to its old
value after completion, restoring the fingerprint. Matching hashes cannot turn
an open reader into joined. The irreversible closed/detached/zero predicate,
current authority, durable enrollment, replacement policy and complete host
facility joins remain separate requirements.

### Scoped verification

| Evidence | Result |
| --- | --- |
| Lifetime CAS unit / runtime reader scope | 1 / 14 passed; 509 / 186 filtered; one documented RustFS case ignored. |
| Complete host library / public node / fleet example | 29 / 92 / 105 passed; none filtered or ignored. |
| Complete application integration | 37 passed; none filtered; 16 documented manual cases ignored. |
| Distinct scoped cases | 278 passed; three new continuation cases. Selected diagnostics are excluded. |
| Host/runtime all-target/all-feature Clippy and API docs | Passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1173 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |

All final native commands ran in the isolated snapshot with all features and
locked dependencies. The active and isolated complete 635-path Rust/Cargo/lock
sets and every byte were checked before and after each command. Manifest SHA256:
`3e46afe3bca79ffc8b45a4d00ef04e691a21f25f4bc11d00f1ed1f1552b58300`.
The original failing regression is `/tmp/cellule-reader-pagination-before.log`;
it was a scoped workstation run. Final logs, source comparisons and evidence are
`/tmp/cellule-reader-pagination-final-*`,
`/tmp/cellule-reader-pagination-source.sha256`,
`/tmp/cellule-reader-pagination-verify.py` and
`/tmp/cellule-reader-pagination-evidence/`.

### Previous-head CI failure retained

Head `9f7d3c6ee86850d6df79de476293a3a1198d92a0` passed workspace/MSRV,
website, decoder fuzz and fast/negative TLC. Its capacity and Compose campaigns
were still in progress at inspection. The
[qualification contract job](https://github.com/crabbuild/cellule/actions/runs/36999783770/job/110814598431)
failed: 36 application cases passed, one failed and 16 documented manual cases
were ignored. `publication_hints_reach_readers_beyond_the_activation_concurrency`
timed out at publication 3 after 2.000884718 seconds, receiving 14 of 19 healthy
hints with five missing sessions, no held peer and one intentionally pending
transport. The workspace pass and isolated passes do not establish its cause.

The original failed-job log is
`/tmp/cellule-fleet-9f7d3c6-contract-failed.log`; SHA256:
`38a9f7767a428bd6bd780ce320196822abe6cf9f9c31ff081861c146b7c61670`.
The failure remains open. No bound, profile or expected evidence was weakened.
Fresh CI for the continuation change and complete W1–W10 work, including role
settlement/finalization and publication-hint diagnosis, remain required.

## October 2 2026 native reader lifetime checkpoint

`CellReadReplica::lifecycle_observation()` reads the original admission CAS word
and shared snapshot state. It reports the last installed receipt, irreversible
admission closure, snapshot attachment and retained snapshot/operation guards.
Accepted queries, native SQL and refresh work stay visible after their callers
cancel. Retained peer handles share the same closure. Closed admission, detached
state and zero guards establish local joining; the count is neither a query count
nor a count of handle copies.

Bounded host reader pages now return these observations through `entries()`.
Callers use `entry.receipt()` for the previous position fields. Pagination,
topology checks, the 128-row limit and one-MiB native reservation remain in the
canonical manager path. The runtime reader module moves to `replica/mod.rs` so
its focused observation module follows the workspace layout contract.

The public regression cancels an accepted native query, detaches shared state,
and observes its retained lifetime through both the peer and managed inventory.
Joining waits for that original callback; retained handles then reject new
queries, including after host shutdown. Existing regressions also observe old
snapshots, cancelled refresh opens, stalled authority reads and concurrent close
waiters. The private CAS case checks that closed zero cannot acquire another
operation or snapshot guard. Resource and error assertions remain required.

### Scoped verification

| Evidence | Result |
| --- | --- |
| New lifetime CAS unit case | 1 passed; 509 filtered. |
| Runtime reader integration scope | 14 passed; 186 filtered; one documented RustFS case ignored. |
| Complete host library / public node / fleet example targets | 29 / 89 / 105 passed; none filtered or ignored. |
| Complete application integration target | 37 passed; none filtered; 16 documented manual cases ignored. |
| Distinct scoped cases | 275 passed; two new cases. Diagnostic repetitions are excluded. |
| Host/runtime all-target/all-feature Clippy and API docs | Passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1173 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |

Every command used the isolated snapshot, all features and locked dependencies.
The complete active and isolated 635-path Rust/Cargo/lock sets and every byte
were checked before and after each command. Manifest SHA256:
`f820e068a457b6fddbd2b2ff70d06e4796afce9c6617b0ec84ac3dc104c7da93`.
Logs and source comparison drivers are `/tmp/cellule-reader-lifetime-final-*`,
`/tmp/cellule-reader-lifetime-source.sha256` and
`/tmp/cellule-reader-lifetime-{app-,}verify.py`.

These local observations still require independent current authority, durable
producer retirement, replacement policy and full host drain evidence. They do
not enable SettleRoles or Finalize. Complete W1–W10 implementation and fresh
process/provider qualification of this source remain required.

## October 2 2026 canonical root capture checkpoint

The entity process driver now waits for canonical object roots after client jobs
and receipt readbacks join. Follower proofs may acknowledge commands before
object publication. One read-only barrier covers the entire original serving
roster under the existing two-second bound. It pins session and endpoint, epoch,
incarnation, code and schema, and preserves original read errors. Missing or
changed authority, an unbounded roster, stalled reads or stalled publication fail.
It does not start another publisher, rotate authority, or use shutdown as proof.

The driver retains each Cell's latest acknowledged sequence across all windows
and emits actual canonical roots, independently checked minima, complete read
passes, elapsed duration and boot-clock bounds. Clock reads are included in the
duration and separately measured for the centisecond clock comparison. The
arrival windows, rate/concurrency ramp, response and arrival latency, overload
classification, receipt ledgers, original ownership, follower epochs and final
root coverage remain required. A barrier must finish within two seconds. See
the [capacity capture contract](../crates/cellule-app/performance/2026-09-29-write-capacity.md#canonical-root-capture-after-client-work).

The native regression first failed the old one-read pattern with `published root
does not cover writes`, then passed after the barrier. It uses a real SQL command
and canonical authority, and reconstructs the captured root to verify both the
new table and durable request record. This observation test does not claim a
network follower-proof response. Seven other cases cover every original writer
binding, a shared deadline across four real Cells and read admission, no progress,
original typed read errors, dropped observation, closed/missing/successor
authority and invalid rosters. Each fixture has a private disk ledger and keeps
all zero-resource shutdown assertions. The initial parallel fixture run failed
because the default disk ledger is shared; the assertions were retained.

### Scoped verification and remaining qualification

| Evidence | Result |
| --- | --- |
| Complete isolated application integration target | 37 passed, none filtered, 16 documented manual cases ignored; eight new cases; 6.53 seconds. |
| Application all-target/all-feature Clippy | Passed with warnings denied. |
| Qualification verifier tests | 35 passed across entity and scaling verifiers, including seven new barrier cases. |
| Static gates | Format/diff, boundaries/layout, 110 documented Rust snippets, 1173 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |
| Original failed follower artifact | All seventeen windows rechecked; the original root coverage gate still rejects Cells 9–11 at 330 versus acknowledged 331. |
| Historical passing follower artifacts | Their original roots still cover every independent acknowledgement; none supplies the newly required barrier records. |

The unchanged 634-path Rust/Cargo/lock manifest has SHA256
`724d50a3f8b1e52fd670211966e938dae994b6f38c94fca289db23c65a202122`.
Active and isolated path sets and every source byte were compared before and
after both Cargo commands. Logs and the comparison driver are
`/tmp/cellule-root-barrier-final-*`, `/tmp/cellule-root-barrier-source.sha256`
and `/tmp/cellule-root-barrier-verify.py`. Original regression and intermediate
fixture failures are retained in the task record. These are native checks;
fresh process/provider evidence for this change remains required.
The native integration binary SHA256 is
`cf83bd2249db7ae27865ff06c45c29231d0d50483dfa3e2b9913a8e0c46c8205`.
Verification used Rust/Cargo 1.97.0, all features, locked dependencies and a
separate Workspace target with `CARGO_INCREMENTAL=0`.

### Root-capture head CI

Head `284dd8cc9d70970a33926885337aecb5c087ebc7` passed workspace/MSRV,
contracts, both capacity campaigns, Compose smoke, website, decoder fuzz and
fast/negative TLC. Broad TLC and simulator were skipped. Both routing
comparisons remained in progress at inspection. These results qualify that head,
not the subsequent reader-lifetime change or the full fleet plan.

Both capacity artifacts ran synthetic merge
`7147e9f3e08e69f26c4698a6fae4b49d368518de`, whose parents are
`191409685b001a82bd02780def45102b4fc2f164` and the root-capture head.
Driver/node binary SHA256 is
`f9695b5596bf2955178e07d06d1b6896dcb5a423a275bc96e7681929000fa0cd`.
The current independent verifier rechecked every returned field against each
original report, including all raw hashes, receipts, resources, original root
coverage and the new timing records.

| Campaign | Repeat windows | Acknowledged writes | Barrier elapsed µs |
| --- | --- | --- | --- |
| [Follower capacity](https://github.com/crabbuild/cellule/actions/runs/36996826665/job/110805347208) | 18 / 18 / 18 | 6228 / 6230 / 6233 | 8560 / 8740 / 9053 |
| [Object capacity](https://github.com/crabbuild/cellule/actions/runs/36996826665/job/110805347410) | 17 / 19 / 19 | 5385 / 7400 / 7403 | 7619 / 9261 / 8642 |

Each repeat captured all twelve original Cells in one pass under the unchanged
two-second limit. Follower repeats verified 6218/6222/6225 follower-proof
responses and 11316/11716/11720 network appends. Their one-pass captures do not
exercise waiting; the controlled native regression covers that case. These
passes do not establish the causes of historical publication-hint failures.

Artifact 11222033521 retains 432 entries; ZIP SHA256:
`ba871f08d332bc74b7d449818c7dfce6959b568e653b34e6bf0e510f10c951bd`.
Artifact 11222293106 retains 435 entries; ZIP SHA256:
`89029d4efe01160eb7e4d3dfb6de9ef2f23fecd435964c7bd56010d63a445807`.
Archives and independent inspection reports are
`/tmp/cellule-fleet-284-{follower,object}-capacity*`.

### Previous head CI

PR head `aa12ad905aadd4189f83b4561334efb935a521e3` passed workspace/MSRV,
contracts, both capacity campaigns, smoke, website, decoder fuzz and fast/negative
TLC. Broad TLC and simulator were skipped; both routing comparisons remained in
progress at inspection. This does not establish the causes of earlier failures.

Follower-capacity [job 110794579951](https://github.com/crabbuild/cellule/actions/runs/36993417592/job/110794579951)
ran three fresh-provider repeats on synthetic merge source
`096dfb2ff42016519d756c8a302ea22c90a515ea` (parents `191409685b001a82bd02780def45102b4fc2f164`
and `aa12ad9`). All four driver/node binary records match SHA256
`ef8ba2ae24a51cb728c937e126e0b94e64ed7110afd06bf4e4cbf2c72fb96ac7`.
The repeats verified 10/9/9 windows, 2118/1443/1435 acknowledged writes,
2104/1424/1420 follower-proof responses and 4106/2826/2780 network appends.
Every recorded raw TSV hash was checked, and the current independent window and
root-coverage verifier rechecked each repeat. Artifact 11220794009 is retained
with all 351 entries; ZIP SHA256 is
`316c295e8e6f33b024354d77d2bff655aa75dbb57a1675dfc2f6bdbab3da3371`.
The archive and inspection are `/tmp/cellule-fleet-aa12-follower-capacity*`.
These historical passes cannot qualify the new barrier. Full W1–W10 work,
including maintenance role settlement/finalization, remains active.

## October 2 2026 live intent refresh checkpoint

`CellNode::refresh_fleet_intent` reads current physical intent and the original
Established boot through the existing atomic enrollment journal contract. Startup
now retains that exact enrollment record instead of a confirmation flag. Acceptance
time, immutable spec and establishment evidence cannot be replaced by another
Established request. Startup confirmation and live refresh share one application
path for the monotonic role gate, with lifecycle-before-startup lock ordering.

The application drives refresh before membership/lease renewal. Cordon/drain
preserve existing owners and close new writer/reader/follower admission. Older or
contradictory replies fail, Active replies cannot clear a local cordon, and replies
after shutdown cannot reopen the node. Deadlines and cancellation drop only the
read waiter; journal errors retain their original source. These failures grant
no renewal authority. The framework starts no additional supervision task.

The reference heartbeat calls this path before canonical directory CAS and guard
renewal. Its actual operational sample advertises Draining even without a Cordon
RPC. The real twelve-writer regression checks canonical advertisement equality,
unchanged original boot evidence, closed new-role admission, successful queries
to every existing actor, and joined shutdown with empty native resource ledgers.
Six further tests cover live intent, competing/delayed replies, shutdown,
deadline/backend errors, cancellation/sticky cordon and alternate boot requests.
The capture nonce uses the existing workspace `try_update` API; this removes the
Rust 1.99 deprecation without changing overflow behavior or the 1.97 minimum.

### Scoped qualification

The unchanged manifest contains 632 Rust/Cargo/lock paths with SHA256
`5a6e98c4c10dd1ad9b6d8842d814db55be7f78819344d4d7989a0f66bce05deb`.
The driver compares both the complete path set and every byte in the active and
isolated checkouts before and after each command.

| Evidence | Result |
| --- | --- |
| Complete host library target | 29 passed; none filtered/ignored. |
| Complete public host node target | 88 passed; none filtered/ignored. |
| Complete fleet example target | 105 passed; none filtered/ignored; 69.28 seconds. |
| Isolated complete application integration target | 29 passed; none filtered; 16 documented manual cases ignored. |
| Distinct scoped cases | 251 passed, including seven new cases. Diagnostic repetitions are not added. |
| Isolated standalone `balance` command | Exit zero; eight releases/activations/retirements and original receipt checks, shared in-flight maximum two, restore maximum 2,550,136,832 bytes, three joined/retired boots, two receivers, controller epoch one, final counts 4/4/4. |
| Host all-target/all-feature Clippy | Passed with warnings denied. |
| Host/runtime API docs | Passed with warnings denied. |
| Static gates | Boundaries/layout, 110 Rust snippets, 1172 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |

Logs and the comparison driver are `/tmp/cellule-live-intent-final-*`,
`/tmp/cellule-live-intent-source.sha256` and `/tmp/cellule-live-intent-verify.py`.
The earlier selected startup/scenario runs are diagnostics, not added coverage.
The isolated CLI binary, source manifest, output and metadata are retained at
`/tmp/cellule-live-intent-cli-evidence/`; binary SHA256 is
`60a3225495cf482b823fd531f369b1ac4e459813ffeaa166da07358273159925`.
Its three intermediate blockers (IncompleteObservation, StaleObservation and
MovementBudget) do not replace the complete final counts or two equilibrium
passes with scheduling enabled. This is a native in-process reference profile,
not process/provider qualification. Verification used Rust/Cargo 1.97.0 with
all features, locked dependencies, `CARGO_INCREMENTAL=0` and separate Workspace
targets. Intermediate failed documentation edits remain in the task record.
Production intent supervision, complete role observation, replacement policy,
maintenance finalization and the full W1–W10 qualification remain unfinished.

### Exact parent CI failures retained

Parent `c40761eb323e480a3b08cac57ebd97d7f8a715ec` workspace
[job 110784161845](https://github.com/crabbuild/cellule/actions/runs/36990132970/job/110784161845)
failed two publication-hint cases at their original two-second bound. Publication
three received 9/19 and 10/19 healthy hints, respectively. Selected native tests
and a full isolated application run pass; they do not establish the x86 Linux
failure's cause. The existing deadlines, healthy-peer sets and assertions remain.
Original logs are `/tmp/cellule-fleet-c407-workspace*.log` and reproduction logs
are `/tmp/cellule-fleet-c407-*-repro.log`.

The follower-capacity
[job 110784162740](https://github.com/crabbuild/cellule/actions/runs/36990133107/job/110784162740)
failed its published-root coverage assertion after seventeen rate windows. The
artifact identifies entities 9, 10 and 11: captured root sequence 330 versus
last acknowledged sequence 331. Its publication telemetry also records successful
sequence-331 publications for these Cells. The driver source captures roots before
requesting node stop. A premature root probe is therefore an inference to
investigate, not an established diagnosis or permission to weaken the gate.
The original ZIP (artifact 11218763624, SHA256
`34cfc1610501fa3fd545cd0497b93d467fae1722a965ae79a1ea3576695f9fe7`)
and all 143 files are retained under
`/tmp/cellule-fleet-c407-follower-capacity*`; the exact mismatch report is
`/tmp/cellule-fleet-c407-root-mismatch.json`. Its process binary SHA256 is
`5716668c0c0e136b0250bfe7cffac4445dc5855bb91ffe3170ff55a5d5f090cf`.

At the recorded capture the same head passes MSRV, contracts, object capacity,
Compose smoke, website, fuzz and fast/negative TLC. Leased/object routing jobs
110784163035 and 110784163093 in
[run 36990133064](https://github.com/crabbuild/cellule/actions/runs/36990133064)
remain confirmed in progress. Broad TLC and simulator are skipped. These parent
checks do not qualify this new source; the full goal remains active.

## October 2 2026 writer profile observation and count convergence checkpoint

The reference observer now captures native pages through `CellNode::fleet_snapshot`
using the original full journal barrier, physical endpoint, fresh nonce and
exclusive deadline. It captures all seven categories and repeats actor topology.
Fresh Cell authority is checked for every catalog-provisioned Cell and reread
before completion; stale actor rows are removed from both count and pressure
inputs. Both canonical advertised-session scans include expired records and
unexpected boots. Directory follower-log discovery includes expired/fenced
leader obligations. The complete journal roster is confirmed after collection.

Complete counts are supported only for the private constructor's closed profile:
three managed boots, twelve catalog-backed SQL writers, and no reader/follower
installation or enrollment. Unbound pages alone supply no absence proof.
Missing or changed authority, Pending/role enrollment, native role installation,
unknown boots or unresolved logs prevent complete counts. Role-enabled production
observation, replacement policy and maintenance finalization remain unfinished.

Capacity samples now renew the original signed boot through canonical directory
CAS. The original signing key and executable identity remain pinned; the local
guard advances only after CAS confirmation. Real classifier sequences and sample
times advance naturally. Fenced, missing or expired boots cannot be recreated.
Withdrawal permits validated same-boot heartbeat successors while preserving
original establishment evidence and requiring joined shutdown plus its exact
permanent directory tombstone.

The new `balance` command drives actual residence and post-batch sample barriers.
Starting at 12/0/0, bounded batches move eight distinct Cells to 4/4/4.
Two further complete passes retain scheduling enabled and allocate no new work.
All eight original command outcomes and SQL values survive, successor authority
is checked, and old handles are fenced. New five-minute receipts cover this
longer scenario; overload/restart retain their one-minute receipts, profiles,
shared bounds and all required two-move assertions. Exit joins all three runtimes,
checks every resource ledger and retires all three exact boot obligations.

### Focused qualification

| Source and evidence | Recorded value |
| --- | --- |
| Parent | `ac739bcdad0232296d3b03a7bd34492873aab224`. |
| Qualified Rust/Cargo manifest | 632 sources/manifests/all lockfiles; SHA256 `6bb973f54342c10a296154111273f77396a385601be2816220bcee6b8633c49f`. Active and isolated source sets both match it after execution. |
| Environment | Rust/Cargo 1.97.0; all features, locked dependencies, `CARGO_INCREMENTAL=0`; separate Workspace targets for active and isolated checkouts. |
| Complete fleet example | 98 passed, none filtered or ignored, in 76.80 seconds; seven cases are new. Selected repetitions are not added. |
| New regression cases | Complete writer coverage and canonical renewal; unknown advertised boot; Pending role/revision change; stale current owner; fenced guard and retirement; a real canonical release preserving eleven unchanged pressure candidates; real-time count convergence/equilibrium. |
| Host all-target Clippy / API docs | Clippy and host/runtime API documentation passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1,172 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |
| Isolated public CLI | `cargo run -p cellule-host --example fleet_operations --all-features --locked -- balance` exited zero. Eight releases, activations, retirements and receipt checks; max inflight two; max restore credit 2,550,136,832 bytes; final counts 4/4/4; three joined nodes and boot retirements. |
| Original CLI executable | SHA256 `b8a813dc120d67c98b080345f6b521a9b2dadba06fef6c1f09369fe60cff8dcd`; executable, raw log, source manifest and metadata archived at `/tmp/cellule-reference-observation-final-cli-evidence/`. |

Logs and manifests are `/tmp/cellule-reference-observation-*`. This is a
real-time in-process reference qualification, without process crash, distributed
provider, sustained workload or mixed-binary evidence. The full W1–W10 objective
remains active and the PR remains draft.

### Intermediate failures and corrections

The first full run passed 94 cases and failed three. Two new fault tests wrongly
expected successful shutdown after deliberately fencing a live writer; the native
owner retained `SharedDrainError(Fenced)`. Cleanup now preserves that original
source across repeated joins, requires non-Stopped status and an unresolved boot
record after failed drain, and still checks zero native resources. The separate
fenced-guard/withdrawal case uses an empty receiver and confirms no advertisement
CAS or boot recreation after fencing. These tests do not manufacture completion.

An intermediate observer discarded all source rows after any topology change,
losing independent pressure candidates. It now disables complete counts and keeps
only unchanged generation/position/cost/blocker rows from both native captures.
A deterministic regression releases one real Cell and requires the other eleven
to remain candidates. Subsequent full runs pass the original overload and
controller-restart assertions. Earlier generic overload/retirement failures lack
sufficient diagnostics to establish their individual causes; detailed pass and
retained-attempt context is now preserved on failure.

A new blanket test assertion prohibited IncompleteObservation even during an
accepted movement batch. That misrepresented the coverage contract: transition
passes can be incomplete and must block count planning. The final test instead
requires complete final counts and two complete equilibrium passes with scheduling
still enabled. An obsolete diagnostic executable retained the earlier assertion;
its terminal 96-pass/one-fail result is recorded separately and is not qualification.
The final rebuilt executable passes all 98 cases. A last review preserves the
original movement error Arc in the count command instead of formatting it away;
the complete suite and isolated CLI were rebuilt and rerun after that change. Intermediate API/test constructor
errors and a needless borrowed journal remain in the logs; failed edit scripts
remain in the task record. Production pressure/residence/deadline bounds and
original assertions are unchanged.

### Parent CI at the recorded capture

Exact parent `ac739bc` workspace/MSRV, contracts, smoke, both capacity campaigns,
website, fuzz and fast/negative TLC pass. Broad TLC and simulator are skipped.
Workspace [run 36983372396](https://github.com/crabbuild/cellule/actions/runs/36983372396)
is terminal success. Compose [run 36983372019](https://github.com/crabbuild/cellule/actions/runs/36983372019)
was still running its leased/object routing comparisons at the captured read;
that timeout/status is not a terminal result. These parent checks do not qualify
the new source. New-head CI and the full plan's remaining gates are required.

## October 2 2026 request bound native page checkpoint

`CellNode::fleet_snapshot` captures one original native category through the
existing two-job fleet action bank. Requests pin the full head/registry,
physical boot, nonce, subject, original native continuation, limit and exclusive
deadline. The journal checks current full versions and endpoint intent in one
transaction before and after capture. Retries preserve the original request and
interval; changed revisions fail instead of restamping a page. Applications
still own authenticated transport and its encoding.

Actor, managed reader, reader producer, inbound follower, managed follower
producer and supervisor pages retain their original native owners, allocation
tokens and error references. Missing owners return explicit Unbound coverage.
The envelope reports actual lifecycle before/after capture, shared mode and
local log identity. A strict runtime binding read preserves poisoned-lock
failure. Reader producer pages now expose their original journal scope and
physical node; persisted formats remain unchanged.

Dropped waiters leave accepted native reads and blocking jobs retained until
join. Effects, inspections and page requests share the original two-job and
retained-byte bounds; shutdown joins their original tasks. Tests hold actual
pre/post authorization calls, cancel waiters, race a registry update after
native capture, reject a foreign boot, inspect the real actor and unchanged
Cell authority, and require all retained credit to return after shutdown.
The independent SQLite client case checks read authorization, deadline,
registry/boot changes and controller-head revision without publishing effects.

| Source and focused evidence | Recorded value |
| --- | --- |
| Parent | `bb08c902e574b8ec40e4580a2a783806abca10e2`. |
| Rust/Cargo manifest | 628 sources/manifests/all lockfiles; SHA256 `9c41f0ee52ae80ddb5929e6158ed6e625377d236d1e7c768c6ec14554554fea6`. |
| Environment | Rust/Cargo 1.97.0; all features, locked dependencies, `CARGO_INCREMENTAL=0`; target `$HOME/Workspace/crabbuild-target/cellule-f9383af7-fleet-operations`. |
| Complete host library / public node | 29 / 88 passed; none filtered or ignored. |
| Selected independent SQLite snapshot authorization | One passed; 90 filtered; none ignored. The complete example suite was not rerun for this checkpoint. |
| Distinct scoped cases | 118 passed, including eight new cases; preliminary repetitions are not added. |
| Clippy | Host all-target Clippy passed with warnings denied. |
| API documentation | Host/runtime all-feature API documentation passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1,172 Markdown links, 28 SQL/peer assertions and 567 validator links passed; document gates were repeated after adding this record. |

Original logs and source manifest are `/tmp/cellule-native-snapshot-*`.
Intermediate compilation caught test comparisons against a non-PartialEq
error, the endpoint test expected the wrong error wrapper, and Clippy caught
an oversized shared job enum. Final tests preserve exact wrapped fencing;
boxing the immutable request fixes the enum without suppressing the lint.

Complete authenticated aggregate observation, current remote authority,
replacement policy, stable traversal and W6–W7 finalization remain unfinished.
The reference observer still reports incomplete coverage; this native page
path cannot enable complete balancing or maintenance on its own.

### Previous checkpoint terminal CI

CI on parent `bb08c90` is terminal: workspace, MSRV, contracts, smoke, both
capacity campaigns, website, fuzz, fast/negative TLC and object-only routing
passed. Broad TLC and simulator were skipped. The leased routing
[job 110747490689](https://github.com/crabbuild/cellule/actions/runs/36978501812/job/110747490689)
failed the unchanged performance gate for `leased/local_command/c1`, after
all eight functional executions passed. Cause remains unestablished. Original
run metadata and logs are `/tmp/cellule-fleet-bb08c90-compose-run.json` and
`/tmp/cellule-fleet-bb08c90-leased-routing.log`. Uploaded artifact
`cell-routing-leased-36978501812-1` has ID 11215841243 and SHA256
`8a542e5db183e14399216e00b41222daa66efdc192cd99c307d4661c68a09821`.
These prior results do not qualify the new checkpoint or complete the plan.

## October 2 2026 retained supervisor observation checkpoint

`CellNode::fleet_durability_supervisor` captures the existing supervisor owner
without waiting on its join lane, provider I/O or native retirement. It exposes
unstarted/running/returned/joined states, an unobserved task exit, request-stop
failure, and the original bounded rotation bank, including automatic claims.
Supervisor and request-stop failures remain separate original references.
Unavailable bank capture is an explicit error; missing, idle, canceled or joined
work supplies no role-absence or safe-finalization proof.

The owner charges four KiB to the shared retained-byte ledger before provider
work starts. `CellRuntime::try_reserve_node_metadata_bytes` permits bounded
lifecycle metadata before lease admission and after fencing. It authorizes no
native work or role; `try_reserve_node_bytes` still checks the node lease, and
terminal drain closes both allocation paths. Capture uses the installed charge
after new byte admission closes. Shutdown releases that owner and its charge.

The original task join is committed before stopping its request bank. A failed
stop cannot leave a consumed JoinHandle available for a second poll. Tests join
real successful and panicked supervisor tasks with a poisoned original bank,
repeat the join, and compare the concrete original source addresses and separate
error Arcs. Public tests capture unresolved retirement and recruitment during
drain, preserve completed original rotation/proof references, and distinguish
unbound, wrong-type, invalid-time and exhausted admission without creating work.
Runtime tests require metadata/native allocations to share the same capacity,
preserve fencing, reject zero/full/closed allocation and release exact credit.

The first complete node run passed 83 cases and failed the existing provider
installation before a lease with `Fenced`. The final metadata path preserves
that startup contract; the original test is unchanged. Earlier public-test
builder omissions and a trait-object vtable identity assertion are retained in
the intermediate logs. Concrete downcast source addresses avoid duplicate
vtable identities while keeping the original-source assertion.

| Source and focused evidence | Recorded value |
| --- | --- |
| Parent | `9faedb991363cbe2d34dfe90690c8e87cc5e605c`. |
| Rust/Cargo manifest | 623 sources/manifests/all lockfiles; SHA256 `f1f06c9f28802aef58710332984425116ab4f01681f903c10533d6c651ab6340`. All indexed blobs match the tested manifest. |
| Environment | Rust/Cargo 1.97.0, all features, locked dependencies, `CARGO_INCREMENTAL=0`, separate Workspace targets for the active and isolated checkouts. |
| Complete host library / public node / fleet example | 26 / 84 / 90 passed; none filtered or ignored. |
| Isolated runtime lease selection | Five passed, 196 filtered; none ignored. Includes the new shared metadata/native credit and fencing case. |
| Isolated complete application integration | 29 passed; none filtered; 16 documented manual cases ignored. |
| Distinct scoped cases | 234 passed, including ten new cases. Repetitions and intermediate runs are not added. |
| Clippy / API documentation | Runtime/host/application all-target Clippy and runtime/host API docs passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1,172 local Markdown links, 28 SQL/peer assertions and 567 validator links passed. Document gates were rerun after adding the final record. |

Both drivers compare every file and the complete path set after each command.
Logs, drivers and manifest are `/tmp/cellule-supervisor-observation-final-*`;
intermediate evidence is `/tmp/cellule-supervisor-observation-*`, including
`pre-startup-fix`. The full plan and authenticated aggregate observer remain
incomplete; this checkpoint is a native-owner observation prerequisite.

### Previous checkpoint terminal qualification

The corrected `9faedb9` Linux ARM64 campaign is terminal. With the pinned image
below, two CPUs, four GiB, zero swap and 8,192 descriptors, run 1 passed 89
example cases and failed `measured_overload_moves_real_cells_after_durable_controller_reconstruction`
with `real movement did not settle both attempts`; its cause is unestablished.
Runs 2–5 each passed all 90 cases, in 28.83, 31.62, 57.06 and 51.62 seconds.
The 128-reader and canonical renewal cases passed all five runs, including
beyond the original 30-second expiry. Every before/build/after source manifest
matches the previous 619-path `fbc5eb81…` source. The original binary SHA256 is
`6ba713e78de3c0de84d6d6b997681b7a6736199bd94255bba47dccad5ee51a14`.
All raw logs, build JSON, exact executable, limits, hashes and terminal no-OOM
state are `/tmp/cellule-host-linux-evidence-reader-pressure-fix/`.
The first failure prevents claiming this entire campaign passes.

Exact-head `9faedb9` workspace
[job 110731568907](https://github.com/crabbuild/cellule/actions/runs/36973266806/job/110731568907)
passes 89 example cases and fails the controller-restart requirement of two
retained lost release replies: one reply and one attempt remain, with one
retirement reported. Cause remains unestablished. Its Compose smoke
[job 110736670337](https://github.com/crabbuild/cellule/actions/runs/36973266798/job/110736670337)
fails `reader-6 made no progress replacement reader replacement` during scale
verification. Both jobs checked out PR merge revision
`3b3bb0a13eae209a845ded4e4ef9316fdc9a930d`, merging `9faedb9` into
`191409685b001a82bd02780def45102b4fc2f164`; the smoke artifact records that
exact source. Original logs are `/tmp/cellule-fleet-9faedb9-{workspace,smoke}.log`;
the original smoke artifact is `/tmp/cellule-fleet-9faedb9-compose.zip`.
Its replacement interval was 14,247,556 microseconds. Reader lane 6 started
three calls in that interval, each returned `behind` at approximately five
seconds, and none supplied an `ok` result wholly inside the interval. This is
failure evidence; the reason for those responses remains unestablished. The raw
artifact SHA256 is `5ab774d86a259c548dae1107118bd806d8685e32ce1fe5c1e985b8b698099900`;
extracted records and the derived summary are `/tmp/cellule-fleet-9faedb9-compose/`
and `/tmp/cellule-fleet-9faedb9-compose-reader-loss-summary.json`.
Contracts, MSRV, both capacity jobs, website, fuzz smoke and fast/negative TLC
pass; broad TLC and the simulator are skipped. Leased routing job 110736670549
completed success at 2026-10-02 07:19:32 UTC; the object-only comparison was
still running at the 07:22 UTC capture. These results do not certify the new
source or complete W1–W10. The new commit requires its own CI; the PR stays draft.

## October 1 2026 reader fixture admission and lease checkpoint

The 128-view pagination case now has explicit native admission headroom and
caller-driven canonical directory renewal. It still requires 128 original
requests, two byte-bounded pages, all 128 installed native views, restored value
readback and joined zero ledgers. The receiver's local guard starts from its
actual published advertisement and advances only after directory refresh confirms.
Expired or withdrawn boot evidence fails; renewal cannot recreate that boot.
No additional background scheduler is introduced.

The original two-GiB native ceiling plus 256 MiB of retained credit caused the
fixture to depend on opening speed. A real 127-view capture measured
1,598,029,824 native bytes and 24,969,216 retained bytes, zero jobs and zero disk
reservations. Their combined utilization exceeds the classifier's 600-permille
recovery threshold. Holding that workload through the real dwell produced
`Constrained` and the original `Capacity("node pressure")` refusal. The clean
regression also reproduced this refusal during the opening loop. The large
fixture now reserves four GiB of native credit on each runtime; its 128 slots,
256-MiB retained ceiling and 192-KiB enrollment charges are unchanged. Ordinary
memory budgets and production profiles/classifier thresholds are unchanged.

Before the last Pending request, the case requires Normal pressure across at
least 1,500 ms of actual sample timestamps. The first corrected full run exposed
another original fixture limit: at 31.98 seconds its unrenewed 30-second
advertisement expired and opening returned `Fenced`. The large case now renews
both original signed boots during opening, the dwell and readback. A new
canonical test keeps those boot identities and their guard live beyond the
original two-second advertisement expiry, then explicitly fences the guard.
No expiry assertion or timing bound is relaxed.

| Source and focused evidence | Recorded value |
| --- | --- |
| Parent | `353904eed67a7e673fc3bc18dcc390e8527473a4`, including the separately authored control-plane plan/audit. Both were preserved identically when advancing the checkout. |
| Rust/Cargo manifest | 619 sources/manifests/all lockfiles; SHA256 `fbc5eb815cfc35d883bc56f84451e8fa52f196ad4934f9070fe2b3090538d835`. |
| Complete host library / public node / fleet example | 20 / 81 / 90 passed; none filtered or ignored. |
| Isolated complete application integration | 29 passed; none filtered; 16 documented manual cases ignored. |
| Distinct scoped cases | 220 passed, including the new canonical renewal case. Repetitions are not added. |
| Clippy / API documentation | Host/application all-target Clippy and host API docs passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, 1,172 local Markdown links, 28 SQL/peer assertions and 567 validator links passed; the Rust/Markdown gates were rerun after adding this evidence. |

Both drivers recheck every source and the complete path set after each command.
Logs, drivers and manifest are `/tmp/cellule-reader-pressure-final-*`.
Original diagnostic/refusal logs are `/tmp/cellule-reader-pressure-*`; the
intermediate complete example failure is retained under `pre-heartbeat`.
An intermediate renewal-helper compilation error and its corrected selected
case are retained under `/tmp/cellule-reader-heartbeat-*`. Diagnostic logging
was confined to the isolated reproducer and is absent from this change.

Five full Linux ARM64 baseline runs used two CPUs, four GiB and zero swap with
the pinned Rust image described below. All failed, including original operating
system file descriptor exhaustion; additional controller/timing failures remain
unexplained. They do not isolate the x86 CI causes. The operating system descriptor
limit was not recorded for that original campaign. Every build/run source check
matches the original 618-file manifest
`9eb1e0bef426cfd02bc3212e12d058fe7d123f7a0bafaa731a2f554d60d96fbf`; original binary SHA256 is
`0c281c09e65e562a774ecc409a207f750c3e49514d060d9e8b49a588611c69ad`.
Source hashes, all five logs, build output, original binary and terminal container
configuration/state are retained in `/tmp/cellule-host-linux-evidence-489c5a9/`.
The corrected source ran separately with an explicit 8,192-descriptor limit;
its terminal results are recorded in the October 2 checkpoint above.

### Current CI limitations

Parent `353904e` workspace
[job 110723513586](https://github.com/crabbuild/cellule/actions/runs/36970580571/job/110723513586)
passes 88 example cases and fails the same large reader admission refusal.
Original log is `/tmp/cellule-fleet-36970580571-workspace.log`.
Previous `489c5a9` workspace
[job 110716061446](https://github.com/crabbuild/cellule/actions/runs/36968076991/job/110716061446)
fails the reader refusal and the controller-restart expectation of two retained
lost release replies. Its contract
[job 110716061458](https://github.com/crabbuild/cellule/actions/runs/36968076984/job/110716061458)
times out on publication 3 with 10 of 19 healthy hints, nine missing peers and
one intentionally stalled transport. Original logs are
`/tmp/cellule-fleet-489c5a9-{workspace,contract}.log`. The latter two causes remain
unestablished. Local passes cannot qualify those failures or complete W1–W10.
The new commit requires its own CI; the implementation PR remains draft.

## October 1 2026 follower producer inventory checkpoint

`CellNode::fleet_follower_enrollments_page` captures every retained original
epoch through the existing supervisor's provider. It includes all selected
member requests and unknown acceptance, original signed-attempt/proof digests,
native dispatch/delivery/closure flags, and original retirement/error Arcs.
Signed ensembles, provider CAS tokens and native proofs are hashed in place
rather than deep-copied. The [producer inventory](../crates/cellule-host/src/durability/enrollment/inventory/mod.rs)
reserves one MiB before copying fixed-size follower metadata. Its 1–32-epoch
pages use the producer's existing 32-epoch bound. Continuations bind every
epoch, pending attempt, shared mode and protocol state; missing keys or changed
progress require restarting the scan.

Capture does not await provider/journal/native calls or acquire the async
protocol lane. A busy lane with zero rows exposes preparation before an original
request exists. An idle lane or zero rows cannot prove a joined supervisor,
empty native lanes or fleet settlement. Typed component lookup now preserves
wrong-type and poisoned-lock failures; unbound `None`, exhausted credit and a
closed runtime cannot be treated as empty coverage. This remains advisory
interval observation, with separate current authority and complete role
envelopes required.

Three new public scenarios use the same SQLite journal, signed directory and
two actual native follower stores. They cover paused preparation, partial member
acceptance, original immutable requests/proof identities, exact page charge and
release, bounds/missing/stale continuations, memory refusal, and original failed
member/error Arcs during requested rotation. The latter pauses the next native
retry before comparing two captures, preserving the original response without
a scheduling race. The existing deadline case additionally requires a closed
runtime to refuse a page while its unresolved original epoch remains visible
through retained completion. Three new unit cases cover cursor validation,
bounded fixed metadata/signing scratch, and native/proof/error progress hashing.
Public component assertions distinguish missing, unowned and wrong-type owners.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `8cff2c3e6e516ebd5b4434e31fd0cad83c24a509` |
| Rust/Cargo manifest | 618 sources/manifests/all lockfiles; SHA256 `9eb1e0bef426cfd02bc3212e12d058fe7d123f7a0bafaa731a2f554d60d96fbf`. |
| Environment | Rust/Cargo 1.97.0, all features, locked dependencies, `CARGO_INCREMENTAL=0`, separate Workspace targets for the active and isolated checkouts. |
| Complete host library target | 20 passed; none filtered/ignored. |
| Complete public host node target | 81 passed; none filtered/ignored. |
| Complete fleet example target | 89 passed; none filtered/ignored. |
| Isolated complete application integration target | 29 passed; none filtered; 16 documented manual cases ignored. |
| Distinct scoped cases | 219 passed, including six new cases; repetitions/intermediate runs are not added. |
| Host/application all-target Clippy and host API docs | Passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, local Markdown links, 28 SQL/peer assertions and 567 validator links passed. |

Both verification drivers compare the complete unchanged Rust/Cargo path set
and manifest after every command. Final reproduction, manifest and logs are
`/tmp/cellule-follower-inventory-final-*`. The active link gate also includes the
independently edited control-plane plan/audit; the final isolated documentation
snapshot excludes them. Intermediate module/helper/ownership/macro compilation
failures are archived under `/tmp/cellule-follower-inventory-*`. An intermediate
full example run passed 87 cases and failed the added page assertion after runtime
closure. The final case retains the original proof assertions and requires
`RuntimeClosed`; the separate live rotation case proves original response
identity before closure. The earlier 219-case run before the deterministic retry
gate is archived as `pre-retirement-gate` and is not added to the final count.

### Publication-hint reproduction and baseline CI

Baseline `8cff2c3` workspace
[job 110698431351](https://github.com/crabbuild/cellule/actions/runs/36962309533/job/110698431351)
failed `pending_reader_activation_retains_a_new_publication_hint` at its
two-second post-publication bound: 28 application cases passed and 16 manual
cases were ignored. The log does not identify the occurrence or missing peers.
The workspace stopped before the host example target. Cause remains unestablished.

Fresh isolated baseline builds passed one initial selected run, 100 serial and
100 concurrent repetitions, plus ten complete macOS application runs. A copied
source snapshot from the baseline Git archive (SHA256
`186c9b451dc62fac9c7d9b39014cb0a94cb37517a3bdd00f161d6eac93ce11db`)
also passed twenty complete Linux ARM64 application runs, each 29 passed and
16 manual cases ignored. The pinned Rust image was
`rust@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`,
with two CPUs, four GiB memory and zero swap in the shared eight-CPU Colima VM.
Linux binary SHA256 was
`fe3f3c993893686b1c14b0adff85a1ce929382b85ded5fe7d366b7f4e08cb8f5`.
These repetitions do not explain the original x86 CI failure or qualify the new
source. Linux post-run source hashes were not retained. The final new-source
application run above has its own unchanged 618-file manifest.

Diagnostic drivers/logs are `/tmp/cellule-hint-repeat*`,
`/tmp/cellule-hint-full-repeat*` and `/tmp/cellule-hint-linux-evidence/`; the latter
includes every original run, build, binary identity and container limits/state.
The dedicated Linux container exited zero without OOM and was removed after
evidence capture. Its build/Cargo volumes remain for reproduction. Initial host
bind mounts failed because the Docker VM cannot follow the Workspace volume's
host symlink; copying the pinned archive into the isolated container resolved
that environment limitation.

The regression now retains partial received-peer state in its timeout assertion
and reports the publication occurrence, elapsed time, missing/held peers and
pending transports. Its two-second bound, five-second periodic tick, healthy-peer
set, duplicate assertions and joined shutdown remain unchanged. No recruitment
algorithm or qualification profile was changed; the failure remains open pending
cause evidence.

Baseline `8cff2c3` passes MSRV, both capacity campaigns, contracts, website,
fuzz smoke, fast/negative TLC and
[Compose smoke and both routing comparisons](https://github.com/crabbuild/cellule/actions/runs/36962309460).
Leased job 110698430939 completed success at 2026-10-02 04:23:26 UTC;
object-only job 110698430922 at 04:41:35 UTC. Broad TLC/simulator were skipped.
These green comparisons do not establish the cause of the historical routing
failures below, and new-source CI remains required.

Complete authenticated role/current-authority envelopes, supervisor and failed
boot evidence, replacement policy, SettleRoles/Finalize, cross-session receiver
recovery, remaining primitive/fault matrices, convergence, complete maintenance
and receiver-loss scenarios, process/provider/mixed-binary qualification and
rollout/runbooks remain required. This checkpoint does not complete W1–W10.

## October 1 2026 reader producer inventory checkpoint

`ReadReplicaManager::fleet_reader_enrollments_page` supplies bounded original
reader requests and progress while acceptance, native opening or result
publication awaits its original reply. The canonical activation lane still owns
all mutations; brief index and progress locks replace the record lock previously
held over I/O. Native execution errors are retained before publication awaits,
independently of journal errors. No alternate opening or retirement path is added.

Every page reserves one MiB before copying, allows 1–128 rows, scans at most
10,000 obligations, and preserves original specifications, acceptance, source,
events and errors. Its fixed-width continuation binds boot/scope, mode, job
counts and all retained row progress. Changes require a fresh scan. Accepted
preparation before a request exists remains visible as running work. Finished
jobs without their original response and handles held by another join owner
remain explicitly unknown. Returned responses do not discard retained epilogue
joins. Unbound managers provide no coverage; closed or exhausted runtime ledgers
fail without synthesizing an empty inventory.

Six new public example cases use the shared SQLite journal and actual native
readers. They cover paused/lost acceptance, paused establishment, preparation
before any journal row, stable multiple pages, progress/retirement/mode changes,
invalid cursors/bounds, ledger admission refusal, and exact page charge/release.
Existing native VFS pause and cordon/refusal cases now inspect progress before
releasing their original owner. The public host inventory case additionally
checks unbound/invalid enrollment capture. Six new unit cases cover cursor
encoding, live/joining jobs, a real panicked owner without its response, returned
protocols, variable byte admission and distinct original error Arcs.

Variable payloads stop copying before the byte boundary and return a continuation,
while every remaining original row still enters the topology hash. An oversized
first row fails rather than being skipped. The large native fixture preserves
128 original requests across pages while the final acceptance is paused (127
installed views plus one Pending request). After that owner resumes, it confirms
all 128 native views and reads their restored value through receipt-bound queries.
Joined shutdown leaves native/retained bytes, SQL jobs and disk reservations at
zero. The fixture explicitly admits 128 slots, 256 MiB of retained credit and a
two-GiB native ceiling; ordinary cases retain their original eight slots,
16 MiB of retained credit and 128-MiB receiver ceiling. Production profiles are
unchanged.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `58721227e56dac3ebcda6b74b4ed2a514f8cc42b` |
| Rust/Cargo manifest | 615 Rust sources, Cargo manifests and all lockfiles; sorted SHA256/path lines. SHA256 `bee6e0b5bdbe8c136830f152462bc7ad006840462ea906374c76bb03c25e9112`. |
| Environment | Rust/Cargo 1.97.0, all features, locked dependencies, `CARGO_INCREMENTAL=0`, this checkout's Workspace target directory. |
| Complete host library target | 17 passed; none filtered/ignored. |
| Complete public host node target | 81 passed; none filtered/ignored. |
| Complete fleet example target | 86 passed; none filtered/ignored. |
| Distinct scoped cases | 184 passed, including twelve new cases; intermediate runs are not added. |
| Host all-target Clippy/API docs | Passed with warnings denied. |
| Static gates | Format/diff, boundaries/layout, 110 Rust snippets, documentation links and 28 SQL/peer assertions plus 567 validator links passed. |

The verification driver checked the unchanged source manifest after every
command. Reproduction, manifest and logs are `/tmp/cellule-reader-inventory-*`.
Initial test compilation used nonexistent role accessors, ordered Digest instead
of its bytes, and reached a too-narrow private join helper; those now use the
canonical source identity, byte ordering and the enrollment family's join helper.
An intermediate example run passed 84 cases and failed an assumption that a
Stopped runtime could admit an inventory page. The final case observes joined
empty manager inventory before runtime closure, then requires `RuntimeClosed`.
The active checkout's intermediate link gate encountered a concurrently edited
control plane document's unfinished audit link. An isolated intermediate snapshot
passed all static gates, and the final active-checkout pipeline passed after that
independent document appeared. Its link count includes those separate edits;
an isolated final snapshot qualifies this checkpoint's committed documentation.
The first large fixture reached the unchanged 16-MiB retained credit limit,
which cannot hold 128 obligations at 192 KiB each; its explicit large-scenario
credit now matches that intended workload. A subsequent publication capture timed
out without native phase diagnostics; a diagnostic rerun passed. The final
scenario pauses acceptance, so capture timing is independent of the last native
open, and still requires all 128 views and readbacks after resume. These
intermediate failures remain archived; their runs are not added to the final
count. Moving example module entries beside their child tests exposed an initial
manifest driver assumption about deleted paths. The corrected driver enumerates
all current tracked/nonignored sources, excludes explicitly deleted entries,
and rediscovers the complete path set after each command. No broad, process or
provider suite ran locally.

### Baseline CI and unresolved qualification

The original `5872122` workspace job
[110679369745](https://github.com/crabbuild/cellule/actions/runs/36956132510/job/110679369745)
failed after 79 example cases passed: the follower failure case observed no
execution error immediately after its 80 ms drain waiter deadline. That deadline
bounds the waiter, not completion of retained native retirement. The test now
awaits the original failure and retirement observation within the fixture's
three-second capture bound, keeping the 80 ms deadline and every original
closure/error/authority/registry/resource assertion. The original CI log does
not establish which native phase was pending. Its terminal failure remains
archived at `/tmp/cellule-fleet-5872122-workspace.log`; the new source needs CI.

Baseline MSRV, both capacity campaigns, contracts, website, fuzz smoke,
fast/negative TLC and Compose smoke passed. Broad TLC/simulator were skipped.
Both routing jobs finished failure after all eight functional executions in each
mode passed. The unchanged median-of-four gate requires p95/p99 at most 110%
and throughput at least 90%. Original failing comparison rows:

| Case | Throughput ratio | p95 ratio | p99 ratio |
| --- | ---: | ---: | ---: |
| `leased/local_query/c1` | 0.98936 | 1.07740 | **1.18368** |
| `object_only/local_query_expired_bursts/c16` | 1.00000 | 1.00871 | **1.15413** |

Leased [job 110680072679](https://github.com/crabbuild/cellule/actions/runs/36956132515/job/110680072679)
completed at 2026-10-02 03:11:43 UTC; object-only
[job 110680072513](https://github.com/crabbuild/cellule/actions/runs/36956132515/job/110680072513)
completed at 03:12:18 UTC. The comparison artifacts preserve raw rows/windows,
logs, manifests and frozen binaries:

| Artifact | ID | ZIP SHA256 |
| --- | --- | --- |
| `cell-routing-leased-36956132515-1` | 11207545900 | `0d86d9cb16c83846e0432468b80425bcdd01f6fc02d9bb2e45bb3cb8b291f7f7` |
| `cell-routing-object_only-36956132515-1` | 11206579342 | `accb887edde0b11f11f9e02338a7e685847216a78be20a1608a6ca71f0fc7dcb` |

Both manifests pin candidate `4b6b091679636b3eeeba985e26f4c9a5164cc44c`,
whose GitHub commit parents are `fbfd84f9c4dfcb6de497072efd9aafa5a6f409cc`
and PR head `58721227e56dac3ebcda6b74b4ed2a514f8cc42b`. The frozen baseline
is `0dc04a658bd99668936f7ec58032d054f6fbc141`. Candidate binary SHA256 is
`5f2914db0810cce008b272fb2e27fb0dbfcd733c1d6bbbed65e1ace77a50f136`,
baseline binary is `08108609e742a3f8091616675152cab3de21f0e021f41588465d60b49a25a83c`,
and both use harness `425b55c168f55ef4b7395c69948027dd77b2bd72836e962daa399120d74149d7`.
The candidate binary matches the prior checkpoint's routing candidate exactly;
this fact alone does not establish the comparison failures' cause. Original ZIPs,
logs and extracted evidence are `/tmp/cellule-fleet-5872122-routing-*`.
No production profile or required evidence was weakened.

The W1–W10 goal remains active. These advisory producer pages are an input to
complete role observation, not authenticated atomic envelopes, current authority,
replacement policy or finalization proof. Complete failed-session and leader
producer observation, receiver-loss recovery, role actions/settlement, primitive
fault matrices, sustained convergence, complete reference scenarios and
process/provider/mixed-binary qualification and rollout remain required.

## October 1 2026 durable roster observation checkpoint

The reconciler now fully traverses retained physical intents and enrollment
records before invoking `FleetObserver::observe`. That adapter receives the
original `FleetRoster`, including Pending work, failed boots, both original
role endpoints and terminal records. Every page uses the canonical codec's
128-row/one-MiB bounds, exact version and strict continuation; each aggregate
is capped at 10,000 rows. The full journal head and registry are rechecked
before/after traversal and after native capture. A lost read or changed version
returns no partial roster. Expired deadlines reject another request before its
adapter future is constructed; dispatched native journal jobs keep their
ordinary retained ownership and join.

Count balancing now also requires exact established boot advertisements,
current physical intents, no Pending enrollment and ownership rows matching
signed counts. An adapter's completeness flag cannot override these checks.
Missing failed boots, unknown live boots, duplicate boot rows, replaced sessions
and omitted writers disable count balancing. Partial pressure relief keeps the
existing source/receiver gates. Every retained row's original status, evidence,
inputs and timestamps enters the roster digest and the planner's v4 input
producer domain. Persisted record codecs and signed peer formats are unchanged.

The reference collector consumes this same roster and continues to report
incomplete native-role coverage. Driver fixtures now explicitly establish their
synthetic boot rows before testing counts; native boot qualification remains
in the separate public startup scenarios. No original assertion or profile was
weakened. New cases qualify multiple pages with 132 intents/130 enrollments and
all statuses, reconstruction, independent commits/lost acceptance replies,
controller-head changes, cursor/version errors, aggregate bounds, source error
preservation, deadlines, failed boot preservation, signed coverage, omitted
writers, enrollment during capture and Pending enrollment with pressure relief.

| Source and focused evidence | Recorded value |
| --- | --- |
| Baseline | `73751090f289f4d302852605f311039adf83d393` |
| Rust/Cargo manifest | 610 tracked/nonignored Rust sources, Cargo manifests and all lockfiles; sorted SHA256, two spaces, relative path lines. SHA256 `4892861ee8518547b233e6bbc5d454131112ab85729cdd6ebd48ba9062cfdefc`. |
| Toolchain/environment | Rust/Cargo 1.97.0; all features, locked dependencies, `CARGO_INCREMENTAL=0`, this checkout's Workspace target directory. |
| Complete host library target | 11 passed; none filtered or ignored. |
| Complete public host node target | 81 passed; none filtered or ignored. |
| Complete fleet example target | 80 passed; none filtered or ignored. |
| Distinct scoped cases | 172 passed, including 17 new cases. Intermediate runs are not added to this total. |
| Host lints/API docs | All targets Clippy and host API documentation passed with warnings denied. |
| Static gates | Format, diff, crate boundaries/module layout, 110 Rust snippets, 1151 Markdown links, 28 SQL/peer assertions and 567 validator links passed. |

The final pipeline checked the unchanged manifest after every command. Logs,
manifest and reproduction driver are `/tmp/cellule-fleet-roster-*`. An initial
compile attempted a private advertisement method and used an obsolete journal
method name; both now use the canonical public APIs. An intermediate pipeline
omitted five nested lockfiles from its manifest selection; the final pipeline
includes all lockfiles and reran every selected gate. No broad/process/provider
suite executed locally, and these scoped checks do not prove fleet qualification.

CI for baseline `7375109` passes workspace/MSRV, both capacity campaigns,
contracts, website, fuzz smoke, fast/negative TLC and Compose smoke. Broad TLC
and deterministic simulator were skipped. Leased job 110671204057 is now
terminal failure (2026-10-02 02:31:11 UTC), after all eight functional runs passed.
The unchanged median-of-four gate requires p95/p99 at most 110% and throughput
at least 90%. `forwarded_command/c1` p99 ratio was 1.25613;
`forwarded_command/c16` p95/p99 ratios were 1.15368/1.11897;
`local_query_expired_bursts/c16` p99 ratio was 1.34762. Throughput passed for
these rows. Object-only job 110671203955 completed success at 02:35:43 UTC.
Artifact `cell-routing-leased-36953458504-1`, ID 11205313244, ZIP SHA256
`dd328ccd12d670f1bcb2f05d3af441ab85abb26dd8a4577d1b524e24b880e724`
preserves the original evidence under `/tmp/cellule-fleet-7375109-routing-*`.
Its candidate source `55ae5c21290319e996b6c4b7506abf6ffdf1d00a` is the
synthetic merge of `7375109` into `fbfd84f9c4dfcb6de497072efd9aafa5a6f409cc`,
compared with frozen baseline `0dc04a658bd99668936f7ec58032d054f6fbc141`.
The cause is unestablished. New source requires independent CI evidence;
earlier comparison failures remain recorded below.

The complete W1–W10 goal remains active. Next, match bounded native actor,
managed/pending reader, cold follower and leader-enrollment observations against
this roster, using authenticated request-bound envelopes and fresh exact
ordinary authority for every obligation, including failed sessions. Replacement
policy, role evacuation/finalization, remaining primitives/fault matrices,
sustained convergence, complete reference scenarios, process/provider and
mixed-binary qualification, rollout and operator runbooks remain required.
Roster traversal, boot coverage and matching writer counts do not establish
SettleRoles, Finalize, or completion of the plan.

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
completed with a failed frozen-binary comparison. All 16 functional executions
passed. `leased/local_command/c16` failed the unchanged median gate:
throughput ratio 0.88824 (required at least 0.90), p95 ratio 1.12470 (required
at most 1.10), and p99 ratio 1.06528 (passed). Reads were 2192 for both.
The cause is unestablished. Artifact `cell-routing-36947731323-1`, ID
11204931295, retains raw rows, windows, logs and frozen binaries. ZIP SHA256:
`140559620e498df8bd134f9902e422cf95c5ba4658846ed15f463af716e32ed2`.
Candidate synthetic merge `51a7ee50ed76d2a37311317d5df6291888714859`
combines PR `cc7aa30` with base `0dc04a658bd99668936f7ec58032d054f6fbc141`.
The newer routing workflow measures its head independently; passing measurements
would not prove this earlier comparison regression resolved.
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

The [embedding example](../crates/cellule-host/minion/README.md)
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

## October 4 2026 failed-owner follower maintenance checkpoint

The public maintenance-policy matcher now consumes an exact canonical
recovered-follower closure together with the matching failed-boot process
closure. At one full snapshot and capture barrier, it verifies the recovered
log was retired, every original member row is present in the current roster,
and the old source process is confirmed retired against that same log. Only
those exact failed-owner source-side follower requests move from
`SourceSuccessor` to `RecoveredFollower`. A recovered log without the process
closure remains unchecked. Planner inputs bind these closures; the new policy
status has its own versioned coverage digest.

The runnable minion scenario exercises successful composition and the
negative case where canonical log retirement alone leaves the maintenance
obligations open. Both preserve the original maintenance capture and do not
grant settlement or finalization.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --all-features --locked --example fleet_operations scenario::recovered_followers::failed_boot::writer_tests::successors::observation::combined::` | 4 passed; 330 filtered out. |
| `cargo test -p cellule-host --all-features --locked --example fleet_operations` | 334 passed; 0 failed; 69.81 seconds. |

The final source shape also passed workspace all-target/all-feature `cargo
check`, warning-denied workspace Clippy, and warning-denied workspace API docs.
Format, boundary, module-layout, Rust-fence, Markdown-link, SQL/peer-contract,
and whitespace gates passed: 137 Rust snippets, 1307 Markdown links, and 28
schema/protocol assertions with 570 validator links. Hosted CI has not yet run
for these uncommitted changes.

### Remaining work at the failed-owner follower checkpoint

This closes one source-side policy gap only. Complete the aggregate
authenticated observation and connect role policy, native/external accepted
work and provider/process retention to `SettleRoles`/`Finalize`. Implement the
host action executor beyond Cordon, remaining live-owner/failed-owner role
evacuation, receiver adoption, Cron/Blob owner handling, broad fault and load
qualification, and the W9–W10 rollout/runbooks. PR #37 is already merged;
the continuation PR #56 was mergeable at this checkpoint, before these local
changes were committed or pushed.

## October 4 2026 maintenance inspection checkpoint

`CellNode::inspect_fleet_action` now accepts fresh maintenance `Inspect`
requests through the existing owned finite-action lane. It authorizes the exact
head, registry and endpoint, reads the live local admission gate, and returns
`Cordoned` only for `Draining`; `Active` and `Cordoned` return the explicit
`IncompleteObservation` blocker. The read performs no action acceptance,
release, role settlement or shutdown. A dropped Cordon result can therefore be
recovered from local state without treating a historical result as current.

Public node tests cover the active and draining responses, a lost durable
Cordon-result reply, preservation of the existing Cell owner, and the rule that
maintenance inspection never reports `RolesSettled` or `Stopped`.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --all-features --locked --test node fleet_maintenance::` | 6 passed; 109 filtered out. |

The full `node` integration target also passed (115 tests). Workspace check,
warning-denied Clippy, and warning-denied API docs passed. Format, boundary,
module-layout, Rust-fence, Markdown-link, SQL/peer-contract, and whitespace
gates passed: 137 Rust snippets, 1307 Markdown links, and 28 schema/protocol
assertions with 570 validator links.

The remaining maintenance inspection gap is the full role, accepted-work,
facility, Stopped, and withdrawal barrier. This local check is only a Cordon
recovery step and cannot move the operation into Closing or Completed.

## October 4 2026 writer-only end-to-end maintenance checkpoint

The `maintenance` command now drives a real three-node reference fleet through
the public reconciler and native node actions. It cordons node 0, moves all 12
SQL Cells to nodes 1 and 2, observes the complete writer-only role inventory,
commits `SettleRoles`, finalizes the drain, withdraws and retires the exact boot,
and verifies each command receipt and final placement. The observer derives
active node sessions from the unresolved Established enrollment rows, so the
post-finalization check can still prove the stopped node is absent while
checking follower references across all physical nodes.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations --locked maintenance_moves_every_cell_then_settles_roles_and_withdraws_the_node -- --nocapture` | 1 passed. |
| `cargo test -p cellule-host --example fleet_operations --locked` | 340 passed; 0 failed; 68.34 seconds. |
| `cargo run -p cellule-host --example fleet_operations --locked -- maintenance` | 12 released, activated and retired; 12 receipt checks; two receivers; final counts `[0, 6, 6]`; maintenance completed and boot withdrawn. |

The workspace all-target/all-feature `cargo check`, warning-denied Clippy, and
warning-denied API docs passed. Clippy prompted sharing the large settlement
proof through `Arc`; the focused maintenance test passed again after that
change. Format, boundaries, module layout, Rust fences, Markdown links,
SQL/peer contracts, and `git diff --check` also passed. The full 340-test
example run preceded only that proof-storage change. The PR #56 routing rerun
and hosted checks for the current local diff have not completed.

This establishes one runnable, real-node writer-only maintenance path. Its
empty reader/follower inventory comes from the scenario's closed writer-only
constructor. It does not qualify reader/follower-enabled maintenance, foreign
live- or failed-owner obligations, receiver loss, controller restart during
maintenance, provider/process failures or rollout operations. PR #56's hosted
routing rerun still targets its earlier committed head and was in progress when
this checkpoint was recorded; it does not cover these local changes.

## October 4 2026 closed-boot role-settlement checkpoint

Role settlement now distinguishes a live maintenance target from one whose
original boot has already been retired. The opaque `FleetRoleSettlement`
retains the exact failed-boot closure digest when the target process is closed,
and verifies that the retired boot, canonical fence, closure digest, full head,
and registry all match the settlement barrier. Missing or ambiguous live/closed
target evidence refuses settlement.

`FleetReconciler` sends that proof through a dedicated closed-boot transport
method instead of dispatching SettleRoles to a stopped `CellNode`. The default
transport refuses this case. The reference SQLite adapter accepts the exact
maintenance action and publishes the checked `RolesSettledAt` result through
the shared journal after validating both opaque proofs. The minion now exercises
the full failed-owner path: complete follower-only role settlement, terminal
`Finalize` from the same fresh failed-boot closure, and no transport call to the
retired `CellNode`. The scenario drops the committed `Stopped` reply, waits
beyond actual controller lease expiry, and completes under a different
controller session replaying the same SQLite journal record.

| Command | Observed result |
| --- | --- |
| `cargo check -p cellule-host --example fleet_operations --locked` | Passed. |
| `cargo test -p cellule-host --example fleet_operations --locked` | 344 passed; 0 failed at the prior full-suite checkpoint. |
| `cargo test -p cellule-host --example fleet_operations --locked failed_owner_maintenance_settles_and_finalizes_only_from_fresh_process_closure` | 1 passed; 0 failed in 4.49s on this checkpoint. |
| `cargo test -p cellule-host --lib role_settlement::tests --locked` | 3 passed; 0 failed. |
| `cargo clippy -p cellule-host --example fleet_operations --locked -- -D warnings` | Passed. |
| `cargo fmt --all --check` | Passed. |

### Remaining W7 work after closed-boot settlement and finalization

Exercise this transport path with the full role matrix, recovered follower
closure and joined process evidence. Closed-boot terminal Finalize is now
covered for a follower-only failed owner, including a lost durable reply and
controller restart; extend that evidence to the remaining role and fault
combinations. Then qualify provider and process faults. W4
receiver-session-loss reconciliation, the W8 receiver-loss executable, and W9–W10
qualification/rollout remain high-priority work. These local changes are
uncommitted; PR #56 remains clean at its prior remote head and does not include
this checkpoint.

## October 4 2026 receiver-route acceptance checkpoint

W4 now has a bounded post-release receiver-route record. Each hop names the
previous and next exact node/session, a nonzero closed-process digest, and a
bootstrapped registry version. The route limits retries to two handoffs,
rejects source/repeated nodes and nonmonotonic registry revisions, and binds the
ordered route into a separate routed-action key and action family. Ordinary
movement action keys and encoded bytes remain unchanged.

First acceptance checks the current head, endpoint and registry together. The
reference SQLite journal rejects first acceptance through ordinary dispatch;
its routed acceptance entry point requires the opaque `FleetFailedBootClosure`
and compares its exact endpoint, digest, and full head/registry snapshot before
publishing. Route history is bounded and rejects competing branches. The journal
can enumerate accepted endpoints after a controller restart, and reconciliation
chooses the unique deepest retained route.

| Command | Observed result |
| --- | --- |
| `cargo check -p cellule-runtime --lib --locked` | Passed. |
| `cargo check -p cellule-host --example fleet_operations --locked` | Passed. |
| `cargo test -p cellule-runtime --lib fleet::operations::tests::contracts::receiver_continuation_is_registry_bound_endpoint_bound_and_versioned --locked` | 1 passed; 0 failed. |
| `cargo test -p cellule-host --example fleet_operations --locked routed_receiver_acceptance_requires_typed_closed_boot_proof` | 1 passed; 0 failed. |
| `cargo clippy -p cellule-runtime --lib --locked -- -D warnings` | Passed. |
| `cargo clippy -p cellule-host --example fleet_operations --locked -- -D warnings` | Passed. |
| `cargo fmt --all --check`, `git diff --check`, boundary and documentation checks | Passed; 137 Rust snippets and 1,307 Markdown links/anchors checked. |

This checkpoint establishes only the durable route contract and acceptance
boundary. The reconciler still selects the original receiver, and the executor
does not yet prepare or recover on a replacement boot. W4 still needs fresh
eligible-node selection, closed-boot route creation before dispatch, new-session
resource admission, takeover of a prior receiver that reached `Serving` or
`Recovering`, and end-to-end unknown/refusal/restart tests. These local changes
remain uncommitted and are not included in PR #56.

## October 4 2026 routed receiver execution checkpoint

The reconciler now consumes a fresh roster and placement capture before it
routes `Activate` or `Cancel`. It requires the exact previous receiver's
retired-process closure, confirms that current advertisements map to established
active boots, excludes source and already visited nodes, projects other charged
attempts, and checks signed headroom before choosing an activation target. It
then first-accepts the routed action through the typed closed-boot journal API
before transport dispatch. The reference minion sends the action to the route's
exact node/session, and its journal validates that endpoint's current active
intent.

A replacement receiver does not inherit the old process's local reservation.
Its routed activation uses ordinary runtime admission after reading canonical
control. It proceeds only from an unowned `Idle` control with the exact released
root and a valid acquisition basis; a same-boot serving result is rechecked
through the actor. A routed retry rereads control under the same accepted route.
Routed cleanup can settle the original receiver's lost credit only after the
closed-process proof is recorded and the routed receiver has no unresolved
local activation.

The path remains fail-closed when the adapter omits process closures, the
roster is incomplete, the replacement lacks advertised capacity, or a previous
receiver may have reached `Serving` or `Recovering`. The latter still needs the
separate canonical failed-owner recovery proof. The normal movement observer
does not synthesize failed-receiver closures; the dedicated test scenario below
uses an explicit test-only process provider. Controller reconstruction, the
Serving/Recovering refusal boundary, app-specific Cell contracts, and network
transport adapters remain to be qualified.

| Check | Observed result |
| --- | --- |
| `cargo check -p cellule-host --example fleet_operations --locked` | Passed after routed dispatch, minion endpoint validation, and new-session activation changes. |
| `cargo clippy -p cellule-host --example fleet_operations --locked -- -D warnings` | Passed after the same changes. |
| `cargo fmt --all --check` | Passed. |
| Process-failure routed execution scenario | Added below; focused test passed. |

These local changes remain uncommitted and are not included in PR #56. The
highest remaining W4 work is controller-reconstruction and refusal/fault
coverage, followed by previous-receiver `Serving`/`Recovering` recovery,
app-contract selection, and qualification of routed remote transports.

## October 4 2026 routed receiver-loss scenario checkpoint

The executable minion fixture now covers receiver loss after durable source
release. It starts three real `CellNode`s, reserves the original receiver,
releases the source, joins the original receiver shutdown, withdraws its exact
canonical session, and publishes a typed failed-boot closure. The observer
recaptures that closure against each current head. Reconciliation then accepts
both `Activate` and receiver `Cancel` at the exact replacement node/session,
restores the released root at the next epoch, retires the attempt, and reads
back the acknowledged SQL receipt and committed value. It replays the exact
accepted `Activate` after the reply is lost and confirms the receiver still has
one active Cell. The test then lets the first controller lease expire and
resumes with a new controller session that adopts the retained action and
completes cleanup. It shuts down all nodes and verifies runtime and
disk-reservation resources are released.

The process provider is a test-only in-process stand-in that treats joined
`CellNode` shutdown as retained evidence. It exercises the provider contract
and routing barrier but is not process-supervision or multi-process
qualification.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations driver_routes_activation_and_cleanup_after_receiver_boot_closure --locked -- --nocapture` | 1 passed; 0 failed. |
| `cargo test -p cellule-host --example fleet_operations successor_tests::driver_ --locked -- --nocapture` | 2 passed; 0 failed. |
| `cargo clippy -p cellule-host --example fleet_operations --all-targets --locked -- -D warnings` | Passed. |
| `cargo fmt --all --check`, `git diff --check`, Rust-fence and Markdown-link checks | Passed; 137 snippets and 1,307 links/anchors checked. |

W4 still needs an explicit routed stale-generation check and refusal when the
closed receiver's control has advanced to `Serving` or `Recovering`. Existing
public cases cover pre-release capacity refusal, duplicate preparation,
activation and cleanup, exact idle-release generation checks, and shutdown
resource joins. App-specific Cell contract selection and routed network
adapters also remain unqualified. PR #56 and PR #37 are merged; this follow-up
work remains local until separately reviewed.

## October 4 2026 accepted receiver activation continuation checkpoint

An accepted activation whose receiver stopped before claiming authority no
longer stalls at failed endpoint inspection. The reconciler can consult fresh
typed process closure, durably accept the bounded replacement route, and invoke
the ordinary receiver executor. A failed inspection or routing attempt keeps
the first original error in the report, bounded to one error per attempt.
Controller deadlines do not authorize this continuation.

The shared real-node fixture now checks both unaccepted and already accepted
activation from the unchanged Idle root, including lost replacement replies,
duplicate replay, controller lease expiry, receipt readback and joined cleanup.
Two further cases stop the original receiver after real authority transitions
to Recovering or Serving but before actor/result publication. Routed execution
leaves that control unchanged, records Unknown without an acquisition basis,
installs no replacement writer, and retains the full fleet charge. Explicit
receiver cancellation also stays unresolved rather than erasing that claim.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --locked -- --nocapture` | 5 passed; 0 failed. |
| `cargo clippy -p cellule-host --example fleet_operations --all-targets --locked -- -D warnings` | Passed for the continuation and shared fixtures. |
| Module ownership, crate boundaries, whitespace, Rust fences and Markdown links | Passed; 137 snippets and 1,307 links/anchors checked. |

These are in-process scenarios with an explicit test-only stopped-process
provider. The next implementation priorities remain canonical recovery of a
previous receiver's failed ownership claim, routed stale-generation and
provider-fault cases, complete maintenance/controller-restart combinations,
app-specific contracts and production transport/process adapters. W9 measured
provider and mixed-version qualification and W10 rollout evidence remain
required. Narrow tests do not establish completion of W4–W10.

## October 4 2026 canonical failed-receiver recovery checkpoint

Routed activation can now recover a failed receiver that already claimed the
Cell. It reads the exact failed control, requires a canonical NodeTakeoverProof
for a process-closed boot in the accepted route, verifies the release prefix,
then calls the existing observed runtime takeover. ReceiverRecoveryBasis is
durably confirmed before the ownership CAS; ReceiverRecoveryEvidence is
confirmed before actor admission. These bounded records use separate kinds 30
and 31 and retain the original routed acceptance. Source recovery and clean
source release keep their existing semantics and bytes.

The SQLite reference adapter retains receiver records in a separate table,
compares immutable inputs on replay, forbids mixing them with an Idle
acquisition basis, and checks the exact retained basis/evidence before publishing
Activated. Fresh serving binds the entire input and recovered control to native
acquisition history, verifies any original pinned overlay suffix and the source
release prefix, and rechecks the actor. Missing canonical proof returns Unknown
without another ownership CAS; missing journal support refuses acquisition.

Two new real-node cases stop the receiver after canonical Serving/Recovering
transitions but before actor/result publication. They recover at the replacement
node, lose and replay the committed reply, wait for controller lease expiry,
reopen an independent SQLite journal client, then retire after exact cleanup.
They preserve the release root, advance through both receiver epochs, read the
original command receipt/value, and join all node resources. The original
refusal tests still prove that an unconfigured recovery provider preserves the
foreign claim and full fleet charge. These are in-process lifecycle cases, not
actual OS crashes or provider-backed qualification.

Hosted workspace CI on PR #57's prior head caught a regression in the earlier
source RecoveryBasis validation: an arbitrary nonzero action key was accepted.
This change restores the exact source Recover key and preferred endpoint checks.
The existing mutation test passes unchanged. The local warning-denied lint also
passes after replacing a newly deprecated atomic update in the reply-loss
fixture with a bounded-count CAS loop supported by the declared Rust minimum.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-runtime --lib fleet::operations::tests:: --locked` | 75 passed; 0 failed, including the CI regression and both new codec cases. |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --locked -- --nocapture` | 7 passed; 0 failed. |
| `cargo test -p cellule-host --test node fleet_receivers:: --locked -- --test-threads=2` | 29 passed; 0 failed. |
| `cargo clippy -p cellule-host --example fleet_operations --all-targets --locked -- -D warnings` | Passed after the final prefix/publication changes. |
| Boundaries, module ownership, whitespace, Rust fences and Markdown links | Passed; 137 Rust snippets and 1,348 local links/anchors checked. |

Highest next priorities are the standalone receiver-loss executable; interrupted
receiver basis/evidence writes and inherited-overlay cases; routed generation
and provider-fault coverage; and the remaining maintenance role/fault matrix.
Production process/transport adapters, W9 measured provider and mixed-version
qualification, and W10 rollout/runbook evidence remain required. PR #57 remains
a draft while hosted CI verifies the updated implementation.

## October 4 2026 standalone receiver-loss checkpoint

Source: parent `b92b038`; implementation/CI diff SHA-256 `a3efc37e199cfd54b227cf322358848ac720c1dc19cab843bdeb470ef14b4954`.
Reproduce this fingerprint from the checkpoint commit with
`git diff --binary b92b038 HEAD -- .github/workflows/rust.yml crates/cellule-host/minion ':(exclude)*.md' | shasum -a 256`.

Minion now exposes `receiver-loss` through the same finite owner used by the
other commands. It reserves one native receiver, proves clean source release,
joins the failed receiver, and publishes typed closure of the exact enrolled
boot. Shared reference adapters supply that closure and lose one committed
routed activation reply. The exported reconciler selects the replacement and
uses ordinary host execution. The command replays the exact retained action,
waits for controller lease expiry, reopens an independent SQLite client, and
settles the unchanged release with controller epoch 2. The former controller
returns Fenced and cannot change the successor journal.

The command checks all twelve original receipts and SQL values, the moved
source handle's fencing, one destination actor, final placement `[11, 0, 1]`,
and unchanged release root/epoch progression. Both journal clients close on
every exit; the outer owner joins all nodes, retires all boots and checks empty
resource ledgers. The closure is joined in-process evidence, not OS crash or
external-job supervision qualification. Other focused successor cases retain
the separate failed Serving/Recovering receiver takeover coverage.

The normal-stack focused test exposed excessive stack use when CLI future
construction was nested with complete receiver observation. Scenario and
continuation factories now construct heap-owned futures before polling them.
The executable and ordinary two-worker tests pass without increasing the stack
or changing deadlines. Hosted workspace CI on the preceding `b92b038` head also
aborted during the overload command and failed an outdated observation-count
assertion. That model now asserts the exact planning, activation and cleanup
capture counts at each phase while retaining all movement assertions. CI now
prints assertion details immediately so a later abort cannot hide them.

| Command | Observed result |
| --- | --- |
| `cargo run -p cellule-host --example fleet_operations --all-features --locked -- receiver-loss` | Exit 0; one release/activation/retirement, twelve receipt checks, epoch 2, one lost activation reply and replay, three joined nodes/retired boots. |
| Same command with `overload` | Exit 0; two releases/activations/retirements, two receipt checks, three joined nodes/retired boots. |
| Same command with `controller-restart` | Exit 0; two lost release replies, epoch 2, two receiver-credit cleanups, two receipt checks, three joined nodes/retired boots. |
| Same command with `maintenance` | Exit 0; twelve releases/activations/retirements and receipt checks, final counts `[0, 6, 6]`, Completed and exact withdrawal, three joined nodes/retired boots. |
| `cargo test -p cellule-host --example fleet_operations receiver_loss_command_preserves_every_receipt_and_joins_all_owners --locked -- --nocapture` | 1 passed; 0 failed. |
| Same test command with `reconciler_tests:: --all-features` | 27 passed; 0 failed. |
| Same test command with `successor_tests:: --all-features` | 7 passed; 0 failed. |
| Same test command with `scenario::tests:: --all-features` | 3 passed; 0 failed. |
| Host example/all-target/all-feature warning-denied Clippy, format, boundaries, module ownership, whitespace, documented Rust and local Markdown links | Passed; 137 snippets and 1,348 links/anchors checked. |

The preceding hosted follower-only maintenance case failed before the abort;
its two focused local reruns pass without changing its deadline, pass limit or
required Completed/withdrawal/follower assertions. Its diagnostic assertion now
retains the complete reconcile report. The broader hosted result remains
unverified until CI runs this updated head. Focused success does not establish
W4–W10 completion.

Highest remaining priorities: explain any current-head CI failures; interrupted
receiver basis/evidence persistence, inherited overlays and stale generations;
the maintenance role/controller/fault matrix; production process/transport
adapters and measured W9 fault/mixed-version qualification; and W10 rollout and
runbook evidence. The four required CLI scenarios are available, while W8's
complete application integration and qualification obligations remain open.

## October 4 2026 interrupted receiver recovery checkpoint

Source: parent `c61fc55`; implementation/CI diff SHA-256
`e333a421e2ad6a98bfe10c459df6a6be0072366c85b7bc8284fd80eb7b6ae33a`.
Reproduce from this checkpoint commit with
`git diff --binary c61fc55 HEAD -- .github/workflows/rust.yml crates/cellule-host/src/fleet/movement crates/cellule-host/minion ':(exclude)*.md' | shasum -a 256`.

Actual transaction-boundary faults exposed a liveness gap: an interrupted
receiver evidence write left a safe native rollback root, but replay could not
publish its result after ordinary acquisition because the recovery evidence was
missing. The host now reconstructs that record only from the exact original
canonical acquisition input/materialization. A retained record keeps its
original capture time. Missing, corrupt or valid-but-substituted history cannot
be replaced by current owner, root equality or epoch counters.

Replay also resumes an Idle rollback itself through ordinary admitted native
acquisition. It confirms the original recovery evidence and verifies its
materialized prefix and the clean release prefix before another ownership CAS.
The original routed acceptance, basis and movement charge remain unchanged;
there is no second movement or replacement Idle basis. A same-session ordinary
acquisition winner passes the same current-serving/history checks. Fresh
inspection performs neither evidence repair nor acquisition.

Nine new real-node cases pause SQLite basis/evidence writes before commit or
after commit before reply. They cover both basis boundaries, both evidence
boundaries, cancelled waiters before and after takeover, another interrupted
reconstruction write, automatic Idle resumption and an ordinary acquisition
winner. Missing/corrupt/substituted native records leave authority unchanged and
retain the full charge; an already retained journal recovery record cannot
replace missing native history. The substituted record comes from a different
fully joined canonical acquisition and decodes successfully for the same
Cell/incarnation/epoch. Each successful case reopens an independent journal
client, compares immutable records, settles through the public reconciler,
resolves the original command receipt/value and joins all node resources.
While paused, active-cell credit is distinguished from actual Owned actor
inventory. Original injected I/O errors remain in the source chain.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --all-features --locked -- --nocapture --test-threads=2` | 16 passed; 0 failed, including the 9 new fault cases. |
| `cargo test -p cellule-host --test node fleet_receivers:: --all-features --locked -- --test-threads=2` | 29 passed; 0 failed. |
| `cargo test -p cellule-host --example fleet_operations reference_observer_reconciles_follower_only_maintenance_to_completion --all-features --locked -- --nocapture --test-threads=1` | 1 passed; 0 failed with unchanged deadlines and completion assertions. |
| `cargo run -p cellule-host --example fleet_operations --all-features --locked -- receiver-loss` | Exit 0; 12 receipt checks, one release/activation/retirement, controller epoch 2, final placement `[11, 0, 1]`, 3 joined nodes/retired boots and lost activation reply replay. |
| Host example/all-target/all-feature Clippy with `-D warnings`, format, boundaries, module ownership, whitespace, documented Rust and local Markdown links | Passed; 137 Rust snippets and 1,348 local links/anchors checked. |

Hosted Rust workspace CI on `c61fc55` ran all 352 minion cases: 351 passed and
one follower-only maintenance collection hit its finite observer deadline.
There was no stack abort or observation-count failure. The new CI scheduling
isolates unrelated fixtures with one harness test thread; every case retains
its native worker concurrency, races, deadlines and required evidence. This is
a scheduling change, not a measured diagnosis or proof that the hosted timeout
is resolved. Hosted current-head results must confirm it. Object-proof,
follower-proof, MSRV, contract and quality checks passed on `c61fc55`; those
results do not qualify this newer source.

Highest next priorities:

1. Verify current-head CI; qualify routed recovery with inherited overlays,
   historical suffix loss/substitution and stale generation/provider faults.
2. Complete maintenance controller loss during evacuation/closing and the
   remaining reader/follower/primitive fault combinations, including external
   Cron/Blob owners and accepted-work/process closure.
3. Deliver production process/transport integration and the versioned W9 fleet
   qualification profile/runner with actual provider, load, resource and
   mixed-binary evidence; exercise W10 rollout, rollback and runbooks.

This remains a focused W4/W5 increment. In-process process-closure providers
and selected test success do not establish completion of W4–W10.

## October 4 2026 interrupted native takeover continuation checkpoint

Source: parent `8e11aca`; implementation diff SHA-256
`18f546f8f5c515376c4d3873610556912bf7b7c5be665e98e21170cd1e536f28`.
Reproduce from this checkpoint commit with
`git diff --binary 8e11aca HEAD -- crates/cellule-runtime/src/cell/actor crates/cellule-host/src/fleet/movement crates/cellule-host/tests/node/fleet_receivers crates/cellule-host/minion ':(exclude)*.md' | shasum -a 256`.

A failed overlay materialization can retain the target's own Recovering control
and inherited suffix. Asking for another failed-owner takeover cannot resume
that same accepted claim. The native runtime now exposes
`resume_takeover_restored_observed`: validate the original fenced input and
current claim, reconfirm the original recorder, then finish without another
ownership CAS. A current materialized claim must equal the complete canonical
result derived from the original pinned manifest. Changed scope, epoch, owner
or publication is refused. It retains immutable native acquisition input/result
before the recorder's pre-admission confirmation. Initial takeover and resume
share one materialization/activation/rollback owner and the existing resource
admission path. No persisted record format or action key changes.

Failed-source Recover and routed receiver Activate now use this native
continuation for their own interrupted claims. Routed resumption verifies the
clean release prefix before native effects and obtains the failed predecessor's
proof from the original basis. Fresh inspection remains read-only.

Two new public host cases use an actual sealed follower suffix. One removes the
manifest, dispatches and replays failed recovery, and confirms the exact owned
Recovering claim/overlay with no actor or acquisition metadata. Restoring the
manifest allows a cancelled replay waiter to join the same accepted native work.
The other pauses a direct native embedding owner after real materialization and
durable recording, interrupts that owner, refuses a changed original control,
then resumes through the host. Both retain the original epoch in the suffix,
activate one writer at the already claimed epoch, and resolve an acknowledged
source command receipt. A historical manifest substitution remains a blocker
while the live actor can still read its acknowledged state.

The added minion routed case models the post-CAS interruption window with a
real canonical authority transition after durable basis confirmation. Replay
creates actual native acquisition/materialization history and activates without
another epoch; independent journal readback and resource joining use the existing
fixture. This model does not represent an OS crash or a complete routed overlay
and controller succession campaign.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --test node fleet_receivers:: --all-features --locked -- --test-threads=2` | 31 passed; 0 failed, including both new public overlay cases. |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --all-features --locked -- --nocapture --test-threads=2` | 17 passed; 0 failed, including the new exact routed-claim replay. |
| `cargo test -p cellule-runtime --test runtime lifecycle::ownership::recovery:: --all-features --locked -- --test-threads=2` | 8 passed; 0 failed; 1 provider case remains explicitly ignored without its documented isolated RustFS environment. |
| `cargo check -p cellule-host --all-targets --all-features --locked` | Passed. |
| Runtime/host all-target/all-feature Clippy with `-D warnings`, format, boundaries, module ownership, whitespace, Rust fences and Markdown links | Passed; 137 snippets and 1,348 local links/anchors checked. |

The previous checkpoint's hosted Rust run `37242017429` completed with failure:
360 of 361 minion cases passed, and the follower-only maintenance observer again
timed out in `fleet-follower-evacuation-deadline`. Serial harness scheduling did
not resolve that failure. MSRV, workspace feature/target checks, API
documentation, workspace unit/integration tests and the balanced three-process
smoke steps passed on `8e11aca`. Those results do not qualify this newer native
implementation. Reproduce and diagnose the observer timeout without changing
its deadlines or required completion evidence; this checkpoint also requires
its own broad CI after publication.

Highest next priorities:

1. Verify hosted CI and combine inherited overlays with routed boot/controller
   succession, historical suffix/lineage faults and provider outages. Complete
   missing-evidence continuation of safely rolled-back failed-source recovery.
2. Complete maintenance controller/role/primitive fault combinations and actual
   accepted external-job/process closure, including Cron/Blob obligations.
3. Deliver the W9 versioned measured fleet profile/runner and provider/process,
   mixed-binary and load/resource evidence; exercise W10 staged operations,
   rollback and runbooks. These remain required for full plan completion.


## 2026-10-05 — Failed-source evidence repair and bound Idle suffix proof

Parent: `15b4f2923a8ea58cab3fcf0cf09cab5e546d3637`. The code-only diff from
that parent, excluding Markdown, has SHA-256
`fafc62ff7fdb11f4b27c9dcd0e46894fa443ecf856af90590a8f87913fde711b`.
This checkpoint advances W4 recovery; it does not complete W4–W10.

Accepted failed-source Recover replay now repairs a missing evidence write only
from the exact original retained basis and immutable native acquisition
input/materialization. A committed evidence reply lost in transport retains the
original record and time. A safe Idle rollback can resume ordinary admitted
acquisition after prefix/origin verification; an ordinary acquisition winner
retains its actor and must satisfy the same original-history checks. Fresh
inspection writes no metadata and starts no acquisition. Failed evidence writes
keep the full attempt charged and preserve their original I/O source error.

Actual sealed-suffix replay exposed a missing pre-acquisition contract: the
Serving suffix verifier correctly rejects Idle. The new explicit
`verify_recovered_idle_prefix` requires a complete unowned Idle control with
cleared overlay, shares the bounded canonical-history/origin walk and runtime
I/O/memory admission, and rechecks the complete control before and after I/O.
It supplies no ownership, actor, retention, role or settlement rights. The
existing Serving verifier retains its strict state requirement.

Ten new public host cases cover lost evidence replies, failed and repeated
writes, cancelled repair waiters, ordinary acquisition winners, missing/corrupt
original history in both Idle and Serving states, valid substituted history,
missing materialized origin, and actual sealed-suffix manifest faults. Restoring
exact original bytes permits replay without another movement permit or rewritten
basis. Completion checks current writer, original canonical evidence, repeated
action identity, acknowledged command resolution and joined resources.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --test node fleet_receivers:: --all-features --locked -- --test-threads=2` | 41 passed; 0 failed, including all ten new cases. |
| `cargo test -p cellule-runtime --lib control::authority::acquisition::tests:: --all-features --locked -- --test-threads=2` | 11 passed; 0 failed. Idle refusal by the Serving API, zero bound, missing acquisition and stale complete control covered. |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --all-features --locked -- --nocapture --test-threads=2` | 17 passed; 0 failed after the temporary deadline probes were compiled. |
| Exact minion `reference_observer_reconciles_follower_only_maintenance_to_completion` | 1 passed; 0 failed, 2.55 seconds locally. This does not establish a hosted CI fix. |
| Runtime/host all-target/all-feature Clippy with `-D warnings`; API docs with `RUSTDOCFLAGS='-D warnings'` | Passed. |
| Format, whitespace, boundaries, module ownership, Rust fences, Markdown links and runtime SQL/peer validator | Passed: 137 snippets, 1,348 Markdown links/anchors, 28 protocol/schema assertions and 571 validator links. |

Authoritative hosted results for parent `15b4f29`: Rust run `37244070031`
finished with workspace failure, 343 of 362 minion cases passing and 19 failing.
Most failures were observation deadlines; the previously failing follower-only
observer failed again. Workspace all-feature/target checks, workspace tests,
API docs, MSRV and the balanced three-process smoke passed before that step.
Follower and object proof checks also passed on the parent. These results do
not qualify the new code. Contract run `37244070048` failed the unchanged
paused-clock LTX compaction test: 500 ms observed versus the required 400 ms.
Neither failure is resolved by this checkpoint.

Temporary test-only `[DEBUG-fleet-57]` probes report failed/cancelled collection
stages, native snapshot waits, policy verification and original elapsed budgets
in the next hosted run. Normal successful observations print no probe output.
No deadlines, qualification profiles or assertions were changed. Remove the
probes after the hosted cause is confirmed and fixed. The isolated ARM64 Linux
observer and LTX repetitions passed earlier; AMD64 emulation failed in native
compiler/linker setup before tests and supplies no AMD64 test evidence.

Highest next priorities:

1. Diagnose the hosted observer and LTX failures with original deadlines and
   evidence intact; verify this checkpoint in CI and keep PR #57 mergeable.
2. Complete the routed inherited-overlay/boot/controller succession campaign,
   historical suffix/lineage and provider faults, and the remaining maintenance
   role/primitive fault combinations and accepted external-job/process closure.
3. Deliver the W9 versioned measured fleet profile/runner, actual provider/process
   and mixed-binary/load/resource evidence, then exercise W10 staged operations,
   rollout/rollback and runbooks. These remain required for full plan completion.


## 2026-10-05 — Routed inherited suffix and controller fault campaign

Parent: `a509e2c1d087f39086c93802837060f0453ea81e`. The code-only diff, excluding
Markdown, has SHA-256
`5127a30bbfbe190133f197722bbde06029f85b14e51e69604ca0b1cc7c4cc7e5`.
This checkpoint adds three selected W4/W5 fault models; full W4–W10 delivery
remains open. Production authority, formats, action keys and profiles are unchanged.

The routed fixture now acquires the released Cell through an actual earlier
native owner, captures a SQLite tail and fsyncs it to every selected follower.
Each original member request is durably Pending before canonical enrollment.
Activation follows all first-append acknowledgments. Native recovery seals and
pins the complete tail; complete member retirement and typed fleet publication
retain every original request before the later receiver's process closure.

Removing that original manifest makes a real preferred-receiver takeover commit
its claim, then fail materialization. The current Recovering control inherits
the exact earlier overlay; no successful acquisition record is fabricated.
Routed recovery retains that control as its original basis while preserving the
manifest's earlier Cell epoch. The additional runtime and all fleet runtimes
join their jobs, actors, descriptors and admission credits before private paths
are dropped. Process evidence remains the documented in-process reference.

The new cases prove:

- Evidence-write failure rolls back safely to Idle; replay confirms the original
  canonical materialization before ordinary admitted reacquisition. The explicit
  native suffix proof identifies the earlier manifest epoch and exact acquisition
  that materialized it, despite a missing interrupted intermediate record.
- Missing/corrupt historical manifests keep the attempt charged with original
  storage/manifest errors and no writer or additional ownership claim. Restoring
  original bytes resumes the same evidence. A replacement controller waits for
  real lease expiry, opens an independent SQLite client, adopts the original route
  and checked result, joins receiver credit and retires the attempt. The old
  controller is fenced and leaves the journal unchanged.
- A manifest outage after a routed ownership CAS preserves that exact Recovering
  claim. Restored bytes permit native resumption without another epoch. A replay
  waiter cancelled at the actual pre-admission evidence write cannot cancel
  materialization or actor admission; the owned action retains exact native
  evidence and the replacement controller joins its completion.

Every completion resolves the original acknowledged command and reads the
actual materialized suffix value. Retention and complete process/provider
qualification remain separate.

Sharing the larger fixture initially caused a reproducible stack overflow in
an existing receiver evidence test, both alone and in the combined suite.
Boxing the shared finite constructor and completion futures fixes that seam
without increasing stack limits or changing test profiles. The same failing
case and the full successor suite then pass. The existing member transport
moved into one focused module and is reused by both fixture families.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --all-features --locked -- --nocapture --test-threads=2` | Final code: 20 passed; 0 failed, 54.13 seconds. An intermediate repeated run hit controller Fenced in an existing history fixture while its journal write was paused; that timing debt remains recorded rather than weakening its expectation. |
| `cargo test -p cellule-host --example fleet_operations successor_tests:: --all-features --locked -- --nocapture --test-threads=1` | Final code: 20 passed; 0 failed, 131.63 seconds using the serial CI harness. |
| `cargo test -p cellule-host --example fleet_operations scenario::recovered_followers::tests:: --all-features --locked -- --nocapture --test-threads=2` | 7 passed; 0 failed after the shared transport move. |
| Host all-target/all-feature Clippy with `-D warnings`; format, whitespace, architecture and module ownership | Passed. |

Hosted parent Rust run `37246672617` is terminal Failure: 343 of 362 minion
cases passed, 19 failed. Its test-only probes localize repeated routed failures
to failed-boot closure with about 1.05 seconds of remaining budget versus about
1.10 seconds elapsed. The follower-only case exhausted its roughly 2.50-second
share during native collection and roster confirmation. No native snapshot hang
is shown by those traces. The exact cause and correction for the consumed
controller partitions remain open; do not extend deadlines or suppress failures.
Contract run `37246672563`, follower/object proof, MSRV and cookbook quality
checks passed on that parent. The intermittent earlier LTX timer failure is
not established as fixed by this one green run. New-head CI remains required.

Highest next priorities:

1. Reproduce and fix the hosted collection/controller-budget failures with
   original profiles and proof checks intact; remove temporary probes only after
   confirming the cause and regression. Diagnose the observed fixture lease-expiry
   race and retain broad CI evidence for each published head.
2. Extend routed provider/backend/lineage faults and successive boot failures,
   complete remaining maintenance role/primitive combinations, and qualify
   actual accepted external-work/process providers, including Cron/Blob closure.
3. Deliver W9's committed measured fleet profile/runner and actual provider/process,
   mixed-version/load/resource campaign; exercise W10 rollout, rollback and
   runbooks. Keep PR #57 synchronized with main and mergeable throughout.


## 2026-10-05 — Native deadline reproduction and canonical signature boundaries

Parent: `ed16bf9cade53c831242f34f3524c4084854c978`. The code-only diff,
excluding Markdown, has SHA-256
`503e4e80ad3ded04d265a36ee71651b51a27a748699cb636a0c475052e1dff2e`.
This checkpoint fixes redundant authentication at specific directory boundaries;
it does not claim green fleet CI or completion of W4–W10.

An isolated native Ubuntu 24.04 workflow reproduced all three selected hosted
failures with their original debug harness, deadlines, controller profile and
assertions. Stage probes measured routed collection at 922 ms: foreign reference
collection/rechecks consumed 631 ms, while journal confirmation used less than
one millisecond. Follower collection took 2.69 seconds, including 1.61 seconds
in those reference scans. This rules out journal confirmation as the principal
cost in these captures; it does not establish a general provider latency bound.

Canonical decoding had already authenticated identity and understood placement
signatures before exact loads, live/advertised scans and follower inventories
verified the same immutable bytes again. Those consumers now retain every fresh
read, canonical-byte/path check and their existing scope/time policy, while
using the decoder's original authentication. Create and heartbeat refresh still
validate signatures before any CAS, then use the one canonical serializer on
the unchanged candidate. Unverified producers retain the checked encode path.
There is no signature cache across reads, changed bytes or directory scans;
formats, signature inputs, error sources, action keys and budgets are unchanged.

Seven regression cases cover all three advertisement forms, verification counts,
canonical emission/readback, changed valid bytes, forged identity/placement
signatures, expiry, future issue time, foreign scope and misplaced paths.
Invalid producers retain their original signature errors and cannot create or
change a canonical record. Three read/scan tests and the producer test failed
with two verification passes before their respective fixes.

Native diagnostics, each selecting one exact case per command:

| Snapshot / run / job | Observed result |
| --- | --- |
| `573214e`; [run 37249579053 / job 111574292502](https://github.com/crabbuild/cellule/actions/runs/37249579053/job/111574292502) | Original routed claim, interrupted evidence write and follower-only maintenance each failed. Stage timings above come from this run. |
| `80691d4`; [run 37250078568 / job 111575760319](https://github.com/crabbuild/cellule/actions/runs/37250078568/job/111575760319) | Both routed cases passed after the read-boundary change. Follower collection improved to 1.81 seconds but policy verification still exhausted its unchanged 2.50-second share. |
| `8a73df3`; [run 37250615027 / job 111577285575](https://github.com/crabbuild/cellule/actions/runs/37250615027/job/111577285575) | Both routed cases passed after producer emission also stopped repeating verification: 6.14 and 4.51 seconds for the complete cases. Follower-only maintenance reached a different failure: native Host snapshot reported the original `fleet-action-journal` Conflict after 329 ms, with 2.17 seconds of observation budget remaining. This is a changed-barrier refusal, not a proven complete CI fix. |

The diagnostic snapshots add only a temporary workflow and stage probes to the
recorded code. They are not protected qualification receipts and do not replace
current-head broad CI. Raw logs are retained in the linked workflow runs.
Temporary probes remain because the follower conflict and broad failures still
need diagnosis; no deadline, pass partition, assertion or qualification profile
was relaxed.

| Command | Observed result |
| --- | --- |
| `cargo test -p cellule-runtime --lib node:: --all-features --locked -- --test-threads=2` | 119 passed; 0 failed, including all seven new cases. |
| Exact minion follower-only maintenance selector | 1 passed locally, 2.30 seconds; native changed-barrier failure above remains open. |
| Serial minion successor suite after the read-boundary change | 20 passed; 0 failed, 127.62 seconds. |
| Serial minion successor suite with the final producer change and probes | 20 passed; 0 failed, 97.32 seconds. |
| Runtime/host all-target/all-feature Clippy and warning-denied API docs | Passed for the final producer change and test additions. |
| Format, whitespace, boundaries, module ownership, document and SQL/peer gates | Passed: 137 snippets, 1,351 Markdown links, 28 protocol/schema assertions and 571 validator links. |

Highest next priorities:

1. Diagnose the native follower registry conflict and preserve its original
   changed-barrier refusal while making safe reconciliation resumable. Verify
   the published head's complete CI, the observed controller-expiry timing debt
   and the intermittent LTX timer case; remove probes after confirmed regression.
2. Extend routed provider/backend/lineage and successive-boot faults, complete
   remaining maintenance role/primitive combinations, and qualify actual external
   work/process providers, including Cron/Blob closure.
3. Deliver W9's measured profile/runner, provider/process and mixed-version/load
   evidence; exercise W10 staged rollout, rollback and runbooks. Keep PR #57
   synchronized with main and mergeable.
