# Fleet operations implementation evidence

The [implementation plan](fleet-operations-plan.md) remains the full scope.
This page records focused checkpoints; it does not establish complete fleet
balancing, maintenance, or deployment qualification.

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
