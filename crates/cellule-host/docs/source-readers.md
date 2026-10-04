# Source reader succession

A reader can live on another node while its original writer enters maintenance.
The host checks that exact reader's native retirement against the actual writer
successor and current reader policy before marking its policy obligation checked.
This uses the existing reader manager, actor FIFO, origin verifier and peer path.

| Input | Required evidence |
| --- | --- |
| Original request set | Every immutable original page and the full current roster. |
| Reader retirement | Either the retained `ReaderEnrollmentRetirement` from exact native removal or a `FleetFailedReaderClosure` bound to the original process and accepted-work proof. |
| Minimum prefix | Native removal requires its last installed root after accepted reader work joined. A failed receiver requires at least its original enrollment root. |
| Writer successor | An actual managed native writer on an Active physical node outside maintenance, at a strictly newer epoch. |
| Origin | Canonical derivation of the exact final root and availability of all current dependencies. |
| Reader policy | Exact current revision, incarnation and desired count; absent differs from explicit zero. |
| Replacements | Canonically selected Active boots, exact Established requests and actual ready native prefixes. |
| Capture | One full journal/roster barrier, absolute deadline and monotonic interval of at most 30 seconds. |

## Retain and collect

The embedding application authenticates both endpoints and canonical backend
mappings. Its existing accepted-work owner retains each native retirement or
failed-process closure and returns the same `Arc<FleetSourceReaderInputs>` on
repeated lookup. `None` means unknown. A terminal enrollment row cannot
reconstruct either proof.

After writer handoff, join a live original reader with
`ReadReplicaManager::remove_enrolled`. If its receiver process has stopped,
publish `FleetFailedReaderRetirement` only after the application confirms the
original process and all accepted work are durably closed. Both paths still
require the actual current writer, verified origin and current ready-reader
policy. Ordinary recruitment installs current replacements. The same
still-Active receiver may hold a new request under the successor; that request
cannot replace the original retirement proof.

```rust
use cellule_host::fleet::{
    FleetJournal, FleetMaintenanceEnrollments, FleetReaderEvacuationVerifier,
    FleetRoster, FleetSourceReaderPolicies, FleetSourceReaderSuccessors,
};
use cellule_runtime::Result;
use tokio::time::Instant;

async fn source_policies(
    verifier: &FleetReaderEvacuationVerifier,
    journal: &dyn FleetJournal,
    original: &FleetMaintenanceEnrollments,
    roster: &FleetRoster,
    successors: &dyn FleetSourceReaderSuccessors,
    deadline: Instant,
    clock: impl FnMut() -> Result<i64>,
) -> Result<FleetSourceReaderPolicies> {
    verifier
        .collect_source_readers(journal, original, roster, successors, deadline, clock)
        .await
}
```

The collector visits the complete original/current request set. It verifies
current native writers around origin and reader probes, then globally rechecks
policy, selected boots, native prefixes, provider presence/identity and the full
roster. Changed inputs refuse the capture. Native origin I/O uses the runtime's
existing reservations; applications account bounded metadata copies.

Build the outer `FleetObservation` after all native collection finishes. Retain
`with_source_reader_policies` and `with_maintenance_enrollments` before
`check_maintenance_policies`. Both attachment orders validate the same original
capture, full head, registry, roster and interval. Signed successor/replacement
boots and any captured writer row must match the checked native evidence.
Planner identity v13 binds native and failed-process inputs. Missing evidence
stays `SourceSuccessor`; a checked exact source reader is `SourceReader`.

## Scope and runnable evidence

The canonical minion suite exercises managed writer handoff, refresh past the
original opening, exact native closure, a new reader request on the same
receiver, and failed receiver closure. A failed receiver with desired readers
still set to one remains blocked when no replacement is available; reducing the
canonical policy to zero permits the checked closure. Independent SQLite
journal clients and public reconciliation are also covered. The suite exercises
missing/changed provider evidence, foreign origins, clock/deadline/cancellation
and policy, journal or writer changes behind actual signed native replies.

```sh
cargo test -p cellule-host --example fleet_operations --all-features --locked \
  scenario::reader_tests::evacuation::source:: -- --test-threads=2
```

These local checks do not qualify production process/provider retention or
upgrade an incomplete observation, and they do not authorize SettleRoles or
Finalize. Original writer proof, follower succession, native/external accepted
work and full maintenance remain required by the
[fleet plan](../../../docs/fleet-operations-plan.md).
