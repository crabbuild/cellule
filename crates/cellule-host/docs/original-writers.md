# Retain the original failed-boot writer set

Use `FleetOriginalWriterCapture` to retain every original ownership observation
before starting effects that depend on a failed physical boot's complete writer
set. The application authenticates the process and catalog providers. The host
uses the existing authority, catalog readers, fleet registry and finite journal
work owner.

## Integration order

1. Capture and retain the original `FleetFailedBootProcessRequest` with
   `capture_fenced`, before recovering or retiring its leader log. Preserve its
   identity across controller reconstruction.
2. Publish the maintenance operation and its Draining intent in the existing
   journal. Confirm a live controller and recovery claimant.
3. Supply `FleetOriginalCatalogs` with every canonical application/tenant storage
   source the original boot could write. Its durable witness binds authenticated
   original configuration and canonical backend mappings. An empty set requires
   explicit authenticated no-writer configuration.
4. Capture and publish the original set using the same journal transaction
   domain. Confirm publication before dependent effects. On an ambiguous reply,
   use `FleetOriginalWriterInventory::load` to reconstruct the committed manifest
   and every exact page; preserve their original bytes and capture interval.
5. Independently verify every original acknowledged prefix, exact dependency
   availability and current successor serving. Combine complete writer, reader,
   follower and accepted-work evidence before role settlement or node finalization.

The caller accounts bounded metadata work through its existing finite owner and
uses one absolute deadline. Application providers own credentials, authorization,
accepted external jobs and durable process joining. A PID, expired lease, filtered
resident list or successful reconstruction cannot establish a complete set.

```rust
use cellule_host::fleet::{
    FleetFailedBootProcessRequest, FleetFailedBootProcesses,
    FleetOriginalCatalogs, FleetOriginalWriterCapture, FleetOriginalWriterJournal,
};
use cellule_runtime::{Result, identity::SessionId, node::NodeDirectory};
use cellule_runtime::fleet::operations::OriginalWriterInventoryRecord;
use tokio::time::Instant;

async fn retain_original_writers(
    journal: &dyn FleetOriginalWriterJournal,
    directory: &NodeDirectory,
    processes: &dyn FleetFailedBootProcesses,
    catalogs: &dyn FleetOriginalCatalogs,
    original: &FleetFailedBootProcessRequest,
    claimant: SessionId,
    deadline: Instant,
    mut clock: impl FnMut() -> Result<i64>,
) -> Result<OriginalWriterInventoryRecord> {
    let capture = FleetOriginalWriterCapture::capture(
        journal, directory, processes, catalogs, original,
        claimant, deadline, &mut clock,
    ).await?;
    capture.publish(
        journal, directory, processes, catalogs,
        claimant, deadline, &mut clock,
    ).await
}
```

After controller restart, load the current complete journal snapshot and supply
the retained operation ID and original process-request digest. The loader checks
the committed pointer and all ordered pages before exposing an inventory. `None`
means no committed set at that read; only a returned manifest with zero owners
establishes retained empty input. Neither result proves accepted capture work
cannot still publish.

```rust
use cellule_host::fleet::{
    FleetJournalSnapshot, FleetOriginalWriterInventory, FleetOriginalWriterJournal,
};
use cellule_runtime::{Result, identity::Digest};
use cellule_runtime::fleet::operations::OperationId;
use tokio::time::Instant;

async fn reload_original_writers(
    journal: &dyn FleetOriginalWriterJournal,
    current: &FleetJournalSnapshot,
    operation: OperationId,
    original_process_request: Digest,
    deadline: Instant,
) -> Result<Option<FleetOriginalWriterInventory>> {
    FleetOriginalWriterInventory::load(
        journal, current, operation, original_process_request, deadline,
    ).await
}
```

The inventory remains immutable historical input. Its `writers()` iterator spans
every validated page. It does not refresh the collection interval or confirm
current native roles. Aggregate collectors must independently recheck current
authority, every successor prefix, process closure and the complete operation
barrier before acting.

## Collect the original boot's complete sealed suffixes

After publishing the original writer set and canonically sealing recovery, use
`FleetOriginalBootSuffixInventory::collect`. Supply the retained original process
request and the application-authenticated manifest store for that original log.
The collector reads `NodeDirectory::recovered_session`, follows its exact manifest
digest, and includes rows from every application. Every row must match a retained
original owner epoch and exact predecessor. It repeats the original process,
canonical log and full journal checks after manifest I/O.

Open/Recovering logs, uncommitted writers, omitted owners, changed predecessors,
missing/corrupt manifest bytes and changed process evidence refuse. A canonical
log with no manifest still retains all object-covered and rootless writers.
The application accounts the bounded copied writer index and manifest buffers;
the manifest read is bounded to 2 MiB. One absolute deadline spans collection.

This supplies complete metadata inputs for that original log. It does not verify
referenced bundles or current successors. Original Controls also retain inherited
recovery overlays from earlier boots; those must receive their own exact manifest
and successor proofs. Verify every original root and suffix through native prefix
verification, then recheck current native serving and the complete role/work
barrier before settlement. Sealed log recovery permits no log retirement or node
finalization by itself.

## Checks and limits

| Boundary | Required behavior |
| --- | --- |
| Original lifetime | Join the exact original process and every accepted native/external owner through the existing provider; recheck canonical fence, claimant and full roster. |
| Complete sources | At most 128 unique sorted application/tenant scopes; reread the same request-bound source set and durable witness. Providers attest the actual canonical backend, including independent adapters. |
| Catalog traversal | Capture all 256 original heads before pages; ordinary verified page reads and final head/ETag revalidation must complete. |
| Authority history | Inspect every catalog entry, including unused bootstrap absence and owners outside the original boot. Missing legacy/restored history is `OwnerHistoryIncomplete`, never empty. |
| Retained owners | Preserve full original Controls and targets for every matching ownership epoch, including rootless, recovering and object-covered writers. |
| Bounds | At most 10,000 total catalog entries and 10,000 inspected ownership epochs; at most 10,000 retained observations, 64 per page. Record is at most 64 KiB; page is at most 1 MiB. Excess refuses without truncation. |
| First publication | Within the original monotonic 30-second interval and operation/controller deadlines, compare full head/registry, boot and intent; atomically publish all pages, the original pointer and one registry advance. |
| Replay | Exact committed bytes return as historical retention, even after the original deadline. A different original set conflicts. No refreshed timestamp or latest-set replacement. |
| Reconstruction | `FleetOriginalWriterInventory::load` checks the full pointer barrier and validates every immutable page before returning. Missing/corrupt pages refuse the entire set; source errors are preserved. |

Catalog heads are sequential observations. These metadata records provide no
atomic global snapshot, root-retention pin, authority grant or successor proof.
The original boot must already be joined; accepted original work cannot mutate
its catalog after that barrier. Other boots may continue ordinary authority work.

## Reference evidence

The canonical [minion](../minion/README.md) SQLite adapter commits manifest/pages
in the existing accepted blocking-job owner. Its public capture tests traverse
two independently configured application/tenant catalogs and retain writers
removed by actual canonical takeover. They exercise exact replay, reconstruction,
provider errors, missing history, competing clients and canceled/lost replies.
Reload cases distinguish absent and authenticated empty sets, refuse a stale
snapshot or missing/corrupt final page, and preserve the original SQL error.
The suffix fixture publishes real SQLite roots, captures later mutations and
fsyncs their encoded node frames to two follower stores. Ordinary recovery pins
and seals one complete two-application manifest. The collector checks its complete
scope after independent journal reconstruction. Fault cases omit an original,
change a predecessor, remove/corrupt manifest bytes and change or fail the final
process read. These are input-boundary cases, not complete maintenance evidence.
Their joined child is a lifetime stand-in; OS-crashed CellNode, accepted external
jobs, successor prefixes and provider fault qualification remain required.

```sh
cargo test -p cellule-host --example fleet_operations --all-features --locked writer_tests
```
