# Immutable root publication

The `replica` feature prepares Cell-scoped objects. Preparation does not grant
a lease or make a root authoritative.

```mermaid
sequenceDiagram
    participant DB as Db
    participant R as CellReplica
    participant O as Object store
    participant A as Runtime authority
    DB->>R: Captured cuts
    R->>O: Upload verified immutable dependencies
    R-->>A: PreparedRoot proposal
    A->>O: CAS exact control record
    O-->>A: Success or conflict
    A->>DB: Prune exact acknowledged batch only on success
```

| Step | Contract |
| --- | --- |
| `CellReplica::prepare` | Verifies cuts and writes immutable root dependencies. |
| `prepare_bundle` | Selects this Cell's exact rows from a shared bundle. |
| `prepare_compaction` | Rewrites representation without changing logical state. |
| `prepare_after_compaction` | Appends to a private compaction while retaining its original authority predecessor. |
| Runtime CAS | Names the authoritative owner and exact root. |
| `Db::prune_captured` | Removes only the successfully published batch. |

A failed CAS leaves unreachable content, never an acknowledged state. Provider
retry pins and rechecks the selected capture bytes, so path replacement cannot
change an in-flight proposal. The host owns request admission, deadlines, and
reconciliation after ambiguous results.

A representation-only compaction can remain private while its successor append
uploads. `prepare_after_compaction` verifies that the compaction preserves the
predecessor's position, commit sequence, Cell and incarnation. The runtime selects
the append's schema and can choose the final root with one CAS
against the original authority record. Every immutable dependency still finishes
uploading before the successor proposal is returned.

## Verify current origin dependencies

`CellReplica::reachable_objects` authenticates the exact root's complete current
origin graph, including root metadata, descriptor pages, directory coverage and
all body/index bytes. A process cache cannot certify availability.
`reachable_objects_bounded` uses the same walk with a caller-selected maximum
inventory count. Zero or excess objects refuse with
`LimitKind::RootInventoryObjects`; callers never receive a truncated inventory.
Directory digests obey that bound while descriptor work keeps the existing fixed
root/segment ceilings. The caller owns memory admission, bounded Store stream
chunks and the enclosing deadline. This graph proof grants no selected authority,
retention pin or current serving.
