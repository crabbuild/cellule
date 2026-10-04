# Safety, failure classes, and limits

LTX CRC64 protects file structure and rolling database state; it is not an
authenticator. Cell roots and objects carry BLAKE3 digests. The embedding
service authenticates the authority record that selects a root.

| Invariant | Effect |
| --- | --- |
| First segment is a full snapshot | No delta-only restore. |
| Ordered, contiguous checksum chain | No gap or reordered mutation. |
| Declared size, hash, page coverage, checksums verified | Corrupt bytes fail closed. |
| Fresh restore destination | Existing state is never overwritten. |
| Fenced capture state | No acknowledgement after an unprovable cut. |
| Cancellation of dispatched work | Caller awaits or reconciles; it does not assume rollback. |

`LtxError::classify()` is the caller contract:

| Class | Caller response |
| --- | --- |
| `Retryable { after }` | Retry within own budget and honor provider delay. |
| `Capacity` | Reduce or release resources; side effect was refused. |
| `Permanent` | Change inputs or selected state. |
| `Ambiguous` | Reconcile before retrying. |
| `Fenced` | Close and restore authoritative state. |

`Limits` bounds file, capture, and database size. `DiskBudget` and host resource
admission bound local scratch, retained cuts, and remote I/O. The host owns
scheduling and cancellation; see [cellule-host](../../cellule-host/docs/README.md).

New recovery and compaction jobs acquire dirty-memory and recovery capacity as
one cohort. While either pool is busy, a new waiter retains neither permit;
ordinary preparations can use the available dirty-memory capacity. A queue
shared by the exact pair of pools prevents competing new cohorts from exchanging
partial permits indefinitely. Existing charged scopes can finish or extend their
work while that queue waits. Both permits and ledger charges follow dispatched
work through completion or cancellation cleanup, with the same resource ceilings.

Semaphore timing counts acquisitions, including retries during cohort admission.
Compaction's total duration also includes time waiting in the cohort queue.

```sh
cargo test -p cellule-ltx --no-default-features --locked
cargo test -p cellule-ltx --features replica --locked
```

External decoder vectors and fuzz entry points live under
[`tests/vectors`](../tests/vectors/README.md) and [`fuzz`](../fuzz/README.md).
