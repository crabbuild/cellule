# Signed follower append grants

The receiver can authorize a bounded node-sequence window through fresh
`NodeDirectory` observations, then verify ordinary appends locally. The example
HTTP transport uses this path. `EnrolledPeerVerifier` still represents one
consumed fresh observation; it is never reused as a TTL cache.

## Signed format

`CRBGRNT1` has a fixed 376-byte body followed by a 64-byte Ed25519 signature over
`crab.node-append-grant.v1\0` and the BLAKE3 body digest. Integers are unsigned
big-endian; issue/expiry times must fit valid nonnegative milliseconds. No
trailing bytes or alternate encodings are accepted.

| Body bytes | Binding |
| --- | --- |
| 0–7 | `CRBGRNT1` |
| 8–103 | Fleet, image and release digests, 32 bytes each |
| 104–199 | Leader physical node and boot (16 each), key and certificate digest (32 each) |
| 200–295 | Receiver physical node and boot, key and certificate digest |
| 296–327 | Original physical follower ensemble, two 16-byte slots; unused final slot is zero |
| 328–375 | Epoch, inclusive first/last sequence, coverage floor, issue/expiry time; six u64 values |

One grant covers at most 512 sequences and five seconds, further bounded by
both observed node advertisement expiries. Issuance reads the leader's exact
mTLS-bound enrollment and the receiver's exact boot record. The sender also
refreshes its original receiver pin when renewing. The receiver's local
monotonic horizon starts before those reads; wall-clock rollback and slow I/O
cannot extend it. This is a bounded authorization horizon, not a device
persistence or clock-skew qualification.

## Lifecycle and ownership

1. Install a fresh receiver boot before serving traffic.
2. Acquire the per-lane lifecycle gate before issuance reads or registry changes.
3. Check the current receiver lease, exact original ensemble, epoch and durable
   sealed/retired markers. Only this fresh local path can install a registry entry.
4. On append, authenticate signed request bytes and mTLS identity, match its
   grant digest, check every frame's sequence and native integrity, and clamp
   pruning to the grant's fresh coverage floor.
5. Hold the gate through native `sync_data` and recheck the local grant horizon
   and terminal receiver lease before releasing the receipt.
6. Seal and retire use the same gate and existing synced marker/directory
   barriers. Marker installation invalidates grants. Uncovered retirement fails;
   a lost seal supplies no closure receipt.
7. Collection uses the same gate. A token replay cannot re-register permission
   after marker removal: registry creation requires fresh authority inside the
   gate, and delayed issuance cannot overtake closure. Restart drops the registry
   and uses a new boot, even with a persisted TLS key.

The registry has 1,024 credits and charges each entry to the existing follower
index budget. Rejected/cancelled issuance evicts empty transient memory slots;
invalid append tokens never create lanes. Dispatched native jobs own their gate
through completion. The example serializes renewal and append per member,
retains signed request/response and pinned mTLS checks, and permits one fresh
renewal plus exact-frame replay after an ambiguous first response. Native
sequence/digest checks reconcile duplicates without another SQL execution.
HTTP deadlines are rechecked after native work.

## Evidence and limits

Native tests cover every modified signed byte, key/certificate/session/window
mismatches, fresh-read amortization, expiry, lease loss, original boot rejection,
seal, uncovered retirement, token replay after collection and transient-slot
cleanup. The bounded [write-proof model](../model/README.md) checks lifecycle
interleavings and includes deliberately unsafe fence and expiry configurations.
It abstracts fsync as a successful barrier; physical crash/power-loss behavior,
provider outages and performance gates still require qualification.

Grants do not select a Cell root, upload a bundle, or grant bucket durability.
The existing per-Cell fenced response and recoverable follower proof contracts
remain authoritative.
