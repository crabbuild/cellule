# Plan 004: Qualify follower-proof write capacity across three processes

> **Executor:** This finishes Plan 002's separate follower-enabled lane. Keep
> its object-proof workload, provider, Cell count, rate schedule, and proof
> rules fixed. A same-process `LocalFollowerTransport` is not evidence for
> networked follower capacity. Stop if an acknowledged follower cut cannot be
> recovered after owner loss.

## Status and boundary

- **Priority:** P1 for response-latency comparison.
- **Effort:** L; the application fixture currently has no networked
  `NodeLogTransport` or authority enrollment adapter.
- **Risk:** HIGH for transport authorization, CAS ordering, and recovery.
- **Depends on:** [Plan 002](002-write-throughput-bottleneck.md).
- **Status:** TODO.

The existing three-process fixture in
`crates/cellule-app/tests/entities/process.rs` starts object-only nodes.
`crates/cellule-app/tests/process_node.rs` advertises nodes and renews leases,
but does not install follower stores or a durability provider. Runtime
`node/log_transport.rs` defines the transport contract and a deliberately
in-process test implementation; `node/directory/log.rs` owns authoritative
enrollment, activation, coverage, rotation, and recovery authorization.
Keep the network protocol and credentials in the embedding application or its
test fixture, outside `cellule-runtime` and `cellule-app` production APIs.

## Steps

### 1. Add a private-disk, authenticated follower endpoint

Extend the process fixture with a bounded peer listener for append, seal,
retire, and paged tail. Give each node a `FollowerStore` on its existing
private `/scratch` volume. Authenticate the caller and exact session, member,
epoch, operation, and request deadline before touching that store. Use
`NodeDirectory::authorize_log_append`, `authorize_log_retire`, and
`authorize_log_recovery` as appropriate; make frame and response limits
explicit. The client implements `NodeLogTransport` and connects to the
advertised peer endpoint. Exercise refusal of wrong member, stale epoch,
unauthorized retire, oversized batch, and expired deadline in focused fixture
tests. Never treat an HTTP/TCP acknowledgment as follower fsync unless the
store returned its authenticated receipt.

### 2. Enroll one host-owned durability generation

In `tests/process_node.rs`, install the follower store and a
`NodeDurabilityProvider` before `host.start()`. The provider recruits a
complete ensemble through `NodeDirectory::try_recruit_log`, then builds a
`NodeDurabilityConfig` for the exact host session, node, epoch, members,
transport, lease, limits, and telemetry. Serialize its activation, coverage,
rotation, withdrawal, and heartbeat refresh against the same versioned
advertisement; reconcile ambiguous CAS only when the exact session and epoch
still match. Keep the host task group responsible for shutdown and drain.

Add a readiness marker only after all three nodes are live, follower stores
are listening, and each owner has an enrolled generation. Verify the first
follower-proof response is preceded by all-member fsync and authoritative
activation. Test clean rotation, rejected follower append, owner loss before
object publication, sealed-tail replay, and byte-identical root recovery.

### 3. Run the same scheduled shapes in an isolated lane

Add a `capacity-follower` selector to
`crates/cellule-app/tests/entities/process/driver.rs` and
`qualification/scale.py`. Reuse the 12-Cell hot, uniform, and skewed schedule,
arrival accounting, minimum-receipt readback, provider counters, and resource
sampling from the object-proof lane. Record response winner, follower append
bytes and latency, node-log epoch and covered sequence, publication drain,
root lag, retries, and p50/p95/p99 for successful and all scheduled actions.
The parser rejects missing proof records, false fully-served labels,
unrecovered acknowledged writes, or publisher backlog at the end of a drain
window. Keep the existing 300-write qualification profile unchanged.

Add a separate CI job in `.github/workflows/write-capacity.yml` with three
fresh repeats, pinned source/binary/provider digests, fixed resource limits,
and artifact retention. Pre-pull pinned images for the run and retain their
digests so registry quota cannot be mistaken for workload saturation. Compare
the two lanes only at equal offered rates and report sustainable response
rate and published roots/s separately. Run a cold activation and a steady
resident window in each repeat.

### 4. Publish decision evidence

Update `crates/cellule-app/performance/2026-09-29-write-capacity.md` with
per-shape bounds, response-source mix, p95/p99, root drain, provider requests
per logical write, recovery outcome, and checksums of raw external evidence.
Classify each overload using the phase and resource record. If the follower
lane releases responses faster but roots cannot drain, report the stable
response bound separately from publisher capacity. Do not change runtime
durability or publication semantics as part of this measurement.

## Verification and done criteria

- [ ] Focused transport authorization, enrollment/CAS, cancellation, and
      owner-loss recovery tests pass.
- [ ] Parser tests reject missing follower proof, readback, and root-drain
      evidence; format, Clippy, boundaries, module layout, and docs checks pass.
- [ ] Three follower-enabled repeats for each shape have a fully served and
      overloaded point with the same fixed schedule and resource profile as
      the object-proof lane.
- [ ] Every acknowledged receipt survives owner loss or has an exact
      authority-pinned object root; no unsafe replay or false success occurs.
- [ ] Raw logs, samples, source/binary/image digests, and checksums are kept
      outside the checkout; the dated report distinguishes response throughput
      from eventual publication throughput.

Run process and provider checks in CI or an isolated snapshot, with
`CARGO_TARGET_DIR` under `$HOME/Workspace/crabbuild-target` for each checkout.
The STOP conditions are any unauthorized follower operation, a successful
response without its durable proof, a failed exact recovery, or a changed
object-proof qualification profile.
