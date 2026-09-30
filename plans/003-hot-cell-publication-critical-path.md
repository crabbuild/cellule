# Plan 003: Reduce the hot Cell's object-publication critical path

> **Executor:** First establish a stable baseline on a runner with spare CPU
> for the driver, three one-CPU nodes, and RustFS. Do not change root format,
> authority fencing, response proof, or retention semantics to hit a rate.
> Stop if host scheduling or registry failures prevent a clean A/A baseline.

## Status and evidence

- **Priority:** P1 for object-proof throughput; follower-proof latency is a
  separate lane.
- **Effort:** M for critical-path attribution; implementation effort depends
  on the measured subphase.
- **Risk:** LOW for finite timing observations, HIGH for publication changes.
- **Depends on:** [Plan 002](002-write-throughput-bottleneck.md).
- **Status:** TODO. The limiter is narrowed to serial publication, but the
  current shared-runner capacity thresholds are inconclusive.

[The capacity report](../crates/cellule-app/performance/2026-09-29-write-capacity.md)
retains three raw object-proof repeats in each of two CI runs. In the finer
run, the hot owner's first overloaded window published about 61–66 roots/s;
mean publication time was 14–16 ms/root. Owner capacity refusals began at
24–32 actions/node/s, with zero end-window root lag and no node CPU
throttling. Provider PUT p95 was 6.7–7.5 ms, while capture p95 was under
1 ms. A later instrumented run observed 59–67 ms actor-queue p95 in two
hot overload windows while worker round-trip p95 was about 4 ms. Its
fully-served thresholds varied sharply, including scheduler-late arrivals at
only 4 actions/node/s. This supports examining the serialized publication
token; it does **not** establish a safe code change or a stable maximum rate.

## Scope and invariants

Inspect `crates/cellule-runtime/src/cell/actor/requests.rs` and
`src/publication/mod.rs` for publication serialization and authority CAS;
`crates/cellule-ltx/src/replica/prepare.rs`, `upload.rs`, and
`directory/mod.rs` for predecessor verification, directory update, and
immutable uploads. The embedding application owns any HTTP, credentials,
follower transport, or deployment changes.

Preserve one fenced writer; ordered committed outcomes; a successful response
only after an exact authority-pinned root or recoverable follower proof; all
required predecessor and chunk verification; and bounded retention/admission.
An optimization may not skip origin verification merely because local memory
contains a root or digest. Do not alter persisted IDs, object paths, LTX
formats, or signed peer messages. Read each crate's `AGENTS.md`, producers,
consumers, sibling implementations, and tests before editing.

## Steps

### 1. Establish a clean baseline

Run the existing `--workload capacity` hot and uniform shapes at the same
revision and binary digest for three repeats on a dedicated runner with at
least two CPUs beyond the Compose limits for three nodes and the driver.
Record host CPU pressure and runnable-task delay as well as the existing
node cgroups, provider timings, response proof, root drain, readback, and
scheduled arrivals. Pre-pull pinned images once for the run or use a registry
with sufficient quota; retain exact image digests. No rate is fully served
if any arrival is late, refused, or missing.

**Gate:** All three repeats reach the same fully served and first overloaded
rate interval for the hot shape, with no low-rate scheduler-late arrivals.
If this fails, fix the runner or harness and repeat; do not tune publication.

### 2. Attribute one root's serial time

Add finite, nonblocking timing observations around predecessor graph load,
directory update, immutable dependency upload, root-document upload, worker
`bind_prepared`, authority CAS, and worker `confirm_published`. Keep the
observed root sequence in raw local evidence for correlation, never a metric
label. Report overlap explicitly: do not sum parallel upload percentiles.
Measure count and latency of GET/HEAD/range/PUT per acknowledged write and
identify compaction windows separately from ordinary appends.

**Gate:** For each of three repeats, the same subphase accounts for the
largest avoidable part of the hot Cell's serial critical path. Confirm that
queue growth begins only when offered writes exceed published roots/s. If
the dominant subphase differs by repeat, report the split and stop.

### 3. Change only the measured subphase

- If verified predecessor reads dominate, reuse only work that can be tied to
  the exact current published root and still fail closed on missing origin
  metadata. Test restart, takeover, missing predecessor, and retention races.
- If redundant immutable uploads dominate, prove which content-addressed
  objects are already required by the current authority-pinned root before
  skipping an upload. Invalidate any memo on failure or owner change, and
  verify recovery after deleting a required object.
- If directory computation or compaction dominates, reduce that work without
  changing the canonical checksum or compaction debt bound. Test byte-identical
  roots and restoration across checkpoint and truncate/regrow cases.
- If authority CAS dominates, stop and write a separate authority protocol
  proposal; do not omit or defer the fenced CAS.

Keep one implementation path and no speculative tuning flags. A failed
preparation must leave only unreachable objects; a failed CAS must fence the
owner and cannot release a success response.

### 4. Prove the change

Run paired baseline/candidate builds on the same controlled runner, provider
image, Cell count, node limits, and rate schedule, with three alternating
repeats per build. Require at least **20% more fully served hot logical
writes/s** or **20% lower hot p95 response latency at the same offered rate**.
Require p99 to improve or stay within 5% of baseline, zero failed proofs,
zero readback failures, no higher root lag, and no increase in object requests
per logical write. Report published roots/s separately from response rate;
do not count follower-first responses as proof of publisher capacity.

Run focused runtime/LTX tests, both application integration and parser tests,
format, all-feature check, Clippy, boundaries, module layout, documentation
gates, and the SQL/peer contract validator. Run broad and provider suites in
CI or an isolated verification snapshot using a checkout-specific target
under `$HOME/Workspace/crabbuild-target`.

## Done criteria

- [ ] A stable, integrity-verified A/A baseline identifies the same hot
      publication subphase in three repeats.
- [ ] One change to that subphase preserves all proof and recovery invariants.
- [ ] Paired evidence meets the numeric rate or latency gate without increased
      retry, root lag, failed proof, or object-request pressure.
- [ ] Raw samples, binary/image digests, and checksums are retained outside
      the checkout; the dated report and runnable example are updated.

## STOP conditions

- A scheduler-late or provider-registry error prevents the baseline.
- A cache would hide a missing required object or a stale authority root.
- A proposed shortcut changes the root format, fenced CAS, or response proof.
- Any acknowledged receipt cannot be reconstructed exactly after owner loss.
