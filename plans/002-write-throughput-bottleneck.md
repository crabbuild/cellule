# Plan 002: Measure the write-throughput limit and select one safe optimization

> **Executor:** This is a measurement and decision plan. Do not change
> publication, SQLite, or LTX semantics before the bottleneck is demonstrated.
> Keep all raw provider evidence outside the checkout. Stop on an invariant
> failure rather than altering the qualification profile.
>
> **Drift check:** `git diff --stat dc387a8..HEAD -- crates/cellule-runtime/src/cell crates/cellule-runtime/src/publication crates/cellule-runtime/src/fleet/telemetry.rs crates/cellule-ltx/src/replica crates/cellule-app/tests crates/cellule-app/qualification`
> Re-read any changed path against the facts below before editing.

## Status

- **Priority:** P1
- **Effort:** M for attribution and workload evidence; later optimization is a separate plan
- **Risk:** LOW for bounded instrumentation, HIGH for an unmeasured durability change
- **Depends on:** none; compare its results with Plan 001 before prioritizing code
- **Category:** performance
- **Planned at:** `dc387a8`, 2026-09-29
- **Execution update, 2026-09-29:** Separate response, publication, LTX,
  capture, and upload observations are implemented. A fixed 12-Cell capacity
  selector and verifier are implemented and locally tested. Three isolated
  object-proof repeats passed in CI runs 36649534205 and 36651191247. The
  finer rate ramp and per-operation provider timing narrowed tested bounds,
  but the owner response/publication gap and scheduler-late skewed arrivals
  leave the limiter unresolved. Actor queue and SQL worker timing passed
  integrity checks in CI run 36653277555 attempt 1, but host scheduling
  outliers at 4 actions/node/s made its saturation curves inconclusive. A
  same-revision rerun built but could not start because the bucket-init image
  registry rate-limited the pull. Final CI run 36655966439 passed three
  repeats with the same hot threshold (24 fully served, 32 overloaded actions
  per node per second) and attributed hot backlog to serial object
  publication ahead of a roughly 2 ms SQL worker. Plan 003 now targets that
  measured limiter. Uniform and skewed thresholds and the follower-enabled
  lane remain unresolved. Plan 004 specifies the required networked follower
  fixture and separate capacity evidence. CI run 36659182959 passed another
  three object-proof repeats after the bucket-init image registry change;
  hot throughput was fully served at 16 and overloaded at 24 actions/node/s
  in each repeat, confirming the limiting phase while showing runner-dependent
  absolute capacity.

## Why this matters

Removing a forwarding lookup may improve a request but cannot raise a hot
Cell's write rate if its publisher or durability proof is saturated. The
existing response telemetry distinguishes recorded, follower, and object
proofs, while root preparation and SQL/capture costs are not yet tied to the
same action and offered-load curve. This plan establishes the sustainable
rate and identifies the single phase that limits it.

## Current state and contracts

- `crates/cellule-runtime/src/cell/actor/requests.rs:289` owns
  `prove_command`; `src/cell/actor/admission.rs:146` reports the final
  `CommandResponseSource` and elapsed/confirmation time.
- `crates/cellule-runtime/src/fleet/telemetry.rs:115-177` defines the bounded
  `CellTelemetry` callbacks for response source, publication cost, LTX phase,
  control reads, and admission. New labels must be finite; do not use Cell IDs
  or tenant strings.
- `crates/cellule-runtime/src/publication/mod.rs` serializes each Cell's
  object-root preparation and authority CAS. A follower proof may release a
  response before object publication finishes; completed publication time is
  therefore not automatically response latency.
- `crates/cellule-ltx/src/db/mod.rs:520` has `capture_deferred`, the path used
  by the runtime; a `capture()` comparison has a different sync barrier.
- `crates/cellule-ltx/src/replica/prepare.rs:448-461,549` prepares immutable
  roots and uploads dependencies. The predecessor graph still checks origin
  presence, and missing metadata must fail closed.
- `crates/cellule-app/tests/process_scaling.rs:392-480` schedules 300 writes
  200 ms apart across a 60-second mixed-reader window. Its Python verifier in
  `qualification/scale.py` requires that schedule. It is a correctness and
  5 writes/s profile, not a maximum-throughput curve.
- `crates/cellule-runtime/docs/vfs-ltx-scale-plan.md` work packet 4 lists
  outstanding phase and saturation experiments. Its dated 0.3 ms capture and
  87–139 ms root-preparation figures use different harnesses and cannot be
  subtracted to infer end-to-end latency.

Preserve one fenced writer, ordered receipts, exact-root reconstruction, and
the rule that a successful command follows object publication or a
recoverable follower-log proof. Read root and nearest crate `AGENTS.md` before
changing a crate.

## Scope

**May modify:** finite phase instrumentation in
`crates/cellule-runtime/src/cell/actor/requests.rs`,
`src/cell/actor/admission.rs`, `src/fleet/telemetry.rs`, and focused sibling
tests; a benchmark-only workload under `crates/cellule-app/tests/` with its
integration-suite entry; a corresponding parser/test under
`crates/cellule-app/qualification/`; and a dated report under
`crates/cellule-app/performance/`. Add other production instrumentation only
after a scope review names the exact file and callback.

**Do not modify:** SQL sync mode, LTX format, root validation, authority CAS,
follower quorum, existing qualification schedules, resource ceilings, or
the root's expected evidence. Add a separate workload selector; do not
parameterize the existing 300-write verifier until its contract tests are
updated independently.

## Commands

| Check | Command | Expected |
| --- | --- | --- |
| Focused runtime tests | `CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-capacity-002" cargo test -p cellule-runtime --features test-support --locked` | Pass. |
| App integration | `CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-capacity-002" cargo test -p cellule-app --test integration --locked` | Non-ignored tests pass. |
| Evidence parser | `python3 -m unittest discover -s crates/cellule-app/qualification -p test_scale.py` | Pass. |
| Format and lint | `cargo fmt --all --check` and `CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-capacity-002" cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Both pass. |
| Boundaries/docs | `python3 scripts/check-boundaries.py`, `python3 scripts/check-module-layout.py`, `python3 scripts/check-doc-rust-fences.py`, `python3 scripts/check-doc-links.py`, `node crates/cellule-runtime/docs/validate.mjs` | All pass. |

Run broad suites, process faults, and provider runs in CI or an isolated
snapshot. Set `CARGO_TARGET_DIR` under the mounted Workspace volume with a
directory unique to the checkout. Use a disposable provider and new object
prefix for each run.

## Steps

### 1. Make response and background publication separately visible

Add bounded phase observations around queue wait, SQL handler/commit,
`capture_deferred`, follower append/proof, root preparation, provider I/O,
authority CAS, and final confirmation. Each observation must carry only a
fixed phase/outcome label and timing. Tie phases to one request in an
isolated trace or raw benchmark record, without putting request or Cell IDs
in aggregate metric labels. Record separately (a) which proof released the
response and (b) when background object publication drained. A cancellation
or unknown mutation must not be counted as a successful response.

**Verify:** focused runtime tests pass. Add a unit test that a follower-first
response has exactly one response winner and may have a later object proof;
an object-first response has one object winner; and cancellation produces no
false successful response. Compare event counts with existing committed
sequence assertions.

### 2. Add a scheduled capacity workload without weakening existing tests

Add a new ignored application integration workload with fixed Cell count and
recorded arrival times. Use at least three shapes: one hot writable Cell,
many evenly distributed Cells, and skewed Cells. At each shape, run increasing
offered rates until one fully served rate is followed by an overloaded rate.
Record every scheduled, started, completed, rejected, timed-out, and late
arrival; retain one stable mutation identity for any retry. Check each
acknowledged receipt with a minimum-receipt readback. Record owner/epoch,
CPU, memory, open Cells, publisher queue age, unpublished bytes, root lag,
object GET/HEAD/PUT and bytes, attempts, and p50/p95/p99 for successful and
all scheduled actions. The parser must reject missing samples, an inflated
success rate, a missing readback, and a rate mislabeled as fully served.

Use the existing `process_scaling.rs` scheduled-arrival and
`qualification/scale.py` receipt checks as patterns, but give the new
workload its own selector and evidence filenames. Keep the 300-write mixed
reader workload and its tests unchanged.

**Verify:** app tests and parser tests pass. A deliberately incomplete
synthetic report must be rejected. A completed report must show both a fully
served point and an explicit overloaded point for every workload shape.

### 3. Run paired evidence and locate saturation

In an isolated provider environment, run each shape three times with the
same revision, binary digest, node CPU/memory limits, provider identity,
Cell count, and offered-rate schedule. Run a follower-enabled and an
object-proof-only lane; do not combine their percentiles. Include a cold
activation phase and steady resident phase. Preserve raw samples and logs
outside the checkout, then write a dated summary under
`crates/cellule-app/performance/` linking their locations and checksums.

Calculate the maximum **fully served** logical writes/s for each shape.
Classify each overloaded point as admission, SQL/queue, capture, follower,
publication, provider, or CPU/memory saturation using its phase and resource
evidence. Report published roots/s and root lag independently of response
throughput. A response released on follower proof does not establish that
the publisher can drain indefinitely.

**Verify:** all three repeats agree on the dominant phase for each shape,
or the report explicitly says the result is inconclusive. Every successful
write has readback evidence; no run reports a supported rate at an overloaded
point. The report includes p95/p99 and raw evidence checksums.

### 4. Write the next implementation plan for the measured limiter

If root preparation dominates, inspect its predecessor GET/HEAD, directory
read, immutable PUT, provider wait, and CAS split before proposing a change.
If queue/SQL dominates, inspect worker occupancy and checkpoint/full-image
events. If follower proof dominates, inspect enrollment and append wait.
If the runs disagree or the provider is saturated externally, repeat the
measurement under an isolated profile instead of changing code.

Write a new plan under `plans/` for **one** measured limiter. It must name
the exact source paths, unchanged safety invariants, a before/after workload,
and a numeric gate: improved fully served rate or p95/p99 without more
failed proofs, retry pressure, root lag, or object requests per logical write.
Do not implement that follow-up as part of Plan 002.

**Verify:** the new plan identifies one limiting phase with supporting raw
measurements and a regression test for its corresponding invariant. Update
`plans/README.md` with the next plan and this plan's status.

## Done criteria

- [x] Response-winning proof and later publication are separately measured.
- [x] A new scheduled workload covers hot, uniform, and skewed Cells without
      changing the existing mixed-reader qualification contract.
- [x] Three repeats per shape distinguish fully served from overloaded rates
      and retain raw readback, resource, and object-store evidence.
- [x] A single measured bottleneck has a follow-up implementation plan, or
      the report says precisely why the evidence is inconclusive.
- [x] Focused tests, parser tests, format, lint, boundaries, and docs checks pass.
- [ ] The follower-enabled lane runs separately with its own proof, response,
      root-drain, and recovery evidence on the same scheduled shapes.

## STOP conditions

- A successful response can no longer be tied to exactly one durable proof.
- The workload needs to weaken the existing 300-write/60-second profile or
  omit scheduled arrivals to pass.
- A phase counter would block the actor or add unbounded-cardinality labels.
- Any acknowledged receipt fails readback or owner-loss recovery.
- The measured limiter cannot be distinguished from shared-host provider or
  resource contention after three controlled runs.

## Maintenance notes

Review phase labels when the publication pipeline changes. Preserve both
response rate and eventual root-drain rate in future capacity reports.
Never treat a faster local `capture()` benchmark as evidence for the runtime's
`capture_deferred()` path.
