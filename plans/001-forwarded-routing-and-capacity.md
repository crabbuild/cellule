# Plan 001: Reduce forwarded owner lookup cost and prove the gain

> **Executor:** Follow the steps in order and preserve raw evidence outside the
> checkout. Do not weaken owner fencing, peer authentication, or the durable
> response gate. If a STOP condition applies, report it before editing further.
>
> **Drift check:** `git diff --stat dc387a8..HEAD -- crates/cellule-peer-http crates/cellule-runtime/src/client`
> If a listed file changed, compare the current-state facts below with live
> code before starting. Re-plan any changed behavior.

## Status

- **Priority:** P1
- **Effort:** L, split into measurement, implementation, and qualification commits
- **Risk:** MED, because a stale route or ambiguous mutation must fail safely
- **Depends on:** none
- **Category:** performance
- **Planned at:** `dc387a8`, 2026-09-29
- **Execution update, 2026-09-29:** Adapter benchmark, bounded owner hint,
  failure tests, and paired comparison completed. The application ingress
  follow-up remains with the embedding application maintainer.

## Why this matters

The generic non-owner path may read catalog and control at ingress, then read
control and a signed node advertisement in the HTTP peer sender before the
request reaches the owner. The sender repeats its two reads on each request.
Removing those reads from a healthy warm path could lower latency and object
store load, but the benefit must be measured against the peer RTT and owner
execution cost. A route observation is only a destination hint; the receiving
owner still verifies and admits every request.

## Current state

- `crates/cellule-runtime/src/client/runtime.rs:53-59,76-92`: the default
  `RuntimeCellTransport` resolver loads catalog and exact control before
  deciding whether a handle is local. A product may supply another resolver.
- `crates/cellule-peer-http/src/lib.rs:93-128,163-195`: `send_inner` permits
  two attempts inside one deadline; `owner` loads exact control and then the
  signed directory record on each attempt. `lib.rs:198-225` caches pinned
  HTTP clients, not owner observations.
- `crates/cellule-runtime/src/client/mod.rs:618-627` provides
  `with_observed_description` so a caller with an authority description can
  skip `Describe`. Do not create a second description mechanism.
- `crates/cellule-runtime/src/peer/dispatch/mod.rs:134-153` authorizes a
  pre-resolved peer request; actor admission fences a stale owner.
- `crates/cellule-peer-http/src/tls.rs:214-220` uses HTTP/1.1 and an existing
  idle connection pool. Change it only if connection evidence calls for it.
- `crates/cellule-app/tests/host.rs` has forwarded action probes, but its
  `peer_round_trip` is a custom TCP fixture from `tests/fleet.rs`. It does
  **not** exercise `PeerHttpRoundTrip` and cannot measure this adapter change.
- `crates/cellule-runtime/docs/vfs-ltx-scale-plan.md` is a historical design
  record. It says an embedding product implemented a bounded owner hint;
  inspect that product separately before proposing ingress edits there.

Follow the root and nearest crate `AGENTS.md`: preserve source errors, use
no `unwrap`, `expect`, or `panic!` outside tests, and keep tests beside the
module. Product HTTP endpoints and authorization are outside Cellule.

## Scope

**May modify:** `crates/cellule-peer-http/src/lib.rs`, `src/tests.rs`,
`docs/routing.md`, and a test-only feature declaration in `Cargo.toml` for
the counting-store fixture. A new sibling test module may be added if the
module-layout check requires one. The executor may update `plans/README.md` status.

**Do not modify:** runtime authority, catalog, actor, peer protobuf, LTX,
follower proof, SQLite durability, read replica policy, product routing, or
resource limits. Do not add a public cache configuration option.

## Commands

Use a unique target directory beneath a mounted `$HOME/Workspace/crabbuild-target`.
For this plan, the examples use `cellule-route-001`:

| Check | Command | Expected |
| --- | --- | --- |
| Adapter tests | `CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-route-001" cargo test -p cellule-peer-http --locked` | All pass. |
| Runtime peer tests | `CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-route-001" cargo test -p cellule-runtime peer --locked` | All pass. |
| Format | `cargo fmt --all --check` | Exit 0. |
| Lint | `CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellule-route-001" cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Exit 0. |
| Layout/boundaries | `python3 scripts/check-module-layout.py` and `python3 scripts/check-boundaries.py` | Both exit 0. |
| Docs/contracts | `python3 scripts/check-doc-rust-fences.py`, `python3 scripts/check-doc-links.py`, `node crates/cellule-runtime/docs/validate.mjs` | All exit 0. |

Run broad suites and provider/process tests in CI or an isolated snapshot.
Use a disposable store and fresh object prefix. Record revision, binary digest,
limits, workload parameters, raw samples, and failures for every comparison.

## Steps

### 1. Establish an adapter-specific baseline

Create one ignored benchmark test in `cellule-peer-http/src/tests.rs`, modeled
on the existing `fixture`, `TestClients`, and local Axum response fixtures.
Set up a valid published control record and signed owner advertisement using
the existing authority/directory APIs. Use a fixture store that counts exact
control and node-record reads; do not bypass record validation. Return a
valid encoded peer reply. Measure one cold send and at least 1,000 warm sends
at concurrency 1 and 16. Record lookup time, HTTP time, end-to-end adapter
p50/p95/p99, successful requests/s, send attempts, and metadata reads per
logical request. A small non-ignored test must assert the report has all
lanes and cannot silently contain zero samples.

Run the benchmark three times on `dc387a8` under fixed CPU and network
conditions, saving raw samples outside the checkout. Check `--list` for the
exact ignored test selector before running it, then require one executed test.
Do not present this local adapter result as product ingress or RustFS capacity.

**Verify:** the adapter test command passes; each of three baseline reports
contains at least 1,000 warm samples in each lane, exact attempted/success
counts, and the expected nonzero control and directory reads. If sender
lookup is below 10% of adapter p95 in all three runs, STOP and report the
dominant measured phase instead of adding a cache.

### 2. Add a bounded advisory owner observation

In `cellule-peer-http/src/lib.rs`, share a private owner-observation map
across `PeerHttpRoundTrip` clones. Key by `CellId` only after
`scope.check_target`. Store the owner session, parsed enrolled endpoint,
pinned certificate and public key, insertion time, and signed node expiry.
Populate only after the existing successful control load, directory load,
and endpoint-match check. Cap at 4,096 entries. Expire each observation at
the earlier of insertion plus five seconds and signed advertisement expiry
minus one second. Do not cache absence or authorization errors.

The first attempt may use a live observation. A proven not-started refusal
invalidates only the attempted session and forces the existing one
authoritative retry under the original deadline. An invalid/lost response
remains an unknown outcome and is never blindly resent. A delayed older
lookup must not replace a newer observation. Hold no cache lock across
provider I/O, TLS construction, or network send. Preserve signed bytes,
peer hop limit, receiver authorization, and actor fencing.

**Verify:** adapter tests pass. Add a counting-store assertion that two
healthy sends to one Cell cause one sender control read and one directory
read total; an expired hint causes one new lookup; and one stale hint causes
at most one authoritative retry.

### 3. Prove failure behavior

Extend `cellule-peer-http/src/tests.rs` using its existing HTTP response
classification fixtures. Cover owner transfer, node expiration or
retirement, endpoint/key change, deletion, five simultaneous expired-hint
callers, 429/503 with and without `Retry-After`, a delayed old lookup, and
response loss after a mutation may have been accepted. Test cancellation
releases cache state and a later request can proceed. Verify that only a
proven not-started outcome is retried and an ambiguous mutation keeps its
unknown-outcome classification. Do not infer correctness from a cache hit.

**Verify:** adapter and runtime peer test commands pass; instrumented tests
show no resend after unknown outcome, at most two sends on a proven
not-started retry, and no send to a stale session after invalidation.

### 4. Compare and decide

Repeat the exact Step 1 adapter benchmark three times on the candidate,
using the same CPU/network limits and request count. Compare cold and warm
p50/p95/p99, successful requests/s at both concurrency levels, control and
directory reads/request, attempts, 503s, and unknown outcomes. Keep raw
samples and per-run paired comparisons.

**Verify:** warm healthy sends have zero sender control/directory reads.
Keep the cache only if warm adapter p95 improves at least 10% in two of three
paired runs **or** fully successful requests/s at concurrency 16 improves at
least 10%, with no p99 regression beyond 5%, no increase in unknown outcomes
or unexplained 503s, and no increase in metadata reads per action. These
are experiment gates, not a product SLO. If the gate fails, revert the cache
change and retain the benchmark evidence.

### 5. Record the next end-to-end and throughput work

Update `cellule-peer-http/docs/routing.md` with the exact benchmark command,
baseline/candidate revisions, paired results, and what the adapter benchmark
does and does not cover. Hand the embedding application maintainer two
specific checks: (1) determine whether ingress shares its bounded owner
hint with the outgoing transport and passes its observed description through
`with_observed_description`; (2) run same-image, fixed-offered-rate local
versus forwarded actions across hot-Cell and many-Cell stages, with scheduled
arrivals, owner loss during load, raw receipt verification, retries, and
object-store requests/action. Inspect that product repo before prescribing
its file edits. The adapter cache alone cannot remove entry-router reads.

For write throughput, use the existing response-source telemetry and the
phase experiments in `crates/cellule-runtime/docs/vfs-ltx-scale-plan.md`
work packet 4. Attribute queue, SQLite, capture, follower proof, root
preparation, provider wait, and control CAS; identify the largest measured
limiter and create a separate implementation plan for it. Do not change
publication or SQLite policy under this routing plan.

**Verify:** the routing document contains the paired adapter results,
the remaining dominant adapter phase, and the product/throughput follow-up
owner. The docs/contracts commands above pass.

## Done criteria

- [x] Three raw baseline and three raw candidate adapter reports exist outside
      the checkout, with cold/warm and concurrency 1/16 samples.
- [x] A warm sender performs zero control/directory reads while its hint is live; miss and
      invalidation retain bounded exact reads and the original deadline.
- [x] Unknown mutation outcomes are never blindly replayed; stale-session,
      cancellation, authentication, and refusal tests pass.
- [x] The numerical Step 4 gate passes, or the cache is reverted and the
      measured reason is documented.
- [x] Focused tests, format, lint, boundaries, layout, and docs/contracts pass.
- [x] Only in-scope source/docs files and `plans/README.md` change.

## STOP conditions

- Drift changed the owner-resolution or retry contract.
- The fixture cannot distinguish an ambiguous outcome from a proven
  not-started refusal.
- A hint would need to grant ownership, bypass receiver authorization,
  extend a signed lease, or relax durable response gating.
- Step 1 shows sender lookup is not a material adapter latency phase.
- A failing verification recurs after one focused repair attempt, or a
  required edit lies outside Scope.

## Maintenance notes

Review cache invalidation when peer response codes or node advertisement
fields change. Keep hint expiry shorter than the signed session lease.
Connection-pool tuning, read replicas, product ingress routing, and write
publication need their own measured decisions.
