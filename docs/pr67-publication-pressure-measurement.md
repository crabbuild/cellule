# PR 67: publication working credit and application comparison

This report describes `6e2ba162`. The subsequent
[root-cut delivery](pr67-root-cut-measurement.md) removes its selected-capture
ownership dependency; that change has no completed fresh qualification.

**Parity and performance acceptance remain unmet; PR #67 remains a draft.**
The retained change fixes excess working-credit retention after root I/O. Two
fresh diagnostics measure the candidate at 589.85 and 583.92 successful Fleet
writes/s. Its repeat worsens successful scheduled p99 and fails warm retry
availability, so these observations do not establish an acceptable performance
improvement or sustainable capacity.

## What the celld comparison measures

Celld's [documented application API](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/docs/README.md)
is JavaScript Workers and Durable Objects. Its daemon is Rust and embeds V8.
The fixture actually deploys `scripts/perf/celld/index.js`: a JavaScript Worker
routes HTTP to an Orders Durable Object using `storage.sql` and
`storage.transactionSync`. Cellule runs an adapted Rust/Axum application from
`crates/cellule-axum/examples/sql.rs`. Neither application bypasses its framework's
response durability gate. Celld is not invoked through its Rust library here.

Both receive requests from identical Rust HTTP client/auditor binaries. That
does not make celld's server application Rust. These are matched HTTP/SQL
application comparisons, with matching payloads, mutations, durable idempotent
responses and recovery checks. They are not identical application runtimes or
SQL statement sequences: routing, metadata/ledger schemas and admission policies
differ. Cellule's peer adapter uses mTLS; this celld fixture uses its signed peer
protocol on the VM's loopback network. These costs are part of the deployed
systems being compared. A TPS ratio alone cannot attribute them to language,
SQL execution, replication or publication.

Celld also has a [Rust library target](https://github.com/denoland/celld/blob/f2bf648663a610eefde71f3547ad61e9b896b1f0/crates/celld/Cargo.toml)
and exported implementation modules. A direct Rust comparison of storage or
replication would be a separate subsystem benchmark. It must use identical
bytes, ordered ranges, concurrency and durability completion boundaries and
audit every ACK. Calling local SQLite or a log store directly does not measure
the application's complete fenced response path. No such subsystem measurement
is claimed by this report. See the [harness methodology](../scripts/perf/README.md).

## Retained change and deterministic verification

Root materialization previously held its `6 × suffix bytes + 4 MiB` working
reservation through a queued authority checkpoint callback, although root I/O,
overlay buffers and preparation had finished. The original materializer now
splits the pre-admitted retained-prefix credit and drops working credit before
awaiting that callback. The retained witness remains charged through the
original callback and SQLite worker bind. Completion, publication accounting
and drain still wait for those original operations. Error/cancellation releases
remaining guards. The eight-materializer and 64-MiB retained bounds are unchanged;
formats, fresh proof checks and issued-range closure are unchanged.

The regression uses real managed actors and the original authority, holds
checkpoint callbacks before delegation, commits later selected commands, and
observes canonical roots after root I/O. The identical final fixture fails on
the old production path at 30,604,276 retained bytes. The fixed path reports
21,102,068 bytes initially and 21,102,032 bytes in three repeated runs, below the
unchanged producer reservation plus bounded prefix metadata. Each run resumes
the original callbacks, checks reads/retries and complete shutdown, restores
every command outcome cold, and checks zero remaining resource credit before
the final credit verdict. Earlier incorrect test seams and their failures are
preserved outside Git. This demonstrates the resource-lifetime fix; it does not
prove end-to-end TPS or availability acceptance.

## Two fresh application diagnostics

The baseline is retained production `80caaeab`; the candidate is `6e2ba162`;
celld is pinned to `f2bf6486`. Each fresh case uses 2,000 uniformly addressed
Cells, 96-byte SQL-ledger values, 128 clients/queue slots, 2,000 offered writes/s,
no reads, 30 seconds of warmup and 60 measured seconds, one owner/two followers,
tmpfs local state, WAL NORMAL and a fresh pinned RustFS volume. The first order
is candidate/baseline/celld; the repeat reverses to celld/baseline/candidate.
Both pairs use identical candidate, baseline, client and auditor binaries.

| Pair | Arm | Successful writes/s | Successful scheduled p99 ms | Errors | Dropped offers |
| --- | --- | ---: | ---: | ---: | ---: |
| First | Cellule baseline | 388.70 | 530.70 | 40,771 | 55,782 |
| First | Credit candidate | 589.85 | 400.37 | 22,986 | 61,489 |
| First | celld | 1,646.95 | 606.11 | 0 | 20,927 |
| Repeat | Cellule baseline | 535.17 | 345.91 | 43,433 | 44,324 |
| Repeat | Credit candidate | 583.92 | 408.79 | 18,906 | 65,862 |
| Repeat | celld | 1,997.87 | 53.07 | 0 | 117 |

TPS counts successful completions within the measured window. Successful p99 is
reconstructed from successful measured offers through client drain, starting at
scheduled arrival and excluding fast refusals. Every original response, offer,
attempt, error, drop, trailing completion, per-Cell count and payload count is
independently reconciled against hashed raw journals. All 2,000 Cells have
measured successes in all six cases. The candidate has 35,391/35,035 in-window
successes and 134/197 trailing successes.

The first pair observes 51.75% more TPS and 24.56% lower p99. The repeat observes
9.11% more TPS but 18.18% higher p99 and failed warm availability. The unchanged
baseline varies 388.70–535.17/s and celld varies 1,646.95–1,997.87/s; celld p99
also varies substantially. These are short overloaded diagnostics, not an A/A
variance study or proof of a repeatable causal capacity gain.

| Pair/arm | Complete ACK rows | Warm mutations/original retries | Cold audit | Joined successful-case drain |
| --- | ---: | --- | --- | ---: |
| First baseline | 47,489 | All pass | All pass | 63.95 s |
| First candidate | 66,839 | All pass | All pass | 58.15 s |
| First celld | 160,892 | All pass | All pass | 18.47 s |
| Repeat baseline | 58,320 | All pass | All pass | 47.17 s |
| Repeat candidate | 67,909 | 713 HTTP 503 retry errors | Not reached | No successful-case drain result |
| Repeat celld | 181,716 | All pass | All pass | 14.49 s |

The repeat candidate's failed warm audit and original cleanup are retained;
it is not retried into a passing result. Its cleanup logs all 2,000 Cells idle,
but the case is incomplete and has no cold recovery evidence. HTTP 503s establish
failed availability, not acknowledged-data loss. None of the six cases passes
qualification; even celld drops offers in both measured windows.

## Debt and cost limit the result

| Pair/arm | Retained RAM start → end, MB | Unpublished log start → end, MB | Capture bytes start → end, MB | Oldest publication start → end, s | Total observed store bytes/success |
| --- | --- | --- | --- | --- | ---: |
| First baseline | 63.35 → 65.09 | 25.59 → 42.63 | 2.77 → 0.25 | 52.96 → 92.46 | 78,056 |
| First candidate | 54.55 → 49.86 | 41.96 → 82.03 | 12.38 → 18.10 | 51.37 → 69.75 | 84,609 |
| Repeat baseline | 63.18 → 66.23 | 28.00 → 51.01 | 0.22 → 0.49 | 49.31 → 58.62 | 77,543 |
| Repeat candidate | 50.01 → 55.24 | 45.87 → 81.36 | 12.70 → 14.06 | 50.39 → 71.82 | 92,177 |

MB is decimal. These store ratios include reads, writes and all coordination
families, normalized by in-window successful commands; they are concurrent
window observations, not individual request traces. Candidate total bytes per
success rise 8.40%/18.87%. Lower retained RAM does not establish bounded debt:
both candidate log backlogs grow and later captures remain held while the
publisher completes root callbacks. Materialization still owns the publisher
needed by selected-capture cleanup. Removing that ownership dependency requires
exact tracking of the materialized prefix and later suffix, separate publication
obligations, and worker binding that preserves later selected captures.

Native submission's seven measured phases reconcile exactly. Global-order wait
averages below 0.003 ms and publication-slot wait below 0.001 ms in all four
Cellule windows. The separate Fleet-proof cohort averages 63.72–82.33 ms;
followers group approximately 8.55–11.23 delivered frames per data sync. These
overlapping caller waits cannot be added as serial service time. Each follower
still awaits one grouped RPC. Full ordered streaming remains open work.

The verified reference also defaults to one-shot HTTP: the recorded celld cases
do not set `CELLD_LOG_TRANSPORT=stream`. Its default pipeline is four and its
stream window is zero, while Cellule uses eight rounds. Streaming is a celld
capability, not an explanation established by these default-transport results.
The [core-path contract](write-performance-proposal.md#core-write-path-reference)
records the actual reference configuration and remaining publication/capture
differences. A subsequent stream-interface regression spike is unintegrated,
preserved externally and removed from production; no throughput gain is claimed
for it.

A separate paged-catalog experiment is rejected: one 2,000-Cell single update
reduces encoded metadata 62.36%, but a uniform 64-Cell cohort saves only 0.37%
and adds another metadata I/O stage. Its fresh run falls 436.55→364.37 writes/s,
worsens successful p99 419.97→1,000.33 ms and fails warm availability. That
format is absent from the retained change. Single-update savings are not
extrapolated to the node-wide workload.

## Delivery limits

All 16 isolated verification routes pass on `6e2ba162`: full workspace tests
(2,000 reported passing executions, zero failures, 38 environment-dependent
ignores), 60 local LTX tests, 42 script tests, all features/targets, Rust 1.97/1.99
Clippy with warnings denied, API docs, boundaries/layout, Rust fences/links,
SQL/peer contracts and three repeated credit regressions. Documentation-only
delivery receives separate checks and preserves measured executable source.

The whole Docker VM has eight CPUs and approximately 8 GiB shared by owner,
followers, store and client. Container 8-CPU/16-GiB ceilings do not supply a
dedicated owner with those resources. Cellule's 64-MiB retained/1-GiB managed
disk policies are unchanged and not matched to celld's internal policies.
Tmpfs and NORMAL do not qualify physical-device power-loss durability. This
SQL ledger differs from the laptop's 1,000-Cell bounded KV workload. No read,
mixed-load or full fault qualification is claimed.

Acceptance still requires sustained repeated target throughput/latency, zero
errors/drops, bounded debt, complete all-ACK recovery and read guardrails. Raw
source, failed trials, binaries, original journals, audits and immutable hash
indexes remain outside Git in
`/Volumes/Workspace/crabbuild-target/native-materializer-credit-20261009`
and the rejected experiment's
`/Volumes/Workspace/crabbuild-target/native-record-catalog-20261009`.
