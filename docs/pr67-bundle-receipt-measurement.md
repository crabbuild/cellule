# Current bundle receipt implementation: measured writes

**Write parity is not achieved.** The frozen working tree completed 514.67
Fleet writes/s and 225.77 Bucket writes/s in short Docker diagnostics. Fleet
was 2.3% below the retained baseline; Bucket was 8.78 times its baseline.
One overloaded repetition with workstation memory pressure cannot establish
a repeatable regression or an attributable improvement.

## What was measured

The candidate is working-tree code above `2dfbaa20223fa9473f9a25d4de16cc36f08f6941`,
including the admitted bundle receipt/actor integration and its assignment
sharing repair. It is **not an unmodified Git commit or the published PR head**.
The complete source manifest SHA-256 is
`fc9b1a91dd03ee11e726718ae3cf02d6fb8981555f450ed22d4eb5d627deba08`.
The measured Linux release SQL binary SHA-256 is
`864d68bec0eed92cf9c870d5b6f27ad2573ca72f7cd28d77dc17e2f13eba0af1`.
Baseline: `b1856728984781cee5398f8d7185fb88fde23993`.
Celld: pinned `f2bf648663a610eefde71f3547ad61e9b896b1f0` image.

The example application does not install the new node bundle producer.
Bundle-proof and bundle-response histogram counts were zero. Existing shared
data packing operates, but ordinary responses and capture cleanup still depend
on follower proofs or per-Cell roots. These numbers measure the current real
application path; they do not measure an activated shared-selection ACK lane.

All six cases used identical client/auditor binaries, fixture bytes, pinned
images, loaded runner, Docker host and point settings. Each had a fresh RustFS
volume. There were 1,000 uniformly active Cells, 96-byte values, 128 clients,
a 128-offer queue, SQL INSERT plus SELECT, and a two-hour request/result ledger.
This does not reproduce the laptop's bounded KV workload.

## Environment and qualification limits

The original 8-vCPU/16-GiB, 300-second profile completed one baseline window at
275.90 writes/s, then failed its warm audit. Its current-code run was interrupted
when host swap left less than 512 MiB free on macOS. That interrupted candidate
has no accepted TPS result. Its partial evidence and the original VM remain
retained. The qualification profile and expected evidence remain unchanged.

The completed measurements below are explicitly a separate **diagnostic**:
one 60-second window after 30 seconds of warmup, on an ARM64 Linux VM with
8 vCPUs and nominal 8 GiB **total** RAM. Owner, two Fleet followers, RustFS and
client share that VM. Node container ceilings remain 8 CPUs/16 GiB, with
4-GiB tmpfs state; these ceilings exceed the available shared memory.
RustFS has a 2-CPU/8-GiB ceiling and the client 4 CPUs/4 GiB.
Cellule retains its matched 64-MiB retained-memory and 1-GiB managed-disk budgets.
The physical workstation also runs another VM and experienced host memory/swap
pressure. These observations establish neither dedicated-node capacity nor
physical-device durability. No separate read or mixed workload was measured.

## Independently reconciled windows

TPS counts successful logical writes completed inside the timed window.
Latency is the exact nearest-rank percentile of measured successful attempts,
including trailing successful completions. Scheduled latency starts at the
offered arrival; request latency starts when sent. Errors and drops are separate.

| Mode / system | Successful writes/s | Successful scheduled p99 ms | Successful request p99 ms | Errors | Queue drops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet / baseline | 526.90 | 219.92 | 145.74 | 753,964 | 114,422 |
| Fleet / current code | 514.67 | 144.77 | 107.99 | 716,143 | 152,977 |
| Fleet / celld | 3,949.52 | 146.95 | 78.92 | 0 | 662,773 |
| Bucket / baseline | 25.70 | 36,560.60 | 14,049.79 | 0 | 118,202 |
| Bucket / current code | 225.77 | 4,856.26 | 4,159.45 | 0 | 106,198 |
| Bucket / celld | 861.78 | 1,553.14 | 758.28 | 0 | 68,038 |

Fleet offered 15,000 writes/s; Bucket offered 2,000/s. The original offers,
attempts, errors, successes, trailing successes and drops reconcile exactly.
Current Fleet had 30,880 successes inside its window and no late successes.
Current Bucket had 13,546 inside and 256 after it. None passes the unchanged
delivery/latency targets or establishes sustainable maximum throughput.

Both Cellule arms passed all-ACK warm reads/retries and bucket-only cold
recovery in both modes. Current Fleet checked 53,045 ACKs and drained in 6.17 s;
current Bucket checked 21,805 and drained in 7.70 s. These cohorts include setup
and warmup. Celld Bucket checked 84,460 ACKs and passed both audits. Celld Fleet
failed the warm audit with 330,004 errors among 394,221 ACKs; cold recovery was
not reached. Its throughput is an observed window rate, not qualified capacity.
The audit failures establish unavailable checks, not proven data loss.

## Attempted second repetition

A second diagnostic attempted the same immutable binaries and workload with
the baseline before the candidate. Its Fleet baseline completed 29,539 writes
inside 60 seconds: **492.32 writes/s**, successful scheduled p99 **181.72 ms**,
696,148 HTTP 503 errors and 174,313 queue drops. Counts reconcile to 900,000
offers. All 50,867 ACKs, including setup and warmup, passed both warm and cold
read/retry audits; drain took 7.53 seconds.

The runner then refused to start the candidate because host disk headroom had
fallen below its unchanged 4-GiB pre-case boundary as workstation swap grew.
The benchmark VM was stopped. Only one of six planned cases was attempted;
there is **no second candidate or celld measurement**, no second paired result,
and no basis for treating the first pair's difference as a repeatable gain.
The independent journal replay verified this baseline window and the frozen
source; qualification remains false.

Its external evidence label is `bundle-receipt-r2-vm8c-20261008`; the evidence
index SHA-256 is
`7d367ee810099d30a13ae6cb13d44328fb4bc50c370d56a03a972c7bd1a5b221`.

In the completed first comparison, current Fleet selected 13,201 roots,
averaging 2.38 materialized commits/root;
Bucket selected 13,643, averaging 1.01. Window-only successful provider PUTs per
completed command were 1.72 and 3.60 respectively. These exclude trailing work
and SDK-internal retries. Per-Cell publication remains sparse; the design's
dense checkpoint and admitted materializer goals have not been demonstrated.

## Verification and evidence

The frozen source passed all 11 contributor verification routes: 1,949 workspace
tests passed, with 38 documented ignored tests. An independent journal replay
verified all six window counts, successful percentiles, ACK stream hashes,
source manifests, adapted build cache key and measured binary hashes. Provider
capacity and lifecycle samples remained valid. This verifies measurement
integrity, not qualification. Three paired five-minute repetitions, the
16-GiB serving-node profile, bounded stable debt, read guardrails and remaining
[design exit gates](../crates/cellule-runtime/docs/write-performance-design.md)
remain outstanding.

Raw evidence stays outside Git under
`/Volumes/Workspace/crabbuild-target/cellule-write-perf-8ad1`, label
`bundle-receipt-r2-vm8b-20261008`. The evidence index SHA-256 is
`c7f3e02daa28f100ac1b1df233357842dfe5a4f32da4d27c419d4564288200b8`.
Failed preflight attempts and interrupted runs are retained separately.
