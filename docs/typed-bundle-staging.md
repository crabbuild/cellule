# Typed bundle staging

October 10, 2026. Production protocol APIs now separate immutable encoding from
upload, permitting one exact successor to prepare while its predecessor's PUT
is pending. At `d42b978` the managed serving publisher remained serial. The
[managed pipeline](managed-bundle-pipeline.md) now connects those phases to the
runtime producer. **No new application throughput or capacity claim follows
from this change.**

## Protocol and ownership

| Phase | Value | Permitted action |
| --- | --- | --- |
| Encode | `Arc<StagedNodeBundle>` | Share immutable bytes; prepare one contiguous successor |
| Upload completes | `PreparedNodeBundle` | Attempt complete fresh verification and ordered selection |
| Authoritative selection completes | `BundleCoverageProof` | Confirm original assignments through the durability gate |

`prepare_node_bundle_with_checkpoints` retains the existing manual/managed path
by calling staging then upload. There is one encoder and one upload path. The
uploaded value shares its staged allocation; it does not clone the catalog or
body. Staged values cannot be passed to selection, enforced by a compile-fail
API test.

A staged predecessor must extend the exact currently observed head in the same
owner session and log epoch and retain its complete native assignments. A third
unselected cohort is rejected before metadata reads. Once selection advances,
the same two-stage window can advance with it. No node-wide transaction order
is imposed on application transactions beyond the existing replication lane.

The predecessor view supplies catalog metadata only. It is a different type
from the fresh origin view required by selection, which still reads the complete
uploaded object and verifies every needed reconstruction dependency. Immutable
PUT completion supplies no authority or response credit. A changed catalog or
fenced writer rejects selection; heartbeat-only rebasing keeps its canonical
checks. CNB3 encoding and the existing frame, history and object bounds remain
unchanged.

## Deterministic checks

The real 64-Cell/2,000-binding case stages two cohorts without any PUT, rejects
an uploaded successor with a missing predecessor, rejects selection out of
order after both uploads, then selects in order. Every participating Cell
cold-restores its seed and both exact mutation/result pairs. Losing a required
predecessor later still rejects fresh full verification.

A held-PUT case starts the first upload, prepares the successor while that PUT
is pending, completes the second upload and checks zero coverage. After the
first upload joins, coverage advances only through the two ordered selections.
Other cases cover intervening sibling binding, fenced late uploads, another
owner session and the two-stage bound. Existing byte-compatibility and cold
recovery tests continue to exercise the unchanged encoding.

The bounded model includes preparation ahead of upload/selection and retains
all existing safety invariants and five negative checks. An expected
`NeverPrepareAhead` counterexample proves the new schedule is reachable. It
does not establish adapter lifecycle, memory accounting, liveness or provider
persistence.

## Serving integration boundary

Manual callers own admission and the joined lifetime of accepted uploads. At
`d42b978` the managed producer admitted 20 MiB working memory plus its 2 MiB
shard cache and ran one complete selection at a time. Production overlap must
admit both retained stages and receipt allocations before dispatching overlapping
work, preserve heartbeat renewal and exact checkpoint callbacks, and join all
accepted I/O on failure, fence, cancellation and drain. Receipt pressure must
release catalog ordering before waiting for credit that needs a checkpoint.
The managed pipeline uses nonblocking admission while holding ordering and
returns to the serial path if that extra credit is unavailable. The startup
contract and budgets remain unchanged.

The [external prototype](bundle-preparation-pipeline-prototype.md) supplied the
protocol experiment. The [current measurements](single-pass-bundle-encoding.md)
remain below the requested 2,000 writes/s plus 20,000 reads/s target. Production
load measurement follows managed integration, not the new API alone. The
[managed integration report](managed-bundle-pipeline.md) describes the subsequent
admitted overlap, serial fallback and joined lifetime checks.

## Verification evidence

All 13 contributor routes pass on an isolated snapshot, including 2,090 workspace
tests with 43 environment-dependent cases ignored, strict Clippy and API docs.
The additional focused bundle route passes 120 cases; the workspace includes
11 further bundle codec cases, for 131 in total. The compile-fail staged-selection
case passes. The model checks 47,498 distinct states, retains all five expected
negative violations and reaches the two-preparation schedule. All 1,357 Rust/Cargo
files match the tested source.

Logs, source manifests and tooling remain outside Git under
`cellule-ios-parity-20261010/typed-bundle-staging`. The `evidence-index.json`
SHA-256 is `6fe2244ac5e1f908bfdefb60241cdd508fb6a5cedecd51a3ba604841bde11b27`.
