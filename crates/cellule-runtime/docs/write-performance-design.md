# Node write and read performance design

Status: implementation in progress. The application path is not qualified at
the targets below. Component I/O reductions are not application TPS.

Cellule is the management process for a node's Cells. A Cell retains one fenced
writer; node-wide admission, native workers, publication, recovery and collection
must share bounded resources rather than create an independent service per Cell.
The embedding application continues to own ingress and authorization.

## Capacity contract

| Dimension | Required node result |
| --- | --- |
| Serving node | 8 vCPUs, 16 GiB memory; report storage, filesystem, network and SQLite policy |
| Population | 2,000 uniformly active Cells, sub-100-byte values |
| Writes | 10,000 successful commands/s |
| Reads | 50,000 successful queries/s |
| Tail latency | Fleet writes p99 at most 50 ms; Bucket writes p99 at most 200 ms; report read p50/p95/p99 |
| Delivery | Zero errors, dropped offers or unissued requests; at least 99% completed within the window |
| Evidence | Three paired repetitions of at least five minutes, matched celld revision and workload |
| Durability | All acknowledged mutations and retry outcomes survive warm audit, joined drain and cold restore |
| Stability | Bounded memory, native-job occupancy and debt; debt has no sustained positive slope |

Qualify read-only, write-only and simultaneous 10K-write/50K-read load separately.
Count the client, provider and followers separately from the serving node.
Docker can simulate the deployment, but sharing one 8-CPU VM between all roles
does not qualify an 8-CPU serving node. Report KV overwrite and SQL commands with
the durable request/result ledger separately. tmpfs results do not qualify
physical-media durability. Preserve the stronger historical comparison gates in
the [workspace proposal](../../../docs/write-performance-proposal.md); this node
capacity contract does not turn a failed earlier profile into a passing one.

## Canonical write path

```mermaid
flowchart LR
    A[Bounded admission before SQL] --> B[Mutation and retry result commit together]
    B --> C[Exact complete capture assignment]
    C --> D[Ordered shared native log]
    D --> E[Authenticated follower durable proof]
    E --> F[Fleet ACK]
    C --> G[One admitted cross-Cell bundle]
    G --> H[Original bindings and exact range verification]
    H --> I[Canonical node selector CAS]
    I --> J[Bucket ACK and proven visibility]
    I --> K[Bounded joined root materializer]
    K --> L[Exact root cohort checkpoint]
```

Uploaded bytes are not authority. Bundle ACKs remain disabled until the actor,
worker, read/retry endpoint, recovery and lifecycle consumers agree on one
opaque complete proof. Do not advance the follower reclamation frontier before
failed-owner recovery understands the selected bundle prefix.

## Locator density and checkpoint cost

Under the measured small-tail four-PUT materialization path, 64 commands per
shared selection and 64 roots per checkpoint give the conditional cost
`2/64 + (4 + 2/64)/K`. At most 0.05 publication PUTs/command therefore requires
at least **215 commands per Cell checkpoint**, before compaction, retries and
collection. Larger tails can leave the four-PUT path and must be measured.

The prior 32 inline frame locators cannot represent this density under uniform
traffic. Increasing only that array also increases every sibling's shard read.
The implemented development representation separates small binding rows from authenticated,
independently addressed locator histories:

- Keep the 256-shard fixed authenticated header and original binding pins.
- Keep unchanged binding shards and histories by exact object/range/digest.
- Store a Cell history in a bounded extent in the same selection object; adding
  a history adds bytes, not a separate PUT. Never rewrite old native bodies.
- Load histories only for the requested Cell/cohort, retaining sibling history
  references without decoding their contents. Preflight aggregate selected
  shard and history metadata against the existing 4 MiB protocol-input bound.
- Bound each history to 256 exact frame references and 32 KiB encoded metadata;
  preserve the 4 MiB verified native-suffix byte bound and 64 frames/selection.
- New selectors use a distinct format version. Existing complete catalogs and
  inline indexed shards remain readable; upgrade every recovery consumer before
  selecting the new format. Use a fresh development prefix during rollout.

Those are protocol bounds, not host admission. The materializer scheduler must
charge retained history, native verification, scratch and outcomes to the node
ledgers. Uniform 10K writes over 2,000 Cells means about five commands/Cell/s:
215 commands span about 43 seconds. Measure the actual native bytes retained
over that interval and reject the model if the 16-GiB node cannot hold its debt.

## Remaining implementation and exit gates

| Work | Required verification |
| --- | --- |
| Dense histories | 215 exact commands without checkpoint; corrupt/missing/cross-scope history rejection; bounded selected metadata; byte-identical cold restore; measured larger-tail cost |
| Actor ACK/read/retry integration | Response and visible retry/query state use the same original proof; captured ranges release only after exact matching; unproven suffix remains hidden |
| Canonical coverage frontier | Bundle selector and contiguous native coverage selected atomically; no second authority CAS; ambiguous replies, heartbeat, lease loss and cancellation tested |
| Admitted materializer | One bounded node scheduler, fair cohorts, debt limit and joined native/storage jobs on cancellation and shutdown |
| Failed-owner recovery | Seal complete issued suffix including prior Fleet ACKs; selected prefix plus follower tail restore exactly before closing all bindings or admitting replacement writers |
| Safe collection | Complete cross-Cell root/catalog/proof/reference inventory, admitted scans, quiesced writes and grace-qualified deletion |
| Read efficiency | Bounded workers and views, proven snapshots, minimal per-query authority I/O; overload retains renewal/publication headroom |
| Paired qualification | Node capacity contract and unchanged durability/read/drain gates pass; otherwise report measured gaps with source and binary identities |

Implementation and prior measured results are tracked in the
[bundle delivery record](../../../docs/bundle-coverage-implementation.md) and
[WAL comparison](../../../docs/pr67-normal-wal-reevaluation.md). Keep bulk logs,
raw request ledgers and source manifests outside the repository.

The recovery coordinator now discovers selected-only Cells and joins exact
origin prefixes with the complete sealed follower witness in one admitted
file-backed reconstruction path. It rejects conflicting overlap and incomplete
Cell inventory. Bound overlays retain the original pin, and the canonical node
seal refuses unfinished materialization/checkpoint closure. `recover_and_seal`
now materializes original bound roots through ordinary lineage and exact Cell
CAS, then stages bounded catalog cohorts and selects their complete terminal
inventory with one recovery-claim node CAS. Quiet Cells participate. Exact
manifest reuse supports partial-root retries, and closed original roots support
resumption after transfer or interrupted log seal. Provisional enrollment
resolution, admitted host scheduling and full qualification remain required
before enabling the canonical coverage frontier or actor ACKs.
