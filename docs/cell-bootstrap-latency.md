# Cell creation and first-request latency

Creating a Cell performs durable work before its handle can serve a request.
Putting creation inside an HTTP handler moves that work onto the handler's
critical path. It does not remove the publication or ownership checks.

## Required ordering

| Stage | Framework boundary | Required result |
| --- | --- | --- |
| Catalog | `CellCatalog::provision` | Immutable entry selected by its tenant/application shard head |
| Ownership reservation | `CellAuthority::create_initial` | Rootless, Recovering control with one original owner |
| SQLite initialization | `CellRuntime::bootstrap` | Runtime metadata and application schema committed and captured on a SQL worker |
| Initial publication | `prepare_initial` / `publish_prepared` | Verified initial root selected by the fenced Cell control; state becomes Serving |
| Bundle enrollment, when installed | `NodeBundleAuthority` | Provisional node inventory selected, original Cell pinned, then Open inventory selected |
| Serving | Actor activation completion | Handle returned only after publication and enrollment complete |

Ownership is reserved **before** SQLite initialization. Serving authority follows
initial root publication. Bundle enrollment applies when the node has installed
that authority; an ordinary object-only bootstrap does not add it automatically.

The ordinary Cell catalog has 256 shards and no framework-wide async mutex.
The node bundle catalog is a separate inventory. Its authority adapter orders
enrollment, publication and closure around one selected node head. An embedding
can introduce additional serialization outside these framework boundaries.

Independent activations and immutable LTX uploads already overlap. The remaining
enrollment sequence has dependent publication steps, so simply spawning more
requests against a serialized adapter does not eliminate its object-store cost.

## Bounded enrollment cohorts

`NodeDirectory::bind_bundle_cells` enrolls up to eight ready Cells in one
provisional/Open catalog pair. The existing single-Cell method uses this same
implementation. Each Cell still receives its own original control CAS.

For a fresh cohort, without retries:

| Eight ready enrollments | Immutable catalog PUTs | Node selector CASes | Cell pin CASes | Total PUTs |
| --- | ---: | ---: | ---: | ---: |
| Original per-Cell adapter | 16 | 16 | 8 | 40 |
| One bounded cohort | 2 | 2 | 8 | 12 |

Initial SQLite/root publication is additional work in both rows. A lone Cell
still performs five enrollment PUTs. Fresh node and catalog reads remain part
of each pin's authority check; cached metadata does not replace them.

The runtime retains one epoch-owned worker with an eight-entry queue and an
eight-Cell running cohort. One scheduler yield collects ready activations,
without a batching timer or a barrier over the node's entire Cell population.
The runtime ledger reserves 576 KiB for queued/running control metadata and
bookkeeping. Cohort admission checks authenticated shard sizes so simultaneous
pin checks stay within the original single-pin raw metadata allowance. If the
cohort cannot fit, the reference adapter declines it before enrollment uploads
or Cell pinning and invokes individual callbacks. A maximal shard therefore
remains admissible individually, and heartbeat work can run between callbacks.

Implement the optional `NodeBundleAuthority::bind_batch` hook using the canonical
cohort method under the original catalog ordering. The
[reference adapter](../crates/cellule-axum/examples/fleet/authority/mod.rs)
does this. Adapters without the hook continue through individual `bind`
callbacks and receive each result as soon as that callback completes. Different
Cells must reach activation concurrently to benefit: an embedding that holds
one creation-map lock through all bootstrap I/O must narrow that lock first.
Sharing catalog publication does not combine the Cells' SQLite transactions.

All pin attempts finish before the Open catalog is selected. A failed or
cancelled attempt can leave a durable provisional obligation; it cannot open
SQL admission or erase a pinned sibling. Retries reload authority and reuse
the original bindings. Epoch shutdown joins accepted work independently of
caller cancellation and releases the worker's reservation.

## Fresh controlled measurements

The diagnostics run real SQLite initialization, capture, root publication,
control CAS, enrollment, and first reads through the runtime APIs. After each
run they drain and restore the authoritative roots and verify initialized
state. Repeated warm leased reads must perform zero object-store operations.

Both adapters run in the same test binary to isolate enrollment behavior.
The fixture uses an in-memory object store with controlled per-call delays,
temporary local SQLite files, two SQL workers, and three fresh runs per setting.
Each small Cell initializes one counter table to zero, plus runtime metadata.
The shared workstation also runs other workloads. Results describe this local
diagnostic; provider, HTTP, three-VM capacity and ordinary write throughput
require their separate qualification runs.

Run with Rust 1.97.0:

```sh
CARGO_PROFILE_TEST_DEBUG=0 cargo +1.97.0 test -p cellule-runtime --lib comparison \
  --locked -- --ignored --nocapture --test-threads=1
```

`enrollment_measurement` reports ready-to-enroll latency and object operations.
`bootstrap_measurement` reports catalog provisioning through the first SQL read,
including owner creation, SQLite, initial root and enrollment. Each record
contains the adapter, delay, population, sample number and per-Cell latencies.

Fresh full-path results, including the first SQL read (milliseconds):

| Per-call delay | Concurrent new Cells | Serial p50 / p95 | Cohort p50 / p95 |
| --- | ---: | ---: | ---: |
| 5 ms | 1 | 136.6 / 139.5 | 133.8 / 157.9 |
| 5 ms | 8 | 351.5 / 639.3 | 212.1 / 220.6 |
| 20 ms | 1 | 386.7 / 428.6 | 386.4 / 425.3 |
| 20 ms | 8 | 1,121.5 / 2,009.4 | 640.7 / 665.6 |

These are nearest-rank sample percentiles: three observations for a lone Cell,
24 for each eight-Cell setting. The full eight-Cell runs used two enrollment
cohorts as initialization completed, reducing total creation PUTs from 96 to
72 and GETs from 48 to 42. When all eight Cells were already ready to enroll,
the isolated stage used one cohort: 40 to 12 PUTs and 32 to 25 GETs. With 20 ms
per-call delay, that stage's p95 was 1,689.0 versus 251.0 ms.

The single-Cell path keeps its mandatory durable work. Cohorts reduce burst
queueing; a response in a cohort still waits for all its pin checks and the
shared Open selection. See the [raw observations and source checksums](measurements/cell-bootstrap-latency.json).

For known populations, an embedding can provision expected Cells with bounded
concurrency before admitting external traffic. Lazy creation still pays the
durable bootstrap cost on its first request. Neither approach changes the
first write's normal durability/output gate.
