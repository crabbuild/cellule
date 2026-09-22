# Run a local orders Cell

The [orders example](../crates/cellule-app/examples/orders.rs) is a small executable application with one SQL Cell. From the workspace root, run:

```sh
cargo run -p cellule-app --example orders --locked
```

It provisions a Cell in an in-memory object store, commits order 42, reads its total at the commit receipt, and prints `order 42 total: 1999 cents`. It uses a temporary SQLite file and needs no credentials. The example shows module registration, Cell provisioning, a typed SQL handle, and a published read in one file.

## Run the reference application

The [reference application](../crates/cellule-app/tests/reference_application.rs) is a compiled Rust application with seven Cells and typed handles. Its storefront smoke places an order, saves a cart, uploads an attachment, sends a notification, completes a workflow activity, and delivers a scheduled invoice effect. Run it from the workspace root:

```sh
cargo test -p cellule-app --test reference_application \
  performance::reference_storefront_smoke \
  --locked -- --exact --nocapture
```

The test uses temporary SQLite files and an in-memory object store. It needs no cloud credentials or server process. Each action checks its published result through a typed handle. The `PERF` lines each contain one sample and are completion evidence, not useful latency estimates. The test exercises the same runtime and LTX code that an embedding service uses, but it does not prove cloud-provider or multi-node deployment behavior.

## Follow one complete use case

The [application source](../crates/cellule-app/tests/reference_application.rs) declares modules and compiles their descriptor with `ApplicationBuilder`. The [fixture](../crates/cellule-app/tests/reference_application/performance_fixture.rs) creates the Cells and typed handles. The [smoke actions](../crates/cellule-app/tests/reference_application/performance.rs) invoke those handles and check visible results. Follow the path in this order:

1. `ReferenceApplication::register` declares the SQL, KV, Blob, Queue, Cron, Workflow, Activity, and Effect modules. Each namespace and operation ID is stable because the descriptor is persisted with Cells.
2. `compiled` binds those modules to seven `CellType` declarations. The builder validates topology and produces a digest that is checked on restore.
3. `PerfFixture::start` creates real SQLite databases, publishes their initial roots, and routes the typed handles locally.
4. `reference_storefront_smoke` runs one verified action per lane in `run_reference_primitive_performance`.

| User action | Primitive | Call sequence in the smoke | Verified result |
| --- | --- | --- | --- |
| Place an order | SQL | `batch` → `query` at the commit receipt | The committed total is returned. |
| Save a cart | KV | `atomic(Put)` → `get` at the commit receipt | The saved cart bytes are returned. |
| Upload an attachment | Blob | `mutate(Begin)` → `mutate(PutPart)` → `mutate(Complete)` → `query(Read)` | A range read returns the uploaded bytes. |
| Send a notification | Queue | `send` → `claim` → `validate_claim` → `ack` | The claimed message matches and its lease is acknowledged. |
| Fulfill an order | Workflow and Activity | `start` → `ActivitySupervisor::run_once` → `state` | The registered Rust activity completes the workflow. |
| Schedule an invoice | Cron and Effect | `mutate(Upsert)` → maintenance tick → `get` → effect delivery → SQL `query` | The fired schedule reaches its destination and the receipt is readable. |

To inspect recovery and capability checks, run the separate `typed_application_executes_every_primitive_through_a_local_router` test from the same target. The [application crate guide](../crates/cellule-app/README.md) explains descriptor ownership; the [runtime guide](../crates/cellule-runtime/README.md) explains durability and failure paths.

## Try a multi-process run

The ignored [performance harness](../crates/cellule-app/PERFORMANCE.md) starts three owner processes, routes signed peer requests over loopback TCP, and verifies all six action lanes. Run one iteration per lane:

```sh
CELLULE_PERF_ITERATIONS=1 cargo test -p cellule-app --test reference_application \
  process_performance::reference_three_process_fleet_end_to_end_performance \
  --locked -- --ignored --exact --nocapture
```

This local run is not provider or production capacity qualification.

To exercise one additional routing hop, run the balanced variant. A loopback listener sends each request to one of three owner processes; the receiving process serves a local Cell or forwards the signed request to its owner. The test checks that every process both serves and forwards requests:

```sh
CELLULE_PERF_ITERATIONS=1 cargo test -p cellule-app --test reference_application \
  process_performance::reference_balanced_three_process_fleet_end_to_end_performance \
  --locked -- --ignored --exact --nocapture
```

The [performance guide](../crates/cellule-app/PERFORMANCE.md) describes the topology and measurement limits. These runs use a test-only filesystem CAS store and static placement; they do not qualify cloud storage or production routing.
