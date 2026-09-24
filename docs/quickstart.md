# Run Cellule locally

The [orders example](../crates/cellule-app/examples/orders.rs) is a small executable application with one SQL Cell. From the workspace root, run:

```sh
cargo run -p cellule-app --example orders --locked
```

It provisions a Cell in an in-memory object store, commits order 42, reads its total at the commit receipt, and prints `order 42 total: 1999 cents`. It uses a temporary SQLite file and needs no credentials. The example shows module registration, Cell provisioning, a typed SQL handle, and a published read in one file.

## Run a local cart Cell

The [carts example](../crates/cellule-app/examples/carts.rs) uses the same one-Cell setup with a typed KV handle. Run it from the workspace root:

```sh
cargo run -p cellule-app --example carts --locked
```

It checks that `customer/42` has no `cart` key, atomically saves `book x2`, reads at the commit receipt, and prints `cart for customer 42: book x2`. Its module descriptor includes the exact version-one KV schema that bootstrap installs. Like orders, it uses a fresh in-memory store and a temporary SQLite file, so its fixed demo release identity is not for persisted deployments.

## Run a local notification Queue

The [notifications example](../crates/cellule-app/examples/notifications.rs) starts one Queue Cell. Run it from the workspace root:

```sh
cargo run -p cellule-app --example notifications --locked
```

It sends `order 42 ready`, claims the message, validates the published lease before processing, acknowledges that exact message and token, and confirms the Queue reports one acknowledgement. It prints `notification acknowledged: order 42 ready`. Its descriptor declares the Queue's version-one schema and enough capacity for the Queue's maximum send payload. The store and SQLite file are fresh on every run.

## Run a native consumer

The [consumers example](../crates/cellule-app/examples/consumers.rs) starts one Queue Cell with a compiled `QueueConsumer`. Run it from the workspace root:

```sh
cargo run -p cellule-app --example consumers --locked
```

It sends two jobs, runs one consumer pass that acknowledges the steady job and retries the flaky one with the consumer's delay, then runs again after the retry. A single ready job is deferred until the batch timeout closes the batch, which shows how the claim forms batches before leasing anything. The final count read prints `consumer settled 3 jobs`.

## Run a fulfillment Workflow and Activity

The [fulfillment example](../crates/cellule-app/examples/fulfillment.rs) starts one Workflow Cell with a registered native Activity. Run it from the workspace root:

```sh
cargo run -p cellule-app --example fulfillment --locked
```

The workflow starts in `Running`, schedules a `pack-order` Activity, and the supervisor claims and executes that Rust handler. The definition uses `decode_workflow_activity_event` to handle either a completed or failed Activity without parsing event bytes itself. The activity completion advances the workflow to `Completed`; a read at the completion receipt verifies the result before printing `fulfillment completed: packed order 42`. The definition digest tracks the example source, and its module descriptor contains the exact version-one Workflow schema. This example also uses fresh local storage on every run.

## Run a Cron invoice delivery

The [invoices example](../crates/cellule-app/examples/invoices.rs) starts a Cron Cell and a destination SQL Cell. Run it from the workspace root:

```sh
cargo run -p cellule-app --example invoices --locked
```

It creates an invoice schedule, fires one due maintenance tick, and verifies the schedule advanced to occurrence one. The effect supervisor then delivers a signed invocation through an in-process peer hop to the SQL Cell. A SQL read confirms the destination row before the example prints `invoice delivered: invoice 42 ready`. The hop exercises signing, verification, authorization, and the destination inbox without requiring a network service. The example uses temporary SQLite files and in-memory object storage; a deployed service supplies its own peer transport and credentials.

## Run a Timer reservation timeout

The [timeouts example](../crates/cellule-app/examples/timeouts.rs) starts a Timer Cell and a destination SQL Cell. Run it from the workspace root:

```sh
cargo run -p cellule-app --example timeouts --locked
```

It reserves stock, schedules a one-shot deadline that is already due, fires one maintenance tick, and verifies the deadline was removed in the same transaction. The effect supervisor then delivers the signed invocation through an in-process peer hop to the SQL Cell. A SQL read confirms the reservation was released before the example prints `timeout delivered: reservation 42 released by timeout`. Use Timer when a mutation must run once at a chosen time; use Cron when the trigger recurs.

## Run the reference application

The [reference application](../crates/cellule-app/tests/reference_application.rs) is a compiled Rust application with nine Cells and typed handles. Its storefront smoke places an order, saves a cart, uploads an attachment, sends a notification, completes a workflow activity, delivers a scheduled invoice effect, and projects an order change into a read model. Run it from the workspace root:

```sh
cargo test -p cellule-app --test reference_application \
  performance::reference_storefront_smoke \
  --locked -- --exact --nocapture
```

The test uses temporary SQLite files and an in-memory object store. It needs no cloud credentials or server process. Each action checks its published result through a typed handle. The `PERF` lines each contain one sample and are completion evidence, not useful latency estimates. The test exercises the same runtime and LTX code that an embedding service uses, but it does not prove cloud-provider or multi-node deployment behavior.

## Follow one complete use case

The [application source](../crates/cellule-app/tests/reference_application.rs) declares modules and compiles their descriptor with `ApplicationBuilder`. The [fixture](../crates/cellule-app/tests/reference_application/performance_fixture.rs) creates the Cells and typed handles. The [smoke actions](../crates/cellule-app/tests/reference_application/performance.rs) invoke those handles and check visible results. Follow the path in this order:

1. `ReferenceApplication::register` declares the SQL, KV, Blob, Queue, Cron, Timer, Read-model, Workflow, Activity, and Effect modules. Each namespace and operation ID is stable because the descriptor is persisted with Cells. Its version-one migration SQL matches the schema installed at bootstrap.
2. `compiled` binds those modules to nine `CellType` declarations. The builder validates topology and produces a digest that is checked on restore.
3. `PerfFixture::start` creates real SQLite databases, publishes their initial roots, and routes the typed handles locally.
4. `reference_storefront_smoke` runs one verified action per lane in `run_reference_primitive_performance`.

| User action | Primitive | Call sequence in the smoke | Verified result |
| --- | --- | --- | --- |
| Place an order | SQL | `batch` → `query` at the commit receipt | The committed total is returned. |
| Save a cart | KV | `atomic(Put)` → `get` at the commit receipt | The saved cart bytes are returned. |
| Upload an attachment | Blob | `mutate(Begin)` → `mutate(PutPart)` → `mutate(Complete)` → `query(Read)` | A range read returns the uploaded bytes. |
| Send a notification | Queue | `send` → `claim` → `validate_claim` → `ack` | The claimed message matches and its lease is acknowledged. |
| Consume notifications | Queue consumer | `send` → `queue_consumer::<M>(lease_ms)` → `run_once` | The full batch settles and the acknowledged count grows. |
| Fulfill an order | Workflow and Activity | `start` → `ActivitySupervisor::run_once` → `state` | The registered Rust activity completes the workflow. |
| Schedule an invoice | Cron and Effect | `mutate(Upsert)` → maintenance tick → `get` → effect delivery → SQL `query` | The fired schedule reaches its destination and the receipt is readable. |
| Release a reservation | Timer and Effect | `mutate(Set)` → maintenance tick → `get` → effect delivery → SQL `query` | The fired deadline is removed and the destination row shows the release. |
| Project an order | Projection and Effect | `command(PublishOrderChange)` → `run_effect_once` → SQL `query` → `ProjectionStatusQuery` | The read model row and its per-source watermark advance together. |

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
