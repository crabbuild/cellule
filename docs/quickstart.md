# Run the reference application

The [reference application](../crates/cellule-app/tests/reference_application.rs) is a compiled Rust application with seven Cells and typed handles. Its integration test performs user actions through a local router, waits for durable publication, and reads the visible result. Run it from the workspace root:

```sh
cargo test -p cellule-app --test reference_application \
  typed_application_executes_every_primitive_through_a_local_router \
  --locked -- --exact --nocapture
```

The test uses temporary SQLite files and an in-memory object store. It needs no cloud credentials or server process. It exercises the same runtime and LTX code that an embedding service uses, but it does not prove cloud-provider or multi-node deployment behavior.

## Follow one complete use case

The source declares modules, compiles their descriptor with `ApplicationBuilder`, creates the Cells, and invokes the typed `ApplicationHandle`. Follow the path in this order:

1. `ReferenceApplication::register` declares the SQL, KV, Blob, Queue, Cron, Workflow, Activity, and Effect modules. Each namespace and operation ID is stable because the descriptor is persisted with Cells.
2. `compiled` binds those modules to seven `CellType` declarations. The builder validates topology and produces a digest that is checked on restore.
3. `typed_application_executes_every_primitive_through_a_local_router` creates real SQLite databases, publishes their initial roots, and uses typed handles for the actions below.

| User action | Primitive | Verified result |
| --- | --- | --- |
| Place an order | SQL | Insert through a parameterized batch; query the committed total. |
| Save a cart | KV | Write bytes with a mutation identity; read those bytes at the commit receipt. |
| Upload an attachment | Blob | Store content-addressed parts, commit the manifest, then verify a range read. |
| Send a notification | Queue | Send, claim with a lease, and acknowledge; a duplicate request retains its outcome. |
| Fulfill an order | Workflow and Activity | Start a workflow, execute its registered Rust activity, and read terminal state. |
| Schedule an invoice | Cron and Effect | Fire the schedule, deliver the durable effect to its destination Cell, and query the SQL receipt. |

For a shorter first read, run `reference_application_uses_typed_handle_for_a_real_commit` from the same test target. The [application crate guide](../crates/cellule-app/README.md) explains descriptor ownership; the [runtime guide](../crates/cellule-runtime/README.md) explains durability and failure paths.

## Try a multi-process run

The ignored [performance harness](../crates/cellule-app/PERFORMANCE.md) starts three owner processes, routes signed peer requests over loopback TCP, and verifies all six action lanes. Set `CELLULE_PERF_ITERATIONS=1` for a quick functional run. These local examples are not provider or production capacity qualification.
