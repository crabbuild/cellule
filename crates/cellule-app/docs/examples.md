# Runnable primitive examples

Run these from the workspace root with Rust 1.97 or newer. Each example uses
temporary SQLite files, an in-memory object store, and a local fenced owner.
They need no cloud credentials. Read the ASCII flow at the top of each source
file, then follow its `main` function from descriptor compilation to shutdown.

```text
modules + Cell types -> compiled descriptor -> catalog + fenced owner
    -> managed SQLite/LTX -> typed ApplicationHandle -> command + receipt
```

| Run | Primitives | What it demonstrates |
| --- | --- | --- |
| `cargo run -p cellule-app --example basic --locked` | KV, Queue | A scoped atomic setting write and receipt-bound read; send, claim, validate, and acknowledge one leased job. |
| `cargo run -p cellule-app --example sql --locked` | SQL | Parameterized INSERT and SELECT in one Cell, with the write receipt as the read minimum. |
| `cargo run -p cellule-app --example blob --locked` | Blob | Begin, stage one part, complete a Blob, then verify bytes and content type at the completion receipt. |
| `cargo run -p cellule-app --example workflow --locked` | Workflow, Activities | Start a durable run, have an `ActivitySupervisor` validate and execute its activity, then read the completed state. |
| `cargo run -p cellule-app --example schedules --locked` | Cron, Effects | Register a fixed-interval schedule, drive one due maintenance tick, deliver its effect through a signed local peer loopback, and count the destination row. |

For a complete HTTP service, run
`cargo run --manifest-path examples/application-builder-service/Cargo.toml --locked`.
The [service guide](../../../examples/application-builder-service/README.md)
uses native module registration and scoped factories, authorized Axum routes,
OpenAPI, retained command evidence, and ordered host shutdown.

## How each path works

**[basic.rs](../examples/basic.rs)** compiles two modules with distinct
namespaces and roles. `KvNamespace::atomic` writes `theme=dark` in one scope;
`get(Some(receipt))` proves the published value is visible. The Queue handle
sends `send-email`, claims a lease, checks that exact token against the owner,
and acknowledges it. Real consumers perform idempotent external work between
validation and acknowledgement because delivery is at least once.

**[sql.rs](../examples/sql.rs)** declares a SQL migration and fixed
command/query IDs, provisions one Cell, then calls `SqlCell::batch` with
parameterized values and a stable request identity. The returned receipt gates
a query that checks the committed order total. The example drains the runtime
on both success and failure.

**[blob.rs](../examples/blob.rs)** adds a `BlobArtifactStore` to its typed
application handle. `Begin` creates an upload, `PutPart` stages immutable
bytes, and `Complete` publishes a reference in the Blob Cell. The completion
receipt gates the read. A staged part alone is not a visible attachment.

**[workflow.rs](../examples/workflow.rs)** pins a definition digest and
registers an `Echo` activity. Starting the workflow records a `Running` state
and activity intent. The installed supervisor claims and validates that work,
runs the handler outside SQLite, and records completion. A receipt-bound state
query then reports `Completed`.

**[schedules.rs](../examples/schedules.rs)** compiles the Cron source and SQL
receiver contracts together. The application registers a schedule and reads it
at its receipt. One explicit maintenance tick emits a durable source effect;
the `EffectSupervisor` validates its lease and delivers a signed command to the
receiver's idempotent inbox. A SQL query confirms one occurrence. The
[shared local fixture](../examples/support/mod.rs) only supplies provider,
catalog, authority, and runtime setup for these advanced lessons. A serving
application owns its scheduler, peer receiver, authorization, and supervisors.

## Continue with recovery and process behavior

The [application integration suite](../tests/integration.rs) covers all eight
primitives, restores from published roots under a successor owner, and checks
host routing, replicas, and rollout. Its local primitive scenario is:

```sh
cargo test -p cellule-app --test integration \
  primitives::typed_application_executes_every_primitive_through_a_local_router \
  --locked -- --exact --nocapture
```

The [contract suite](../tests/contracts.rs) checks stable identities and
descriptors. The [performance guide](../PERFORMANCE.md) names workloads and
their environment boundaries. Local examples and tests do not establish
cloud-provider or production qualification; see the
[qualification guide](../../cellule-runtime/qualification/README.md).
